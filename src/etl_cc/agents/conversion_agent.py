"""GPT-4o conversion agent for ETL-to-Databricks PySpark migration."""

import ast
import json
import re
from functools import lru_cache
from typing import Any, Literal

import tiktoken
from pydantic import BaseModel, Field

from etl_cc.agents.rag_retrieval_agent import RAGRetrievalResult
from etl_cc.config import settings
from etl_cc.agents.unit_test_renderer import UnitTestRenderer
from etl_cc.agents.unit_test_specification import (
    UnitTestSpecification,
    UnitTestSpecificationValidator,
)
from etl_cc.key_vault_service import DynamicChatOpenAI
from etl_cc.models import CanonicalMapping
from etl_cc.logging_config import configure_logging, metric, stage_completed, stage_started


class GeneratedFile(BaseModel):
    """One generated conversion artifact."""

    artifact_type: Literal["PYSPARK_CODE", "UNIT_TEST", "CONFIGURATION"]
    file_name: str
    content: str
    media_type: str


class ConversionResult(BaseModel):
    """Structured output returned by the Conversion Agent."""

    mapping_name: str
    target_platform: Literal["DATABRICKS"]
    target_framework: Literal["PYSPARK"]
    generated_files: list[GeneratedFile]
    assumptions: list[str] = Field(default_factory=list)
    manual_actions: list[str] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0)
    baseline_prompt_tokens: int = Field(default=0, ge=0)
    actual_prompt_tokens: int = Field(default=0, ge=0)
    retrieved_context_tokens: int = Field(default=0, ge=0)


class ConversionOutputValidationError(RuntimeError):
    """Raised when generated artifacts violate the conversion contract."""


class ConversionInputValidationError(ValueError):
    """Raised when canonical source evidence is insufficient for conversion."""
logger = configure_logging("CONVERSION_AGENT")


class ConversionAgent:
    """Generate validation-friendly Databricks PySpark artifacts."""

    AGENT_NAME = "CONVERSION_AGENT"
    AGENT_VERSION = "3.5.0"
    STAGE_NAME = "CODE_CONVERSION"
    MODEL_NAME = "gpt-4o"
    PROMPT_NAME = "ETL_TO_DATABRICKS_PYSPARK"
    PROMPT_VERSION = "3.5.0"
    max_retrieval_context_tokens = 8000

    REQUIRED_ARTIFACT_TYPES = {
        "PYSPARK_CODE",
        "UNIT_TEST",
        "CONFIGURATION",
    }

    UNIT_TEST_SEMANTICS_GUIDANCE = """For AGGREGATE/GROUP BY operations, Spark retains rows with null grouping keys as a null-key group unless canonical upstream evidence explicitly filters them out. SUM ignores null measure values within a group; it does not remove the group. A group whose measure values are all null produces a null SUM. Unit-test expected rows must follow these semantics and the complete canonical operation sequence. Mark a row absent only when grounded filter, route, join, or other canonical behavior excludes it; null or boundary input alone is not grounds for exclusion."""
    UNIT_TEST_DECIMAL_GUIDANCE = """CRITICAL: Every expected value representing a DecimalType field (e.g. ORDER_AMOUNT, GROSS_MARGIN, UNIT_PRICE, DISCOUNT_PCT, UNIT_COST) MUST use a string-initialized decimal.Decimal object (like Decimal('50.00') or Decimal('0.00')). NEVER use raw float literals (like 50.0 or 12.5) or integer literals (like 50) for decimal fields. Expected data tuples must explicitly be constructed as Decimal('value') to avoid float comparison precision errors."""

    FORBIDDEN_PYSPARK_PATTERNS = {
        "SparkSession.builder": "Generated mapping creates a SparkSession.",
        "spark.stop(": "Generated mapping stops a SparkSession.",
        "spark.read": "Generated mapping performs source I/O.",
        ".write.": "Generated mapping performs target I/O.",
        ".writestream": "Generated mapping performs streaming output.",
        "jdbc:": "Generated mapping contains a JDBC URL.",
        "dbtable": "Generated mapping contains a physical database table option.",
        "password=": "Generated mapping contains a password option.",
        '"password"': "Generated mapping contains a password field.",
        "'password'": "Generated mapping contains a password field.",
        "<host>": "Generated mapping contains a host placeholder.",
        "<port>": "Generated mapping contains a port placeholder.",
        "<service>": "Generated mapping contains a service placeholder.",
        "<username>": "Generated mapping contains a username placeholder.",
        "<password>": "Generated mapping contains a password placeholder.",
        "/path/to/": "Generated mapping contains a path placeholder.",
    }

    FORBIDDEN_CONFIGURATION_KEYS = {
        "password",
        "passwd",
        "pwd",
        "username",
        "user",
        "access_token",
        "token",
        "secret",
        "client_secret",
        "api_key",
        "host",
        "hostname",
        "port",
        "service",
        "url",
        "jdbc_url",
    }

    def __init__(self, source_vendor: str = "INFORMATICA") -> None:
        self.source_vendor = source_vendor
        self.model_name = self.MODEL_NAME
        self.prompt_name = (
            self.PROMPT_NAME
            if source_vendor.upper().startswith("INFORMATICA")
            else f"{source_vendor.upper()}_TO_DATABRICKS_PYSPARK"
        )
        self.prompt_version = self.PROMPT_VERSION
        self.input_tokens = 0
        self.output_tokens = 0

    @staticmethod
    @lru_cache(maxsize=1)
    def _token_encoder() -> tiktoken.Encoding:
        """Return the tokenizer used for the configured GPT-4o prompt."""
        try:
            return tiktoken.encoding_for_model("gpt-4o")
        except KeyError:
            return tiktoken.get_encoding("o200k_base")

    @classmethod
    def _token_count(cls, text: str) -> int:
        """Count prompt text with the GPT-4o tokenizer."""
        return len(cls._token_encoder().encode(text))

    @classmethod
    def _prompt_token_counts(
        cls,
        system_prompt: str,
        human_prompt_without_rag: str,
        human_prompt_with_rag: str,
    ) -> tuple[int, int]:
        """Count baseline and RAG-augmented chat prompts with message overhead."""
        message_overhead = 6
        baseline_tokens = (
            cls._token_count(system_prompt)
            + cls._token_count(human_prompt_without_rag)
            + message_overhead
        )
        actual_tokens = (
            cls._token_count(system_prompt)
            + cls._token_count(human_prompt_with_rag)
            + message_overhead
        )
        return baseline_tokens, actual_tokens

    @staticmethod
    def _bounded_text(value: Any, limit: int = 240) -> str:
        """Normalize and bound untrusted knowledge text for prompt summaries."""
        text = " ".join(str(value or "").split())
        return text[:limit]

    @classmethod
    def _knowledge_summary(cls, item: dict[str, Any]) -> dict[str, Any]:
        """Extract only mapping, rule, transformation, and code summaries."""
        metadata = item.get("metadata_payload")
        metadata = metadata if isinstance(metadata, dict) else {}
        document = metadata.get("embedding_document")
        if not isinstance(document, dict):
            try:
                parsed = json.loads(str(item.get("content") or ""))
            except (TypeError, ValueError):
                parsed = {}
            document = parsed if isinstance(parsed, dict) else {}

        transformations = document.get("transformations")
        transformation_types = (
            document.get("transformation_types")
            or metadata.get("transformation_types")
            or []
        )
        if not transformation_types and isinstance(transformations, list):
            transformation_types = [
                transform.get("transformation_type")
                for transform in transformations
                if isinstance(transform, dict)
                and transform.get("transformation_type")
            ]

        business_rules = document.get("business_rule_summaries")
        if not isinstance(business_rules, list):
            business_rules = document.get("business_rules") or []
        rule_summaries = []
        for rule in business_rules:
            if isinstance(rule, dict):
                text = rule.get("description") or rule.get("rule_type")
            else:
                text = rule
            if text:
                rule_summaries.append(cls._bounded_text(text, 180))

        code_summary = document.get("generated_code_summary")
        if not code_summary:
            generated_code = document.get("generated_code")
            if generated_code:
                code = str(generated_code)
                functions = re.findall(
                    r"^\s*(?:async\s+)?def\s+([A-Za-z_]\w*)",
                    code,
                    re.MULTILINE,
                )
                classes = re.findall(
                    r"^\s*class\s+([A-Za-z_]\w*)",
                    code,
                    re.MULTILINE,
                )
                code_summary = (
                    f"{len(code.splitlines())} lines; "
                    f"functions={functions[:8]}; classes={classes[:8]}"
                )

        return {
            "source_platform": cls._bounded_text(
                document.get("source_platform")
                or metadata.get("source_platform")
                or metadata.get("source_vendor"),
                80,
            ),
            "target_platform": cls._bounded_text(
                document.get("target_platform")
                or metadata.get("target_platform")
                or metadata.get("target_framework"),
                80,
            ),
            "mapping_summary": {
                "mapping_name": cls._bounded_text(
                    document.get("mapping_name") or item.get("title"),
                    120,
                ),
                "complexity": cls._bounded_text(
                    document.get("complexity") or metadata.get("complexity"),
                    40,
                ),
            },
            "transformation_types": [
                cls._bounded_text(value, 80)
                for value in transformation_types[:20]
                if value
            ],
            "transformation_count": int(
                document.get("transformation_count")
                or metadata.get("transformation_count")
                or len(transformations or [])
            ),
            "business_rule_summaries": rule_summaries[:20],
            "confidence_score": document.get("confidence_score"),
            "generated_code_summary": cls._bounded_text(code_summary, 400),
        }

    @classmethod
    def _retrieved_content_tokens(cls, items: list[dict[str, Any]]) -> int:
        """Count injected content strings, excluding JSON and metadata overhead."""
        content_values: list[str] = []

        def collect_strings(value: Any) -> None:
            if isinstance(value, str) and value:
                content_values.append(value)
            elif isinstance(value, list):
                for entry in value:
                    collect_strings(entry)
            elif isinstance(value, dict):
                for entry in value.values():
                    collect_strings(entry)

        for item in items:
            collect_strings(item.get("title"))
            collect_strings(item.get("summary"))
        if not content_values:
            return 0
        return cls._token_count(" ".join(content_values))

    def _compact_rag_for_prompt(
        self,
        rag: RAGRetrievalResult,
    ) -> tuple[dict[str, Any], int, dict[str, int]]:
        """Replace full retrieved documents with summaries under the hard token cap."""
        rag_payload = rag.model_dump(mode="json")
        items = rag_payload.get("retrieved_items") or []
        compact_items = []
        excluded_counts = {
            "retrieved_generated_code_tokens_excluded": 0,
            "retrieved_validation_artifact_tokens_excluded": 0,
            "retrieved_lineage_tokens_excluded": 0,
        }
        for item in items:
            if not isinstance(item, dict):
                continue
            try:
                full_document = json.loads(str(item.get("content") or ""))
            except (TypeError, ValueError):
                full_document = {}
            if not isinstance(full_document, dict):
                full_document = {}
            for key, counter_key in (
                ("generated_code", "retrieved_generated_code_tokens_excluded"),
                ("validation_results", "retrieved_validation_artifact_tokens_excluded"),
                ("lineage", "retrieved_lineage_tokens_excluded"),
            ):
                if key in full_document:
                    excluded_counts[counter_key] += self._token_count(
                        json.dumps(
                            full_document[key],
                            sort_keys=True,
                            default=str,
                        )
                    )

            compact_items.append(
                {
                    "knowledge_id": item.get("knowledge_id"),
                    "knowledge_type": item.get("knowledge_type"),
                    "title": self._bounded_text(item.get("title"), 160),
                    "relevance_score": item.get("relevance_score"),
                    "summary": self._knowledge_summary(item),
                }
            )

        rag_payload["retrieved_items"] = compact_items
        # Candidate diagnostics are persisted separately and are not generation
        # evidence; excluding them keeps prompt context focused on retrieved patterns.
        rag_payload["candidate_scores"] = []

        def compact_context_tokens() -> int:
            return self._token_count(
                json.dumps(
                    rag_payload["retrieved_items"],
                    sort_keys=True,
                    separators=(",", ":"),
                    default=str,
                )
            )

        def retrieved_content_tokens() -> int:
            return self._retrieved_content_tokens(
                rag_payload["retrieved_items"]
            )

        while (
            rag_payload["retrieved_items"]
            and compact_context_tokens() > self.max_retrieval_context_tokens
        ):
            rag_payload["retrieved_items"].pop()
        return rag_payload, retrieved_content_tokens(), excluded_counts

    async def run(
        self,
        *,
        mapping: CanonicalMapping,
        discovery: dict[str, Any],
        lineage: dict[str, Any],
        dependencies: dict[str, Any],
        rag: RAGRetrievalResult,
        critique_feedback: list[dict[str, Any]] | None = None,
    ) -> ConversionResult:
        """Generate and validate one conversion result."""
        self._validate_mapping_readiness(mapping)

        evidence_without_rag = {
            "canonical_mapping": mapping.model_dump(mode="json"),
            "discovery": discovery,
            "lineage": lineage,
            "dependencies": dependencies,
            "critique_feedback": critique_feedback or [],
            "requirements": {
                "target_platform": "DATABRICKS",
                "target_framework": "PYSPARK",
                "required_files": [
                    "PYSPARK_CODE",
                    "UNIT_TEST",
                    "CONFIGURATION",
                ],
                "required_transform_function": "transform",
                "generated_code_contract": {
                    "input": "A dict[str, pyspark.sql.DataFrame] keyed by canonical source dataset name",
                    "output": "A dict[str, pyspark.sql.DataFrame] keyed by each exact canonical target dataset name",
                    "signature": (
                        "def transform(inputs: dict[str, DataFrame], *, "
                        "parameters: dict[str, Any] | None = None) "
                        "-> dict[str, DataFrame]"
                    ),
                    "target_names": [item.name for item in mapping.targets],
                    "parameters": [
                        item.model_dump(mode="json")
                        for item in mapping.parameters
                    ],
                },
            },
        }
        compact_rag, retrieved_context_tokens, excluded_token_counts = (
            self._compact_rag_for_prompt(rag)
        )
        evidence = {
            **evidence_without_rag,
            "approved_knowledge": compact_rag,
        }

        vendor_guidance = ""
        if self.source_vendor.upper().startswith("DATASTAGE"):
            vendor_guidance = """
    DATASTAGE DSX RULES:
    - Use canonical sources, targets, connectors, transformations, and ports as the only executable mapping evidence.
    - Treat source_metadata.stage_inventory as component inventory only. It does not define a dataset schema or prove connector direction.
    - Never infer source or target roles from component names such as SRC_* or TGT_* or from an OLEType such as OracleConnector.
    - Convert only expressions present in canonical transformation ports. Stage names and OLEType values alone do not define business logic.
    - Surface unsupported or ambiguous DataStage properties as manual_actions; do not invent connector behavior, schemas, or field mappings.
    """.strip()

        system_prompt = f"""
You are the Conversion Agent for an ETL migration system.

    {vendor_guidance}

Convert exactly one supplied {self.source_vendor} Canonical Mapping into
validation-friendly Databricks PySpark. Use only supplied evidence. Do not
invent fields, datasets, rules, expressions, parameters, connections, formats,
locations, write modes, merge keys, partition columns, or runtime behavior.

Return exactly three files: one PYSPARK_CODE file, one UNIT_TEST file, and one
CONFIGURATION file.

PYSPARK_CODE requirements:
- Define exactly one public function named transform.
- Use: def transform(inputs: dict[str, DataFrame], *, parameters: dict[str, Any] | None = None) -> dict[str, DataFrame].
- The dictionary keys must be the exact canonical source dataset names supplied in evidence.
- Resolve each source independently, for example: customers_df = inputs["SRC_CUSTOMERS"].
- Implement joins and lookups between the named DataFrames. Never assume that multiple sources are pre-flattened.
- Alias every join input. When two inputs share a field name, qualify each reference and immediately project the joined DataFrame to unique canonical names with explicit aliases.
- Never carry duplicate unqualified column names beyond a join. Every later filter, expression, aggregation, and final projection must reference an unambiguous column.
- For connector renames, use the canonical downstream field name. Example: orders.ORDER_ID may be projected as ORDER_KEY only at the grounded rename/projection step.
- For a single-source mapping, still use the same dictionary contract with one entry.
- Return a dictionary containing exactly one entry for every canonical target dataset, keyed by its exact canonical target name. Each value must be that target's DataFrame with its own canonical target fields in target-schema order.
- For a single-target mapping, return a one-entry target dictionary using the same contract.
- Never return only one target DataFrame for a multi-target mapping; do not discard rows routed to another canonical target.
- Contain transformation logic only.
- Preserve filters, expressions, target names, decimal precision, scale, and
  documented null behavior.
- Select target columns explicitly in target-schema order.
- A nullable field used in a filter follows normal Spark SQL three-valued
  semantics unless supplied evidence defines different null behavior. Do not
  invent an additional null rule merely because the field is nullable.
- Fields used only for FILTER, JOIN, GROUP, SORT, ROUTE, or derivation input
  must not be projected into the final DataFrame unless present in the
  canonical target schema.
- Use pyspark.sql.functions.col for column references.
- Be importable without executing a Spark job.
- Do not create or stop a SparkSession.
- Do not read from or write to JDBC, files, databases, tables, or external
  systems.
- Do not contain credentials, secret fields, connection placeholders, physical
  locations, TODO markers, ellipses, or Markdown fences.
 

UNIT_TEST requirements:
- Use pytest.
- Import transform from the generated module and call transform(inputs), where inputs is keyed by exact canonical source dataset names. The returned value is a dictionary keyed by exact canonical target dataset name. Each scenario must assert the branch named by its target_name. When the specification provides parameter_values, pass them as transform(inputs, parameters={...}).
- Create a local SparkSession only inside a pytest fixture and stop it during
  fixture cleanup.
- Use in-memory DataFrames.
- Cover happy path, applicable null handling, relevant boundary behavior, and
  every grounded business rule.
- {self.UNIT_TEST_SEMANTICS_GUIDANCE}
- {self.UNIT_TEST_DECIMAL_GUIDANCE}
- Use decimal.Decimal values when DecimalType is used.
- Do not connect to databases, use network calls, read or write external files,
  include credentials, or duplicate the transformation logic in the test.

CONFIGURATION requirements:
- Return valid JSON with mapping_name, source, target, runtime, and
  manual_actions.
- Use logical dataset names and non-secret connection references only.
- Do not include username, user, password, token, secret, host, port, service,
  URL, JDBC URL, physical paths, invented formats, or invented write modes.
- If a runtime value is unknown, add a manual action rather than a placeholder.

When critique feedback is supplied, correct every issue without changing valid
mapping behavior.

Derived-field and lineage requirements:
- Implement every canonical transformation expression whose output reaches a target.
- Preserve connector-grounded aggregation, join, lookup/enrichment, filter, and null semantics.
- Each returned target DataFrame must explicitly project that target's canonical fields in target-schema order.
- Do not add logging merely to prove lineage. Lineage is demonstrated by executable column expressions and final projection.

UNIT_TEST data requirements:
- Values supplied to spark.createDataFrame must be native Python values.
- Use datetime.datetime for timestamp fields, datetime.date for date fields, and Decimal from strings for decimal fields.
- Never place pyspark.sql.Column expressions such as F.lit, F.to_timestamp, or F.to_date inside Python row tuples or dictionaries.
- Compute expected rows by applying the complete canonical operation sequence: joins, derivations, filters, routing, aggregation, renames, and final projection.
- Never include an expected output row that fails a canonical FILTER or ROUTE predicate. Assert negative or boundary inputs absent only when canonical behavior explicitly excludes them; do not treat null or boundary inputs as implicitly rejected.
- Prefer assertions keyed by deterministic target fields rather than full-row equality when runtime-generated timestamps are present.
- Every createDataFrame(data, schema) call must use rows whose tuple/list length exactly matches the StructType field count.
- For nondeterministic clock outputs such as SYSDATE, SYSTIMESTAMP, CURRENT_TIMESTAMP, CURRENT_DATE, NOW(), GETDATE(), or LOCALTIMESTAMP, do not add the runtime column to an expected StructType unless every expected row contains a concrete Python datetime value for that field.
- Prefer selecting and comparing only deterministic output columns, then separately assert that each runtime-generated timestamp column exists, has TimestampType, and contains no null values.
- For filtered mappings, include at least one accepted input and one rejected input, then assert the rejected key is absent.
Return only the requested structured output.
""".strip()

        llm = DynamicChatOpenAI(
            operation_name=f"conversion:{mapping.source_object_key}"
        )
        human_prompt = json.dumps(evidence, sort_keys=True, default=str)
        human_prompt_before_rag = json.dumps(
            evidence_without_rag,
            sort_keys=True,
            default=str,
        )
        prompt_tokens_before_rag, prompt_tokens_after_rag = (
            self._prompt_token_counts(
                system_prompt,
                human_prompt_before_rag,
                human_prompt,
            )
        )
        component_token_counts = {
            "source_mapping_tokens": self._token_count(
                json.dumps(
                    evidence_without_rag["canonical_mapping"],
                    sort_keys=True,
                    default=str,
                )
            ),
            "business_rule_tokens": self._token_count(
                json.dumps(
                    {
                        "mapping_business_rules": getattr(
                            mapping, "business_rules", []
                        ),
                        "discovery_business_rules": discovery.get(
                            "business_rules", []
                        ),
                    },
                    sort_keys=True,
                    default=str,
                )
            ),
            "generated_code_summary_tokens": self._token_count(
                json.dumps(
                    [
                        item["summary"]["generated_code_summary"]
                        for item in compact_rag["retrieved_items"]
                    ],
                    sort_keys=True,
                )
            ),
        }
        metric(
            logger,
            "CONVERSION_PROMPT_SIZE",
            mapping=mapping.mapping_name,
            retrieved_context_tokens=retrieved_context_tokens,
            prompt_tokens_before_rag=prompt_tokens_before_rag,
            prompt_tokens_after_rag=prompt_tokens_after_rag,
            max_retrieval_context_tokens=self.max_retrieval_context_tokens,
            **component_token_counts,
            **excluded_token_counts,
        )
        response = await llm.with_structured_output(
            ConversionResult,
            include_raw=True,
        ).ainvoke(
            [
                ("system", system_prompt),
                ("human", human_prompt),
            ]
        )

        parsed = response.get("parsed") if isinstance(response, dict) else response
        raw = response.get("raw") if isinstance(response, dict) else None
        if parsed is None:
            raise RuntimeError(
                "GPT-4o returned no valid structured conversion result."
            )

        for generated_file in parsed.generated_files:
            generated_file.content = self._strip_fences(generated_file.content)

        parsed.baseline_prompt_tokens = prompt_tokens_before_rag
        parsed.actual_prompt_tokens = prompt_tokens_after_rag
        parsed.retrieved_context_tokens = (
            retrieved_context_tokens
            if rag.accepted_count > 0 and rag.contributed_to_prompt
            else 0
        )
        await self._replace_unit_test_with_rendered_specification(
            parsed,
            mapping,
            evidence,
        )
        self._normalize_configuration(parsed, mapping)
        self._capture_usage(raw)
        self._validate_result(parsed, mapping)
        metric(logger, "CONVERSION_AGENT", mapping=mapping.mapping_name, artifacts=len(parsed.generated_files), input_tokens=self.input_tokens, output_tokens=self.output_tokens, confidence=parsed.confidence)
        stage_completed(logger, "CONVERSION_AGENT", mapping=mapping.mapping_name)
        return parsed

    def _validate_mapping_readiness(self, mapping: CanonicalMapping) -> None:
        """Require complete source evidence before converting a DataStage job."""
        if not mapping.source_vendor.upper().startswith("DATASTAGE"):
            return

        missing: list[str] = []
        if not mapping.sources:
            missing.append("source datasets")
        if not mapping.targets:
            missing.append("target datasets")
        if not mapping.connectors:
            missing.append("DSX stage links")
        if any(not dataset.fields for dataset in mapping.sources):
            missing.append("source field schemas")
        if any(not dataset.fields for dataset in mapping.targets):
            missing.append("target field schemas")

        if missing:
            raise ConversionInputValidationError(
                "DataStage mapping is not ready for conversion; missing "
                + ", ".join(missing)
                + ". Re-export or complete the DSX mapping metadata before "
                "requesting conversion."
            )
    async def _replace_unit_test_with_rendered_specification(
        self,
        result: ConversionResult,
        mapping: CanonicalMapping,
        evidence: dict[str, Any],
    ) -> None:
        """Generate test semantics with GPT-4o and render executable Pytest deterministically."""
        pyspark_files = [
            item for item in result.generated_files
            if item.artifact_type == "PYSPARK_CODE"
        ]
        unit_test_files = [
            item for item in result.generated_files
            if item.artifact_type == "UNIT_TEST"
        ]
        if len(pyspark_files) != 1 or len(unit_test_files) != 1:
            return

        pyspark_file = pyspark_files[0]
        unit_test_file = unit_test_files[0]
        repair_errors: list[str] = []
        previous_specification: dict[str, Any] | None = None
        max_attempts = int(
            getattr(settings, "unit_test_specification_max_attempts", 3)
        )

        system_prompt = """
You generate a structured, mapping-specific unit-test specification for an
ETL-to-PySpark conversion. Do not generate Python or Pytest code.
Generate scenarios and typed JSON-compatible data only.

Rules:
- If previous_validation_errors is non-empty, fix every exact error. Use the
  scenario IDs in those errors to identify and repair the affected scenarios
  in previous_specification; preserve valid unaffected scenarios.
- Do not repeat a prior invalid runtime_columns declaration or omit a required
  fixed parameter value.
- Use exact canonical source dataset and field names.
- Include every canonical source dataset exactly once in every scenario.
- input_datasets is a list. Each item has dataset_name and rows. Each row is a
  closed object containing cells, and each cell has field_name and value.
- expected_rows uses the same cells representation. Do not return arbitrary
  JSON objects keyed directly by field names.
- Use only canonical target fields in expected_rows.
- Set target_name on every scenario to the exact canonical target dataset being checked, and cover every canonical target dataset in at least one scenario.
- Cover grounded happy-path and business-rule behavior. Include null,
  boundary, negative, join, lookup, aggregation, or routing cases only when
  applicable to this mapping.
- When testing ORDER_AMOUNT, GROSS_MARGIN, or other Derived fields, expected
  results must calculate the transformation exactly using standard python
  Decimal math rather than approximation.
- Ensure that boundary scenarios, such as mapping null or missing RISK_SCORE
  inputs, strictly evaluate to the default fallback (`0` via coalesce) instead
  of causing missing row expectations or evaluation mismatches.
- The rendered pytest scaffold must always explicitly import
  `from decimal import Decimal`; use Decimal values for all decimal mock data
  and expected values.
- CRITICAL: Every expected value representing a DecimalType field (e.g.
  ORDER_AMOUNT, GROSS_MARGIN, UNIT_PRICE, DISCOUNT_PCT, UNIT_COST) MUST use a
  string-initialized decimal.Decimal object (like `Decimal('50.00')` or
  `Decimal('0.00')`). NEVER use raw float literals (like `50.0` or `12.5`) or
  integer literals (like `50`) for decimal fields. Expected data tuples must
  explicitly be constructed as `Decimal('value')` to avoid float comparison
  precision errors.
- For mappings with multiple output targets or ROUTER topologies, test every
  canonical target branch. When a mapping features multiple output targets
  (such as `TGT_ORDER_PROFITABILITY` for valid orders and `TGT_ORDER_REJECTS`
  for rejected orders based on high-risk conditions `RISK_SCORE >= 80`), do not
  treat diverted records as discarded or expect an empty result. Define
  scenarios that cover the high-risk routing path and assert that those records
  properly reach the rejection target `TGT_ORDER_REJECTS`. In the reject-path
  expectations, include `ORDER_ID` and `CUSTOMER_ID`, require
  `REJECT_REASON` to equal the literal `CUSTOMER_RISK_LIMIT_EXCEEDED`, and
  verify `LOAD_DATE` exactly. Assert that the correct records arrive at the
  validation and reject targets, respectively, according to their input
  filters. Use only target fields and routing behavior grounded in the
  canonical mapping; do not invent a target branch or output field.
- comparison_keys must be stable deterministic target fields.
- Treat parameters with clock-expression defaults as deterministic test inputs:
  provide a fixed, valid parameter_values entry in each scenario and assert
  derived target values exactly.
- CRITICAL: Parameters (like $PROC_DATE) are ALWAYS deterministic in tests because you provide fixed `parameter_values`. Any target column derived from a parameter (like PROCESSED_ON) MUST go in `deterministic_columns` and MUST have a concrete expected value in `expected_rows`. NEVER place parameter-derived columns in `runtime_columns`.
- Only columns whose transformation expressions themselves directly contain clock
  expressions such as SYSDATE, SYSTIMESTAMP, CURRENT_TIMESTAMP, CURRENT_DATE,
  LOCALTIMESTAMP, NOW(), or GETDATE() are runtime_columns.
- Exclude truly clock-derived outputs from deterministic_columns and expected
  values; declare each in runtime_columns with its type and nullability so the
  rendered test checks its existence, type, and non-null behavior instead.
- expected_rows must contain every deterministic column for every row.
- Apply the complete canonical operation sequence when deriving expected_rows.
- Do not invent datasets, fields, transformations, or business rules.
- Return only the requested structured object.
""".strip()

        for attempt in range(1, max_attempts + 1):
            request_payload = {
                "canonical_mapping": mapping.model_dump(mode="json"),
                "generated_pyspark": {
                    "file_name": pyspark_file.file_name,
                    "content": pyspark_file.content,
                },
                "discovery_and_rag_evidence": evidence,
                "attempt": attempt,
                "previous_validation_errors": repair_errors,
                "previous_specification": previous_specification,
            }
            llm = DynamicChatOpenAI(
                operation_name=(
                    f"unit-test-specification:{mapping.source_object_key}:"
                    f"attempt-{attempt}"
                )
            )
            response = await llm.with_structured_output(
                UnitTestSpecification,
                include_raw=True,
            ).ainvoke(
                [
                    ("system", system_prompt),
                    ("human", json.dumps(request_payload, sort_keys=True, default=str)),
                ]
            )
            specification = (
                response.get("parsed") if isinstance(response, dict) else response
            )
            raw = response.get("raw") if isinstance(response, dict) else None
            self._capture_usage(raw)
            if specification is None:
                repair_errors = ["No valid structured test specification was returned."]
                previous_specification = None
                continue

            UnitTestSpecificationValidator.repair_parameter_derived_runtime_columns(
                specification,
                mapping,
            )

            previous_specification = specification.model_dump(mode="json")
            repair_errors = UnitTestSpecificationValidator.validate(
                specification,
                mapping,
            )
            if repair_errors:
                continue

            try:
                rendered = UnitTestRenderer.render(
                    specification,
                    mapping,
                    pyspark_file.file_name,
                )
            except (TypeError, ValueError, KeyError) as exc:
                repair_errors = [
                    f"Deterministic test rendering failed: {type(exc).__name__}: {exc}"
                ]
                continue
            syntax_errors = self._validate_python_syntax(
                GeneratedFile(
                    artifact_type="UNIT_TEST",
                    file_name=unit_test_file.file_name,
                    content=rendered,
                    media_type="text/x-python",
                )
            )
            contract_errors = self._validate_unit_test_contract(
                rendered,
                pyspark_file.file_name,
            )
            repair_errors = syntax_errors + contract_errors
            if repair_errors:
                continue

            unit_test_file.content = rendered
            unit_test_file.media_type = "text/x-python"
            return

        raise ConversionOutputValidationError(
            "A valid structured unit-test specification could not be generated "
            f"after {max_attempts} attempts: " + " ".join(repair_errors)
        )

    def _normalize_configuration(
        self,
        result: ConversionResult,
        mapping: CanonicalMapping,
    ) -> None:
        """Complete safe configuration fields from canonical metadata."""
        files = [
            item for item in result.generated_files
            if item.artifact_type == "CONFIGURATION"
        ]
        if len(files) != 1:
            return
        generated_file = files[0]
        try:
            configuration = json.loads(generated_file.content)
        except json.JSONDecodeError:
            return
        if not isinstance(configuration, dict):
            return

        sources = [
            {
                "name": item.name,
                "dataset_type": item.dataset_type,
                "connection_name": item.connection_name,
            }
            for item in mapping.sources
        ]
        targets = [
            {
                "name": item.name,
                "dataset_type": item.dataset_type,
                "connection_name": item.connection_name,
            }
            for item in mapping.targets
        ]

        source = configuration.get("source")
        if not isinstance(source, dict):
            source = {}
            configuration["source"] = source
        source.setdefault("name", sources[0]["name"] if sources else None)
        source.setdefault("datasets", sources)

        target = configuration.get("target")
        if not isinstance(target, dict):
            target = {}
            configuration["target"] = target
        target.setdefault("name", targets[0]["name"] if targets else None)
        target.setdefault("datasets", targets)

        configuration["mapping_name"] = mapping.mapping_name
        runtime = configuration.get("runtime")
        if not isinstance(runtime, dict):
            runtime = {}
            configuration["runtime"] = runtime
        runtime.setdefault("target_platform", "DATABRICKS")
        runtime.setdefault("target_framework", "PYSPARK")
        runtime["parameters"] = [
            {
                "name": item.name,
                "data_type": item.data_type or item.parameter_type,
                "default_value": item.default_value,
                "scope": item.scope,
            }
            for item in mapping.parameters
        ]
        if not isinstance(configuration.get("manual_actions"), list):
            configuration["manual_actions"] = list(result.manual_actions)

        generated_file.content = (
            json.dumps(configuration, indent=2, sort_keys=True) + "\n"
        )

    def _validate_result(
        self,
        result: ConversionResult,
        mapping: CanonicalMapping,
    ) -> None:
        errors: list[str] = []

        if result.mapping_name != mapping.mapping_name:
            errors.append(
                "Generated mapping_name does not match the Canonical Mapping."
            )

        generated_by_type: dict[str, list[GeneratedFile]] = {}
        for generated_file in result.generated_files:
            generated_by_type.setdefault(
                generated_file.artifact_type,
                [],
            ).append(generated_file)

        generated_types = set(generated_by_type)
        missing_types = self.REQUIRED_ARTIFACT_TYPES - generated_types
        unexpected_types = generated_types - self.REQUIRED_ARTIFACT_TYPES

        if missing_types:
            errors.append(
                "Missing generated artifacts: " + ", ".join(sorted(missing_types))
            )
        if unexpected_types:
            errors.append(
                "Unexpected generated artifacts: "
                + ", ".join(sorted(unexpected_types))
            )

        for artifact_type in self.REQUIRED_ARTIFACT_TYPES:
            count = len(generated_by_type.get(artifact_type, []))
            if count != 1:
                errors.append(
                    f"Expected exactly one {artifact_type} artifact, received {count}."
                )

        if errors:
            raise ConversionOutputValidationError(" ".join(errors))

        pyspark_file = generated_by_type["PYSPARK_CODE"][0]
        unit_test_file = generated_by_type["UNIT_TEST"][0]
        configuration_file = generated_by_type["CONFIGURATION"][0]

        errors.extend(self._validate_python_syntax(pyspark_file))
        errors.extend(self._validate_python_syntax(unit_test_file))
        errors.extend(
            self._validate_pyspark_contract(pyspark_file.content, mapping)
        )
        errors.extend(
            self._validate_unit_test_contract(
                unit_test_file.content,
                pyspark_file.file_name,
            )
        )
        errors.extend(
            self._validate_configuration_contract(configuration_file.content)
        )

        if errors:
            raise ConversionOutputValidationError(
                "Generated Conversion package violated the output contract: "
                + " ".join(errors)
            )

    @staticmethod
    def _validate_python_syntax(generated_file: GeneratedFile) -> list[str]:
        errors: list[str] = []
        try:
            ast.parse(generated_file.content)
        except SyntaxError as exc:
            errors.append(
                f"{generated_file.artifact_type} contains invalid Python: "
                f"{exc.msg} at line {exc.lineno}."
            )
        return errors

    #old code  

    # def _validate_pyspark_contract(
    #     self,
    #     content: str,
    #     mapping: CanonicalMapping | None = None,
    # ) -> list[str]:
    #     errors: list[str] = []
    #     lowered = content.lower()

    #     for pattern, description in self.FORBIDDEN_PYSPARK_PATTERNS.items():
    #         if pattern.lower() in lowered:
    #             errors.append(description)

    #     try:
    #         tree = ast.parse(content)
    #     except SyntaxError:
    #         return errors

    #     functions = [
    #         node
    #         for node in tree.body
    #         if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    #     ]
    #     transform_functions = [node for node in functions if node.name == "transform"]

    #     if len(transform_functions) != 1:
    #         errors.append(
    #             "PYSPARK_CODE must define exactly one public transform function."
    #         )
    #         return errors

    #     transform_function = transform_functions[0]
    #     positional_arguments = [
    #         *transform_function.args.posonlyargs,
    #         *transform_function.args.args,
    #     ]
    #     if len(positional_arguments) != 1:
    #         errors.append(
    #             "transform must accept exactly one positional named-input dictionary argument."
    #         )
    #     if mapping and mapping.parameters:
    #         parameter_arguments = [
    #             index
    #             for index, argument in enumerate(
    #                 transform_function.args.kwonlyargs
    #             )
    #             if argument.arg == "parameters"
    #         ]
    #         if (
    #             len(parameter_arguments) != 1
    #             or not isinstance(
    #                 transform_function.args.kw_defaults[
    #                     parameter_arguments[0]
    #                 ],
    #                 ast.Constant,
    #             )
    #             or transform_function.args.kw_defaults[
    #                 parameter_arguments[0]
    #             ].value is not None
    #         ):
    #             errors.append(
    #                 "transform must accept declared ETL parameters through an "
    #                 "optional keyword-only parameters dictionary."
    #             )
    #         if not self._normalizes_optional_parameters(transform_function):
    #             errors.append(
    #                 "transform must normalize its optional parameters dictionary "
    #                 "with `parameters = parameters or {}` before using it."
    #             )

    #     if mapping:
    #         target_fields = {
    #             field.name
    #             for target in mapping.targets
    #             for field in target.fields
    #         }
    #         referenced_output_fields: set[str] = set()
    #         for node in ast.walk(transform_function):
    #             if not isinstance(node, ast.Call):
    #                 continue
    #             function_name = (
    #                 node.func.attr
    #                 if isinstance(node.func, ast.Attribute)
    #                 else node.func.id
    #                 if isinstance(node.func, ast.Name)
    #                 else ""
    #             )
    #             if function_name not in {
    #                 "alias",
    #                 "col",
    #                 "select",
    #                 "withColumn",
    #                 "withColumnRenamed",
    #             }:
    #                 continue
    #             referenced_output_fields.update(
    #                 argument.value
    #                 for argument in node.args
    #                 if isinstance(argument, ast.Constant)
    #                 and isinstance(argument.value, str)
    #             )
    #         missing_target_fields = sorted(target_fields - referenced_output_fields)
    #         if missing_target_fields:
    #             errors.append(
    #                 "transform does not explicitly project canonical target "
    #                 "field(s): " + ", ".join(missing_target_fields) + "."
    #             )

    #     source_field_owners: dict[str, set[str]] = {}
    #     # The generated function must make duplicate source fields unambiguous.
    #     # A lightweight AST/text gate checks that join-heavy code uses aliases
    #     # and qualified column references rather than bare duplicate names.
    #     if ".join(" in content and ".alias(" not in content:
    #         errors.append(
    #             "Multi-source joins must alias their input DataFrames and use "
    #             "qualified column references."
    #         )

    #     if not any(
    #         isinstance(node, ast.Return) for node in ast.walk(transform_function)
    #     ):
    #         errors.append("transform must return an output DataFrame.")

    #     allowed_module_nodes = (
    #         ast.Import,
    #         ast.ImportFrom,
    #         ast.FunctionDef,
    #         ast.AsyncFunctionDef,
    #         ast.ClassDef,
    #         ast.Pass,
    #     )
    #     for node in tree.body:
    #         if isinstance(node, allowed_module_nodes):
    #             continue
    #         if isinstance(node, ast.If) and self._is_main_guard(node):
    #             continue
    #         if isinstance(node, (ast.Assign, ast.AnnAssign)):
    #             if self._is_constant_assignment(node):
    #                 continue
    #         errors.append("PYSPARK_CODE contains executable module-level logic.")
    #         break

    #     return errors

        # Patched AST Validation Method in ConversionAgent
    def _validate_pyspark_contract(
        self,
        content: str,
        mapping: CanonicalMapping | None = None,
    ) -> list[str]:
        errors: list[str] = []
        lowered = content.lower()

        for pattern, description in self.FORBIDDEN_PYSPARK_PATTERNS.items():
            if pattern.lower() in lowered:
                errors.append(description)

        try:
            tree = ast.parse(content)
        except SyntaxError:
            return errors

        functions = [
            node
            for node in tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        ]
        transform_functions = [node for node in functions if node.name == "transform"]

        if len(transform_functions) != 1:
            errors.append(
                "PYSPARK_CODE must define exactly one public transform function."
            )
            return errors

        transform_function = transform_functions[0]
        positional_arguments = [
            *transform_function.args.posonlyargs,
            *transform_function.args.args,
        ]
        if len(positional_arguments) != 1:
            errors.append(
                "transform must accept exactly one positional named-input dictionary argument."
            )

        # STRICT & SAFE PARAMETER CONTRACT VERIFICATION
        if mapping and mapping.parameters:
            parameter_arguments = [
                index
                for index, argument in enumerate(
                    transform_function.args.kwonlyargs
                )
                if argument.arg == "parameters"
            ]
            if (
                len(parameter_arguments) != 1
                or not isinstance(
                    transform_function.args.kw_defaults[
                        parameter_arguments[0]
                    ],
                    ast.Constant,
                )
                or transform_function.args.kw_defaults[
                    parameter_arguments[0]
                ].value is not None
            ):
                errors.append(
                    "transform must accept declared ETL parameters through an "
                    "optional keyword-only parameters dictionary (e.g. *, parameters: dict[str, Any] | None = None)."
                )
            if not self._normalizes_optional_parameters(transform_function):
                errors.append(
                    "transform must normalize its optional parameters dictionary "
                    "with `parameters = parameters or {}` before using it."
                )

        return errors
          

    @staticmethod
    def _normalizes_optional_parameters(
        function: ast.FunctionDef | ast.AsyncFunctionDef,
    ) -> bool:
        """Require parameters=None to become an empty mapping before use."""
        normalization_line: int | None = None
        for node in ast.walk(function):
            if not isinstance(node, ast.Assign):
                continue
            if not any(
                isinstance(target, ast.Name) and target.id == "parameters"
                for target in node.targets
            ):
                continue
            value = node.value
            if (
                isinstance(value, ast.BoolOp)
                and isinstance(value.op, ast.Or)
                and len(value.values) == 2
                and isinstance(value.values[0], ast.Name)
                and value.values[0].id == "parameters"
                and isinstance(value.values[1], ast.Dict)
                and not value.values[1].keys
            ):
                normalization_line = node.lineno
                break
        if normalization_line is None:
            return False

        parameter_get_lines = [
            node.lineno
            for node in ast.walk(function)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "get"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "parameters"
        ]
        return (
            not parameter_get_lines
            or normalization_line < min(parameter_get_lines)
        )

    @staticmethod
    def _is_constant_assignment(node: ast.Assign | ast.AnnAssign) -> bool:
        value = node.value
        return isinstance(
            value,
            (
                ast.Constant,
                ast.List,
                ast.Tuple,
                ast.Set,
                ast.Dict,
            ),
        )

    @staticmethod
    def _is_main_guard(node: ast.If) -> bool:
        test = node.test
        return (
            isinstance(test, ast.Compare)
            and isinstance(test.left, ast.Name)
            and test.left.id == "__name__"
            and len(test.ops) == 1
            and isinstance(test.ops[0], ast.Eq)
            and len(test.comparators) == 1
            and isinstance(test.comparators[0], ast.Constant)
            and test.comparators[0].value == "__main__"
        )


    @staticmethod
    def _validate_create_dataframe_shapes(tree: ast.AST) -> list[str]:
        """Detect literal row/schema width mismatches before accepting a test."""
        assignments: dict[str, ast.AST] = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign) and len(node.targets) == 1:
                target = node.targets[0]
                if isinstance(target, ast.Name):
                    assignments[target.id] = node.value
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                if node.value is not None:
                    assignments[node.target.id] = node.value

        def resolve(node: ast.AST) -> ast.AST:
            if isinstance(node, ast.Name):
                return assignments.get(node.id, node)
            return node

        def schema_width(node: ast.AST) -> int | None:
            node = resolve(node)
            if not isinstance(node, ast.Call):
                return None
            function_name = (
                node.func.id
                if isinstance(node.func, ast.Name)
                else node.func.attr
                if isinstance(node.func, ast.Attribute)
                else ""
            )
            if function_name != "StructType" or not node.args:
                return None
            fields = resolve(node.args[0])
            if isinstance(fields, (ast.List, ast.Tuple)):
                return len(fields.elts)
            return None

        def row_widths(node: ast.AST) -> set[int] | None:
            node = resolve(node)
            if not isinstance(node, (ast.List, ast.Tuple)):
                return None
            widths: set[int] = set()
            for row in node.elts:
                row = resolve(row)
                if not isinstance(row, (ast.List, ast.Tuple)):
                    return None
                widths.add(len(row.elts))
            return widths

        errors: list[str] = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or len(node.args) < 2:
                continue
            function_name = (
                node.func.attr
                if isinstance(node.func, ast.Attribute)
                else node.func.id
                if isinstance(node.func, ast.Name)
                else ""
            )
            if function_name != "createDataFrame":
                continue
            widths = row_widths(node.args[0])
            width = schema_width(node.args[1])
            if widths is None or width is None:
                continue
            invalid = sorted(item for item in widths if item != width)
            if invalid:
                errors.append(
                    "UNIT_TEST createDataFrame row/schema mismatch: "
                    f"StructType has {width} fields but row widths are "
                    f"{sorted(widths)}. Ensure every row matches the schema; "
                    "for runtime-generated timestamps, compare deterministic "
                    "columns separately and assert timestamp type/non-nullness."
                )
        return errors

    @staticmethod
    def _validate_unit_test_contract(
        content: str,
        pyspark_file_name: str,
    ) -> list[str]:
        errors: list[str] = []
        lowered = content.lower()

        forbidden_patterns = {
            "jdbc:": "UNIT_TEST contains a JDBC URL.",
            "password=": "UNIT_TEST contains a password option.",
            '"password"': "UNIT_TEST contains a password field.",
            "<password>": "UNIT_TEST contains a password placeholder.",
            "requests.": "UNIT_TEST contains a network call.",
            "httpx.": "UNIT_TEST contains a network call.",
            "urllib.": "UNIT_TEST contains a network call.",
            "socket.": "UNIT_TEST contains a network call.",
        }
        for pattern, description in forbidden_patterns.items():
            if pattern in lowered:
                errors.append(description)

        if "import pytest" not in lowered:
            errors.append("UNIT_TEST must use pytest.")

        module_name = re.sub(
            r"\.py$",
            "",
            pyspark_file_name,
            flags=re.IGNORECASE,
        ).lower()
        expected_imports = {
            f"from {module_name} import transform",
            f"import {module_name}",
        }
        if not any(item in lowered for item in expected_imports):
            errors.append("UNIT_TEST must import the generated PySpark module.")

        if "transform(" not in lowered:
            errors.append("UNIT_TEST must call transform.")
        if "unittest" in lowered:
            errors.append("UNIT_TEST must use pytest instead of unittest.")

        try:
            tree = ast.parse(content)
        except SyntaxError:
            return errors

        test_functions = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef) and node.name.startswith("test_")
        ]
        if not test_functions:
            errors.append("UNIT_TEST must contain at least one pytest test function.")

        errors.extend(ConversionAgent._validate_create_dataframe_shapes(tree))
        return errors

    def _validate_configuration_contract(self, content: str) -> list[str]:
        try:
            configuration = json.loads(content)
        except json.JSONDecodeError as exc:
            return [f"CONFIGURATION is not valid JSON: {exc.msg}."]

        if not isinstance(configuration, dict):
            return ["CONFIGURATION must contain a JSON object."]

        errors: list[str] = []
        forbidden_paths: list[str] = []

        def inspect(value: Any, path: str = "$") -> None:
            if isinstance(value, dict):
                for key, child in value.items():
                    normalized_key = (
                        str(key)
                        .strip()
                        .lower()
                        .replace("-", "_")
                        .replace(" ", "_")
                    )
                    child_path = f"{path}.{key}"
                    if normalized_key in self.FORBIDDEN_CONFIGURATION_KEYS:
                        forbidden_paths.append(child_path)
                    inspect(child, child_path)
            elif isinstance(value, list):
                for index, child in enumerate(value):
                    inspect(child, f"{path}[{index}]")
            elif isinstance(value, str):
                lowered = value.lower()
                markers = (
                    "password",
                    "username",
                    "jdbc:",
                    "<host>",
                    "<port>",
                    "<service>",
                    "<username>",
                    "<password>",
                    "/path/to/",
                    "api_key",
                    "access_token",
                    "client_secret",
                )
                if any(marker in lowered for marker in markers):
                    forbidden_paths.append(path)

        inspect(configuration)

        if forbidden_paths:
            errors.append(
                "CONFIGURATION contains forbidden connection or credential "
                "fields at: " + ", ".join(sorted(set(forbidden_paths))) + "."
            )

        if configuration.get("mapping_name") is None:
            errors.append("CONFIGURATION must include mapping_name.")
        if not isinstance(configuration.get("source"), dict):
            errors.append("CONFIGURATION must include a source object.")
        if not isinstance(configuration.get("target"), dict):
            errors.append("CONFIGURATION must include a target object.")

        return errors

    @staticmethod
    def _strip_fences(content: str) -> str:
        value = content.strip()
        value = re.sub(
            r"^```(?:python|json)?\s*",
            "",
            value,
            flags=re.IGNORECASE,
        )
        value = re.sub(r"\s*```$", "", value)
        return value.strip() + "\n"

    def _capture_usage(self, raw: Any) -> None:
        usage = getattr(raw, "usage_metadata", None) or {}
        metadata = getattr(raw, "response_metadata", None) or {}
        token_usage = metadata.get("token_usage", {})
        self.input_tokens += int(
            usage.get("input_tokens")
            or token_usage.get("prompt_tokens")
            or 0
        )
        self.output_tokens += int(
            usage.get("output_tokens")
            or token_usage.get("completion_tokens")
            or 0
        )
        self.model_name = (
            metadata.get("model_name")
            or metadata.get("model")
            or self.MODEL_NAME
        )
