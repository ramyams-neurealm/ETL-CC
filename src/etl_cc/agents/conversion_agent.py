"""GPT-4o conversion agent for Informatica-to-Databricks PySpark migration."""

import ast
import json
import re
from typing import Any, Literal

from pydantic import BaseModel, Field

from etl_cc.agents.rag_retrieval_agent import RAGRetrievalResult
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


class ConversionOutputValidationError(RuntimeError):
    """Raised when generated artifacts violate the conversion contract."""


logger = configure_logging("CONVERSION_AGENT")


class ConversionAgent:
    """Generate validation-friendly Databricks PySpark artifacts."""

    AGENT_NAME = "CONVERSION_AGENT"
    AGENT_VERSION = "2.4.0"
    STAGE_NAME = "CODE_CONVERSION"
    MODEL_NAME = "gpt-4o"
    PROMPT_NAME = "INFORMATICA_TO_DATABRICKS_PYSPARK"
    PROMPT_VERSION = "2.4.0"

    REQUIRED_ARTIFACT_TYPES = {
        "PYSPARK_CODE",
        "UNIT_TEST",
        "CONFIGURATION",
    }

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

    def __init__(self) -> None:
        self.model_name = self.MODEL_NAME
        self.prompt_name = self.PROMPT_NAME
        self.prompt_version = self.PROMPT_VERSION
        self.input_tokens = 0
        self.output_tokens = 0

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
        stage_started(logger, "CONVERSION_AGENT", mapping=mapping.mapping_name, sources=len(mapping.sources), targets=len(mapping.targets), transformations=len(mapping.transformations))

        evidence = {
            "canonical_mapping": mapping.model_dump(mode="json"),
            "discovery": discovery,
            "lineage": lineage,
            "dependencies": dependencies,
            "approved_knowledge": rag.model_dump(mode="json"),
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
                    "output": "One pyspark.sql.DataFrame",
                    "signature": "def transform(inputs: dict[str, DataFrame]) -> DataFrame",
                },
            },
        }

        system_prompt = """
You are the Conversion Agent for an ETL migration system.

Convert exactly one supplied Informatica Canonical Mapping into
validation-friendly Databricks PySpark. Use only supplied evidence. Do not
invent fields, datasets, rules, expressions, parameters, connections, formats,
locations, write modes, merge keys, partition columns, or runtime behavior.

Return exactly three files: one PYSPARK_CODE file, one UNIT_TEST file, and one
CONFIGURATION file.

PYSPARK_CODE requirements:
- Define exactly one public function named transform.
- Use: def transform(inputs: dict[str, DataFrame]) -> DataFrame.
- The dictionary keys must be the exact canonical source dataset names supplied in evidence.
- Resolve each source independently, for example: customers_df = inputs["SRC_CUSTOMERS"].
- Implement joins and lookups between the named DataFrames. Never assume that multiple sources are pre-flattened.
- Alias every join input. When two inputs share a field name, qualify each reference and immediately project the joined DataFrame to unique canonical names with explicit aliases.
- Never carry duplicate unqualified column names beyond a join. Every later filter, expression, aggregation, and final projection must reference an unambiguous column.
- For connector renames, use the canonical downstream field name. Example: orders.ORDER_ID may be projected as ORDER_KEY only at the grounded rename/projection step.
- For a single-source mapping, still use the same dictionary contract with one entry.
- Accept one dictionary of named source DataFrames and return one output DataFrame.
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
- Import transform from the generated module and call transform(inputs), where inputs is keyed by exact canonical source dataset names.
- Create a local SparkSession only inside a pytest fixture and stop it during
  fixture cleanup.
- Use in-memory DataFrames.
- Cover happy path, applicable null handling, relevant boundary behavior, and
  every grounded business rule.
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
- The final select must explicitly project every canonical target field in target order.
- Do not add logging merely to prove lineage. Lineage is demonstrated by executable column expressions and final projection.

UNIT_TEST data requirements:
- Values supplied to spark.createDataFrame must be native Python values.
- Use datetime.datetime for timestamp fields, datetime.date for date fields, and Decimal from strings for decimal fields.
- Never place pyspark.sql.Column expressions such as F.lit, F.to_timestamp, or F.to_date inside Python row tuples or dictionaries.
- Compute expected rows by applying the complete canonical operation sequence: joins, derivations, filters, routing, aggregation, renames, and final projection.
- Never include an expected output row that fails a canonical FILTER or ROUTE predicate. Negative and boundary inputs must be asserted as absent from output.
- Prefer assertions keyed by deterministic target fields rather than full-row equality when runtime-generated timestamps are present.
- For filtered mappings, include at least one accepted input and one rejected input, then assert the rejected key is absent.
Return only the requested structured output.
""".strip()

        llm = DynamicChatOpenAI(
            operation_name=f"conversion:{mapping.source_object_key}"
        )
        response = await llm.with_structured_output(
            ConversionResult,
            include_raw=True,
        ).ainvoke(
            [
                ("system", system_prompt),
                ("human", json.dumps(evidence, sort_keys=True, default=str)),
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

        self._normalize_configuration(parsed, mapping)
        self._capture_usage(raw)
        self._validate_result(parsed, mapping)
        metric(logger, "CONVERSION_AGENT", mapping=mapping.mapping_name, artifacts=len(parsed.generated_files), input_tokens=self.input_tokens, output_tokens=self.output_tokens, confidence=parsed.confidence)
        stage_completed(logger, "CONVERSION_AGENT", mapping=mapping.mapping_name)
        return parsed

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
        errors.extend(self._validate_pyspark_contract(pyspark_file.content))
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

    def _validate_pyspark_contract(self, content: str) -> list[str]:
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

        source_field_owners: dict[str, set[str]] = {}
        # The generated function must make duplicate source fields unambiguous.
        # A lightweight AST/text gate checks that join-heavy code uses aliases
        # and qualified column references rather than bare duplicate names.
        if ".join(" in content and ".alias(" not in content:
            errors.append(
                "Multi-source joins must alias their input DataFrames and use "
                "qualified column references."
            )

        if not any(
            isinstance(node, ast.Return) for node in ast.walk(transform_function)
        ):
            errors.append("transform must return an output DataFrame.")

        allowed_module_nodes = (
            ast.Import,
            ast.ImportFrom,
            ast.FunctionDef,
            ast.AsyncFunctionDef,
            ast.ClassDef,
            ast.Pass,
        )
        for node in tree.body:
            if isinstance(node, allowed_module_nodes):
                continue
            if isinstance(node, ast.If) and self._is_main_guard(node):
                continue
            if isinstance(node, (ast.Assign, ast.AnnAssign)):
                if self._is_constant_assignment(node):
                    continue
            errors.append("PYSPARK_CODE contains executable module-level logic.")
            break

        return errors

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
        self.input_tokens = int(
            usage.get("input_tokens")
            or token_usage.get("prompt_tokens")
            or 0
        )
        self.output_tokens = int(
            usage.get("output_tokens")
            or token_usage.get("completion_tokens")
            or 0
        )
        self.model_name = (
            metadata.get("model_name")
            or metadata.get("model")
            or self.MODEL_NAME
        )
