"""GPT-4o critique agent plus deterministic conversion checks."""

import ast
import json
from typing import Literal

from pydantic import BaseModel, Field

from etl_cc.agents.conversion_agent import ConversionResult
from etl_cc.key_vault_service import DynamicChatOpenAI
from etl_cc.models import CanonicalMapping


class DeterministicCheck(BaseModel):
    check_name: str
    passed: bool
    details: str


class CritiqueIssue(BaseModel):
    severity: Literal["LOW", "MEDIUM", "HIGH", "CRITICAL"]
    category: str
    description: str
    recommendation: str


class CritiqueResult(BaseModel):
    decision: Literal["PASS", "REVISE"]
    issues: list[CritiqueIssue] = Field(default_factory=list)
    business_rule_coverage: float = Field(ge=0.0, le=1.0)
    lineage_coverage: float = Field(ge=0.0, le=1.0)
    revision_required: bool
    confidence: float = Field(ge=0.0, le=1.0)
    deterministic_checks: list[DeterministicCheck] = Field(default_factory=list)


class CritiqueAgent:
    AGENT_NAME = "CRITIQUE_AGENT"
    AGENT_VERSION = "1.2.0"
    STAGE_NAME = "CONVERSION_CRITIQUE"
    MODEL_NAME = "gpt-4o"
    PROMPT_NAME = "PYSPARK_CONVERSION_CRITIQUE"
    PROMPT_VERSION = "1.2.0"

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
        discovery: dict,
        lineage: dict,
        conversion: ConversionResult,
    ) -> CritiqueResult:
        checks = self.deterministic_checks(mapping, discovery, conversion)
        evidence = {
            "canonical_mapping": mapping.model_dump(mode="json"),
            "discovery": discovery,
            "lineage": lineage,
            "conversion": conversion.model_dump(mode="json"),
            "deterministic_checks": [item.model_dump() for item in checks],
        }
        prompt = """Review generated Databricks PySpark against supplied
Informatica evidence. Never invent requirements. Treat failed deterministic
checks, missing grounded business rules, missing canonical target fields,
syntax errors, secrets, TODOs, or placeholders as requiring revision.

Field-role rules:
- Require final projection only for canonical target fields.
- A source field used only by FILTER, JOIN, GROUP, SORT, ROUTE, or as a
  derivation input is covered when used in that operation. Do not require it
  in the final projection.
- Spark filter predicates already exclude rows where the predicate evaluates
  to null. Do not require an explicit isNotNull check unless supplied canonical
  evidence defines a distinct null-handling rule.
- Do not recommend logging or projecting intermediate fields merely to improve
  lineage coverage.
Return structured critique only."""
        llm = DynamicChatOpenAI(
            operation_name=f"critique:{mapping.source_object_key}"
        )
        response = await llm.with_structured_output(
            CritiqueResult,
            include_raw=True,
        ).ainvoke([
            ("system", prompt),
            ("human", json.dumps(evidence, sort_keys=True, default=str)),
        ])
        parsed = response.get("parsed") if isinstance(response, dict) else response
        raw = response.get("raw") if isinstance(response, dict) else None
        if parsed is None:
            raise RuntimeError("GPT-4o returned no valid structured critique result.")
        parsed.deterministic_checks = checks
        parsed = self._normalize_critique(
            parsed,
            mapping,
            conversion,
            checks,
        )
        self._capture_usage(raw)
        return parsed

    @staticmethod
    def _normalize_critique(
        result: CritiqueResult,
        mapping: CanonicalMapping,
        conversion: ConversionResult,
        checks: list[DeterministicCheck],
    ) -> CritiqueResult:
        """Remove ungrounded critique findings using canonical field roles."""
        files = {
            item.artifact_type: item
            for item in conversion.generated_files
        }
        code = (files.get("PYSPARK_CODE").content if files.get("PYSPARK_CODE") else "")
        upper_code = code.upper()
        target_fields = {
            field.name.upper()
            for target in mapping.targets
            for field in target.fields
        }

        retained: list[CritiqueIssue] = []
        for issue in result.issues:
            text = " ".join((
                issue.category,
                issue.description,
                issue.recommendation,
            )).upper()

            mentioned_fields = {
                field.name.upper()
                for source in mapping.sources
                for field in source.fields
                if field.name.upper() in text
            }
            fields_used_in_code = {
                name for name in mentioned_fields if name in upper_code
            }
            non_target_fields = mentioned_fields - target_fields

            explicit_null_required = any(
                marker in json.dumps(
                    mapping.model_dump(mode="json"),
                    default=str,
                ).upper()
                for marker in (
                    "NULL HANDLING",
                    "IS NULL",
                    "ISNULL(",
                    "COALESCE(",
                )
            )
            null_only_finding = (
                "NULL" in text
                and bool(fields_used_in_code)
                and not explicit_null_required
            )
            projection_only_finding = (
                bool(non_target_fields)
                and bool(non_target_fields & fields_used_in_code)
                and any(term in text for term in (
                    "FINAL OUTPUT",
                    "FINAL PROJECTION",
                    "INCLUDE THIS FIELD",
                    "LOGGING",
                    "LINEAGE COVERAGE",
                ))
            )
            if null_only_finding or projection_only_finding:
                continue
            retained.append(issue)

        result.issues = retained
        failed_checks = [item for item in checks if not item.passed]
        if failed_checks or retained:
            result.decision = "REVISE"
            result.revision_required = True
        else:
            result.decision = "PASS"
            result.revision_required = False
            result.business_rule_coverage = 1.0
            result.lineage_coverage = 1.0
        return result

    @staticmethod
    def deterministic_checks(
        mapping: CanonicalMapping,
        discovery: dict,
        conversion: ConversionResult,
    ) -> list[DeterministicCheck]:
        files = {item.artifact_type: item for item in conversion.generated_files}
        checks: list[DeterministicCheck] = []
        code = files.get("PYSPARK_CODE")
        try:
            ast.parse(code.content if code else "")
            checks.append(DeterministicCheck(check_name="PYTHON_SYNTAX", passed=code is not None, details="Generated Python parses successfully." if code else "PySpark file is missing."))
        except SyntaxError as exc:
            checks.append(DeterministicCheck(check_name="PYTHON_SYNTAX", passed=False, details=f"Syntax error at line {exc.lineno}: {exc.msg}"))
        combined = "\n".join(item.content for item in conversion.generated_files)
        placeholders = [value for value in ("TODO", "<PLACEHOLDER>", "...") if value in combined]
        checks.append(DeterministicCheck(check_name="NO_PLACEHOLDERS", passed=not placeholders, details="No placeholders found." if not placeholders else f"Found: {', '.join(placeholders)}"))
        secret_terms = [value for value in ("OPENAI_API_KEY", "AZURE_CLIENT_SECRET", "password=") if value.lower() in combined.lower()]
        checks.append(DeterministicCheck(check_name="NO_SECRETS", passed=not secret_terms, details="No secret markers found." if not secret_terms else f"Found: {', '.join(secret_terms)}"))
        targets = {field.name for target in mapping.targets for field in target.fields}
        missing_targets = sorted(field for field in targets if field not in combined)
        checks.append(DeterministicCheck(check_name="TARGET_FIELD_COVERAGE", passed=not missing_targets, details="All target fields are referenced." if not missing_targets else f"Missing target fields: {', '.join(missing_targets)}"))
        expressions = [item.get("source_expression") for item in discovery.get("business_rules", []) if item.get("source_expression")]
        checks.append(DeterministicCheck(check_name="BUSINESS_RULE_EVIDENCE", passed=bool(expressions) or not discovery.get("business_rules"), details="Business-rule evidence is available." if expressions else "No source expressions were available for rule coverage."))
        checks.append(DeterministicCheck(check_name="REQUIRED_FILES", passed=set(files) == {"PYSPARK_CODE", "UNIT_TEST", "CONFIGURATION"}, details="All required artifacts are present." if set(files) == {"PYSPARK_CODE", "UNIT_TEST", "CONFIGURATION"} else "One or more required artifacts are missing."))
        target_instances = {target.name for target in mapping.targets}
        connected_target_fields = {
            connector.to_field
            for connector in mapping.connectors
            if connector.to_instance in target_instances and connector.to_field
        }
        missing_connector_targets = sorted(targets - connected_target_fields)
        checks.append(
            DeterministicCheck(
                check_name="TARGET_CONNECTOR_COVERAGE",
                passed=not missing_connector_targets,
                details=(
                    "Every target field has an incoming canonical connector."
                    if not missing_connector_targets
                    else "Target fields without incoming connectors: "
                    + ", ".join(missing_connector_targets)
                ),
            )
        )
        transformation_expressions = {
            port.name: port.expression
            for transformation in mapping.transformations
            for port in transformation.ports
            if port.expression
        }
        derived_targets = sorted(targets.intersection(transformation_expressions))
        missing_derived_implementation = sorted(
            field for field in derived_targets if field not in combined
        )
        checks.append(
            DeterministicCheck(
                check_name="DERIVED_FIELD_IMPLEMENTATION",
                passed=not missing_derived_implementation,
                details=(
                    "Connector-grounded derived target fields are represented in generated artifacts."
                    if not missing_derived_implementation
                    else "Derived target fields missing from generated artifacts: "
                    + ", ".join(missing_derived_implementation)
                ),
            )
        )
        return checks

    def _capture_usage(self, raw) -> None:
        usage = getattr(raw, "usage_metadata", None) or {}
        metadata = getattr(raw, "response_metadata", None) or {}
        token_usage = metadata.get("token_usage", {})
        self.input_tokens = int(usage.get("input_tokens") or token_usage.get("prompt_tokens") or 0)
        self.output_tokens = int(usage.get("output_tokens") or token_usage.get("completion_tokens") or 0)
        self.model_name = metadata.get("model_name") or metadata.get("model") or self.MODEL_NAME
