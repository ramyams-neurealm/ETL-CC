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
    AGENT_VERSION = "1.0.0"
    STAGE_NAME = "CONVERSION_CRITIQUE"
    MODEL_NAME = "gpt-4o"
    PROMPT_NAME = "PYSPARK_CONVERSION_CRITIQUE"
    PROMPT_VERSION = "1.0.0"

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
Informatica evidence. Never invent requirements. Treat any failed deterministic
check, missing business rule, missing target field, syntax error, secret, TODO,
or placeholder as requiring revision. Return structured critique only."""
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
        if any(not item.passed for item in checks):
            parsed.decision = "REVISE"
            parsed.revision_required = True
        self._capture_usage(raw)
        return parsed

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
        return checks

    def _capture_usage(self, raw) -> None:
        usage = getattr(raw, "usage_metadata", None) or {}
        metadata = getattr(raw, "response_metadata", None) or {}
        token_usage = metadata.get("token_usage", {})
        self.input_tokens = int(usage.get("input_tokens") or token_usage.get("prompt_tokens") or 0)
        self.output_tokens = int(usage.get("output_tokens") or token_usage.get("completion_tokens") or 0)
        self.model_name = metadata.get("model_name") or metadata.get("model") or self.MODEL_NAME
