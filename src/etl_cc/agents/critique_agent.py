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
    AGENT_VERSION = "1.4.0"
    STAGE_NAME = "CONVERSION_CRITIQUE"
    MODEL_NAME = "gpt-4o"
    PROMPT_NAME = "PYSPARK_CONVERSION_CRITIQUE"
    PROMPT_VERSION = "1.5.0"

    UNIT_TEST_SEMANTICS_GUIDANCE = """Review UNIT_TEST expected rows against the complete canonical operation sequence, not just the generated PySpark. For AGGREGATE/GROUP BY, Spark retains null grouping keys as a null-key group unless canonical upstream evidence explicitly filters them out. SUM ignores null measure values within a group but does not remove the group; an all-null group has a null SUM. A unit-test expectation that drops a null-key group without a grounded exclusion rule is a HIGH severity UNIT_TEST_SEMANTICS issue and requires revision."""
    UNIT_TEST_DECIMAL_GUIDANCE = """For every DecimalType output, require expected pytest dataset values to be decimal.Decimal values constructed from string literals, such as Decimal('50.00') or Decimal('0.00'). Never accept raw Python float/double literals such as 50.0 or 12.5 in expected values. Spark returns DecimalType values as decimal.Decimal; a DecimalType assertion comparing a collected Decimal to a float is a HIGH severity UNIT_TEST_NUMERIC_TYPE issue and requires revision."""

    def __init__(self, source_vendor: str = "INFORMATICA") -> None:
        self.source_vendor = source_vendor
        self.model_name = self.MODEL_NAME
        self.prompt_name = (
            self.PROMPT_NAME
            if source_vendor.upper().startswith("INFORMATICA")
            else f"{source_vendor.upper()}_{self.PROMPT_NAME}"
        )
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
        vendor_guidance = ""
        if self.source_vendor.upper().startswith("DATASTAGE"):
            vendor_guidance = """
    DataStage-specific review:
    - Treat canonical source/target schemas and stage links as authoritative.
    - Stage inventory and OLEType identify components but do not establish source/target direction or data flow.
    - Flag unsupported or ambiguous DataStage properties for manual review; do not infer behavior from stage names.
    """.strip()
        prompt = f"""Review generated Databricks PySpark against supplied
    {self.source_vendor} evidence. Never invent requirements. Treat failed deterministic
checks, missing grounded business rules, missing canonical target fields,
syntax errors, secrets, TODOs, or placeholders as requiring revision.

    {vendor_guidance}

Field-role rules:
- Require final projection only for canonical target fields.
- A source field used only by FILTER, JOIN, GROUP, SORT, ROUTE, or as a
  derivation input is covered when used in that operation. Do not require it
  in the final projection.
- Spark filter predicates already exclude rows where the predicate evaluates
  to null. Do not require an explicit isNotNull check unless supplied canonical
  evidence defines a distinct null-handling rule.
- {self.UNIT_TEST_SEMANTICS_GUIDANCE}
- {self.UNIT_TEST_DECIMAL_GUIDANCE}
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
        unit_test = (
            files.get("UNIT_TEST").content
            if files.get("UNIT_TEST")
            else ""
        )
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
                and not any(
                    marker in text
                    for marker in (
                        "UNIT_TEST",
                        "EXPECTED ROW",
                        "TEST EXPECTATION",
                        "ASSERTION",
                    )
                )
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
            matching_risk_score_default = (
                issue.category.upper() == "UNIT_TEST_SEMANTICS"
                and "RISK_SCORE" in text
                and "NULL" in text
                and CritiqueAgent._has_matching_null_risk_score_default(
                    code,
                    unit_test,
                )
            )
            if (
                null_only_finding
                or projection_only_finding
                or matching_risk_score_default
            ):
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
    def _has_matching_null_risk_score_default(
        code: str,
        unit_test: str,
    ) -> bool:
        """Check that null RISK_SCORE is coalesced and asserted as zero."""
        def sequence_elements(node: ast.expr | None) -> list[ast.expr]:
            return node.elts if isinstance(node, (ast.List, ast.Tuple)) else []

        def is_zero(node: ast.expr) -> bool:
            if (
                isinstance(node, ast.Constant)
                and isinstance(node.value, (int, float))
                and not isinstance(node.value, bool)
            ):
                return node.value == 0
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "Decimal"
                and node.args
                and isinstance(node.args[0], ast.Constant)
                and isinstance(node.args[0].value, str)
            ):
                try:
                    return float(node.args[0].value) == 0
                except ValueError:
                    return False
            return False

        try:
            code_tree = ast.parse(code)
            test_tree = ast.parse(unit_test)
        except SyntaxError:
            return False

        has_zero_coalesce = any(
            isinstance(node, ast.Call)
            and (
                (
                    isinstance(node.func, ast.Name)
                    and node.func.id.lower() == "coalesce"
                )
                or (
                    isinstance(node.func, ast.Attribute)
                    and node.func.attr.lower() == "coalesce"
                )
            )
            and any(
                isinstance(child, ast.Constant)
                and isinstance(child.value, str)
                and child.value.upper() == "RISK_SCORE"
                for child in ast.walk(node)
            )
            and any(
                isinstance(child, ast.Constant)
                and isinstance(child.value, (int, float))
                and not isinstance(child.value, bool)
                and child.value == 0
                for child in ast.walk(node)
            )
            for node in ast.walk(code_tree)
        )
        if not has_zero_coalesce:
            return False

        for function in (
            node for node in ast.walk(test_tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        ):
            assignments: dict[str, ast.expr] = {}
            for node in ast.walk(function):
                if not isinstance(node, ast.Assign):
                    continue
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        assignments[target.id] = node.value

            try:
                expected_columns = ast.literal_eval(
                    assignments.get("deterministic_columns")
                )
            except (TypeError, ValueError):
                continue
            expected_rows = sequence_elements(
                assignments.get("expected_rows")
            )
            if not expected_columns or not expected_rows:
                continue
            risk_score_output_index = next(
                (
                    index
                    for index, name in enumerate(expected_columns)
                    if isinstance(name, str) and name.upper() == "RISK_SCORE"
                ),
                None,
            )
            if risk_score_output_index is None:
                continue

            for schema_name, schema_expression in assignments.items():
                if not schema_name.startswith("schema_"):
                    continue
                rows_expression = assignments.get(
                    schema_name.replace("schema_", "rows_", 1)
                )
                if rows_expression is None:
                    continue
                schema_arguments = (
                    schema_expression.args
                    if isinstance(schema_expression, ast.Call)
                    and isinstance(schema_expression.func, ast.Name)
                    and schema_expression.func.id == "StructType"
                    else []
                )
                schema_items = sequence_elements(
                    schema_arguments[0] if schema_arguments else None
                )
                schema_fields = [
                    item.args[0].value
                    for item in schema_items
                    if (
                        isinstance(item, ast.Call)
                        and isinstance(item.func, ast.Name)
                        and item.func.id == "StructField"
                        and item.args
                        and isinstance(item.args[0], ast.Constant)
                        and isinstance(item.args[0].value, str)
                    )
                ]
                if "RISK_SCORE" not in [name.upper() for name in schema_fields]:
                    continue
                risk_score_input_index = next(
                    index
                    for index, name in enumerate(schema_fields)
                    if name.upper() == "RISK_SCORE"
                )
                input_rows = sequence_elements(rows_expression)
                has_null_input = any(
                    len(row_values) > risk_score_input_index
                    and isinstance(
                        row_values[risk_score_input_index],
                        ast.Constant,
                    )
                    and row_values[risk_score_input_index].value is None
                    for row in input_rows
                    for row_values in [sequence_elements(row)]
                )
                if not has_null_input:
                    continue
                if any(
                    len(row_values) > risk_score_output_index
                    and is_zero(row_values[risk_score_output_index])
                    for row in expected_rows
                    for row_values in [sequence_elements(row)]
                ):
                    return True
        return False

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
