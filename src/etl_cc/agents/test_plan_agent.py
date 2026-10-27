"""Deterministic validation test-plan generation from canonical ETL metadata."""

from pydantic import BaseModel, Field

from etl_cc.models import CanonicalMapping


class PlannedTestCase(BaseModel):
    test_id: str
    test_name: str
    category: str
    severity: str
    expected_result: str
    evidence_payload: dict = Field(default_factory=dict)


class TestPlanResult(BaseModel):
    mapping_name: str
    test_count: int
    test_cases: list[PlannedTestCase] = Field(default_factory=list)


class TestPlanAgent:
    AGENT_NAME = "TEST_PLAN_AGENT"
    AGENT_VERSION = "1.0.0"
    STAGE_NAME = "TEST_PLANNING"
    MODEL_NAME = "DETERMINISTIC_METADATA"

    def run(self, mapping: CanonicalMapping, discovery: dict) -> TestPlanResult:
        tests = [
            PlannedTestCase(test_id="STATIC-001", test_name="Required artifacts", category="ARTIFACT", severity="CRITICAL", expected_result="PySpark, unit-test, and configuration artifacts are present."),
            PlannedTestCase(test_id="STATIC-002", test_name="PySpark syntax", category="SYNTAX", severity="CRITICAL", expected_result="Generated PySpark parses as Python."),
            PlannedTestCase(test_id="STATIC-003", test_name="Unit-test syntax", category="SYNTAX", severity="HIGH", expected_result="Generated unit tests parse as Python."),
            PlannedTestCase(test_id="STATIC-004", test_name="Configuration JSON", category="CONFIGURATION", severity="HIGH", expected_result="Configuration is a valid JSON object."),
            PlannedTestCase(test_id="STATIC-005", test_name="Databricks runtime safety", category="RUNTIME_SAFETY", severity="CRITICAL", expected_result="No placeholder paths, global SparkSession creation, or spark.stop calls."),
            PlannedTestCase(test_id="STATIC-006", test_name="Embedded secret detection", category="SECURITY", severity="CRITICAL", expected_result="No credentials or secret markers are embedded."),
            PlannedTestCase(test_id="STATIC-007", test_name="Safe test imports", category="SECURITY", severity="CRITICAL", expected_result="Generated tests use no prohibited network or process imports."),
        ]
        tests.extend([
            PlannedTestCase(test_id="PARITY-001", test_name="Source coverage", category="FUNCTIONAL_PARITY", severity="HIGH", expected_result="All canonical sources are represented."),
            PlannedTestCase(test_id="PARITY-002", test_name="Target coverage", category="FUNCTIONAL_PARITY", severity="HIGH", expected_result="All canonical targets are represented."),
            PlannedTestCase(test_id="PARITY-003", test_name="Target-field coverage", category="FUNCTIONAL_PARITY", severity="HIGH", expected_result="All target fields are represented."),
            PlannedTestCase(test_id="PARITY-004", test_name="Business-rule coverage", category="BUSINESS_RULE", severity="HIGH", expected_result="All grounded business rules are represented."),
            PlannedTestCase(test_id="PARITY-005", test_name="Lineage coverage", category="LINEAGE", severity="HIGH", expected_result="All target fields have resolved lineage."),
            PlannedTestCase(test_id="PARITY-006", test_name="Precision and scale coverage", category="SCHEMA", severity="HIGH", expected_result="Decimal precision and scale are preserved."),
            PlannedTestCase(test_id="PARITY-007", test_name="Overall functional parity", category="FUNCTIONAL_PARITY", severity="CRITICAL", expected_result="Overall parity meets the configured threshold."),
        ])
        for index, rule in enumerate(discovery.get("business_rules", []), start=1):
            tests.append(PlannedTestCase(test_id=f"RULE-{index:03d}", test_name=rule.get("description") or f"Business rule {index}", category="BUSINESS_RULE", severity="HIGH", expected_result="Generated implementation preserves the grounded rule.", evidence_payload={"source_expression": rule.get("source_expression"), "source_object": rule.get("source_object")}))
        return TestPlanResult(mapping_name=mapping.mapping_name, test_count=len(tests), test_cases=tests)
