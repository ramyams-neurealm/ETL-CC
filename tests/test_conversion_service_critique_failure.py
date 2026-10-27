from etl_cc.agents.critique_agent import (
    CritiqueIssue,
    CritiqueResult,
    DeterministicCheck,
)
from etl_cc.conversion_service import _critique_failure_reason


def test_critique_failure_reason_includes_findings_and_actual_attempt_counts():
    critique = CritiqueResult(
        decision="REVISE",
        issues=[
            CritiqueIssue(
                severity="HIGH",
                category="UNIT_TEST_SEMANTICS",
                description="Expected rows do not include the threshold boundary.",
                recommendation="Add an input row at the threshold.",
            )
        ],
        business_rule_coverage=1.0,
        lineage_coverage=1.0,
        revision_required=True,
        confidence=0.95,
        deterministic_checks=[
            DeterministicCheck(
                check_name="REQUIRED_FILES",
                passed=False,
                details="The configuration artifact is missing.",
            )
        ],
    )

    reason = _critique_failure_reason(
        "g_load_high_value_txns",
        conversion_attempts=2,
        critique_attempts=1,
        critique=critique,
    )

    assert "conversion_attempts=2, critique_attempts=1" in reason
    assert "HIGH/UNIT_TEST_SEMANTICS" in reason
    assert "threshold boundary" in reason
    assert "FAILED REQUIRED_FILES" in reason
    assert "configuration artifact is missing" in reason


def test_critique_failure_reason_handles_missing_actionable_findings():
    critique = CritiqueResult(
        decision="REVISE",
        issues=[],
        business_rule_coverage=1.0,
        lineage_coverage=1.0,
        revision_required=True,
        confidence=0.5,
    )

    reason = _critique_failure_reason(
        "mapping",
        conversion_attempts=1,
        critique_attempts=1,
        critique=critique,
    )

    assert "The critique returned no actionable findings." in reason
