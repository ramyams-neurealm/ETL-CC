from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from etl_cc.services import _conversion_workbench_metrics


def test_conversion_metrics_match_workbench_payload_shape():
    started_at = datetime(2026, 10, 20, tzinfo=timezone.utc)
    audits = [
        SimpleNamespace(
            started_at=started_at,
            completed_at=started_at + timedelta(seconds=30),
            input_tokens=120,
            output_tokens=40,
            model_name="gpt-4o",
        ),
        SimpleNamespace(
            started_at=started_at + timedelta(seconds=10),
            completed_at=started_at + timedelta(seconds=45),
            input_tokens=80,
            output_tokens=20,
            model_name="PGVECTOR_HYBRID",
        ),
    ]
    mapping = SimpleNamespace(
        id=167,
        complexity_score=18,
        transformation_count=2,
        migration_status="CONVERTED",
        dependencies={},
    )

    metrics = _conversion_workbench_metrics(
        mapping,
        audits,
        {"confidence": 0.95},
        {
            "confidence": 0.95,
            "decision": "PASS",
            "revision_required": False,
            "business_rule_coverage": 1,
            "lineage_coverage": 1,
            "deterministic_checks": [{"passed": True}],
        },
        "line one\n\nline two\n",
    )

    assert metrics["agent_performance"] == {
        "input_tokens": 200,
        "output_tokens": 60,
        "total_tokens": 260,
        "duration_seconds": 45.0,
        "duration_display": "0m 45s",
        "model_names": ["PGVECTOR_HYBRID", "gpt-4o"],
        "agent_cost": None,
        "agent_cost_status": "TOKEN_PRICING_NOT_CONFIGURED",
    }
    assert metrics["economics"]["calculation_status"] == "CALCULATED"
    assert metrics["quality"] == {
        "lines_converted": 2,
        "confidence": 0.95,
        "critique_decision": "PASS",
        "revision_required": False,
        "business_rule_coverage": 1,
        "lineage_coverage": 1,
        "code_coverage_accuracy": None,
        "coverage_status": "PENDING_VALIDATION",
        "deterministic_checks": [{"passed": True}],
    }


def test_conversion_metrics_support_missing_audits_and_artifacts():
    mapping = SimpleNamespace(
        id=167,
        complexity_score=None,
        transformation_count=0,
        migration_status="NOT_STARTED",
        dependencies={},
    )

    metrics = _conversion_workbench_metrics(mapping, [], {}, {}, "")

    assert metrics["agent_performance"]["total_tokens"] == 0
    assert metrics["agent_performance"]["model_names"] == []
    assert metrics["quality"]["lines_converted"] == 0
    assert metrics["quality"]["deterministic_checks"] == []
