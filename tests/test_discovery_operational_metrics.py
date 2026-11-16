import asyncio
from types import SimpleNamespace

from etl_cc.models import DiscoveryOperationalMetrics
from etl_cc.services import (
    _discovery_operational_metrics,
    get_discovery_dashboard,
)


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class _MetricsSession:
    def __init__(self, *, mappings, discovery_audits=(), agent_rows=()):
        self._results = [mappings, discovery_audits, agent_rows]
        self.statements = []

    async def execute(self, statement):
        self.statements.append(statement)
        return _Result(self._results.pop(0))


def test_operational_metrics_classifies_statuses_and_aggregates_tokens():
    mappings = [
        (1, "DISCOVERED", "CONVERTED", {"discovery_analysis": {"human_review_required": True}}),
        (2, "ANALYZED", "VALIDATED", {}),
        (3, "ANALYZED", "DEPLOYED", {}),
        (4, "DISCOVERED", "NOT_STARTED", {}),
        (5, "ANALYZED", "QUEUED", {}),
        (6, "ANALYZED", "CONVERTING", {}),
        (7, "ANALYZED", "VALIDATING", {}),
        (8, "ANALYZED", "DEPLOYMENT_QUEUED", {}),
        (9, "ANALYZED", "DEPLOYMENT_IN_PROGRESS", {}),
        (10, "ANALYZED", "CONVERSION_FAILED", {}),
        (11, "ANALYZED", "VALIDATION_FAILED", {}),
        (12, "ANALYZED", "DEPLOYMENT_FAILED", {}),
        (13, "ANALYZED", "REVIEW_REQUIRED", {}),
        (14, "FAILED", "NOT_STARTED", {}),
    ]
    discovery_audits = [
        (2, {"human_review_required": True}),
        (1, {"human_review_required": False}),
        (1, {"human_review_required": True}),
    ]
    agent_rows = [
        ("DISCOVERY_AGENT", "COMPLETED", 2, 100, 40),
        ("DISCOVERY_AGENT", "FAILED", 1, 25, None),
        ("DISCOVERY_AGENT", "RUNNING", 1, None, 5),
        ("DISCOVERY_AGENT", "UNKNOWN", 1, 10, 2),
        ("CONVERSION_AGENT", "COMPLETED", 1, 50, 20),
    ]
    session = _MetricsSession(
        mappings=mappings,
        discovery_audits=discovery_audits,
        agent_rows=agent_rows,
    )

    metrics = asyncio.run(_discovery_operational_metrics(session, 7))

    assert isinstance(metrics, DiscoveryOperationalMetrics)
    assert metrics.total_mappings == 14
    assert metrics.converted_mappings == 3
    assert metrics.pending_mappings == 7
    assert metrics.failed_mappings == 3
    assert metrics.review_required_mappings == 1
    assert metrics.automation_rate == 21.4
    assert metrics.failure_rate == 21.4
    assert metrics.review_rate == 7.1
    assert metrics.total_objects == 14
    assert metrics.total_discovery_objects == 14
    assert metrics.discovered_objects == 13
    assert metrics.undiscovered_objects == 1
    assert metrics.coverage_percentage == 92.9
    assert metrics.total_agent_runs == 6
    assert metrics.agent_terminal_runs == 4
    assert metrics.successful_agent_runs == 3
    assert metrics.failed_agent_runs == 1
    assert metrics.running_agent_runs == 1
    assert metrics.overall_agent_success_rate == 75.0
    assert metrics.total_input_tokens == 185
    assert metrics.total_output_tokens == 67
    assert metrics.total_tokens == 252
    assert metrics.average_tokens_per_mapping == 18.0
    assert metrics.avg_agent_runs_per_mapping == 0.4
    assert metrics.mappings_requiring_review == 1
    assert metrics.mappings_not_requiring_review == 13
    assert metrics.review_required_rate == 7.1
    assert metrics.agent_breakdown["DISCOVERY_AGENT"] == {
        "total_runs": 5,
        "successful_runs": 2,
        "failed_runs": 1,
        "running_runs": 1,
        "success_rate": 66.7,
    }
    discovery_query = next(
        statement
        for statement in session.statements
        if "response_payload" in str(statement)
    )
    rendered_query = str(discovery_query).lower()
    assert "coalesce" in rendered_query
    assert "completed_at" in rendered_query
    assert "created_at" in rendered_query
    assert rendered_query.rfind("agent_response_etl.id desc") > 0


def test_agent_success_rate_excludes_running_and_unknown_statuses():
    session = _MetricsSession(
        mappings=[(1, "ANALYZED", "CONVERTED", {})],
        agent_rows=[
            ("DISCOVERY_AGENT", "COMPLETED", 2, 0, 0),
            ("DISCOVERY_AGENT", "FAILED", 1, 0, 0),
            ("DISCOVERY_AGENT", "RUNNING", 5, 0, 0),
            ("DISCOVERY_AGENT", "UNKNOWN", 10, 0, 0),
        ],
    )

    metrics = asyncio.run(_discovery_operational_metrics(session, 7))

    assert metrics.total_agent_runs == 18
    assert metrics.agent_terminal_runs == 3
    assert metrics.overall_agent_success_rate == 66.7
    assert metrics.running_agent_runs == 5
    assert metrics.agent_breakdown["DISCOVERY_AGENT"]["success_rate"] == 66.7


def test_latest_discovery_audit_uses_completed_timestamp_then_created_timestamp():
    mappings = [
        (1, "ANALYZED", "CONVERTED", {}),
    ]
    discovery_audits = [
        (1, {"human_review_required": False}),
        (1, {"human_review_required": True}),
    ]
    session = _MetricsSession(
        mappings=mappings,
        discovery_audits=discovery_audits,
        agent_rows=[],
    )

    metrics = asyncio.run(_discovery_operational_metrics(session, 7))

    assert metrics.mappings_requiring_review == 0
    assert metrics.review_required_rate == 0.0
    query = next(
        statement
        for statement in session.statements
        if "response_payload" in str(statement)
    )
    query_text = str(query).lower()
    assert "coalesce" in query_text
    assert "completed_at" in query_text
    assert "created_at" in query_text
    assert "agent_response_etl.id desc" in query_text


def test_operational_metrics_handles_empty_repository_and_zero_division():
    metrics = asyncio.run(
        _discovery_operational_metrics(
            _MetricsSession(mappings=[]),
            999,
        )
    )

    assert metrics.total_mappings == 0
    assert metrics.automation_rate == 0.0
    assert metrics.coverage_percentage == 0.0
    assert metrics.overall_agent_success_rate == 0.0
    assert metrics.average_tokens_per_mapping == 0.0
    assert metrics.avg_agent_runs_per_mapping == 0.0
    assert metrics.review_required_rate == 0.0
    assert metrics.failure_rate == 0.0
    assert metrics.review_rate == 0.0
    assert metrics.agent_terminal_runs == 0
    assert metrics.total_discovery_objects == 0
    assert metrics.agent_breakdown == {}


def test_operational_metrics_handles_missing_payloads_and_token_values():
    session = _MetricsSession(
        mappings=[(1, "DISCOVERED", "QUEUED", None)],
        discovery_audits=[(1, None)],
        agent_rows=[("DISCOVERY_AGENT", "RUNNING", 1, None, None)],
    )

    metrics = asyncio.run(_discovery_operational_metrics(session, 3))

    assert metrics.mappings_requiring_review == 0
    assert metrics.mappings_not_requiring_review == 1
    assert metrics.total_input_tokens == 0
    assert metrics.total_output_tokens == 0
    assert metrics.running_agent_runs == 1
    assert metrics.overall_agent_success_rate == 0.0


def test_discovery_dashboard_adds_operational_metrics_without_removing_fields(
    monkeypatch,
):
    class _DashboardSession:
        async def get(self, _model, _key):
            return SimpleNamespace(
                id=7,
                repository_name="repo",
                source_type="INFORMATICA",
                connection_type="POWERCENTER",
                environment="DEV",
            )

        async def scalar(self, _statement):
            return None

    summary = SimpleNamespace(
        model_dump=lambda **_kwargs: {
            "repository_id": 7,
            "total_mappings": 1,
            "need_to_discover": 0,
            "discovered": 1,
            "complexity_distribution": {
                "LOW": 1,
                "MEDIUM": 0,
                "HIGH": 0,
                "UNKNOWN": 0,
            },
            "ready": 1,
            "review_required": 0,
            "blocked": 0,
            "dependency_cycles": 0,
            "total_input_tokens": 0,
            "total_output_tokens": 0,
        }
    )
    metrics = DiscoveryOperationalMetrics(total_mappings=1).model_dump(
        mode="json"
    )

    async def _get_summary(_session, _repository_id):
        return summary

    async def _list_mappings(_session, _repository_id):
        return []

    async def _get_metrics(_session, _repository_id):
        return DiscoveryOperationalMetrics.model_validate(metrics)

    monkeypatch.setattr("etl_cc.services.get_discovery_summary", _get_summary)
    monkeypatch.setattr("etl_cc.services.list_repository_mappings", _list_mappings)
    monkeypatch.setattr(
        "etl_cc.services._discovery_operational_metrics",
        _get_metrics,
    )

    response = asyncio.run(get_discovery_dashboard(_DashboardSession(), 7))

    assert response["discovery_summary"] == summary.model_dump()
    assert response["economics"] is not None
    assert response["operational_metrics"]["total_mappings"] == 1
    assert response["repository"]["repository_id"] == 7
