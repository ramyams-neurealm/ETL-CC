"""Offline regression tests for the discovery -> deployment flow fixes."""

import asyncio
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from etl_cc import deployment_service, langgraph_orchestrator, worker
from etl_cc.agents.validation_agent import ValidationAgent
from etl_cc.models import CanonicalDataset, CanonicalField, CanonicalMapping


FIXTURES = Path(__file__).parent / "fixtures"


class _FakeSession:
    """Minimal async session for code that only gets, adds, and commits."""

    def __init__(self, rows=None, scalar_results=None, scalars_results=None):
        self.rows = rows or {}
        self.added = []
        self.committed = False
        self._scalar_results = list(scalar_results or [])
        self._scalars_results = list(scalars_results or [])

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, _model, key):
        return self.rows.get(key)

    async def scalar(self, _statement):
        return self._scalar_results.pop(0) if self._scalar_results else None

    async def scalars(self, _statement):
        return SimpleNamespace(all=lambda: self._scalars_results.pop(0))

    def add(self, item):
        self.added.append(item)

    async def flush(self):
        pass

    async def commit(self):
        self.committed = True


def _mapping_with_key(precision: int) -> CanonicalMapping:
    return CanonicalMapping(
        source_vendor="INFORMATICA",
        source_object_key="f/MAPPING/m",
        mapping_name="m",
        sources=[
            CanonicalDataset(
                name="SRC",
                dataset_type="SOURCE",
                fields=[CanonicalField(name="CODE", data_type="char", precision=precision, nullable=False)],
            )
        ],
    )


def _run_with_timeout(function, seconds=20):
    result = {}

    def target():
        try:
            result["value"] = function()
        except Exception as exc:
            result["error"] = exc

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(seconds)
    assert not thread.is_alive(), "simulation expansion did not terminate"
    if "error" in result:
        raise result["error"]
    return result["value"]


def test_simulated_expansion_terminates_for_short_string_keys():
    rows = _run_with_timeout(
        lambda: ValidationAgent._expand_simulated_rows(
            [{"CODE": "A"}], ["CODE"], 50, _mapping_with_key(5)
        )
    )

    codes = [row["CODE"] for row in rows]
    assert len(codes) == 50
    assert len(set(codes)) == 50
    assert all(len(code) <= 5 for code in codes)


def test_simulated_expansion_reports_when_no_unique_key_fits():
    with pytest.raises(ValueError, match="precision 1"):
        _run_with_timeout(
            lambda: ValidationAgent._expand_simulated_rows(
                [{"CODE": "A"}], ["CODE"], 40, _mapping_with_key(1)
            )
        )


def test_discovery_routes_datastage_dsx_uploads(monkeypatch):
    content = (FIXTURES / "sample_customers.dsx").read_bytes()

    async def load_manifest(_session, _source_id):
        return {
            "product_code": "DATASTAGE",
            "method_code": "DSX_UPLOAD",
            "connection_type": "DSX_UPLOAD",
            "config": {"original_file_name": "sample_customers.dsx"},
        }

    async def load_content(_session, _source_id):
        return content

    monkeypatch.setattr("etl_cc.worker.load_manifest", load_manifest)
    monkeypatch.setattr("etl_cc.worker.load_content", load_content)
    repository = SimpleNamespace(id=1, source_type="DATASTAGE", connection_type="DSX_UPLOAD")
    workflow = SimpleNamespace(workflow_id="DISC-test", scope_payload={"source_id": "source-1"})

    mappings = asyncio.run(worker._load_mappings(None, repository, workflow))

    assert [item.mapping_name for item in mappings] == ["Job_Customers"]
    assert mappings[0].source_vendor == "DATASTAGE_DSX"


def test_discovery_failure_is_persisted_with_triage(monkeypatch):
    workflow = SimpleNamespace(
        id=7,
        workflow_id="DISC-7",
        repository_id=1,
        current_stage="DISCOVERY",
        attempt_count=1,
        started_at=None,
        job_status="RUNNING",
        overall_status="RUNNING",
    )
    session = _FakeSession(rows={7: workflow})
    monkeypatch.setattr(worker, "SessionFactory", lambda: session)
    monkeypatch.setattr(
        worker.gliner_service,
        "triage_failure",
        lambda stage, reason: {"label": "HUMAN_REVIEW", "confidence": 0.8, "suggested_step": "Inspect it."},
    )

    asyncio.run(worker._fail(7, ValueError("No mappings matched the discovery scope.")))

    assert workflow.job_status == workflow.overall_status == "FAILED"
    assert workflow.failure_reason == "No mappings matched the discovery scope."
    assert workflow.completed_at is not None
    assert session.committed
    payload = session.added[-1].event_payload
    assert session.added[-1].event_type == "WORKFLOW_FAILED"
    assert payload["failure_category"] == "HUMAN_REVIEW"
    assert payload["gliner_decision"] == "HUMAN_REVIEW"
    assert payload["gliner_confidence"] == 0.8
    assert payload["failure_suggested_step"] == "Inspect it."


@pytest.mark.parametrize(
    ("state", "expected"),
    [
        ({"deployable": True, "deployment_queued": True}, "deployment"),
        ({"deployable": True, "deployment_queued": False}, "complete"),
        ({"deployable": False, "failed": True}, "failed"),
    ],
)
def test_validation_only_routes_to_deployment_when_queued(state, expected):
    assert langgraph_orchestrator.after_validation(state) == expected


def test_standalone_jobs_dispatch_to_their_stage(monkeypatch):
    calls = []

    async def record(name, run_id, *_args):
        calls.append((name, run_id))

    monkeypatch.setattr(langgraph_orchestrator.conversion_service, "execute_conversion_workflow", lambda run_id: record("conversion", run_id))
    monkeypatch.setattr(langgraph_orchestrator.deployment_service, "execute_manual_deployment_workflow", lambda run_id: record("deployment", run_id))

    asyncio.run(langgraph_orchestrator.run_standalone_job({"job_type": "CONVERSION", "run_id": 3}))
    asyncio.run(langgraph_orchestrator.run_standalone_job({"job_type": "DEPLOYMENT", "run_id": 4}))

    assert calls == [("conversion", 3), ("deployment", 4)]


def test_deployment_uses_newest_artifact_of_each_type():
    def artifact(artifact_id, artifact_type, version):
        return SimpleNamespace(id=artifact_id, artifact_type=artifact_type, artifact_version=version)

    conversion = SimpleNamespace(id=11)
    # Rows arrive ordered newest version first, as the query requests.
    rows = [
        artifact(30, "PYSPARK_CODE", 2),
        artifact(31, "UNIT_TEST", 2),
        artifact(20, "PYSPARK_CODE", 1),
        artifact(21, "UNIT_TEST", 1),
        artifact(22, "CONFIGURATION", 1),
    ]
    session = _FakeSession(scalar_results=[conversion], scalars_results=[rows])
    validation = SimpleNamespace(workflow_id="VAL-1", scope_payload={"conversion_workflow_id": "CONV-1"})

    selected = asyncio.run(
        deployment_service._deployable_artifacts(session, validation, SimpleNamespace(id=5))
    )

    assert sorted(item.id for item in selected) == [22, 30, 31]
