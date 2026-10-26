import asyncio
import io
import json
from pathlib import Path
from types import SimpleNamespace

from etl_cc.ab_initio_parser import AbInitioGraphParser
from etl_cc.api import workflow_status
from etl_cc.agents.discovery_agent import (
    DiscoveryAgent,
    DiscoverySemanticAnalysis,
)
from etl_cc.agents.gliner_decision_client import gliner_decide
from etl_cc.config import settings
from etl_cc.gliner_service import GLiNERService


class FakeResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def test_gliner_client_accepts_decision_response(monkeypatch):
    monkeypatch.setattr(
        "etl_cc.agents.gliner_decision_client.urllib.request.urlopen",
        lambda request, timeout: FakeResponse(
            json.dumps(
                {"label": "AUTO_CONVERT", "confidence": 0.95}
            ).encode("utf-8")
        ),
    )

    assert gliner_decide("simple mapping", "http://127.0.0.1:8100") == {
        "label": "AUTO_CONVERT",
        "confidence": 0.95,
    }


def test_gliner_client_rejects_invalid_response(monkeypatch):
    monkeypatch.setattr(
        "etl_cc.agents.gliner_decision_client.urllib.request.urlopen",
        lambda request, timeout: FakeResponse(
            json.dumps({"label": "UNKNOWN", "confidence": 1.5}).encode("utf-8")
        ),
    )

    assert gliner_decide("simple mapping", "http://127.0.0.1:8100") is None


def test_discovery_routing_applies_confidence_threshold(monkeypatch):
    monkeypatch.setattr(settings, "enable_gliner", True)
    service = GLiNERService()
    monkeypatch.setattr(
        service,
        "_decide",
        lambda text: {"label": "AUTO_CONVERT", "confidence": 0.89},
    )

    assert service.decide_discovery_routing("mapping summary") == {
        "decision": "HUMAN_REVIEW",
        "confidence": 0.89,
    }


def test_discovery_agent_persists_gliner_routing_fields(monkeypatch):
    monkeypatch.setattr(settings, "enable_gliner", True)
    monkeypatch.setattr(settings, "gliner_shadow_mode", True)
    agent = DiscoveryAgent()

    async def fake_run_llm(**kwargs):
        return DiscoverySemanticAnalysis(
            business_purpose="Load high-value transactions.",
            confidence=0.9,
            human_review_required=False,
        )

    monkeypatch.setattr(agent, "_run_llm", fake_run_llm)
    monkeypatch.setattr(
        "etl_cc.agents.discovery_agent.gliner_service.decide_discovery_routing",
        lambda summary: {"decision": "AUTO_CONVERT", "confidence": 0.96},
    )
    mapping_path = (
        Path(__file__).parents[1]
        / "runtime_sources"
        / "ab_initio_graph"
        / "sample_ab_initio_graph.json"
    )
    mapping = AbInitioGraphParser().parse_file(mapping_path)[0]

    result = asyncio.run(agent.run(mapping))

    assert result.gliner_decision == "AUTO_CONVERT"
    assert result.gliner_confidence == 0.96


def test_validation_uncertainty_is_conservative_when_service_unavailable(
    monkeypatch,
):
    monkeypatch.setattr(settings, "enable_gliner", True)
    service = GLiNERService()
    monkeypatch.setattr(service, "_decide", lambda text: None)

    assert service.check_validation_uncertainty("validation summary") == {
        "uncertain": True,
        "confidence": 0.0,
        "decision": "UNAVAILABLE",
    }


def test_validation_uncertainty_uses_decision_and_threshold(monkeypatch):
    monkeypatch.setattr(settings, "enable_gliner", True)
    service = GLiNERService()
    monkeypatch.setattr(
        service,
        "_decide",
        lambda text: {"label": "AUTO_CONVERT", "confidence": 0.89},
    )

    assert service.check_validation_uncertainty("validation summary") == {
        "uncertain": True,
        "confidence": 0.89,
        "decision": "AUTO_CONVERT",
    }


def test_failure_triage_uses_live_decision_and_action(monkeypatch):
    monkeypatch.setattr(settings, "enable_gliner", True)
    service = GLiNERService()
    monkeypatch.setattr(
        service,
        "_decide",
        lambda text: {"label": "BLOCK", "confidence": 0.91},
    )

    assert service.triage_failure("DISCOVERY", "unsupported graph") == {
        "label": "BLOCK",
        "confidence": 0.91,
        "suggested_step": "Resolve the blocking failure before retrying the workflow.",
    }


def test_workflow_status_exposes_failure_triage(monkeypatch):
    workflow = SimpleNamespace(
        id=7,
        workflow_id="DISC-test",
        repository_id=12,
        current_stage="DISCOVERY",
        overall_status="FAILED",
        job_status="FAILED",
        scope_payload={},
        failure_stage="DISCOVERY",
        failure_reason="parse failure",
        created_at=None,
        started_at=None,
        completed_at=None,
    )
    event = SimpleNamespace(
        event_payload={
            "failure_category": "HUMAN_REVIEW",
            "failure_suggested_step": "Inspect the mapping.",
            "gliner_decision": "HUMAN_REVIEW",
            "gliner_confidence": 0.93,
        }
    )

    class FakeSession:
        async def scalar(self, query):
            return event

    async def fake_get_workflow(session, workflow_id):
        return workflow

    monkeypatch.setattr(
        "etl_cc.api.get_workflow_by_external_id",
        fake_get_workflow,
    )

    payload = asyncio.run(workflow_status("DISC-test", FakeSession()))

    assert payload["failure_category"] == "HUMAN_REVIEW"
    assert payload["failure_suggested_step"] == "Inspect the mapping."
    assert payload["gliner_decision"] == "HUMAN_REVIEW"
    assert payload["gliner_confidence"] == 0.93
