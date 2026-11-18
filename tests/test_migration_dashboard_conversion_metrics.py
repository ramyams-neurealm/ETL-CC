import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from etl_cc import services
from etl_cc.services import (
    _conversion_workbench_metrics,
    _conversion_workbench_source_payload,
)


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


def test_conversion_workbench_source_payload_exposes_uploaded_file_metadata():
    source_definition = {
        "source_vendor": "INFORMATICA",
        "source_metadata": {"source_reference": "JOB_CUSTOMER_SALES_SUMMARY.xml"},
        "mapping_name": "JOB_CUSTOMER_SALES_SUMMARY",
    }
    mapping = SimpleNamespace(
        source_object_key="SALES_DW/JOB_CUSTOMER_SALES_SUMMARY",
        object_name="JOB_CUSTOMER_SALES_SUMMARY",
        source_definition=source_definition,
    )

    source = _conversion_workbench_source_payload(mapping)

    assert source["display_label"] == "JOB_CUSTOMER_SALES_SUMMARY.xml"
    assert source["source_file_name"] == "JOB_CUSTOMER_SALES_SUMMARY.xml"
    assert source["source_format"] == "XML"
    assert source["source_vendor"] == "INFORMATICA"
    assert source["source_definition"] == source_definition


def test_mapping_conversion_includes_source_and_existing_agent_results(monkeypatch):
    source_definition = {
        "source_vendor": "INFORMATICA",
        "source_metadata": {"source_reference": "sales.xml"},
    }
    mapping = SimpleNamespace(
        id=293,
        repository_id=50,
        object_name="JOB_CUSTOMER_SALES_SUMMARY",
        migration_wave=1,
        migration_status="REVIEW_REQUIRED",
        source_object_key="SALES_DW/JOB_CUSTOMER_SALES_SUMMARY",
        source_definition=source_definition,
    )
    payloads = {
        "RAG_RETRIEVAL_AGENT": {"status": "NO_MATCH"},
        "CONVERSION_AGENT": {"generated_files": [{"file_name": "transform.py"}]},
        "CRITIQUE_AGENT": {"decision": "REVISE"},
    }

    class FakeSession:
        async def get(self, _model, etl_object_id):
            if etl_object_id == mapping.id:
                return mapping
            return SimpleNamespace(source_content=b"<DSX/> ")

    async def latest_agent_response(
        _session, *, etl_object_id, agent_name
    ):
        payload = payloads.get(agent_name)
        return SimpleNamespace(response_payload=payload) if payload else None

    monkeypatch.setattr(services, "_latest_agent_response", latest_agent_response)
    result = asyncio.run(services.get_mapping_conversion(FakeSession(), 293))

    assert result["source"]["source_file_name"] == "sales.xml"
    assert result["source"]["content"] == "<DSX/> "
    assert result["rag"] == payloads["RAG_RETRIEVAL_AGENT"]
    assert result["conversion"] == payloads["CONVERSION_AGENT"]
    assert result["critique"] == payloads["CRITIQUE_AGENT"]


def test_start_discovery_persists_uploaded_source_content(monkeypatch):
    source_bytes = b"<DSX>uploaded source</DSX>"
    manifest = {
        "fingerprint": "verified-fingerprint",
        "connection_name": "Sales DW",
        "product_code": "DATASTAGE",
        "method_code": "DSX_UPLOAD",
        "connection_type": "DSX_UPLOAD",
        "environment": "DEV",
        "config": {"original_file_name": "sales.dsx"},
        "credential_ciphertext": None,
    }

    class FakeSession:
        def __init__(self):
            self.added = []

        def add(self, item):
            self.added.append(item)

        async def flush(self):
            for item in self.added:
                if isinstance(item, services.RepositoryETL):
                    item.id = 50

        async def commit(self):
            pass

    async def get_manifest(_session, _source_id):
        return manifest

    async def get_source_content(_session, _source_id):
        return source_bytes

    monkeypatch.setattr(services, "load_manifest", get_manifest)
    monkeypatch.setattr(services, "load_content", get_source_content)
    monkeypatch.setattr(
        services, "verify_connection_test_token", lambda *_args: None
    )
    session = FakeSession()
    request = SimpleNamespace(
        source_id="source-id",
        test_token="test-token",
        scope_type="ALL_MAPPINGS",
        selected_mapping_keys=[],
    )

    asyncio.run(services.start_discovery(session, request))

    repository = next(
        item for item in session.added if isinstance(item, services.RepositoryETL)
    )
    assert repository.source_content == source_bytes
