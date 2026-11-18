import asyncio
import json
from types import SimpleNamespace

from etl_cc.knowledge_capture_service import (
    capture_successful_conversion_knowledge,
)
from etl_cc.embedding_service import EmbeddingService as BaseEmbeddingService
from etl_cc.models import CanonicalMapping
from etl_cc.services import (
    _latest_rag_retrieval_metrics,
    _migration_knowledge_metrics,
    _rag_automation_metrics,
)


class _CaptureSession:
    def __init__(self, conversion_workflow, artifact, generated_code=None):
        self.conversion_workflow = conversion_workflow
        self.artifact = artifact
        self.generated_code = generated_code or "def transform(inputs):\n    return inputs\n"
        self.knowledge = None
        self.add_count = 0

    async def scalar(self, statement):
        query = str(statement)
        if "generated_artifact_etl" in query:
            return self.artifact
        if "knowledge_base_etl" in query:
            return self.knowledge
        if "workflow_run_etl" in query:
            return self.conversion_workflow
        raise AssertionError(f"Unexpected scalar query: {query}")

    async def get(self, _model, artifact_id):
        assert artifact_id == self.artifact.id
        return SimpleNamespace(
            content_text=self.generated_code,
            content_json=None,
            content_binary=None,
            encoding="UTF-8",
        )

    def add(self, row):
        self.knowledge = row
        self.add_count += 1

    async def flush(self):
        if self.knowledge is not None and self.knowledge.id is None:
            self.knowledge.id = 42

    async def commit(self):
        return None


class _CaptureEmbeddingService:
    calls = 0
    embedding_inputs = []

    async def embed_knowledge(self, knowledge):
        type(self).calls += 1
        embedding_input = BaseEmbeddingService.knowledge_text(knowledge)
        type(self).embedding_inputs.append(embedding_input)
        assert len(embedding_input) <= 3000
        if (
            knowledge.embedding is not None
            and knowledge.embedding_status == "COMPLETED"
            and knowledge.embedding_content_hash == knowledge.content_hash
        ):
            return False
        knowledge.embedding = [0.1] * 1536
        knowledge.embedding_model = "text-embedding-3-small"
        knowledge.embedding_status = "COMPLETED"
        knowledge.embedding_content_hash = knowledge.content_hash
        knowledge.embedding_error = None
        return True


def _capture_context():
    mapping = SimpleNamespace(
        id=11,
        repository_id=7,
        object_name="g_customer",
        source_definition=CanonicalMapping(
            source_vendor="AB_INITIO",
            source_object_key="graphs/customer",
            mapping_name="g_customer",
        ).model_dump(mode="json"),
        business_rules=[{"rule_type": "FILTER", "description": "Active rows"}],
        lineage={"field_lineage": [{"source": "ACTIVE", "target": "ACTIVE"}]},
        complexity="MEDIUM",
    )
    validation_workflow = SimpleNamespace(
        id=91,
        batch_id="MIG-101",
        scope_payload={"conversion_workflow_id": "CONV-101"},
    )
    conversion_workflow = SimpleNamespace(
        id=80,
        batch_id="MIG-101",
        scope_payload={
            "target_platform": "DATABRICKS",
            "target_framework": "PYSPARK",
        },
    )
    artifact = SimpleNamespace(
        id=55,
        artifact_version=1,
        artifact_type="PYSPARK_CODE",
    )
    validation_result = {
        "status": "PASSED",
        "deployable": True,
        "confidence_index": 0.92,
        "match_percentage": 98.0,
    }
    return (
        mapping,
        validation_workflow,
        conversion_workflow,
        artifact,
        validation_result,
    )


def test_capture_stores_approved_structured_knowledge_and_embeds_it(monkeypatch):
    _CaptureEmbeddingService.calls = 0
    _CaptureEmbeddingService.embedding_inputs = []
    monkeypatch.setattr(
        "etl_cc.knowledge_capture_service.EmbeddingService",
        _CaptureEmbeddingService,
    )
    mapping, validation, conversion, artifact, result = _capture_context()
    session = _CaptureSession(conversion, artifact)

    knowledge = asyncio.run(
        capture_successful_conversion_knowledge(
            session,
            validation,
            mapping,
            result,
            trigger="VALIDATION",
        )
    )

    document = json.loads(knowledge.content)
    assert knowledge.knowledge_type == "SUCCESSFUL_CONVERSION"
    assert knowledge.approval_status == "APPROVED"
    assert knowledge.confidence_score == 92
    assert knowledge.embedding_status == "COMPLETED"
    assert document["source_platform"] == "AB_INITIO"
    assert document["target_platform"] == "DATABRICKS"
    assert document["mapping_name"] == "g_customer"
    assert document["business_rules"] == mapping.business_rules
    assert document["lineage"] == mapping.lineage
    assert document["generated_code"].startswith("def transform")
    assert document["validation_results"] == result
    assert document["confidence_score"] == 0.92
    metadata = dict(knowledge.metadata_payload)
    embedding_document = metadata.pop("embedding_document")
    assert metadata == {
        "source_platform": "AB_INITIO",
        "target_platform": "DATABRICKS",
        "source_vendor": "AB_INITIO",
        "target_framework": "PYSPARK",
        "complexity": "MEDIUM",
        "transformation_count": 0,
        "transformation_types": [],
        "business_rule_types": ["FILTER"],
        "repository_id": 7,
        "migration_id": "MIG-101",
        "etl_object_id": 11,
        "trigger": "VALIDATION",
        "confidence_score": 0.92,
        "confidence_score_percentage": 92,
    }
    assert embedding_document["source_platform"] == "AB_INITIO"
    assert embedding_document["target_platform"] == "DATABRICKS"
    assert embedding_document["mapping_name"] == "g_customer"
    assert "generated_code" not in embedding_document
    assert "lineage" not in embedding_document
    assert "validation_results" not in embedding_document


def test_500k_generated_code_is_stored_but_compact_document_is_embedded(monkeypatch):
    _CaptureEmbeddingService.calls = 0
    _CaptureEmbeddingService.embedding_inputs = []
    monkeypatch.setattr(
        "etl_cc.knowledge_capture_service.EmbeddingService",
        _CaptureEmbeddingService,
    )
    mapping, validation, conversion, artifact, result = _capture_context()
    large_generated_code = "x" * 500_000
    session = _CaptureSession(conversion, artifact, large_generated_code)

    knowledge = asyncio.run(
        capture_successful_conversion_knowledge(
            session,
            validation,
            mapping,
            result,
            trigger="VALIDATION",
        )
    )

    full_document = json.loads(knowledge.content)
    embedding_input = _CaptureEmbeddingService.embedding_inputs[0]
    assert len(full_document["generated_code"]) == 500_000
    assert large_generated_code in knowledge.content
    assert len(embedding_input) <= 3000
    assert large_generated_code not in embedding_input


def test_validation_and_deployment_capture_is_idempotent_for_same_content(
    monkeypatch,
):
    _CaptureEmbeddingService.calls = 0
    monkeypatch.setattr(
        "etl_cc.knowledge_capture_service.EmbeddingService",
        _CaptureEmbeddingService,
    )
    mapping, validation, conversion, artifact, result = _capture_context()
    session = _CaptureSession(conversion, artifact)

    validation_knowledge = asyncio.run(
        capture_successful_conversion_knowledge(
            session,
            validation,
            mapping,
            result,
            trigger="VALIDATION",
        )
    )
    deployment_knowledge = asyncio.run(
        capture_successful_conversion_knowledge(
            session,
            validation,
            mapping,
            result,
            trigger="DEPLOYMENT",
        )
    )

    assert validation_knowledge is deployment_knowledge
    assert session.add_count == 1
    assert _CaptureEmbeddingService.calls == 2
    assert deployment_knowledge.embedding_status == "COMPLETED"


def test_capture_hash_includes_repository_migration_and_mapping_scope(monkeypatch):
    monkeypatch.setattr(
        "etl_cc.knowledge_capture_service.EmbeddingService",
        _CaptureEmbeddingService,
    )
    mapping, validation, conversion, artifact, result = _capture_context()
    first_session = _CaptureSession(conversion, artifact)
    first_knowledge = asyncio.run(
        capture_successful_conversion_knowledge(
            first_session,
            validation,
            mapping,
            result,
            trigger="VALIDATION",
        )
    )

    other_mapping, other_validation, other_conversion, other_artifact, other_result = (
        _capture_context()
    )
    other_mapping.id = 12
    other_mapping.repository_id = 8
    other_validation.batch_id = "MIG-102"
    other_conversion.batch_id = "MIG-102"
    second_session = _CaptureSession(other_conversion, other_artifact)
    second_knowledge = asyncio.run(
        capture_successful_conversion_knowledge(
            second_session,
            other_validation,
            other_mapping,
            other_result,
            trigger="VALIDATION",
        )
    )

    assert first_knowledge.content_hash != second_knowledge.content_hash
    assert json.loads(first_knowledge.content)["repository_id"] == 7
    assert json.loads(second_knowledge.content)["repository_id"] == 8
    assert json.loads(first_knowledge.content)["migration_id"] == "MIG-101"
    assert json.loads(second_knowledge.content)["migration_id"] == "MIG-102"
    assert json.loads(first_knowledge.content)["etl_object_id"] == 11
    assert json.loads(second_knowledge.content)["etl_object_id"] == 12


def test_failed_or_non_deployable_validation_is_not_captured(monkeypatch):
    mapping, validation, conversion, artifact, result = _capture_context()
    session = _CaptureSession(conversion, artifact)

    result["status"] = "FAILED"
    assert asyncio.run(
        capture_successful_conversion_knowledge(
            session,
            validation,
            mapping,
            result,
            trigger="VALIDATION",
        )
    ) is None
    assert session.add_count == 0


class _MetricsSession:
    def __init__(self):
        self.scalar_rows = [100, 101]
        self.audit_rows = [
            (
                11,
                {
                    "contributed_to_prompt": True,
                    "retrieved_items": [
                        {
                            "knowledge_id": 100,
                            "knowledge_type": "SUCCESSFUL_CONVERSION",
                        }
                    ],
                },
            ),
            (
                12,
                {
                    "contributed_to_prompt": True,
                    "retrieved_items": [
                        {"knowledge_id": 999, "knowledge_type": "GUIDANCE"}
                    ],
                },
            ),
            (
                13,
                {
                    "contributed_to_prompt": False,
                    "retrieved_items": [],
                },
            ),
        ]

    async def scalars(self, _statement):
        return SimpleNamespace(all=lambda: self.scalar_rows)

    async def execute(self, _statement):
        return SimpleNamespace(all=lambda: self.audit_rows)


def test_migration_knowledge_metrics_count_patterns_reuse_and_contribution():
    metrics = asyncio.run(
        _migration_knowledge_metrics(
            _MetricsSession(),
            "MIG-101",
            7,
            [11, 12, 13],
        )
    )

    assert metrics == {
        "total_successful_patterns": 2,
        "knowledge_reuse_rate": 50.0,
        "rag_contribution_rate": 66.7,
    }


def test_rag_automation_metrics_use_persisted_economics_tokens_and_audits():
    metrics = _rag_automation_metrics(
        [
            {
                "conversion_prompt_tokens": {
                    "baseline_prompt_tokens": 2000,
                    "actual_prompt_tokens": 1490,
                    "retrieved_context_tokens": 500,
                    "full_context_tokens": 1900,
                },
                "economics": {
                    "calculation_status": "CALCULATED",
                    "manual_hours": 6.0,
                    "manual_cost": 600.0,
                    "effort_saved_hours": 4.2,
                    "cost_saved": 420.0,
                }
            }
        ],
        {"total_tokens": 1490},
        {
            "total_successful_patterns": 2,
            "knowledge_reuse_rate": 50.0,
            "rag_contribution_rate": 66.7,
        },
        {
            "candidate_count": 10,
            "accepted_count": 7,
            "used_for_generation": True,
            "retrieval_success_rate": 70.0,
            "retrieval_status": "COMPLETED",
        },
    )

    assert metrics.manual_hours_required == 6.0
    assert metrics.manual_cost == 600.0
    assert metrics.effort_saved_hours == 4.2
    assert metrics.effort_saved_percentage == 70.0
    assert metrics.baseline_total_tokens == 2000
    assert metrics.actual_total_tokens == 1490
    assert metrics.token_utilization_saved == 510
    assert metrics.token_reduction_percentage == 25.5
    assert metrics.token_savings_status == "CALCULATED"
    assert metrics.retrieved_context_tokens == 500
    assert metrics.prompt_compression_savings == 1400
    assert metrics.rag_token_impact == -510
    assert metrics.rag_token_impact_status == "REDUCED_PROMPT_SIZE"
    assert metrics.cost_saved == 420.0
    assert metrics.cost_saved_percentage == 70.0
    assert metrics.total_successful_patterns == 2
    assert metrics.knowledge_reuse_rate == 50.0
    assert metrics.rag_contribution_rate == 66.7
    assert metrics.retrieval_success_rate == 70.0
    assert metrics.candidate_count == 10
    assert metrics.accepted_count == 7
    assert metrics.used_for_generation is True


def test_rag_automation_metrics_return_zero_with_statuses_when_data_missing():
    metrics = _rag_automation_metrics(
        [],
        {"total_tokens": 0},
        {
            "total_successful_patterns": 0,
            "knowledge_reuse_rate": 0.0,
            "rag_contribution_rate": 0.0,
        },
        {
            "candidate_count": 0,
            "accepted_count": 0,
            "used_for_generation": False,
            "retrieval_success_rate": 0.0,
            "retrieval_status": "NO_RETRIEVAL_DATA",
        },
    )

    assert metrics.manual_hours_required == 0
    assert metrics.effort_saved_percentage == 0
    assert metrics.cost_saved_percentage == 0
    assert metrics.economics_status == "NO_ECONOMICS_DATA"
    assert metrics.baseline_total_tokens == 0
    assert metrics.actual_total_tokens == 0
    assert metrics.token_utilization_saved == 0
    assert metrics.token_reduction_percentage == 0
    assert metrics.token_savings_status == "NO_RAG_CONTRIBUTION"
    assert metrics.retrieved_context_tokens == 0
    assert metrics.prompt_compression_savings == 0
    assert metrics.rag_token_impact == 0
    assert metrics.rag_token_impact_status == "NO_CHANGE"
    assert metrics.knowledge_reuse_status == "NO_SUCCESSFUL_PATTERNS"
    assert metrics.rag_contribution_status == "NO_MIGRATION_MAPPINGS"
    assert metrics.retrieval_status == "NO_RETRIEVAL_DATA"


def test_rag_token_impact_reports_increased_and_reduced_prompt_size():
    empty_knowledge = {
        "total_successful_patterns": 0,
        "knowledge_reuse_rate": 0.0,
        "rag_contribution_rate": 0.0,
    }
    contributing_retrieval = {
        "candidate_count": 1,
        "accepted_count": 1,
        "used_for_generation": True,
        "retrieval_success_rate": 0.0,
        "retrieval_status": "COMPLETED",
    }

    increased = _rag_automation_metrics(
        [
            {
                "conversion_prompt_tokens": {
                    "baseline_prompt_tokens": 45_000,
                    "actual_prompt_tokens": 52_000,
                    "retrieved_context_tokens": 8_000,
                    "full_context_tokens": 12_000,
                    "accepted_count": 1,
                    "contributed_to_prompt": True,
                }
            }
        ],
        {},
        empty_knowledge,
        contributing_retrieval,
    )
    reduced = _rag_automation_metrics(
        [
            {
                "conversion_prompt_tokens": {
                    "baseline_prompt_tokens": 45_000,
                    "actual_prompt_tokens": 32_000,
                    "retrieved_context_tokens": 4_000,
                    "full_context_tokens": 10_000,
                    "accepted_count": 1,
                    "contributed_to_prompt": True,
                }
            }
        ],
        {},
        empty_knowledge,
        contributing_retrieval,
    )
    unchanged = _rag_automation_metrics(
        [
            {
                "conversion_prompt_tokens": {
                    "baseline_prompt_tokens": 45_000,
                    "actual_prompt_tokens": 45_000,
                    "retrieved_context_tokens": 4_000,
                    "full_context_tokens": 10_000,
                    "accepted_count": 1,
                    "contributed_to_prompt": True,
                }
            }
        ],
        {},
        empty_knowledge,
        contributing_retrieval,
    )

    assert increased.rag_token_impact == 7_000
    assert increased.rag_token_impact_status == "INCREASED_PROMPT_SIZE"
    assert increased.prompt_compression_savings == 4_000
    assert reduced.rag_token_impact == -13_000
    assert reduced.rag_token_impact_status == "REDUCED_PROMPT_SIZE"
    assert reduced.prompt_compression_savings == 6_000
    assert unchanged.rag_token_impact == 0
    assert unchanged.rag_token_impact_status == "NO_CHANGE"

    clamped_compression = _rag_automation_metrics(
        [
            {
                "conversion_prompt_tokens": {
                    "baseline_prompt_tokens": 45_000,
                    "actual_prompt_tokens": 45_000,
                    "retrieved_context_tokens": 4_000,
                    "full_context_tokens": 2_000,
                    "accepted_count": 1,
                    "contributed_to_prompt": True,
                }
            }
        ],
        {},
        empty_knowledge,
        contributing_retrieval,
    )
    assert clamped_compression.prompt_compression_savings == 0


def test_token_savings_are_zero_without_accepted_rag_contribution():
    no_knowledge = {
        "total_successful_patterns": 0,
        "knowledge_reuse_rate": 0.0,
        "rag_contribution_rate": 0.0,
    }

    for retrieval_metrics in (
        {
            "candidate_count": 0,
            "accepted_count": 0,
            "used_for_generation": False,
            "retrieval_status": "NO_CANDIDATES",
        },
        {
            "candidate_count": 1,
            "accepted_count": 0,
            "used_for_generation": False,
            "retrieval_status": "COMPLETED",
        },
        {
            "candidate_count": 1,
            "accepted_count": 1,
            "used_for_generation": False,
            "retrieval_status": "COMPLETED",
        },
    ):
        metrics = _rag_automation_metrics(
            [
                {
                    "conversion_prompt_tokens": {
                        "baseline_prompt_tokens": 45_000,
                        "actual_prompt_tokens": 52_000,
                        "retrieved_context_tokens": 8_000,
                        "full_context_tokens": 132_879,
                    }
                }
            ],
            {},
            no_knowledge,
            retrieval_metrics,
        )

        assert metrics.baseline_total_tokens == 0
        assert metrics.actual_total_tokens == 0
        assert metrics.retrieved_context_tokens == 0
        assert metrics.prompt_compression_savings == 0
        assert metrics.rag_token_impact == 0
        assert metrics.rag_token_impact_status == "NO_CHANGE"
        assert metrics.token_utilization_saved == 0
        assert metrics.token_reduction_percentage == 0
        assert metrics.token_savings_status == "NO_RAG_CONTRIBUTION"


class _RetrievalAuditSession:
    def __init__(self, audits):
        self.audits = audits

    async def scalars(self, _statement):
        return SimpleNamespace(all=lambda: self.audits)


def test_latest_rag_retrieval_metrics_aggregate_latest_audit_per_mapping():
    audits = [
        SimpleNamespace(
            id=3,
            etl_object_id=11,
            candidate_count=8,
            accepted_count=4,
            contributed_to_prompt=True,
        ),
        SimpleNamespace(
            id=2,
            etl_object_id=12,
            candidate_count=5,
            accepted_count=2,
            contributed_to_prompt=False,
        ),
        SimpleNamespace(
            id=1,
            etl_object_id=11,
            candidate_count=20,
            accepted_count=20,
            contributed_to_prompt=True,
        ),
    ]

    metrics = asyncio.run(
        _latest_rag_retrieval_metrics(
            _RetrievalAuditSession(audits),
            "MIG-101",
            [11, 12],
        )
    )

    assert metrics == {
        "candidate_count": 13,
        "accepted_count": 6,
        "used_for_generation": True,
        "retrieval_success_rate": 46.2,
        "retrieval_status": "COMPLETED",
    }
