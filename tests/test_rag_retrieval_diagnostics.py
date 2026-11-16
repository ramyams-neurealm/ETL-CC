import asyncio
import logging
from contextlib import asynccontextmanager
from types import SimpleNamespace

from etl_cc.agents.rag_retrieval_agent import RAGRetrievalAgent
from etl_cc.config import settings
from etl_cc.embedding_service import EmbeddingService
from etl_cc.models import CanonicalMapping, CanonicalTransformation
from etl_cc.services import _rag_api_response


def test_embedding_service_returns_input_token_usage(monkeypatch):
    class FakeOpenAI:
        def __init__(self, *, api_key):
            assert api_key == "test-key"
            self.embeddings = self

        def create(self, **_kwargs):
            return SimpleNamespace(
                data=[
                    SimpleNamespace(
                        embedding=[0.1] * settings.rag_embedding_dimensions
                    )
                ],
                usage=SimpleNamespace(prompt_tokens=27, total_tokens=27),
            )

    monkeypatch.setattr(
        "etl_cc.embedding_service.key_vault_service.get_openai_key",
        lambda _operation_name: "test-key",
    )
    monkeypatch.setattr("etl_cc.embedding_service.OpenAI", FakeOpenAI)

    embedding, input_tokens = asyncio.run(
        EmbeddingService().embed_text_with_usage(
            "mapping details",
            "rag-query:test",
        )
    )

    assert len(embedding) == settings.rag_embedding_dimensions
    assert input_tokens == 27


def test_ab_initio_vendor_metadata_contributes_to_rag_score():
    mapping = CanonicalMapping(
        source_vendor="AB_INITIO",
        source_object_key="orders/graph",
        mapping_name="g_orders",
        transformations=[
            CanonicalTransformation(
                name="ROUTE_ORDERS",
                transformation_type="ROUTER",
            )
        ],
    )
    knowledge = SimpleNamespace(
        metadata_payload={
            "source_vendor": "AB_INITIO",
            "target_framework": "DATABRICKS PYSPARK",
            "transformation_types": ["ROUTER"],
        }
    )

    assert RAGRetrievalAgent._metadata_score(mapping, knowledge) == 1.0


def test_rag_api_diagnostics_expose_result_and_embedding_usage():
    audit = SimpleNamespace(
        response_payload={
            "status": "NO_MATCH",
            "eligible_knowledge_count": 1,
            "candidate_scores": [
                {
                    "knowledge_id": 12,
                    "accepted": False,
                    "rejection_reasons": ["BELOW_MIN_FINAL_SIMILARITY"],
                }
            ],
        },
        status="COMPLETED",
        input_tokens=27,
    )

    response = _rag_api_response(audit)

    assert response["status"] == "NO_MATCH"
    assert response["audit_status"] == "COMPLETED"
    assert response["embedding_input_tokens"] == 27
    assert response["eligible_knowledge_count"] == 1
    assert response["candidate_scores"][0]["accepted"] is False


def test_rag_result_explains_candidate_rejected_by_final_threshold(monkeypatch):
    class FakeEmbeddingService:
        def mapping_text(self, _mapping):
            return "mapping details"

        async def embed_text_with_usage(self, *_args):
            return [0.1] * settings.rag_embedding_dimensions, 31

    class FakeSession:
        def __init__(self):
            self.audit_event = None

        async def scalar(self, _statement):
            return 1

        async def execute(self, _statement):
            candidate = SimpleNamespace(
                id=42,
                title="Unrelated transform guidance",
                knowledge_type="GUIDANCE",
                content="This guidance is not sufficiently relevant.",
                metadata_payload={},
            )
            return SimpleNamespace(all=lambda: [(candidate, 0.433095)])

        @asynccontextmanager
        async def begin_nested(self):
            yield

        def add(self, event):
            self.audit_event = event

        async def flush(self):
            return None

    monkeypatch.setattr(
        "etl_cc.agents.rag_retrieval_agent.EmbeddingService",
        FakeEmbeddingService,
    )
    monkeypatch.setattr(settings, "rag_min_vector_similarity", 0.5)
    monkeypatch.setattr(settings, "rag_min_similarity", 0.65)

    mapping = CanonicalMapping(
        source_vendor="AB_INITIO",
        source_object_key="orders/graph",
        mapping_name="g_orders",
        transformations=[
            CanonicalTransformation(
                name="ROUTE_ORDERS",
                transformation_type="ROUTER",
            )
        ],
    )
    agent = RAGRetrievalAgent()
    session = FakeSession()
    result = asyncio.run(
        agent.run(
            session,
            mapping,
            repository_id=7,
            workflow_run_id=9,
            etl_object_id=11,
        )
    )

    assert result.status == "NO_MATCH"
    assert result.candidate_count == 1
    assert result.eligible_knowledge_count == 1
    assert result.retrieval_count == 0
    assert result.accepted_count == 0
    assert result.embedding_input_tokens == 31
    assert result.candidate_scores[0].rejection_reasons == [
        "BELOW_MIN_FINAL_SIMILARITY"
    ]
    assert agent.input_tokens == 31
    assert session.audit_event.accepted_count == 0


def test_rag_prioritizes_successful_conversion_patterns(monkeypatch):
    class FakeEmbeddingService:
        def mapping_text(self, _mapping):
            return "mapping details"

        async def embed_text_with_usage(self, *_args):
            return [0.1] * settings.rag_embedding_dimensions, 10

    class FakeSession:
        async def scalar(self, _statement):
            return 2

        async def execute(self, _statement):
            common_metadata = {
                "source_vendor": "AB_INITIO",
                "target_framework": "DATABRICKS PYSPARK",
                "transformation_types": ["ROUTER"],
            }
            successful_pattern = SimpleNamespace(
                id=1,
                title="Successful routing conversion",
                knowledge_type="SUCCESSFUL_CONVERSION",
                content="Known-good routed conversion pattern.",
                metadata_payload=common_metadata,
            )
            general_guidance = SimpleNamespace(
                id=2,
                title="General routing guidance",
                knowledge_type="GUIDANCE",
                content="General guidance for routing.",
                metadata_payload=common_metadata,
            )
            return SimpleNamespace(
                all=lambda: [
                    (successful_pattern, 0.15),
                    (general_guidance, 0.05),
                ]
            )

        @asynccontextmanager
        async def begin_nested(self):
            yield

        def add(self, _event):
            return None

        async def flush(self):
            return None

    monkeypatch.setattr(
        "etl_cc.agents.rag_retrieval_agent.EmbeddingService",
        FakeEmbeddingService,
    )
    monkeypatch.setattr(settings, "rag_min_vector_similarity", 0.5)
    monkeypatch.setattr(settings, "rag_min_similarity", 0.65)

    mapping = CanonicalMapping(
        source_vendor="AB_INITIO",
        source_object_key="orders/graph",
        mapping_name="g_orders",
        transformations=[
            CanonicalTransformation(
                name="ROUTE_ORDERS",
                transformation_type="ROUTER",
            )
        ],
    )
    result = asyncio.run(
        RAGRetrievalAgent().run(FakeSession(), mapping, repository_id=7)
    )

    assert result.retrieval_count == 2
    assert result.retrieved_items[0].knowledge_type == "SUCCESSFUL_CONVERSION"
    assert result.retrieved_items[0].knowledge_id == 1


def test_rag_debug_logs_each_failed_eligibility_filter(monkeypatch, caplog):
    class FakeEmbeddingService:
        def mapping_text(self, _mapping):
            return "mapping details"

        async def embed_text_with_usage(self, *_args):
            return [0.1] * settings.rag_embedding_dimensions, 10

    class FakeSession:
        def __init__(self):
            self.execute_calls = 0

        async def scalar(self, _statement):
            return 0

        async def execute(self, _statement):
            self.execute_calls += 1
            if self.execute_calls == 1:
                row = SimpleNamespace(
                    id=5,
                    approval_status="APPROVED",
                    approval_pass=True,
                    embedding_status="COMPLETED",
                    embedding_status_pass=True,
                    embedding_present=True,
                    embedding_model=settings.rag_embedding_deployment,
                    embedding_model_pass=True,
                    embedding_content_hash="stale-hash",
                    embedding_hash_pass=False,
                    repository_id=7,
                    repository_scope_pass=True,
                )
                return SimpleNamespace(all=lambda: [row])
            return SimpleNamespace(all=lambda: [])

        @asynccontextmanager
        async def begin_nested(self):
            yield

        def add(self, _event):
            return None

        async def flush(self):
            return None

    monkeypatch.setattr(
        "etl_cc.agents.rag_retrieval_agent.EmbeddingService",
        FakeEmbeddingService,
    )
    mapping = CanonicalMapping(
        source_vendor="AB_INITIO",
        source_object_key="orders/graph",
        mapping_name="g_orders",
    )

    with caplog.at_level(
        logging.DEBUG,
        logger="etl_cc.agents.rag_retrieval_agent",
    ):
        result = asyncio.run(
            RAGRetrievalAgent().run(FakeSession(), mapping, repository_id=7)
        )

    assert result.eligible_knowledge_count == 0
    diagnostic = next(
        record.message
        for record in caplog.records
        if "RAG_ELIGIBILITY_DIAGNOSTIC" in record.message
    )
    assert "knowledge_id=5" in diagnostic
    assert "embedding_hash_matches=False" in diagnostic
    assert "failed_filters=['embedding_content_hash_matches']" in diagnostic
