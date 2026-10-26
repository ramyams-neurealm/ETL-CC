"""Approved vector RAG retrieval with deterministic metadata reranking."""
from __future__ import annotations
import hashlib
import logging
import re
import time
from pydantic import BaseModel, Field
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from etl_cc.config import settings
from etl_cc.embedding_service import EmbeddingService
from etl_cc.models import CanonicalMapping, KnowledgeBaseETL, RAGRetrievalEventETL

logger = logging.getLogger(__name__)

class RetrievedKnowledge(BaseModel):
    knowledge_id: int
    knowledge_type: str
    title: str
    content: str
    relevance_score: float = Field(ge=0.0, le=1.0)
    vector_similarity: float = Field(ge=0.0, le=1.0)
    metadata_score: float = Field(ge=0.0, le=1.0)
    metadata_payload: dict = Field(default_factory=dict)

class RAGRetrievalResult(BaseModel):
    mapping_name: str
    status: str
    retrieved_items: list[RetrievedKnowledge] = Field(default_factory=list)
    candidate_count: int = 0
    retrieval_count: int = 0
    accepted_count: int = 0
    confidence: float = Field(ge=0.0, le=1.0)
    average_similarity: float = Field(ge=0.0, le=1.0)
    retrieval_duration_ms: float = 0.0
    contributed_to_prompt: bool = False

class RAGRetrievalAgent:
    AGENT_NAME = "RAG_RETRIEVAL_AGENT"
    AGENT_VERSION = "2.1.0"
    STAGE_NAME = "KNOWLEDGE_RETRIEVAL"
    MODEL_NAME = "PGVECTOR_HYBRID"

    @staticmethod
    def _normalize_term(value: object) -> str:
        return re.sub(r"[^a-z0-9]+", " ", str(value or "").casefold()).strip()

    @classmethod
    def _metadata_score(cls, mapping: CanonicalMapping, row: KnowledgeBaseETL) -> float:
        metadata = row.metadata_payload or {}
        wanted = {
            cls._normalize_term(item.transformation_type)
            for item in mapping.transformations
            if item.transformation_type
        }
        available = {
            cls._normalize_term(item)
            for item in metadata.get("transformation_types", [])
            if item
        }
        overlap = len(wanted & available) / max(len(wanted), 1)
        framework = cls._normalize_term(metadata.get("target_framework"))
        vendor = cls._normalize_term(metadata.get("source_vendor"))
        return round(min(1.0, 0.6 * overlap + 0.2 * ("pyspark" in framework) + 0.2 * ("informatica" in vendor)), 4)

    async def run(self, session: AsyncSession, mapping: CanonicalMapping, repository_id: int, *, workflow_run_id: int | None = None, etl_object_id: int | None = None) -> RAGRetrievalResult:
        started = time.perf_counter()
        embedding_service = EmbeddingService()
        query_text = embedding_service.mapping_text(mapping)
        query_embedding = await embedding_service.embed_text(query_text, f"rag-query:{mapping.source_object_key}")
        distance = KnowledgeBaseETL.embedding.cosine_distance(query_embedding)
        statement = (
            select(KnowledgeBaseETL, distance.label("distance"))
            .where(
                KnowledgeBaseETL.approval_status == "APPROVED",
                KnowledgeBaseETL.embedding_status == "COMPLETED",
                KnowledgeBaseETL.embedding.is_not(None),
                or_(KnowledgeBaseETL.repository_id.is_(None), KnowledgeBaseETL.repository_id == repository_id),
            )
            .order_by(distance)
            .limit(settings.rag_candidate_count)
        )
        candidates = list((await session.execute(statement)).all())
        ranked = []
        candidate_similarities: list[float] = []
        for row, raw_distance in candidates:
            similarity = max(0.0, min(1.0, 1.0 - float(raw_distance)))
            candidate_similarities.append(similarity)
            metadata_score = self._metadata_score(mapping, row)
            final_score = max(0.0, min(1.0, settings.rag_vector_weight * similarity + settings.rag_metadata_weight * metadata_score))
            accepted = similarity >= settings.rag_min_vector_similarity and final_score >= settings.rag_min_similarity
            logger.info(
                "RAG_CANDIDATE_SCORED | mapping=%s | knowledge_id=%s | vector_similarity=%.4f | metadata_score=%.4f | final_score=%.4f | vector_threshold=%.4f | final_threshold=%.4f | accepted=%s",
                mapping.mapping_name, row.id, similarity, metadata_score, final_score,
                settings.rag_min_vector_similarity, settings.rag_min_similarity, accepted,
            )
            if accepted:
                ranked.append((final_score, similarity, metadata_score, row))
        ranked.sort(key=lambda item: (-item[0], item[3].id))
        ranked = ranked[: settings.rag_max_items]
        items = [RetrievedKnowledge(knowledge_id=row.id, knowledge_type=row.knowledge_type, title=row.title, content=row.content, relevance_score=round(score, 4), vector_similarity=round(similarity, 4), metadata_score=round(metadata_score, 4), metadata_payload=row.metadata_payload or {}) for score, similarity, metadata_score, row in ranked]
        elapsed = round((time.perf_counter() - started) * 1000, 3)
        confidence = max((x.relevance_score for x in items), default=0.0)
        top_candidate_similarity = max(candidate_similarities, default=0.0)
        average = sum(candidate_similarities) / len(candidate_similarities) if candidate_similarities else 0.0
        result = RAGRetrievalResult(mapping_name=mapping.mapping_name, status="CONTRIBUTED" if items else "NO_MATCH", retrieved_items=items, candidate_count=len(candidates), retrieval_count=len(items), accepted_count=len(items), confidence=round(confidence, 4), average_similarity=round(average, 4), retrieval_duration_ms=elapsed, contributed_to_prompt=bool(items))
        try:
            async with session.begin_nested():
                session.add(RAGRetrievalEventETL(workflow_run_id=workflow_run_id, repository_id=repository_id, etl_object_id=etl_object_id, query_hash=hashlib.sha256(query_text.encode()).hexdigest(), embedding_model=settings.rag_embedding_deployment, candidate_count=len(candidates), retrieved_count=len(items), accepted_count=len(items), top_similarity=top_candidate_similarity if candidate_similarities else None, average_similarity=average if candidate_similarities else None, retrieval_duration_ms=elapsed, context_tokens=max(len(" ".join(x.content for x in items)) // 4, 0), contributed_to_prompt=bool(items), calculation_basis="BASELINE_NOT_CONFIGURED"))
                await session.flush()
        except Exception:
            logger.exception("RAG_RETRIEVAL_AUDIT_PERSISTENCE_FAILED | repository_id=%s | etl_object_id=%s | mapping=%s", repository_id, etl_object_id, mapping.mapping_name)
        return result
