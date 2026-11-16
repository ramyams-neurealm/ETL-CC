"""Approved vector RAG retrieval with deterministic metadata reranking."""
from __future__ import annotations
import hashlib
import logging
import re
import time
from pydantic import BaseModel, Field
from sqlalchemy import case, func, or_, select
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

class RAGCandidateScore(BaseModel):
    knowledge_id: int
    title: str
    vector_similarity: float = Field(ge=0.0, le=1.0)
    metadata_score: float = Field(ge=0.0, le=1.0)
    relevance_score: float = Field(ge=0.0, le=1.0)
    accepted: bool
    retrieved: bool
    rejection_reasons: list[str] = Field(default_factory=list)

class RAGRetrievalResult(BaseModel):
    mapping_name: str
    status: str
    embedding_model: str
    retrieved_items: list[RetrievedKnowledge] = Field(default_factory=list)
    candidate_count: int = 0
    eligible_knowledge_count: int = 0
    retrieval_count: int = 0
    accepted_count: int = 0
    confidence: float = Field(ge=0.0, le=1.0)
    average_similarity: float = Field(ge=0.0, le=1.0)
    retrieval_duration_ms: float = 0.0
    contributed_to_prompt: bool = False
    embedding_input_tokens: int = 0
    context_tokens: int = 0
    candidate_scores: list[RAGCandidateScore] = Field(default_factory=list)
    thresholds: dict[str, float] = Field(default_factory=dict)
    eligible_filters: list[str] = Field(default_factory=list)
    context_tokens_basis: str = "APPROX_CHARACTERS_DIVIDED_BY_4"

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
        mapping_vendor = cls._normalize_term(mapping.source_vendor)
        vendor_match = bool(mapping_vendor and mapping_vendor in vendor)
        return round(min(1.0, 0.6 * overlap + 0.2 * ("pyspark" in framework) + 0.2 * vendor_match), 4)

    async def run(self, session: AsyncSession, mapping: CanonicalMapping, repository_id: int, *, workflow_run_id: int | None = None, etl_object_id: int | None = None) -> RAGRetrievalResult:
        started = time.perf_counter()
        self.input_tokens = 0
        self.output_tokens = 0
        embedding_service = EmbeddingService()
        query_text = embedding_service.mapping_text(mapping)
        query_embedding, embedding_input_tokens = (
            await embedding_service.embed_text_with_usage(
                query_text,
                f"rag-query:{mapping.source_object_key}",
            )
        )
        self.input_tokens = embedding_input_tokens
        distance = KnowledgeBaseETL.embedding.cosine_distance(query_embedding)
        eligible_filters = (
            KnowledgeBaseETL.approval_status == "APPROVED",
            KnowledgeBaseETL.embedding_status == "COMPLETED",
            KnowledgeBaseETL.embedding.is_not(None),
            KnowledgeBaseETL.embedding_model == settings.rag_embedding_deployment,
            KnowledgeBaseETL.embedding_content_hash == KnowledgeBaseETL.content_hash,
            or_(
                KnowledgeBaseETL.repository_id.is_(None),
                KnowledgeBaseETL.repository_id == repository_id,
            ),
        )
        if logger.isEnabledFor(logging.DEBUG):
            scope_filter = or_(
                KnowledgeBaseETL.repository_id.is_(None),
                KnowledgeBaseETL.repository_id == repository_id,
            )
            diagnostics = (
                await session.execute(
                    select(
                        KnowledgeBaseETL.id,
                        KnowledgeBaseETL.approval_status,
                        (
                            KnowledgeBaseETL.approval_status == "APPROVED"
                        ).label("approval_pass"),
                        KnowledgeBaseETL.embedding_status,
                        (
                            KnowledgeBaseETL.embedding_status == "COMPLETED"
                        ).label("embedding_status_pass"),
                        KnowledgeBaseETL.embedding.is_not(None).label(
                            "embedding_present"
                        ),
                        KnowledgeBaseETL.embedding_model,
                        (
                            KnowledgeBaseETL.embedding_model
                            == settings.rag_embedding_deployment
                        ).label("embedding_model_pass"),
                        KnowledgeBaseETL.embedding_content_hash,
                        (
                            KnowledgeBaseETL.embedding_content_hash
                            == KnowledgeBaseETL.content_hash
                        ).label("embedding_hash_pass"),
                        KnowledgeBaseETL.repository_id,
                        scope_filter.label("repository_scope_pass"),
                    )
                    .where(
                        KnowledgeBaseETL.knowledge_type
                        == "SUCCESSFUL_CONVERSION"
                    )
                    .order_by(KnowledgeBaseETL.id)
                )
            ).all()
            filter_names = (
                ("approval_status", "approval_pass"),
                ("embedding_status", "embedding_status_pass"),
                ("embedding_present", "embedding_present"),
                ("embedding_model", "embedding_model_pass"),
                ("embedding_content_hash_matches", "embedding_hash_pass"),
                ("repository_scope", "repository_scope_pass"),
            )
            for row in diagnostics:
                failed_filters = [
                    name
                    for name, result_name in filter_names
                    if not bool(getattr(row, result_name))
                ]
                logger.debug(
                    "RAG_ELIGIBILITY_DIAGNOSTIC | repository_id=%s | "
                    "knowledge_id=%s | approval_status=%s | "
                    "embedding_status=%s | embedding_present=%s | "
                    "embedding_model=%s | embedding_hash_matches=%s | "
                    "knowledge_repository_id=%s | repository_scope_pass=%s | "
                    "failed_filters=%s",
                    repository_id,
                    row.id,
                    row.approval_status,
                    row.embedding_status,
                    row.embedding_present,
                    row.embedding_model,
                    row.embedding_hash_pass,
                    row.repository_id,
                    row.repository_scope_pass,
                    failed_filters or "NONE",
                )
        statement = (
            select(KnowledgeBaseETL, distance.label("distance"))
            .where(*eligible_filters)
            .order_by(
                case(
                    (
                        KnowledgeBaseETL.knowledge_type
                        == "SUCCESSFUL_CONVERSION",
                        0,
                    ),
                    else_=1,
                ),
                distance,
            )
            .limit(settings.rag_candidate_count)
        )
        eligible_count_statement = select(func.count(KnowledgeBaseETL.id)).where(
            *eligible_filters
        )
        eligible_knowledge_count = int(
            await session.scalar(eligible_count_statement) or 0
        )
        candidates = list((await session.execute(statement)).all())
        ranked = []
        candidate_similarities: list[float] = []
        candidate_scores: list[RAGCandidateScore] = []
        for row, raw_distance in candidates:
            similarity = max(0.0, min(1.0, 1.0 - float(raw_distance)))
            candidate_similarities.append(similarity)
            metadata_score = self._metadata_score(mapping, row)
            final_score = max(0.0, min(1.0, settings.rag_vector_weight * similarity + settings.rag_metadata_weight * metadata_score))
            accepted = similarity >= settings.rag_min_vector_similarity and final_score >= settings.rag_min_similarity
            rejection_reasons = []
            if similarity < settings.rag_min_vector_similarity:
                rejection_reasons.append("BELOW_MIN_VECTOR_SIMILARITY")
            if final_score < settings.rag_min_similarity:
                rejection_reasons.append("BELOW_MIN_FINAL_SIMILARITY")
            candidate_scores.append(
                RAGCandidateScore(
                    knowledge_id=row.id,
                    title=row.title,
                    vector_similarity=round(similarity, 4),
                    metadata_score=round(metadata_score, 4),
                    relevance_score=round(final_score, 4),
                    accepted=accepted,
                    retrieved=False,
                    rejection_reasons=rejection_reasons,
                )
            )
            logger.info(
                "RAG_CANDIDATE_SCORED | mapping=%s | knowledge_id=%s | vector_similarity=%.4f | metadata_score=%.4f | final_score=%.4f | vector_threshold=%.4f | final_threshold=%.4f | accepted=%s",
                mapping.mapping_name, row.id, similarity, metadata_score, final_score,
                settings.rag_min_vector_similarity, settings.rag_min_similarity, accepted,
            )
            if accepted:
                ranked.append((final_score, similarity, metadata_score, row))
        ranked.sort(
            key=lambda item: (
                item[3].knowledge_type != "SUCCESSFUL_CONVERSION",
                -item[0],
                item[3].id,
            )
        )
        ranked = ranked[: settings.rag_max_items]
        retrieved_ids = {row.id for _, _, _, row in ranked}
        candidate_scores = [
            item.model_copy(
                update={"retrieved": item.knowledge_id in retrieved_ids}
            )
            for item in candidate_scores
        ]
        items = [RetrievedKnowledge(knowledge_id=row.id, knowledge_type=row.knowledge_type, title=row.title, content=row.content, relevance_score=round(score, 4), vector_similarity=round(similarity, 4), metadata_score=round(metadata_score, 4), metadata_payload=row.metadata_payload or {}) for score, similarity, metadata_score, row in ranked]
        elapsed = round((time.perf_counter() - started) * 1000, 3)
        confidence = max((x.relevance_score for x in items), default=0.0)
        top_candidate_similarity = max(candidate_similarities, default=0.0)
        average = sum(candidate_similarities) / len(candidate_similarities) if candidate_similarities else 0.0
        context_tokens = max(len(" ".join(x.content for x in items)) // 4, 0)
        result = RAGRetrievalResult(
            mapping_name=mapping.mapping_name,
            status="CONTRIBUTED" if items else "NO_MATCH",
            embedding_model=settings.rag_embedding_deployment,
            retrieved_items=items,
            candidate_count=len(candidates),
            eligible_knowledge_count=eligible_knowledge_count,
            retrieval_count=len(items),
            accepted_count=sum(item.accepted for item in candidate_scores),
            confidence=round(confidence, 4),
            average_similarity=round(average, 4),
            retrieval_duration_ms=elapsed,
            contributed_to_prompt=bool(items),
            embedding_input_tokens=embedding_input_tokens,
            context_tokens=context_tokens,
            candidate_scores=candidate_scores,
            thresholds={
                "min_vector_similarity": settings.rag_min_vector_similarity,
                "min_relevance_score": settings.rag_min_similarity,
                "vector_weight": settings.rag_vector_weight,
                "metadata_weight": settings.rag_metadata_weight,
                "candidate_limit": float(settings.rag_candidate_count),
                "max_retrieved_items": float(settings.rag_max_items),
            },
            eligible_filters=[
                "approval_status=APPROVED",
                "embedding_status=COMPLETED",
                "embedding IS NOT NULL",
                f"embedding_model={settings.rag_embedding_deployment}",
                "embedding_content_hash=content_hash",
                "repository_id IS NULL OR repository_id matches the mapping repository",
            ],
        )
        try:
            async with session.begin_nested():
                session.add(RAGRetrievalEventETL(workflow_run_id=workflow_run_id, repository_id=repository_id, etl_object_id=etl_object_id, query_hash=hashlib.sha256(query_text.encode()).hexdigest(), embedding_model=settings.rag_embedding_deployment, candidate_count=len(candidates), retrieved_count=len(items), accepted_count=result.accepted_count, top_similarity=top_candidate_similarity if candidate_similarities else None, average_similarity=average if candidate_similarities else None, retrieval_duration_ms=elapsed, context_tokens=context_tokens, contributed_to_prompt=bool(items), calculation_basis="BASELINE_NOT_CONFIGURED"))
                await session.flush()
        except Exception:
            logger.exception("RAG_RETRIEVAL_AUDIT_PERSISTENCE_FAILED | repository_id=%s | etl_object_id=%s | mapping=%s", repository_id, etl_object_id, mapping.mapping_name)
        return result
