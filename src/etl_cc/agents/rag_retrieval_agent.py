"""Deterministic approved-knowledge retrieval for conversion context."""

from pydantic import BaseModel, Field
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_cc.config import settings
from etl_cc.models import CanonicalMapping, KnowledgeBaseETL


class RetrievedKnowledge(BaseModel):
    knowledge_id: int
    knowledge_type: str
    title: str
    content: str
    relevance_score: float = Field(ge=0.0, le=1.0)
    metadata_payload: dict = Field(default_factory=dict)


class RAGRetrievalResult(BaseModel):
    mapping_name: str
    retrieved_items: list[RetrievedKnowledge] = Field(default_factory=list)
    retrieval_count: int
    confidence: float = Field(ge=0.0, le=1.0)


class RAGRetrievalAgent:
    AGENT_NAME = "RAG_RETRIEVAL_AGENT"
    AGENT_VERSION = "1.0.0"
    STAGE_NAME = "KNOWLEDGE_RETRIEVAL"
    MODEL_NAME = "DETERMINISTIC_KEYWORD"

    async def run(
        self,
        session: AsyncSession,
        mapping: CanonicalMapping,
        repository_id: int,
    ) -> RAGRetrievalResult:
        rows = list((await session.scalars(
            select(KnowledgeBaseETL).where(
                KnowledgeBaseETL.approval_status == "APPROVED",
                or_(
                    KnowledgeBaseETL.repository_id.is_(None),
                    KnowledgeBaseETL.repository_id == repository_id,
                ),
            )
        )).all())
        terms = {
            mapping.mapping_name.lower(),
            "pyspark",
            "informatica",
            *(item.transformation_type.lower() for item in mapping.transformations),
        }
        scored = []
        for row in rows:
            haystack = " ".join([
                row.knowledge_type,
                row.title,
                row.content,
                str(row.metadata_payload or {}),
            ]).lower()
            hits = sum(term in haystack for term in terms if term)
            if hits:
                scored.append((min(1.0, hits / max(len(terms), 1)), row))
        scored.sort(key=lambda item: (-item[0], item[1].id))
        items = [
            RetrievedKnowledge(
                knowledge_id=row.id,
                knowledge_type=row.knowledge_type,
                title=row.title,
                content=row.content,
                relevance_score=round(score, 3),
                metadata_payload=row.metadata_payload or {},
            )
            for score, row in scored[: settings.rag_max_items]
        ]
        confidence = max((item.relevance_score for item in items), default=0.0)
        return RAGRetrievalResult(
            mapping_name=mapping.mapping_name,
            retrieved_items=items,
            retrieval_count=len(items),
            confidence=confidence,
        )
