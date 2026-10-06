"""Backfill embeddings for approved knowledge records."""
import asyncio
from sqlalchemy import or_, select
from etl_cc.database import SessionFactory
from etl_cc.embedding_service import EmbeddingService
from etl_cc.models import KnowledgeBaseETL
from etl_cc.config import settings

async def main():
    service = EmbeddingService()
    async with SessionFactory() as session:
        rows = list((await session.scalars(select(KnowledgeBaseETL).where(KnowledgeBaseETL.approval_status == "APPROVED", or_(KnowledgeBaseETL.embedding.is_(None), KnowledgeBaseETL.embedding_status.in_(["PENDING", "FAILED", "STALE"])) ).limit(settings.rag_embedding_batch_size))).all())
        completed = 0
        for row in rows:
            completed += int(await service.embed_knowledge(row))
        await session.commit()
        print({"selected": len(rows), "completed": completed})

if __name__ == "__main__":
    asyncio.run(main())
