"""Azure OpenAI embedding generation with per-call Key Vault credentials."""
from __future__ import annotations
import asyncio
import hashlib
from datetime import datetime, timezone
from typing import Any
from openai import OpenAI
from etl_cc.config import settings
from etl_cc.key_vault_service import key_vault_service

class EmbeddingService:
    MODEL_NAME = "text-embedding-3-small"

    async def embed_text(self, text: str, operation_name: str) -> list[float]:
        normalized = " ".join(str(text or "").split())
        if not normalized:
            raise ValueError("Embedding text cannot be empty.")
        key = await asyncio.to_thread(key_vault_service.get_openai_key, operation_name)
        def invoke() -> list[float]:
            client = OpenAI(api_key=key)
            response = client.embeddings.create(
                model=settings.rag_embedding_deployment,
                input=normalized,
                dimensions=settings.rag_embedding_dimensions,
            )
            return list(response.data[0].embedding)
        vector = await asyncio.to_thread(invoke)
        if len(vector) != settings.rag_embedding_dimensions:
            raise ValueError(f"Embedding dimension {len(vector)} does not match configured {settings.rag_embedding_dimensions}.")
        return vector

    @staticmethod
    def knowledge_text(row: Any) -> str:
        metadata = row.metadata_payload or {}
        return "\n".join([
            f"Knowledge type: {row.knowledge_type}",
            f"Title: {row.title}",
            f"Source vendor: {metadata.get('source_vendor', '')}",
            f"Target framework: {metadata.get('target_framework', '')}",
            f"Transformation types: {metadata.get('transformation_types', [])}",
            f"Business rules: {metadata.get('business_rule_types', [])}",
            f"Content: {row.content}",
        ])

    @staticmethod
    def mapping_text(mapping: Any) -> str:
        transformations = [str(x.transformation_type) for x in mapping.transformations]
        expressions = [str(p.expression) for x in mapping.transformations for p in x.ports if p.expression]
        return "\n".join([
            "Source vendor: Informatica PowerCenter",
            "Target framework: PySpark",
            f"Mapping: {mapping.mapping_name}",
            f"Sources: {[x.name for x in mapping.sources]}",
            f"Targets: {[x.name for x in mapping.targets]}",
            f"Transformation types: {transformations}",
            f"Expressions: {expressions}",
        ])

    async def embed_knowledge(self, row: Any) -> bool:
        if row.embedding is not None and row.embedding_status == "COMPLETED" and row.embedding_content_hash == row.content_hash:
            return False
        try:
            row.embedding = await self.embed_text(self.knowledge_text(row), f"rag-knowledge:{row.id}")
            row.embedding_model = settings.rag_embedding_deployment
            row.embedding_status = "COMPLETED"
            row.embedding_content_hash = row.content_hash
            row.embedding_error = None
            row.embedded_at = datetime.now(timezone.utc)
            return True
        except Exception as exc:
            row.embedding_status = "FAILED"
            row.embedding_error = f"{type(exc).__name__}: {exc}"[:4000]
            row.embedded_at = None
            return False
