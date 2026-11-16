"""Automatic capture of validated ETL conversion patterns for RAG."""
from __future__ import annotations

import hashlib
import json
import logging
import re
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_cc.embedding_service import EmbeddingService
from etl_cc.models import (
    ArtifactContentETL,
    CanonicalMapping,
    ETLObjectETL,
    GeneratedArtifactETL,
    KnowledgeBaseETL,
    WorkflowRunETL,
)

logger = logging.getLogger(__name__)
MAX_EMBEDDING_DOCUMENT_CHARS = 3000


def _short_text(value: Any, limit: int = 80) -> str:
    """Return bounded ASCII text so the embedding document has a hard size cap."""
    text = " ".join(str(value or "").split())
    return text.encode("ascii", errors="replace").decode("ascii")[:limit]


def _generated_code_summary(code: str) -> str:
    """Summarize generated code without placing its full source in the embedding."""
    functions = re.findall(r"^\s*(?:async\s+)?def\s+([A-Za-z_]\w*)", code, re.MULTILINE)
    classes = re.findall(r"^\s*class\s+([A-Za-z_]\w*)", code, re.MULTILINE)
    imports = re.findall(r"^\s*(?:from\s+\S+\s+import\s+\S+|import\s+\S+)", code, re.MULTILINE)
    preview = _short_text(code, 240)
    summary = (
        f"lines={len(code.splitlines())}; "
        f"functions={functions[:8]}; classes={classes[:8]}; "
        f"imports={imports[:8]}; preview={preview}"
    )
    return _short_text(summary, 400)


def _embedding_document(
    *,
    source_platform: Any,
    target_platform: Any,
    mapping_name: Any,
    complexity: Any,
    transformation_types: list[str],
    transformation_count: int,
    business_rules: list[dict[str, Any]],
    confidence_score: float,
    generated_code: str,
) -> dict[str, Any]:
    """Build a compact, bounded embedding representation of a full knowledge record."""
    document = {
        "source_platform": _short_text(source_platform),
        "target_platform": _short_text(target_platform),
        "mapping_name": _short_text(mapping_name),
        "complexity": _short_text(complexity),
        "transformation_types": [
            _short_text(item) for item in transformation_types[:8]
        ],
        "transformation_count": transformation_count,
        "business_rule_summaries": [
            _short_text(item.get("description") or item.get("rule_type"))
            for item in business_rules[:8]
            if isinstance(item, dict)
        ],
        "confidence_score": confidence_score,
        "generated_code_summary": _generated_code_summary(generated_code),
    }
    serialized = json.dumps(document, sort_keys=True, separators=(",", ":"))
    if len(serialized) > MAX_EMBEDDING_DOCUMENT_CHARS:
        raise ValueError(
            "The compact embedding document exceeded its character budget."
        )
    return document


def _confidence_score(validation_result: dict[str, Any]) -> tuple[float, int]:
    """Return the raw confidence and its bounded integer percentage."""
    raw_value = validation_result.get("confidence_index")
    confidence = float(raw_value) if raw_value is not None else 0.0
    percentage = confidence * 100.0 if confidence <= 1.0 else confidence
    return confidence, max(0, min(100, round(percentage)))


async def _artifact_text(
    session: AsyncSession,
    artifact: GeneratedArtifactETL,
) -> str:
    """Load persisted generated artifact text from the content table."""
    content = await session.get(ArtifactContentETL, artifact.id)
    if content is None:
        raise RuntimeError(
            f"Generated artifact content is missing for artifact {artifact.id}."
        )
    if content.content_text is not None:
        return content.content_text
    if content.content_json is not None:
        return json.dumps(content.content_json, sort_keys=True, indent=2)
    if content.content_binary is not None:
        return bytes(content.content_binary).decode(content.encoding or "UTF-8")
    raise RuntimeError(f"Generated artifact {artifact.id} has no content.")


async def capture_successful_conversion_knowledge(
    session: AsyncSession,
    validation_workflow: WorkflowRunETL,
    mapping: ETLObjectETL,
    validation_result: dict[str, Any],
    *,
    trigger: str,
) -> KnowledgeBaseETL | None:
    """Persist and embed a knowledge pattern only for a passed, deployable result.

    The content hash makes validation and deployment triggers idempotent when
    they capture the same conversion output and validation result.
    """
    if (
        validation_result.get("status") != "PASSED"
        or validation_result.get("deployable") is not True
    ):
        return None

    conversion_workflow_id = (
        validation_workflow.scope_payload or {}
    ).get("conversion_workflow_id")
    if not conversion_workflow_id:
        raise RuntimeError(
            "Cannot capture successful conversion knowledge without its "
            "conversion workflow ID."
        )
    conversion_workflow = await session.scalar(
        select(WorkflowRunETL).where(
            WorkflowRunETL.workflow_id == conversion_workflow_id,
            WorkflowRunETL.job_type == "CONVERSION",
            WorkflowRunETL.repository_id == mapping.repository_id,
        )
    )
    if conversion_workflow is None:
        raise RuntimeError(
            f"Conversion workflow {conversion_workflow_id} was not found."
        )

    canonical = CanonicalMapping.model_validate(mapping.source_definition)
    artifact = await session.scalar(
        select(GeneratedArtifactETL)
        .where(
            GeneratedArtifactETL.workflow_run_id == conversion_workflow.id,
            GeneratedArtifactETL.etl_object_id == mapping.id,
            GeneratedArtifactETL.artifact_type == "PYSPARK_CODE",
        )
        .order_by(
            GeneratedArtifactETL.artifact_version.desc(),
            GeneratedArtifactETL.id.desc(),
        )
        .limit(1)
    )
    if artifact is None:
        raise RuntimeError(
            f"Generated PySpark code is missing for mapping {mapping.id}."
        )
    generated_code = await _artifact_text(session, artifact)

    confidence, confidence_percentage = _confidence_score(validation_result)
    conversion_settings = conversion_workflow.scope_payload or {}
    target_platform = conversion_settings.get("target_platform")
    target_framework = conversion_settings.get("target_framework")
    business_rules = mapping.business_rules or [
        item.model_dump(mode="json") for item in canonical.business_rules
    ]
    transformations = [
        item.model_dump(mode="json") for item in canonical.transformations
    ]
    migration_id = (
        conversion_workflow.batch_id or validation_workflow.batch_id
    )
    transformation_types = sorted(
        {
            item.transformation_type
            for item in canonical.transformations
            if item.transformation_type
        }
    )
    embedding_document = _embedding_document(
        source_platform=canonical.source_vendor,
        target_platform=target_platform,
        mapping_name=mapping.object_name,
        complexity=mapping.complexity,
        transformation_types=transformation_types,
        transformation_count=len(transformations),
        business_rules=business_rules,
        confidence_score=confidence,
        generated_code=generated_code,
    )
    validation_evidence = dict(validation_result)
    document = {
        "knowledge_type": "SUCCESSFUL_CONVERSION",
        "source_platform": canonical.source_vendor,
        "target_platform": target_platform,
        "mapping_name": mapping.object_name,
        "complexity": mapping.complexity,
        "transformation_count": len(transformations),
        "transformations": transformations,
        "transformation_types": transformation_types,
        "business_rules": business_rules,
        "lineage": mapping.lineage or {},
        "generated_code": generated_code,
        "validation_results": validation_evidence,
        "confidence_score": confidence,
        "repository_id": mapping.repository_id,
        "migration_id": migration_id,
        "etl_object_id": mapping.id,
    }
    content = json.dumps(document, sort_keys=True, indent=2, default=str)
    content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
    metadata = {
        "source_platform": canonical.source_vendor,
        "target_platform": target_platform,
        "source_vendor": canonical.source_vendor,
        "target_framework": target_framework,
        "complexity": mapping.complexity,
        "transformation_count": len(transformations),
        "transformation_types": transformation_types,
        "business_rule_types": sorted(
            {
                str(item.get("rule_type"))
                for item in business_rules
                if isinstance(item, dict) and item.get("rule_type")
            }
        ),
        "repository_id": mapping.repository_id,
        "migration_id": migration_id,
        "etl_object_id": mapping.id,
        "trigger": trigger,
        "confidence_score": confidence,
        "confidence_score_percentage": confidence_percentage,
        "embedding_document": embedding_document,
    }
    knowledge = await session.scalar(
        select(KnowledgeBaseETL).where(
            KnowledgeBaseETL.content_hash == content_hash
        )
    )
    if knowledge is None:
        knowledge = KnowledgeBaseETL(
            repository_id=mapping.repository_id,
            etl_object_id=mapping.id,
            workflow_run_id=validation_workflow.id,
            knowledge_type="SUCCESSFUL_CONVERSION",
            title=mapping.object_name,
            content=content,
            metadata_payload=metadata,
            content_hash=content_hash,
            approval_status="APPROVED",
            confidence_score=confidence_percentage,
            source_agent="AUTOMATIC_KNOWLEDGE_CAPTURE",
        )
        session.add(knowledge)
    else:
        knowledge.repository_id = mapping.repository_id
        knowledge.etl_object_id = mapping.id
        knowledge.workflow_run_id = validation_workflow.id
        knowledge.knowledge_type = "SUCCESSFUL_CONVERSION"
        knowledge.title = mapping.object_name
        knowledge.content = content
        knowledge.metadata_payload = metadata
        knowledge.approval_status = "APPROVED"
        knowledge.confidence_score = confidence_percentage
        knowledge.source_agent = "AUTOMATIC_KNOWLEDGE_CAPTURE"

    await session.flush()
    await session.commit()

    await EmbeddingService().embed_knowledge(knowledge)
    await session.commit()
    if knowledge.embedding_status != "COMPLETED":
        logger.error(
            "SUCCESSFUL_CONVERSION_KNOWLEDGE_EMBEDDING_FAILED | "
            "knowledge_id=%s | mapping_id=%s | trigger=%s | error=%s",
            knowledge.id,
            mapping.id,
            trigger,
            knowledge.embedding_error,
        )
    else:
        logger.info(
            "SUCCESSFUL_CONVERSION_KNOWLEDGE_CAPTURED | "
            "knowledge_id=%s | mapping_id=%s | trigger=%s",
            knowledge.id,
            mapping.id,
            trigger,
        )
    return knowledge
