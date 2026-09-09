"""Wave-aware worker for RAG, GPT-4o conversion, critique, and artifacts."""

import asyncio
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_cc.agents.conversion_agent import ConversionAgent, ConversionResult
from etl_cc.agents.critique_agent import CritiqueAgent
from etl_cc.agents.rag_retrieval_agent import RAGRetrievalAgent
from etl_cc.artifact_store import artifact_store
from etl_cc.config import settings
from etl_cc.database import SessionFactory
from etl_cc.models import (
    AgentResponseETL,
    CanonicalMapping,
    ETLObjectETL,
    GeneratedArtifactETL,
    RepositoryETL,
    WorkflowEventETL,
    WorkflowRunETL,
)


async def _event(session, workflow, event_type, status, message, progress, payload=None, agent_response_id=None):
    session.add(WorkflowEventETL(
        workflow_run_id=workflow.id,
        agent_response_id=agent_response_id,
        event_type=event_type,
        stage_name="CONVERSION",
        status=status,
        progress_percentage=progress,
        message=message,
        event_payload=payload or {},
        actor_type="SYSTEM",
    ))
    await session.flush()


async def _claim() -> int | None:
    async with SessionFactory() as session:
        async with session.begin():
            workflow = await session.scalar(
                select(WorkflowRunETL)
                .where(
                    WorkflowRunETL.job_status == "QUEUED",
                    WorkflowRunETL.job_type == "CONVERSION",
                )
                .order_by(WorkflowRunETL.priority, WorkflowRunETL.created_at)
                .with_for_update(skip_locked=True)
                .limit(1)
            )
            if workflow is None:
                return None
            workflow.job_status = "RUNNING"
            workflow.overall_status = "RUNNING"
            workflow.current_stage = "CONVERSION"
            workflow.started_at = datetime.now(timezone.utc)
            workflow.attempt_count += 1
            return workflow.id


async def _next_attempt(session, workflow_id, object_id, agent_name, stage):
    value = await session.scalar(
        select(func.coalesce(func.max(AgentResponseETL.attempt_number), 0)).where(
            AgentResponseETL.workflow_run_id == workflow_id,
            AgentResponseETL.etl_object_id == object_id,
            AgentResponseETL.agent_name == agent_name,
            AgentResponseETL.stage_name == stage,
        )
    )
    return int(value or 0) + 1


async def _audit_start(session, workflow, row, agent, request_payload):
    attempt = await _next_attempt(session, workflow.id, row.id, agent.AGENT_NAME, agent.STAGE_NAME)
    audit = AgentResponseETL(
        workflow_run_id=workflow.id,
        etl_object_id=row.id,
        agent_name=agent.AGENT_NAME,
        agent_version=agent.AGENT_VERSION,
        stage_name=agent.STAGE_NAME,
        attempt_number=attempt,
        status="RUNNING",
        request_payload=request_payload,
        response_payload={},
        model_name=agent.MODEL_NAME,
        prompt_name=getattr(agent, "PROMPT_NAME", None),
        prompt_version=getattr(agent, "PROMPT_VERSION", None),
        input_tokens=0,
        output_tokens=0,
        started_at=datetime.now(timezone.utc),
    )
    session.add(audit)
    await session.flush()
    await session.commit()
    return audit


async def _audit_complete(session, audit, agent, result):
    audit.status = "COMPLETED"
    audit.response_payload = result.model_dump(mode="json")
    audit.model_name = getattr(agent, "model_name", audit.model_name)
    audit.input_tokens = int(getattr(agent, "input_tokens", 0) or 0)
    audit.output_tokens = int(getattr(agent, "output_tokens", 0) or 0)
    audit.completed_at = datetime.now(timezone.utc)
    await session.commit()


async def _audit_fail(session, audit, exc):
    audit.status = "FAILED"
    audit.error_message = str(exc)[:4000]
    audit.completed_at = datetime.now(timezone.utc)
    await session.commit()


async def _latest_payload(session, row_id, agent_name):
    record = await session.scalar(
        select(AgentResponseETL)
        .where(
            AgentResponseETL.etl_object_id == row_id,
            AgentResponseETL.agent_name == agent_name,
            AgentResponseETL.status == "COMPLETED",
        )
        .order_by(AgentResponseETL.id.desc())
        .limit(1)
    )
    return record.response_payload if record else {}


async def _save_artifacts(session, workflow, row, result: ConversionResult, critique_passed: bool):
    for generated in result.generated_files:
        path, digest = artifact_store.save_text(
            workflow_id=workflow.workflow_id,
            mapping_name=row.object_name,
            file_name=generated.file_name,
            content=generated.content,
        )
        session.add(GeneratedArtifactETL(
            workflow_run_id=workflow.id,
            etl_object_id=row.id,
            artifact_type=generated.artifact_type,
            artifact_version=1,
            file_name=generated.file_name,
            storage_provider="LOCAL",
            storage_path=str(path),
            content_hash=digest,
            media_type=generated.media_type,
            validation_status="CRITIQUE_APPROVED" if critique_passed else "CRITIQUE_FAILED",
            is_deployable=False,
        ))
    await session.commit()


async def _process_mapping(session, workflow, repository, row):
    mapping = CanonicalMapping.model_validate(row.source_definition)
    discovery = await _latest_payload(session, row.id, "DISCOVERY_AGENT")
    lineage = row.lineage or {}
    dependencies = row.dependencies or {}

    rag_agent = RAGRetrievalAgent()
    rag_audit = await _audit_start(session, workflow, row, rag_agent, {"mapping": mapping.mapping_name})
    try:
        rag = await rag_agent.run(session, mapping, repository.id)
        await _audit_complete(session, rag_audit, rag_agent, rag)
    except Exception as exc:
        await _audit_fail(session, rag_audit, exc)
        raise

    feedback = []
    final_conversion = None
    final_critique = None
    for _ in range(settings.conversion_max_attempts):
        conversion_agent = ConversionAgent()
        conversion_audit = await _audit_start(session, workflow, row, conversion_agent, {
            "canonical_mapping": mapping.model_dump(mode="json"),
            "discovery": discovery,
            "lineage": lineage,
            "dependencies": dependencies,
            "rag": rag.model_dump(mode="json"),
            "critique_feedback": feedback,
        })
        try:
            conversion = await conversion_agent.run(
                mapping=mapping,
                discovery=discovery,
                lineage=lineage,
                dependencies=dependencies,
                rag=rag,
                critique_feedback=feedback,
            )
            await _audit_complete(session, conversion_audit, conversion_agent, conversion)
        except Exception as exc:
            await _audit_fail(session, conversion_audit, exc)
            raise

        critique_agent = CritiqueAgent()
        critique_audit = await _audit_start(session, workflow, row, critique_agent, {
            "conversion_agent_response_id": conversion_audit.id,
            "mapping_name": mapping.mapping_name,
        })
        try:
            critique = await critique_agent.run(
                mapping=mapping,
                discovery=discovery,
                lineage=lineage,
                conversion=conversion,
            )
            await _audit_complete(session, critique_audit, critique_agent, critique)
        except Exception as exc:
            await _audit_fail(session, critique_audit, exc)
            raise

        final_conversion = conversion
        final_critique = critique
        if critique.decision == "PASS" and not critique.revision_required:
            break
        feedback = [item.model_dump(mode="json") for item in critique.issues]

    if final_conversion is None or final_critique is None:
        raise RuntimeError("Conversion did not produce a result.")
    approved = final_critique.decision == "PASS" and not final_critique.revision_required
    await _save_artifacts(session, workflow, row, final_conversion, approved)
    row.migration_status = "CONVERTED" if approved else "REVIEW_REQUIRED"
    await session.commit()
    if not approved:
        raise RuntimeError(f"Critique did not approve mapping {row.object_name}.")


async def _process(workflow_id: int):
    async with SessionFactory() as session:
        workflow = await session.get(WorkflowRunETL, workflow_id)
        repository = await session.get(RepositoryETL, workflow.repository_id) if workflow else None
        if workflow is None or repository is None:
            raise RuntimeError("Conversion workflow or repository was not found.")
        ids = workflow.scope_payload.get("etl_object_ids", [])
        rows = list((await session.scalars(
            select(ETLObjectETL)
            .where(ETLObjectETL.id.in_(ids), ETLObjectETL.repository_id == repository.id)
            .order_by(ETLObjectETL.migration_wave, ETLObjectETL.id)
        )).all())
        if len(rows) != len(set(ids)):
            raise RuntimeError("One or more selected mappings were not found.")
        total = len(rows)
        for index, row in enumerate(rows, 1):
            workflow.current_stage = "CONVERSION"
            row.migration_status = "CONVERTING"
            await _event(session, workflow, "MAPPING_CONVERSION_STARTED", "RUNNING", f"Conversion started for {row.object_name}.", int((index - 1) / total * 90), {"etl_object_id": row.id, "migration_wave": row.migration_wave})
            await session.commit()
            await _process_mapping(session, workflow, repository, row)
            await _event(session, workflow, "MAPPING_CONVERSION_COMPLETED", "COMPLETED", f"Conversion completed for {row.object_name}.", int(index / total * 90), {"etl_object_id": row.id, "migration_wave": row.migration_wave})
            await session.commit()
        workflow.current_stage = "CONVERSION_COMPLETED"
        workflow.job_status = "COMPLETED"
        workflow.overall_status = "COMPLETED"
        workflow.completed_at = datetime.now(timezone.utc)
        await _event(session, workflow, "CONVERSION_COMPLETED", "COMPLETED", "Conversion workflow completed.", 100, {"mapping_count": total})
        await session.commit()


async def _fail(workflow_id, exc):
    async with SessionFactory() as session:
        workflow = await session.get(WorkflowRunETL, workflow_id)
        if workflow:
            workflow.job_status = "FAILED"
            workflow.overall_status = "FAILED"
            workflow.failure_stage = "CONVERSION"
            workflow.failure_reason = str(exc)[:4000]
            workflow.completed_at = datetime.now(timezone.utc)
            await _event(session, workflow, "CONVERSION_FAILED", "FAILED", "Conversion workflow failed.", 100, {"error_type": type(exc).__name__})
            await session.commit()


async def run_conversion_worker():
    while True:
        workflow_id = await _claim()
        if workflow_id is None:
            await asyncio.sleep(settings.worker_poll_seconds)
            continue
        try:
            await _process(workflow_id)
        except Exception as exc:
            await _fail(workflow_id, exc)


if __name__ == "__main__":
    asyncio.run(run_conversion_worker())
