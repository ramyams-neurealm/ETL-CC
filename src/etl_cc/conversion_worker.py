"""Wave-aware worker for RAG, GPT-4o conversion, critique, retries, and artifacts."""

import asyncio
import hashlib
from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_cc.agents.conversion_agent import (
    ConversionAgent,
    ConversionOutputValidationError,
    ConversionResult,
)
from etl_cc.agents.critique_agent import CritiqueAgent
from etl_cc.agents.rag_retrieval_agent import RAGRetrievalAgent
from etl_cc.config import settings
from etl_cc.database import SessionFactory
from etl_cc.logging_config import configure_logging, log_event, log_exception
from etl_cc.models import (
    AgentResponseETL,
    ArtifactContentETL,
    CanonicalMapping,
    ETLObjectETL,
    GeneratedArtifactETL,
    RepositoryETL,
    WorkflowEventETL,
    WorkflowRunETL,
)


logger = configure_logging("CONVERSION_WORKER")


async def _event(
    session: AsyncSession,
    workflow: WorkflowRunETL,
    event_type: str,
    event_status: str,
    message: str,
    progress: int,
    payload: dict | None = None,
    agent_response_id: int | None = None,
) -> None:
    session.add(
        WorkflowEventETL(
            workflow_run_id=workflow.id,
            agent_response_id=agent_response_id,
            event_type=event_type,
            stage_name="CONVERSION",
            status=event_status,
            progress_percentage=progress,
            message=message,
            event_payload=payload or {},
            actor_type="SYSTEM",
        )
    )
    await session.flush()


async def _claim() -> int | None:
    """Atomically claim one queued Conversion workflow."""
    async with SessionFactory() as session:
        async with session.begin():
            workflow = await session.scalar(
                select(WorkflowRunETL)
                .where(
                    WorkflowRunETL.job_status == "QUEUED",
                    WorkflowRunETL.job_type == "CONVERSION",
                )
                .order_by(
                    WorkflowRunETL.priority,
                    WorkflowRunETL.created_at,
                )
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


async def _next_attempt(
    session: AsyncSession,
    workflow_id: int,
    object_id: int,
    agent_name: str,
    stage: str,
) -> int:
    value = await session.scalar(
        select(
            func.coalesce(
                func.max(AgentResponseETL.attempt_number),
                0,
            )
        ).where(
            AgentResponseETL.workflow_run_id == workflow_id,
            AgentResponseETL.etl_object_id == object_id,
            AgentResponseETL.agent_name == agent_name,
            AgentResponseETL.stage_name == stage,
        )
    )
    return int(value or 0) + 1


async def _audit_start(
    session: AsyncSession,
    workflow: WorkflowRunETL,
    mapping_row: ETLObjectETL,
    agent,
    request_payload: dict,
) -> AgentResponseETL:
    attempt = await _next_attempt(
        session,
        workflow.id,
        mapping_row.id,
        agent.AGENT_NAME,
        agent.STAGE_NAME,
    )
    audit = AgentResponseETL(
        workflow_run_id=workflow.id,
        etl_object_id=mapping_row.id,
        agent_name=agent.AGENT_NAME,
        agent_version=agent.AGENT_VERSION,
        stage_name=agent.STAGE_NAME,
        attempt_number=attempt,
        status="RUNNING",
        request_payload=request_payload,
        response_payload={},
        model_name=getattr(agent, "model_name", agent.MODEL_NAME),
        prompt_name=getattr(agent, "prompt_name", None),
        prompt_version=getattr(agent, "prompt_version", None),
        input_tokens=0,
        output_tokens=0,
        started_at=datetime.now(timezone.utc),
    )
    session.add(audit)
    await session.flush()
    await session.commit()
    log_event(
        logger,
        "AGENT_INPUT",
        workflow_id=workflow.workflow_id,
        repository_id=workflow.repository_id,
        etl_object_id=mapping_row.id,
        mapping_name=mapping_row.object_name,
        agent_name=agent.AGENT_NAME,
        stage_name=agent.STAGE_NAME,
        attempt_number=attempt,
    )
    return audit


async def _audit_complete(
    session: AsyncSession,
    audit: AgentResponseETL,
    agent,
    result,
) -> None:
    audit.status = "COMPLETED"
    audit.response_payload = result.model_dump(mode="json")
    audit.model_name = getattr(agent, "model_name", audit.model_name)
    audit.prompt_name = getattr(agent, "prompt_name", audit.prompt_name)
    audit.prompt_version = getattr(
        agent,
        "prompt_version",
        audit.prompt_version,
    )
    audit.input_tokens = int(getattr(agent, "input_tokens", 0) or 0)
    audit.output_tokens = int(getattr(agent, "output_tokens", 0) or 0)
    audit.completed_at = datetime.now(timezone.utc)
    await session.commit()
    log_event(
        logger,
        "AGENT_OUTPUT",
        workflow_run_id=audit.workflow_run_id,
        etl_object_id=audit.etl_object_id,
        agent_name=audit.agent_name,
        stage_name=audit.stage_name,
        attempt_number=audit.attempt_number,
        model_name=audit.model_name,
        input_tokens=audit.input_tokens,
        output_tokens=audit.output_tokens,
        status=audit.status,
        result_summary=audit.response_payload,
    )


async def _audit_fail(
    session: AsyncSession,
    audit: AgentResponseETL,
    exc: Exception,
    agent=None,
) -> None:
    audit.status = "FAILED"
    audit.error_message = str(exc)[:4000]
    if agent is not None:
        audit.model_name = getattr(agent, "model_name", audit.model_name)
        audit.prompt_name = getattr(agent, "prompt_name", audit.prompt_name)
        audit.prompt_version = getattr(
            agent,
            "prompt_version",
            audit.prompt_version,
        )
        audit.input_tokens = int(getattr(agent, "input_tokens", 0) or 0)
        audit.output_tokens = int(getattr(agent, "output_tokens", 0) or 0)
    audit.completed_at = datetime.now(timezone.utc)
    await session.commit()


async def _latest_payload(
    session: AsyncSession,
    mapping_id: int,
    agent_name: str,
) -> dict:
    record = await session.scalar(
        select(AgentResponseETL)
        .where(
            AgentResponseETL.etl_object_id == mapping_id,
            AgentResponseETL.agent_name == agent_name,
            AgentResponseETL.status == "COMPLETED",
        )
        .order_by(AgentResponseETL.id.desc())
        .limit(1)
    )
    return record.response_payload if record else {}


async def _next_artifact_version(
    session: AsyncSession,
    mapping_id: int,
    artifact_type: str,
) -> int:
    value = await session.scalar(
        select(
            func.coalesce(
                func.max(GeneratedArtifactETL.artifact_version),
                0,
            )
        ).where(
            GeneratedArtifactETL.etl_object_id == mapping_id,
            GeneratedArtifactETL.artifact_type == artifact_type,
        )
    )
    return int(value or 0) + 1


async def _save_artifacts(
    session: AsyncSession,
    workflow: WorkflowRunETL,
    mapping_row: ETLObjectETL,
    result: ConversionResult,
    critique_passed: bool,
) -> None:
    for generated in result.generated_files:
        content_bytes = generated.content.encode("utf-8")
        digest = hashlib.sha256(content_bytes).hexdigest()
        artifact_version = await _next_artifact_version(
            session,
            mapping_row.id,
            generated.artifact_type,
        )
        artifact = GeneratedArtifactETL(
                workflow_run_id=workflow.id,
                etl_object_id=mapping_row.id,
                artifact_type=generated.artifact_type,
                artifact_version=artifact_version,
                file_name=generated.file_name,
                storage_provider="POSTGRESQL",
                storage_path="pending",
                content_hash=digest,
                media_type=generated.media_type,
                validation_status=(
                    "CRITIQUE_APPROVED"
                    if critique_passed
                    else "CRITIQUE_FAILED"
                ),
                is_deployable=False,
            )
        session.add(artifact)
        await session.flush()
        artifact.storage_path = f"postgresql://artifact/{artifact.id}"
        log_event(
            logger,
            "ARTIFACT_STORED_IN_POSTGRESQL",
            workflow_id=workflow.workflow_id,
            repository_id=workflow.repository_id,
            etl_object_id=mapping_row.id,
            mapping_name=mapping_row.object_name,
            artifact_id=artifact.id,
            artifact_type=generated.artifact_type,
            file_name=generated.file_name,
            content_hash=digest,
            content_size_bytes=len(content_bytes),
            line_count=len(generated.content.splitlines()),
        )
        session.add(ArtifactContentETL(
            artifact_id=artifact.id,
            content_text=generated.content,
            encoding="UTF-8",
            content_size_bytes=len(content_bytes),
        ))
    await session.commit()


async def _process_mapping(
    session: AsyncSession,
    workflow: WorkflowRunETL,
    repository: RepositoryETL,
    mapping_row: ETLObjectETL,
) -> None:
    mapping = CanonicalMapping.model_validate(mapping_row.source_definition)
    discovery = await _latest_payload(
        session,
        mapping_row.id,
        "DISCOVERY_AGENT",
    )
    lineage = mapping_row.lineage or {}
    dependencies = mapping_row.dependencies or {}

    rag_agent = RAGRetrievalAgent()
    rag_audit = await _audit_start(
        session,
        workflow,
        mapping_row,
        rag_agent,
        {"mapping": mapping.mapping_name},
    )
    try:
        rag = await rag_agent.run(session, mapping, repository.id)
        await _audit_complete(session, rag_audit, rag_agent, rag)
    except Exception as exc:
        await _audit_fail(session, rag_audit, exc, rag_agent)
        raise

    feedback: list[dict] = []
    final_conversion: ConversionResult | None = None
    final_critique = None
    latest_contract_error: str | None = None

    for attempt_index in range(1, settings.conversion_max_attempts + 1):
        conversion_agent = ConversionAgent()
        conversion_audit = await _audit_start(
            session,
            workflow,
            mapping_row,
            conversion_agent,
            {
                "canonical_mapping": mapping.model_dump(mode="json"),
                "discovery": discovery,
                "lineage": lineage,
                "dependencies": dependencies,
                "rag": rag.model_dump(mode="json"),
                "critique_feedback": feedback,
                "attempt": attempt_index,
            },
        )

        try:
            conversion = await conversion_agent.run(
                mapping=mapping,
                discovery=discovery,
                lineage=lineage,
                dependencies=dependencies,
                rag=rag,
                critique_feedback=feedback,
            )
            await _audit_complete(
                session,
                conversion_audit,
                conversion_agent,
                conversion,
            )
        except ConversionOutputValidationError as exc:
            latest_contract_error = str(exc)
            await _audit_fail(
                session,
                conversion_audit,
                exc,
                conversion_agent,
            )
            feedback = [
                {
                    "category": "OUTPUT_CONTRACT",
                    "severity": "CRITICAL",
                    "description": str(exc),
                    "recommendation": (
                        "Regenerate all three artifacts and comply with the "
                        "pure transform, pytest, and secret-free configuration "
                        "contracts exactly."
                    ),
                }
            ]
            await _event(
                session,
                workflow,
                "CONVERSION_OUTPUT_CONTRACT_RETRY",
                "RETRYING" if attempt_index < settings.conversion_max_attempts else "FAILED",
                (
                    f"Conversion output contract failed for "
                    f"{mapping_row.object_name} on attempt {attempt_index}."
                ),
                20,
                {
                    "etl_object_id": mapping_row.id,
                    "attempt": attempt_index,
                    "maximum_attempts": settings.conversion_max_attempts,
                    "reason": str(exc)[:1000],
                },
                conversion_audit.id,
            )
            await session.commit()
            continue
        except Exception as exc:
            await _audit_fail(
                session,
                conversion_audit,
                exc,
                conversion_agent,
            )
            raise

        critique_agent = CritiqueAgent()
        critique_audit = await _audit_start(
            session,
            workflow,
            mapping_row,
            critique_agent,
            {
                "conversion_agent_response_id": conversion_audit.id,
                "mapping_name": mapping.mapping_name,
                "attempt": attempt_index,
            },
        )
        try:
            critique = await critique_agent.run(
                mapping=mapping,
                discovery=discovery,
                lineage=lineage,
                conversion=conversion,
            )
            await _audit_complete(
                session,
                critique_audit,
                critique_agent,
                critique,
            )
        except Exception as exc:
            await _audit_fail(
                session,
                critique_audit,
                exc,
                critique_agent,
            )
            raise

        final_conversion = conversion
        final_critique = critique

        if critique.decision == "PASS" and not critique.revision_required:
            break

        feedback = [
            item.model_dump(mode="json")
            for item in critique.issues
        ]
        await _event(
            session,
            workflow,
            "CRITIQUE_REVISION_REQUESTED",
            "RETRYING" if attempt_index < settings.conversion_max_attempts else "FAILED",
            (
                f"Critique requested revision for {mapping_row.object_name} "
                f"on attempt {attempt_index}."
            ),
            40,
            {
                "etl_object_id": mapping_row.id,
                "attempt": attempt_index,
                "maximum_attempts": settings.conversion_max_attempts,
                "issue_count": len(feedback),
                "issues": feedback,
            },
            critique_audit.id,
        )
        await session.commit()

    if final_conversion is None or final_critique is None:
        mapping_row.migration_status = "REVIEW_REQUIRED"
        await session.commit()
        raise RuntimeError(
            "Conversion output contract failed after "
            f"{settings.conversion_max_attempts} attempts for "
            f"{mapping_row.object_name}. Last error: "
            f"{latest_contract_error or 'No valid conversion result was produced.'}"
        )

    approved = (
        final_critique.decision == "PASS"
        and not final_critique.revision_required
    )

    await _save_artifacts(
        session,
        workflow,
        mapping_row,
        final_conversion,
        approved,
    )

    mapping_row.migration_status = (
        "CONVERTED" if approved else "REVIEW_REQUIRED"
    )
    await session.commit()

    if not approved:
        raise RuntimeError(
            f"Critique did not approve mapping {mapping_row.object_name} "
            f"after {settings.conversion_max_attempts} attempts."
        )


async def _queue_automatic_validation(
    session: AsyncSession,
    conversion: WorkflowRunETL,
) -> WorkflowRunETL | None:
    """Queue SIMULATE validation for a parent MIG migration."""
    if not conversion.batch_id:
        return None
    parent = await session.scalar(select(WorkflowRunETL).where(
        WorkflowRunETL.workflow_id == conversion.batch_id,
        WorkflowRunETL.job_type == "MIGRATION",
    ))
    if parent is None:
        return None
    options = parent.scope_payload.get("validation_options") or {}
    validation = WorkflowRunETL(
        workflow_id=f"VAL-{uuid4()}",
        batch_id=parent.workflow_id,
        repository_id=conversion.repository_id,
        current_stage="VALIDATION",
        overall_status="QUEUED",
        job_type="VALIDATION",
        job_status="QUEUED",
        scope_payload={
            "etl_object_ids": conversion.scope_payload.get("etl_object_ids", []),
            "conversion_workflow_id": conversion.workflow_id,
            "validation_mode": "STATIC_AND_UNIT_TEST",
            "minimum_functional_parity": float(options.get("minimum_functional_parity", 0.95)),
            "input_mode": "SIMULATE",
            "simulation_options": {
                "target_row_count": int(options.get("target_row_count", 1000)),
                "include_null_cases": True,
                "include_boundary_cases": True,
                "include_negative_cases": True,
                "include_duplicate_cases": False,
            },
            "dataset_files": [],
            "database_tables": None,
        },
        max_attempts=1,
    )
    session.add(validation)
    await session.flush()
    session.add(WorkflowEventETL(
        workflow_run_id=validation.id,
        event_type="VALIDATION_QUEUED",
        stage_name="VALIDATION",
        status="QUEUED",
        progress_percentage=0,
        message="Validation was queued automatically after Conversion and Critique.",
        event_payload={"conversion_workflow_id": conversion.workflow_id},
        actor_type="SYSTEM",
    ))
    parent.current_stage = "VALIDATION"
    parent.scope_payload = {**parent.scope_payload, "validation_workflow_id": validation.workflow_id}
    session.add(WorkflowEventETL(
        workflow_run_id=parent.id,
        event_type="AUTOMATIC_VALIDATION_QUEUED",
        stage_name="MIGRATION",
        status="RUNNING",
        progress_percentage=60,
        message="Conversion and critique completed. Validation was queued.",
        event_payload={"conversion_workflow_id": conversion.workflow_id, "validation_workflow_id": validation.workflow_id},
        actor_type="SYSTEM",
    ))
    return validation


async def _process(workflow_id: int) -> None:
    async with SessionFactory() as session:
        workflow = await session.get(WorkflowRunETL, workflow_id)
        repository = (
            await session.get(RepositoryETL, workflow.repository_id)
            if workflow
            else None
        )
        if workflow is None or repository is None:
            raise RuntimeError(
                "Conversion workflow or repository was not found."
            )

        selected_ids = workflow.scope_payload.get("etl_object_ids", [])
        rows = list(
            (
                await session.scalars(
                    select(ETLObjectETL)
                    .where(
                        ETLObjectETL.id.in_(selected_ids),
                        ETLObjectETL.repository_id == repository.id,
                    )
                    .order_by(
                        ETLObjectETL.migration_wave,
                        ETLObjectETL.id,
                    )
                )
            ).all()
        )
        if len(rows) != len(set(selected_ids)):
            raise RuntimeError(
                "One or more selected mappings were not found."
            )

        total = len(rows)
        for index, mapping_row in enumerate(rows, start=1):
            workflow.current_stage = "CONVERSION"
            mapping_row.migration_status = "CONVERTING"
            await _event(
                session,
                workflow,
                "MAPPING_CONVERSION_STARTED",
                "RUNNING",
                f"Conversion started for {mapping_row.object_name}.",
                int((index - 1) / max(total, 1) * 90),
                {
                    "etl_object_id": mapping_row.id,
                    "migration_wave": mapping_row.migration_wave,
                },
            )
            await session.commit()

            await _process_mapping(
                session,
                workflow,
                repository,
                mapping_row,
            )

            await _event(
                session,
                workflow,
                "MAPPING_CONVERSION_COMPLETED",
                "COMPLETED",
                f"Conversion completed for {mapping_row.object_name}.",
                int(index / max(total, 1) * 90),
                {
                    "etl_object_id": mapping_row.id,
                    "migration_wave": mapping_row.migration_wave,
                },
            )
            await session.commit()

        workflow.current_stage = "CONVERSION_COMPLETED"
        workflow.job_status = "COMPLETED"
        workflow.overall_status = "COMPLETED"
        workflow.failure_stage = None
        workflow.failure_reason = None
        workflow.completed_at = datetime.now(timezone.utc)
        await _event(
            session,
            workflow,
            "CONVERSION_COMPLETED",
            "COMPLETED",
            "Conversion workflow completed.",
            100,
            {"mapping_count": total},
        )
        await _queue_automatic_validation(session, workflow)
        await session.commit()


async def _fail(workflow_id: int, exc: Exception) -> None:
    async with SessionFactory() as session:
        workflow = await session.get(WorkflowRunETL, workflow_id)
        if workflow is None:
            return

        workflow.job_status = "FAILED"
        workflow.overall_status = "FAILED"
        workflow.failure_stage = "CONVERSION"
        workflow.failure_reason = str(exc)[:4000]
        workflow.completed_at = datetime.now(timezone.utc)
        if workflow.batch_id:
            parent = await session.scalar(
                select(WorkflowRunETL).where(
                    WorkflowRunETL.workflow_id == workflow.batch_id
                )
            )
            if parent is not None:
                parent.job_status = "FAILED"
                parent.overall_status = "FAILED"
                parent.current_stage = "CONVERSION_FAILED"
                parent.failure_stage = "CONVERSION"
                parent.failure_reason = str(exc)[:4000]
                parent.completed_at = datetime.now(timezone.utc)

        await _event(
            session,
            workflow,
            "CONVERSION_FAILED",
            "FAILED",
            "Conversion workflow failed.",
            100,
            {
                "error_type": type(exc).__name__,
                "error": str(exc)[:1000],
            },
        )
        await session.commit()


async def run_conversion_worker() -> None:
    """Continuously poll for queued Conversion workflows."""
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
