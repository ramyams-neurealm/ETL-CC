"""Queue worker for consolidated NeuFlow Zero-Touch Validation."""

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_cc.agents.validation_agent import (
    ValidationAgent,
    ValidationContext,
    ValidationResult,
    ValidationStageResult,
)
from etl_cc.config import settings
from etl_cc.validation_store import validation_store
from etl_cc.connectors import load_postgresql_table
from etl_cc.security import validation_credential_cipher
from etl_cc.models import ValidationDatabaseConnectionETL
from etl_cc.database import SessionFactory
from etl_cc.logging_config import configure_logging, log_event, log_exception
from etl_cc.models import (
    AgentResponseETL,
    ArtifactContentETL,
    CanonicalMapping,
    ETLObjectETL,
    GeneratedArtifactETL,
    WorkflowEventETL,
    WorkflowRunETL,
)
from etl_cc.validation_test_case_service import (
    replace_planned_test_cases,
    upsert_test_case,
)


logger = configure_logging("VALIDATION_WORKER")


async def _claim() -> int | None:
    """Atomically claim one queued Validation workflow."""
    async with SessionFactory() as session:
        async with session.begin():
            workflow = await session.scalar(
                select(WorkflowRunETL)
                .where(
                    WorkflowRunETL.job_status == "QUEUED",
                    WorkflowRunETL.job_type == "VALIDATION",
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
            workflow.current_stage = "VALIDATION"
            workflow.failure_stage = None
            workflow.failure_reason = None
            workflow.started_at = datetime.now(timezone.utc)
            workflow.attempt_count += 1
            return workflow.id


async def _event(
    session: AsyncSession,
    workflow: WorkflowRunETL,
    event_type: str,
    event_status: str,
    message: str,
    progress: int,
    payload: dict[str, Any] | None = None,
    agent_response_id: int | None = None,
) -> None:
    session.add(
        WorkflowEventETL(
            workflow_run_id=workflow.id,
            agent_response_id=agent_response_id,
            event_type=event_type,
            stage_name="VALIDATION",
            status=event_status,
            progress_percentage=progress,
            message=message,
            event_payload=payload or {},
            actor_type="SYSTEM",
        )
    )
    await session.flush()


async def _next_attempt(
    session: AsyncSession,
    workflow_run_id: int,
    etl_object_id: int,
    agent_name: str,
    stage_name: str,
) -> int:
    value = await session.scalar(
        select(
            func.coalesce(
                func.max(AgentResponseETL.attempt_number),
                0,
            )
        ).where(
            AgentResponseETL.workflow_run_id == workflow_run_id,
            AgentResponseETL.etl_object_id == etl_object_id,
            AgentResponseETL.agent_name == agent_name,
            AgentResponseETL.stage_name == stage_name,
        )
    )
    return int(value or 0) + 1


async def _audit_stage(
    session: AsyncSession,
    workflow: WorkflowRunETL,
    mapping: ETLObjectETL,
    stage: ValidationStageResult,
) -> AgentResponseETL:
    """Persist one internal ValidationAgent stage."""
    attempt = await _next_attempt(
        session,
        workflow.id,
        mapping.id,
        stage.agent_name,
        stage.stage_name,
    )
    now = datetime.now(timezone.utc)
    audit = AgentResponseETL(
        workflow_run_id=workflow.id,
        etl_object_id=mapping.id,
        agent_name=stage.agent_name,
        agent_version=stage.agent_version,
        stage_name=stage.stage_name,
        attempt_number=attempt,
        status=stage.status,
        request_payload={
            "etl_object_id": mapping.id,
            "mapping_name": mapping.object_name,
        },
        response_payload=stage.response_payload,
        model_name=stage.model_name,
        prompt_name=stage.prompt_name,
        prompt_version=stage.prompt_version,
        input_tokens=stage.input_tokens,
        output_tokens=stage.output_tokens,
        error_message=stage.error_message,
        started_at=now,
        completed_at=now,
    )
    session.add(audit)
    await session.flush()
    log_event(
        logger,
        "VALIDATION_STAGE_OUTPUT",
        workflow_id=workflow.workflow_id,
        repository_id=workflow.repository_id,
        etl_object_id=mapping.id,
        mapping_name=mapping.object_name,
        agent_name=stage.agent_name,
        stage_name=stage.stage_name,
        status=stage.status,
        input_tokens=stage.input_tokens,
        output_tokens=stage.output_tokens,
        response_payload=stage.response_payload,
        error_message=stage.error_message,
    )
    return audit


async def _latest_discovery(
    session: AsyncSession,
    mapping_id: int,
) -> dict[str, Any]:
    row = await session.scalar(
        select(AgentResponseETL)
        .where(
            AgentResponseETL.etl_object_id == mapping_id,
            AgentResponseETL.agent_name == "DISCOVERY_AGENT",
            AgentResponseETL.status == "COMPLETED",
        )
        .order_by(AgentResponseETL.id.desc())
        .limit(1)
    )
    return row.response_payload if row else {}


async def _conversion_artifacts(
    session: AsyncSession,
    conversion_workflow_id: str,
    repository_id: int,
    mapping_id: int,
) -> dict[str, GeneratedArtifactETL]:
    """Load artifacts from the successful requested Conversion workflow.

    GeneratedArtifactETL.validation_status is mutable after Validation. It must
    not be used to find the immutable output of a successful Conversion.
    """
    conversion = await session.scalar(
        select(WorkflowRunETL).where(
            WorkflowRunETL.workflow_id == conversion_workflow_id,
            WorkflowRunETL.repository_id == repository_id,
            WorkflowRunETL.job_type == "CONVERSION",
            WorkflowRunETL.job_status == "COMPLETED",
            WorkflowRunETL.overall_status == "COMPLETED",
        )
    )
    if conversion is None:
        raise RuntimeError(
            "A completed Conversion workflow for this repository was not found."
        )

    rows = list(
        (
            await session.scalars(
                select(GeneratedArtifactETL)
                .where(
                    GeneratedArtifactETL.workflow_run_id == conversion.id,
                    GeneratedArtifactETL.etl_object_id == mapping_id,
                )
                .order_by(GeneratedArtifactETL.id.desc())
            )
        ).all()
    )
    selected: dict[str, GeneratedArtifactETL] = {}
    for row in rows:
        selected.setdefault(row.artifact_type, row)

    required = {"PYSPARK_CODE", "UNIT_TEST", "CONFIGURATION"}
    missing = required - set(selected)
    if missing:
        raise RuntimeError(
            "Required Conversion artifacts are missing: "
            + ", ".join(sorted(missing))
        )
    return selected


async def _persist_planned_cases(
    session: AsyncSession,
    workflow: WorkflowRunETL,
    mapping: ETLObjectETL,
    result: ValidationResult,
) -> None:
    plan_stage = next(
        (
            stage
            for stage in result.stages
            if stage.agent_name == "TEST_PLAN_AGENT"
        ),
        None,
    )
    if plan_stage is None:
        return

    raw_cases = plan_stage.response_payload.get("test_cases", [])
    if not raw_cases:
        return

    class PlannedCase:
        def __init__(self, payload: dict[str, Any]) -> None:
            self.test_id = payload["test_id"]
            self.test_name = (
                payload.get("test_name")
                or payload.get("title")
                or payload["test_id"]
            )
            self.category = payload.get("category", "GENERAL")
            self.severity = payload.get("severity", "HIGH")
            self.expected_result = payload.get("expected_result", "")
            self.evidence_payload = payload.get("evidence_payload", {})

    await replace_planned_test_cases(
        session,
        workflow.id,
        mapping.id,
        [PlannedCase(item) for item in raw_cases],
    )


async def _persist_result(
    session: AsyncSession,
    workflow: WorkflowRunETL,
    mapping: ETLObjectETL,
    result: ValidationResult,
) -> None:
    """Persist stage audits and all detailed tests returned by ValidationAgent."""
    await _persist_planned_cases(session, workflow, mapping, result)

    audit_by_agent: dict[str, AgentResponseETL] = {}
    for stage in result.stages:
        audit_by_agent[stage.agent_name] = await _audit_stage(
            session,
            workflow,
            mapping,
            stage,
        )

    summary_stage = ValidationStageResult(
        agent_name="VALIDATION_AGENT",
                agent_version=ValidationAgent.AGENT_VERSION,
        stage_name="ZERO_TOUCH_VALIDATION",
        model_name="HYBRID_GPT4O_DETERMINISTIC",
        prompt_name=None,
        prompt_version=None,
        input_tokens=0,
        output_tokens=0,
        status="COMPLETED" if result.status == "PASSED" else "FAILED",
        response_payload=result.model_dump(mode="json"),
        error_message=result.failure_reason,
    )
    audit_by_agent[summary_stage.agent_name] = await _audit_stage(
        session, workflow, mapping, summary_stage
    )

    category_agent = {
        "STATIC_VALIDATION": "STATIC_VALIDATION_AGENT",
        "UNIT_TEST": "UNIT_TEST_EXECUTION_AGENT",
        "SYNTHETIC_DATA": "SYNTHETIC_DATA_VALIDATOR",
        "FUNCTIONAL_PARITY": "FUNCTIONAL_PARITY_AGENT",
        "DATA_PARITY": "MATCHFLOW_COMPARATOR",
    }

    for test in result.test_cases:
        audit = audit_by_agent.get(category_agent.get(test.category, ""))
        await upsert_test_case(
            session,
            workflow_run_id=workflow.id,
            etl_object_id=mapping.id,
            agent_response_id=audit.id if audit else None,
            test_id=test.test_id,
            test_name=test.test_name,
            category=test.category,
            status=test.status,
            severity=test.severity,
            expected_result=test.expected_result or "",
            actual_result=test.actual_result or "",
            details=test.details or "",
            evidence_payload=test.evidence_payload,
            duration_seconds=test.duration_seconds,
        )


async def _process(workflow_id: int) -> None:
    """Run consolidated ValidationAgent for each selected mapping."""
    async with SessionFactory() as session:
        workflow = await session.get(WorkflowRunETL, workflow_id)
        if workflow is None:
            raise RuntimeError("Validation workflow was not found.")

        mapping_ids = workflow.scope_payload.get("etl_object_ids", [])
        conversion_workflow_id = workflow.scope_payload.get(
            "conversion_workflow_id"
        )
        if not conversion_workflow_id:
            raise RuntimeError("conversion_workflow_id is required.")

        minimum = float(
            workflow.scope_payload.get(
                "minimum_functional_parity",
                settings.minimum_functional_parity,
            )
        )
        mappings = list(
            (
                await session.scalars(
                    select(ETLObjectETL)
                    .where(
                        ETLObjectETL.id.in_(mapping_ids),
                        ETLObjectETL.repository_id == workflow.repository_id,
                    )
                    .order_by(
                        ETLObjectETL.migration_wave,
                        ETLObjectETL.id,
                    )
                )
            ).all()
        )
        if not mappings:
            raise RuntimeError("No mappings were selected for Validation.")
        if len(mappings) != len(set(mapping_ids)):
            raise RuntimeError(
                "One or more selected mappings were not found in the repository."
            )

        all_passed = True
        failure_reasons: list[str] = []

        for index, mapping in enumerate(mappings, start=1):
            mapping.migration_status = "VALIDATING"
            await _event(
                session,
                workflow,
                "MAPPING_VALIDATION_STARTED",
                "RUNNING",
                f"Validation started for {mapping.object_name}.",
                int((index - 1) / len(mappings) * 90),
                {
                    "etl_object_id": mapping.id,
                    "migration_wave": mapping.migration_wave,
                },
            )
            await session.commit()

            artifact_rows = await _conversion_artifacts(
                session,
                conversion_workflow_id,
                workflow.repository_id,
                mapping.id,
            )
            temporary_directory = TemporaryDirectory(
                prefix=f"etlcc-{workflow.workflow_id}-{mapping.id}-",
                dir="/dev/shm" if Path("/dev/shm").is_dir() else None,
            )
            artifact_paths = {}
            for artifact_type, row in artifact_rows.items():
                content_row = await session.get(ArtifactContentETL, row.id)
                if content_row is None:
                    raise RuntimeError(
                            f"Artifact content is missing for artifact {row.id}."
                        )
                if content_row.content_text is not None:
                    content = content_row.content_text
                elif content_row.content_json is not None:
                    import json
                    content = json.dumps(content_row.content_json, indent=2, sort_keys=True)
                elif content_row.content_binary is not None:
                    content = bytes(content_row.content_binary).decode(content_row.encoding or "UTF-8")
                else:
                    raise RuntimeError(f"Artifact {row.id} has no content value.")
                path = Path(temporary_directory.name) / Path(row.file_name).name
                path.write_text(content, encoding="utf-8")
                artifact_paths[artifact_type] = path

            canonical = CanonicalMapping.model_validate(
                mapping.source_definition
            )
            discovery = await _latest_discovery(session, mapping.id)

            input_mode = workflow.scope_payload.get("input_mode", "SIMULATE")
            input_datasets = {}
            expected_output_rows = None
            requested_row_count = int((workflow.scope_payload.get("simulation_options") or {}).get("target_row_count", settings.validation_simulation_default_rows))
            if input_mode == "DATASET_FILE":
                for item in workflow.scope_payload.get("dataset_files", []):
                    input_datasets[item["dataset_name"]] = validation_store.load_upload(item["upload_id"])
            elif input_mode == "DATABASE_TABLES":
                specification = workflow.scope_payload.get("database_tables") or {}
                for item in specification.get("source_tables", []):
                    connection_row = await session.get(ValidationDatabaseConnectionETL, item["connection_id"])
                    if connection_row is None: raise RuntimeError("Validation database connection was not found.")
                    if connection_row.database_type != "POSTGRESQL": raise RuntimeError("UNSUPPORTED_DATABASE_TYPE")
                    password = validation_credential_cipher().decrypt(connection_row.credential_ciphertext)
                    input_datasets[item["dataset_name"]] = await load_postgresql_table(connection_row.connection_config, password, item["schema_name"], item["table_name"], int(specification.get("row_limit", settings.validation_database_row_limit)))
                target_spec = specification.get("target_table")
                if specification.get("validation_type") == "TABLE_TO_TABLE_COMPARE" and target_spec:
                    connection_row = await session.get(ValidationDatabaseConnectionETL, target_spec["connection_id"])
                    password = validation_credential_cipher().decrypt(connection_row.credential_ciphertext)
                    expected_output_rows = await load_postgresql_table(connection_row.connection_config,password,target_spec["schema_name"],target_spec["table_name"],int(specification.get("row_limit",settings.validation_database_row_limit)))

            log_event(
                logger,
                "VALIDATION_INPUT",
                workflow_id=workflow.workflow_id,
                repository_id=workflow.repository_id,
                etl_object_id=mapping.id,
                mapping_name=mapping.object_name,
                input_mode=input_mode,
                requested_row_count=requested_row_count,
                canonical_mapping=canonical.model_dump(mode="json"),
                discovery=discovery,
                lineage=mapping.lineage or {},
                artifact_types=sorted(artifact_paths),
            )
            try:
                result = await ValidationAgent().run(
                    ValidationContext(
                        workflow_id=workflow.workflow_id,
                        mapping=canonical,
                        discovery=discovery,
                        lineage=mapping.lineage or {},
                        artifact_paths=artifact_paths,
                        minimum_functional_parity=minimum,
                        input_mode=input_mode,
                        input_datasets=input_datasets,
                        expected_output_rows=expected_output_rows,
                        requested_row_count=requested_row_count,
                    )
                )
            finally:
                temporary_directory.cleanup()
            await _persist_result(
                session,
                workflow,
                mapping,
                result,
            )
            log_event(
                logger,
                "VALIDATION_OUTPUT",
                workflow_id=workflow.workflow_id,
                repository_id=workflow.repository_id,
                etl_object_id=mapping.id,
                mapping_name=mapping.object_name,
                validation_result=result.model_dump(mode="json"),
            )

            passed = result.status == "PASSED" and result.deployable
            all_passed = all_passed and passed
            mapping.migration_status = (
                "VALIDATED" if passed else "VALIDATION_FAILED"
            )

            for artifact in artifact_rows.values():
                # Keep CRITIQUE_APPROVED after failure so approved Conversion
                # artifacts can be validated again. Promote only on success.
                if passed:
                    artifact.validation_status = "VALIDATED"
                artifact.is_deployable = passed

            if not passed:
                failure_reasons.append(
                    f"{mapping.object_name}: "
                    f"{result.failure_reason or 'Validation failed.'}"
                )

            await _event(
                session,
                workflow,
                "MAPPING_VALIDATION_COMPLETED",
                "COMPLETED" if passed else "FAILED",
                (
                    f"Validation {'passed' if passed else 'failed'} "
                    f"for {mapping.object_name}."
                ),
                int(index / len(mappings) * 90),
                {
                    "etl_object_id": mapping.id,
                    "passed": passed,
                    "deployable": result.deployable,
                    "match_percentage": result.match_percentage,
                    "rows_compared": result.rows_compared,
                    "confidence_index": result.confidence_index,
                    "failure_reason": result.failure_reason,
                    "recommendation": result.recommendation,
                    "data_files": result.data_files,
                },
            )
            await session.commit()

        workflow.current_stage = "VALIDATION_COMPLETED"
        workflow.job_status = "COMPLETED" if all_passed else "FAILED"
        workflow.overall_status = "COMPLETED" if all_passed else "FAILED"
        workflow.failure_stage = None if all_passed else "VALIDATION"
        workflow.failure_reason = (
            None if all_passed else " | ".join(failure_reasons)[:4000]
        )
        workflow.completed_at = datetime.now(timezone.utc)
        await _event(
            session,
            workflow,
            "VALIDATION_COMPLETED",
            workflow.overall_status,
            "Validation workflow completed.",
            100,
            {"all_passed": all_passed, "mapping_count": len(mappings)},
        )
        if workflow.batch_id:
            parent = await session.scalar(select(WorkflowRunETL).where(
                WorkflowRunETL.workflow_id == workflow.batch_id,
                WorkflowRunETL.job_type == "MIGRATION",
            ))
            if parent is not None:
                auto_deploy = bool(
                    all_passed
                    and settings.auto_deploy_after_validation
                    and settings.git_deployment_repository_url
                    and settings.git_deployment_access_token
                )
                if auto_deploy:
                    parent.current_stage = "DEPLOYMENT_QUEUED"
                    parent.overall_status = "RUNNING"
                    parent.job_status = "QUEUED"
                    parent.failure_stage = None
                    parent.failure_reason = None
                    parent.completed_at = None
                    for mapping in mappings:
                        if mapping.migration_status == "VALIDATED":
                            mapping.migration_status = "DEPLOYMENT_QUEUED"
                    event_type = "MIGRATION_DEPLOYMENT_QUEUED"
                    status_value = "QUEUED"
                    progress = 90
                    message = "Validation passed. Automatic Git deployment queued."
                elif all_passed:
                    parent.current_stage = "READY_FOR_DEPLOYMENT"
                    parent.overall_status = "COMPLETED"
                    parent.job_status = "COMPLETED"
                    parent.failure_stage = None
                    parent.failure_reason = None
                    parent.completed_at = datetime.now(timezone.utc)
                    event_type = "MIGRATION_READY_FOR_DEPLOYMENT"
                    status_value = "COMPLETED"
                    progress = 100
                    message = "Migration processing completed. Git deployment configuration is missing."
                else:
                    parent.current_stage = "VALIDATION_FAILED"
                    parent.overall_status = "FAILED"
                    parent.job_status = "FAILED"
                    parent.failure_stage = "VALIDATION"
                    parent.failure_reason = workflow.failure_reason
                    parent.completed_at = datetime.now(timezone.utc)
                    event_type = "MIGRATION_VALIDATION_FAILED"
                    status_value = "FAILED"
                    progress = 100
                    message = "Migration failed during Validation."
                session.add(WorkflowEventETL(
                    workflow_run_id=parent.id,
                    event_type=event_type,
                    stage_name="MIGRATION",
                    status=status_value,
                    progress_percentage=progress,
                    message=message,
                    event_payload={
                        "validation_workflow_id": workflow.workflow_id,
                        "repository_url_configured": bool(settings.git_deployment_repository_url),
                        "base_branch": settings.git_deployment_base_branch,
                    },
                    actor_type="SYSTEM",
                ))
        await session.commit()


async def _fail(workflow_id: int, exc: Exception) -> None:
    """Mark an unexpected worker or ValidationAgent failure."""
    async with SessionFactory() as session:
        workflow = await session.get(WorkflowRunETL, workflow_id)
        if workflow is None:
            return
        workflow.current_stage = "VALIDATION"
        workflow.job_status = "FAILED"
        workflow.overall_status = "FAILED"
        workflow.failure_stage = "VALIDATION"
        workflow.failure_reason = str(exc)[:4000]
        workflow.completed_at = datetime.now(timezone.utc)
        await _event(
            session,
            workflow,
            "VALIDATION_FAILED",
            "FAILED",
            "Validation workflow failed unexpectedly.",
            100,
            {
                "error_type": type(exc).__name__,
                "error": str(exc)[:1000],
            },
        )
        if workflow.batch_id:
            parent = await session.scalar(
                select(WorkflowRunETL).where(
                    WorkflowRunETL.workflow_id == workflow.batch_id,
                    WorkflowRunETL.job_type == "MIGRATION",
                )
            )
            if parent is not None:
                parent.current_stage = "VALIDATION_FAILED"
                parent.overall_status = "FAILED"
                parent.job_status = "FAILED"
                parent.failure_stage = "VALIDATION"
                parent.failure_reason = workflow.failure_reason
                parent.completed_at = datetime.now(timezone.utc)
                session.add(
                    WorkflowEventETL(
                        workflow_run_id=parent.id,
                        event_type="MIGRATION_VALIDATION_FAILED",
                        stage_name="MIGRATION",
                        status="FAILED",
                        progress_percentage=100,
                        message="Migration failed during Validation.",
                        event_payload={
                            "validation_workflow_id": workflow.workflow_id,
                            "error": workflow.failure_reason,
                        },
                        actor_type="SYSTEM",
                    )
                )
        await session.commit()


async def run_validation_worker() -> None:
    """Continuously poll for queued Validation workflows."""
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
    asyncio.run(run_validation_worker())
