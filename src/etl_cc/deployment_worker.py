"""Queued Git deployment worker with Migration-level aggregation."""
from __future__ import annotations

import asyncio
import hashlib
import json
import shutil
import re
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from sqlalchemy import select

from etl_cc.agents.deployment_agent import (
    DeploymentArtifact,
    GitDeploymentAgent,
    GitDeploymentContext,
)
from etl_cc.config import settings
from etl_cc.database import SessionFactory
from etl_cc.logging_config import configure_logging, log_exception
from etl_cc.models import (
    AgentResponseETL,
    ArtifactContentETL,
    ETLObjectETL,
    GeneratedArtifactETL,
    WorkflowEventETL,
    WorkflowRunETL,
)

logger = configure_logging("DEPLOYMENT_WORKER")
TERMINAL = {"COMPLETED", "FAILED", "CANCELLED"}


def _content_candidates(content_row: ArtifactContentETL) -> list[bytes]:
    """Return deterministic byte representations for persisted artifact content."""
    encoding = content_row.encoding or "UTF-8"
    if content_row.content_binary is not None:
        return [bytes(content_row.content_binary)]
    if content_row.content_text is not None:
        return [content_row.content_text.encode(encoding)]
    if content_row.content_json is not None:
        value = content_row.content_json
        candidates = [
            json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode(encoding),
            json.dumps(value, indent=2, sort_keys=True, default=str).encode(encoding),
            json.dumps(value, default=str).encode(encoding),
        ]
        unique: list[bytes] = []
        for candidate in candidates:
            if candidate not in unique:
                unique.append(candidate)
        return unique
    raise RuntimeError(
        f"Artifact {content_row.artifact_id} has no populated content value."
    )


def _select_content_bytes(
    artifact: GeneratedArtifactETL,
    content_row: ArtifactContentETL,
) -> bytes:
    """Select the persisted representation that matches the artifact hash."""
    candidates = _content_candidates(content_row)
    expected_hash = (artifact.content_hash or "").strip().lower()
    if not expected_hash:
        return candidates[0]
    for candidate in candidates:
        if hashlib.sha256(candidate).hexdigest().lower() == expected_hash:
            return candidate
    raise RuntimeError(
        f"Stored content hash mismatch for artifact {artifact.id}."
    )


async def _materialize_artifacts(
    session,
    artifacts: list[GeneratedArtifactETL],
    destination: Path,
) -> list[DeploymentArtifact]:
    """Materialize PostgreSQL or local artifacts into safe temporary files."""
    materialized: list[DeploymentArtifact] = []
    used_names: set[str] = set()
    artifact_root = settings.artifact_directory.resolve()

    for artifact in artifacts:
        safe_name = Path(artifact.file_name or "").name
        if not safe_name or safe_name in {".", ".."}:
            raise RuntimeError(f"Artifact {artifact.id} has an invalid filename.")
        if safe_name in used_names:
            safe_name = f"{artifact.id}_{safe_name}"
        used_names.add(safe_name)
        target_path = (destination / safe_name).resolve()
        if destination.resolve() not in target_path.parents:
            raise RuntimeError(f"Artifact {artifact.id} has an unsafe filename.")

        content_row = await session.get(ArtifactContentETL, artifact.id)
        if content_row is not None:
            content = _select_content_bytes(artifact, content_row)
            target_path.write_bytes(content)
        else:
            storage_path = artifact.storage_path or ""
            if storage_path.lower().startswith("postgresql:"):
                raise RuntimeError(
                    f"PostgreSQL content is missing for artifact {artifact.id}."
                )
            source_path = Path(storage_path).resolve()
            if artifact_root not in source_path.parents or not source_path.is_file():
                raise RuntimeError(
                    f"Artifact content is unavailable for artifact {artifact.id}."
                )
            shutil.copyfile(source_path, target_path)
            actual_hash = hashlib.sha256(target_path.read_bytes()).hexdigest()
            if artifact.content_hash and actual_hash.lower() != artifact.content_hash.lower():
                raise RuntimeError(
                    f"Stored content hash mismatch for artifact {artifact.id}."
                )

        materialized.append(DeploymentArtifact(
            artifact_id=artifact.id,
            artifact_type=artifact.artifact_type,
            file_name=artifact.file_name,
            storage_path=str(target_path),
            content_hash=artifact.content_hash,
        ))
    return materialized


async def _claim() -> int | None:
    async with SessionFactory() as session:
        async with session.begin():
            migration = await session.scalar(
                select(WorkflowRunETL)
                .where(
                    WorkflowRunETL.job_type == "MIGRATION",
                    WorkflowRunETL.current_stage == "DEPLOYMENT_QUEUED",
                    WorkflowRunETL.job_status == "QUEUED",
                )
                .order_by(WorkflowRunETL.priority, WorkflowRunETL.id)
                .with_for_update(skip_locked=True)
                .limit(1)
            )
            if migration is None:
                return None
            migration.current_stage = "DEPLOYMENT_IN_PROGRESS"
            migration.job_status = "RUNNING"
            migration.overall_status = "RUNNING"
            migration.attempt_count += 1
            session.add(WorkflowEventETL(
                workflow_run_id=migration.id,
                event_type="MIGRATION_DEPLOYMENT_STARTED",
                stage_name="DEPLOYMENT",
                status="RUNNING",
                progress_percentage=92,
                message="Automatic Git deployment started.",
                event_payload={"migration_id": migration.workflow_id},
                actor_type="SYSTEM",
            ))
            return migration.id


def _event_payload(migration: WorkflowRunETL, mapping: ETLObjectETL, **extra) -> dict[str, Any]:
    return {
        "migration_id": migration.workflow_id,
        "etl_object_id": mapping.id,
        "mapping_name": mapping.object_name,
        **extra,
    }


async def _latest_validation(session, migration_id: str):
    return await session.scalar(
        select(WorkflowRunETL)
        .where(
            WorkflowRunETL.batch_id == migration_id,
            WorkflowRunETL.job_type == "VALIDATION",
            WorkflowRunETL.job_status == "COMPLETED",
        )
        .order_by(WorkflowRunETL.id.desc())
        .limit(1)
    )


async def _deploy_mapping(session, migration, validation, mapping, index, total) -> bool:
    mapping.migration_status = "DEPLOYMENT_IN_PROGRESS"
    safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "-", mapping.object_name).strip("-").lower()
    branch = f"{settings.git_deployment_branch_prefix.strip('/')}/{safe_name}/{migration.workflow_id.split('-', 1)[-1][:8]}"
    session.add(WorkflowEventETL(
        workflow_run_id=migration.id,
        event_type="MAPPING_DEPLOYMENT_STARTED",
        stage_name="DEPLOYMENT",
        status="RUNNING",
        progress_percentage=92 + int((index - 1) / max(total, 1) * 7),
        message=f"Deployment started for {mapping.object_name}.",
        event_payload=_event_payload(migration, mapping, branch_name=branch),
        actor_type="SYSTEM",
    ))
    await session.flush()

    validation_agent = await session.scalar(
        select(AgentResponseETL)
        .where(
            AgentResponseETL.workflow_run_id == validation.id,
            AgentResponseETL.etl_object_id == mapping.id,
            AgentResponseETL.agent_name == "VALIDATION_AGENT",
        )
        .order_by(AgentResponseETL.id.desc()).limit(1)
    )
    evidence = dict(validation_agent.response_payload or {}) if validation_agent else {}
    if not (evidence.get("status") == "PASSED" and evidence.get("deployable")):
        raise RuntimeError(f"Mapping {mapping.object_name} is not eligible for deployment.")

    artifacts = list((await session.scalars(
        select(GeneratedArtifactETL).where(
            GeneratedArtifactETL.etl_object_id == mapping.id,
            GeneratedArtifactETL.validation_status == "VALIDATED",
            GeneratedArtifactETL.is_deployable.is_(True),
        ).order_by(GeneratedArtifactETL.artifact_version.desc(), GeneratedArtifactETL.id.desc())
    )).all())
    if not artifacts:
        raise RuntimeError(f"No validated deployable artifacts found for {mapping.object_name}.")

    with TemporaryDirectory(prefix=f"etlcc-{migration.workflow_id}-{mapping.id}-") as temp_dir:
        materialized = await _materialize_artifacts(session, artifacts, Path(temp_dir))
        context = GitDeploymentContext(
            workflow_id=migration.workflow_id,
            repository_id=migration.repository_id,
            mapping_id=mapping.id,
            mapping_name=mapping.object_name,
            validation_workflow_id=validation.workflow_id,
            remote_url=settings.git_deployment_repository_url,
            base_branch=settings.git_deployment_base_branch,
            branch_name=branch,
            repository_path=settings.git_deployment_repository_path,
            commit_message=f"Deploy validated mapping {mapping.object_name}",
            create_pull_request=settings.git_deployment_create_pull_request,
            pull_request_title=f"NeuFlow deployment: {mapping.object_name}",
            pull_request_body=f"Automatic deployment for {migration.workflow_id}.",
            access_token=settings.git_deployment_access_token,
            artifacts=materialized,
            validation_evidence=evidence,
        )
        result = await asyncio.to_thread(GitDeploymentAgent().run, context)

    response = AgentResponseETL(
        workflow_run_id=migration.id,
        etl_object_id=mapping.id,
        agent_name=GitDeploymentAgent.AGENT_NAME,
        agent_version=GitDeploymentAgent.AGENT_VERSION,
        stage_name=GitDeploymentAgent.STAGE_NAME,
        model_name=GitDeploymentAgent.MODEL_NAME,
        status="COMPLETED" if result.status == "DEPLOYED" else "FAILED",
        response_payload=result.model_dump(mode="json"),
        error_message=result.failure_reason,
        input_tokens=0,
        output_tokens=0,
        started_at=datetime.now(timezone.utc),
        completed_at=datetime.now(timezone.utc),
    )
    session.add(response)
    await session.flush()
    success = result.status == "DEPLOYED"
    mapping.migration_status = "DEPLOYED" if success else "DEPLOYMENT_FAILED"
    if success:
        for artifact in artifacts:
            artifact.git_path = next((x for x in result.deployed_paths if x.endswith(Path(artifact.file_name).name)), artifact.git_path)
    session.add(WorkflowEventETL(
        workflow_run_id=migration.id,
        agent_response_id=response.id,
        event_type="MAPPING_DEPLOYMENT_COMPLETED" if success else "MAPPING_DEPLOYMENT_FAILED",
        stage_name="DEPLOYMENT",
        status="COMPLETED" if success else "FAILED",
        progress_percentage=92 + int(index / max(total, 1) * 7),
        message=(f"Deployment completed for {mapping.object_name}." if success else result.failure_reason or "Git deployment failed."),
        event_payload=_event_payload(migration, mapping, **result.model_dump(mode="json")),
        actor_type="SYSTEM",
    ))
    return success


async def _process(run_id: int) -> None:
    async with SessionFactory() as session:
        migration = await session.get(WorkflowRunETL, run_id)
        if migration is None or migration.job_type != "MIGRATION":
            raise RuntimeError("Migration workflow was not found.")
        if not settings.git_deployment_repository_url:
            raise RuntimeError("GIT_DEPLOYMENT_REPOSITORY_URL is not configured.")
        if not settings.git_deployment_access_token:
            raise RuntimeError("GIT_DEPLOYMENT_ACCESS_TOKEN is not configured.")
        validation = await _latest_validation(session, migration.workflow_id)
        if validation is None:
            raise RuntimeError("Completed Validation workflow was not found.")
        ids = list(dict.fromkeys(validation.scope_payload.get("etl_object_ids", [])))
        mappings = list((await session.scalars(select(ETLObjectETL).where(
            ETLObjectETL.repository_id == migration.repository_id,
            ETLObjectETL.id.in_(ids),
        ).order_by(ETLObjectETL.id))).all())
        failures = []
        for index, mapping in enumerate(mappings, 1):
            try:
                if not await _deploy_mapping(session, migration, validation, mapping, index, len(mappings)):
                    failures.append(f"{mapping.object_name}: deployment failed")
            except Exception as exc:
                mapping.migration_status = "DEPLOYMENT_FAILED"
                failures.append(f"{mapping.object_name}: {exc}")
                session.add(WorkflowEventETL(
                    workflow_run_id=migration.id,
                    event_type="MAPPING_DEPLOYMENT_FAILED",
                    stage_name="DEPLOYMENT",
                    status="FAILED",
                    progress_percentage=92 + int(index / max(len(mappings), 1) * 7),
                    message=f"Deployment failed for {mapping.object_name}.",
                    event_payload=_event_payload(migration, mapping, failure_reason=str(exc)[:4000]),
                    actor_type="SYSTEM",
                ))
            await session.commit()

        migration.completed_at = datetime.now(timezone.utc)
        migration.current_stage = "DEPLOYMENT_FAILED" if failures else "DEPLOYMENT_COMPLETED"
        migration.overall_status = "FAILED" if failures else "COMPLETED"
        migration.job_status = "FAILED" if failures else "COMPLETED"
        migration.failure_stage = "DEPLOYMENT" if failures else None
        migration.failure_reason = " | ".join(failures)[:4000] if failures else None
        session.add(WorkflowEventETL(
            workflow_run_id=migration.id,
            event_type="MIGRATION_DEPLOYMENT_FAILED" if failures else "MIGRATION_DEPLOYMENT_COMPLETED",
            stage_name="DEPLOYMENT",
            status=migration.job_status,
            progress_percentage=100,
            message="One or more deployments failed." if failures else "All mappings deployed successfully.",
            event_payload={"migration_id": migration.workflow_id, "mapping_count": len(mappings), "failed_count": len(failures)},
            actor_type="SYSTEM",
        ))
        await session.commit()


async def _mark_failed(run_id: int, exc: Exception) -> None:
    async with SessionFactory() as session:
        migration = await session.get(WorkflowRunETL, run_id)
        if migration is None:
            return
        migration.current_stage = "DEPLOYMENT_FAILED"
        migration.overall_status = migration.job_status = "FAILED"
        migration.failure_stage = "DEPLOYMENT"
        migration.failure_reason = str(exc)[:4000]
        migration.completed_at = datetime.now(timezone.utc)
        session.add(WorkflowEventETL(
            workflow_run_id=migration.id,
            event_type="MIGRATION_DEPLOYMENT_FAILED",
            stage_name="DEPLOYMENT",
            status="FAILED",
            progress_percentage=100,
            message="Automatic Git deployment failed.",
            event_payload={"migration_id": migration.workflow_id, "failure_reason": migration.failure_reason},
            actor_type="SYSTEM",
        ))
        await session.commit()


async def main() -> None:
    while True:
        run_id = await _claim()
        if run_id is None:
            await asyncio.sleep(settings.worker_poll_seconds)
            continue
        try:
            await _process(run_id)
        except Exception as exc:
            log_exception(logger, "MIGRATION_DEPLOYMENT_FAILED", exc, workflow_run_id=run_id)
            await _mark_failed(run_id, exc)


if __name__ == "__main__":
    asyncio.run(main())
