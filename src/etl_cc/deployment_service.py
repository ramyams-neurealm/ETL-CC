"""Queued Git deployment worker with Migration-level aggregation."""
from __future__ import annotations

import asyncio
import hashlib
import json
import shutil
import re
from dataclasses import dataclass
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
    RepositoryETL,
    WorkflowEventETL,
    WorkflowRunETL,
)
from etl_cc.security import credential_cipher

logger = configure_logging("DEPLOYMENT_SERVICE")
TERMINAL = {"COMPLETED", "FAILED", "CANCELLED"}


@dataclass(frozen=True)
class DeploymentOptions:
    """Git target settings for one mapping deployment."""

    remote_url: str
    base_branch: str
    branch_name: str
    repository_path: str
    commit_message: str
    create_pull_request: bool
    pull_request_title: str
    pull_request_body: str
    access_token: str | None
    # Workflow that owns the GIT_DEPLOYMENT_AGENT audit row.
    audit_run_id: int


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


def _automatic_options(migration: WorkflowRunETL, mapping: ETLObjectETL) -> DeploymentOptions:
    safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "-", mapping.object_name).strip("-").lower()
    return DeploymentOptions(
        remote_url=settings.git_deployment_repository_url,
        base_branch=settings.git_deployment_base_branch,
        branch_name=f"{settings.git_deployment_branch_prefix.strip('/')}/{safe_name}/{migration.workflow_id.split('-', 1)[-1][:8]}",
        repository_path=settings.git_deployment_repository_path,
        commit_message=f"Deploy validated mapping {mapping.object_name}",
        create_pull_request=settings.git_deployment_create_pull_request,
        pull_request_title=f"NeuFlow deployment: {mapping.object_name}",
        pull_request_body=f"Automatic deployment for {migration.workflow_id}.",
        access_token=settings.git_deployment_access_token,
        audit_run_id=migration.id,
    )


async def _deployable_artifacts(session, validation, mapping) -> list[GeneratedArtifactETL]:
    """Return the newest validated artifact of each type from the validated conversion."""
    conversion = await session.scalar(
        select(WorkflowRunETL).where(
            WorkflowRunETL.workflow_id == validation.scope_payload.get("conversion_workflow_id"),
            WorkflowRunETL.job_type == "CONVERSION",
        )
    )
    if conversion is None:
        raise RuntimeError(f"Conversion workflow for validation {validation.workflow_id} was not found.")
    rows = (await session.scalars(
        select(GeneratedArtifactETL).where(
            GeneratedArtifactETL.workflow_run_id == conversion.id,
            GeneratedArtifactETL.etl_object_id == mapping.id,
            GeneratedArtifactETL.validation_status == "VALIDATED",
            GeneratedArtifactETL.is_deployable.is_(True),
        ).order_by(GeneratedArtifactETL.artifact_version.desc(), GeneratedArtifactETL.id.desc())
    )).all()
    latest: dict[str, GeneratedArtifactETL] = {}
    for artifact in rows:
        latest.setdefault(artifact.artifact_type, artifact)
    return list(latest.values())


async def _deploy_mapping(session, migration, validation, mapping, index, total, options: DeploymentOptions) -> bool:
    mapping.migration_status = "DEPLOYMENT_IN_PROGRESS"
    branch = options.branch_name
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

    artifacts = await _deployable_artifacts(session, validation, mapping)
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
            remote_url=options.remote_url,
            base_branch=options.base_branch,
            branch_name=branch,
            repository_path=options.repository_path,
            commit_message=options.commit_message,
            create_pull_request=options.create_pull_request,
            pull_request_title=options.pull_request_title,
            pull_request_body=options.pull_request_body,
            access_token=options.access_token,
            artifacts=materialized,
            validation_evidence=evidence,
        )
        result = await asyncio.to_thread(GitDeploymentAgent().run, context)

    response = AgentResponseETL(
        workflow_run_id=options.audit_run_id,
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
                if not await _deploy_mapping(session, migration, validation, mapping, index, len(mappings), _automatic_options(migration, mapping)):
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


async def _finish_migration_if_done(session, migration: WorkflowRunETL) -> None:
    """Aggregate API-started DEPLOYMENT children into the parent Migration."""
    children = (await session.scalars(
        select(WorkflowRunETL).where(
            WorkflowRunETL.batch_id == migration.workflow_id,
            WorkflowRunETL.job_type == "DEPLOYMENT",
        )
    )).all()
    if any(child.job_status not in TERMINAL for child in children):
        return
    failures = [child for child in children if child.job_status != "COMPLETED"]
    migration.completed_at = datetime.now(timezone.utc)
    migration.current_stage = "DEPLOYMENT_FAILED" if failures else "DEPLOYMENT_COMPLETED"
    migration.overall_status = migration.job_status = "FAILED" if failures else "COMPLETED"
    migration.failure_stage = "DEPLOYMENT" if failures else None
    migration.failure_reason = (
        " | ".join(child.failure_reason or child.workflow_id for child in failures)[:4000]
        if failures else None
    )
    session.add(WorkflowEventETL(
        workflow_run_id=migration.id,
        event_type="MIGRATION_DEPLOYMENT_FAILED" if failures else "MIGRATION_DEPLOYMENT_COMPLETED",
        stage_name="DEPLOYMENT",
        status=migration.job_status,
        progress_percentage=100,
        message="One or more deployments failed." if failures else "All mappings deployed successfully.",
        event_payload={"migration_id": migration.workflow_id, "mapping_count": len(children), "failed_count": len(failures)},
        actor_type="SYSTEM",
    ))


async def _process_manual(run_id: int) -> None:
    """Deploy one mapping for a DEPLOYMENT workflow created by POST /deployments."""
    async with SessionFactory() as session:
        deployment = await session.get(WorkflowRunETL, run_id)
        if deployment is None or deployment.job_type != "DEPLOYMENT":
            raise RuntimeError("Deployment workflow was not found.")
        scope = deployment.scope_payload or {}
        migration = await session.scalar(select(WorkflowRunETL).where(
            WorkflowRunETL.workflow_id == deployment.batch_id,
            WorkflowRunETL.job_type == "MIGRATION",
        ))
        validation = await session.scalar(select(WorkflowRunETL).where(
            WorkflowRunETL.workflow_id == scope.get("validation_workflow_id"),
            WorkflowRunETL.job_type == "VALIDATION",
        ))
        mapping = await session.get(ETLObjectETL, deployment.etl_object_id) if deployment.etl_object_id else None
        target = await session.get(RepositoryETL, scope.get("target_repository_id"))
        if migration is None or validation is None or mapping is None:
            raise RuntimeError("Deployment migration, validation, or mapping was not found.")
        if target is None or (target.connection_config or {}).get("repository_role") != "DEPLOYMENT_TARGET":
            raise RuntimeError("The saved GitHub deployment target was not found.")
        target_config = target.connection_config or {}

        deployment.job_status = deployment.overall_status = "RUNNING"
        deployment.started_at = datetime.now(timezone.utc)
        deployment.attempt_count += 1
        await session.commit()

        options = DeploymentOptions(
            remote_url=target_config["repository_url"],
            base_branch=scope.get("base_branch") or target_config.get("branch") or "main",
            branch_name=scope["branch_name"],
            repository_path=scope.get("repository_path") or settings.git_deployment_repository_path,
            commit_message=scope.get("commit_message") or f"Deploy validated mapping {mapping.object_name}",
            create_pull_request=bool(scope.get("create_pull_request")),
            pull_request_title=scope.get("pull_request_title") or f"NeuFlow deployment: {mapping.object_name}",
            pull_request_body=scope.get("pull_request_body") or f"Deployment for {migration.workflow_id}.",
            access_token=credential_cipher.decrypt(target.credential_ciphertext) if target.credential_ciphertext else None,
            audit_run_id=deployment.id,
        )
        success = await _deploy_mapping(session, migration, validation, mapping, 1, 1, options)

        deployment.completed_at = datetime.now(timezone.utc)
        deployment.current_stage = "DEPLOYMENT_COMPLETED" if success else "DEPLOYMENT_FAILED"
        deployment.job_status = deployment.overall_status = "COMPLETED" if success else "FAILED"
        deployment.failure_stage = None if success else "DEPLOYMENT"
        deployment.failure_reason = None if success else f"{mapping.object_name}: deployment failed"
        await session.flush()
        await _finish_migration_if_done(session, migration)
        await session.commit()


async def _mark_manual_failed(run_id: int, exc: Exception) -> None:
    async with SessionFactory() as session:
        deployment = await session.get(WorkflowRunETL, run_id)
        if deployment is None:
            return
        deployment.current_stage = "DEPLOYMENT_FAILED"
        deployment.overall_status = deployment.job_status = "FAILED"
        deployment.failure_stage = "DEPLOYMENT"
        deployment.failure_reason = str(exc)[:4000]
        deployment.completed_at = datetime.now(timezone.utc)
        if deployment.etl_object_id:
            mapping = await session.get(ETLObjectETL, deployment.etl_object_id)
            if mapping is not None:
                mapping.migration_status = "DEPLOYMENT_FAILED"
        session.add(WorkflowEventETL(
            workflow_run_id=deployment.id,
            event_type="DEPLOYMENT_FAILED",
            stage_name="DEPLOYMENT",
            status="FAILED",
            progress_percentage=100,
            message="Git deployment failed.",
            event_payload={"etl_object_id": deployment.etl_object_id, "failure_reason": deployment.failure_reason},
            actor_type="SYSTEM",
        ))
        await session.flush()
        migration = await session.scalar(select(WorkflowRunETL).where(
            WorkflowRunETL.workflow_id == deployment.batch_id,
            WorkflowRunETL.job_type == "MIGRATION",
        )) if deployment.batch_id else None
        if migration is not None:
            await _finish_migration_if_done(session, migration)
        await session.commit()


async def execute_manual_deployment_workflow(workflow_run_id: int) -> None:
    """Execute one DEPLOYMENT workflow queued through the API."""
    await _process_manual(workflow_run_id)


async def fail_manual_deployment_workflow(workflow_run_id: int, exc: Exception) -> None:
    """Persist a terminal failure for an API-queued DEPLOYMENT workflow."""
    await _mark_manual_failed(workflow_run_id, exc)


async def execute_deployment_workflow(workflow_run_id: int) -> None:
    """Execute one claimed workflow. LangGraph owns all cross-stage routing."""
    await _process(workflow_run_id)


async def fail_deployment_workflow(workflow_run_id: int, exc: Exception) -> None:
    """Persist a terminal stage failure."""
    await _mark_failed(workflow_run_id, exc)
