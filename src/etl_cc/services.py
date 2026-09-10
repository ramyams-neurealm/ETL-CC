"""Source analysis, repository persistence, and discovery services."""

import hashlib
import json
from typing import Any
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_cc.config import settings
from etl_cc.connectors import GitHubSource, PowerCenterSource
from etl_cc.informatica_parser import InformaticaXMLParser
from etl_cc.models import (
    AgentResponseETL,
    AgentResponseSummary,
    ArtifactContentResponse,
    ArtifactResponse,
    ComplexityDistribution,
    ConversionMappingResponse,
    DependencyPlanResponse,
    DiscoverySummaryResponse,
    ETLObjectETL,
    GeneratedArtifactETL,
    GitHubConnectionRequest,
    MappingDiscoveryDetailsResponse,
    MappingInventoryResponse,
    MappingLineageDetailsResponse,
    PowerCenterConnectionRequest,
    RepositoryETL,
    RepositoryResponse,
    SaveRepositoryResponse,
    SourceAnalysisResponse,
    StartConversionRequest,
    StartConversionResponse,
    StartDiscoveryRequest,
    StartValidationRequest,
    StartValidationResponse,
    StartDeploymentRequest,
    StartDeploymentResponse,
    DeploymentMappingResponse,
    ValidationMappingResponse,
    ValidationReportResponse,
    ValidationTestCaseETL,
    ValidationTestCaseResponse,
    WorkflowEventETL,
    WorkflowRunETL,
)
from etl_cc.security import (
    create_connection_configuration_hash,
    create_connection_test_token,
    credential_cipher,
    verify_connection_test_token,
)
from etl_cc.source_store import load_manifest, save_bytes, write_manifest


def _source_hash(payload: dict) -> str:
    return create_connection_configuration_hash(payload)


def _response(connection_type: str, source_id: str, payload: dict, mappings, message: str):
    fingerprint = _source_hash({"source_id": source_id, "connection_type": connection_type, **payload})
    return SourceAnalysisResponse(
        status="SUCCESS",
        message=message,
        connection_type=connection_type,
        source_id=source_id,
        test_token=create_connection_test_token(fingerprint),
        expires_in_seconds=settings.connection_test_ttl_seconds,
        mapping_count=len(mappings),
        mappings=mappings,
    ), fingerprint


async def analyze_powercenter(request: PowerCenterConnectionRequest) -> SourceAnalysisResponse:
    payload = request.model_dump(exclude={"password"}, mode="json")
    source_id = str(uuid4())
    config = {
        "host": request.host,
        "port": request.port,
        "domain_name": request.domain_name,
        "repository_name": request.repository_name,
        "security_domain": request.security_domain,
        "username": request.username,
    }
    source = PowerCenterSource(config, request.password.get_secret_value())
    mappings = await source.list_mappings()
    response, fingerprint = _response("POWERCENTER", source_id, payload, mappings, "PowerCenter connection verified and mappings loaded.")
    directory = settings.project_root / "runtime_sources" / "powercenter" / source_id
    directory.mkdir(parents=True, exist_ok=True)
    write_manifest(source_id, directory, {
        "source_id": source_id,
        "fingerprint": fingerprint,
        "connection_type": "POWERCENTER",
        "connection_name": request.connection_name,
        "environment": request.environment,
        "config": config,
        "credential_ciphertext": credential_cipher.encrypt(request.password.get_secret_value()),
    })
    return response


async def analyze_github(request: GitHubConnectionRequest) -> SourceAnalysisResponse:
    payload = request.model_dump(exclude={"access_token"}, mode="json")
    source_id = str(uuid4())
    directory = settings.project_root / "runtime_sources" / "github" / source_id
    config = {
        "repository_url": str(request.repository_url),
        "branch": request.branch,
        "folder_path": request.folder_path,
    }
    token = request.access_token.get_secret_value() if request.access_token else None
    source = GitHubSource(config, token, directory)
    mappings = await source.list_mappings()
    response, fingerprint = _response("GITHUB", source_id, payload, mappings, "GitHub repository verified and Informatica mappings loaded.")
    write_manifest(source_id, directory, {
        "source_id": source_id,
        "fingerprint": fingerprint,
        "connection_type": "GITHUB",
        "connection_name": request.connection_name,
        "environment": request.environment,
        "config": config,
        "credential_ciphertext": credential_cipher.encrypt(token) if token else None,
    })
    return response


async def analyze_xml_upload(
    connection_name: str,
    environment: str,
    file_name: str,
    content: bytes,
) -> SourceAnalysisResponse:
    if not file_name.lower().endswith(".xml"):
        raise ValueError("Only .xml files are accepted.")
    if not content:
        raise ValueError("The uploaded XML file is empty.")
    source_id, file_path, digest = save_bytes("XML_UPLOAD", file_name, content)
    parser = InformaticaXMLParser()
    mappings = parser.list_mappings(file_path, file_path.name)
    payload = {
        "connection_name": connection_name,
        "environment": environment,
        "file_name": file_path.name,
        "content_hash": digest,
    }
    response, fingerprint = _response("XML_UPLOAD", source_id, payload, mappings, "Informatica XML validated and mappings loaded.")
    write_manifest(source_id, file_path.parent, {
        "source_id": source_id,
        "fingerprint": fingerprint,
        "connection_type": "XML_UPLOAD",
        "connection_name": connection_name,
        "environment": environment,
        "config": {
            "original_file_name": file_path.name,
            "stored_file_path": str(file_path),
            "content_hash": digest,
            "file_size": len(content),
        },
        "credential_ciphertext": None,
    })
    return response


async def start_discovery(
    session: AsyncSession,
    request: StartDiscoveryRequest,
) -> SaveRepositoryResponse:
    manifest = load_manifest(request.source_id)
    verify_connection_test_token(request.test_token, manifest["fingerprint"])
    if request.scope_type == "SELECTED_MAPPINGS" and not request.selected_mapping_keys:
        raise ValueError("Select at least one mapping.")
    repository = RepositoryETL(
        repository_name=manifest["connection_name"],
        source_type="INFORMATICA",
        connection_type=manifest["connection_type"],
        environment=manifest["environment"],
        connection_config={**manifest["config"], "source_id": request.source_id},
        credential_ciphertext=manifest.get("credential_ciphertext"),
        credential_algorithm="FERNET" if manifest.get("credential_ciphertext") else None,
        credential_key_version=settings.etl_credential_key_version if manifest.get("credential_ciphertext") else None,
        connection_status="CONNECTED" if manifest["connection_type"] != "XML_UPLOAD" else "ANALYZED",
        last_tested_at=datetime.now(timezone.utc),
    )
    session.add(repository)
    await session.flush()
    workflow = WorkflowRunETL(
        workflow_id=f"DISC-{uuid4()}",
        repository_id=repository.id,
        current_stage="DISCOVERY",
        overall_status="QUEUED",
        job_type="DISCOVERY",
        job_status="QUEUED",
        scope_payload={
            "source_id": request.source_id,
            "connection_type": manifest["connection_type"],
            "scope_type": request.scope_type,
            "selected_mapping_keys": request.selected_mapping_keys,
        },
    )
    session.add(workflow)
    await session.flush()
    session.add(WorkflowEventETL(
        workflow_run_id=workflow.id,
        event_type="DISCOVERY_QUEUED",
        stage_name="DISCOVERY",
        status="QUEUED",
        progress_percentage=0,
        message="Source saved and discovery queued.",
        event_payload={"source_id": request.source_id, "connection_type": manifest["connection_type"]},
        actor_type="SYSTEM",
    ))
    await session.commit()
    return SaveRepositoryResponse(
        repository_id=repository.id,
        workflow_id=workflow.workflow_id,
        status="QUEUED",
        message="Source saved and discovery started.",
    )


async def list_repositories(session: AsyncSession) -> list[RepositoryResponse]:
    rows = (await session.scalars(select(RepositoryETL).order_by(RepositoryETL.created_at.desc()))).all()
    return [RepositoryResponse(
        repository_id=row.id,
        repository_name=row.repository_name,
        source_type=row.source_type,
        connection_type=row.connection_type,
        environment=row.environment,
        connection_status=row.connection_status,
        connection_config=row.connection_config,
        created_at=row.created_at,
    ) for row in rows]


async def get_repository(session: AsyncSession, repository_id: int) -> RepositoryResponse | None:
    row = await session.get(RepositoryETL, repository_id)
    if not row:
        return None
    return RepositoryResponse(
        repository_id=row.id,
        repository_name=row.repository_name,
        source_type=row.source_type,
        connection_type=row.connection_type,
        environment=row.environment,
        connection_status=row.connection_status,
        connection_config=row.connection_config,
        created_at=row.created_at,
    )


async def list_repository_mappings(
    session: AsyncSession,
    repository_id: int,
) -> list[MappingInventoryResponse]:
    rows = list((await session.scalars(
        select(ETLObjectETL)
        .where(ETLObjectETL.repository_id == repository_id)
        .order_by(ETLObjectETL.object_name)
    )).all())
    result: list[MappingInventoryResponse] = []
    for row in rows:
        agent = await _latest_agent_response(
            session,
            etl_object_id=row.id,
            agent_name="DISCOVERY_AGENT",
        )
        payload = agent.response_payload if agent else {}
        stage, ui_status, ui_result, review = _readiness(payload, row)
        result.append(MappingInventoryResponse(
            etl_object_id=row.id,
            repository_id=row.repository_id,
            source_object_key=row.source_object_key,
            mapping_name=row.object_name,
            folder_name=row.folder_path,
            complexity=row.complexity,
            complexity_score=row.complexity_score,
            transformation_count=row.transformation_count,
            migration_wave=row.migration_wave,
            discovery_status=row.discovery_status,
            migration_status=row.migration_status,
            stage=stage,
            status=ui_status,
            result=ui_result,
            confidence=payload.get("confidence"),
            human_review_required=review,
            has_dependency_cycle=bool(row.has_dependency_cycle),
        ))
    return result


async def get_workflow_by_external_id(session: AsyncSession, workflow_id: str):
    """Return one workflow using its public workflow identifier."""
    return await session.scalar(
        select(WorkflowRunETL).where(WorkflowRunETL.workflow_id == workflow_id)
    )


async def list_workflow_events(
    session: AsyncSession,
    workflow_run_id: int,
    after_event_id: int = 0,
):
    """Return durable workflow events after a supplied event ID."""
    return list((await session.scalars(
        select(WorkflowEventETL)
        .where(
            WorkflowEventETL.workflow_run_id == workflow_run_id,
            WorkflowEventETL.id > after_event_id,
        )
        .order_by(WorkflowEventETL.id)
    )).all())


async def _latest_agent_response(
    session: AsyncSession,
    *,
    agent_name: str,
    etl_object_id: int | None = None,
    workflow_run_id: int | None = None,
):
    query = select(AgentResponseETL).where(
        AgentResponseETL.agent_name == agent_name
    )
    if etl_object_id is not None:
        query = query.where(AgentResponseETL.etl_object_id == etl_object_id)
    if workflow_run_id is not None:
        query = query.where(
            AgentResponseETL.workflow_run_id == workflow_run_id
        )
    return await session.scalar(
        query.order_by(AgentResponseETL.id.desc()).limit(1)
    )


def _readiness(
    payload: dict[str, Any],
    row: ETLObjectETL,
) -> tuple[str, str, str, bool]:
    review = (
        bool(payload.get("human_review_required"))
        or bool(row.has_dependency_cycle)
    )
    blocked = (
        bool(payload.get("unsupported_constructs"))
        or row.discovery_status in {"ANALYSIS_FAILED", "FAILED"}
    )
    if blocked:
        return "DISCOVERY_COMPLETE", "BLOCKED", "FAIL", review
    if review:
        return "DISCOVERY_COMPLETE", "REVIEW_REQUIRED", "WARNING", True
    if row.discovery_status == "ANALYZED":
        return "DISCOVERY_COMPLETE", "READY", "PASS", False
    return "DISCOVERY", "PENDING", "PENDING", review


async def get_discovery_summary(
    session: AsyncSession,
    repository_id: int,
) -> DiscoverySummaryResponse | None:
    repository = await session.get(RepositoryETL, repository_id)
    if repository is None:
        return None
    rows = list((await session.scalars(
        select(ETLObjectETL).where(
            ETLObjectETL.repository_id == repository_id
        )
    )).all())
    distribution = {"LOW": 0, "MEDIUM": 0, "HIGH": 0, "UNKNOWN": 0}
    ready = review_required = blocked = cycles = discovered = 0
    input_tokens = output_tokens = 0
    for row in rows:
        level = (row.complexity or "UNKNOWN").upper()
        distribution[level if level in distribution else "UNKNOWN"] += 1
        discovered += row.discovery_status == "ANALYZED"
        agent = await _latest_agent_response(
            session,
            etl_object_id=row.id,
            agent_name="DISCOVERY_AGENT",
        )
        payload = agent.response_payload if agent else {}
        _, status, _, _ = _readiness(payload, row)
        ready += status == "READY"
        review_required += status == "REVIEW_REQUIRED"
        blocked += status == "BLOCKED"
        cycles += bool(row.has_dependency_cycle)
        if agent:
            input_tokens += agent.input_tokens or 0
            output_tokens += agent.output_tokens or 0
    return DiscoverySummaryResponse(
        repository_id=repository_id,
        total_mappings=len(rows),
        need_to_discover=len(rows) - discovered,
        discovered=discovered,
        complexity_distribution=ComplexityDistribution(**distribution),
        ready=ready,
        review_required=review_required,
        blocked=blocked,
        dependency_cycles=cycles,
        total_input_tokens=input_tokens,
        total_output_tokens=output_tokens,
    )


async def get_mapping_discovery_details(
    session: AsyncSession,
    etl_object_id: int,
) -> MappingDiscoveryDetailsResponse | None:
    row = await session.get(ETLObjectETL, etl_object_id)
    if row is None:
        return None
    agent = await _latest_agent_response(
        session,
        etl_object_id=row.id,
        agent_name="DISCOVERY_AGENT",
    )
    payload = agent.response_payload if agent else {}
    _, ui_status, _, review = _readiness(payload, row)
    return MappingDiscoveryDetailsResponse(
        etl_object_id=row.id,
        repository_id=row.repository_id,
        source_object_key=row.source_object_key,
        mapping_name=row.object_name,
        folder_name=row.folder_path,
        business_purpose=row.business_purpose,
        business_rules=row.business_rules or [],
        prerequisites=row.prerequisites or [],
        complexity=row.complexity,
        complexity_score=row.complexity_score,
        complexity_reasons=payload.get("complexity_reasons", []),
        risks=payload.get("risks", []),
        unsupported_constructs=payload.get("unsupported_constructs", []),
        assumptions=payload.get("assumptions", []),
        migration_recommendations=payload.get(
            "migration_recommendations", []
        ),
        confidence=payload.get("confidence"),
        human_review_required=review,
        readiness_status=ui_status,
        discovery_status=row.discovery_status,
        model_name=agent.model_name if agent else None,
        prompt_name=agent.prompt_name if agent else None,
        prompt_version=agent.prompt_version if agent else None,
        input_tokens=agent.input_tokens if agent else 0,
        output_tokens=agent.output_tokens if agent else 0,
        agent_response_id=agent.id if agent else None,
        analyzed_at=agent.completed_at if agent else None,
    )


async def get_mapping_lineage_details(
    session: AsyncSession,
    etl_object_id: int,
) -> MappingLineageDetailsResponse | None:
    row = await session.get(ETLObjectETL, etl_object_id)
    if row is None:
        return None
    lineage = dict(row.lineage or {})
    lineage["migration_wave"] = row.migration_wave
    lineage["has_dependency_cycle"] = bool(row.has_dependency_cycle)
    return MappingLineageDetailsResponse(
        etl_object_id=row.id,
        repository_id=row.repository_id,
        mapping_name=row.object_name,
        lineage=lineage,
        dependencies=row.dependencies or {},
        migration_wave=row.migration_wave,
        has_dependency_cycle=bool(row.has_dependency_cycle),
    )


async def get_workflow_dependency_plan(
    session: AsyncSession,
    workflow_id: str,
) -> DependencyPlanResponse | None:
    workflow = await get_workflow_by_external_id(session, workflow_id)
    if workflow is None:
        return None
    agent = await _latest_agent_response(
        session,
        workflow_run_id=workflow.id,
        agent_name="DEPENDENCY_PLANNER",
    )
    if agent is None:
        return None
    return DependencyPlanResponse(
        workflow_id=workflow.workflow_id,
        repository_id=workflow.repository_id,
        status=agent.status,
        model_name=agent.model_name,
        response_payload=agent.response_payload or {},
        agent_response_id=agent.id,
        completed_at=agent.completed_at,
    )


async def list_mapping_agent_responses(
    session: AsyncSession,
    etl_object_id: int,
) -> list[AgentResponseSummary] | None:
    row = await session.get(ETLObjectETL, etl_object_id)
    if row is None:
        return None
    records = list((await session.scalars(
        select(AgentResponseETL)
        .where(AgentResponseETL.etl_object_id == etl_object_id)
        .order_by(AgentResponseETL.id.desc())
    )).all())
    workflow_ids: dict[int, str] = {}
    if records:
        workflows = (await session.scalars(
            select(WorkflowRunETL).where(
                WorkflowRunETL.id.in_(
                    {record.workflow_run_id for record in records}
                )
            )
        )).all()
        workflow_ids = {item.id: item.workflow_id for item in workflows}
    return [AgentResponseSummary(
        agent_response_id=record.id,
        workflow_id=workflow_ids.get(record.workflow_run_id, ""),
        etl_object_id=record.etl_object_id,
        agent_name=record.agent_name,
        agent_version=record.agent_version,
        stage_name=record.stage_name,
        attempt_number=record.attempt_number,
        status=record.status,
        model_name=record.model_name,
        prompt_name=record.prompt_name,
        prompt_version=record.prompt_version,
        input_tokens=record.input_tokens,
        output_tokens=record.output_tokens,
        error_message=record.error_message,
        recommendation=record.recommendation,
        request_payload=record.request_payload or {},
        response_payload=record.response_payload or {},
        started_at=record.started_at,
        completed_at=record.completed_at,
        created_at=record.created_at,
    ) for record in records]


async def start_conversion(
    session: AsyncSession,
    request: StartConversionRequest,
) -> StartConversionResponse:
    repository = await session.get(RepositoryETL, request.repository_id)
    if repository is None:
        raise ValueError("Repository not found.")
    selected_ids = list(dict.fromkeys(request.etl_object_ids))
    rows = list((await session.scalars(
        select(ETLObjectETL).where(
            ETLObjectETL.repository_id == request.repository_id,
            ETLObjectETL.id.in_(selected_ids),
        )
    )).all())
    if len(rows) != len(selected_ids):
        raise ValueError(
            "One or more selected mappings do not belong to the repository."
        )
    not_ready = [
        row.object_name
        for row in rows
        if row.discovery_status != "ANALYZED"
        or row.has_dependency_cycle
        or row.migration_wave is None
    ]
    if not_ready:
        raise ValueError(
            "Mappings are not ready for conversion: "
            + ", ".join(sorted(not_ready))
        )
    workflow = WorkflowRunETL(
        workflow_id=f"CONV-{uuid4()}",
        repository_id=request.repository_id,
        current_stage="CONVERSION",
        overall_status="QUEUED",
        job_type="CONVERSION",
        job_status="QUEUED",
        scope_payload={
            "etl_object_ids": selected_ids,
            "target_platform": request.target_platform,
            "target_framework": request.target_framework,
            "follow_migration_waves": request.follow_migration_waves,
        },
        max_attempts=1,
    )
    session.add(workflow)
    await session.flush()
    session.add(WorkflowEventETL(
        workflow_run_id=workflow.id,
        event_type="CONVERSION_QUEUED",
        stage_name="CONVERSION",
        status="QUEUED",
        progress_percentage=0,
        message="Conversion workflow queued.",
        event_payload={
            "etl_object_ids": selected_ids,
            "mapping_count": len(rows),
        },
        actor_type="SYSTEM",
    ))
    for row in rows:
        row.migration_status = "QUEUED"
    await session.commit()
    return StartConversionResponse(
        workflow_id=workflow.workflow_id,
        repository_id=workflow.repository_id,
        status="QUEUED",
        mapping_count=len(rows),
        message="Conversion workflow queued.",
    )


async def get_conversion_workflow(
    session: AsyncSession,
    workflow_id: str,
) -> WorkflowRunETL | None:
    row = await get_workflow_by_external_id(session, workflow_id)
    if row is None or row.job_type != "CONVERSION":
        return None
    return row


async def list_conversion_mappings(
    session: AsyncSession,
    workflow_id: str,
) -> list[ConversionMappingResponse] | None:
    workflow = await get_conversion_workflow(session, workflow_id)
    if workflow is None:
        return None
    ids = workflow.scope_payload.get("etl_object_ids", [])
    rows = list((await session.scalars(
        select(ETLObjectETL)
        .where(ETLObjectETL.id.in_(ids))
        .order_by(ETLObjectETL.migration_wave, ETLObjectETL.id)
    )).all())
    result: list[ConversionMappingResponse] = []
    for row in rows:
        conversion = await _latest_agent_response(
            session,
            etl_object_id=row.id,
            workflow_run_id=workflow.id,
            agent_name="CONVERSION_AGENT",
        )
        critique = await _latest_agent_response(
            session,
            etl_object_id=row.id,
            workflow_run_id=workflow.id,
            agent_name="CRITIQUE_AGENT",
        )
        result.append(ConversionMappingResponse(
            etl_object_id=row.id,
            mapping_name=row.object_name,
            migration_wave=row.migration_wave,
            migration_status=row.migration_status,
            latest_conversion=(
                conversion.response_payload if conversion else None
            ),
            latest_critique=(critique.response_payload if critique else None),
        ))
    return result


async def get_mapping_conversion(
    session: AsyncSession,
    etl_object_id: int,
) -> dict[str, Any] | None:
    row = await session.get(ETLObjectETL, etl_object_id)
    if row is None:
        return None
    rag = await _latest_agent_response(
        session,
        etl_object_id=etl_object_id,
        agent_name="RAG_RETRIEVAL_AGENT",
    )
    conversion = await _latest_agent_response(
        session,
        etl_object_id=etl_object_id,
        agent_name="CONVERSION_AGENT",
    )
    critique = await _latest_agent_response(
        session,
        etl_object_id=etl_object_id,
        agent_name="CRITIQUE_AGENT",
    )
    return {
        "etl_object_id": row.id,
        "mapping_name": row.object_name,
        "migration_wave": row.migration_wave,
        "migration_status": row.migration_status,
        "rag": rag.response_payload if rag else None,
        "conversion": conversion.response_payload if conversion else None,
        "critique": critique.response_payload if critique else None,
    }


async def list_mapping_artifacts(
    session: AsyncSession,
    etl_object_id: int,
) -> list[ArtifactResponse] | None:
    row = await session.get(ETLObjectETL, etl_object_id)
    if row is None:
        return None
    artifacts = list((await session.scalars(
        select(GeneratedArtifactETL)
        .where(GeneratedArtifactETL.etl_object_id == etl_object_id)
        .order_by(GeneratedArtifactETL.id.desc())
    )).all())
    workflow_ids: dict[int, str] = {}
    if artifacts:
        workflows = (await session.scalars(
            select(WorkflowRunETL).where(
                WorkflowRunETL.id.in_(
                    {artifact.workflow_run_id for artifact in artifacts}
                )
            )
        )).all()
        workflow_ids = {item.id: item.workflow_id for item in workflows}
    return [ArtifactResponse(
        artifact_id=artifact.id,
        workflow_id=workflow_ids.get(artifact.workflow_run_id, ""),
        etl_object_id=artifact.etl_object_id,
        artifact_type=artifact.artifact_type,
        artifact_version=artifact.artifact_version,
        file_name=artifact.file_name,
        storage_provider=artifact.storage_provider,
        storage_path=artifact.storage_path,
        content_hash=artifact.content_hash,
        media_type=artifact.media_type,
        validation_status=artifact.validation_status,
        is_deployable=artifact.is_deployable,
        created_at=artifact.created_at,
    ) for artifact in artifacts]


async def get_artifact_content(
    session: AsyncSession,
    artifact_id: int,
) -> ArtifactContentResponse | None:
    artifact = await session.get(GeneratedArtifactETL, artifact_id)
    if artifact is None:
        return None
    workflow = await session.get(WorkflowRunETL, artifact.workflow_run_id)
    artifact_path = Path(artifact.storage_path).resolve()
    artifact_root = settings.artifact_directory.resolve()
    if artifact_root not in artifact_path.parents or not artifact_path.is_file():
        raise ValueError(
            "Artifact file is unavailable or outside the configured artifact root."
        )
    return ArtifactContentResponse(
        artifact_id=artifact.id,
        workflow_id=workflow.workflow_id if workflow else "",
        etl_object_id=artifact.etl_object_id,
        artifact_type=artifact.artifact_type,
        artifact_version=artifact.artifact_version,
        file_name=artifact.file_name,
        storage_provider=artifact.storage_provider,
        storage_path=artifact.storage_path,
        content_hash=artifact.content_hash,
        media_type=artifact.media_type,
        validation_status=artifact.validation_status,
        is_deployable=artifact.is_deployable,
        created_at=artifact.created_at,
        content=artifact_path.read_text(encoding="utf-8"),
    )




async def start_validation(
    session: AsyncSession,
    request: StartValidationRequest,
) -> StartValidationResponse:
    if request.input_mode == "DATASET_FILE" and not request.dataset_files:
        raise ValueError("dataset_files is required for DATASET_FILE mode.")
    if request.input_mode == "DATABASE_TABLES" and request.database_tables is None:
        raise ValueError("database_tables is required for DATABASE_TABLES mode.")
    if request.input_mode == "SIMULATE" and (request.dataset_files or request.database_tables):
        raise ValueError("SIMULATE mode cannot include file or database inputs.")
    repository = await session.get(RepositoryETL, request.repository_id)
    if repository is None:
        raise ValueError("Repository not found.")

    conversion = await session.scalar(
        select(WorkflowRunETL).where(
            WorkflowRunETL.workflow_id == request.conversion_workflow_id,
            WorkflowRunETL.repository_id == request.repository_id,
            WorkflowRunETL.job_type == "CONVERSION",
            WorkflowRunETL.overall_status == "COMPLETED",
        )
    )
    if conversion is None:
        raise ValueError(
            "A completed conversion workflow was not found for this repository."
        )

    selected_ids = list(dict.fromkeys(request.etl_object_ids))
    conversion_ids = set(conversion.scope_payload.get("etl_object_ids", []))
    if not set(selected_ids).issubset(conversion_ids):
        raise ValueError(
            "One or more selected mappings are not part of the conversion workflow."
        )

    mappings = list((await session.scalars(
        select(ETLObjectETL).where(
            ETLObjectETL.repository_id == request.repository_id,
            ETLObjectETL.id.in_(selected_ids),
        )
    )).all())
    if len(mappings) != len(selected_ids):
        raise ValueError(
            "One or more selected mappings do not belong to the repository."
        )

    artifacts = list((await session.scalars(
        select(GeneratedArtifactETL).where(
            GeneratedArtifactETL.workflow_run_id == conversion.id,
            GeneratedArtifactETL.etl_object_id.in_(selected_ids),
        )
    )).all())
    required = {"PYSPARK_CODE", "UNIT_TEST", "CONFIGURATION"}
    artifacts_by_mapping = {mapping_id: set() for mapping_id in selected_ids}
    for artifact in artifacts:
        if artifact.etl_object_id in artifacts_by_mapping:
            artifacts_by_mapping[artifact.etl_object_id].add(
                artifact.artifact_type
            )
    missing = [
        str(mapping_id)
        for mapping_id, types in artifacts_by_mapping.items()
        if not required.issubset(types)
    ]
    if missing:
        raise ValueError(
            "Required conversion artifacts are missing for mapping IDs: "
            + ", ".join(missing)
        )

    workflow = WorkflowRunETL(
        workflow_id=f"VAL-{uuid4()}",
        repository_id=request.repository_id,
        current_stage="VALIDATION",
        overall_status="QUEUED",
        job_type="VALIDATION",
        job_status="QUEUED",
        scope_payload={
            "etl_object_ids": selected_ids,
            "conversion_workflow_id": request.conversion_workflow_id,
            "validation_mode": request.validation_mode,
            "minimum_functional_parity": request.minimum_functional_parity,
            "input_mode": request.input_mode,
            "simulation_options": request.simulation_options.model_dump(mode="json") if request.simulation_options else None,
            "dataset_files": [item.model_dump(mode="json") for item in request.dataset_files],
            "database_tables": request.database_tables.model_dump(mode="json") if request.database_tables else None,
        },
        max_attempts=1,
    )
    session.add(workflow)
    await session.flush()
    session.add(WorkflowEventETL(
        workflow_run_id=workflow.id,
        event_type="VALIDATION_QUEUED",
        stage_name="VALIDATION",
        status="QUEUED",
        progress_percentage=0,
        message="Validation workflow queued.",
        event_payload={
            "etl_object_ids": selected_ids,
            "conversion_workflow_id": request.conversion_workflow_id,
            "mapping_count": len(mappings),
        },
        actor_type="SYSTEM",
    ))
    await session.commit()
    return StartValidationResponse(
        workflow_id=workflow.workflow_id,
        repository_id=workflow.repository_id,
        status="QUEUED",
        mapping_count=len(mappings),
        message="Validation workflow queued.",
    )


async def get_validation_workflow(
    session: AsyncSession,
    workflow_id: str,
) -> WorkflowRunETL | None:
    row = await get_workflow_by_external_id(session, workflow_id)
    if row is None or row.job_type != "VALIDATION":
        return None
    return row


async def list_validation_mappings(
    session: AsyncSession,
    workflow_id: str,
) -> list[ValidationMappingResponse] | None:
    workflow = await get_validation_workflow(session, workflow_id)
    if workflow is None:
        return None
    result: list[ValidationMappingResponse] = []
    for mapping_id in workflow.scope_payload.get("etl_object_ids", []):
        mapping = await session.get(ETLObjectETL, mapping_id)
        if mapping is None:
            continue
        static = await _latest_agent_response(
            session,
            etl_object_id=mapping_id,
            workflow_run_id=workflow.id,
            agent_name="STATIC_VALIDATION_AGENT",
        )
        unit = await _latest_agent_response(
            session,
            etl_object_id=mapping_id,
            workflow_run_id=workflow.id,
            agent_name="UNIT_TEST_EXECUTION_AGENT",
        )
        parity = await _latest_agent_response(
            session,
            etl_object_id=mapping_id,
            workflow_run_id=workflow.id,
            agent_name="FUNCTIONAL_PARITY_AGENT",
        )
        result.append(ValidationMappingResponse(
            etl_object_id=mapping.id,
            mapping_name=mapping.object_name,
            migration_status=mapping.migration_status,
            static_validation=static.response_payload if static else None,
            unit_test_execution=unit.response_payload if unit else None,
            functional_parity=parity.response_payload if parity else None,
        ))
    return result


async def get_validation_report(
    session: AsyncSession,
    workflow_id: str,
) -> ValidationReportResponse | None:
    workflow = await get_validation_workflow(session, workflow_id)
    if workflow is None:
        return None
    mappings = await list_validation_mappings(session, workflow_id) or []
    passed_mappings = sum(
        item.migration_status == "VALIDATED" for item in mappings
    )
    passed_tests = failed_tests = skipped_tests = 0
    for item in mappings:
        unit = item.unit_test_execution or {}
        passed_tests += int(unit.get("passed", 0) or 0)
        failed_tests += int(unit.get("failed", 0) or 0)
        skipped_tests += int(unit.get("skipped", 0) or 0)
    return ValidationReportResponse(
        workflow_id=workflow.workflow_id,
        repository_id=workflow.repository_id,
        overall_status=workflow.overall_status,
        mapping_count=len(mappings),
        passed_mappings=passed_mappings,
        failed_mappings=len(mappings) - passed_mappings,
        total_tests=passed_tests + failed_tests + skipped_tests,
        passed_tests=passed_tests,
        failed_tests=failed_tests,
        skipped_tests=skipped_tests,
        mappings=mappings,
    )



async def list_validation_test_cases(
    session: AsyncSession,
    workflow_id: str,
    *,
    status: str | None = None,
    category: str | None = None,
    etl_object_id: int | None = None,
) -> list[ValidationTestCaseResponse] | None:
    workflow = await get_validation_workflow(session, workflow_id)
    if workflow is None:
        return None
    query = select(ValidationTestCaseETL).where(
        ValidationTestCaseETL.workflow_run_id == workflow.id
    )
    if status:
        query = query.where(ValidationTestCaseETL.status == status.upper())
    if category:
        query = query.where(ValidationTestCaseETL.category == category.upper())
    if etl_object_id is not None:
        query = query.where(
            ValidationTestCaseETL.etl_object_id == etl_object_id
        )
    rows = list((await session.scalars(
        query.order_by(
            ValidationTestCaseETL.etl_object_id,
            ValidationTestCaseETL.test_id,
        )
    )).all())
    mapping_ids = {
        row.etl_object_id for row in rows if row.etl_object_id is not None
    }
    names: dict[int, str] = {}
    if mapping_ids:
        mappings = (await session.scalars(
            select(ETLObjectETL).where(ETLObjectETL.id.in_(mapping_ids))
        )).all()
        names = {item.id: item.object_name for item in mappings}
    return [ValidationTestCaseResponse(
        test_case_id=row.id,
        workflow_id=workflow.workflow_id,
        etl_object_id=row.etl_object_id,
        mapping_name=names.get(row.etl_object_id),
        test_id=row.test_id,
        test_name=row.test_name,
        category=row.category,
        status=row.status,
        severity=row.severity,
        expected_result=row.expected_result,
        actual_result=row.actual_result,
        details=row.details,
        evidence_payload=row.evidence_payload or {},
        duration_seconds=round(
            (row.duration_milliseconds or 0) / 1000,
            4,
        ),
        created_at=row.created_at,
        updated_at=row.updated_at,
    ) for row in rows]


async def create_validation_database_connection(session, request):
    from etl_cc.models import ValidationDatabaseConnectionETL, DatabaseConnectionResponse
    from etl_cc.security import validation_credential_cipher
    row=ValidationDatabaseConnectionETL(connection_name=request.connection_name,database_type=request.database_type,connection_config={"host":request.host,"port":request.port,"database_name":request.database_name,"username":request.username,"ssl_mode":request.ssl_mode},credential_ciphertext=validation_credential_cipher().encrypt(request.password.get_secret_value()),credential_key_version=settings.etl_credential_key_version)
    session.add(row); await session.commit(); await session.refresh(row)
    return DatabaseConnectionResponse(connection_id=row.id,connection_name=row.connection_name,database_type=row.database_type,**{k:row.connection_config[k] for k in ("host","port","database_name","username")})


async def start_deployment(session: AsyncSession, request: StartDeploymentRequest) -> StartDeploymentResponse:
    repository = await session.get(RepositoryETL, request.repository_id)
    if repository is None: raise ValueError("Repository not found.")
    target = await session.get(RepositoryETL, request.target_repository_id)
    if target is None or target.connection_type != "GITHUB": raise ValueError("A saved GitHub repository is required as the deployment target.")
    validation = await session.scalar(select(WorkflowRunETL).where(WorkflowRunETL.workflow_id==request.validation_workflow_id,WorkflowRunETL.repository_id==request.repository_id,WorkflowRunETL.job_type=="VALIDATION",WorkflowRunETL.job_status=="COMPLETED"))
    if validation is None: raise ValueError("A completed Validation workflow was not found.")
    selected_ids=list(dict.fromkeys(request.etl_object_ids)); validation_ids=set(validation.scope_payload.get("etl_object_ids",[]))
    if not set(selected_ids).issubset(validation_ids): raise ValueError("One or more mappings are not part of the Validation workflow.")
    mappings=list((await session.scalars(select(ETLObjectETL).where(ETLObjectETL.repository_id==request.repository_id,ETLObjectETL.id.in_(selected_ids)))).all())
    if len(mappings)!=len(selected_ids): raise ValueError("One or more mappings do not belong to the repository.")
    workflows=[]
    for mapping in mappings:
        validation_agent=await _latest_agent_response(session,etl_object_id=mapping.id,workflow_run_id=validation.id,agent_name="VALIDATION_AGENT")
        payload=validation_agent.response_payload if validation_agent else {}
        if payload.get("status")!="PASSED" or not payload.get("deployable") or payload.get("oracle_strength")!="STRONG" or not payload.get("full_reference_coverage") or payload.get("human_review_required") or payload.get("unsupported_constructs"):
            raise ValueError(f"Mapping {mapping.object_name} is not eligible for Git deployment.")
        suffix=validation.workflow_id.split("-")[1][:8] if "-" in validation.workflow_id else validation.workflow_id[:8]
        clean=re.sub(r"[^A-Za-z0-9_.-]+","-",mapping.object_name).strip("-").lower()
        branch=f"{request.branch_prefix}/{clean}/{suffix}"
        workflow=WorkflowRunETL(workflow_id=f"DEP-{uuid4()}",batch_id=None,repository_id=request.repository_id,etl_object_id=mapping.id,current_stage="DEPLOYMENT",overall_status="QUEUED",job_type="DEPLOYMENT",job_status="QUEUED",scope_payload={"validation_workflow_id":validation.workflow_id,"target_repository_id":request.target_repository_id,"base_branch":request.base_branch,"repository_path":request.repository_path,"branch_name":branch,"commit_message":request.commit_message or f"Deploy validated migration for {mapping.object_name}","create_pull_request":request.create_pull_request,"pull_request_title":request.pull_request_title,"pull_request_body":request.pull_request_body},max_attempts=settings.deployment_max_attempts)
        session.add(workflow); await session.flush(); workflows.append(workflow); mapping.migration_status="DEPLOYMENT_QUEUED"
        session.add(WorkflowEventETL(workflow_run_id=workflow.id,event_type="DEPLOYMENT_QUEUED",stage_name="DEPLOYMENT",status="QUEUED",progress_percentage=0,message="Git deployment queued.",event_payload={"mapping_id":mapping.id,"branch_name":branch},actor_type="SYSTEM"))
    await session.commit()
    return StartDeploymentResponse(workflow_id=workflows[0].workflow_id,repository_id=request.repository_id,status="QUEUED",mapping_count=len(workflows),message="Git deployment workflow queued." if len(workflows)==1 else f"{len(workflows)} Git deployment workflows queued; first workflow ID returned.")

async def get_deployment_workflow(session: AsyncSession, workflow_id: str) -> WorkflowRunETL | None:
    row=await get_workflow_by_external_id(session,workflow_id)
    return row if row and row.job_type=="DEPLOYMENT" else None

async def list_deployment_mappings(session: AsyncSession, workflow_id: str) -> list[DeploymentMappingResponse] | None:
    workflow=await get_deployment_workflow(session,workflow_id)
    if workflow is None: return None
    mapping=await session.get(ETLObjectETL,workflow.etl_object_id)
    response=await _latest_agent_response(session,etl_object_id=workflow.etl_object_id,workflow_run_id=workflow.id,agent_name="GIT_DEPLOYMENT_AGENT")
    payload=response.response_payload if response else {}
    return [DeploymentMappingResponse(etl_object_id=mapping.id,mapping_name=mapping.object_name,migration_status=mapping.migration_status,branch_name=payload.get("branch_name") or workflow.scope_payload.get("branch_name"),commit_sha=payload.get("commit_sha"),pull_request_url=payload.get("pull_request_url"),deployment_status=payload.get("status") or workflow.job_status)]
