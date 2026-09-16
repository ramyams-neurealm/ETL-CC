"""Source analysis, repository persistence, and discovery services."""

import hashlib
import json
import re
from typing import Any
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_cc.config import settings
from etl_cc.economics import (
    calculate_conversion_economics,
    calculate_discovery_economics,
)
from etl_cc.ab_initio_parser import AbInitioGraphParser
from etl_cc.connectors import GitHubDeploymentTargetConnector, GitHubSource, PowerCenterSource
from etl_cc.informatica_parser import InformaticaXMLParser
from etl_cc.models import (
    AgentResponseETL,
    AgentResponseSummary,
    ArtifactContentETL,
    ArtifactContentResponse,
    ArtifactResponse,
    ComplexityDistribution,
    ConversionMappingResponse,
    DependencyPlanResponse,
    DiscoverySummaryResponse,
    ETLObjectETL,
    GeneratedArtifactETL,
    GitHubConnectionRequest,
    AbInitioGraphUploadRequest,
    MappingDiscoveryDetailsResponse,
    MappingInventoryResponse,
    MappingLineageDetailsResponse,
    PowerCenterConnectionRequest,
    RepositoryETL,
    RepositoryResponse,
    SaveRepositoryResponse,
    SourceAnalysisResponse,
    StartConversionRequest,
    StartMigrationRequest,
    StartMigrationResponse,
    StartConversionResponse,
    StartDiscoveryRequest,
    StartValidationRequest,
    StartValidationResponse,
    StartDeploymentRequest,
    StartDeploymentResponse,
    DeploymentMappingResponse,
    DeploymentTargetResponse,
    GitHubDeploymentTargetTestRequest,
    GitHubDeploymentTargetTestResponse,
    SaveDeploymentTargetRequest,
    DeploymentWorkflowReference,
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
from etl_cc.source_store import create_source, load_manifest, update_manifest


SUPPORTED_SOURCE_SELECTIONS = {
    ("INFORMATICA", "POWERCENTER"),
    ("INFORMATICA", "GITHUB"),
    ("INFORMATICA", "XML_UPLOAD"),
    ("AB_INITIO", "AB_INITIO_GRAPH_UPLOAD"),
}

def _validate_source_selection(product_code: str, method_code: str, expected_method: str) -> None:
    if method_code != expected_method:
        raise ValueError(f"This endpoint requires method_code={expected_method}.")
    if (product_code, method_code) not in SUPPORTED_SOURCE_SELECTIONS:
        raise ValueError(f"{method_code} is not supported for {product_code}.")

def _source_hash(payload: dict) -> str:
    return create_connection_configuration_hash(payload)


def _response(product_code: str, method_code: str, source_id: str, payload: dict, mappings, message: str):
    fingerprint = _source_hash({"source_id": source_id, "product_code": product_code, "method_code": method_code, **payload})
    return SourceAnalysisResponse(
        status="SUCCESS",
        message=message,
        product_code=product_code,
        method_code=method_code,
        connection_type=method_code,
        source_id=source_id,
        test_token=create_connection_test_token(fingerprint),
        expires_in_seconds=settings.connection_test_ttl_seconds,
        mapping_count=len(mappings),
        mappings=mappings,
    ), fingerprint


async def analyze_powercenter(session: AsyncSession, request: PowerCenterConnectionRequest) -> SourceAnalysisResponse:
    _validate_source_selection(request.product_code, request.method_code, "POWERCENTER")
    payload = request.model_dump(exclude={"password"}, mode="json")
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
    source_id, _ = await create_source(session, "POWERCENTER", {})
    response, fingerprint = _response(request.product_code, request.method_code, source_id, payload, mappings, "PowerCenter connection verified and mappings loaded.")
    await update_manifest(session, source_id, {
        "fingerprint": fingerprint, "product_code": request.product_code, "method_code": request.method_code, "connection_type": request.method_code,
        "connection_name": request.connection_name, "environment": request.environment,
        "config": config,
        "credential_ciphertext": credential_cipher.encrypt(request.password.get_secret_value()),
    })
    await session.commit()
    return response


async def analyze_github(session: AsyncSession, request: GitHubConnectionRequest) -> SourceAnalysisResponse:
    _validate_source_selection(request.product_code, request.method_code, "GITHUB")
    payload = request.model_dump(exclude={"access_token"}, mode="json")
    source_id, _ = await create_source(session, "GITHUB", {})
    directory = Path(settings.memory_workspace_root) / "github" / source_id
    directory.mkdir(parents=True, exist_ok=True)
    config = {
        "repository_url": str(request.repository_url),
        "branch": request.branch,
        "folder_path": request.folder_path,
    }
    token = request.access_token.get_secret_value() if request.access_token else None
    source = GitHubSource(config, token, directory)
    mappings = await source.list_mappings()
    response, fingerprint = _response(request.product_code, request.method_code, source_id, payload, mappings, "GitHub repository verified and Informatica mappings loaded.")
    await update_manifest(session, source_id, {
        "source_id": source_id,
        "fingerprint": fingerprint,
        "product_code": request.product_code,
        "method_code": request.method_code,
        "connection_type": request.method_code,
        "connection_name": request.connection_name,
        "environment": request.environment,
        "config": config,
        "credential_ciphertext": credential_cipher.encrypt(token) if token else None,
    })
    await session.commit()
    return response


async def analyze_xml_upload(
    session: AsyncSession,
    product_code: str,
    method_code: str,
    connection_name: str,
    environment: str,
    file_name: str,
    content: bytes,
) -> SourceAnalysisResponse:
    _validate_source_selection(product_code, method_code, "XML_UPLOAD")
    if not file_name.lower().endswith(".xml"):
        raise ValueError("Only .xml files are accepted.")
    if not content:
        raise ValueError("The uploaded XML file is empty.")
    parser = InformaticaXMLParser()
    mappings = parser.list_mappings_bytes(content, Path(file_name).name)
    source_id, digest = await create_source(session, "XML_UPLOAD", {}, content)
    payload = {
        "connection_name": connection_name, "environment": environment,
        "file_name": Path(file_name).name, "content_hash": digest,
    }
    response, fingerprint = _response(product_code, method_code, source_id, payload, mappings, "Informatica XML validated and mappings loaded.")
    await update_manifest(session, source_id, {
        "fingerprint": fingerprint, "product_code": product_code, "method_code": method_code, "connection_type": method_code,
        "connection_name": connection_name, "environment": environment,
        "config": {"original_file_name": Path(file_name).name, "content_hash": digest, "file_size": len(content)},
        "credential_ciphertext": None,
    })
    await session.commit()
    return response


async def analyze_ab_initio_graph(
    session: AsyncSession,
    request: AbInitioGraphUploadRequest,
    file_name: str,
    content: bytes,
) -> SourceAnalysisResponse:
    _validate_source_selection(
        request.product_code,
        request.method_code,
        "AB_INITIO_GRAPH_UPLOAD",
    )
    if not file_name.lower().endswith(".json"):
        raise ValueError("Only .json Ab Initio graph exports are accepted.")
    if not content:
        raise ValueError("The uploaded Ab Initio graph is empty.")

    parser = AbInitioGraphParser()
    source_reference = Path(file_name).name
    mappings = parser.list_mappings_bytes(content, source_reference)
    source_id, digest = await create_source(
        session,
        "AB_INITIO_GRAPH_UPLOAD",
        {},
        content,
    )
    payload = {
        "connection_name": request.connection_name,
        "environment": request.environment,
        "file_name": source_reference,
        "content_hash": digest,
    }
    response, fingerprint = _response(
        request.product_code,
        request.method_code,
        source_id,
        payload,
        mappings,
        "Ab Initio graph validated and graphs loaded.",
    )
    await update_manifest(session, source_id, {
        "fingerprint": fingerprint,
        "product_code": request.product_code,
        "method_code": request.method_code,
        "connection_type": request.method_code,
        "connection_name": request.connection_name,
        "environment": request.environment,
        "config": {
            "original_file_name": source_reference,
            "source_format": AbInitioGraphParser.FORMAT,
            "content_hash": digest,
            "file_size": len(content),
        },
        "credential_ciphertext": None,
    })
    await session.commit()
    return response


async def start_discovery(
    session: AsyncSession,
    request: StartDiscoveryRequest,
) -> SaveRepositoryResponse:
    manifest = await load_manifest(session, request.source_id)
    verify_connection_test_token(request.test_token, manifest["fingerprint"])
    if request.scope_type == "SELECTED_MAPPINGS" and not request.selected_mapping_keys:
        raise ValueError("Select at least one mapping.")
    selected_mapping_keys = (
        request.selected_mapping_keys
        if request.scope_type == "SELECTED_MAPPINGS"
        else []
    )
    repository = RepositoryETL(
        repository_name=manifest["connection_name"],
        source_type=manifest["product_code"],
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
            "product_code": manifest["product_code"],
            "method_code": manifest["method_code"],
            "connection_type": manifest["connection_type"],
            "scope_type": request.scope_type,
            "selected_mapping_keys": selected_mapping_keys,
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
        event_payload={"source_id": request.source_id, "product_code": manifest["product_code"], "method_code": manifest["method_code"], "connection_type": manifest["connection_type"]},
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


async def list_migration_events(
    session: AsyncSession,
    migration_id: str,
    after_event_id: int = 0,
    limit: int = 500,
) -> list[dict[str, Any]]:
    """Return durable parent and child workflow events for one Migration.

    Events are ordered by the globally increasing event primary key, allowing
    an SSE client to resume safely from Last-Event-ID without maintaining an
    in-memory queue in the API process.
    """
    bounded_limit = max(1, min(int(limit), 2000))
    workflow_rows = list((await session.scalars(
        select(WorkflowRunETL).where(
            (WorkflowRunETL.workflow_id == migration_id)
            | (WorkflowRunETL.batch_id == migration_id)
        )
    )).all())
    if not workflow_rows:
        return []

    workflow_by_id = {row.id: row for row in workflow_rows}
    events = list((await session.scalars(
        select(WorkflowEventETL)
        .where(
            WorkflowEventETL.workflow_run_id.in_(workflow_by_id),
            WorkflowEventETL.id > max(int(after_event_id), 0),
        )
        .order_by(WorkflowEventETL.id)
        .limit(bounded_limit)
    )).all())

    result: list[dict[str, Any]] = []
    for event in events:
        workflow = workflow_by_id[event.workflow_run_id]
        result.append({
            "event_id": event.id,
            "migration_id": migration_id,
            "workflow_id": workflow.workflow_id,
            "job_type": workflow.job_type,
            "event_type": event.event_type,
            "stage_name": event.stage_name,
            "status": event.status,
            "progress_percentage": event.progress_percentage,
            "message": event.message,
            "event_payload": event.event_payload or {},
            "created_at": event.created_at,
        })
    return result


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


async def get_discovery_dashboard(
    session: AsyncSession,
    repository_id: int,
    workflow_id: str | None = None,
) -> dict[str, Any] | None:
    """Return one deterministic snapshot for the Discovery dashboard."""
    repository = await session.get(RepositoryETL, repository_id)
    if repository is None:
        return None

    workflow_query = select(WorkflowRunETL).where(
        WorkflowRunETL.repository_id == repository_id,
        WorkflowRunETL.job_type == "DISCOVERY",
    )
    if workflow_id:
        workflow_query = workflow_query.where(
            WorkflowRunETL.workflow_id == workflow_id
        )
    workflow_query = workflow_query.order_by(WorkflowRunETL.created_at.desc())
    workflow = await session.scalar(workflow_query)

    summary = await get_discovery_summary(session, repository_id)
    mappings = await list_repository_mappings(session, repository_id)

    mapping_payloads = [item.model_dump(mode="json") for item in mappings]
    total = len(mapping_payloads)
    converted = sum(
        1
        for item in mapping_payloads
        if item.get("migration_status")
        in {"CONVERTED", "CONVERSION_COMPLETE", "VALIDATED", "DEPLOYED"}
    )
    validation_stage = sum(
        1
        for item in mapping_payloads
        if item.get("migration_status") in {"VALIDATED", "DEPLOYED"}
    )
    deployment_stage = sum(
        1
        for item in mapping_payloads
        if item.get("migration_status") == "DEPLOYED"
    )

    progress = 0
    if workflow is not None:
        latest_event = await session.scalar(
            select(WorkflowEventETL)
            .where(WorkflowEventETL.workflow_run_id == workflow.id)
            .order_by(WorkflowEventETL.id.desc())
        )
        progress = (
            latest_event.progress_percentage
            if latest_event and latest_event.progress_percentage is not None
            else (100 if workflow.overall_status == "COMPLETED" else 0)
        )

    summary_payload = (
        summary.model_dump(mode="json")
        if summary is not None
        else {
            "repository_id": repository_id,
            "total_mappings": 0,
            "need_to_discover": 0,
            "discovered": 0,
            "complexity_distribution": {
                "LOW": 0, "MEDIUM": 0, "HIGH": 0, "UNKNOWN": 0
            },
            "ready": 0,
            "review_required": 0,
            "blocked": 0,
            "dependency_cycles": 0,
            "total_input_tokens": 0,
            "total_output_tokens": 0,
        }
    )

    return {
        "repository": {
            "repository_id": repository.id,
            "repository_name": repository.repository_name,
            "product_code": repository.source_type,
            "method_code": repository.connection_type,
            "environment": repository.environment,
        },
        "workflow": None if workflow is None else {
            "workflow_id": workflow.workflow_id,
            "current_stage": workflow.current_stage,
            "overall_status": workflow.overall_status,
            "job_status": workflow.job_status,
            "progress_percentage": progress,
            "failure_stage": workflow.failure_stage,
            "failure_reason": workflow.failure_reason,
            "created_at": workflow.created_at,
            "started_at": workflow.started_at,
            "completed_at": workflow.completed_at,
        },
        "pipeline_counts": {
            "need_to_discover": summary_payload["need_to_discover"],
            "discovered": summary_payload["discovered"],
            "code_converted": converted,
            "validation_stage": validation_stage,
            "deployment_stage": deployment_stage,
            "overall_complete_percentage": (
                round((summary_payload["discovered"] / total) * 100)
                if total else 0
            ),
        },
        "discovery_summary": summary_payload,
        "mappings": mapping_payloads,
        "action_items": {
            "open": (
                summary_payload["review_required"]
                + summary_payload["blocked"]
                + summary_payload["dependency_cycles"]
            ),
            "review_required": summary_payload["review_required"],
            "blocked": summary_payload["blocked"],
            "dependency_cycles": summary_payload["dependency_cycles"],
        },
        "economics": calculate_discovery_economics(
            mappings=mapping_payloads,
            actual_input_tokens=int(
                summary_payload.get("total_input_tokens") or 0
            ),
            actual_output_tokens=int(
                summary_payload.get("total_output_tokens") or 0
            ),
        ),
    }


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


async def start_migration(
    session: AsyncSession,
    request: StartMigrationRequest,
) -> StartMigrationResponse:
    """Create a parent migration and queue Conversion.

    Critique remains part of Conversion. Validation is queued automatically by
    the Conversion worker after successful conversion and critique.
    """
    migration_id = f"MIG-{uuid4()}"
    parent = WorkflowRunETL(
        workflow_id=migration_id,
        batch_id=migration_id,
        repository_id=request.repository_id,
        current_stage="CONVERSION",
        overall_status="RUNNING",
        job_type="MIGRATION",
        job_status="RUNNING",
        scope_payload={
            "etl_object_ids": list(dict.fromkeys(request.etl_object_ids)),
            "target_platform": request.target_platform,
            "target_framework": request.target_framework,
            "follow_migration_waves": request.follow_migration_waves,
            "validation_options": request.validation_options.model_dump(mode="json"),
            "deployment_mode": "USER_CONFIGURED_GITHUB",
        },
        started_at=datetime.now(timezone.utc),
        max_attempts=1,
    )
    session.add(parent)
    await session.flush()
    session.add(WorkflowEventETL(
        workflow_run_id=parent.id,
        event_type="MIGRATION_STARTED",
        stage_name="MIGRATION",
        status="RUNNING",
        progress_percentage=0,
        message="Migration started. Conversion was queued.",
        event_payload={"etl_object_ids": request.etl_object_ids},
        actor_type="SYSTEM",
    ))

    conversion_response = await start_conversion(
        session,
        StartConversionRequest(
            repository_id=request.repository_id,
            etl_object_ids=request.etl_object_ids,
            target_platform=request.target_platform,
            target_framework=request.target_framework,
            follow_migration_waves=request.follow_migration_waves,
        ),
    )
    conversion = await get_workflow_by_external_id(
        session, conversion_response.workflow_id
    )
    conversion.batch_id = migration_id
    parent.scope_payload = {
        **parent.scope_payload,
        "conversion_workflow_id": conversion.workflow_id,
    }
    await session.commit()
    return StartMigrationResponse(
        migration_id=migration_id,
        conversion_workflow_id=conversion.workflow_id,
        repository_id=request.repository_id,
        status="QUEUED",
        mapping_count=len(set(request.etl_object_ids)),
        message="Conversion, critique, and automatic validation were started.",
    )


async def get_repository_token_utilization(
    session: AsyncSession,
    repository_id: int,
) -> dict[str, Any]:
    """Aggregate actual token usage for all repository agent attempts.

    Completed, failed, and retried attempts are included because each attempt
    may consume tokens. Deterministic agents naturally contribute zero.
    """
    rows = list(
        (
            await session.execute(
                select(
                    WorkflowRunETL.job_type,
                    AgentResponseETL.agent_name,
                    func.coalesce(
                        func.sum(AgentResponseETL.input_tokens),
                        0,
                    ),
                    func.coalesce(
                        func.sum(AgentResponseETL.output_tokens),
                        0,
                    ),
                    func.count(AgentResponseETL.id),
                )
                .join(
                    WorkflowRunETL,
                    WorkflowRunETL.id
                    == AgentResponseETL.workflow_run_id,
                )
                .where(
                    WorkflowRunETL.repository_id == repository_id
                )
                .group_by(
                    WorkflowRunETL.job_type,
                    AgentResponseETL.agent_name,
                )
                .order_by(
                    WorkflowRunETL.job_type,
                    AgentResponseETL.agent_name,
                )
            )
        ).all()
    )

    by_stage: dict[str, dict[str, int]] = {}
    by_agent: dict[str, dict[str, int]] = {}
    total_input_tokens = 0
    total_output_tokens = 0
    total_attempts = 0

    for (
        job_type,
        agent_name,
        input_tokens,
        output_tokens,
        attempt_count,
    ) in rows:
        stage_key = str(job_type or "UNKNOWN").lower()
        agent_key = str(agent_name or "UNKNOWN")
        input_value = int(input_tokens or 0)
        output_value = int(output_tokens or 0)
        attempt_value = int(attempt_count or 0)

        stage = by_stage.setdefault(
            stage_key,
            {
                "input_tokens": 0,
                "output_tokens": 0,
                "total_tokens": 0,
                "attempt_count": 0,
            },
        )
        stage["input_tokens"] += input_value
        stage["output_tokens"] += output_value
        stage["total_tokens"] += input_value + output_value
        stage["attempt_count"] += attempt_value

        agent = by_agent.setdefault(
            agent_key,
            {
                "input_tokens": 0,
                "output_tokens": 0,
                "total_tokens": 0,
                "attempt_count": 0,
            },
        )
        agent["input_tokens"] += input_value
        agent["output_tokens"] += output_value
        agent["total_tokens"] += input_value + output_value
        agent["attempt_count"] += attempt_value

        total_input_tokens += input_value
        total_output_tokens += output_value
        total_attempts += attempt_value

    return {
        "scope": "REPOSITORY_ALL_WORKFLOWS_ALL_AGENT_ATTEMPTS",
        "repository_id": repository_id,
        "input_tokens": total_input_tokens,
        "output_tokens": total_output_tokens,
        "total_tokens": total_input_tokens + total_output_tokens,
        "attempt_count": total_attempts,
        "by_stage": by_stage,
        "by_agent": by_agent,
        "includes_failed_attempts": True,
        "includes_retries": True,
        "token_utilization_saved": None,
        "savings_status": "BASELINE_NOT_CONFIGURED",
    }


async def get_migration_token_utilization(
    session: AsyncSession,
    migration_id: str,
) -> dict[str, Any]:
    """Aggregate tokens only for child workflows of one migration."""
    rows = list(
        (
            await session.execute(
                select(
                    WorkflowRunETL.job_type,
                    AgentResponseETL.agent_name,
                    func.coalesce(func.sum(AgentResponseETL.input_tokens), 0),
                    func.coalesce(func.sum(AgentResponseETL.output_tokens), 0),
                    func.count(AgentResponseETL.id),
                )
                .join(
                    WorkflowRunETL,
                    WorkflowRunETL.id == AgentResponseETL.workflow_run_id,
                )
                .where(
                    WorkflowRunETL.batch_id == migration_id,
                    WorkflowRunETL.workflow_id != migration_id,
                )
                .group_by(
                    WorkflowRunETL.job_type,
                    AgentResponseETL.agent_name,
                )
                .order_by(
                    WorkflowRunETL.job_type,
                    AgentResponseETL.agent_name,
                )
            )
        ).all()
    )
    by_stage: dict[str, dict[str, int]] = {}
    by_agent: dict[str, dict[str, int]] = {}
    total_input = total_output = total_attempts = 0
    for job_type, agent_name, input_tokens, output_tokens, attempts in rows:
        stage_key = str(job_type or "UNKNOWN").lower()
        agent_key = str(agent_name or "UNKNOWN")
        input_value = int(input_tokens or 0)
        output_value = int(output_tokens or 0)
        attempt_value = int(attempts or 0)
        for bucket in (
            by_stage.setdefault(stage_key, {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0, "attempt_count": 0}),
            by_agent.setdefault(agent_key, {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0, "attempt_count": 0}),
        ):
            bucket["input_tokens"] += input_value
            bucket["output_tokens"] += output_value
            bucket["total_tokens"] += input_value + output_value
            bucket["attempt_count"] += attempt_value
        total_input += input_value
        total_output += output_value
        total_attempts += attempt_value
    return {
        "scope": "MIGRATION_ALL_CHILD_WORKFLOWS_ALL_AGENT_ATTEMPTS",
        "migration_id": migration_id,
        "input_tokens": total_input,
        "output_tokens": total_output,
        "total_tokens": total_input + total_output,
        "attempt_count": total_attempts,
        "by_stage": by_stage,
        "by_agent": by_agent,
        "includes_failed_attempts": True,
        "includes_retries": True,
        "token_utilization_saved": None,
        "savings_status": "BASELINE_NOT_CONFIGURED",
    }


def _public_validation_files(data_files: dict[str, Any] | None) -> dict[str, str]:
    """Return safe filenames without exposing internal absolute server paths."""
    return {
        str(name): Path(str(path)).name
        for name, path in (data_files or {}).items()
        if path
    }


async def get_migration_dashboard(
    session: AsyncSession,
    migration_id: str,
) -> dict[str, Any] | None:
    parent = await session.scalar(select(WorkflowRunETL).where(
        WorkflowRunETL.workflow_id == migration_id,
        WorkflowRunETL.job_type == "MIGRATION",
    ))
    if parent is None:
        return None
    children = list((await session.scalars(
        select(WorkflowRunETL)
        .where(
            WorkflowRunETL.batch_id == migration_id,
            WorkflowRunETL.workflow_id != migration_id,
        )
        .order_by(WorkflowRunETL.created_at)
    )).all())
    conversion = next((x for x in children if x.job_type == "CONVERSION"), None)
    validation = next((x for x in children if x.job_type == "VALIDATION"), None)
    ids = parent.scope_payload.get("etl_object_ids", [])
    mappings = list((await session.scalars(
        select(ETLObjectETL)
        .where(ETLObjectETL.id.in_(ids))
        .order_by(ETLObjectETL.migration_wave, ETLObjectETL.id)
    )).all())
    all_events = []
    for workflow in [parent, *children]:
        events = await list_workflow_events(session, workflow.id, 0)
        all_events.extend({
            "event_id": e.id, "workflow_id": workflow.workflow_id,
            "job_type": workflow.job_type, "event_type": e.event_type,
            "stage_name": e.stage_name, "status": e.status,
            "progress_percentage": e.progress_percentage,
            "message": e.message, "event_payload": e.event_payload,
            "created_at": e.created_at,
        } for e in events)
    unique_events = {item["event_id"]: item for item in all_events}
    all_events = sorted(
        unique_events.values(),
        key=lambda item: (item["created_at"], item["event_id"]),
    )

    result_mappings = []
    for mapping in mappings:
        audit_rows = list((await session.scalars(
            select(AgentResponseETL)
            .join(WorkflowRunETL, WorkflowRunETL.id == AgentResponseETL.workflow_run_id)
            .where(
                (WorkflowRunETL.batch_id == migration_id) | (WorkflowRunETL.workflow_id == migration_id),
                AgentResponseETL.etl_object_id == mapping.id,
            )
            .order_by(AgentResponseETL.id)
        )).all())
        latest = {a.agent_name: a for a in audit_rows}
        critique = latest.get("CRITIQUE_AGENT")
        validation_agent = latest.get("VALIDATION_AGENT")
        deployment_agent = latest.get("GIT_DEPLOYMENT_AGENT")
        critique_payload = critique.response_payload if critique else {}
        validation_payload = validation_agent.response_payload if validation_agent else {}
        deployment_payload = deployment_agent.response_payload if deployment_agent else {}
        mapping_events = [
            event for event in all_events
            if (event.get("event_payload") or {}).get("etl_object_id") == mapping.id
        ]
        completion_event = next((
            event for event in reversed(mapping_events)
            if event.get("event_type") == "MAPPING_VALIDATION_COMPLETED"
        ), None)
        completion_payload = (completion_event.get("event_payload") or {}) if completion_event else {}
        conversion_done = mapping.migration_status in {
            "CONVERTED", "VALIDATING", "VALIDATED", "VALIDATION_FAILED",
            "DEPLOYMENT_QUEUED", "DEPLOYMENT_IN_PROGRESS", "DEPLOYED",
            "DEPLOYMENT_FAILED", "CONVERSION_FAILED", "REVIEW_REQUIRED",
        }
        validation_done = mapping.migration_status in {"VALIDATED", "VALIDATION_FAILED"}
        progress = 100 if validation_done else 60 if conversion_done else 20 if mapping.migration_status == "CONVERTING" else 0
        validation_passed = mapping.migration_status == "VALIDATED" or completion_payload.get("passed") is True
        validation_failed = mapping.migration_status == "VALIDATION_FAILED" or completion_payload.get("passed") is False
        mapping_validation_status = "COMPLETED" if validation_passed else "FAILED" if validation_failed else validation.overall_status if validation else None
        match_percentage = validation_payload.get("match_percentage", completion_payload.get("match_percentage"))
        functional_parity = validation_payload.get("functional_parity")
        if functional_parity is None and match_percentage is not None:
            functional_parity = round(float(match_percentage) / 100.0, 4)
        deployable = validation_payload.get("deployable", completion_payload.get("deployable"))
        result_mappings.append({
            "etl_object_id": mapping.id,
            "mapping_name": mapping.object_name,
            "complexity": mapping.complexity,
            "complexity_score": mapping.complexity_score,
            "migration_wave": mapping.migration_wave,
            "overall_status": mapping.migration_status,
            "progress_percentage": progress,
            "stages": {
                "conversion": None if conversion is None else {
                    "workflow_id": conversion.workflow_id,
                    "status": (
                        "FAILED" if mapping.migration_status in {"CONVERSION_FAILED", "REVIEW_REQUIRED"}
                        else "COMPLETED" if mapping.migration_status in {"CONVERTED", "VALIDATING", "VALIDATED", "VALIDATION_FAILED", "DEPLOYMENT_QUEUED", "DEPLOYMENT_IN_PROGRESS", "DEPLOYED", "DEPLOYMENT_FAILED"}
                        else "RUNNING" if mapping.migration_status == "CONVERTING"
                        else "QUEUED"
                    ),
                    "started_at": conversion.started_at,
                    "completed_at": conversion.completed_at,
                },
                "critique": None if critique is None else {
                    "status": critique.status,
                    "decision": critique_payload.get("decision"),
                    "revision_required": critique_payload.get("revision_required"),
                    "confidence": critique_payload.get("confidence"),
                },
                "validation": None if validation is None else {
                    "workflow_id": validation.workflow_id,
                    "status": mapping_validation_status,
                    "result": "PASS" if validation_passed else "FAIL" if validation_failed else "PENDING",
                    "functional_parity": functional_parity,
                    "match_percentage": match_percentage,
                    "rows_compared": validation_payload.get("rows_compared", completion_payload.get("rows_compared")),
                    "confidence_index": validation_payload.get("confidence_index", completion_payload.get("confidence_index")),
                    "deployable": deployable,
                    "failure_reason": validation_payload.get("failure_reason") or completion_payload.get("failure_reason"),
                    "recommendation": validation_payload.get("recommendation") or completion_payload.get("recommendation"),
                    "started_at": validation.started_at,
                    "completed_at": validation.completed_at,
                },
                "deployment": {
                    "status": "COMPLETED" if mapping.migration_status == "DEPLOYED" else "FAILED" if mapping.migration_status == "DEPLOYMENT_FAILED" else "RUNNING" if mapping.migration_status == "DEPLOYMENT_IN_PROGRESS" else "QUEUED" if mapping.migration_status == "DEPLOYMENT_QUEUED" else "PENDING",
                    "result": "PASS" if mapping.migration_status == "DEPLOYED" else "FAIL" if mapping.migration_status == "DEPLOYMENT_FAILED" else "PENDING",
                    "repository_url": settings.git_deployment_repository_url,
                    "base_branch": settings.git_deployment_base_branch,
                    "branch_name": deployment_payload.get("branch_name"),
                    "commit_sha": deployment_payload.get("commit_sha"),
                    "pull_request_url": deployment_payload.get("pull_request_url"),
                    "deployed_paths": deployment_payload.get("deployed_paths", []),
                    "failure_reason": deployment_payload.get("failure_reason"),
                },
            },
            "evidence": {
                "test_cases": validation_payload.get("test_cases", []),
                "stages": validation_payload.get("stages", []),
                "data_files": _public_validation_files(
                    validation_payload.get("data_files")
                    or completion_payload.get("data_files")
                ),
            },
            "event_log": mapping_events,
            "links": {
                "workbench": f"/api/v1/conversions/{conversion.workflow_id}/workbench/{mapping.id}" if conversion else None,
                "validation": f"/api/v1/validations/{validation.workflow_id}" if validation else None,
                "lineage": f"/api/v1/migrations/{migration_id}/mappings/{mapping.id}/lineage",
                "business_rules": f"/api/v1/migrations/{migration_id}/mappings/{mapping.id}/business-rules",
            },
        })
    token_usage = await get_migration_token_utilization(session, migration_id)
    mapping_progress = [
        int(item.get("progress_percentage") or 0)
        for item in result_mappings
    ]
    overall_progress = (
        round(sum(mapping_progress) / len(mapping_progress))
        if mapping_progress else 0
    )
    ready_for_deployment = (
        parent.overall_status == "COMPLETED"
        and parent.current_stage == "READY_FOR_DEPLOYMENT"
    )
    return {
        "migration": {
            "migration_id": migration_id,
            "repository_id": parent.repository_id,
            "overall_status": parent.overall_status,
            "current_stage": parent.current_stage,
            "progress_percentage": overall_progress,
            "mapping_count": len(result_mappings),
            "completed_mapping_count": sum(
                value == 100 for value in mapping_progress
            ),
            "target_platform": parent.scope_payload.get("target_platform"),
            "target_framework": parent.scope_payload.get("target_framework"),
            "deployment_status": (
                "USER_CONFIGURATION_REQUIRED"
                if ready_for_deployment else "PENDING"
            ),
            "failure_stage": parent.failure_stage,
            "failure_reason": parent.failure_reason,
        },
        "token_utilization": token_usage,
        "mappings": result_mappings,
        "event_log": all_events,
    }


async def get_migration_business_rules(session: AsyncSession, migration_id: str, etl_object_id: int) -> dict[str, Any] | None:
    parent = await get_workflow_by_external_id(session, migration_id)
    mapping = await session.get(ETLObjectETL, etl_object_id)
    if parent is None or parent.job_type != "MIGRATION" or mapping is None or mapping.id not in parent.scope_payload.get("etl_object_ids", []):
        return None
    analysis = (mapping.source_definition or {}).get("discovery_analysis", {})
    return {
        "etl_object_id": mapping.id,
        "mapping_name": mapping.object_name,
        "objective": mapping.business_purpose or analysis.get("business_purpose"),
        "rules": mapping.business_rules or analysis.get("business_rules", []),
        "assumptions": analysis.get("assumptions", []),
        "recommendations": analysis.get("migration_recommendations", []),
    }


async def get_migration_lineage(session: AsyncSession, migration_id: str, etl_object_id: int) -> dict[str, Any] | None:
    parent = await get_workflow_by_external_id(session, migration_id)
    mapping = await session.get(ETLObjectETL, etl_object_id)
    if parent is None or parent.job_type != "MIGRATION" or mapping is None or mapping.id not in parent.scope_payload.get("etl_object_ids", []):
        return None
    definition = mapping.source_definition or {}
    mapping_node = f"mapping:{mapping.id}"
    nodes = [{"id": mapping_node, "name": mapping.object_name, "node_type": "MAPPING"}]
    edges = []
    for index, item in enumerate(definition.get("sources", [])):
        node_id = f"source:{index}:{item.get('name')}"; nodes.append({"id": node_id, "name": item.get("name"), "node_type": "SOURCE"}); edges.append({"from": node_id, "to": mapping_node})
    for index, item in enumerate(definition.get("targets", [])):
        node_id = f"target:{index}:{item.get('name')}"; nodes.append({"id": node_id, "name": item.get("name"), "node_type": "TARGET"}); edges.append({"from": mapping_node, "to": node_id})
    return {"etl_object_id": mapping.id, "mapping_name": mapping.object_name, "direction": "LEFT_TO_RIGHT", "nodes": nodes, "edges": edges, "field_lineage": mapping.lineage or {}, "dependencies": mapping.dependencies or {}}


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


def _decode_artifact_content(content_row: ArtifactContentETL) -> str:
    """Return artifact content from the existing PostgreSQL content schema."""
    if content_row.content_text is not None:
        return content_row.content_text
    if content_row.content_json is not None:
        return json.dumps(
            content_row.content_json,
            indent=2,
            sort_keys=True,
            default=str,
        )
    if content_row.content_binary is not None:
        encoding = content_row.encoding or "UTF-8"
        try:
            return bytes(content_row.content_binary).decode(encoding)
        except (UnicodeDecodeError, LookupError) as exc:
            raise ValueError(
                f"Binary artifact cannot be decoded using {encoding}."
            ) from exc
    raise ValueError("Artifact content row has no populated content value.")


async def get_conversion_workbench(
    session: AsyncSession,
    workflow_id: str,
    etl_object_id: int,
) -> dict[str, Any] | None:
    """Return one complete Conversion Workbench snapshot."""
    workflow = await get_conversion_workflow(session, workflow_id)
    if workflow is None:
        return None
    mapping = await session.get(ETLObjectETL, etl_object_id)
    if mapping is None or mapping.repository_id != workflow.repository_id:
        return None
    selected_ids = set(workflow.scope_payload.get("etl_object_ids") or [])
    if selected_ids and etl_object_id not in selected_ids:
        return None

    audits = list((await session.scalars(
        select(AgentResponseETL)
        .where(
            AgentResponseETL.workflow_run_id == workflow.id,
            AgentResponseETL.etl_object_id == etl_object_id,
        )
        .order_by(AgentResponseETL.id)
    )).all())
    artifacts = list((await session.scalars(
        select(GeneratedArtifactETL)
        .where(
            GeneratedArtifactETL.workflow_run_id == workflow.id,
            GeneratedArtifactETL.etl_object_id == etl_object_id,
        )
        .order_by(GeneratedArtifactETL.id)
    )).all())

    by_agent: dict[str, AgentResponseETL] = {}
    for audit in audits:
        by_agent[audit.agent_name] = audit
    conversion = by_agent.get("CONVERSION_AGENT")
    critique = by_agent.get("CRITIQUE_AGENT")
    conversion_payload = conversion.response_payload if conversion else {}
    critique_payload = critique.response_payload if critique else {}

    started = [item.started_at for item in audits if item.started_at]
    completed = [item.completed_at for item in audits if item.completed_at]
    duration_seconds = (
        (max(completed) - min(started)).total_seconds()
        if started and completed else 0.0
    )
    input_tokens = sum(int(item.input_tokens or 0) for item in audits)
    output_tokens = sum(int(item.output_tokens or 0) for item in audits)

    artifact_payloads: list[dict[str, Any]] = []
    target_artifact: dict[str, Any] | None = None
    for artifact in artifacts:
        content = None
        content_row = await session.get(ArtifactContentETL, artifact.id)
        if content_row is not None:
            content = _decode_artifact_content(content_row)
        elif artifact.storage_path:
            path = Path(artifact.storage_path).resolve()
            root = settings.artifact_directory.resolve()
            if root in path.parents and path.is_file():
                content = path.read_text(encoding="utf-8")
        payload = {
            "artifact_id": artifact.id,
            "artifact_type": artifact.artifact_type,
            "artifact_version": artifact.artifact_version,
            "file_name": artifact.file_name,
            "media_type": artifact.media_type,
            "validation_status": artifact.validation_status,
            "is_deployable": artifact.is_deployable,
            "content": content,
        }
        artifact_payloads.append(payload)
        if artifact.artifact_type == "PYSPARK_CODE":
            target_artifact = payload

    code = (target_artifact or {}).get("content") or ""
    lines_converted = len([line for line in code.splitlines() if line.strip()])
    recommendations = [
        item.get("recommendation")
        for item in critique_payload.get("issues", [])
        if item.get("recommendation")
    ]
    recommendations.extend(conversion_payload.get("manual_actions") or [])

    mapping_for_economics = {
        "etl_object_id": mapping.id,
        "complexity_score": mapping.complexity_score,
        "transformation_count": mapping.transformation_count,
        "human_review_required": bool(
            critique_payload.get("revision_required", False)
        ),
        "status": (
            "BLOCKED" if mapping.migration_status == "BLOCKED" else "READY"
        ),
        "dependencies": mapping.dependencies or {},
    }

    return {
        "workflow": {
            "workflow_id": workflow.workflow_id,
            "repository_id": workflow.repository_id,
            "current_stage": workflow.current_stage,
            "overall_status": workflow.overall_status,
            "job_status": workflow.job_status,
            "failure_stage": workflow.failure_stage,
            "failure_reason": workflow.failure_reason,
        },
        "mapping": {
            "etl_object_id": mapping.id,
            "mapping_name": mapping.object_name,
            "folder_name": mapping.folder_path,
            "source_object_key": mapping.source_object_key,
            "complexity": mapping.complexity,
            "complexity_score": mapping.complexity_score,
            "migration_wave": mapping.migration_wave,
            "migration_status": mapping.migration_status,
            "target_platform": workflow.scope_payload.get("target_platform"),
            "target_framework": workflow.scope_payload.get("target_framework"),
        },
        "source": {
            "display_type": "CANONICAL_MAPPING",
            "source_object_key": mapping.source_object_key,
            "source_definition": mapping.source_definition or {},
        },
        "target": target_artifact,
        "agent_performance": {
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "total_tokens": input_tokens + output_tokens,
            "duration_seconds": round(duration_seconds, 2),
            "duration_display": (
                f"{int(duration_seconds // 60)}m {int(duration_seconds % 60)}s"
            ),
            "model_names": sorted({
                item.model_name for item in audits if item.model_name
            }),
            "agent_cost": None,
            "agent_cost_status": "TOKEN_PRICING_NOT_CONFIGURED",
        },
        "economics": calculate_conversion_economics(
            mapping_for_economics, duration_seconds
        ),
        "quality": {
            "lines_converted": lines_converted,
            "confidence": critique_payload.get(
                "confidence", conversion_payload.get("confidence")
            ),
            "critique_decision": critique_payload.get("decision"),
            "revision_required": critique_payload.get("revision_required"),
            "business_rule_coverage": critique_payload.get(
                "business_rule_coverage"
            ),
            "lineage_coverage": critique_payload.get("lineage_coverage"),
            "code_coverage_accuracy": None,
            "coverage_status": "PENDING_VALIDATION",
            "deterministic_checks": critique_payload.get(
                "deterministic_checks", []
            ),
        },
        "recommendations": list(dict.fromkeys(recommendations)),
        "conversion": conversion_payload or None,
        "critique": critique_payload or None,
        "artifacts": artifact_payloads,
        "can_proceed_to_validation": bool(
            workflow.overall_status == "COMPLETED"
            and critique_payload.get("decision") == "PASS"
            and not critique_payload.get("revision_required", True)
        ),
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

    content: str | None = None
    content_row = await session.get(ArtifactContentETL, artifact.id)
    if content_row is not None:
        content = _decode_artifact_content(content_row)
    elif artifact.storage_path:
        artifact_path = Path(artifact.storage_path).resolve()
        artifact_root = settings.artifact_directory.resolve()
        if artifact_root in artifact_path.parents and artifact_path.is_file():
            content = artifact_path.read_text(encoding="utf-8")

    if content is None:
        raise ValueError("Artifact content is unavailable.")

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
        content=content,
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


async def test_github_deployment_target(
    session: AsyncSession,
    request: GitHubDeploymentTargetTestRequest,
) -> GitHubDeploymentTargetTestResponse:
    token = request.access_token.get_secret_value() if request.access_token else None
    connector = GitHubDeploymentTargetConnector(
        str(request.repository_url), request.base_branch, token
    )
    capabilities = await connector.test()
    if not capabilities["can_push"]:
        raise ValueError("The GitHub credential does not have repository push permission.")
    target_id, _ = await create_source(session, "GITHUB_DEPLOYMENT_TARGET", {})
    config = {
        "repository_role": "DEPLOYMENT_TARGET",
        "repository_url": str(request.repository_url),
        "branch": request.base_branch,
        "capabilities": capabilities,
    }
    fingerprint = create_connection_configuration_hash({
        "connection_name": request.connection_name,
        "environment": request.environment,
        **config,
    })
    await update_manifest(session, target_id, {
        "fingerprint": fingerprint,
        "connection_name": request.connection_name,
        "environment": request.environment,
        "connection_type": "GITHUB",
        "config": config,
        "credential_ciphertext": credential_cipher.encrypt(token) if token else None,
    })
    await session.commit()
    return GitHubDeploymentTargetTestResponse(
        status="SUCCESS",
        target_id=target_id,
        test_token=create_connection_test_token(fingerprint),
        expires_in_seconds=settings.connection_test_ttl_seconds,
        repository_url=str(request.repository_url),
        base_branch=request.base_branch,
        **capabilities,
    )


async def save_github_deployment_target(
    session: AsyncSession,
    request: SaveDeploymentTargetRequest,
) -> DeploymentTargetResponse:
    manifest = await load_manifest(session, request.target_id)
    if manifest.get("connection_type") != "GITHUB":
        raise ValueError("The tested target is not a GitHub deployment target.")
    config = manifest.get("config") or {}
    if config.get("repository_role") != "DEPLOYMENT_TARGET":
        raise ValueError("The tested GitHub connection is not a deployment target.")
    verify_connection_test_token(request.test_token, manifest["fingerprint"])
    repository = RepositoryETL(
        repository_name=manifest["connection_name"],
        source_type="DEPLOYMENT_TARGET",
        connection_type="GITHUB",
        environment=manifest["environment"],
        connection_config=config,
        credential_ciphertext=manifest.get("credential_ciphertext"),
        credential_algorithm="FERNET" if manifest.get("credential_ciphertext") else None,
        credential_key_version=settings.etl_credential_key_version if manifest.get("credential_ciphertext") else None,
        connection_status="CONNECTED",
        last_tested_at=datetime.now(timezone.utc),
    )
    session.add(repository)
    await session.commit()
    await session.refresh(repository)
    return _deployment_target_response(repository)


def _deployment_target_response(repository: RepositoryETL) -> DeploymentTargetResponse:
    config = repository.connection_config or {}
    return DeploymentTargetResponse(
        target_repository_id=repository.id,
        connection_name=repository.repository_name,
        provider="GITHUB",
        environment=repository.environment,
        repository_url=config.get("repository_url", ""),
        base_branch=config.get("branch", "main"),
        connection_status=repository.connection_status,
        capabilities=config.get("capabilities") or {},
        created_at=repository.created_at,
    )


async def list_deployment_targets(session: AsyncSession) -> list[DeploymentTargetResponse]:
    rows = list((await session.scalars(
        select(RepositoryETL).where(
            RepositoryETL.connection_type == "GITHUB",
            RepositoryETL.connection_config["repository_role"].astext == "DEPLOYMENT_TARGET",
        ).order_by(RepositoryETL.created_at.desc())
    )).all())
    return [_deployment_target_response(row) for row in rows]


async def get_deployment_target(
    session: AsyncSession, target_repository_id: int
) -> DeploymentTargetResponse | None:
    row = await session.get(RepositoryETL, target_repository_id)
    if row is None or row.connection_type != "GITHUB":
        return None
    if (row.connection_config or {}).get("repository_role") != "DEPLOYMENT_TARGET":
        return None
    return _deployment_target_response(row)


async def start_deployment(
    session: AsyncSession,
    request: StartDeploymentRequest,
) -> StartDeploymentResponse:
    repository = await session.get(RepositoryETL, request.repository_id)
    if repository is None:
        raise ValueError("Repository not found.")

    target = await session.get(RepositoryETL, request.target_repository_id)
    if target is None or target.connection_type != "GITHUB":
        raise ValueError("A saved GitHub deployment target is required.")
    target_config = target.connection_config or {}
    if target_config.get("repository_role") != "DEPLOYMENT_TARGET":
        raise ValueError("The selected GitHub repository is not registered as a deployment target.")
    if target.connection_status != "CONNECTED":
        raise ValueError("The selected GitHub deployment target is not connected.")
    if not target_config.get("repository_url"):
        raise ValueError("The saved GitHub target has no repository URL.")
    capabilities = target_config.get("capabilities") or {}
    if not capabilities.get("can_push"):
        raise ValueError("The saved GitHub target does not have push capability.")
    if request.create_pull_request and not capabilities.get("can_create_pull_request"):
        raise ValueError("The saved GitHub target cannot create pull requests.")

    validation = await session.scalar(
        select(WorkflowRunETL).where(
            WorkflowRunETL.workflow_id == request.validation_workflow_id,
            WorkflowRunETL.repository_id == request.repository_id,
            WorkflowRunETL.job_type == "VALIDATION",
            WorkflowRunETL.job_status == "COMPLETED",
        )
    )
    if validation is None:
        raise ValueError("A completed Validation workflow was not found.")
    migration_id = validation.batch_id
    if not migration_id:
        raise ValueError("The Validation workflow is not associated with a Migration.")
    migration = await session.scalar(
        select(WorkflowRunETL).where(
            WorkflowRunETL.workflow_id == migration_id,
            WorkflowRunETL.job_type == "MIGRATION",
        )
    )
    if migration is None:
        raise ValueError("The parent Migration workflow was not found.")

    selected_ids = list(dict.fromkeys(request.etl_object_ids))
    validation_ids = set(validation.scope_payload.get("etl_object_ids", []))
    if not set(selected_ids).issubset(validation_ids):
        raise ValueError("One or more mappings are not part of the Validation workflow.")
    mappings = list((await session.scalars(
        select(ETLObjectETL).where(
            ETLObjectETL.repository_id == request.repository_id,
            ETLObjectETL.id.in_(selected_ids),
        )
    )).all())
    if len(mappings) != len(selected_ids):
        raise ValueError("One or more mappings do not belong to the repository.")

    workflows: list[WorkflowRunETL] = []
    references: list[DeploymentWorkflowReference] = []
    for mapping in mappings:
        validation_agent = await _latest_agent_response(
            session,
            etl_object_id=mapping.id,
            workflow_run_id=validation.id,
            agent_name="VALIDATION_AGENT",
        )
        payload = validation_agent.response_payload if validation_agent else {}
        eligible = (
            payload.get("status") == "PASSED"
            and bool(payload.get("deployable"))
            and payload.get("oracle_strength") == "STRONG"
            and bool(payload.get("full_reference_coverage"))
            and not payload.get("human_review_required")
            and not payload.get("unsupported_constructs")
        )
        if not eligible:
            raise ValueError(f"Mapping {mapping.object_name} is not eligible for Git deployment.")

        suffix = validation.workflow_id.split("-")[1][:8] if "-" in validation.workflow_id else validation.workflow_id[:8]
        clean = re.sub(r"[^A-Za-z0-9_.-]+", "-", mapping.object_name).strip("-").lower()
        branch = f"{request.branch_prefix}/{clean}/{suffix}"
        workflow = WorkflowRunETL(
            workflow_id=f"DEP-{uuid4()}",
            batch_id=migration_id,
            repository_id=request.repository_id,
            etl_object_id=mapping.id,
            current_stage="DEPLOYMENT",
            overall_status="QUEUED",
            job_type="DEPLOYMENT",
            job_status="QUEUED",
            scope_payload={
                "migration_id": migration_id,
                "validation_workflow_id": validation.workflow_id,
                "target_repository_id": request.target_repository_id,
                "base_branch": request.base_branch,
                "repository_path": request.repository_path,
                "branch_name": branch,
                "commit_message": request.commit_message or f"Deploy validated migration for {mapping.object_name}",
                "create_pull_request": request.create_pull_request,
                "pull_request_title": request.pull_request_title,
                "pull_request_body": request.pull_request_body,
            },
            max_attempts=settings.deployment_max_attempts,
        )
        session.add(workflow)
        await session.flush()
        workflows.append(workflow)
        references.append(DeploymentWorkflowReference(
            etl_object_id=mapping.id,
            mapping_name=mapping.object_name,
            workflow_id=workflow.workflow_id,
            branch_name=branch,
        ))
        mapping.migration_status = "DEPLOYMENT_QUEUED"
        session.add(WorkflowEventETL(
            workflow_run_id=workflow.id,
            event_type="DEPLOYMENT_QUEUED",
            stage_name="DEPLOYMENT",
            status="QUEUED",
            progress_percentage=0,
            message="Git deployment queued.",
            event_payload={
                "migration_id": migration_id,
                "etl_object_id": mapping.id,
                "mapping_id": mapping.id,
                "mapping_name": mapping.object_name,
                "branch_name": branch,
            },
            actor_type="SYSTEM",
        ))

    migration.current_stage = "DEPLOYMENT_QUEUED"
    migration.overall_status = "RUNNING"
    migration.job_status = "RUNNING"
    migration.completed_at = None
    session.add(WorkflowEventETL(
        workflow_run_id=migration.id,
        event_type="MIGRATION_DEPLOYMENT_QUEUED",
        stage_name="MIGRATION",
        status="RUNNING",
        progress_percentage=90,
        message="Validated mappings were queued for Git deployment.",
        event_payload={
            "migration_id": migration_id,
            "deployment_workflow_ids": [item.workflow_id for item in workflows],
            "mapping_count": len(workflows),
        },
        actor_type="SYSTEM",
    ))
    await session.commit()
    return StartDeploymentResponse(
        workflow_id=workflows[0].workflow_id,
        workflow_ids=[item.workflow_id for item in workflows],
        migration_id=migration_id,
        repository_id=request.repository_id,
        status="QUEUED",
        mapping_count=len(workflows),
        deployments=references,
        message=f"{len(workflows)} Git deployment workflow(s) queued.",
    )

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
