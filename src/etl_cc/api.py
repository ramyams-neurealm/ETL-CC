"""REST APIs for source ingestion, discovery, conversion, and validation."""

from decimal import Decimal, InvalidOperation
import asyncio
import json


from fastapi import (
    APIRouter,
    Depends,
    File,
    Form,
    HTTPException,
    Request,
    UploadFile,
    status,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.responses import StreamingResponse

from etl_cc.agents.matchflow_comparator import MatchFlowComparator
from etl_cc.connectors import ETLConnectorError
from etl_cc.config import settings
from etl_cc.database import SessionFactory, get_session
from etl_cc.models import (
    AgentResponseSummary,
    ArtifactContentResponse,
    ArtifactResponse,
    ConversionMappingResponse,
    DependencyPlanResponse,
    DiscoverySummaryResponse,
    GitHubConnectionRequest,
    AbInitioGraphUploadRequest,
    MappingDiscoveryDetailsResponse,
    MappingInventoryResponse,
    MappingLineageDetailsResponse,
    PowerCenterConnectionRequest,
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
    ValidationMappingResponse,
    ValidationReportResponse,
    ValidationTestCaseResponse,
    ValidationDatasetUploadResponse,
    DatabaseConnectionCreateRequest,
    DatabaseConnectionResponse,
    StartDeploymentRequest,
    StartDeploymentResponse,
    DeploymentMappingResponse,
    DeploymentTargetResponse,
    GitHubDeploymentTargetTestRequest,
    GitHubDeploymentTargetTestResponse,
    SaveDeploymentTargetRequest,
    ETLProductsResponse,
    ETLProductResponse,
    ETLIngestionMethodResponse,
    EnvironmentType,
)
from etl_cc.security import ConnectionTestTokenError
from etl_cc.services import (
    analyze_github,
    analyze_ab_initio_graph,
    analyze_powercenter,
    analyze_xml_upload,
    get_artifact_content,
    get_conversion_workflow,
    get_conversion_workbench,
    get_discovery_summary,
    get_discovery_dashboard,
    get_mapping_conversion,
    get_mapping_discovery_details,
    get_mapping_lineage_details,
    get_repository,
    get_validation_report,
    get_validation_workflow,
    get_workflow_by_external_id,
    get_workflow_dependency_plan,
    list_conversion_mappings,
    list_mapping_agent_responses,
    list_mapping_artifacts,
    list_repositories,
    list_repository_mappings,
    list_validation_mappings,
    list_validation_test_cases,
    list_workflow_events,
    list_migration_events,
    start_conversion,
    start_migration,
    get_migration_dashboard,
    get_migration_business_rules,
    get_migration_lineage,
    start_discovery,
    start_validation,
    create_validation_database_connection,
    start_deployment,
    get_deployment_workflow,
    list_deployment_mappings,
    get_deployment_target,
    list_deployment_targets,
    save_github_deployment_target,
    test_github_deployment_target,
)
from etl_cc.validation_store import validation_store

router = APIRouter()


def _workflow_payload(row) -> dict:
    return {
        "workflow_id": row.workflow_id,
        "repository_id": row.repository_id,
        "current_stage": row.current_stage,
        "overall_status": row.overall_status,
        "job_status": row.job_status,
        "scope_payload": row.scope_payload,
        "failure_stage": row.failure_stage,
        "failure_reason": row.failure_reason,
        "created_at": row.created_at,
        "started_at": row.started_at,
        "completed_at": row.completed_at,
    }




@router.get(
    "/etl-products",
    response_model=ETLProductsResponse,
    tags=["Discovery"],
    summary="List supported ETL products and ingestion methods",
)
async def list_etl_products() -> ETLProductsResponse:
    """Return deployed ETL capabilities for the Discovery wizard.

    This endpoint is configuration-backed and requires no database table.
    Disabled products remain visible so the UI can display them as unavailable.
    """
    informatica_enabled = bool(settings.enable_informatica)
    ab_initio_enabled = bool(settings.enable_ab_initio)

    methods = [
        ETLIngestionMethodResponse(
            method_code="POWERCENTER",
            method_name="PowerCenter Live Repository",
            description=(
                "Connect to an Informatica PowerCenter repository and load "
                "available mappings."
            ),
            enabled=(
                informatica_enabled
                and settings.enable_informatica_powercenter
            ),
            disabled_reason=(
                None
                if informatica_enabled
                and settings.enable_informatica_powercenter
                else "This ingestion method is not enabled."
            ),
            display_order=1,
        ),
        ETLIngestionMethodResponse(
            method_code="GITHUB",
            method_name="GitHub Repository",
            description=(
                "Analyze Informatica XML exports stored in a GitHub "
                "repository."
            ),
            enabled=(
                informatica_enabled
                and settings.enable_informatica_github
            ),
            disabled_reason=(
                None
                if informatica_enabled
                and settings.enable_informatica_github
                else "This ingestion method is not enabled."
            ),
            display_order=2,
        ),
        ETLIngestionMethodResponse(
            method_code="XML_UPLOAD",
            method_name="Direct XML Upload",
            description=(
                "Upload an Informatica PowerCenter XML export directly."
            ),
            enabled=(
                informatica_enabled
                and settings.enable_informatica_xml_upload
            ),
            disabled_reason=(
                None
                if informatica_enabled
                and settings.enable_informatica_xml_upload
                else "This ingestion method is not enabled."
            ),
            display_order=3,
        ),
    ]

    return ETLProductsResponse(
        products=[
            ETLProductResponse(
                product_code="INFORMATICA",
                product_name="Informatica",
                description="PowerCenter mappings",
                enabled=informatica_enabled and any(
                    method.enabled for method in methods
                ),
                disabled_reason=(
                    None
                    if informatica_enabled and any(
                        method.enabled for method in methods
                    )
                    else "Informatica ingestion is not enabled."
                ),
                icon_key="informatica",
                display_order=1,
                ingestion_methods=methods,
            ),
            ETLProductResponse(
                product_code="DATASTAGE",
                product_name="IBM DataStage",
                description="DataStage jobs",
                enabled=True,
                disabled_reason=None,
                icon_key="datastage",
                display_order=2,
                ingestion_methods=[],
            ),
            ETLProductResponse(
                product_code="AB_INITIO",
                product_name="Ab Initio",
                description="Ab Initio graphs",
                enabled=ab_initio_enabled,
                disabled_reason=(
                    None
                    if ab_initio_enabled
                    else "Ab Initio ingestion is not enabled."
                ),
                icon_key="ab-initio",
                display_order=3,
                ingestion_methods=[
                    ETLIngestionMethodResponse(
                        method_code="AB_INITIO_GRAPH_UPLOAD",
                        method_name="Ab Initio Graph Upload",
                        description="Analyze a portable Ab Initio graph export.",
                        enabled=ab_initio_enabled,
                        disabled_reason=(
                            None
                            if ab_initio_enabled
                            else "Ab Initio ingestion is not enabled."
                        ),
                        display_order=1,
                    )
                ],
            ),
        ]
    )


@router.get("/health", tags=["Health"])
async def health() -> dict[str, str]:
    return {"status": "ok"}


@router.post(
    "/sources/powercenter/test",
    response_model=SourceAnalysisResponse,
    tags=["Sources"],
)
async def test_powercenter(
    request: PowerCenterConnectionRequest,
    session: AsyncSession = Depends(get_session),
):
    try:
        return await analyze_powercenter(session, request)
    except ValueError as exc:
        await session.rollback()
        raise HTTPException(
            status_code=400,
            detail={
                "status": "FAILED",
                "stage": "SOURCE_SELECTION",
                "reason": str(exc),
            },
        ) from exc
    except ETLConnectorError as exc:
        await session.rollback()
        raise HTTPException(
            status_code=502,
            detail={
                "status": "FAILED",
                "stage": "POWERCENTER",
                "reason": str(exc),
            },
        ) from exc


@router.post(
    "/sources/github/test",
    response_model=SourceAnalysisResponse,
    tags=["Sources"],
)
async def test_github(
    request: GitHubConnectionRequest,
    session: AsyncSession = Depends(get_session),
):
    try:
        return await analyze_github(session, request)
    except ValueError as exc:
        await session.rollback()
        raise HTTPException(
            status_code=400,
            detail={
                "status": "FAILED",
                "stage": "SOURCE_SELECTION",
                "reason": str(exc),
            },
        ) from exc
    except ETLConnectorError as exc:
        await session.rollback()
        raise HTTPException(
            status_code=502,
            detail={
                "status": "FAILED",
                "stage": "GITHUB",
                "reason": str(exc),
            },
        ) from exc


@router.post(
    "/sources/xml/analyze",
    response_model=SourceAnalysisResponse,
    tags=["Sources"],
)
async def analyze_xml(
    product_code: str = Form(...),
    method_code: str = Form(...),
    connection_name: str = Form(...),
    file: UploadFile = File(...),
    environment: str = Form("DEV"),
    session: AsyncSession = Depends(get_session),
):
    try:
        if environment not in {"DEV", "STAGING", "PROD"}:
            raise ValueError("Environment must be DEV, STAGING, or PROD.")
        return await analyze_xml_upload(
            session,
            product_code,
            method_code,
            connection_name,
            environment,
            file.filename or "upload.xml",
            await file.read(),
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail={
                "status": "FAILED",
                "stage": "XML_VALIDATION",
                "reason": str(exc),
            },
        ) from exc


@router.post(
    "/sources/ab-initio/analyze",
    response_model=SourceAnalysisResponse,
    tags=["Sources"],
)
async def analyze_ab_initio(
    connection_name: str = Form(...),
    environment: EnvironmentType = Form(...),
    file: UploadFile = File(...),
    session: AsyncSession = Depends(get_session),
):
    try:
        if not settings.enable_ab_initio:
            raise ValueError("Ab Initio ingestion is not enabled.")
        request = AbInitioGraphUploadRequest(
            connection_name=connection_name,
            environment=environment,
        )
        return await analyze_ab_initio_graph(
            session,
            request,
            file.filename or "ab_initio_graph.json",
            await file.read(),
        )
    except ValueError as exc:
        await session.rollback()
        raise HTTPException(
            status_code=400,
            detail={
                "status": "FAILED",
                "stage": "AB_INITIO_VALIDATION",
                "reason": str(exc),
            },
        ) from exc


@router.post(
    "/discoveries",
    response_model=SaveRepositoryResponse,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["Discovery"],
)
async def create_discovery(
    request: StartDiscoveryRequest,
    session: AsyncSession = Depends(get_session),
):
    try:
        return await start_discovery(session, request)
    except (
        ValueError,
        FileNotFoundError,
        ConnectionTestTokenError,
    ) as exc:
        await session.rollback()
        raise HTTPException(
            status_code=400,
            detail={
                "status": "FAILED",
                "stage": "DISCOVERY_START",
                "reason": str(exc),
            },
        ) from exc
    except IntegrityError as exc:
        await session.rollback()
        raise HTTPException(
            status_code=409,
            detail={
                "status": "FAILED",
                "reason": (
                    "A source with this name and environment already exists."
                ),
            },
        ) from exc


@router.get(
    "/repositories",
    response_model=list[RepositoryResponse],
    tags=["Repositories"],
)
async def repositories(session: AsyncSession = Depends(get_session)):
    return await list_repositories(session)


@router.get(
    "/repositories/{repository_id}",
    response_model=RepositoryResponse,
    tags=["Repositories"],
)
async def repository(
    repository_id: int,
    session: AsyncSession = Depends(get_session),
):
    value = await get_repository(session, repository_id)
    if value is None:
        raise HTTPException(status_code=404, detail="Repository not found.")
    return value


@router.get(
    "/repositories/{repository_id}/mappings",
    response_model=list[MappingInventoryResponse],
    tags=["Mappings"],
)
async def mappings(
    repository_id: int,
    session: AsyncSession = Depends(get_session),
):
    return await list_repository_mappings(session, repository_id)


@router.get("/workflows/{workflow_id}", tags=["Workflows"])
async def workflow_status(
    workflow_id: str,
    session: AsyncSession = Depends(get_session),
):
    row = await get_workflow_by_external_id(session, workflow_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Workflow not found.")
    return _workflow_payload(row)


@router.get("/workflows/{workflow_id}/events", tags=["Workflows"])
async def workflow_events(
    workflow_id: str,
    after_event_id: int = 0,
    session: AsyncSession = Depends(get_session),
):
    row = await get_workflow_by_external_id(session, workflow_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Workflow not found.")
    events = await list_workflow_events(
        session,
        row.id,
        after_event_id,
    )
    return [
        {
            "event_id": item.id,
            "workflow_id": row.workflow_id,
            "event_type": item.event_type,
            "stage_name": item.stage_name,
            "status": item.status,
            "progress_percentage": item.progress_percentage,
            "message": item.message,
            "event_payload": item.event_payload,
            "created_at": item.created_at,
        }
        for item in events
    ]


@router.get(
    "/repositories/{repository_id}/discovery-dashboard",
    tags=["Discovery"],
    summary="Get the complete Discovery dashboard snapshot",
)
async def discovery_dashboard(
    repository_id: int,
    workflow_id: str | None = None,
    session: AsyncSession = Depends(get_session),
):
    payload = await get_discovery_dashboard(
        session, repository_id, workflow_id
    )
    if payload is None:
        raise HTTPException(status_code=404, detail="Repository not found.")
    return payload


def _sse_message(
    *, event: str, data: dict, event_id: int | None = None
) -> str:
    lines: list[str] = []
    if event_id is not None:
        lines.append(f"id: {event_id}")
    lines.append(f"event: {event}")
    lines.append("data: " + json.dumps(data, default=str, separators=(",", ":")))
    return "\n".join(lines) + "\n\n"


def _sse_cursor(request: Request, after_event_id: int) -> int:
    """Resolve the standard SSE reconnect cursor and query fallback."""
    raw_header = request.headers.get("last-event-id", "").strip()
    try:
        header_cursor = int(raw_header) if raw_header else 0
    except ValueError:
        header_cursor = 0
    return max(int(after_event_id), header_cursor, 0)


def _sse_event_name(payload: dict) -> str:
    return (
        "mapping_updated"
        if payload.get("etl_object_id") is not None
        else "workflow_progress"
    )


@router.get(
    "/workflows/{workflow_id}/events/stream",
    tags=["Workflows"],
    summary="Stream workflow events using Server-Sent Events",
)
async def workflow_event_stream(
    workflow_id: str,
    request: Request,
    after_event_id: int = 0,
):
    async with SessionFactory() as session:
        workflow = await get_workflow_by_external_id(session, workflow_id)
        if workflow is None:
            raise HTTPException(status_code=404, detail="Workflow not found.")

    async def generate():
        last_event_id = _sse_cursor(request, after_event_id)
        heartbeat_ticks = 0
        while True:
            if await request.is_disconnected():
                break

            async with SessionFactory() as session:
                workflow = await get_workflow_by_external_id(session, workflow_id)
                if workflow is None:
                    yield _sse_message(
                        event="workflow_failed",
                        data={"workflow_id": workflow_id, "reason": "Workflow not found."},
                    )
                    break
                events = await list_workflow_events(
                    session, workflow.id, last_event_id
                )

            for item in events:
                last_event_id = item.id
                payload = {
                    "event_id": item.id,
                    "workflow_id": workflow_id,
                    "event_type": item.event_type,
                    "stage_name": item.stage_name,
                    "status": item.status,
                    "progress_percentage": item.progress_percentage,
                    "message": item.message,
                    "event_payload": item.event_payload or {},
                    "created_at": item.created_at,
                }
                event_name = _sse_event_name(item.event_payload or {})
                yield _sse_message(
                    event=event_name, data=payload, event_id=item.id
                )

            if workflow.overall_status in {"COMPLETED", "FAILED", "CANCELLED"}:
                terminal = (
                    "workflow_completed"
                    if workflow.overall_status == "COMPLETED"
                    else "workflow_cancelled"
                    if workflow.overall_status == "CANCELLED"
                    else "workflow_failed"
                )
                yield _sse_message(
                    event=terminal,
                    data=_workflow_payload(workflow),
                )
                break

            heartbeat_ticks += 1
            if heartbeat_ticks >= 10:
                yield _sse_message(
                    event="heartbeat",
                    data={"workflow_id": workflow_id, "last_event_id": last_event_id},
                )
                heartbeat_ticks = 0
            await asyncio.sleep(1)

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get(
    "/migrations/{migration_id}/events/stream",
    tags=["Migration"],
    summary="Stream all Migration and child-workflow events using SSE",
)
async def migration_event_stream(
    migration_id: str,
    request: Request,
    after_event_id: int = 0,
):
    """Stream one Migration across parent, Conversion, Validation, and Deployment."""
    async with SessionFactory() as session:
        migration = await get_workflow_by_external_id(session, migration_id)
        if migration is None or migration.job_type != "MIGRATION":
            raise HTTPException(status_code=404, detail="Migration not found.")

    async def generate():
        last_event_id = _sse_cursor(request, after_event_id)
        heartbeat_ticks = 0
        yield _sse_message(
            event="connected",
            data={
                "migration_id": migration_id,
                "last_event_id": last_event_id,
            },
        )

        while True:
            if await request.is_disconnected():
                break

            async with SessionFactory() as session:
                migration = await get_workflow_by_external_id(session, migration_id)
                if migration is None or migration.job_type != "MIGRATION":
                    yield _sse_message(
                        event="migration_failed",
                        data={
                            "migration_id": migration_id,
                            "reason": "Migration not found.",
                        },
                    )
                    break
                events = await list_migration_events(
                    session,
                    migration_id,
                    last_event_id,
                )

            for item in events:
                last_event_id = int(item["event_id"])
                yield _sse_message(
                    event=_sse_event_name(item.get("event_payload") or {}),
                    data=item,
                    event_id=last_event_id,
                )

            if migration.overall_status in {"COMPLETED", "FAILED", "CANCELLED"}:
                yield _sse_message(
                    event="migration_terminal",
                    data={
                        **_workflow_payload(migration),
                        "migration_id": migration_id,
                        "last_event_id": last_event_id,
                    },
                )
                break

            heartbeat_ticks += 1
            if heartbeat_ticks >= 10:
                yield ": keep-alive\n\n"
                heartbeat_ticks = 0
            await asyncio.sleep(1)

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get(
    "/repositories/{repository_id}/discovery-summary",
    response_model=DiscoverySummaryResponse,
    tags=["Discovery & Insights"],
)
async def discovery_summary(
    repository_id: int,
    session: AsyncSession = Depends(get_session),
):
    result = await get_discovery_summary(session, repository_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Repository not found.")
    return result


@router.get(
    "/mappings/{etl_object_id}/discovery",
    response_model=MappingDiscoveryDetailsResponse,
    tags=["Discovery & Insights"],
)
async def mapping_discovery(
    etl_object_id: int,
    session: AsyncSession = Depends(get_session),
):
    result = await get_mapping_discovery_details(session, etl_object_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Mapping not found.")
    return result


@router.get(
    "/mappings/{etl_object_id}/lineage",
    response_model=MappingLineageDetailsResponse,
    tags=["Discovery & Insights"],
)
async def mapping_lineage(
    etl_object_id: int,
    session: AsyncSession = Depends(get_session),
):
    result = await get_mapping_lineage_details(session, etl_object_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Mapping not found.")
    return result


@router.get(
    "/workflows/{workflow_id}/dependency-plan",
    response_model=DependencyPlanResponse,
    tags=["Discovery & Insights"],
)
async def dependency_plan(
    workflow_id: str,
    session: AsyncSession = Depends(get_session),
):
    result = await get_workflow_dependency_plan(session, workflow_id)
    if result is None:
        raise HTTPException(
            status_code=404,
            detail="Dependency plan not found.",
        )
    return result


@router.get(
    "/mappings/{etl_object_id}/agent-responses",
    response_model=list[AgentResponseSummary],
    tags=["Discovery & Insights"],
)
async def agent_responses(
    etl_object_id: int,
    session: AsyncSession = Depends(get_session),
):
    result = await list_mapping_agent_responses(session, etl_object_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Mapping not found.")
    return result


@router.post(
    "/migrations",
    response_model=StartMigrationResponse,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["Migration Orchestration"],
)
async def create_migration(request: StartMigrationRequest, session: AsyncSession = Depends(get_session)):
    try:
        return await start_migration(session, request)
    except ValueError as exc:
        await session.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/migrations/{migration_id}/dashboard", tags=["Migration Orchestration"])
async def migration_dashboard(migration_id: str, session: AsyncSession = Depends(get_session)):
    result = await get_migration_dashboard(session, migration_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Migration not found.")
    return result


@router.get("/migrations/{migration_id}/mappings/{etl_object_id}/business-rules", tags=["Migration Orchestration"])
async def migration_business_rules(migration_id: str, etl_object_id: int, session: AsyncSession = Depends(get_session)):
    result = await get_migration_business_rules(session, migration_id, etl_object_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Migration mapping not found.")
    return result


@router.get("/migrations/{migration_id}/mappings/{etl_object_id}/lineage", tags=["Migration Orchestration"])
async def migration_lineage(migration_id: str, etl_object_id: int, session: AsyncSession = Depends(get_session)):
    result = await get_migration_lineage(session, migration_id, etl_object_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Migration mapping not found.")
    return result


@router.post(
    "/conversions",
    response_model=StartConversionResponse,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["Conversion Workbench"],
)
async def create_conversion(
    request: StartConversionRequest,
    session: AsyncSession = Depends(get_session),
):
    try:
        return await start_conversion(session, request)
    except ValueError as exc:
        await session.rollback()
        raise HTTPException(
            status_code=400,
            detail={
                "status": "FAILED",
                "stage": "CONVERSION_START",
                "reason": str(exc),
            },
        ) from exc


@router.get(
    "/conversions/{workflow_id}",
    tags=["Conversion Workbench"],
)
async def conversion_status(
    workflow_id: str,
    session: AsyncSession = Depends(get_session),
):
    row = await get_conversion_workflow(session, workflow_id)
    if row is None:
        raise HTTPException(
            status_code=404,
            detail="Conversion workflow not found.",
        )
    return _workflow_payload(row)


@router.get(
    "/conversions/{workflow_id}/mappings",
    response_model=list[ConversionMappingResponse],
    tags=["Conversion Workbench"],
)
async def conversion_mappings(
    workflow_id: str,
    session: AsyncSession = Depends(get_session),
):
    result = await list_conversion_mappings(session, workflow_id)
    if result is None:
        raise HTTPException(
            status_code=404,
            detail="Conversion workflow not found.",
        )
    return result


@router.get(
    "/conversions/{workflow_id}/workbench/{etl_object_id}",
    tags=["Conversion Workbench"],
    summary="Get the complete Conversion Workbench snapshot",
)
async def conversion_workbench(
    workflow_id: str,
    etl_object_id: int,
    session: AsyncSession = Depends(get_session),
):
    result = await get_conversion_workbench(
        session, workflow_id, etl_object_id
    )
    if result is None:
        raise HTTPException(
            status_code=404,
            detail="Conversion workflow or mapping was not found.",
        )
    return result


@router.get(
    "/mappings/{etl_object_id}/conversion",
    tags=["Conversion Workbench"],
)
async def mapping_conversion(
    etl_object_id: int,
    session: AsyncSession = Depends(get_session),
):
    result = await get_mapping_conversion(session, etl_object_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Mapping not found.")
    return result


@router.get(
    "/mappings/{etl_object_id}/artifacts",
    response_model=list[ArtifactResponse],
    tags=["Conversion Workbench"],
)
async def mapping_artifacts(
    etl_object_id: int,
    session: AsyncSession = Depends(get_session),
):
    result = await list_mapping_artifacts(session, etl_object_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Mapping not found.")
    return result


@router.get(
    "/artifacts/{artifact_id}",
    response_model=ArtifactContentResponse,
    tags=["Conversion Workbench"],
)
async def artifact_content(
    artifact_id: int,
    session: AsyncSession = Depends(get_session),
):
    try:
        result = await get_artifact_content(session, artifact_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    if result is None:
        raise HTTPException(status_code=404, detail="Artifact not found.")
    return result




@router.post("/validations/datasets", response_model=ValidationDatasetUploadResponse, tags=["Zero-Touch Validation"])
async def upload_validation_dataset(file: UploadFile = File(...)):
    from etl_cc.validation_store import validation_store
    try:
        return validation_store.stage(file.filename or "dataset", await file.read())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

@router.post("/validation-database-connections", response_model=DatabaseConnectionResponse, tags=["Zero-Touch Validation"])
async def create_validation_connection(request: DatabaseConnectionCreateRequest, session: AsyncSession = Depends(get_session)):
    try:
        return await create_validation_database_connection(session, request)
    except Exception as exc:
        await session.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from exc

@router.post(
    "/validations",
    response_model=StartValidationResponse,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["Zero-Touch Validation"],
)
async def create_validation(
    request: StartValidationRequest,
    session: AsyncSession = Depends(get_session),
):
    try:
        return await start_validation(session, request)
    except ValueError as exc:
        await session.rollback()
        raise HTTPException(
            status_code=400,
            detail={
                "status": "FAILED",
                "stage": "VALIDATION_START",
                "reason": str(exc),
            },
        ) from exc


@router.get(
    "/validations/{workflow_id}",
    tags=["Zero-Touch Validation"],
)
async def validation_status(
    workflow_id: str,
    session: AsyncSession = Depends(get_session),
):
    row = await get_validation_workflow(session, workflow_id)
    if row is None:
        raise HTTPException(
            status_code=404,
            detail="Validation workflow not found.",
        )
    return _workflow_payload(row)


@router.get(
    "/validations/{workflow_id}/mappings",
    response_model=list[ValidationMappingResponse],
    tags=["Zero-Touch Validation"],
)
async def validation_mappings(
    workflow_id: str,
    session: AsyncSession = Depends(get_session),
):
    result = await list_validation_mappings(session, workflow_id)
    if result is None:
        raise HTTPException(
            status_code=404,
            detail="Validation workflow not found.",
        )
    return result


@router.get(
    "/validations/{workflow_id}/report",
    response_model=ValidationReportResponse,
    tags=["Zero-Touch Validation"],
)
async def validation_report(
    workflow_id: str,
    session: AsyncSession = Depends(get_session),
):
    result = await get_validation_report(session, workflow_id)
    if result is None:
        raise HTTPException(
            status_code=404,
            detail="Validation workflow not found.",
        )
    return result


@router.get(
    "/validations/{workflow_id}/test-cases",
    response_model=list[ValidationTestCaseResponse],
    tags=["Zero-Touch Validation"],
)
async def validation_test_cases(
    workflow_id: str,
    status_filter: str | None = None,
    category: str | None = None,
    etl_object_id: int | None = None,
    session: AsyncSession = Depends(get_session),
):
    result = await list_validation_test_cases(
        session,
        workflow_id,
        status=status_filter,
        category=category,
        etl_object_id=etl_object_id,
    )
    if result is None:
        raise HTTPException(
            status_code=404,
            detail="Validation workflow not found.",
        )
    return result


@router.post(
    "/validations/{workflow_id}/data-comparison",
    tags=["Zero-Touch Validation"],
)
async def data_comparison(
    workflow_id: str,
    comparison_keys: str = Form(...),
    numeric_tolerance: str = Form("0"),
    legacy_output: UploadFile = File(...),
    pyspark_output: UploadFile = File(...),
    session: AsyncSession = Depends(get_session),
):
    workflow = await get_validation_workflow(session, workflow_id)
    if workflow is None:
        raise HTTPException(
            status_code=404,
            detail="Validation workflow not found.",
        )

    keys = [
        item.strip()
        for item in comparison_keys.split(",")
        if item.strip()
    ]
    if not keys:
        raise HTTPException(
            status_code=400,
            detail="At least one comparison key is required.",
        )

    try:
        tolerance = Decimal(numeric_tolerance)
        legacy_path = validation_store.save(
            workflow_id,
            "legacy",
            legacy_output.filename or "legacy.csv",
            await legacy_output.read(),
        )
        pyspark_path = validation_store.save(
            workflow_id,
            "pyspark",
            pyspark_output.filename or "pyspark.csv",
            await pyspark_output.read(),
        )
        return MatchFlowComparator().compare(
            legacy_path,
            pyspark_path,
            keys,
            tolerance,
        )
    except (ValueError, InvalidOperation) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post(
    "/deployment-targets/github/test",
    response_model=GitHubDeploymentTargetTestResponse,
    tags=["Git Deployment"],
)
async def test_deployment_target(
    request: GitHubDeploymentTargetTestRequest,
    session: AsyncSession = Depends(get_session),
):
    try:
        return await test_github_deployment_target(session, request)
    except (ValueError, ETLConnectorError) as exc:
        await session.rollback()
        raise HTTPException(status_code=400, detail={
            "status": "FAILED", "stage": "DEPLOYMENT_TARGET_TEST", "reason": str(exc)
        }) from exc


@router.post(
    "/deployment-targets/github",
    response_model=DeploymentTargetResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["Git Deployment"],
)
async def save_deployment_target(
    request: SaveDeploymentTargetRequest,
    session: AsyncSession = Depends(get_session),
):
    try:
        return await save_github_deployment_target(session, request)
    except (ValueError, ConnectionTestTokenError) as exc:
        await session.rollback()
        raise HTTPException(status_code=400, detail={
            "status": "FAILED", "stage": "DEPLOYMENT_TARGET_SAVE", "reason": str(exc)
        }) from exc


@router.get(
    "/deployment-targets",
    response_model=list[DeploymentTargetResponse],
    tags=["Git Deployment"],
)
async def deployment_targets(session: AsyncSession = Depends(get_session)):
    return await list_deployment_targets(session)


@router.get(
    "/deployment-targets/{target_repository_id}",
    response_model=DeploymentTargetResponse,
    tags=["Git Deployment"],
)
async def deployment_target(
    target_repository_id: int,
    session: AsyncSession = Depends(get_session),
):
    result = await get_deployment_target(session, target_repository_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Deployment target not found.")
    return result


@router.post("/deployments", response_model=StartDeploymentResponse, status_code=status.HTTP_202_ACCEPTED, tags=["Git Deployment"])
async def create_deployment(request: StartDeploymentRequest, session: AsyncSession = Depends(get_session)):
    try: return await start_deployment(session, request)
    except ValueError as exc:
        await session.rollback(); raise HTTPException(status_code=400, detail={"status":"FAILED","stage":"DEPLOYMENT_START","reason":str(exc)}) from exc

@router.get("/deployments/{workflow_id}", tags=["Git Deployment"])
async def deployment_status(workflow_id: str, session: AsyncSession = Depends(get_session)):
    row=await get_deployment_workflow(session, workflow_id)
    if row is None: raise HTTPException(status_code=404, detail="Deployment workflow not found.")
    return _workflow_payload(row)

@router.get("/deployments/{workflow_id}/mappings", response_model=list[DeploymentMappingResponse], tags=["Git Deployment"])
async def deployment_mappings(workflow_id: str, session: AsyncSession = Depends(get_session)):
    result=await list_deployment_mappings(session, workflow_id)
    if result is None: raise HTTPException(status_code=404, detail="Deployment workflow not found.")
    return result
