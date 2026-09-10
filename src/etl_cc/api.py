"""REST APIs for source ingestion, discovery, conversion, and validation."""

from decimal import Decimal, InvalidOperation

from fastapi import (
    APIRouter,
    Depends,
    File,
    Form,
    HTTPException,
    UploadFile,
    status,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from etl_cc.agents.matchflow_comparator import MatchFlowComparator
from etl_cc.connectors import ETLConnectorError
from etl_cc.database import get_session
from etl_cc.models import (
    AgentResponseSummary,
    ArtifactContentResponse,
    ArtifactResponse,
    ConversionMappingResponse,
    DependencyPlanResponse,
    DiscoverySummaryResponse,
    GitHubConnectionRequest,
    MappingDiscoveryDetailsResponse,
    MappingInventoryResponse,
    MappingLineageDetailsResponse,
    PowerCenterConnectionRequest,
    RepositoryResponse,
    SaveRepositoryResponse,
    SourceAnalysisResponse,
    StartConversionRequest,
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
)
from etl_cc.security import ConnectionTestTokenError
from etl_cc.services import (
    analyze_github,
    analyze_powercenter,
    analyze_xml_upload,
    get_artifact_content,
    get_conversion_workflow,
    get_discovery_summary,
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
    start_conversion,
    start_discovery,
    start_validation,
    create_validation_database_connection,
    start_deployment,
    get_deployment_workflow,
    list_deployment_mappings,
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


@router.get("/health", tags=["Health"])
async def health() -> dict[str, str]:
    return {"status": "ok"}


@router.post(
    "/sources/powercenter/test",
    response_model=SourceAnalysisResponse,
    tags=["Sources"],
)
async def test_powercenter(request: PowerCenterConnectionRequest):
    try:
        return await analyze_powercenter(request)
    except ETLConnectorError as exc:
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
async def test_github(request: GitHubConnectionRequest):
    try:
        return await analyze_github(request)
    except ETLConnectorError as exc:
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
    connection_name: str = Form(...),
    environment: str = Form(...),
    file: UploadFile = File(...),
):
    try:
        if environment not in {"DEV", "STAGING", "PROD"}:
            raise ValueError("Environment must be DEV, STAGING, or PROD.")
        return await analyze_xml_upload(
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
