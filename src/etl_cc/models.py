"""Database, API, and canonical models for ETL CC source ingestion."""

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, HttpUrl, SecretStr
from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from etl_cc.config import settings
from etl_cc.database import Base

ETL_SCHEMA = settings.pg_schema
ConnectionType = Literal["POWERCENTER", "GITHUB", "XML_UPLOAD"]
EnvironmentType = Literal["DEV", "STAGING", "PROD"]


class ValidationDatabaseConnectionETL(Base):
    __tablename__ = "validation_database_connection_etl"
    __table_args__ = (UniqueConstraint("connection_name", name="uq_validation_db_connection_name"), {"schema": ETL_SCHEMA})
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    connection_name: Mapped[str] = mapped_column(String(200), nullable=False)
    database_type: Mapped[str] = mapped_column(String(30), nullable=False)
    connection_config: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    credential_ciphertext: Mapped[str] = mapped_column(Text, nullable=False)
    credential_algorithm: Mapped[str] = mapped_column(String(30), nullable=False, default="FERNET")
    credential_key_version: Mapped[str] = mapped_column(String(30), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

class RepositoryETL(Base):
    """Store one configured PowerCenter, GitHub, or XML-upload source."""

    __tablename__ = "repository_etl"
    __table_args__ = (
        UniqueConstraint(
            "repository_name",
            "environment",
            name="uq_repository_etl_name_environment",
        ),
        CheckConstraint(
            "connection_type IN ('POWERCENTER', 'GITHUB', 'XML_UPLOAD')",
            name="ck_repository_etl_connection_type",
        ),
        Index("ix_repository_etl_connection_type", "connection_type"),
        {"schema": ETL_SCHEMA},
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    repository_name: Mapped[str] = mapped_column(String(200), nullable=False)
    source_type: Mapped[str] = mapped_column(String(50), nullable=False, default="INFORMATICA")
    connection_type: Mapped[str] = mapped_column(String(30), nullable=False)
    environment: Mapped[str] = mapped_column(String(30), nullable=False)
    connection_config: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    credential_ciphertext: Mapped[str | None] = mapped_column(Text, nullable=True)
    credential_algorithm: Mapped[str | None] = mapped_column(String(30), nullable=True)
    credential_key_version: Mapped[str | None] = mapped_column(String(30), nullable=True)
    connection_status: Mapped[str] = mapped_column(String(30), nullable=False, default="CONNECTED")
    last_tested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_discovered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class ETLObjectETL(Base):
    """Store one normalized ETL mapping and its discovery enrichment."""

    __tablename__ = "etl_object_etl"
    __table_args__ = (
        UniqueConstraint(
            "repository_id",
            "source_object_key",
            name="uq_etl_object_etl_repository_source_key",
        ),
        {"schema": ETL_SCHEMA},
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    repository_id: Mapped[int] = mapped_column(
        ForeignKey(f"{ETL_SCHEMA}.repository_etl.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    source_object_key: Mapped[str] = mapped_column(String(500), nullable=False)
    object_name: Mapped[str] = mapped_column(String(300), nullable=False, index=True)
    object_type: Mapped[str] = mapped_column(String(50), nullable=False, default="MAPPING")
    folder_path: Mapped[str | None] = mapped_column(String(500))
    source_definition: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    business_purpose: Mapped[str | None] = mapped_column(Text)
    business_rules: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False, default=list)
    parameters: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False, default=list)
    prerequisites: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False, default=list)
    lineage: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    dependencies: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    complexity: Mapped[str | None] = mapped_column(String(20))
    complexity_score: Mapped[int | None] = mapped_column(Integer)
    transformation_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    migration_wave: Mapped[int | None] = mapped_column(Integer)
    has_dependency_cycle: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    source_version: Mapped[str | None] = mapped_column(String(100))
    content_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    discovery_status: Mapped[str] = mapped_column(String(30), nullable=False, default="DISCOVERED")
    migration_status: Mapped[str] = mapped_column(String(30), nullable=False, default="NOT_STARTED")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class WorkflowRunETL(Base):
    __tablename__ = "workflow_run_etl"
    __table_args__ = ({"schema": ETL_SCHEMA},)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    workflow_id: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    batch_id: Mapped[str | None] = mapped_column(String(100), index=True)
    repository_id: Mapped[int] = mapped_column(
        ForeignKey(f"{ETL_SCHEMA}.repository_etl.id", ondelete="CASCADE"), nullable=False, index=True
    )
    etl_object_id: Mapped[int | None] = mapped_column(
        ForeignKey(f"{ETL_SCHEMA}.etl_object_etl.id", ondelete="SET NULL"), index=True
    )
    current_stage: Mapped[str] = mapped_column(String(30), nullable=False, default="DISCOVERY")
    overall_status: Mapped[str] = mapped_column(String(30), nullable=False, default="QUEUED", index=True)
    job_type: Mapped[str] = mapped_column(String(40), nullable=False, default="DISCOVERY")
    job_status: Mapped[str] = mapped_column(String(30), nullable=False, default="QUEUED", index=True)
    scope_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    failure_stage: Mapped[str | None] = mapped_column(String(30))
    failure_reason: Mapped[str | None] = mapped_column(Text)
    recommendation: Mapped[str | None] = mapped_column(Text)
    approval_status: Mapped[str] = mapped_column(String(30), nullable=False, default="NOT_REQUIRED")
    approval_details: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    created_by: Mapped[str | None] = mapped_column(String(200))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class AgentResponseETL(Base):
    __tablename__ = "agent_response_etl"
    __table_args__ = ({"schema": ETL_SCHEMA},)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    workflow_run_id: Mapped[int] = mapped_column(
        ForeignKey(f"{ETL_SCHEMA}.workflow_run_etl.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    etl_object_id: Mapped[int | None] = mapped_column(
        ForeignKey(f"{ETL_SCHEMA}.etl_object_etl.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    agent_name: Mapped[str] = mapped_column(String(100), nullable=False)
    agent_version: Mapped[str] = mapped_column(String(30), nullable=False, default="1.0.0")
    stage_name: Mapped[str] = mapped_column(String(50), nullable=False)
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    status: Mapped[str] = mapped_column(String(30), nullable=False)
    request_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    response_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    prompt_name: Mapped[str | None] = mapped_column(String(100))
    prompt_version: Mapped[str | None] = mapped_column(String(30))
    model_name: Mapped[str | None] = mapped_column(String(100))
    input_tokens: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    output_tokens: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_message: Mapped[str | None] = mapped_column(Text)
    recommendation: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class WorkflowEventETL(Base):
    __tablename__ = "workflow_event_etl"
    __table_args__ = ({"schema": ETL_SCHEMA},)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    workflow_run_id: Mapped[int] = mapped_column(
        ForeignKey(f"{ETL_SCHEMA}.workflow_run_etl.id", ondelete="CASCADE"), nullable=False, index=True
    )
    agent_response_id: Mapped[int | None] = mapped_column(
        ForeignKey(f"{ETL_SCHEMA}.agent_response_etl.id", ondelete="SET NULL")
    )
    event_type: Mapped[str] = mapped_column(String(80), nullable=False)
    stage_name: Mapped[str | None] = mapped_column(String(50))
    status: Mapped[str | None] = mapped_column(String(30))
    progress_percentage: Mapped[int | None] = mapped_column(Integer)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    event_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    actor_type: Mapped[str] = mapped_column(String(30), nullable=False, default="SYSTEM")
    actor_id: Mapped[str | None] = mapped_column(String(200))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class GeneratedArtifactETL(Base):
    __tablename__ = "generated_artifact_etl"
    __table_args__ = ({"schema": ETL_SCHEMA},)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    workflow_run_id: Mapped[int] = mapped_column(
        ForeignKey(f"{ETL_SCHEMA}.workflow_run_etl.id", ondelete="CASCADE"),
        nullable=False,
    )
    etl_object_id: Mapped[int | None] = mapped_column(
        ForeignKey(f"{ETL_SCHEMA}.etl_object_etl.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    artifact_type: Mapped[str] = mapped_column(String(80), nullable=False)
    artifact_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    file_name: Mapped[str] = mapped_column(String(500), nullable=False)
    storage_provider: Mapped[str] = mapped_column(String(50), nullable=False, default="LOCAL")
    storage_path: Mapped[str] = mapped_column(Text, nullable=False)
    git_path: Mapped[str | None] = mapped_column(Text)
    content_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    media_type: Mapped[str | None] = mapped_column(String(150))
    validation_status: Mapped[str | None] = mapped_column(String(30))
    is_deployable: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class KnowledgeBaseETL(Base):
    __tablename__ = "knowledge_base_etl"
    __table_args__ = ({"schema": ETL_SCHEMA},)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    repository_id: Mapped[int | None] = mapped_column(
        ForeignKey(f"{ETL_SCHEMA}.repository_etl.id", ondelete="SET NULL")
    )
    knowledge_type: Mapped[str] = mapped_column(String(80), nullable=False)
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    metadata_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    content_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    approval_status: Mapped[str] = mapped_column(String(30), nullable=False, default="PENDING")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class ValidationTestCaseETL(Base):
    """Store one UI-ready validation test-case result."""

    __tablename__ = "validation_test_case_etl"
    __table_args__ = (
        UniqueConstraint(
            "workflow_run_id",
            "etl_object_id",
            "test_id",
            name="uq_validation_test_case_scope",
        ),
        {"schema": ETL_SCHEMA},
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    workflow_run_id: Mapped[int] = mapped_column(
        ForeignKey(f"{ETL_SCHEMA}.workflow_run_etl.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    etl_object_id: Mapped[int | None] = mapped_column(
        ForeignKey(f"{ETL_SCHEMA}.etl_object_etl.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    agent_response_id: Mapped[int | None] = mapped_column(
        ForeignKey(f"{ETL_SCHEMA}.agent_response_etl.id", ondelete="SET NULL"),
        nullable=True,
    )
    test_id: Mapped[str] = mapped_column(String(100), nullable=False)
    test_name: Mapped[str] = mapped_column(String(500), nullable=False)
    category: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="PLANNED")
    severity: Mapped[str] = mapped_column(String(30), nullable=False, default="MEDIUM")
    expected_result: Mapped[str | None] = mapped_column(Text)
    actual_result: Mapped[str | None] = mapped_column(Text)
    details: Mapped[str | None] = mapped_column(Text)
    evidence_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    duration_milliseconds: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class PowerCenterConnectionRequest(BaseModel):
    connection_name: str = Field(min_length=2, max_length=200)
    environment: EnvironmentType
    host: str = Field(min_length=1, max_length=255)
    port: int = Field(ge=1, le=65535)
    domain_name: str = Field(min_length=1, max_length=200)
    repository_name: str = Field(min_length=1, max_length=200)
    security_domain: str = Field(default="Native", max_length=100)
    username: str = Field(min_length=1, max_length=200)
    password: SecretStr


class GitHubConnectionRequest(BaseModel):
    connection_name: str = Field(min_length=2, max_length=200)
    environment: EnvironmentType
    repository_url: HttpUrl
    branch: str = Field(default="main", min_length=1, max_length=255)
    folder_path: str = Field(default="", max_length=1000)
    access_token: SecretStr | None = None


class MappingSummary(BaseModel):
    source_object_key: str
    mapping_name: str
    folder_name: str | None = None
    object_type: str = "MAPPING"
    selectable: bool = True
    source_reference: str | None = None


class SourceAnalysisResponse(BaseModel):
    status: Literal["SUCCESS"]
    message: str
    connection_type: ConnectionType
    source_id: str
    test_token: str
    expires_in_seconds: int
    mapping_count: int
    mappings: list[MappingSummary] = Field(default_factory=list)


class StartDiscoveryRequest(BaseModel):
    source_id: str
    test_token: str
    scope_type: Literal["ENTIRE_REPOSITORY", "SELECTED_MAPPINGS"]
    selected_mapping_keys: list[str] = Field(default_factory=list)


class SaveRepositoryResponse(BaseModel):
    repository_id: int
    workflow_id: str
    status: Literal["QUEUED"]
    message: str


class RepositoryResponse(BaseModel):
    repository_id: int
    repository_name: str
    source_type: str
    connection_type: str
    environment: str
    connection_status: str
    connection_config: dict[str, Any]
    created_at: datetime


class MappingInventoryResponse(BaseModel):
    etl_object_id: int
    repository_id: int
    source_object_key: str
    mapping_name: str
    folder_name: str | None
    complexity: str | None
    complexity_score: int | None = None
    transformation_count: int
    migration_wave: int | None
    discovery_status: str
    migration_status: str
    stage: str = "DISCOVERY"
    status: str = "PENDING"
    result: str = "PENDING"
    confidence: float | None = None
    human_review_required: bool = False
    has_dependency_cycle: bool = False


class CanonicalField(BaseModel):
    name: str
    data_type: str
    precision: int | None = None
    scale: int | None = None
    nullable: bool = True


class CanonicalDataset(BaseModel):
    name: str
    dataset_type: str
    connection_name: str | None = None
    fields: list[CanonicalField] = Field(default_factory=list)
    properties: dict[str, Any] = Field(default_factory=dict)


class CanonicalPort(BaseModel):
    name: str
    direction: str
    data_type: str | None = None
    expression: str | None = None
    properties: dict[str, Any] = Field(default_factory=dict)


class CanonicalTransformation(BaseModel):
    name: str
    transformation_type: str
    ports: list[CanonicalPort] = Field(default_factory=list)
    properties: dict[str, Any] = Field(default_factory=dict)


class CanonicalConnector(BaseModel):
    from_instance: str
    to_instance: str
    from_field: str | None = None
    to_field: str | None = None


class CanonicalParameter(BaseModel):
    name: str
    parameter_type: str | None = None
    data_type: str | None = None
    default_value: str | None = None
    scope: str | None = None


class CanonicalDependency(BaseModel):
    upstream_object_key: str
    downstream_object_key: str
    dependency_source: str
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)


class CanonicalBusinessRule(BaseModel):
    rule_type: str
    description: str
    source_expression: str | None = None
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)


class CanonicalMapping(BaseModel):
    source_vendor: str
    source_object_key: str
    mapping_name: str
    folder_name: str | None = None
    description: str | None = None
    sources: list[CanonicalDataset] = Field(default_factory=list)
    targets: list[CanonicalDataset] = Field(default_factory=list)
    transformations: list[CanonicalTransformation] = Field(default_factory=list)
    connectors: list[CanonicalConnector] = Field(default_factory=list)
    sessions: list[dict[str, Any]] = Field(default_factory=list)
    workflows: list[dict[str, Any]] = Field(default_factory=list)
    parameters: list[CanonicalParameter] = Field(default_factory=list)
    business_rules: list[CanonicalBusinessRule] = Field(default_factory=list)
    dependencies: list[CanonicalDependency] = Field(default_factory=list)
    source_metadata: dict[str, Any] = Field(default_factory=dict)

class ComplexityDistribution(BaseModel):
    LOW: int = 0
    MEDIUM: int = 0
    HIGH: int = 0
    UNKNOWN: int = 0


class DiscoverySummaryResponse(BaseModel):
    repository_id: int
    total_mappings: int
    need_to_discover: int
    discovered: int
    complexity_distribution: ComplexityDistribution
    ready: int
    review_required: int
    blocked: int
    dependency_cycles: int
    total_input_tokens: int
    total_output_tokens: int


class MappingDiscoveryDetailsResponse(BaseModel):
    etl_object_id: int
    repository_id: int
    source_object_key: str
    mapping_name: str
    folder_name: str | None
    business_purpose: str | None
    business_rules: list[dict[str, Any]] = Field(default_factory=list)
    prerequisites: list[dict[str, Any]] = Field(default_factory=list)
    complexity: str | None
    complexity_score: int | None
    complexity_reasons: list[str] = Field(default_factory=list)
    risks: list[dict[str, Any]] = Field(default_factory=list)
    unsupported_constructs: list[str] = Field(default_factory=list)
    assumptions: list[dict[str, Any]] = Field(default_factory=list)
    migration_recommendations: list[str] = Field(default_factory=list)
    confidence: float | None
    human_review_required: bool
    readiness_status: str
    discovery_status: str
    model_name: str | None
    prompt_name: str | None
    prompt_version: str | None
    input_tokens: int
    output_tokens: int
    agent_response_id: int | None
    analyzed_at: datetime | None


class MappingLineageDetailsResponse(BaseModel):
    etl_object_id: int
    repository_id: int
    mapping_name: str
    lineage: dict[str, Any] = Field(default_factory=dict)
    dependencies: dict[str, Any] = Field(default_factory=dict)
    migration_wave: int | None
    has_dependency_cycle: bool


class DependencyPlanResponse(BaseModel):
    workflow_id: str
    repository_id: int
    status: str
    model_name: str | None
    response_payload: dict[str, Any] = Field(default_factory=dict)
    agent_response_id: int
    completed_at: datetime | None


class AgentResponseSummary(BaseModel):
    agent_response_id: int
    workflow_id: str
    etl_object_id: int | None
    agent_name: str
    agent_version: str
    stage_name: str
    attempt_number: int
    status: str
    model_name: str | None
    prompt_name: str | None
    prompt_version: str | None
    input_tokens: int
    output_tokens: int
    error_message: str | None
    recommendation: str | None
    request_payload: dict[str, Any] = Field(default_factory=dict)
    response_payload: dict[str, Any] = Field(default_factory=dict)
    started_at: datetime | None
    completed_at: datetime | None
    created_at: datetime


class StartConversionRequest(BaseModel):
    repository_id: int
    etl_object_ids: list[int] = Field(min_length=1)
    target_platform: Literal["DATABRICKS"] = "DATABRICKS"
    target_framework: Literal["PYSPARK"] = "PYSPARK"
    follow_migration_waves: bool = True


class StartConversionResponse(BaseModel):
    workflow_id: str
    repository_id: int
    status: Literal["QUEUED"]
    mapping_count: int
    message: str


class ConversionMappingResponse(BaseModel):
    etl_object_id: int
    mapping_name: str
    migration_wave: int | None
    migration_status: str
    latest_conversion: dict[str, Any] | None = None
    latest_critique: dict[str, Any] | None = None


class ArtifactResponse(BaseModel):
    artifact_id: int
    workflow_id: str
    etl_object_id: int | None
    artifact_type: str
    artifact_version: int
    file_name: str
    storage_provider: str
    storage_path: str
    content_hash: str
    media_type: str | None
    validation_status: str | None
    is_deployable: bool
    created_at: datetime


class ArtifactContentResponse(ArtifactResponse):
    content: str



class SimulationOptions(BaseModel):
    target_row_count: int = Field(default=1000, ge=1, le=10000)
    include_null_cases: bool = True
    include_boundary_cases: bool = True
    include_negative_cases: bool = True
    include_duplicate_cases: bool = False

class DatasetFileInput(BaseModel):
    upload_id: str = Field(min_length=40, max_length=40)
    dataset_name: str = Field(min_length=1, max_length=200)

class DatabaseTableSelection(BaseModel):
    connection_id: int
    dataset_name: str = Field(min_length=1, max_length=200)
    schema_name: str = Field(min_length=1, max_length=128)
    table_name: str = Field(min_length=1, max_length=128)

class DatabaseTablesInput(BaseModel):
    validation_type: Literal["EXECUTE_AND_COMPARE", "TABLE_TO_TABLE_COMPARE"] = "EXECUTE_AND_COMPARE"
    source_tables: list[DatabaseTableSelection] = Field(min_length=1)
    target_table: DatabaseTableSelection | None = None
    comparison_keys: list[str] = Field(default_factory=list)
    row_limit: int = Field(default=10000, ge=1, le=100000)

class ValidationDatasetUploadResponse(BaseModel):
    upload_id: str
    file_name: str
    file_format: str
    row_count: int
    detected_columns: list[str]

class DatabaseConnectionCreateRequest(BaseModel):
    connection_name: str = Field(min_length=1,max_length=200)
    database_type: Literal["POSTGRESQL"] = "POSTGRESQL"
    host: str
    port: int = Field(default=5432,ge=1,le=65535)
    database_name: str
    username: str
    password: SecretStr
    ssl_mode: str = "prefer"

class DatabaseConnectionResponse(BaseModel):
    connection_id: int
    connection_name: str
    database_type: str
    host: str
    port: int
    database_name: str
    username: str

class StartValidationRequest(BaseModel):
    repository_id: int
    etl_object_ids: list[int] = Field(min_length=1)
    conversion_workflow_id: str = Field(min_length=1, max_length=100)
    validation_mode: Literal["STATIC_AND_UNIT_TEST"] = "STATIC_AND_UNIT_TEST"
    minimum_functional_parity: float = Field(default=0.95, ge=0.0, le=1.0)
    input_mode: Literal["SIMULATE", "DATASET_FILE", "DATABASE_TABLES"] = "SIMULATE"
    simulation_options: SimulationOptions | None = None
    dataset_files: list[DatasetFileInput] = Field(default_factory=list)
    database_tables: DatabaseTablesInput | None = None


class StartValidationResponse(BaseModel):
    workflow_id: str
    repository_id: int
    status: Literal["QUEUED"]
    mapping_count: int
    message: str


class ValidationMappingResponse(BaseModel):
    etl_object_id: int
    mapping_name: str
    migration_status: str
    static_validation: dict[str, Any] | None = None
    unit_test_execution: dict[str, Any] | None = None
    functional_parity: dict[str, Any] | None = None


class ValidationReportResponse(BaseModel):
    workflow_id: str
    repository_id: int
    overall_status: str
    mapping_count: int
    passed_mappings: int
    failed_mappings: int
    total_tests: int
    passed_tests: int
    failed_tests: int
    skipped_tests: int
    mappings: list[ValidationMappingResponse] = Field(default_factory=list)


class ValidationTestCaseResponse(BaseModel):
    test_case_id: int
    workflow_id: str
    etl_object_id: int | None
    mapping_name: str | None
    test_id: str
    test_name: str
    category: str
    status: str
    severity: str
    expected_result: str | None
    actual_result: str | None
    details: str | None
    evidence_payload: dict[str, Any] = Field(default_factory=dict)
    duration_seconds: float
    created_at: datetime
    updated_at: datetime


class StartDeploymentRequest(BaseModel):
    repository_id: int
    etl_object_ids: list[int] = Field(min_length=1)
    validation_workflow_id: str = Field(min_length=1, max_length=100)
    target_repository_id: int
    base_branch: str | None = Field(default=None, max_length=255)
    repository_path: str = Field(default="migrations", min_length=1, max_length=500)
    branch_prefix: str = Field(default="neuflow", min_length=1, max_length=100)
    commit_message: str | None = Field(default=None, max_length=500)
    create_pull_request: bool = False
    pull_request_title: str | None = Field(default=None, max_length=300)
    pull_request_body: str | None = Field(default=None, max_length=4000)

class StartDeploymentResponse(BaseModel):
    workflow_id: str
    repository_id: int
    status: Literal["QUEUED"]
    mapping_count: int
    message: str

class DeploymentMappingResponse(BaseModel):
    etl_object_id: int
    mapping_name: str
    migration_status: str
    branch_name: str | None = None
    commit_sha: str | None = None
    pull_request_url: str | None = None
    deployment_status: str | None = None
