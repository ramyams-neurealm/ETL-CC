"""Application configuration for ETL CC."""

from functools import lru_cache
from pathlib import Path
from urllib.parse import quote_plus
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    app_name: str = "ETL Migration Command Center"
    app_env: str = "local"
    api_prefix: str = "/api/v1"
    pg_host: str
    pg_port: int = 5432
    pg_db: str
    pg_user: str
    pg_password: str
    pg_schema: str = "demooc28"
    etl_credential_encryption_key: str
    etl_credential_key_version: str = "v1"
    connection_test_ttl_seconds: int = 300
    informatica_pmrep_path: str = "pmrep"
    informatica_command_timeout_seconds: int = 120
    informatica_export_directory: str = "runtime_exports"
    worker_poll_seconds: float = 2.0
    source_snapshot_ttl_seconds: int = 1800
    max_xml_upload_bytes: int = 26214400

    # Multi-XML, workflow, session, and worklet package upload.
    xml_package_max_files: int = 100
    xml_package_max_file_bytes: int = 26214400
    xml_package_max_total_bytes: int = 104857600
    xml_package_allow_zip: bool = True
    memory_workspace_root: str = "/dev/shm/etl_cc"

    # Discovery capability flags exposed by GET /etl-products.
    enable_informatica: bool = True
    enable_informatica_powercenter: bool = True
    enable_informatica_github: bool = True
    enable_informatica_xml_upload: bool = True
    enable_datastage: bool = False
    enable_ab_initio: bool = False

    azure_tenant_id: str
    azure_client_id: str
    azure_client_secret: str
    azure_keyvault_url: str
    azure_keyvault_secret_name: str = "openai-key"
    openai_model: str = "gpt-4o"
    openai_temperature: float = 0.1
    openai_timeout_seconds: float = 120.0
    openai_max_output_tokens: int | None = None

    artifact_directory_name: str = "runtime_artifacts"
    rag_max_items: int = 8
    rag_embedding_deployment: str = "text-embedding-3-small"
    rag_embedding_dimensions: int = 1536
    rag_candidate_count: int = 30
    rag_min_vector_similarity: float = 0.50
    rag_min_similarity: float = 0.65
    rag_vector_weight: float = 0.70
    rag_metadata_weight: float = 0.30
    rag_embedding_batch_size: int = 50
    conversion_max_attempts: int = 2
    validation_test_timeout_seconds: int = 120
    minimum_functional_parity: float = 0.95
    validation_confidence_threshold: float = 0.60
    validation_upload_max_bytes: int = 104857600
    validation_upload_max_rows: int = 100000
    # Optional Discovery economics baselines. Economics remain NOT_CONFIGURED
    # until all six values are supplied through the environment.
    discovery_low_manual_hours: float | None = None
    discovery_medium_manual_hours: float | None = None
    discovery_high_manual_hours: float | None = None
    discovery_very_high_manual_hours: float | None = None
    discovery_hourly_labor_cost: float | None = None
    discovery_automation_savings_percentage: float | None = None

    synthetic_validation_max_scenarios: int = 30
    synthetic_validation_max_rows: int = 10000
    validation_simulation_default_rows: int = 1000
    validation_simulation_max_rows: int = 10000
    validation_database_row_limit: int = 10000
    validation_database_timeout_seconds: int = 60

    git_deployment_repository_path: str = "migrations"
    git_deployment_branch_prefix: str = "neuflow"
    git_command_timeout_seconds: int = 180
    github_api_timeout_seconds: float = 30.0
    deployment_max_attempts: int = 1
    auto_deploy_after_validation: bool = True
    git_deployment_repository_url: str | None = None
    git_deployment_base_branch: str = "main"
    git_deployment_create_pull_request: bool = False
    git_deployment_access_token: str | None = None
    model_config = SettingsConfigDict(
        env_file=PROJECT_ROOT / ".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+asyncpg://{quote_plus(self.pg_user)}:{quote_plus(self.pg_password)}"
            f"@{self.pg_host}:{self.pg_port}/{self.pg_db}"
        )

    @property
    def project_root(self) -> Path:
        return PROJECT_ROOT

    @property
    def artifact_directory(self) -> Path:
        path = Path(self.artifact_directory_name)
        return path if path.is_absolute() else PROJECT_ROOT / path

    @property
    def export_directory(self) -> Path:
        path = Path(self.informatica_export_directory)
        return path if path.is_absolute() else PROJECT_ROOT / path


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
