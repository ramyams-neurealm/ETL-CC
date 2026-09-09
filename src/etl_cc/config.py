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

    azure_tenant_id: str
    azure_client_id: str
    azure_client_secret: str
    azure_keyvault_url: str
    azure_keyvault_secret_name: str = "openai-key"
    openai_model: str = "gpt-4o"
    openai_temperature: float = 0.1
    openai_timeout_seconds: float = 120.0
    openai_max_output_tokens: int = 3000

    artifact_directory_name: str = "runtime_artifacts"
    rag_max_items: int = 8
    conversion_max_attempts: int = 2
    validation_test_timeout_seconds: int = 120
    minimum_functional_parity: float = 0.95
    validation_upload_directory_name: str = "runtime_validation_uploads"

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
    def validation_upload_directory(self) -> Path:
        path = Path(self.validation_upload_directory_name)
        return path if path.is_absolute() else PROJECT_ROOT / path

    @property
    def export_directory(self) -> Path:
        path = Path(self.informatica_export_directory)
        return path if path.is_absolute() else PROJECT_ROOT / path


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
