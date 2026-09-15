"""Dynamic Azure Key Vault access and safe standard OpenAI wrappers."""

import asyncio
import logging
import threading
import time
from typing import Any

from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient

from etl_cc.config import settings

logger = logging.getLogger(__name__)

SAFE_OPENAI_MESSAGE = (
    "The AI service is temporarily unavailable because its access credential "
    "could not be refreshed. Please try again shortly or contact the administrator."
)


class OpenAIServiceUnavailableError(RuntimeError):
    """Safe exception raised when an OpenAI credential cannot be obtained."""

    def __init__(self, operation_name: str, reason: str = ""):
        super().__init__(SAFE_OPENAI_MESSAGE)
        self.operation_name = operation_name
        self.reason = reason


def is_authentication_error(exc: Exception) -> bool:
    """Return True for common OpenAI authentication failures."""
    status_code = getattr(exc, "status_code", None)
    response_status = getattr(getattr(exc, "response", None), "status_code", None)
    error_name = type(exc).__name__.lower()
    return (
        status_code == 401
        or response_status == 401
        or "authentication" in error_name
        or "invalidapikey" in error_name
        or "permissiondenied" in error_name
    )


class KeyVaultService:
    """Reuse the Key Vault client but retrieve the secret for every LLM call."""

    def __init__(self) -> None:
        self._credential: ClientSecretCredential | None = None
        self._client: SecretClient | None = None
        self._lock = threading.Lock()

    def _get_client(self) -> SecretClient:
        if self._client is not None:
            return self._client

        with self._lock:
            if self._client is None:
                missing = [
                    name
                    for name, value in {
                        "AZURE_TENANT_ID": settings.azure_tenant_id,
                        "AZURE_CLIENT_ID": settings.azure_client_id,
                        "AZURE_CLIENT_SECRET": settings.azure_client_secret,
                        "AZURE_KEYVAULT_URL": settings.azure_keyvault_url,
                    }.items()
                    if not value.strip()
                ]
                if missing:
                    logger.error(
                        "[KEYVAULT] Missing configuration: %s",
                        ", ".join(missing),
                    )
                    raise OpenAIServiceUnavailableError(
                        "key_vault_configuration",
                        "missing_configuration",
                    )

                self._credential = ClientSecretCredential(
                    tenant_id=settings.azure_tenant_id,
                    client_id=settings.azure_client_id,
                    client_secret=settings.azure_client_secret,
                )
                self._client = SecretClient(
                    vault_url=settings.azure_keyvault_url,
                    credential=self._credential,
                )

        return self._client

    def get_openai_key(self, operation_name: str) -> str:
        """Fetch the current standard OpenAI API key from Key Vault."""
        started = time.perf_counter()
        try:
            secret = self._get_client().get_secret(
                settings.azure_keyvault_secret_name
            )
            key = (secret.value or "").strip()
            if not key:
                raise ValueError("empty secret")

            logger.info(
                "[KEYVAULT] Fresh OpenAI key fetched for %s in %.2fs "
                "(secret_version=%s)",
                operation_name,
                time.perf_counter() - started,
                getattr(secret.properties, "version", "unknown"),
            )
            return key
        except OpenAIServiceUnavailableError:
            raise
        except Exception as exc:
            logger.exception(
                "[KEYVAULT] Credential refresh failed operation=%s error_type=%s",
                operation_name,
                type(exc).__name__,
            )
            raise OpenAIServiceUnavailableError(
                operation_name,
                type(exc).__name__,
            ) from None


key_vault_service = KeyVaultService()


class _Structured:
    def __init__(self, parent: "DynamicChatOpenAI", schema: Any, kwargs: dict[str, Any]):
        self.parent = parent
        self.schema = schema
        self.kwargs = kwargs

    def invoke(self, input_value: Any, config: Any = None, **kwargs: Any) -> Any:
        return self.parent._invoke(
            input_value,
            self.schema,
            self.kwargs,
            config,
            kwargs,
        )

    async def ainvoke(self, input_value: Any, config: Any = None, **kwargs: Any) -> Any:
        return await asyncio.to_thread(
            self.invoke,
            input_value,
            config,
            **kwargs,
        )


class DynamicChatOpenAI:
    """Create a fresh ChatOpenAI client after each Key Vault secret retrieval.

    The application output-token limit is optional. When no value is configured,
    max_tokens is omitted and the OpenAI provider/model deployment applies its
    native output limit.
    """

    def __init__(
        self,
        operation_name: str,
        model: str | None = None,
        temperature: float | None = None,
        timeout: float | None = None,
        max_output_tokens: int | None = None,
        **kwargs: Any,
    ) -> None:
        self.operation_name = operation_name
        self.model = model or settings.openai_model
        self.temperature = (
            settings.openai_temperature
            if temperature is None
            else temperature
        )
        self.timeout = timeout or settings.openai_timeout_seconds
        self.max_output_tokens = (
            max_output_tokens
            if max_output_tokens is not None
            else settings.openai_max_output_tokens
        )
        self.kwargs = kwargs

    def _build(self):
        from langchain_openai import ChatOpenAI

        options: dict[str, Any] = {
            "model": self.model,
            "temperature": self.temperature,
            "api_key": key_vault_service.get_openai_key(
                self.operation_name
            ),
            "timeout": self.timeout,
            "max_retries": 0,
            **self.kwargs,
        }
        if self.max_output_tokens is not None:
            options["max_tokens"] = self.max_output_tokens

        return ChatOpenAI(**options)

    def _invoke(
        self,
        input_value: Any,
        schema: Any,
        structured_kwargs: dict[str, Any],
        config: Any,
        invoke_kwargs: dict[str, Any],
    ) -> Any:
        for attempt in (1, 2):
            try:
                runnable = self._build()
                if schema is not None:
                    runnable = runnable.with_structured_output(
                        schema,
                        **structured_kwargs,
                    )
                return runnable.invoke(
                    input_value,
                    config=config,
                    **invoke_kwargs,
                )
            except Exception as exc:
                if is_authentication_error(exc) and attempt == 1:
                    logger.warning(
                        "[OPENAI] Authentication rejected for %s; refreshing key",
                        self.operation_name,
                    )
                    continue
                if is_authentication_error(exc) or isinstance(
                    exc,
                    OpenAIServiceUnavailableError,
                ):
                    raise OpenAIServiceUnavailableError(
                        self.operation_name,
                        type(exc).__name__,
                    ) from None
                raise

        raise OpenAIServiceUnavailableError(
            self.operation_name,
            "retry_exhausted",
        )

    def invoke(
        self,
        input_value: Any,
        config: Any = None,
        **kwargs: Any,
    ) -> Any:
        return self._invoke(
            input_value,
            None,
            {},
            config,
            kwargs,
        )

    async def ainvoke(
        self,
        input_value: Any,
        config: Any = None,
        **kwargs: Any,
    ) -> Any:
        return await asyncio.to_thread(
            self.invoke,
            input_value,
            config,
            **kwargs,
        )

    def with_structured_output(
        self,
        schema: Any,
        **kwargs: Any,
    ) -> _Structured:
        return _Structured(self, schema, kwargs)

    def with_config(self, **kwargs: Any) -> "DynamicChatOpenAI":
        merged_kwargs = dict(self.kwargs)
        model = kwargs.pop("model", self.model)
        temperature = kwargs.pop(
            "temperature",
            self.temperature,
        )
        timeout = kwargs.pop("timeout", self.timeout)
        max_output_tokens = kwargs.pop(
            "max_output_tokens",
            self.max_output_tokens,
        )
        merged_kwargs.update(kwargs)

        return DynamicChatOpenAI(
            operation_name=self.operation_name,
            model=model,
            temperature=temperature,
            timeout=timeout,
            max_output_tokens=max_output_tokens,
            **merged_kwargs,
        )
