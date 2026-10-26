"""
Security utilities for ETL repository credentials and connection tests.

This module:
- Encrypts repository credentials before database storage.
- Creates short-lived, HMAC-signed connection-test tokens.
- Verifies current v2 tokens.
- Verifies legacy v1 tokens without the raw-signature delimiter bug.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from typing import Any

from cryptography.fernet import Fernet, InvalidToken

from etl_cc.config import settings


_TOKEN_VERSION = "v2"
_HMAC_DIGEST_SIZE = hashlib.sha256().digest_size


class CredentialEncryptionError(Exception):
    """Raised when a repository credential cannot be encrypted or decrypted."""


class ConnectionTestTokenError(Exception):
    """Raised when a connection-test token is invalid, expired, or mismatched."""


class CredentialCipher:
    """Encrypts and decrypts ETL repository credentials using Fernet."""

    def __init__(self, encryption_key: str) -> None:
        if not encryption_key:
            raise CredentialEncryptionError(
                "ETL_CREDENTIAL_ENCRYPTION_KEY is required."
            )

        try:
            self._cipher = Fernet(encryption_key.encode("utf-8"))
        except (TypeError, ValueError) as exc:
            raise CredentialEncryptionError(
                "ETL_CREDENTIAL_ENCRYPTION_KEY is not a valid Fernet key."
            ) from exc

    def encrypt(self, plaintext: str) -> str:
        """Encrypt a repository password or authentication token."""
        if not plaintext:
            raise CredentialEncryptionError(
                "Repository credential cannot be empty."
            )

        encrypted_value = self._cipher.encrypt(plaintext.encode("utf-8"))
        return encrypted_value.decode("utf-8")

    def decrypt(self, ciphertext: str) -> str:
        """Decrypt a repository password or authentication token."""
        if not ciphertext:
            raise CredentialEncryptionError(
                "Encrypted repository credential cannot be empty."
            )

        try:
            decrypted_value = self._cipher.decrypt(ciphertext.encode("utf-8"))
            return decrypted_value.decode("utf-8")
        except InvalidToken as exc:
            raise CredentialEncryptionError(
                "Repository credential could not be decrypted. "
                "Verify the encryption key and key version."
            ) from exc


credential_cipher = CredentialCipher(settings.etl_credential_encryption_key)


def _signing_key() -> bytes:
    """Return the stable HMAC key used by every API process.

    The existing application uses ETL_CREDENTIAL_ENCRYPTION_KEY for both
    Fernet encryption and connection-test HMAC signing. This function keeps
    that behavior for compatibility while failing clearly if the setting is
    absent.
    """
    value = settings.etl_credential_encryption_key

    if not value:
        raise RuntimeError(
            "ETL_CREDENTIAL_ENCRYPTION_KEY is required for "
            "connection-test token signing."
        )

    return value.encode("utf-8")


def _b64url_encode(value: bytes) -> str:
    """Encode bytes using URL-safe Base64 without trailing padding."""
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _b64url_decode(value: str) -> bytes:
    """Decode URL-safe Base64 with optional omitted padding."""
    if not value:
        raise ValueError("Base64 value cannot be empty.")

    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode((value + padding).encode("ascii"))


def create_connection_configuration_hash(
    connection_details: dict[str, Any],
) -> str:
    """Create a stable hash of non-sensitive repository connection details."""
    sanitized_details = {
        key: value
        for key, value in connection_details.items()
        if key
        not in {
            "password",
            "test_token",
            "source_analysis_token",
            "selected_mapping_keys",
            "scope_type",
        }
    }

    serialized_details = json.dumps(
        sanitized_details,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )

    return hashlib.sha256(serialized_details.encode("utf-8")).hexdigest()


def create_connection_test_token(configuration_hash: str) -> str:
    """Create a v2 signed, short-lived connection-test token.

    Token format:
        v2.<base64url(payload-json)>.<base64url(hmac-sha256-signature)>

    Payload and signature are encoded separately. This avoids the legacy bug
    where a raw HMAC containing a period byte could be split incorrectly.
    """
    if not configuration_hash:
        raise ConnectionTestTokenError(
            "Connection configuration hash is required."
        )

    current_time = int(time.time())
    payload = {
        "configuration_hash": configuration_hash,
        "issued_at": current_time,
        "expires_at": current_time + settings.connection_test_ttl_seconds,
        "token_version": 2,
    }

    payload_bytes = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")

    signature = hmac.new(
        _signing_key(),
        payload_bytes,
        hashlib.sha256,
    ).digest()

    return ".".join(
        (
            _TOKEN_VERSION,
            _b64url_encode(payload_bytes),
            _b64url_encode(signature),
        )
    )


def _decode_v2_token(token: str) -> tuple[bytes, bytes]:
    """Decode the current, segmented v2 token format."""
    parts = token.split(".")

    if len(parts) != 3 or parts[0] != _TOKEN_VERSION:
        raise ValueError("Not a v2 connection-test token.")

    payload_bytes = _b64url_decode(parts[1])
    provided_signature = _b64url_decode(parts[2])

    if len(provided_signature) != _HMAC_DIGEST_SIZE:
        raise ValueError("Invalid HMAC signature length.")

    return payload_bytes, provided_signature


def _decode_legacy_token(token: str) -> tuple[bytes, bytes]:
    """Decode the legacy token format safely.

    Legacy format encoded this byte sequence as one Base64 value:
        payload_json + b'.' + raw_32_byte_hmac

    The old implementation used rsplit(b'.', 1), which is unsafe because the
    raw HMAC can contain a period byte. Since SHA-256 HMAC is always 32 bytes,
    the correct legacy boundary is deterministic: the final 32 bytes are the
    signature and the byte immediately before them must be the separator.
    """
    signed_value = base64.urlsafe_b64decode(token.encode("utf-8"))

    minimum_length = _HMAC_DIGEST_SIZE + 2
    if len(signed_value) < minimum_length:
        raise ValueError("Legacy token is too short.")

    separator_index = len(signed_value) - _HMAC_DIGEST_SIZE - 1

    if signed_value[separator_index : separator_index + 1] != b".":
        raise ValueError("Legacy token separator is invalid.")

    payload_bytes = signed_value[:separator_index]
    provided_signature = signed_value[-_HMAC_DIGEST_SIZE:]

    return payload_bytes, provided_signature


def _decode_connection_test_token(token: str) -> tuple[bytes, bytes]:
    """Decode v2 tokens and remain compatible with legacy v1 tokens."""
    if token.startswith(f"{_TOKEN_VERSION}."):
        return _decode_v2_token(token)

    return _decode_legacy_token(token)


def verify_connection_test_token(
    token: str,
    expected_configuration_hash: str,
) -> dict[str, Any]:
    """Validate signature, expiry, and configuration binding."""
    if not token:
        raise ConnectionTestTokenError(
            "Connection-test token is required."
        )

    if not expected_configuration_hash:
        raise ConnectionTestTokenError(
            "Expected connection configuration hash is required."
        )

    try:
        payload_bytes, provided_signature = _decode_connection_test_token(
            token.strip()
        )
    except Exception as exc:
        raise ConnectionTestTokenError(
            "Connection-test token format is invalid."
        ) from exc

    expected_signature = hmac.new(
        _signing_key(),
        payload_bytes,
        hashlib.sha256,
    ).digest()

    if not hmac.compare_digest(provided_signature, expected_signature):
        raise ConnectionTestTokenError(
            "Connection-test token signature is invalid."
        )

    try:
        payload = json.loads(payload_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ConnectionTestTokenError(
            "Connection-test token payload is invalid."
        ) from exc

    if not isinstance(payload, dict):
        raise ConnectionTestTokenError(
            "Connection-test token payload is invalid."
        )

    expires_at = payload.get("expires_at")
    if not isinstance(expires_at, int):
        raise ConnectionTestTokenError(
            "Connection-test token expiry is invalid."
        )

    if expires_at < int(time.time()):
        raise ConnectionTestTokenError(
            "Connection test has expired. "
            "Test the repository connection again."
        )

    token_configuration_hash = payload.get("configuration_hash")
    if not isinstance(token_configuration_hash, str):
        raise ConnectionTestTokenError(
            "Connection-test token configuration hash is invalid."
        )

    if not hmac.compare_digest(
        token_configuration_hash,
        str(expected_configuration_hash),
    ):
        raise ConnectionTestTokenError(
            "Repository connection details changed after the connection test."
        )

    return payload


def validation_credential_cipher() -> CredentialCipher:
    """Return a credential cipher for Validation database credentials."""
    return CredentialCipher(settings.etl_credential_encryption_key)
