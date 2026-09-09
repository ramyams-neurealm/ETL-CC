"""
Security utilities for ETL repository credentials and connection tests.

This module encrypts repository passwords before database storage and
creates short-lived signed tokens after successful connection testing.
"""

import base64
import hashlib
import hmac
import json
import time

from cryptography.fernet import Fernet, InvalidToken

from etl_cc.config import settings


class CredentialEncryptionError(Exception):
    """
    Raised when an ETL repository credential cannot be encrypted or decrypted.
    """


class ConnectionTestTokenError(Exception):
    """
    Raised when a connection-test token is invalid, expired, or mismatched.
    """


class CredentialCipher:
    """
    Encrypts and decrypts ETL repository passwords using Fernet.

    The encryption key is loaded from the project-level .env file.
    """

    def __init__(self, encryption_key: str) -> None:
        try:
            self._cipher = Fernet(
                encryption_key.encode("utf-8")
            )
        except (TypeError, ValueError) as exc:
            raise CredentialEncryptionError(
                "ETL_CREDENTIAL_ENCRYPTION_KEY is not a valid Fernet key."
            ) from exc

    def encrypt(self, plaintext: str) -> str:
        """
        Encrypts a repository password or token.

        Args:
            plaintext: Repository password or authentication token.

        Returns:
            Encrypted credential suitable for database storage.
        """

        if not plaintext:
            raise CredentialEncryptionError(
                "Repository credential cannot be empty."
            )

        encrypted_value = self._cipher.encrypt(
            plaintext.encode("utf-8")
        )

        return encrypted_value.decode("utf-8")

    def decrypt(self, ciphertext: str) -> str:
        """
        Decrypts a repository password or token for connector use.

        Args:
            ciphertext: Encrypted credential stored in repository_etl.

        Returns:
            Original repository password or token.
        """

        if not ciphertext:
            raise CredentialEncryptionError(
                "Encrypted repository credential cannot be empty."
            )

        try:
            decrypted_value = self._cipher.decrypt(
                ciphertext.encode("utf-8")
            )

            return decrypted_value.decode("utf-8")

        except InvalidToken as exc:
            raise CredentialEncryptionError(
                "Repository credential could not be decrypted. "
                "Verify the encryption key and key version."
            ) from exc


credential_cipher = CredentialCipher(
    settings.etl_credential_encryption_key
)


def create_connection_configuration_hash(
    connection_details: dict,
) -> str:
    """
    Creates a stable hash of non-sensitive repository connection details.

    The password must not be included in connection_details. The hash is
    used to ensure that connection details were not changed after the
    Test Connection operation succeeded.
    """

    sanitized_details = {
        key: value
        for key, value in connection_details.items()
        if key not in {
            "password",
            "test_token",
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

    return hashlib.sha256(
        serialized_details.encode("utf-8")
    ).hexdigest()


def create_connection_test_token(
    configuration_hash: str,
) -> str:
    """
    Creates a signed, short-lived connection-test token.

    Args:
        configuration_hash: Hash of the tested repository configuration.

    Returns:
        URL-safe signed token returned to the UI after connection testing.
    """

    current_time = int(time.time())

    payload = {
        "configuration_hash": configuration_hash,
        "issued_at": current_time,
        "expires_at": (
            current_time
            + settings.connection_test_ttl_seconds
        ),
    }

    payload_bytes = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")

    signature = hmac.new(
        settings.etl_credential_encryption_key.encode(
            "utf-8"
        ),
        payload_bytes,
        hashlib.sha256,
    ).digest()

    signed_value = (
        payload_bytes
        + b"."
        + signature
    )

    return base64.urlsafe_b64encode(
        signed_value
    ).decode("utf-8")


def verify_connection_test_token(
    token: str,
    expected_configuration_hash: str,
) -> dict:
    """
    Validates a connection-test token before saving the repository.

    The function verifies the token signature, expiry timestamp, and
    repository configuration hash.

    Args:
        token: Token returned by the Test Connection API.
        expected_configuration_hash: Hash created from the save request.

    Returns:
        Validated token payload.

    Raises:
        ConnectionTestTokenError: If validation fails.
    """

    if not token:
        raise ConnectionTestTokenError(
            "Connection-test token is required."
        )

    try:
        signed_value = base64.urlsafe_b64decode(
            token.encode("utf-8")
        )

        payload_bytes, provided_signature = (
            signed_value.rsplit(
                b".",
                1,
            )
        )

    except Exception as exc:
        raise ConnectionTestTokenError(
            "Connection-test token format is invalid."
        ) from exc

    expected_signature = hmac.new(
        settings.etl_credential_encryption_key.encode(
            "utf-8"
        ),
        payload_bytes,
        hashlib.sha256,
    ).digest()

    if not hmac.compare_digest(
        provided_signature,
        expected_signature,
    ):
        raise ConnectionTestTokenError(
            "Connection-test token signature is invalid."
        )

    try:
        payload = json.loads(
            payload_bytes.decode("utf-8")
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ConnectionTestTokenError(
            "Connection-test token payload is invalid."
        ) from exc

    expires_at = payload.get(
        "expires_at"
    )

    if not isinstance(expires_at, int):
        raise ConnectionTestTokenError(
            "Connection-test token expiry is invalid."
        )

    if expires_at < int(time.time()):
        raise ConnectionTestTokenError(
            "Connection test has expired. "
            "Test the repository connection again."
        )

    token_configuration_hash = payload.get(
        "configuration_hash"
    )

    if not hmac.compare_digest(
        str(token_configuration_hash),
        expected_configuration_hash,
    ):
        raise ConnectionTestTokenError(
            "Repository connection details changed "
            "after the connection test."
        )

    return payload