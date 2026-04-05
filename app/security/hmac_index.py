"""HMAC blind index for searchable encrypted PII.

Truncated to 8 characters for bucket search — query by hash prefix,
then decrypt the small result set to find exact match.

Security note: truncation to 8 hex characters gives 2^32 possible values.
For a typical client base (<100k records) this means near-zero collisions,
while preventing full-value exposure from the hash alone.
"""

from __future__ import annotations

import hashlib
import hmac

import structlog

logger = structlog.get_logger(__name__)

_HMAC_TRUNCATE_LENGTH = 8  # characters (4 bytes of entropy)
_PBKDF2_SALT = b"jip-data-engine-hmac"
_PBKDF2_ITERATIONS = 100_000


class HmacBlindIndex:
    """Compute truncated HMAC blind indexes for PII fields.

    The same plaintext always produces the same index (deterministic),
    enabling WHERE clause lookups without decrypting every row.
    Input is normalized (lowercase, stripped) before hashing so that
    "Foo@Bar.com" and "foo@bar.com " produce the same index.
    """

    def __init__(self, hmac_key: bytes | None = None) -> None:
        """Initialize with HMAC key. Derives from config if not provided."""
        self._hmac_key: bytes = hmac_key if hmac_key is not None else self._derive_local_key()

    def compute(self, value: str) -> str:
        """Compute 8-character truncated HMAC for a PII value.

        Normalizes input (lowercase, strip whitespace) before hashing.

        Args:
            value: plaintext PII value

        Returns:
            First 8 hex characters of HMAC-SHA256 over normalized input
        """
        normalized = value.strip().lower()
        mac = hmac.new(
            key=self._hmac_key,
            msg=normalized.encode("utf-8"),
            digestmod=hashlib.sha256,
        )
        return mac.hexdigest()[:_HMAC_TRUNCATE_LENGTH]

    def search(self, value: str) -> str:
        """Alias for compute — used in search queries for semantic clarity.

        Args:
            value: plaintext PII value to search for

        Returns:
            First 8 hex characters of HMAC-SHA256 over normalized input
        """
        return self.compute(value)

    @staticmethod
    def _derive_local_key() -> bytes:
        """Derive a 256-bit HMAC key from config jwt_secret for local/dev use.

        Uses PBKDF2-HMAC-SHA256 with a salt distinct from the encryption key salt.
        """
        from app.config import get_settings

        settings = get_settings()
        key = hashlib.pbkdf2_hmac(
            hash_name="sha256",
            password=settings.jwt_secret.encode("utf-8"),
            salt=_PBKDF2_SALT,
            iterations=_PBKDF2_ITERATIONS,
            dklen=32,
        )
        return key
