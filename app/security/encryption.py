"""Envelope encryption for PII — master key → per-client DEK → AES-256-GCM.

In production, DEKs are encrypted by AWS KMS CMK (pii_kms_key_arn in settings).
In development, DEKs are encrypted using a local master key derived from jwt_secret.
"""

from __future__ import annotations

import base64
import hashlib
import os

import structlog
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

logger = structlog.get_logger(__name__)

_NONCE_BYTES = 12  # 96-bit nonce for AES-GCM
_DEK_BYTES = 32  # 256-bit DEK
_PBKDF2_SALT = b"jip-data-engine-pii"
_PBKDF2_ITERATIONS = 100_000


class EnvelopeEncryption:
    """Manages envelope encryption lifecycle.

    Each client has a per-client Data Encryption Key (DEK).
    The DEK is encrypted with a master key before being stored in de_client_keys.
    Field encryption uses AES-256-GCM with a fresh random nonce per operation.
    """

    def __init__(self, master_key: bytes | None = None) -> None:
        """Initialize with a local master key for dev, or None to derive from config."""
        self._master_key: bytes = master_key if master_key is not None else self._derive_local_key()
        if len(self._master_key) != 32:
            raise ValueError("Master key must be exactly 32 bytes (256 bits)")

    def generate_dek(self) -> tuple[bytes, bytes]:
        """Generate a new Data Encryption Key.

        Returns:
            (plaintext_dek, encrypted_dek) where:
            - plaintext_dek: raw 32-byte key — use for encrypt/decrypt, never store
            - encrypted_dek: base64-encoded encrypted key — store in de_client_keys
        """
        plaintext_dek = os.urandom(_DEK_BYTES)
        encrypted_dek = self._encrypt_with_master(plaintext_dek)
        logger.debug("dek_generated", dek_size=len(plaintext_dek))
        return plaintext_dek, encrypted_dek

    def decrypt_dek(self, encrypted_dek: bytes) -> bytes:
        """Decrypt a stored DEK using the master key.

        Args:
            encrypted_dek: base64-encoded encrypted DEK from de_client_keys

        Returns:
            Plaintext 32-byte DEK
        """
        return self._decrypt_with_master(encrypted_dek)

    def encrypt_field(self, plaintext: str, dek: bytes) -> str:
        """Encrypt a PII field using AES-256-GCM.

        Args:
            plaintext: UTF-8 string to encrypt
            dek: 32-byte plaintext Data Encryption Key

        Returns:
            base64-encoded string of (nonce[12] + ciphertext + tag[16])
        """
        if len(dek) != 32:
            raise ValueError("DEK must be exactly 32 bytes")
        nonce = os.urandom(_NONCE_BYTES)
        aesgcm = AESGCM(dek)
        ciphertext_with_tag = aesgcm.encrypt(nonce, plaintext.encode("utf-8"), None)
        blob = nonce + ciphertext_with_tag
        return base64.b64encode(blob).decode("ascii")

    def decrypt_field(self, ciphertext_b64: str, dek: bytes) -> str:
        """Decrypt a PII field.

        Args:
            ciphertext_b64: base64-encoded string of (nonce[12] + ciphertext + tag[16])
            dek: 32-byte plaintext Data Encryption Key

        Returns:
            Decrypted UTF-8 plaintext string
        """
        if len(dek) != 32:
            raise ValueError("DEK must be exactly 32 bytes")
        blob = base64.b64decode(ciphertext_b64)
        if len(blob) < _NONCE_BYTES + 16:
            raise ValueError("Ciphertext blob too short — corrupt or wrong format")
        nonce = blob[:_NONCE_BYTES]
        ciphertext_with_tag = blob[_NONCE_BYTES:]
        aesgcm = AESGCM(dek)
        plaintext_bytes = aesgcm.decrypt(nonce, ciphertext_with_tag, None)
        return plaintext_bytes.decode("utf-8")

    def _encrypt_with_master(self, data: bytes) -> bytes:
        """Encrypt raw bytes with the master key. Returns base64-encoded blob."""
        nonce = os.urandom(_NONCE_BYTES)
        aesgcm = AESGCM(self._master_key)
        ciphertext_with_tag = aesgcm.encrypt(nonce, data, None)
        blob = nonce + ciphertext_with_tag
        return base64.b64encode(blob)

    def _decrypt_with_master(self, encrypted_b64: bytes) -> bytes:
        """Decrypt base64-encoded blob with the master key. Returns raw plaintext bytes."""
        blob = base64.b64decode(encrypted_b64)
        if len(blob) < _NONCE_BYTES + 16:
            raise ValueError("Encrypted blob too short — corrupt or wrong format")
        nonce = blob[:_NONCE_BYTES]
        ciphertext_with_tag = blob[_NONCE_BYTES:]
        aesgcm = AESGCM(self._master_key)
        return aesgcm.decrypt(nonce, ciphertext_with_tag, None)

    @staticmethod
    def _derive_local_key() -> bytes:
        """Derive a 256-bit key from config jwt_secret for local/dev use.

        Uses PBKDF2-HMAC-SHA256 with a fixed salt and 100k iterations.
        This is deterministic — the same jwt_secret always produces the same key.
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
