"""High-level PII service — coordinates encryption, HMAC, and key management.

Column mapping (actual de_clients schema):
  Encrypted fields : email_enc, phone_enc, pan_enc
  HMAC indexes     : email_hash, phone_hash, pan_hash
  Plaintext        : name (stored as-is in de_clients.name)

Supported field_name values for search/encrypt/decrypt: "email", "phone", "pan"
"""

from __future__ import annotations

from typing import Optional

import structlog
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.clients import DeClientKeys, DeClients, DePiiAccessLog
from app.security.encryption import EnvelopeEncryption
from app.security.hmac_index import HmacBlindIndex

logger = structlog.get_logger(__name__)

# Maps logical field name → (enc_column_attr, hash_column_attr)
_FIELD_MAP: dict[str, tuple[str, str]] = {
    "email": ("email_enc", "email_hash"),
    "phone": ("phone_enc", "phone_hash"),
    "pan": ("pan_enc", "pan_hash"),
}

_SUPPORTED_FIELDS = frozenset(_FIELD_MAP.keys())


class PiiService:
    """Manages PII encryption lifecycle for client records.

    Coordinates per-client DEK management, field-level AES-256-GCM encryption,
    HMAC blind index computation, key rotation, and access audit logging.
    """

    def __init__(self, encryption: EnvelopeEncryption, hmac_index: HmacBlindIndex) -> None:
        self._encryption = encryption
        self._hmac = hmac_index

    async def encrypt_client_data(
        self,
        session: AsyncSession,
        client_id: str,
        email: Optional[str] = None,
        phone: Optional[str] = None,
        pan: Optional[str] = None,
    ) -> dict[str, Optional[str]]:
        """Encrypt PII fields and compute HMAC indexes.

        Gets or creates DEK for client from de_client_keys.
        Only fields with non-None values are encrypted; others map to None.

        Args:
            session: async DB session
            client_id: client identifier (String(50))
            email: plaintext email or None
            phone: plaintext phone or None
            pan: plaintext PAN or None

        Returns:
            Dict with keys: email_enc, email_hash, phone_enc, phone_hash,
            pan_enc, pan_hash — each value is the encrypted/hashed string or None.
        """
        dek = await self._get_or_create_dek(session, client_id)
        fields = {"email": email, "phone": phone, "pan": pan}
        result: dict[str, Optional[str]] = {}

        for field_name, plaintext in fields.items():
            enc_col, hash_col = _FIELD_MAP[field_name]
            if plaintext is not None:
                result[enc_col] = self._encryption.encrypt_field(plaintext, dek)
                result[hash_col] = self._hmac.compute(plaintext)
                logger.debug(
                    "pii_field_encrypted",
                    client_id=client_id,
                    field=field_name,
                )
            else:
                result[enc_col] = None
                result[hash_col] = None

        return result

    async def decrypt_client_data(
        self,
        session: AsyncSession,
        client_id: str,
        encrypted_fields: dict[str, Optional[str]],
    ) -> dict[str, Optional[str]]:
        """Decrypt PII fields for a client.

        Fetches active DEK from de_client_keys.

        Args:
            session: async DB session
            client_id: client identifier
            encrypted_fields: dict with enc column values, e.g.
                {"email_enc": "...", "phone_enc": "...", "pan_enc": "..."}

        Returns:
            Dict with plaintext values keyed by logical name:
            {"email": "...", "phone": "...", "pan": "..."}
        """
        dek = await self._load_active_dek(session, client_id)
        result: dict[str, Optional[str]] = {}

        for field_name, (enc_col, _hash_col) in _FIELD_MAP.items():
            ciphertext = encrypted_fields.get(enc_col)
            if ciphertext is not None:
                result[field_name] = self._encryption.decrypt_field(ciphertext, dek)
                logger.debug(
                    "pii_field_decrypted",
                    client_id=client_id,
                    field=field_name,
                )
            else:
                result[field_name] = None

        return result

    async def search_by_pii(
        self,
        session: AsyncSession,
        field_name: str,
        search_value: str,
    ) -> list[str]:
        """Search for clients by PII value using HMAC blind index.

        Steps:
        1. Compute HMAC of search_value
        2. Query de_clients WHERE {field_name}_hash = hmac_value
        3. Decrypt and verify exact match (bucket search)

        Args:
            session: async DB session
            field_name: one of "email", "phone", "pan"
            search_value: plaintext value to search for

        Returns:
            List of matching client_ids
        """
        if field_name not in _SUPPORTED_FIELDS:
            raise ValueError(f"Unsupported field '{field_name}'. Must be one of: {_SUPPORTED_FIELDS}")

        enc_col, hash_col = _FIELD_MAP[field_name]
        hmac_value = self._hmac.search(search_value)

        stmt = select(DeClients).where(
            getattr(DeClients, hash_col) == hmac_value
        )
        rows = (await session.execute(stmt)).scalars().all()

        matches: list[str] = []
        for client in rows:
            ciphertext = getattr(client, enc_col)
            if ciphertext is None:
                continue
            try:
                dek = await self._load_active_dek(session, client.client_id)
                plaintext = self._encryption.decrypt_field(ciphertext, dek)
                # Case-insensitive exact match after normalizing
                if plaintext.strip().lower() == search_value.strip().lower():
                    matches.append(client.client_id)
            except Exception:
                logger.warning(
                    "pii_search_decrypt_failed",
                    client_id=client.client_id,
                    field=field_name,
                )

        logger.info(
            "pii_search_completed",
            field=field_name,
            bucket_size=len(rows),
            matches=len(matches),
        )
        return matches

    async def rotate_client_key(
        self,
        session: AsyncSession,
        client_id: str,
    ) -> None:
        """Rotate DEK for a client.

        Steps:
        1. Load all encrypted PII with the old DEK
        2. Generate a new DEK
        3. Re-encrypt all PII fields with the new DEK
        4. Mark old key inactive (append-only audit trail)
        5. Insert new key with incremented version

        Args:
            session: async DB session
            client_id: client identifier
        """
        old_dek = await self._load_active_dek(session, client_id)

        # Fetch current encrypted values
        client_stmt = select(DeClients).where(DeClients.client_id == client_id)
        client = (await session.execute(client_stmt)).scalar_one_or_none()
        if client is None:
            raise ValueError(f"Client '{client_id}' not found")

        # Decrypt all existing PII with old key
        decrypted: dict[str, Optional[str]] = {}
        for field_name, (enc_col, _hash_col) in _FIELD_MAP.items():
            ciphertext = getattr(client, enc_col)
            if ciphertext is not None:
                decrypted[field_name] = self._encryption.decrypt_field(ciphertext, old_dek)
            else:
                decrypted[field_name] = None

        # Generate new DEK
        new_plaintext_dek, new_encrypted_dek = self._encryption.generate_dek()

        # Re-encrypt all PII fields with new DEK
        new_enc_values: dict[str, Optional[str]] = {}
        for field_name, (enc_col, hash_col) in _FIELD_MAP.items():
            plaintext = decrypted.get(field_name)
            if plaintext is not None:
                new_enc_values[enc_col] = self._encryption.encrypt_field(plaintext, new_plaintext_dek)
            else:
                new_enc_values[enc_col] = None

        # Determine next key version
        version_stmt = select(DeClientKeys.key_version).where(
            DeClientKeys.client_id == client_id
        ).order_by(DeClientKeys.key_version.desc())
        latest_version = (await session.execute(version_stmt)).scalar()
        next_version = (latest_version or 0) + 1

        # Mark old key inactive
        deactivate_stmt = (
            update(DeClientKeys)
            .where(DeClientKeys.client_id == client_id, DeClientKeys.is_active.is_(True))
            .values(is_active=False)
        )
        await session.execute(deactivate_stmt)

        # Insert new key (append-only)
        new_key = DeClientKeys(
            client_id=client_id,
            key_version=next_version,
            encrypted_dek=new_encrypted_dek.decode("ascii"),
            is_active=True,
        )
        session.add(new_key)

        # Update client record with re-encrypted values
        update_stmt = (
            update(DeClients)
            .where(DeClients.client_id == client_id)
            .values(**new_enc_values)
        )
        await session.execute(update_stmt)

        logger.info(
            "dek_rotated",
            client_id=client_id,
            new_version=next_version,
        )

    async def log_pii_access(
        self,
        session: AsyncSession,
        client_id: str,
        accessor: str,
        field_accessed: str,
        access_type: str = "decrypt",
        ip_address: Optional[str] = None,
    ) -> None:
        """Log PII access to de_pii_access_log.

        Args:
            session: async DB session
            client_id: client whose PII was accessed
            accessor: identifier of the accessor (user/service)
            field_accessed: logical field name (e.g. "email")
            access_type: operation type — "decrypt", "search", "encrypt"
            ip_address: source IP of the accessor
        """
        # purpose encodes access_type + field for compact audit
        purpose = f"{access_type}:{field_accessed}"
        log_entry = DePiiAccessLog(
            client_id=client_id,
            accessed_by=accessor,
            fields_accessed=[field_accessed],
            purpose=purpose,
            source_ip=ip_address,
        )
        session.add(log_entry)
        logger.info(
            "pii_access_logged",
            client_id=client_id,
            accessor=accessor,
            field=field_accessed,
            access_type=access_type,
        )

    async def _get_or_create_dek(
        self,
        session: AsyncSession,
        client_id: str,
    ) -> bytes:
        """Get active DEK for client, creating one if none exists.

        Args:
            session: async DB session
            client_id: client identifier

        Returns:
            Plaintext 32-byte DEK
        """
        stmt = (
            select(DeClientKeys)
            .where(DeClientKeys.client_id == client_id, DeClientKeys.is_active.is_(True))
            .order_by(DeClientKeys.key_version.desc())
        )
        key_row = (await session.execute(stmt)).scalar_one_or_none()

        if key_row is not None:
            encrypted_dek_bytes = key_row.encrypted_dek.encode("ascii")
            return self._encryption.decrypt_dek(encrypted_dek_bytes)

        # No DEK exists — create one
        plaintext_dek, encrypted_dek = self._encryption.generate_dek()
        new_key = DeClientKeys(
            client_id=client_id,
            key_version=1,
            encrypted_dek=encrypted_dek.decode("ascii"),
            is_active=True,
        )
        session.add(new_key)
        logger.info("dek_created", client_id=client_id, key_version=1)
        return plaintext_dek

    async def _load_active_dek(
        self,
        session: AsyncSession,
        client_id: str,
    ) -> bytes:
        """Load and decrypt the active DEK for a client.

        Raises ValueError if no active DEK is found.

        Args:
            session: async DB session
            client_id: client identifier

        Returns:
            Plaintext 32-byte DEK
        """
        stmt = (
            select(DeClientKeys)
            .where(DeClientKeys.client_id == client_id, DeClientKeys.is_active.is_(True))
            .order_by(DeClientKeys.key_version.desc())
        )
        key_row = (await session.execute(stmt)).scalar_one_or_none()

        if key_row is None:
            raise ValueError(f"No active DEK found for client '{client_id}'")

        encrypted_dek_bytes = key_row.encrypted_dek.encode("ascii")
        return self._encryption.decrypt_dek(encrypted_dek_bytes)
