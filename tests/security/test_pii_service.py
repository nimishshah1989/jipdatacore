"""Tests for PiiService — encrypt, decrypt, search, rotate, log."""

from __future__ import annotations

import os
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.security.encryption import EnvelopeEncryption
from app.security.hmac_index import HmacBlindIndex
from app.security.pii_service import PiiService


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def master_key() -> bytes:
    return os.urandom(32)


@pytest.fixture
def hmac_key() -> bytes:
    return os.urandom(32)


@pytest.fixture
def enc(master_key: bytes) -> EnvelopeEncryption:
    return EnvelopeEncryption(master_key=master_key)


@pytest.fixture
def hi(hmac_key: bytes) -> HmacBlindIndex:
    return HmacBlindIndex(hmac_key=hmac_key)


@pytest.fixture
def svc(enc: EnvelopeEncryption, hi: HmacBlindIndex) -> PiiService:
    return PiiService(encryption=enc, hmac_index=hi)


def make_mock_session() -> AsyncMock:
    """Build a mock AsyncSession with async execute returning empty results by default."""
    session = AsyncMock()
    session.add = MagicMock()
    return session


def make_key_row(client_id: str, enc: EnvelopeEncryption, version: int = 1) -> MagicMock:
    """Build a mock DeClientKeys row with a real encrypted DEK."""
    plaintext_dek, encrypted_dek_bytes = enc.generate_dek()
    row = MagicMock()
    row.client_id = client_id
    row.key_version = version
    row.encrypted_dek = encrypted_dek_bytes.decode("ascii")
    row.is_active = True
    return row, plaintext_dek


# ---------------------------------------------------------------------------
# encrypt_client_data
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_encrypt_client_data_returns_all_encrypted_fields(
    svc: PiiService, enc: EnvelopeEncryption
) -> None:
    client_id = "client-001"
    session = make_mock_session()

    # Mock _get_or_create_dek — no existing key → creates one
    scalar_result = MagicMock()
    scalar_result.scalar_one_or_none.return_value = None
    session.execute.return_value = scalar_result

    result = await svc.encrypt_client_data(
        session=session,
        client_id=client_id,
        email="test@example.com",
        phone="9876543210",
        pan="ABCDE1234F",
    )

    assert "email_enc" in result
    assert "email_hash" in result
    assert "phone_enc" in result
    assert "phone_hash" in result
    assert "pan_enc" in result
    assert "pan_hash" in result
    # All values should be non-None
    assert all(v is not None for v in result.values())


@pytest.mark.asyncio
async def test_encrypt_client_data_none_fields_map_to_none(
    svc: PiiService,
) -> None:
    client_id = "client-002"
    session = make_mock_session()

    scalar_result = MagicMock()
    scalar_result.scalar_one_or_none.return_value = None
    session.execute.return_value = scalar_result

    result = await svc.encrypt_client_data(
        session=session,
        client_id=client_id,
        email="test@example.com",
        phone=None,
        pan=None,
    )

    assert result["email_enc"] is not None
    assert result["phone_enc"] is None
    assert result["phone_hash"] is None
    assert result["pan_enc"] is None
    assert result["pan_hash"] is None


@pytest.mark.asyncio
async def test_encrypt_client_data_hmac_is_8_chars(
    svc: PiiService,
) -> None:
    client_id = "client-003"
    session = make_mock_session()

    scalar_result = MagicMock()
    scalar_result.scalar_one_or_none.return_value = None
    session.execute.return_value = scalar_result

    result = await svc.encrypt_client_data(
        session=session,
        client_id=client_id,
        email="foo@bar.com",
    )

    assert len(result["email_hash"]) == 8


# ---------------------------------------------------------------------------
# decrypt_client_data round-trip
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_decrypt_client_data_round_trips(
    svc: PiiService, enc: EnvelopeEncryption
) -> None:
    client_id = "client-004"
    session = make_mock_session()

    # First call (encrypt): no existing key
    scalar_none = MagicMock()
    scalar_none.scalar_one_or_none.return_value = None

    # Generate a real key we can use
    plaintext_dek, encrypted_dek_bytes = enc.generate_dek()
    key_row = MagicMock()
    key_row.encrypted_dek = encrypted_dek_bytes.decode("ascii")
    key_row.key_version = 1

    # For encrypt: no existing key → creates one
    session.execute.return_value = scalar_none

    encrypted = await svc.encrypt_client_data(
        session=session,
        client_id=client_id,
        email="roundtrip@example.com",
        phone="9999900000",
        pan="ZZZZZ9999Z",
    )

    # For decrypt: return the key row using the same DEK captured during add
    # We need to find out what encrypted_dek was stored.
    # Capture the key object added to the session
    added_key = session.add.call_args_list[0][0][0]
    key_row_real = MagicMock()
    key_row_real.encrypted_dek = added_key.encrypted_dek

    scalar_with_key = MagicMock()
    scalar_with_key.scalar_one_or_none.return_value = key_row_real
    session.execute.return_value = scalar_with_key

    decrypted = await svc.decrypt_client_data(
        session=session,
        client_id=client_id,
        encrypted_fields=encrypted,
    )

    assert decrypted["email"] == "roundtrip@example.com"
    assert decrypted["phone"] == "9999900000"
    assert decrypted["pan"] == "ZZZZZ9999Z"


@pytest.mark.asyncio
async def test_decrypt_client_data_none_enc_returns_none(
    svc: PiiService, enc: EnvelopeEncryption
) -> None:
    client_id = "client-005"
    session = make_mock_session()

    plaintext_dek, encrypted_dek_bytes = enc.generate_dek()
    key_row = MagicMock()
    key_row.encrypted_dek = encrypted_dek_bytes.decode("ascii")

    scalar_with_key = MagicMock()
    scalar_with_key.scalar_one_or_none.return_value = key_row
    session.execute.return_value = scalar_with_key

    decrypted = await svc.decrypt_client_data(
        session=session,
        client_id=client_id,
        encrypted_fields={"email_enc": None, "phone_enc": None, "pan_enc": None},
    )

    assert decrypted["email"] is None
    assert decrypted["phone"] is None
    assert decrypted["pan"] is None


# ---------------------------------------------------------------------------
# search_by_pii
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_by_pii_returns_matching_client_ids(
    svc: PiiService, enc: EnvelopeEncryption, hi: HmacBlindIndex
) -> None:
    client_id = "client-search-001"
    session = make_mock_session()

    plaintext_dek, encrypted_dek_bytes = enc.generate_dek()
    encrypted_email = enc.encrypt_field("match@example.com", plaintext_dek)

    # Build mock client row
    mock_client = MagicMock()
    mock_client.client_id = client_id
    mock_client.email_enc = encrypted_email

    # Build mock key row
    key_row = MagicMock()
    key_row.encrypted_dek = encrypted_dek_bytes.decode("ascii")

    # First execute call (search by hash): returns client list
    scalars_result = MagicMock()
    scalars_result.scalars.return_value.all.return_value = [mock_client]

    # Second execute call (_load_active_dek): returns key row
    key_result = MagicMock()
    key_result.scalar_one_or_none.return_value = key_row

    session.execute.side_effect = [scalars_result, key_result]

    matches = await svc.search_by_pii(
        session=session,
        field_name="email",
        search_value="match@example.com",
    )

    assert client_id in matches


@pytest.mark.asyncio
async def test_search_by_pii_no_exact_match_returns_empty(
    svc: PiiService, enc: EnvelopeEncryption
) -> None:
    client_id = "client-search-002"
    session = make_mock_session()

    plaintext_dek, encrypted_dek_bytes = enc.generate_dek()
    # Encrypt a different email — simulates HMAC collision scenario
    encrypted_email = enc.encrypt_field("other@example.com", plaintext_dek)

    mock_client = MagicMock()
    mock_client.client_id = client_id
    mock_client.email_enc = encrypted_email

    key_row = MagicMock()
    key_row.encrypted_dek = encrypted_dek_bytes.decode("ascii")

    scalars_result = MagicMock()
    scalars_result.scalars.return_value.all.return_value = [mock_client]

    key_result = MagicMock()
    key_result.scalar_one_or_none.return_value = key_row

    session.execute.side_effect = [scalars_result, key_result]

    matches = await svc.search_by_pii(
        session=session,
        field_name="email",
        search_value="nomatch@example.com",
    )

    assert matches == []


@pytest.mark.asyncio
async def test_search_by_pii_invalid_field_raises(svc: PiiService) -> None:
    session = make_mock_session()
    with pytest.raises(ValueError, match="Unsupported field"):
        await svc.search_by_pii(session=session, field_name="invalid", search_value="x")


# ---------------------------------------------------------------------------
# rotate_client_key
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rotate_client_key_creates_new_key_and_deactivates_old(
    svc: PiiService, enc: EnvelopeEncryption
) -> None:
    client_id = "client-rotate-001"
    session = make_mock_session()

    # Setup initial DEK
    old_dek, old_encrypted_dek_bytes = enc.generate_dek()
    old_encrypted_email = enc.encrypt_field("rotate@example.com", old_dek)
    old_encrypted_phone = enc.encrypt_field("9876543210", old_dek)

    old_key_row = MagicMock()
    old_key_row.encrypted_dek = old_encrypted_dek_bytes.decode("ascii")
    old_key_row.key_version = 1

    mock_client = MagicMock()
    mock_client.client_id = client_id
    mock_client.email_enc = old_encrypted_email
    mock_client.phone_enc = old_encrypted_phone
    mock_client.pan_enc = None

    # Execute calls sequence:
    # 1. _load_active_dek → old key row
    # 2. select DeClients → mock_client
    # 3. select key_version (for max version) → scalar 1
    # 4. update (deactivate old key)
    # 5. update (client re-encrypt values)

    key_result = MagicMock()
    key_result.scalar_one_or_none.return_value = old_key_row

    client_result = MagicMock()
    client_result.scalar_one_or_none.return_value = mock_client

    version_result = MagicMock()
    version_result.scalar.return_value = 1

    update_result = MagicMock()

    session.execute.side_effect = [
        key_result,       # _load_active_dek
        client_result,    # select DeClients
        version_result,   # select max key_version
        update_result,    # update deactivate
        update_result,    # update re-encrypt client
    ]

    await svc.rotate_client_key(session=session, client_id=client_id)

    # A new DeClientKeys object must have been added
    added_objects = [c[0][0] for c in session.add.call_args_list]
    from app.models.clients import DeClientKeys
    new_keys = [o for o in added_objects if isinstance(o, DeClientKeys)]
    assert len(new_keys) == 1
    assert new_keys[0].key_version == 2
    assert new_keys[0].is_active is True
    # New encrypted DEK must differ from the old one
    assert new_keys[0].encrypted_dek != old_encrypted_dek_bytes.decode("ascii")


@pytest.mark.asyncio
async def test_rotate_client_key_missing_client_raises(svc: PiiService, enc: EnvelopeEncryption) -> None:
    client_id = "client-rotate-missing"
    session = make_mock_session()

    old_dek, old_encrypted_dek_bytes = enc.generate_dek()
    old_key_row = MagicMock()
    old_key_row.encrypted_dek = old_encrypted_dek_bytes.decode("ascii")

    key_result = MagicMock()
    key_result.scalar_one_or_none.return_value = old_key_row

    client_result = MagicMock()
    client_result.scalar_one_or_none.return_value = None

    session.execute.side_effect = [key_result, client_result]

    with pytest.raises(ValueError, match="not found"):
        await svc.rotate_client_key(session=session, client_id=client_id)


# ---------------------------------------------------------------------------
# log_pii_access
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_log_pii_access_adds_audit_entry(svc: PiiService) -> None:
    session = make_mock_session()

    await svc.log_pii_access(
        session=session,
        client_id="client-log-001",
        accessor="api-service",
        field_accessed="email",
        access_type="decrypt",
        ip_address="10.0.0.1",
    )

    assert session.add.called
    from app.models.clients import DePiiAccessLog
    added = session.add.call_args[0][0]
    assert isinstance(added, DePiiAccessLog)
    assert added.client_id == "client-log-001"
    assert added.accessed_by == "api-service"
    assert "email" in added.fields_accessed
    assert added.source_ip == "10.0.0.1"


@pytest.mark.asyncio
async def test_log_pii_access_without_ip(svc: PiiService) -> None:
    session = make_mock_session()

    await svc.log_pii_access(
        session=session,
        client_id="client-log-002",
        accessor="batch-job",
        field_accessed="pan",
    )

    from app.models.clients import DePiiAccessLog
    added = session.add.call_args[0][0]
    assert isinstance(added, DePiiAccessLog)
    assert added.source_ip is None


# ---------------------------------------------------------------------------
# _get_or_create_dek creates DEK when none exists
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_or_create_dek_creates_key_when_none_exists(
    svc: PiiService,
) -> None:
    client_id = "client-newdek-001"
    session = make_mock_session()

    scalar_result = MagicMock()
    scalar_result.scalar_one_or_none.return_value = None
    session.execute.return_value = scalar_result

    dek = await svc._get_or_create_dek(session=session, client_id=client_id)

    assert len(dek) == 32
    from app.models.clients import DeClientKeys
    added = session.add.call_args[0][0]
    assert isinstance(added, DeClientKeys)
    assert added.client_id == client_id
    assert added.key_version == 1
    assert added.is_active is True


@pytest.mark.asyncio
async def test_get_or_create_dek_returns_existing_dek(
    svc: PiiService, enc: EnvelopeEncryption
) -> None:
    client_id = "client-existdek-001"
    session = make_mock_session()

    real_dek, encrypted_dek_bytes = enc.generate_dek()
    key_row = MagicMock()
    key_row.encrypted_dek = encrypted_dek_bytes.decode("ascii")

    scalar_result = MagicMock()
    scalar_result.scalar_one_or_none.return_value = key_row
    session.execute.return_value = scalar_result

    dek = await svc._get_or_create_dek(session=session, client_id=client_id)

    assert dek == real_dek
    # No new key should have been added
    assert not session.add.called
