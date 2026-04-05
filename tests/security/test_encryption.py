"""Tests for EnvelopeEncryption — generate_dek, encrypt_field, decrypt_field."""

from __future__ import annotations

import base64
import os

import pytest
from cryptography.exceptions import InvalidTag

from app.security.encryption import EnvelopeEncryption


@pytest.fixture
def master_key() -> bytes:
    """Fixed 32-byte master key for deterministic tests."""
    return os.urandom(32)


@pytest.fixture
def enc(master_key: bytes) -> EnvelopeEncryption:
    return EnvelopeEncryption(master_key=master_key)


# ---------------------------------------------------------------------------
# generate_dek
# ---------------------------------------------------------------------------


def test_generate_dek_returns_32_byte_plaintext_key(enc: EnvelopeEncryption) -> None:
    plaintext_dek, _encrypted_dek = enc.generate_dek()
    assert len(plaintext_dek) == 32


def test_generate_dek_returns_base64_encoded_encrypted_dek(enc: EnvelopeEncryption) -> None:
    _plaintext_dek, encrypted_dek = enc.generate_dek()
    # Should be valid base64
    decoded = base64.b64decode(encrypted_dek)
    # nonce(12) + ciphertext(32) + tag(16) = at least 60 bytes
    assert len(decoded) >= 60


def test_generate_dek_produces_unique_keys_each_call(enc: EnvelopeEncryption) -> None:
    dek1, _ = enc.generate_dek()
    dek2, _ = enc.generate_dek()
    assert dek1 != dek2


def test_generate_dek_encrypted_dek_decrypts_back_to_plaintext(enc: EnvelopeEncryption) -> None:
    plaintext_dek, encrypted_dek = enc.generate_dek()
    recovered = enc.decrypt_dek(encrypted_dek)
    assert recovered == plaintext_dek


# ---------------------------------------------------------------------------
# encrypt_field / decrypt_field round-trip
# ---------------------------------------------------------------------------


def test_encrypt_decrypt_round_trip_produces_original_value(enc: EnvelopeEncryption) -> None:
    plaintext_dek, _ = enc.generate_dek()
    original = "test@example.com"
    ciphertext = enc.encrypt_field(original, plaintext_dek)
    recovered = enc.decrypt_field(ciphertext, plaintext_dek)
    assert recovered == original


def test_encrypt_decrypt_round_trip_unicode(enc: EnvelopeEncryption) -> None:
    plaintext_dek, _ = enc.generate_dek()
    original = "Râhul Shâh"
    ciphertext = enc.encrypt_field(original, plaintext_dek)
    recovered = enc.decrypt_field(ciphertext, plaintext_dek)
    assert recovered == original


def test_encrypt_field_is_not_deterministic(enc: EnvelopeEncryption) -> None:
    """Same plaintext encrypted twice must produce different ciphertexts (random nonce)."""
    plaintext_dek, _ = enc.generate_dek()
    value = "ABCDE1234F"
    ct1 = enc.encrypt_field(value, plaintext_dek)
    ct2 = enc.encrypt_field(value, plaintext_dek)
    assert ct1 != ct2


def test_encrypted_output_is_base64_decodable(enc: EnvelopeEncryption) -> None:
    plaintext_dek, _ = enc.generate_dek()
    ct = enc.encrypt_field("test@example.com", plaintext_dek)
    decoded = base64.b64decode(ct)
    # nonce(12) + at least 1 byte ciphertext + tag(16)
    assert len(decoded) >= 29


def test_decrypt_with_wrong_dek_raises(enc: EnvelopeEncryption) -> None:
    plaintext_dek, _ = enc.generate_dek()
    wrong_dek = os.urandom(32)
    ct = enc.encrypt_field("secret-pan", plaintext_dek)
    with pytest.raises((InvalidTag, Exception)):
        enc.decrypt_field(ct, wrong_dek)


def test_decrypt_with_corrupted_ciphertext_raises(enc: EnvelopeEncryption) -> None:
    plaintext_dek, _ = enc.generate_dek()
    ct = enc.encrypt_field("secret", plaintext_dek)
    # Flip a byte in the middle of the ciphertext
    raw = bytearray(base64.b64decode(ct))
    raw[15] ^= 0xFF
    corrupted = base64.b64encode(bytes(raw)).decode("ascii")
    with pytest.raises(Exception):
        enc.decrypt_field(corrupted, plaintext_dek)


def test_decrypt_too_short_blob_raises(enc: EnvelopeEncryption) -> None:
    plaintext_dek, _ = enc.generate_dek()
    short_blob = base64.b64encode(b"short").decode("ascii")
    with pytest.raises(ValueError, match="too short"):
        enc.decrypt_field(short_blob, plaintext_dek)


# ---------------------------------------------------------------------------
# DEK size validation
# ---------------------------------------------------------------------------


def test_encrypt_field_wrong_dek_size_raises(enc: EnvelopeEncryption) -> None:
    with pytest.raises(ValueError, match="32 bytes"):
        enc.encrypt_field("value", b"short-key")


def test_decrypt_field_wrong_dek_size_raises(enc: EnvelopeEncryption) -> None:
    with pytest.raises(ValueError, match="32 bytes"):
        enc.decrypt_field("dGVzdA==", b"short-key")


# ---------------------------------------------------------------------------
# Master key validation
# ---------------------------------------------------------------------------


def test_init_with_wrong_master_key_size_raises() -> None:
    with pytest.raises(ValueError, match="32 bytes"):
        EnvelopeEncryption(master_key=b"too-short")


# ---------------------------------------------------------------------------
# Local key derivation (no DB required — config driven)
# ---------------------------------------------------------------------------


def test_derive_local_key_is_deterministic() -> None:
    """Same jwt_secret must always produce the same master key."""
    from app.security.encryption import EnvelopeEncryption

    key1 = EnvelopeEncryption._derive_local_key()
    key2 = EnvelopeEncryption._derive_local_key()
    assert key1 == key2
    assert len(key1) == 32
