"""Tests for HmacBlindIndex — compute, search, normalization."""

from __future__ import annotations

import os

import pytest

from app.security.hmac_index import HmacBlindIndex


@pytest.fixture
def hmac_key() -> bytes:
    return os.urandom(32)


@pytest.fixture
def hi(hmac_key: bytes) -> HmacBlindIndex:
    return HmacBlindIndex(hmac_key=hmac_key)


# ---------------------------------------------------------------------------
# compute
# ---------------------------------------------------------------------------


def test_compute_returns_8_char_string(hi: HmacBlindIndex) -> None:
    result = hi.compute("test@example.com")
    assert isinstance(result, str)
    assert len(result) == 8


def test_compute_returns_hex_string(hi: HmacBlindIndex) -> None:
    result = hi.compute("ABCDE1234F")
    assert all(c in "0123456789abcdef" for c in result)


def test_compute_is_deterministic(hi: HmacBlindIndex) -> None:
    value = "foo@bar.com"
    assert hi.compute(value) == hi.compute(value)


def test_compute_different_inputs_produce_different_hashes(hi: HmacBlindIndex) -> None:
    h1 = hi.compute("test@example.com")
    h2 = hi.compute("other@example.com")
    assert h1 != h2


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------


def test_compute_case_insensitive(hi: HmacBlindIndex) -> None:
    """Uppercase and lowercase input must produce the same HMAC."""
    assert hi.compute("FOO@BAR.COM") == hi.compute("foo@bar.com")


def test_compute_strips_whitespace(hi: HmacBlindIndex) -> None:
    """Leading/trailing whitespace must be stripped before hashing."""
    assert hi.compute("  test@example.com  ") == hi.compute("test@example.com")


def test_compute_strips_and_lowercases_combined(hi: HmacBlindIndex) -> None:
    assert hi.compute("  ABCDE1234F  ") == hi.compute("abcde1234f")


# ---------------------------------------------------------------------------
# search is alias for compute
# ---------------------------------------------------------------------------


def test_search_is_alias_for_compute(hi: HmacBlindIndex) -> None:
    value = "rahul@example.com"
    assert hi.search(value) == hi.compute(value)


# ---------------------------------------------------------------------------
# Key derivation
# ---------------------------------------------------------------------------


def test_derive_local_key_is_deterministic() -> None:
    key1 = HmacBlindIndex._derive_local_key()
    key2 = HmacBlindIndex._derive_local_key()
    assert key1 == key2
    assert len(key1) == 32


def test_different_hmac_keys_produce_different_results() -> None:
    hi1 = HmacBlindIndex(hmac_key=os.urandom(32))
    hi2 = HmacBlindIndex(hmac_key=os.urandom(32))
    value = "test@example.com"
    # Overwhelmingly likely to differ
    assert hi1.compute(value) != hi2.compute(value)
