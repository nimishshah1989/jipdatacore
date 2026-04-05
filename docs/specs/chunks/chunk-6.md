# Chunk 6: PII Encryption

**Layer:** 2
**Dependencies:** C2
**Complexity:** Medium
**Status:** done

## Files

- `app/security/__init__.py`
- `app/security/encryption.py`
- `app/security/hmac_index.py`
- `app/security/kms.py`
- `tests/security/test_encryption.py`
- `tests/security/test_hmac_index.py`

## Acceptance Criteria

- [ ] **Envelope encryption (encrypt):** Generate 256-bit DEK via `os.urandom(32)` → encrypt DEK with KMS CMK via `boto3.client('kms').encrypt()` → store encrypted DEK in `de_client_keys` → generate random 12-byte nonce → encrypt field with AES-256-GCM → store as `base64(nonce || ciphertext || tag)` in single TEXT column
- [ ] **Envelope encryption (decrypt):** Fetch encrypted DEK from `de_client_keys` (active key only) → decrypt DEK via `boto3.client('kms').decrypt()` → extract nonce (first 12 bytes) + ciphertext + tag → decrypt with AES-256-GCM → return plaintext
- [ ] **HMAC blind index:** Compute `HMAC-SHA256(key=PII_HMAC_KEY, message=normalised_field_value)` → truncate digest to first 8 hex characters → store in `pan_hash` / `email_hash` / `phone_hash`
- [ ] **Normalisation rules:** PAN: uppercase + strip whitespace. Email: lowercase + strip. Phone: digits only, strip all non-numeric.
- [ ] **Bucket search (v1.9.1):** Query `WHERE pan_hash = :truncated_hash` → returns 2-3 rows (intentional collisions) → decrypt each row's `pan_enc` in memory → return exact match
- [ ] **Key rotation (v1.9 — append-only):** Generate new DEK → INSERT new row in `de_client_keys` with `key_version = max + 1, is_active = TRUE` → SET previous version `is_active = FALSE` → decrypt all PII with old DEK → re-encrypt with new DEK → update `de_clients` encrypted columns. Historical keys retained permanently for backup restoration
- [ ] **HMAC key rotation (v1.8):** Support rotation via `hmac_version` column; during rotation window, search checks both old and new hash until `hmac_version` is uniform; process in batches of 100 clients
- [ ] **PII access logging:** Every read of encrypted PII fields (decrypt operation) must log to `de_pii_access_log` (accessed_by from JWT subject, client_id, fields_accessed array, purpose, source_ip)
- [ ] Encryption works without KMS in test environment (use a mock/local key for tests)
- [ ] All PII functions have comprehensive unit tests with exact-match verification
- [ ] No plaintext PII appears in any log, exception message, or structlog output
- [ ] `app/security/kms.py` provides KMS client with retry logic and local override for testing

## Notes

**Envelope encryption rationale:** KMS CMK encrypts the DEK (not the data directly). This allows:
- Key rotation without re-encrypting all data (just re-encrypt the DEK)
- Audit trail via AWS KMS CloudTrail
- KMS CMK can be rotated annually without affecting stored DEKs

**KMS setup (pre-sprint, done manually):**
- CMK alias: `data-engine-pii`, ARN stored in Secrets Manager as `PII_KMS_KEY_ARN`
- Separate HMAC signing key ARN stored as `PII_HMAC_KEY_ARN`

**Why 8-char truncation (v1.9.1):**
- PAN has format `ABCDE1234F` (10 chars, known structure) — very low entropy
- Indian mobile: 10-digit starting with 6-9 — also low entropy
- Full 64-char HMAC on low-entropy data is reversible via offline brute-force if HMAC key is compromised
- 8-char truncation means 2-3 collisions in a 366K row dataset — trivial to resolve in memory

**AES-256-GCM storage format:** `base64(12-byte-nonce || ciphertext || 16-byte-auth-tag)` stored as a single TEXT column. Decode sequence on decrypt: first 12 bytes = nonce, last 16 bytes = auth tag, middle = ciphertext.

**Test isolation:** Unit tests must not call real AWS KMS. Use `unittest.mock.patch` or a local symmetric key for test environment. The `kms.py` module should support `USE_LOCAL_MOCK_KMS=true` env var for testing.

**Regulatory note:** `de_pii_access_log` retention is 7 years (SEBI compliance). Never truncate or archive this table.
