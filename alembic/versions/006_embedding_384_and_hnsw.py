"""Resize embedding columns from vector(1536) to vector(384) + HNSW indexes.

Revision ID: 006_embedding_384
Revises: 005_cron_run
Create Date: 2026-04-13

Why:
- Columns were originally sized for OpenAI text-embedding-3-small (1536 dim),
  but OPENAI_API_KEY is a placeholder on this deployment and the user wants
  free open-source models.
- BAAI/bge-small-en-v1.5 is top-of-MTEB for small models at 384 dim, runs
  locally via fastembed (ONNX) — no external API, no rate limits, no cost.
- At time of migration both columns are empty (0 rows with embeddings), so
  the ALTER is safe — no data loss.

HNSW parameters:
- m=16, ef_construction=64 (pgvector defaults; good balance of build time
  and recall for sub-1M-row tables)
- vector_cosine_ops — matches the similarity metric used by bge-small and
  the app-side search endpoint.
"""

from alembic import op
import sqlalchemy as sa

revision = "006_embedding_384"
down_revision = "005_cron_run"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Drop any existing indexes on the embedding columns (none exist today,
    # but be defensive so this is idempotent under future re-runs).
    op.execute("DROP INDEX IF EXISTS ix_de_qual_documents_embedding")
    op.execute("DROP INDEX IF EXISTS ix_de_qual_extracts_embedding")

    # Resize columns. USING NULL::vector(384) because an empty cast is not
    # valid — pgvector requires an explicit NULL-typed value.
    op.execute(
        "ALTER TABLE de_qual_documents "
        "ALTER COLUMN embedding TYPE vector(384) USING NULL::vector(384)"
    )
    op.execute(
        "ALTER TABLE de_qual_extracts "
        "ALTER COLUMN embedding TYPE vector(384) USING NULL::vector(384)"
    )

    # HNSW indexes for fast cosine-similarity search.
    op.execute(
        "CREATE INDEX ix_de_qual_documents_embedding "
        "ON de_qual_documents USING hnsw (embedding vector_cosine_ops) "
        "WITH (m = 16, ef_construction = 64)"
    )
    op.execute(
        "CREATE INDEX ix_de_qual_extracts_embedding "
        "ON de_qual_extracts USING hnsw (embedding vector_cosine_ops) "
        "WITH (m = 16, ef_construction = 64)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_de_qual_documents_embedding")
    op.execute("DROP INDEX IF EXISTS ix_de_qual_extracts_embedding")
    op.execute(
        "ALTER TABLE de_qual_documents "
        "ALTER COLUMN embedding TYPE vector(1536) USING NULL::vector(1536)"
    )
    op.execute(
        "ALTER TABLE de_qual_extracts "
        "ALTER COLUMN embedding TYPE vector(1536) USING NULL::vector(1536)"
    )
