"""Embed de_qual_documents + de_qual_extracts rows with local bge-small.

Idempotent backfill script. Reads rows where embedding IS NULL, embeds
them in batches, UPDATEs the row. Safe to re-run — it only touches rows
that still lack an embedding.

Usage:
    python -m scripts.ingest.embed_qual_content
    python -m scripts.ingest.embed_qual_content --batch-size 64 --max-rows 1000
    python -m scripts.ingest.embed_qual_content --table de_qual_extracts

Run as part of nightly compute (wired via pipeline_trigger.py) or manually.
Takes ~1 minute for 500 rows on a t3.large.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

_REPO_ROOT = Path(__file__).parent.parent.parent


def _load_env() -> None:
    env_path = _REPO_ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip('"').strip("'")
        if key not in os.environ:
            os.environ[key] = val


_load_env()
sys.path.insert(0, str(_REPO_ROOT))

import psycopg2  # noqa: E402
import psycopg2.extras  # noqa: E402

from app.pipelines.qualitative.local_embedder import (  # noqa: E402
    embed_texts,
    to_pgvector_literal,
)


def _ts() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _log(msg: str) -> None:
    print(f"[{_ts()}] {msg}", flush=True)


def _db_url() -> str:
    url = os.environ.get("DATABASE_URL_SYNC") or os.environ.get("DATABASE_URL", "")
    for prefix in ("postgresql+asyncpg://", "postgresql+psycopg2://"):
        if url.startswith(prefix):
            url = url.replace(prefix, "postgresql://", 1)
    return url


# Per-table config: (SELECT fields, text-building lambda, UPDATE template).
# Why two different text builders: de_qual_documents has raw_text (PDF body),
# de_qual_extracts has view_text+source_quote (an extracted opinion). Both
# are concatenated with their title/context so the embedding captures the
# identifying metadata along with the content.
_TABLES: dict[str, dict] = {
    "de_qual_documents": {
        "select": "SELECT id::text, title, raw_text FROM de_qual_documents "
                  "WHERE embedding IS NULL AND raw_text IS NOT NULL "
                  "AND LENGTH(raw_text) > 50 "
                  "ORDER BY created_at DESC",
        "build_text": lambda row: f"{row['title'] or ''}\n\n{(row['raw_text'] or '')[:3000]}",
        "update": "UPDATE de_qual_documents SET embedding = %s::vector WHERE id = %s::uuid",
    },
    "de_qual_extracts": {
        "select": "SELECT id::text, entity_ref, direction, conviction, view_text, source_quote "
                  "FROM de_qual_extracts "
                  "WHERE embedding IS NULL AND view_text IS NOT NULL "
                  "ORDER BY created_at DESC",
        "build_text": lambda row: " | ".join(filter(None, [
            row.get("entity_ref") or "",
            row.get("direction") or "",
            row.get("conviction") or "",
            row.get("view_text") or "",
            (row.get("source_quote") or "")[:500],
        ])),
        "update": "UPDATE de_qual_extracts SET embedding = %s::vector WHERE id = %s::uuid",
    },
}


def embed_table(cur, conn, table: str, batch_size: int, max_rows: int) -> int:
    cfg = _TABLES[table]
    cur.execute(cfg["select"] + f" LIMIT {max_rows}")
    rows = cur.fetchall()
    _log(f"{table}: found {len(rows)} rows needing embedding")
    if not rows:
        return 0

    embedded = 0
    t0 = time.time()
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        texts = [cfg["build_text"](r) for r in batch]
        vectors = embed_texts(texts)
        for row, vec in zip(batch, vectors):
            literal = to_pgvector_literal(vec)
            cur.execute(cfg["update"], (literal, row["id"]))
            embedded += 1
        conn.commit()
        elapsed = time.time() - t0
        rate = embedded / elapsed if elapsed > 0 else 0
        _log(f"  {table}: {embedded}/{len(rows)} ({rate:.1f} rows/s)")

    return embedded


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill embeddings for qualitative content")
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--max-rows", type=int, default=10_000)
    parser.add_argument(
        "--table",
        choices=list(_TABLES.keys()) + ["all"],
        default="all",
    )
    args = parser.parse_args()

    conn = psycopg2.connect(_db_url())
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    tables = list(_TABLES.keys()) if args.table == "all" else [args.table]
    total = 0
    for t in tables:
        total += embed_table(cur, conn, t, args.batch_size, args.max_rows)

    _log(f"=== DONE: {total} rows embedded ===")
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
