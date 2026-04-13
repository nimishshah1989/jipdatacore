"""Local, free embedding provider for qualitative content.

Uses BAAI/bge-small-en-v1.5 (384 dim) via fastembed — an ONNX-based embedder
library from qdrant that doesn't require torch or a GPU. Runs in ~50ms per
text on a t3.large.

This module is sync-only and deliberately isolates the model load (slow,
~130MB download on first use) behind a lazy singleton, so importing it
anywhere is cheap.

Usage:
    from app.pipelines.qualitative.local_embedder import embed_texts
    vectors = embed_texts(["some text", "another text"])  # list[list[float]]
    vec = embed_texts(["single text"])[0]

Design choices:
- Singleton model load — fastembed holds session state and the ONNX model
  weights (~130MB resident); reloading per call would be catastrophic.
- Batch-oriented API — fastembed internally batches but the caller should
  still hand it multiple texts at once when possible for throughput.
- Returns plain Python lists so callers don't need numpy. pgvector accepts
  the text serialization '[0.1,0.2,...]' equivalently.
- Normalises dims by the caller via vector_cosine_ops; no need to L2-norm
  ourselves (bge outputs are already unit-length).
"""

from __future__ import annotations

import logging
from functools import lru_cache
from typing import Iterable

logger = logging.getLogger(__name__)

EMBEDDING_MODEL = "BAAI/bge-small-en-v1.5"
EMBEDDING_DIM = 384
# bge-small has 512-token window — truncate longer inputs on our side so
# the tokenizer doesn't silently drop content.
MAX_CHARS = 2000  # conservative: ~500 tokens at 4 chars/token average


@lru_cache(maxsize=1)
def _get_model():
    """Lazy-load the fastembed model. First call downloads ~130MB of ONNX
    weights into ~/.cache/fastembed and takes 10-30s. Subsequent calls
    (same process) are instant."""
    try:
        from fastembed import TextEmbedding
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "fastembed not installed. Run: pip install fastembed"
        ) from exc

    logger.info("local_embedder_model_loading", model=EMBEDDING_MODEL)
    model = TextEmbedding(model_name=EMBEDDING_MODEL)
    logger.info("local_embedder_model_loaded", dim=EMBEDDING_DIM)
    return model


def embed_texts(texts: Iterable[str]) -> list[list[float]]:
    """Return a list of 384-dim embeddings, one per input text.

    Empty / None inputs are coerced to " " so the model still returns a
    vector (shape consistency matters for callers that zip with input IDs).
    Long inputs are truncated at MAX_CHARS.
    """
    cleaned = [(t or " ")[:MAX_CHARS] for t in texts]
    if not cleaned:
        return []

    model = _get_model()
    # fastembed returns a generator of numpy arrays
    out = [vec.tolist() for vec in model.embed(cleaned)]
    return out


def to_pgvector_literal(vec: list[float]) -> str:
    """Serialize a vector as the pgvector text literal format: '[0.1,0.2,...]'.

    psycopg2 binds this as a text parameter; cast to ::vector(384) in the SQL.
    """
    return "[" + ",".join(f"{x:.6f}" for x in vec) + "]"
