"""Run Gemini extraction on all Goldilocks documents with raw_text.

Standalone script — psycopg2 + httpx only, no SQLAlchemy needed.

Usage:
    python3 scripts/ingest/run_goldilocks_extraction.py
    python3 scripts/ingest/run_goldilocks_extraction.py --max-docs 10
    python3 scripts/ingest/run_goldilocks_extraction.py --dry-run
    python3 scripts/ingest/run_goldilocks_extraction.py --report-type trend_friend
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

# ---------------------------------------------------------------------------
# Bootstrap .env
# ---------------------------------------------------------------------------
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
        key, val = key.strip(), val.strip().strip('"').strip("'")
        if key not in os.environ:
            os.environ[key] = val


_load_env()

import httpx
import psycopg2
import psycopg2.extras

# ---------------------------------------------------------------------------
# LLM configuration. Default provider is OpenRouter (free open-source models),
# falling back to Gemini if OPENROUTER_API_KEY is not set.
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")
OLLAMA_MODEL = "qwen2.5:3b"
OLLAMA_URL = "http://127.0.0.1:11434/api/generate"
MAX_TEXT = 80_000
USE_OLLAMA = os.environ.get("GOLDILOCKS_USE_OLLAMA", "0") == "1"

# OpenRouter — chain of free open-source models. The script tries each in order
# and falls through on upstream rate limits / 5xx. Override via env if needed
# (comma-separated): OPENROUTER_MODELS="model1,model2,model3"
_DEFAULT_OPENROUTER_MODELS = [
    "openai/gpt-oss-120b:free",                # 120B GPT OSS, 131k ctx
    "nvidia/nemotron-3-nano-30b-a3b:free",     # 30B Nemotron MoE, 256k ctx
    "google/gemma-3-12b-it:free",              # Gemma 3 12B, 131k ctx
    "nvidia/nemotron-nano-9b-v2:free",         # Nemotron Nano 9B, 128k ctx
    "google/gemma-3-4b-it:free",               # Gemma 3 4B fallback, 32k ctx
]
OPENROUTER_MODELS = [
    m.strip() for m in os.environ.get(
        "OPENROUTER_MODELS", ",".join(_DEFAULT_OPENROUTER_MODELS)
    ).split(",") if m.strip()
]
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"


def ts() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _log(msg: str) -> None:
    print(f"[{ts()}] {msg}", flush=True)


def get_db_conn():
    url = os.environ.get("DATABASE_URL_SYNC") or os.environ.get("DATABASE_URL", "")
    for prefix in ["postgresql+asyncpg://", "postgresql+psycopg2://"]:
        if url.startswith(prefix):
            url = url.replace(prefix, "postgresql://", 1)
    return psycopg2.connect(url)


# ---------------------------------------------------------------------------
# Gemini API call
# ---------------------------------------------------------------------------
def call_ollama(prompt: str, retries: int = 2) -> Optional[dict]:
    """Call local Ollama for JSON extraction. No rate limits."""
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
        "format": "json",
        "options": {"temperature": 0.1, "num_predict": 2048},
    }
    for attempt in range(retries):
        try:
            resp = httpx.post(OLLAMA_URL, json=payload, timeout=120.0)
            resp.raise_for_status()
            text = resp.json().get("response", "").strip()
            if not text:
                return None
            # Clean markdown wrapping
            for prefix in ["```json", "```"]:
                if text.startswith(prefix):
                    text = text[len(prefix):]
            if text.endswith("```"):
                text = text[:-3]
            return json.loads(text.strip())
        except json.JSONDecodeError:
            _log(f"  Ollama JSON parse error (attempt {attempt+1})")
            continue
        except Exception as exc:
            _log(f"  Ollama error: {exc}")
            if attempt < retries - 1:
                time.sleep(2)
    return None


def call_llm(prompt: str, api_key: str = "") -> Optional[dict]:
    """Pick a provider in priority order:
       1) Ollama (only if explicitly enabled via env)
       2) OpenRouter (free open-source models, primary)
       3) Gemini (legacy fallback if OpenRouter key absent)
    Raises GeminiTransientError on exhausted retries so the doc stays pending.
    """
    if USE_OLLAMA:
        return call_ollama(prompt)
    or_key = os.environ.get("OPENROUTER_API_KEY", "").strip()
    if or_key:
        return call_openrouter(prompt, or_key)
    return call_gemini(prompt, api_key)


class GeminiTransientError(Exception):
    """Raised when *all* upstream LLM providers return transient errors (5xx,
    429, network) after exhausting retries. Caller should leave the doc as
    'pending' so a future run retries instead of marking it 'failed'.

    Named GeminiTransientError for backwards compat with existing handlers,
    but it covers OpenRouter / Ollama / Gemini equally now."""


def _strip_json_fences(text: Optional[str]) -> str:
    """Strip ```json fences and surrounding whitespace from an LLM JSON reply.

    Returns an empty string if the input is None / empty — callers interpret
    that as a transient parse miss and let the retry chain handle it.
    """
    if not text:
        return ""
    text = text.strip()
    for prefix in ("```json", "```JSON", "```"):
        if text.startswith(prefix):
            text = text[len(prefix):]
            break
    if text.endswith("```"):
        text = text[:-3]
    return text.strip()


def call_openrouter(prompt: str, api_key: str) -> Optional[dict]:
    """Call OpenRouter with a fallback chain of free open-source models.

    Tries each model in OPENROUTER_MODELS in order. On 429 / 5xx / network
    error, immediately falls through to the next model (no per-model retry —
    the next model in the chain is the retry). After exhausting the whole
    chain without a success, raises GeminiTransientError.

    Returns the parsed JSON dict on success, or None if the call succeeded
    but the response wasn't valid JSON (treated as a permanent parse error
    by the caller, doc gets marked 'failed').
    """
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://data.jslwealth.in",
        "X-Title": "JIP Data Engine",
    }

    failures: list[str] = []
    for model in OPENROUTER_MODELS:
        payload = {
            "model": model,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are a financial data extractor. Reply with ONLY a "
                        "single valid JSON object, no prose, no markdown fences."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.1,
            # Some providers honour json_object response format; others ignore
            # it harmlessly. Worth requesting.
            "response_format": {"type": "json_object"},
        }
        try:
            resp = httpx.post(OPENROUTER_URL, headers=headers, json=payload, timeout=120.0)
        except (httpx.TimeoutException, httpx.ConnectError) as exc:
            failures.append(f"{model}: network ({exc.__class__.__name__})")
            continue

        if resp.status_code == 200:
            try:
                body = resp.json()
                # OpenRouter sometimes returns {"error": ...} inside a 200 response
                if "error" in body and "choices" not in body:
                    failures.append(f"{model}: 200 with error: {body['error'].get('message','?')[:80]}")
                    continue
                content = body["choices"][0]["message"]["content"]
            except (KeyError, IndexError, ValueError) as exc:
                failures.append(f"{model}: malformed response ({exc})")
                continue

            cleaned = _strip_json_fences(content)
            if not cleaned:
                # Model returned an empty / None content — treat as transient,
                # try the next model in the chain.
                failures.append(f"{model}: empty content")
                continue
            try:
                parsed = json.loads(cleaned)
                _log(f"  OpenRouter OK via {model}")
                return parsed
            except json.JSONDecodeError as exc:
                _log(f"  {model} JSON parse error: {str(exc)[:60]} | preview: {cleaned[:120]}")
                # Try next model — maybe it'll produce cleaner JSON
                failures.append(f"{model}: parse error")
                continue

        if resp.status_code == 429 or 500 <= resp.status_code < 600:
            # Transient — try next model immediately, no sleep (different upstream)
            try:
                err_msg = resp.json().get("error", {}).get("message", "?")[:80]
            except Exception:
                err_msg = "?"
            failures.append(f"{model}: {resp.status_code} {err_msg}")
            continue

        # Other 4xx — usually a model-specific issue (model retired, bad params).
        # Don't burn the whole chain; just skip this model.
        failures.append(f"{model}: HTTP {resp.status_code}")
        continue

    # Whole chain exhausted
    _log(f"  OpenRouter chain exhausted: {' | '.join(failures[:3])}")
    raise GeminiTransientError(f"openrouter chain exhausted ({len(failures)} models tried)")


def call_gemini(prompt: str, api_key: str, retries: int = 5) -> Optional[dict]:
    """Call Gemini and return parsed JSON dict.

    Retries on 429 (rate limit) and 5xx (Google-side transient). After all
    retries fail, raises GeminiTransientError so the caller can leave the
    document in 'pending' state for the next run.
    """
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "responseMimeType": "application/json",
            "temperature": 0.1,
        },
    }
    resp = None
    last_status = None
    for attempt in range(retries):
        try:
            resp = httpx.post(url, params={"key": api_key}, json=payload, timeout=90.0)
        except (httpx.TimeoutException, httpx.ConnectError) as exc:
            last_status = "network"
            wait = min(120, (attempt + 1) * 15)
            _log(f"  Network error ({exc.__class__.__name__}), waiting {wait}s...")
            time.sleep(wait)
            continue

        last_status = resp.status_code
        if resp.status_code == 429:
            wait = min(120, (attempt + 1) * 30)  # 30s, 60s, 90s, 120s, 120s
            _log(f"  Rate limited, waiting {wait}s...")
            time.sleep(wait)
            continue
        if 500 <= resp.status_code < 600:
            wait = min(120, (attempt + 1) * 20)  # 20s, 40s, 60s, 80s, 100s
            _log(f"  Gemini {resp.status_code}, waiting {wait}s...")
            time.sleep(wait)
            continue
        # Any other non-2xx is a permanent client error — let it raise.
        resp.raise_for_status()
        break

    if resp is None or resp.status_code != 200:
        _log(f"  All {retries} retries exhausted (last={last_status})")
        raise GeminiTransientError(f"transient Gemini failure after {retries} retries (last={last_status})")
    result = resp.json()

    candidates = result.get("candidates", [])
    if not candidates:
        return None

    text = candidates[0].get("content", {}).get("parts", [{}])[0].get("text", "")
    text = text.strip()
    for prefix in ["```json", "```"]:
        if text.startswith(prefix):
            text = text[len(prefix):]
    if text.endswith("```"):
        text = text[:-3]
    text = text.strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        _log(f"  JSON parse error: {text[:200]}")
        return None


def _dec(v: Any) -> Optional[str]:
    if v is None:
        return None
    return str(Decimal(str(v)))


def _trunc(v: Any, n: int) -> Optional[str]:
    """Defensive: clip string values to fit narrow VARCHAR columns. LLMs
    occasionally output longer phrases ('moderately positive') even when
    asked for short enums, and an INSERT failure here means the whole
    document is marked failed."""
    if v is None:
        return None
    s = str(v).strip()
    return s[:n] if s else None


# ---------------------------------------------------------------------------
# Extraction prompts
# ---------------------------------------------------------------------------
TREND_FRIEND_PROMPT = """You are a financial data extractor. Extract from this Goldilocks Trend Friend report:

Return JSON with:
- report_date (YYYY-MM-DD)
- nifty_close, nifty_support_1, nifty_support_2, nifty_resistance_1, nifty_resistance_2 (numbers)
- bank_nifty_close, bank_nifty_support_1, bank_nifty_support_2, bank_nifty_resistance_1, bank_nifty_resistance_2
- trend_direction: "upward" or "downward" or "sideways"
- trend_strength: 1-5 integer (infer from arrows/description if not explicit)
- global_impact: "positive" or "negative" or "neutral"
- headline: one-line market summary
- overall_view: full narrative paragraph
- sectors: array of {sector, trend, outlook, rank}

Copy EXACT numbers from text. Return null for missing values.

TEXT:
"""

STOCK_IDEA_PROMPT = """You are a financial data extractor. Extract from this Goldilocks stock recommendation:

Return JSON with:
- published_date (YYYY-MM-DD)
- symbol (NSE symbol, e.g. LLOYDSME, CHENNPETRO)
- company_name
- idea_type: "stock_bullet" or "big_catch"
- entry_price (number or null), entry_zone_low, entry_zone_high
- target_1, target_2, lt_target (long-term target, null if not given)
- stop_loss (number)
- timeframe (e.g. "2-6 Weeks")
- rationale (key reasoning, 1-2 sentences)
- technical_params: {ema_200, rsi_14, support_1, support_2, resistance_1, resistance_2}

Copy EXACT numbers. Return null for missing.

TEXT:
"""

SECTOR_VIEWS_PROMPT = """You are a financial data extractor. Extract sector analysis from this report:

Return JSON with:
- report_date (YYYY-MM-DD)
- sectors: array of objects, each with:
  - sector (name)
  - trend ("outperforming" or "underperforming" or "neutral")
  - outlook (1-2 sentence description)
  - rank (integer if given, null otherwise)
  - top_picks: array of {symbol, resistance_levels: [number, number]}

TEXT:
"""

GENERAL_VIEWS_PROMPT = """Extract investment views from this financial text.

Return JSON with a "views" array where each view has:
- asset_class: "equity"|"mf"|"bond"|"commodity"|"currency"|"macro"
- entity_ref: specific security/sector/entity name
- direction: "bullish"|"bearish"|"neutral"|"cautious"
- timeframe: e.g. "1-3 months", "long-term"
- conviction: "low"|"medium"|"high"|"very_high"
- view_text: structured summary of the thesis
- quality_score: 0.0-1.0 confidence in this extraction

TEXT:
"""


# ---------------------------------------------------------------------------
# DB insert helpers
# ---------------------------------------------------------------------------
def upsert_market_view(cur, data: dict) -> bool:
    rd = data.get("report_date")
    if not rd:
        return False
    cur.execute("""
        INSERT INTO de_goldilocks_market_view (
            report_date, nifty_close, nifty_support_1, nifty_support_2,
            nifty_resistance_1, nifty_resistance_2,
            bank_nifty_close, bank_nifty_support_1, bank_nifty_support_2,
            bank_nifty_resistance_1, bank_nifty_resistance_2,
            trend_direction, trend_strength, global_impact, headline, overall_view
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (report_date) DO UPDATE SET
            nifty_close=EXCLUDED.nifty_close, nifty_support_1=EXCLUDED.nifty_support_1,
            nifty_support_2=EXCLUDED.nifty_support_2,
            nifty_resistance_1=EXCLUDED.nifty_resistance_1, nifty_resistance_2=EXCLUDED.nifty_resistance_2,
            bank_nifty_close=EXCLUDED.bank_nifty_close,
            bank_nifty_support_1=EXCLUDED.bank_nifty_support_1, bank_nifty_support_2=EXCLUDED.bank_nifty_support_2,
            bank_nifty_resistance_1=EXCLUDED.bank_nifty_resistance_1, bank_nifty_resistance_2=EXCLUDED.bank_nifty_resistance_2,
            trend_direction=EXCLUDED.trend_direction, trend_strength=EXCLUDED.trend_strength,
            global_impact=EXCLUDED.global_impact, headline=EXCLUDED.headline, overall_view=EXCLUDED.overall_view,
            updated_at=NOW()
    """, (
        rd, _dec(data.get("nifty_close")), _dec(data.get("nifty_support_1")), _dec(data.get("nifty_support_2")),
        _dec(data.get("nifty_resistance_1")), _dec(data.get("nifty_resistance_2")),
        _dec(data.get("bank_nifty_close")), _dec(data.get("bank_nifty_support_1")), _dec(data.get("bank_nifty_support_2")),
        _dec(data.get("bank_nifty_resistance_1")), _dec(data.get("bank_nifty_resistance_2")),
        _trunc(data.get("trend_direction"), 20), data.get("trend_strength"),
        _trunc(data.get("global_impact"), 20), data.get("headline"), data.get("overall_view"),
    ))
    # Upsert sectors
    for s in data.get("sectors", []):
        if not s.get("sector"):
            continue
        cur.execute("""
            INSERT INTO de_goldilocks_sector_view (report_date, sector, trend, outlook, rank)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (report_date, sector) DO UPDATE SET
                trend=EXCLUDED.trend, outlook=EXCLUDED.outlook, rank=EXCLUDED.rank, updated_at=NOW()
        """, (rd, _trunc(s["sector"], 100), _trunc(s.get("trend"), 20), s.get("outlook"), s.get("rank")))
    return True


def insert_stock_idea(cur, data: dict, doc_id: str) -> bool:
    if not data.get("symbol"):
        return False
    # Check idempotency
    cur.execute("SELECT 1 FROM de_goldilocks_stock_ideas WHERE document_id = %s LIMIT 1", (doc_id,))
    if cur.fetchone():
        return True
    tp = data.get("technical_params")
    cur.execute("""
        INSERT INTO de_goldilocks_stock_ideas (
            id, document_id, published_date, symbol, company_name, idea_type,
            entry_price, entry_zone_low, entry_zone_high,
            target_1, target_2, lt_target, stop_loss,
            timeframe, rationale, technical_params, status
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'active')
    """, (
        str(uuid.uuid4()), doc_id, data.get("published_date"),
        _trunc(data.get("symbol"), 20), _trunc(data.get("company_name"), 200),
        _trunc(data.get("idea_type"), 20),
        _dec(data.get("entry_price")), _dec(data.get("entry_zone_low")), _dec(data.get("entry_zone_high")),
        _dec(data.get("target_1")), _dec(data.get("target_2")), _dec(data.get("lt_target")),
        _dec(data.get("stop_loss")), _trunc(data.get("timeframe"), 50), data.get("rationale"),
        json.dumps(tp) if tp else None,
    ))
    return True


def upsert_sector_views(cur, data: dict) -> bool:
    rd = data.get("report_date")
    if not rd:
        return False
    for s in data.get("sectors", []):
        if not s.get("sector"):
            continue
        tp = s.get("top_picks")
        cur.execute("""
            INSERT INTO de_goldilocks_sector_view (report_date, sector, trend, outlook, rank, top_picks)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT (report_date, sector) DO UPDATE SET
                trend=EXCLUDED.trend, outlook=EXCLUDED.outlook, rank=EXCLUDED.rank,
                top_picks=EXCLUDED.top_picks, updated_at=NOW()
        """, (rd, _trunc(s["sector"], 100), _trunc(s.get("trend"), 20),
              s.get("outlook"), s.get("rank"),
              json.dumps(tp) if tp else None))
    return True


def insert_general_views(cur, data: dict, doc_id: str) -> int:
    views = data.get("views", [])
    inserted = 0
    for v in views:
        qs = v.get("quality_score")
        if qs is not None and float(qs) < 0.5:
            continue
        cur.execute("""
            INSERT INTO de_qual_extracts (
                id, document_id, asset_class, entity_ref, direction, timeframe,
                conviction, view_text, source_quote, quality_score
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            str(uuid.uuid4()), doc_id,
            _trunc(v.get("asset_class"), 20), _trunc(v.get("entity_ref"), 100),
            _trunc(v.get("direction"), 20), _trunc(v.get("timeframe"), 50),
            _trunc(v.get("conviction"), 20), v.get("view_text"),
            v.get("source_quote"), _dec(v.get("quality_score")),
        ))
        inserted += 1
    return inserted


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-docs", type=int, default=200)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--report-type", type=str, default=None)
    args = parser.parse_args()

    # api_key is only used by the legacy Gemini fallback. Primary path is
    # OpenRouter, which reads OPENROUTER_API_KEY directly inside call_openrouter.
    api_key = os.environ.get("GOOGLE_API_KEY", "")
    if not api_key and not os.environ.get("OPENROUTER_API_KEY") and not USE_OLLAMA:
        _log("[ERROR] No LLM provider configured: set OPENROUTER_API_KEY or GOOGLE_API_KEY")
        sys.exit(1)

    conn = get_db_conn()
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Fetch pending docs
    where_extra = ""
    params = []
    if args.report_type:
        where_extra = "AND d.report_type = %s"
        params.append(args.report_type)

    cur.execute(f"""
        SELECT d.id::text, d.title, d.report_type, d.raw_text, d.processing_status
        FROM de_qual_documents d
        WHERE d.source_id IN (SELECT id FROM de_qual_sources WHERE source_name ILIKE '%%goldilocks%%')
          AND d.raw_text IS NOT NULL AND LENGTH(d.raw_text) > 200
          AND d.processing_status = 'pending'
          {where_extra}
        ORDER BY d.created_at
        LIMIT %s
    """, params + [args.max_docs])

    docs = cur.fetchall()
    _log(f"Found {len(docs)} documents to process (max={args.max_docs})")

    processed = 0
    failed = 0
    by_type = {}

    for doc in docs:
        doc_id = doc["id"]
        title = doc["title"] or ""
        rtype = doc["report_type"] or ""
        raw = doc["raw_text"][:MAX_TEXT]

        _log(f"Processing: [{rtype}] {title[:60]}")

        if args.dry_run:
            _log("  [DRY-RUN] Would extract")
            processed += 1
            continue

        try:
            # Choose extraction based on report_type
            if rtype == "trend_friend":
                data = call_llm(TREND_FRIEND_PROMPT + raw, api_key)
                if data:
                    upsert_market_view(cur, data)
            elif rtype in ("stock_bullet", "big_catch"):
                data = call_llm(STOCK_IDEA_PROMPT + raw, api_key)
                if data:
                    insert_stock_idea(cur, data, doc_id)
            elif rtype in ("sector_trends", "fortnightly"):
                data = call_llm(SECTOR_VIEWS_PROMPT + raw, api_key)
                if data:
                    upsert_sector_views(cur, data)
            else:
                data = None

            # Only extract general views if specific extraction succeeded
            if data:
                gen_data = call_llm(GENERAL_VIEWS_PROMPT + raw, api_key)
                if gen_data:
                    n = insert_general_views(cur, gen_data, doc_id)
                    _log(f"  General views: {n} extracted")

                cur.execute(
                    "UPDATE de_qual_documents SET processing_status='done', updated_at=NOW() WHERE id=%s::uuid",
                    (doc_id,),
                )
                conn.commit()
                processed += 1
                by_type[rtype] = by_type.get(rtype, 0) + 1
                _log(f"  OK: {rtype}")
            else:
                _log("  SKIP: no extraction handler for report_type or empty response")
                # Don't mark as failed — retry next run

        except GeminiTransientError as exc:
            # Transient Google-side problem — keep doc pending so the next
            # cron run picks it up. Don't increment `failed`.
            conn.rollback()
            _log(f"  TRANSIENT: {exc} — leaving doc pending")
        except (AttributeError, TypeError, KeyError) as exc:
            # Likely a shape-of-response issue (None content, missing field)
            # which tends to be transient across LLM runs. Don't mark failed.
            conn.rollback()
            _log(f"  SHAPE: {exc} — leaving doc pending")
        except Exception as exc:
            conn.rollback()
            failed += 1
            _log(f"  FAIL: {exc}")
            cur.execute(
                "UPDATE de_qual_documents SET processing_status='failed', processing_error=%s, updated_at=NOW() WHERE id=%s::uuid",
                (str(exc)[:500], doc_id),
            )
            conn.commit()

        time.sleep(1 if USE_OLLAMA else 10)  # Ollama is local, no rate limit

    _log(f"=== DONE: processed={processed} failed={failed} by_type={by_type} ===")
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
