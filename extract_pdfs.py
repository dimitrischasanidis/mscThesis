#!/usr/bin/env python3
"""
Two-thread PDF text extraction over existing Postgres records.

Pipeline:
  main thread  → download_q → downloader thread → extract_q → extractor thread
                              (download PDFs to disk)          (pdfminer + UPDATE records)

Results stored as:
  - question_pdf_texts / answer_pdf_texts  JSONB: [{url, text}, ...]  (per-PDF)
  - question_text / answer_text            TEXT:  all texts joined    (backward-compat)

Resilience:
  - Download failures (transient): site-down check → wait_for_site → one retry.
  - Download failures (permanent): empty path emitted; extractor writes empty text.
  - Extraction failures: logged to pdf_extraction_errors + Loki; empty text written.
  - Both threads never give up on site-down: extractor drains disk cache while
    downloader is blocked in wait_for_site.

Errors logged to:
  - logs/pdf_extraction.jsonl  →  Promtail → Loki → Grafana
  - pdf_extraction_errors table in Postgres
"""

import io
import json
import queue
import random
import sys
import threading
import time
import traceback as tb
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, Optional

import psycopg2
import psycopg2.extras
import requests
from loguru import logger

from main import (
    BASE,
    REQUEST_JITTER_MAX_S,
    REQUEST_JITTER_MIN_S,
    _pdf_cache_path,
    fetch_pdf_bytes,
    new_session,
)

try:
    from pdfminer.high_level import extract_text as _pdfminer_extract
    _PDFMINER_OK = True
except ImportError:
    _PDFMINER_OK = False

import os

PG_DSN = os.environ.get(
    "PG_DSN",
    "host=localhost port=5433 dbname=parliament user=parliament password=parliament",
)
BATCH_SIZE = 50
SITE_CHECK_INTERVAL_S = 30
DOWNLOAD_Q_MAX = 500
EXTRACT_Q_MAX = 500

LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

# ── Sentinel ──────────────────────────────────────────────────────────────────

_SENTINEL = object()  # signals end-of-stream on both queues

# ── Logging ───────────────────────────────────────────────────────────────────

logger.remove()
logger.add(sys.stderr, level="INFO")
logger.add(
    LOG_DIR / "pdf_extraction.jsonl",
    level="DEBUG",
    rotation="50 MB",
    retention="30 days",
    compression="zip",
    serialize=True,
    enqueue=True,  # thread-safe async sink
)

# ── SQL ───────────────────────────────────────────────────────────────────────

SELECT_SQL = """
SELECT pcm_id, question_pdfs, answer_pdfs
FROM records
WHERE blocked = FALSE
  AND (
        (jsonb_array_length(coalesce(question_pdfs, '[]'::jsonb)) > 0
         AND question_pdf_texts IS NULL)
     OR (jsonb_array_length(coalesce(answer_pdfs, '[]'::jsonb)) > 0
         AND answer_pdf_texts IS NULL)
      )
ORDER BY pcm_id
"""

COUNT_DONE_SQL = """
SELECT COUNT(*) FROM records
WHERE blocked = FALSE
  AND (question_pdf_texts IS NOT NULL OR answer_pdf_texts IS NOT NULL)
"""

UPDATE_SQL = """
UPDATE records
SET question_pdf_texts = %s,
    answer_pdf_texts   = %s,
    question_text      = %s,
    answer_text        = %s
WHERE pcm_id = %s
"""

INSERT_ERROR_SQL = """
INSERT INTO pdf_extraction_errors (pcm_id, url, kind, error_type, error_msg, traceback)
VALUES (%s, %s, %s, %s, %s, %s)
"""


# ── Data types ────────────────────────────────────────────────────────────────

@dataclass
class ExtractItem:
    """Produced by downloader thread, consumed by extractor thread."""
    pcm_id: str
    question: list  # [(url, local_path_or_empty), ...]
    answer: list    # [(url, local_path_or_empty), ...]


# ── Error classifier ──────────────────────────────────────────────────────────

_TRANSIENT_EXC = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.ReadTimeout,
    ConnectionResetError,
    TimeoutError,
)


def classify(exc: Exception) -> Literal["transient", "permanent"]:
    """Classify exception as transient (retry) or permanent (log and skip)."""
    if isinstance(exc, _TRANSIENT_EXC):
        return "transient"
    if isinstance(exc, requests.exceptions.HTTPError):
        code = exc.response.status_code if exc.response is not None else 0
        if code in (429,) or code >= 500:
            return "transient"
        return "permanent"  # 403, 404, 410, etc.
    return "permanent"


# ── Site health ───────────────────────────────────────────────────────────────

def site_is_up(session: requests.Session) -> bool:
    try:
        r = session.get(BASE, timeout=10, allow_redirects=True)
        return r.ok
    except Exception:
        return False


def wait_for_site(session: requests.Session) -> None:
    """Block until parliament site responds. Polls every 30s. Never gives up."""
    attempt = 0
    while True:
        attempt += 1
        logger.warning(
            "Site appears to be down — waiting {}s before retry (attempt {})",
            SITE_CHECK_INTERVAL_S,
            attempt,
        )
        time.sleep(SITE_CHECK_INTERVAL_S)
        if site_is_up(session):
            logger.info("Site is back up after {} poll(s)", attempt)
            return


# ── Error persistence ─────────────────────────────────────────────────────────

def log_error(pg, pcm_id: str, url: str, kind: str, exc: Exception) -> None:
    error_type = type(exc).__name__
    error_msg = str(exc)
    trace = tb.format_exc()

    logger.error(
        "PDF extraction failed — pcm_id={} kind={} url={} error_type={} error_msg={}",
        pcm_id, kind, url, error_type, error_msg,
    )

    try:
        with pg.cursor() as cur:
            cur.execute(INSERT_ERROR_SQL, (pcm_id, url, kind, error_type, error_msg, trace))
        pg.commit()
    except Exception as db_exc:
        logger.warning("Could not write error to DB (pg error): {}", db_exc)
        try:
            pg.rollback()
        except Exception:
            pass


# ── Helpers ───────────────────────────────────────────────────────────────────

def _strip_nul(s: Optional[str]) -> Optional[str]:
    """Remove NUL bytes that Postgres TEXT/JSONB rejects."""
    return s.replace("\x00", "") if s else s


def _fmt_duration(seconds: float) -> str:
    s = int(seconds)
    if s < 60:
        return f"{s}s"
    m, s = divmod(s, 60)
    if m < 60:
        return f"{m}m{s:02d}s"
    h, m = divmod(m, 60)
    if h < 24:
        return f"{h}h{m:02d}m"
    d, h = divmod(h, 24)
    return f"{d}d{h:02d}h"


def _join_texts(entries: list[dict]) -> str:
    """Concatenate all PDF texts for backward-compat columns."""
    parts = []
    for i, e in enumerate(entries):
        if e.get("text"):
            parts.append(f"=== PDF {i + 1} ===\n{e['text']}")
    return "\n\n".join(parts)


def _jitter() -> None:
    time.sleep(random.uniform(REQUEST_JITTER_MIN_S, REQUEST_JITTER_MAX_S))


def _flush_batch(pg, pending: list) -> None:
    """Write a batch of UPDATE_SQL tuples to Postgres. Rolls back on failure."""
    try:
        with pg.cursor() as cur:
            psycopg2.extras.execute_batch(cur, UPDATE_SQL, pending)
        pg.commit()
    except Exception as exc:
        logger.error("Batch commit failed: {}", exc)
        try:
            pg.rollback()
        except Exception:
            pass


# ── Download helpers ──────────────────────────────────────────────────────────

def _download_one(session: requests.Session, pcm_id: str, kind: str, url: str) -> str:
    """
    Download PDF to disk (or use cache). Returns local path as str.
    On transient error: checks site, waits if down, retries once.
    On any failure: logs warning and returns "" (empty path).
    """
    def _attempt() -> str:
        _jitter()
        fetch_pdf_bytes(url, session)  # saves to pdfs/<sha1>.pdf as side-effect
        return str(_pdf_cache_path(url))

    try:
        return _attempt()
    except Exception as exc:
        if classify(exc) == "transient":
            if not site_is_up(session):
                wait_for_site(session)
            # one retry after transient or site recovery
            try:
                return _attempt()
            except Exception as exc2:
                logger.warning(
                    "Download failed after retry: pcm_id={} kind={} url={} err={}",
                    pcm_id, kind, url, exc2,
                )
                return ""
        else:
            logger.warning(
                "Permanent download error: pcm_id={} kind={} url={} err={}",
                pcm_id, kind, url, exc,
            )
            return ""


# ── Extraction helpers ────────────────────────────────────────────────────────

def _extract_text_from_path(path: str) -> str:
    """Run pdfminer on a local file. Raises on failure."""
    if not _PDFMINER_OK:
        raise RuntimeError("pdfminer.six not installed")
    with open(path, "rb") as fh:
        text = _pdfminer_extract(fh)
    return (text or "").strip()


# ── Worker threads ────────────────────────────────────────────────────────────

def downloader_worker(download_q: queue.Queue, extract_q: queue.Queue) -> None:
    """
    Pulls (pcm_id, qpdfs, apdfs) from download_q.
    For each PDF URL: downloads to disk (cache hit = instant).
    Puts ExtractItem(pcm_id, [(url, path), ...]) on extract_q.
    On SENTINEL: forwards SENTINEL to extract_q and exits.
    """
    session = new_session()
    logger.info("[downloader] started")

    while True:
        work = download_q.get()
        if work is _SENTINEL:
            download_q.task_done()
            extract_q.put(_SENTINEL)
            logger.info("[downloader] done — sentinel forwarded to extractor")
            return

        pcm_id, qpdfs, apdfs = work
        try:
            q_pairs = [
                (url, _download_one(session, pcm_id, "question", url))
                for url in (qpdfs or [])
            ]
            a_pairs = [
                (url, _download_one(session, pcm_id, "answer", url))
                for url in (apdfs or [])
            ]
            extract_q.put(ExtractItem(pcm_id=pcm_id, question=q_pairs, answer=a_pairs))
        except Exception:
            logger.exception("[downloader] unexpected error pcm_id={}", pcm_id)
            # Emit ExtractItem with empty paths so extractor marks the row
            extract_q.put(ExtractItem(
                pcm_id=pcm_id,
                question=[(u, "") for u in (qpdfs or [])],
                answer=[(u, "") for u in (apdfs or [])],
            ))
        finally:
            download_q.task_done()


def extractor_worker(
    extract_q: queue.Queue,
    pg_dsn: str,
    batch_size: int,
    total: int,
) -> None:
    """
    Pulls ExtractItem from extract_q.
    Reads each PDF from disk, runs pdfminer, batches UPDATE records.
    On SENTINEL: flushes remaining batch and closes DB connection.
    """
    pg = psycopg2.connect(pg_dsn)
    pg.autocommit = False
    pending: list[tuple] = []
    done = errors = 0
    t0 = time.monotonic()
    logger.info("[extractor] started")

    while True:
        item = extract_q.get()
        if item is _SENTINEL:
            extract_q.task_done()
            break

        try:
            q_entries: list[dict] = []
            a_entries: list[dict] = []
            item_has_error = False

            for url, path in item.question:
                if not path:
                    q_entries.append({"url": url, "text": ""})
                    item_has_error = True
                    continue
                try:
                    text = _strip_nul(_extract_text_from_path(path))
                    q_entries.append({"url": url, "text": text or ""})
                except Exception as exc:
                    q_entries.append({"url": url, "text": ""})
                    log_error(pg, item.pcm_id, url, "question", exc)
                    item_has_error = True

            for url, path in item.answer:
                if not path:
                    a_entries.append({"url": url, "text": ""})
                    item_has_error = True
                    continue
                try:
                    text = _strip_nul(_extract_text_from_path(path))
                    a_entries.append({"url": url, "text": text or ""})
                except Exception as exc:
                    a_entries.append({"url": url, "text": ""})
                    log_error(pg, item.pcm_id, url, "answer", exc)
                    item_has_error = True

            q_jsonb = json.dumps(q_entries, ensure_ascii=False) if q_entries else None
            a_jsonb = json.dumps(a_entries, ensure_ascii=False) if a_entries else None
            q_text = _strip_nul(_join_texts(q_entries))
            a_text = _strip_nul(_join_texts(a_entries))

            pending.append((q_jsonb, a_jsonb, q_text, a_text, item.pcm_id))

            if item_has_error:
                errors += 1
            else:
                done += 1

            if len(pending) >= batch_size:
                _flush_batch(pg, pending)
                pending.clear()

            # Progress line
            elapsed = time.monotonic() - t0
            processed = done + errors
            rate = processed / elapsed if elapsed > 0 else 0
            eta_s = (total - processed) / rate if rate > 0 else 0
            print(
                f"\r  extracted={done}/{total}  errors={errors}"
                f"  rate={rate:.1f}r/s  eta={_fmt_duration(eta_s)}",
                end="", flush=True,
            )

        except Exception:
            logger.exception("[extractor] unexpected error pcm_id={}", item.pcm_id)
        finally:
            extract_q.task_done()

    # Flush remaining batch
    if pending:
        _flush_batch(pg, pending)

    print()  # newline after progress line
    elapsed = time.monotonic() - t0
    logger.info(
        "[extractor] done: extracted={} errors={} elapsed={}",
        done, errors, _fmt_duration(elapsed),
    )
    print(
        f"Extractor done. {done} extracted, {errors} with errors, "
        f"{elapsed:.1f}s elapsed."
    )
    pg.close()


# ── Main ──────────────────────────────────────────────────────────────────────

def run(pg_dsn: str = PG_DSN, batch_size: int = BATCH_SIZE) -> None:
    # Initial read: count done + fetch all pending rows
    pg = psycopg2.connect(pg_dsn)
    try:
        with pg.cursor() as cur:
            cur.execute(COUNT_DONE_SQL)
            already_done = cur.fetchone()[0]
        with pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(SELECT_SQL)
            rows = cur.fetchall()
    finally:
        pg.close()

    total = len(rows)
    logger.info(
        "PDF extraction pipeline starting: already_done={} pending={}",
        already_done, total,
    )
    print(f"Already extracted : {already_done}")
    print(f"Pending this run  : {total}")

    if total == 0:
        print("Nothing pending — all PDFs already extracted.")
        return

    download_q: queue.Queue = queue.Queue(maxsize=DOWNLOAD_Q_MAX)
    extract_q: queue.Queue = queue.Queue(maxsize=EXTRACT_Q_MAX)

    dl = threading.Thread(
        target=downloader_worker,
        args=(download_q, extract_q),
        name="downloader",
        daemon=False,
    )
    ex = threading.Thread(
        target=extractor_worker,
        args=(extract_q, pg_dsn, batch_size, total),
        name="extractor",
        daemon=False,
    )

    dl.start()
    ex.start()

    t0 = time.monotonic()
    try:
        for row in rows:
            download_q.put((
                row["pcm_id"],
                row["question_pdfs"] or [],
                row["answer_pdfs"] or [],
            ))
        download_q.put(_SENTINEL)
    except KeyboardInterrupt:
        logger.warning("Interrupted — injecting sentinel to drain workers gracefully")
        download_q.put(_SENTINEL)

    dl.join()
    ex.join()

    elapsed = time.monotonic() - t0
    logger.info("Pipeline complete: elapsed={}", _fmt_duration(elapsed))
    print(f"Pipeline complete in {_fmt_duration(elapsed)}.")


if __name__ == "__main__":
    run()
