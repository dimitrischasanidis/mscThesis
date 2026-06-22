#!/usr/bin/env python3
"""
PDF text extractor — always-on loop.

Reads PDFs from disk (written by downloader.py), extracts text with pdfminer,
falls back to OCR (tesseract via pytesseract) for scanned/image-only PDFs, and
stores the results in Postgres.

Coordination with downloader.py (file-existence protocol):
  pdfs/<sha1>.pdf        → extract text (pdfminer → OCR if empty)
  pdfs/<sha1>.pdf.failed → permanent download failure; store empty text
  neither exists         → downloader still working; skip record this cycle

Environment variables:
  PG_DSN            Postgres DSN (default: localhost:5433)
  PDF_CACHE_DIR     Local PDF cache directory (default: pdfs)
  POLL_INTERVAL_S   Seconds to sleep when no pending work (default: 60)
  BATCH_SIZE        Records fetched per DB query (default: 50)
"""

from __future__ import annotations

import json
import os
import time
import traceback as tb
from pathlib import Path
from typing import Optional

import psycopg2
import psycopg2.extras
from loguru import logger

try:
    from pdfminer.high_level import extract_text as _pdfminer_extract
    _PDFMINER_OK = True
except ImportError:
    _PDFMINER_OK = False
    logger.warning("pdfminer.six not installed — text extraction unavailable")

try:
    import pytesseract
    from pdf2image import convert_from_path
    _OCR_OK = True
except ImportError:
    _OCR_OK = False
    logger.warning("pytesseract/pdf2image not installed — OCR fallback unavailable")

from shared import (
    PDF_CACHE_DIR,
    _failed_marker,
    _pdf_cache_path,
    get_pg,
    setup_logging,
)

POLL_INTERVAL_S = int(os.environ.get("POLL_INTERVAL_S", "60"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "50"))

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
LIMIT %(limit)s
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


def _strip_nul(s: Optional[str]) -> Optional[str]:
    return s.replace("\x00", "") if s else s


def _join_texts(entries: list[dict]) -> str:
    parts = []
    for i, e in enumerate(entries):
        if e.get("text"):
            parts.append(f"=== PDF {i + 1} ===\n{e['text']}")
    return "\n\n".join(parts)


def _extract_pdfminer(path: Path) -> str:
    if not _PDFMINER_OK:
        raise RuntimeError("pdfminer.six not installed")
    with open(path, "rb") as fh:
        return (_pdfminer_extract(fh) or "").strip()


def _extract_ocr(path: Path) -> str:
    if not _OCR_OK:
        raise RuntimeError("pytesseract/pdf2image not installed")
    logger.info("OCR fallback for {}", path.name)
    pages = convert_from_path(str(path), dpi=300)
    return "\n".join(pytesseract.image_to_string(p, lang="ell+eng") for p in pages).strip()


def extract_text(path: Path) -> str:
    """pdfminer first; if empty and OCR available, try tesseract."""
    text = _extract_pdfminer(path)
    if not text and _OCR_OK:
        text = _extract_ocr(path)
    return text


def _is_ready(urls: list[str]) -> bool:
    """True when every URL has either .pdf or .pdf.failed (downloader finished)."""
    for url in urls:
        if not _pdf_cache_path(url).exists() and not _failed_marker(url).exists():
            return False
    return True


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
        logger.warning("Could not write error to DB: {}", db_exc)
        try:
            pg.rollback()
        except Exception:
            pass


def _process_urls(pg, pcm_id: str, urls: list[str], kind: str) -> list[dict]:
    entries: list[dict] = []
    for url in urls:
        if _failed_marker(url).exists():
            entries.append({"url": url, "text": ""})
            continue
        pdf = _pdf_cache_path(url)
        try:
            text = _strip_nul(extract_text(pdf))
            entries.append({"url": url, "text": text or ""})
        except Exception as exc:
            entries.append({"url": url, "text": ""})
            log_error(pg, pcm_id, url, kind, exc)
    return entries


def process_record(pg, pcm_id: str, q_urls: list[str], a_urls: list[str]) -> bool:
    """
    Extract text for one record. Returns True if processed, False if still pending.
    """
    if not _is_ready(q_urls + a_urls):
        return False

    q_entries = _process_urls(pg, pcm_id, q_urls, "question")
    a_entries = _process_urls(pg, pcm_id, a_urls, "answer")

    q_jsonb = json.dumps(q_entries, ensure_ascii=False) if q_entries else None
    a_jsonb = json.dumps(a_entries, ensure_ascii=False) if a_entries else None
    q_text = _strip_nul(_join_texts(q_entries))
    a_text = _strip_nul(_join_texts(a_entries))

    try:
        with pg.cursor() as cur:
            cur.execute(UPDATE_SQL, (q_jsonb, a_jsonb, q_text, a_text, pcm_id))
        pg.commit()
    except Exception as exc:
        logger.error("UPDATE failed pcm_id={}: {}", pcm_id, exc)
        try:
            pg.rollback()
        except Exception:
            pass

    return True


def run() -> None:
    setup_logging("extractor")
    logger.info("Extractor starting: POLL_INTERVAL_S={} BATCH_SIZE={}", POLL_INTERVAL_S, BATCH_SIZE)

    pg = get_pg()

    while True:
        try:
            with pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(SELECT_SQL, {"limit": BATCH_SIZE})
                rows = cur.fetchall()
        except psycopg2.OperationalError:
            logger.warning("PG connection lost — reconnecting")
            try:
                pg.close()
            except Exception:
                pass
            pg = get_pg()
            continue

        if not rows:
            logger.debug("No pending records — sleeping {}s", POLL_INTERVAL_S)
            time.sleep(POLL_INTERVAL_S)
            continue

        processed = skipped = 0
        for row in rows:
            pcm_id = row["pcm_id"]
            try:
                if process_record(pg, pcm_id, row["question_pdfs"] or [], row["answer_pdfs"] or []):
                    processed += 1
                else:
                    skipped += 1
            except Exception:
                logger.exception("Unexpected error pcm_id={}", pcm_id)
                skipped += 1

        logger.info("Batch: processed={} skipped_pending={}", processed, skipped)

        if processed == 0:
            time.sleep(POLL_INTERVAL_S)


if __name__ == "__main__":
    run()
