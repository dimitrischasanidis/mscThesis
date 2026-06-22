#!/usr/bin/env python3
"""
PDF text extractor — always-on loop.

Reads PDFs from disk (written by downloader.py), extracts text with pdfminer,
falls back to OCR (Surya on GPU) for scanned/image-only PDFs, and stores
results in Postgres.

Coordination with downloader.py (file-existence protocol):
  pdfs/<sha1>.pdf        → extract text (pdfminer → OCR if empty)
  pdfs/<sha1>.pdf.failed → permanent download failure; store empty text
  neither exists         → downloader still working; skip record this cycle

Environment variables:
  PG_DSN            Postgres DSN (default: localhost:5433)
  PDF_CACHE_DIR     Local PDF cache directory (default: pdfs)
  POLL_INTERVAL_S   Seconds to sleep when no pending work (default: 60)
  BATCH_SIZE        Records fetched per DB query (default: 50)
  TORCH_DEVICE      Device for Surya OCR: 'cuda' or 'cpu' (default: cpu)
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
    import PIL.Image
    from pdf2image import convert_from_path
    PIL.Image.MAX_IMAGE_PIXELS = None  # suppress DecompressionBombWarning for large scans
    _PDF2IMAGE_OK = True
except ImportError:
    _PDF2IMAGE_OK = False
    logger.warning("pdf2image/Pillow not installed — OCR unavailable")

# Surya OCR — GPU-accelerated, multilingual (includes Greek)
_OCR_OK = False
_det_predictor = None
_rec_predictor = None

if _PDF2IMAGE_OK:
    try:
        from surya.recognition import RecognitionPredictor
        from surya.detection import DetectionPredictor

        _device = os.environ.get("TORCH_DEVICE", "cpu")
        logger.info("Loading Surya OCR models on device={}", _device)
        _det_predictor = DetectionPredictor()
        _rec_predictor = RecognitionPredictor()
        _OCR_OK = True
        logger.info("Surya OCR models loaded — OCR fallback active on device={}", _device)
    except Exception as _surya_err:
        logger.warning("Surya OCR unavailable ({}): {}", type(_surya_err).__name__, _surya_err)
        logger.warning("Records with scanned PDFs will log errors to pdf_extraction_errors")

from shared import (
    PDF_CACHE_DIR,
    _failed_marker,
    _pdf_cache_path,
    get_pg,
    setup_logging,
)

POLL_INTERVAL_S = int(os.environ.get("POLL_INTERVAL_S", "60"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "50"))

# Newest-first: parse DD/MM/YYYY with a regex guard so malformed dates sort last
_DATE_ORDER = r"""
    CASE WHEN date ~ '^\d{2}/\d{2}/\d{4}$'
         THEN to_date(date, 'DD/MM/YYYY')
    END DESC NULLS LAST
"""

SELECT_SQL = f"""
SELECT pcm_id, date, question_pdfs, answer_pdfs
FROM records
WHERE blocked = FALSE
  AND (
        (jsonb_array_length(coalesce(question_pdfs, '[]'::jsonb)) > 0
         AND question_pdf_texts IS NULL)
     OR (jsonb_array_length(coalesce(answer_pdfs, '[]'::jsonb)) > 0
         AND answer_pdf_texts IS NULL)
      )
ORDER BY {_DATE_ORDER}, pcm_id
LIMIT %(limit)s
"""

UPDATE_SQL = """
UPDATE records
SET question_pdf_texts    = %s,
    answer_pdf_texts      = %s,
    question_text         = %s,
    answer_text           = %s,
    pdf_extraction_method = %s
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
    """Rasterize PDF pages and run Surya OCR (GPU if TORCH_DEVICE=cuda)."""
    if not _OCR_OK:
        raise RuntimeError("Surya OCR not available — check startup logs")
    pages = convert_from_path(str(path), dpi=192)
    results = _rec_predictor(pages, [None] * len(pages), _det_predictor)
    lines = []
    for page_result in results:
        for line in page_result.text_lines:
            if line.text:
                lines.append(line.text)
    return "\n".join(lines).strip()


def extract_text(path: Path) -> tuple[str, str]:
    """pdfminer first; if empty and OCR available, try Surya. Returns (text, method)."""
    text = _extract_pdfminer(path)
    if text:
        return text, "pdfminer"
    if _OCR_OK:
        logger.info("OCR fallback for {}", path.name)
        text = _extract_ocr(path)
        return text, "ocr"
    return "", "pdfminer"


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
            text, method = extract_text(pdf)
            text = _strip_nul(text)
            logger.info(
                "Extracted pcm_id={} kind={} method={} chars={} file={}",
                pcm_id, kind, method, len(text or ""), pdf.name,
            )
            entries.append({"url": url, "text": text or "", "method": method})
        except Exception as exc:
            entries.append({"url": url, "text": ""})
            log_error(pg, pcm_id, url, kind, exc)
    return entries


def process_record(pg, pcm_id: str, q_urls: list[str], a_urls: list[str]) -> bool:
    """
    Extract text for one record. Returns True if processed, False if still pending.
    Any unexpected record-level error is captured in pdf_extraction_errors.
    """
    if not _is_ready(q_urls + a_urls):
        return False

    try:
        q_entries = _process_urls(pg, pcm_id, q_urls, "question")
        a_entries = _process_urls(pg, pcm_id, a_urls, "answer")

        q_jsonb = json.dumps(q_entries, ensure_ascii=False) if q_entries else None
        a_jsonb = json.dumps(a_entries, ensure_ascii=False) if a_entries else None
        q_text = _strip_nul(_join_texts(q_entries))
        a_text = _strip_nul(_join_texts(a_entries))

        all_methods = {e["method"] for e in q_entries + a_entries if e.get("method")}
        if not all_methods:
            extraction_method = None
        elif all_methods == {"pdfminer"}:
            extraction_method = "pdfminer"
        elif all_methods == {"ocr"}:
            extraction_method = "ocr"
        else:
            extraction_method = "mixed"

        with pg.cursor() as cur:
            cur.execute(UPDATE_SQL, (q_jsonb, a_jsonb, q_text, a_text, extraction_method, pcm_id))
        pg.commit()
    except Exception as exc:
        logger.error("Record-level failure pcm_id={}: {}", pcm_id, exc)
        try:
            pg.rollback()
        except Exception:
            pass
        log_error(pg, pcm_id, "", "record", exc)

    return True


def run() -> None:
    setup_logging("extractor")
    logger.info(
        "Extractor starting: POLL_INTERVAL_S={} BATCH_SIZE={} OCR={}",
        POLL_INTERVAL_S, BATCH_SIZE, "surya" if _OCR_OK else "disabled",
    )

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
