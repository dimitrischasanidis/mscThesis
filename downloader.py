#!/usr/bin/env python3
"""
PDF downloader — always-on loop.

Polls Postgres for records with unextracted PDFs and downloads each PDF to the
local cache (pdfs/<sha1>.pdf). On permanent failure writes a .failed marker so
extractor.py knows to skip the URL.

Coordination with extractor.py:
  pdfs/<sha1>.pdf        → successfully downloaded; extractor will process
  pdfs/<sha1>.pdf.failed → permanent failure; extractor stores empty text

Environment variables:
  PG_DSN            Postgres DSN (default: localhost:5433)
  PDF_CACHE_DIR     Local PDF cache directory (default: pdfs)
  POLL_INTERVAL_S   Seconds to sleep when no pending work (default: 60)
  BATCH_SIZE        Records fetched per DB query (default: 200)
"""

from __future__ import annotations

import os
import random
import time
import traceback as tb
from pathlib import Path

import psycopg2
import psycopg2.extras
import requests
from loguru import logger

from shared import (
    BASE,
    PDF_CACHE_DIR,
    REQUEST_JITTER_MIN_S,
    REQUEST_JITTER_MAX_S,
    _failed_marker,
    _pdf_cache_path,
    fetch_pdf_bytes,
    get_pg,
    new_session,
    setup_logging,
)

POLL_INTERVAL_S = int(os.environ.get("POLL_INTERVAL_S", "60"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "200"))
SITE_CHECK_INTERVAL_S = 30

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

INSERT_ERROR_SQL = """
INSERT INTO pdf_extraction_errors (pcm_id, url, kind, error_type, error_msg, traceback)
VALUES (%s, %s, %s, %s, %s, %s)
"""

_TRANSIENT_EXC = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.ReadTimeout,
    ConnectionResetError,
    TimeoutError,
)


def classify(exc: Exception) -> str:
    if isinstance(exc, _TRANSIENT_EXC):
        return "transient"
    if isinstance(exc, requests.exceptions.HTTPError):
        code = exc.response.status_code if exc.response is not None else 0
        if code in (429,) or code >= 500:
            return "transient"
        return "permanent"
    return "permanent"


def site_is_up(session: requests.Session) -> bool:
    try:
        r = session.get(BASE, timeout=10, allow_redirects=True)
        return r.ok
    except Exception:
        return False


def wait_for_site(session: requests.Session) -> None:
    attempt = 0
    while True:
        attempt += 1
        logger.warning("Site appears down — waiting {}s (attempt {})", SITE_CHECK_INTERVAL_S, attempt)
        time.sleep(SITE_CHECK_INTERVAL_S)
        if site_is_up(session):
            logger.info("Site back up after {} poll(s)", attempt)
            return


def _jitter() -> None:
    time.sleep(random.uniform(REQUEST_JITTER_MIN_S, REQUEST_JITTER_MAX_S))


def log_error(pg, pcm_id: str, url: str, kind: str, exc: Exception) -> None:
    error_type = type(exc).__name__
    error_msg = str(exc)
    trace = tb.format_exc()
    logger.error(
        "PDF download failed — pcm_id={} kind={} url={} error_type={} error_msg={}",
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


def download_url(session: requests.Session, pg, pcm_id: str, kind: str, url: str) -> None:
    """Download one PDF. Idempotent: skips if cached or already failed."""
    pdf_path = _pdf_cache_path(url)
    failed_path = _failed_marker(url)

    if pdf_path.exists() or failed_path.exists():
        return

    def _attempt() -> None:
        _jitter()
        fetch_pdf_bytes(url, session)

    try:
        _attempt()
        logger.debug("Downloaded pcm_id={} kind={} url={}", pcm_id, kind, url)
        return
    except Exception as exc:
        if classify(exc) == "transient":
            if not site_is_up(session):
                wait_for_site(session)
            try:
                _attempt()
                logger.debug("Downloaded (retry) pcm_id={} kind={} url={}", pcm_id, kind, url)
                return
            except Exception as exc2:
                exc = exc2

    # Permanent or retry-exhausted: write .failed marker
    PDF_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    failed_path.write_bytes(b"")
    log_error(pg, pcm_id, url, kind, exc)


def run() -> None:
    setup_logging("downloader")
    logger.info("Downloader starting: POLL_INTERVAL_S={} BATCH_SIZE={}", POLL_INTERVAL_S, BATCH_SIZE)

    pg = get_pg()
    session = new_session()
    PDF_CACHE_DIR.mkdir(parents=True, exist_ok=True)

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
            logger.debug("No pending PDFs — sleeping {}s", POLL_INTERVAL_S)
            time.sleep(POLL_INTERVAL_S)
            continue

        logger.info("Downloading PDFs for {} records", len(rows))
        for row in rows:
            pcm_id = row["pcm_id"]
            for url in (row["question_pdfs"] or []):
                download_url(session, pg, pcm_id, "question", url)
            for url in (row["answer_pdfs"] or []):
                download_url(session, pg, pcm_id, "answer", url)

        logger.info("Batch done")


if __name__ == "__main__":
    run()
