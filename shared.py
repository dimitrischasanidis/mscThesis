#!/usr/bin/env python3
"""
Shared utilities for the parliament scraper pipeline.
Imported by scraper.py, downloader.py, and extractor.py.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import random
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import psycopg2
import psycopg2.extras
import requests
from loguru import logger

# ── Constants ─────────────────────────────────────────────────────────────────

BASE = "https://www.hellenicparliament.gr"

LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

FAILURES_CSV = LOG_DIR / "failures.csv"

PDF_CACHE_DIR = Path(os.environ.get("PDF_CACHE_DIR", "pdfs"))

REQUEST_JITTER_MIN_S = 0.2
REQUEST_JITTER_MAX_S = 1.2
MAX_403_RETRIES = 3
WAIT_ON_403_SECONDS = 600

USER_AGENTS = [
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
]

# ── Exception ─────────────────────────────────────────────────────────────────


class Blocked403(RuntimeError):
    def __init__(self, url: str, attempts: int):
        super().__init__(f"403 after {attempts} attempts: {url}")
        self.url = url
        self.attempts = attempts


# ── Logging ───────────────────────────────────────────────────────────────────


def setup_logging(log_name: str = "scraper") -> None:
    """Configure loguru: stderr INFO + logs/{log_name}.jsonl (rotated, thread-safe)."""
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger.add(
        LOG_DIR / f"{log_name}.jsonl",
        level="DEBUG",
        rotation="50 MB",
        retention="30 days",
        compression="zip",
        serialize=True,
        enqueue=True,
    )


# ── HTTP helpers ──────────────────────────────────────────────────────────────


def _append_failure_row(row: Dict[str, str]) -> None:
    header = ["ts", "phase", "type_name", "pcm_id", "url", "status_code", "attempt", "action", "note"]
    exists = FAILURES_CSV.exists()
    with open(FAILURES_CSV, "a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        if not exists:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in header})


def new_session(user_agent: Optional[str] = None) -> requests.Session:
    ua = user_agent or random.choice(USER_AGENTS)
    s = requests.Session()
    s.headers.update({
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "el,en-US;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
    })
    s.cookies.set("agreeToCookies", "1", domain="www.hellenicparliament.gr", path="/")
    return s


def request(
        session: requests.Session,
        method: str,
        url: str,
        *,
        params: Optional[Dict[str, str]] = None,
        data: Optional[Dict[str, str]] = None,
        referer: Optional[str] = None,
        timeout: int = 30,
        phase: str = "",
        type_name: str = "",
        pcm_id: str = "",
) -> tuple[str, requests.Session]:
    headers: Dict[str, str] = {}
    if referer:
        headers["Referer"] = referer

    for attempt in range(1, MAX_403_RETRIES + 1):
        if REQUEST_JITTER_MAX_S > 0:
            time.sleep(random.uniform(REQUEST_JITTER_MIN_S, REQUEST_JITTER_MAX_S))

        resp = session.request(
            method=method.upper(),
            url=url,
            params=params,
            data=data,
            headers=headers,
            timeout=timeout,
            allow_redirects=True,
        )

        if resp.status_code == 403:
            now = datetime.now().isoformat(timespec="seconds")
            logger.warning(
                "HTTP 403 | phase={} type={} pcm_id={} | attempt {}/{} | url={} | waiting {}s then refreshing session",
                phase, type_name, pcm_id, attempt, MAX_403_RETRIES, url, WAIT_ON_403_SECONDS,
            )
            _append_failure_row({
                "ts": now, "phase": phase, "type_name": type_name, "pcm_id": pcm_id,
                "url": url, "status_code": "403", "attempt": str(attempt),
                "action": f"sleep {WAIT_ON_403_SECONDS}s + refresh session + rotate UA", "note": "",
            })

            if attempt >= MAX_403_RETRIES:
                raise Blocked403(url=url, attempts=attempt)

            time.sleep(WAIT_ON_403_SECONDS)
            try:
                session.close()
            except Exception:
                pass
            session = new_session()
            continue

        resp.raise_for_status()
        return resp.text, session

    raise Blocked403(url=url, attempts=MAX_403_RETRIES)


# ── PDF helpers ───────────────────────────────────────────────────────────────


def _pdf_cache_path(url: str) -> Path:
    return PDF_CACHE_DIR / (hashlib.sha1(url.encode()).hexdigest() + ".pdf")


def _failed_marker(url: str) -> Path:
    """Sentinel file written by downloader on permanent download failure."""
    cache = _pdf_cache_path(url)
    return cache.parent / (cache.name + ".failed")


def fetch_pdf_bytes(url: str, session: requests.Session) -> bytes:
    cache = _pdf_cache_path(url)
    if cache.exists():
        return cache.read_bytes()
    resp = session.get(url, timeout=60, allow_redirects=True)
    resp.raise_for_status()
    PDF_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache.write_bytes(resp.content)
    return resp.content


# ── Database helpers ──────────────────────────────────────────────────────────

PG_DSN = os.environ.get(
    "PG_DSN",
    "host=localhost port=5433 dbname=parliament user=parliament password=parliament",
)

JSONB_COLS = frozenset({"submitters", "ministries", "ministers", "question_pdfs", "answer_pdfs", "raw_fields"})


def _strip_nul(v: object) -> object:
    """Remove NUL bytes that Postgres TEXT/JSONB rejects."""
    if isinstance(v, str):
        return v.replace("\x00", "")
    return v


def _parse_jsonb(val: object) -> Optional[psycopg2.extras.Json]:
    """Coerce a value to psycopg2.extras.Json for JSONB columns."""
    if val is None:
        return None
    if isinstance(val, (list, dict)):
        return psycopg2.extras.Json(val)
    if isinstance(val, str) and val:
        try:
            return psycopg2.extras.Json(json.loads(val))
        except (json.JSONDecodeError, TypeError):
            return psycopg2.extras.Json(val)
    return None


def coerce_record(rec: Dict) -> Dict:
    """Prepare a record dict for Postgres UPSERT (type coercions, NUL stripping)."""
    out = dict(rec)
    for col in JSONB_COLS:
        out[col] = _parse_jsonb(out.get(col))
    out["blocked"] = bool(out.get("blocked"))
    nullable_text = (
        "block_reason", "question_text", "answer_text", "protocol_number",
        "type_label", "session_period", "parliamentary_group", "last_modified",
        "subject", "detail_url",
    )
    for col in nullable_text:
        v = out.get(col)
        if isinstance(v, str):
            v = _strip_nul(v) or None
        out[col] = v or None
    for col in ("pcm_id", "type_name", "type_id", "date"):
        v = out.get(col)
        if isinstance(v, str):
            out[col] = _strip_nul(v)
    if not out.get("scraped_at"):
        out["scraped_at"] = datetime.now().isoformat(timespec="seconds")
    return out


# Scraper UPSERT: never overwrites extracted text (COALESCE preserves extractor's work)
SCRAPER_UPSERT_SQL = """
INSERT INTO records (
    pcm_id, type_name, type_id, date, protocol_number, type_label,
    session_period, subject, parliamentary_group, last_modified,
    submitters, ministries, ministers, question_pdfs, answer_pdfs,
    question_text, answer_text, blocked, block_reason, detail_url,
    raw_fields, scraped_at
) VALUES (
    %(pcm_id)s, %(type_name)s, %(type_id)s, %(date)s, %(protocol_number)s, %(type_label)s,
    %(session_period)s, %(subject)s, %(parliamentary_group)s, %(last_modified)s,
    %(submitters)s, %(ministries)s, %(ministers)s, %(question_pdfs)s, %(answer_pdfs)s,
    %(question_text)s, %(answer_text)s, %(blocked)s, %(block_reason)s, %(detail_url)s,
    %(raw_fields)s, %(scraped_at)s
)
ON CONFLICT (pcm_id) DO UPDATE SET
    type_name           = EXCLUDED.type_name,
    type_id             = EXCLUDED.type_id,
    date                = EXCLUDED.date,
    protocol_number     = EXCLUDED.protocol_number,
    type_label          = EXCLUDED.type_label,
    session_period      = EXCLUDED.session_period,
    subject             = EXCLUDED.subject,
    parliamentary_group = EXCLUDED.parliamentary_group,
    last_modified       = EXCLUDED.last_modified,
    submitters          = EXCLUDED.submitters,
    ministries          = EXCLUDED.ministries,
    ministers           = EXCLUDED.ministers,
    question_pdfs       = EXCLUDED.question_pdfs,
    answer_pdfs         = EXCLUDED.answer_pdfs,
    question_text       = COALESCE(EXCLUDED.question_text, records.question_text),
    answer_text         = COALESCE(EXCLUDED.answer_text, records.answer_text),
    blocked             = EXCLUDED.blocked,
    block_reason        = EXCLUDED.block_reason,
    detail_url          = EXCLUDED.detail_url,
    raw_fields          = EXCLUDED.raw_fields,
    scraped_at          = EXCLUDED.scraped_at
"""


def get_pg(dsn: str = PG_DSN):
    """Open a new psycopg2 connection (autocommit=False)."""
    pg = psycopg2.connect(dsn)
    pg.autocommit = False
    return pg


def pg_upsert(pg, rec: Dict) -> None:
    """Upsert one record into records table. Commits on success, rolls back on error."""
    row = coerce_record(rec)
    try:
        with pg.cursor() as cur:
            cur.execute(SCRAPER_UPSERT_SQL, row)
        pg.commit()
    except Exception:
        try:
            pg.rollback()
        except Exception:
            pass
        raise
