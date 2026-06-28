#!/usr/bin/env python3
"""
Audit extracted records for anomalies.

Anomaly categories:
  empty_only   — extracted (texts NOT NULL) but every text entry is blank/empty
                 → silent extraction failure; safe to reset
  missing_pdf  — PDF not on disk (neither .pdf nor .pdf.failed) AND text is empty
                 → can't re-extract without re-downloading; reset so downloader retries
  orphan_fail  — pdf.failed marker exists but record was somehow extracted with text
                 → shouldn't happen given extractor logic; flag for inspection

Usage:
  python audit_extractions.py [--fix] [--fix-empty] [--fix-missing]

Flags:
  --fix          reset ALL anomaly categories (equivalent to --fix-empty --fix-missing)
  --fix-empty    reset only empty_only records
  --fix-missing  reset only records where PDF is missing AND text is empty

Environment:
  PG_DSN        Postgres DSN (default: same as services)
  PDF_CACHE_DIR PDF directory to check against (default: pdfs)
"""
from __future__ import annotations

import hashlib
import os
import sys
from pathlib import Path

import psycopg2
import psycopg2.extras

PG_DSN = os.environ.get(
    "PG_DSN",
    "host=localhost port=5433 dbname=parliament user=parliament password=parliament",
)
PDF_CACHE_DIR = Path(os.environ.get("PDF_CACHE_DIR", "pdfs"))

FIX_EMPTY = "--fix-empty" in sys.argv or "--fix" in sys.argv
FIX_MISSING = "--fix-missing" in sys.argv or "--fix" in sys.argv
DRY_RUN = not (FIX_EMPTY or FIX_MISSING)


def pdf_path(url: str) -> Path:
    return PDF_CACHE_DIR / (hashlib.sha1(url.encode()).hexdigest() + ".pdf")


def failed_path(url: str) -> Path:
    p = pdf_path(url)
    return p.parent / (p.name + ".failed")


def text_is_empty(entries: list[dict]) -> bool:
    return all(not e.get("text", "").strip() for e in entries)


def all_missing(urls: list[str]) -> bool:
    return all(not pdf_path(u).exists() and not failed_path(u).exists() for u in urls)


def any_orphan_fail(urls: list[str], entries: list[dict]) -> bool:
    """Has .pdf.failed marker but non-empty text was stored."""
    has_failed = any(failed_path(u).exists() for u in urls)
    has_text = any(e.get("text", "").strip() for e in entries)
    return has_failed and has_text


SELECT_SQL = """
SELECT pcm_id, question_pdfs, answer_pdfs, question_pdf_texts, answer_pdf_texts
FROM records
WHERE blocked = FALSE
  AND (question_pdf_texts IS NOT NULL OR answer_pdf_texts IS NOT NULL)
ORDER BY pcm_id
"""

RESET_SQL = """
UPDATE records
SET question_pdf_texts    = NULL,
    answer_pdf_texts      = NULL,
    question_text         = NULL,
    answer_text           = NULL,
    pdf_extraction_method = NULL
WHERE pcm_id = ANY(%s)
"""


def main() -> None:
    print(f"PDF_CACHE_DIR : {PDF_CACHE_DIR.resolve()}")
    print(f"PG_DSN        : {PG_DSN.split('password=')[0]}...")
    print(f"Mode          : {'DRY RUN' if DRY_RUN else 'FIX'} "
          f"(fix-empty={FIX_EMPTY}, fix-missing={FIX_MISSING})")
    print()

    pg = psycopg2.connect(PG_DSN)
    with pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(SELECT_SQL)
        rows = cur.fetchall()

    total = len(rows)
    print(f"Fetched {total:,} extracted records from DB")
    print()

    empty_only: list[str] = []       # all text blank
    missing_and_empty: list[str] = []  # PDF gone + text blank
    orphan_fail: list[str] = []       # .pdf.failed exists but text non-empty

    for row in rows:
        pcm_id = row["pcm_id"]
        q_urls: list[str] = row["question_pdfs"] or []
        a_urls: list[str] = row["answer_pdfs"] or []
        q_entries: list[dict] = row["question_pdf_texts"] or []
        a_entries: list[dict] = row["answer_pdf_texts"] or []

        all_urls = q_urls + a_urls
        all_entries = q_entries + a_entries

        q_empty = text_is_empty(q_entries) if q_entries else True
        a_empty = text_is_empty(a_entries) if a_entries else True
        both_empty = q_empty and a_empty

        if both_empty:
            empty_only.append(pcm_id)
            if all_missing(all_urls):
                missing_and_empty.append(pcm_id)

        if any_orphan_fail(all_urls, all_entries):
            orphan_fail.append(pcm_id)

    print("=" * 60)
    print("ANOMALY REPORT")
    print("=" * 60)
    print(f"  empty_only (all text blank)           : {len(empty_only):>8,}")
    print(f"  missing_and_empty (PDF gone + blank)  : {len(missing_and_empty):>8,}")
    print(f"  orphan_fail (failed marker + text)    : {len(orphan_fail):>8,}")
    print(f"  clean records                         : {total - len(set(empty_only) | set(orphan_fail)):>8,}")
    print()

    if DRY_RUN:
        print("DRY RUN — pass --fix / --fix-empty / --fix-missing to apply resets")
        pg.close()
        return

    to_reset: set[str] = set()
    if FIX_EMPTY:
        to_reset.update(empty_only)
        print(f"Queued {len(empty_only):,} empty_only records for reset")
    if FIX_MISSING:
        to_reset.update(missing_and_empty)
        print(f"Queued {len(missing_and_empty):,} missing_and_empty records for reset")

    if not to_reset:
        print("Nothing to reset.")
        pg.close()
        return

    ids = list(to_reset)
    print(f"Resetting {len(ids):,} records...")
    with pg.cursor() as cur:
        cur.execute(RESET_SQL, (ids,))
    pg.commit()
    print(f"Done. Reset {len(ids):,} records to NULL — extractor will reprocess after downloader refetches.")
    pg.close()


if __name__ == "__main__":
    main()
