#!/usr/bin/env python3
"""
Migrate records from SQLite (data/parliament.db) → Postgres.
Safe to run repeatedly: uses ON CONFLICT DO UPDATE (upsert).
Safe to run while scraper is active: reads SQLite with WAL, streams in batches.
"""

import json
import sqlite3
import sys
import time
from pathlib import Path

import psycopg2
import psycopg2.extras

SQLITE_PATH = Path("data/parliament.db")

import os
PG_DSN = os.environ.get(
    "PG_DSN",
    "host=localhost port=5433 dbname=parliament user=parliament password=parliament",
)

BATCH_SIZE = 500

JSONB_COLS = {"submitters", "ministries", "ministers", "question_pdfs", "answer_pdfs", "raw_fields"}


def parse_jsonb(val: str | None) -> psycopg2.extras.Json | None:
    if not val:
        return None
    try:
        return psycopg2.extras.Json(json.loads(val))
    except (json.JSONDecodeError, TypeError):
        return psycopg2.extras.Json(val)


def _strip_nul(v: object) -> object:
    if isinstance(v, str):
        return v.replace("\x00", "")
    return v


def coerce_row(row: dict) -> dict:
    out = dict(row)
    for col in JSONB_COLS:
        out[col] = parse_jsonb(out.get(col))
    out["blocked"] = bool(out.get("blocked"))
    nullable_text = (
        "block_reason", "question_text", "answer_text", "protocol_number",
        "type_label", "session_period", "parliamentary_group", "last_modified",
        "subject", "detail_url",
    )
    for col in nullable_text:
        v = out.get(col)
        out[col] = _strip_nul(v) or None
    for col in ("pcm_id", "type_name", "type_id", "date"):
        out[col] = _strip_nul(out.get(col))
    scraped = out.get("scraped_at")
    out["scraped_at"] = scraped if scraped else None
    return out


UPSERT_SQL = """
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
    type_name          = EXCLUDED.type_name,
    type_id            = EXCLUDED.type_id,
    date               = EXCLUDED.date,
    protocol_number    = EXCLUDED.protocol_number,
    type_label         = EXCLUDED.type_label,
    session_period     = EXCLUDED.session_period,
    subject            = EXCLUDED.subject,
    parliamentary_group = EXCLUDED.parliamentary_group,
    last_modified      = EXCLUDED.last_modified,
    submitters         = EXCLUDED.submitters,
    ministries         = EXCLUDED.ministries,
    ministers          = EXCLUDED.ministers,
    question_pdfs      = EXCLUDED.question_pdfs,
    answer_pdfs        = EXCLUDED.answer_pdfs,
    question_text      = EXCLUDED.question_text,
    answer_text        = EXCLUDED.answer_text,
    blocked            = EXCLUDED.blocked,
    block_reason       = EXCLUDED.block_reason,
    detail_url         = EXCLUDED.detail_url,
    raw_fields         = EXCLUDED.raw_fields,
    scraped_at         = EXCLUDED.scraped_at
"""


def migrate(sqlite_path: Path = SQLITE_PATH, pg_dsn: str = PG_DSN, batch_size: int = BATCH_SIZE):
    if not sqlite_path.exists():
        print(f"SQLite not found: {sqlite_path}", file=sys.stderr)
        sys.exit(1)

    sq = sqlite3.connect(f"file:{sqlite_path}?mode=ro", uri=True)
    sq.row_factory = sqlite3.Row

    total = sq.execute("SELECT COUNT(*) FROM records").fetchone()[0]
    if total == 0:
        print("SQLite has 0 records — nothing to migrate yet.")
        sq.close()
        return

    pg = psycopg2.connect(pg_dsn)
    pg.autocommit = False

    inserted = updated = errors = 0
    t0 = time.monotonic()

    cur_sq = sq.execute("SELECT * FROM records")
    batch = cur_sq.fetchmany(batch_size)

    while batch:
        rows = [coerce_row(dict(r)) for r in batch]
        try:
            with pg.cursor() as cur:
                psycopg2.extras.execute_batch(cur, UPSERT_SQL, rows, page_size=batch_size)
            pg.commit()
            inserted += len(rows)
        except Exception as exc:
            pg.rollback()
            errors += len(rows)
            print(f"Batch error: {exc}", file=sys.stderr)

        elapsed = time.monotonic() - t0
        rate = inserted / elapsed if elapsed > 0 else 0
        remaining = total - inserted - errors
        eta = remaining / rate if rate > 0 else 0
        print(
            f"\r  migrated={inserted}/{total}  errors={errors}"
            f"  rate={rate:.0f}r/s  eta={eta:.0f}s",
            end="", flush=True,
        )
        batch = cur_sq.fetchmany(batch_size)

    print()
    elapsed = time.monotonic() - t0
    print(f"Done. {inserted} rows migrated, {errors} errors, {elapsed:.1f}s elapsed.")

    sq.close()
    pg.close()


if __name__ == "__main__":
    migrate()
