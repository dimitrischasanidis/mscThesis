#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Parliament scraper — monitoring loop.

Polls hellenicparliament.gr for new/updated questions and stores metadata
(including PDF URLs) in Postgres. Does NOT download or extract PDFs —
those are handled by downloader.py and extractor.py.

Environment variables:
  PG_DSN            Postgres DSN (default: localhost:5433)
  POLL_INTERVAL_S   Seconds between monitoring cycles (default: 3600)
  LOOKBACK_DAYS     Date window for monitoring mode (default: 30)
  DATE_FROM         Override start date DD/MM/YYYY (RUN_ONCE only)
  DATE_TO           Override end date DD/MM/YYYY (RUN_ONCE only)
  RUN_ONCE          Set to 1/true to do a single pass then exit (backfill mode)
"""

from __future__ import annotations

import csv
import json
import os
import time
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import parse_qs, parse_qsl, urljoin, urlparse, urlencode, urlsplit, urlunsplit
import re
import random
import sys

import requests
from bs4 import BeautifulSoup
from loguru import logger

from shared import (
    BASE,
    LOG_DIR,
    Blocked403,
    _append_failure_row,
    get_pg,
    new_session,
    pg_upsert,
    request,
    setup_logging,
)

# ── Config ────────────────────────────────────────────────────────────────────

VISITED_PAGES_CSV = LOG_DIR / "visited_pages.csv"

_POSTBACK_RE = re.compile(
    r"__doPostBack\('(?P<target>[^']+)','(?P<arg>[^']*)'\)",
    re.IGNORECASE,
)
_LANG_PREFIX_RE = re.compile(r"^[a-z]{2}$", re.IGNORECASE)

SEARCH_PATH = "/Koinovouleftikos-Elenchos/Mesa-Koinovouleutikou-Elegxou"
SEARCH_URL = f"{BASE}{SEARCH_PATH}"

QUESTION_TYPES: Dict[str, str] = {
    "erotiseis": "63c1d403-0d19-409f-bb0d-055e01e1487c",
    "epikaires_erotiseis_v2": "5fc3564d-121b-47b0-b4e6-0a4782aef8cc",
    "ake_se_syndiasmo_me_erotisi": "4f55c103-067d-46ed-946f-39280e569ebe",
    "anafores": "dea07d68-8b31-4ded-9869-4e737c09cdb7",
    "eperotiseis": "c090f08b-5674-4d01-a410-a89c141d5863",
    "ake_pou_metatrapike_se_eperotisi": "bac77b5f-fe1c-45b6-b845-c1c00906427d",
    "ake": "7774b073-6685-4f72-9c5b-cae515c766c2",
    "erotisi_se_syndiasmo_me_ake": "188081d2-ec03-492e-8304-cdbc6e577ba5",
    "epikaires_erotiseis": "a34418d1-b5dc-4001-b2fb-d9402785d387",
}

TYPES_TO_RUN: List[str] = list(QUESTION_TYPES.keys())

# ── Checkpoint ────────────────────────────────────────────────────────────────


@dataclass
class CheckpointStore:
    """Per-type resume state for RUN_ONCE backfill mode."""
    path: Path
    visited_page_nos: Set[int] = field(default_factory=set)
    done_pcm_ids: Set[str] = field(default_factory=set)
    enabled: bool = field(default=True)

    @classmethod
    def load(cls, type_name: str) -> "CheckpointStore":
        path = LOG_DIR / f"checkpoint_{type_name}.json"
        if path.exists():
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                return cls(
                    path=path,
                    visited_page_nos=set(data.get("visited_page_nos", [])),
                    done_pcm_ids=set(data.get("done_pcm_ids", [])),
                )
            except Exception:
                logger.warning("Checkpoint corrupt for {}, starting fresh", type_name)
        return cls(path=path)

    @classmethod
    def noop(cls) -> "CheckpointStore":
        """Non-persistent checkpoint for monitoring mode."""
        return cls(path=Path("/dev/null"), enabled=False)

    def save(self) -> None:
        if not self.enabled:
            return
        self.path.write_text(
            json.dumps(
                {
                    "visited_page_nos": sorted(self.visited_page_nos),
                    "done_pcm_ids": sorted(self.done_pcm_ids),
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

    def mark_page(self, page_no: int) -> None:
        self.visited_page_nos.add(page_no)
        self.save()

    def mark_pcm_id(self, pcm_id: str) -> None:
        self.done_pcm_ids.add(pcm_id)
        self.save()


# ── Discovered-pcm_id files (RUN_ONCE resumption) ─────────────────────────────


def _pcm_id_file(type_name: str) -> Path:
    return LOG_DIR / f"pcm_ids_{type_name}.txt"


def load_discovered_pcm_ids(type_name: str) -> Set[str]:
    path = _pcm_id_file(type_name)
    if not path.exists():
        return set()
    return {line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()}


def append_discovered_pcm_ids(type_name: str, pcm_ids: Iterable[str]) -> None:
    path = _pcm_id_file(type_name)
    with open(path, "a", encoding="utf-8") as f:
        for pid in pcm_ids:
            f.write(pid + "\n")


# ── HTTP / WebForms abstraction ───────────────────────────────────────────────


@dataclass
class WebFormsPage:
    session: requests.Session
    url: str
    html: str

    @classmethod
    def load(cls, session: requests.Session, url: str) -> "WebFormsPage":
        html, new_sess = request(session, "GET", url, referer=url, phase="search_load")
        return cls(session=new_sess, url=url, html=html)

    @property
    def soup(self) -> BeautifulSoup:
        return BeautifulSoup(self.html, "html.parser")

    def hidden_fields(self) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for inp in self.soup.select("input[type='hidden'][name]"):
            name = inp.get("name")
            if not isinstance(name, str) or not name:
                continue
            out[name] = inp.get("value", "") or ""
        return out

    def submit(self, fields: Dict[str, str], *, referer: Optional[str] = None) -> "WebFormsPage":
        payload = {**self.hidden_fields(), **fields}
        html, new_sess = request(
            self.session, "POST", self.url,
            data=payload, referer=referer or self.url, phase="search_submit",
        )
        return WebFormsPage(session=new_sess, url=self.url, html=html)


# ── Parsing helpers ───────────────────────────────────────────────────────────


def _is_greek_url(url: str) -> bool:
    path = urlsplit(url).path.lstrip("/")
    if not path:
        return True
    first_segment = path.split("/", 1)[0]
    return len(first_segment) != 2


def extract_pcm_ids(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    ids: Set[str] = set()
    for a in soup.select("a[href*='pcm_id=']"):
        href = a.get("href") or ""
        full = urljoin(BASE, href)
        q = parse_qs(urlparse(full).query)
        if q.get("pcm_id"):
            ids.add(q["pcm_id"][0])
    return sorted(ids)


def pdfs_under_heading(soup: BeautifulSoup, heading_text: str) -> List[str]:
    needle = soup.find(string=lambda s: s and heading_text in s)
    if not needle:
        return []

    container = needle.find_parent()
    for _ in range(8):
        if not container:
            break
        pdfs = container.select("a[href$='.pdf'], a[href$='.PDF']")
        if pdfs:
            break
        container = container.find_parent()

    if not container:
        return []

    out: Set[str] = set()
    for a in container.select("a[href$='.pdf'], a[href$='.PDF']"):
        href = a.get("href")
        if href:
            out.add(urljoin(BASE, href))
    return sorted(out)


def extract_question_answer_pdfs(detail_html: str) -> Tuple[List[str], List[str]]:
    soup = BeautifulSoup(detail_html, "html.parser")
    qpdf = pdfs_under_heading(soup, "Αρχεία Μέσων Κοινοβουλευτικού Ελέγχου")
    apdf = pdfs_under_heading(soup, "Αρχεία Απαντήσεων")
    return qpdf, apdf


def extract_entry_date(detail_html: str) -> str:
    soup = BeautifulSoup(detail_html, "html.parser")
    date_re = re.compile(r"\b(\d{1,2}/\d{1,2}/\d{4})\b")
    keywords = ("Ημερομην", "Κατάθε", "Συζήτ", "Απάντη", "Ημ.")

    for txt in soup.stripped_strings:
        if any(k in txt for k in keywords):
            m = date_re.search(txt)
            if m:
                return m.group(1)

    all_text = soup.get_text(strip=True)
    m = date_re.search(all_text)
    return m.group(1) if m else ""


def extract_detail_fields(detail_html: str) -> Dict[str, object]:
    soup = BeautifulSoup(detail_html, "html.parser")
    raw: Dict[str, object] = {}

    for dt in soup.select("dt"):
        key = dt.get_text(" ", strip=True).rstrip(":")
        if not key:
            continue
        dd = dt.find_next_sibling("dd")
        if not dd:
            continue
        val = dd.get_text("\n", strip=True)
        if val:
            raw[key] = val

    for tr in soup.select("tr"):
        cells = tr.find_all(["th", "td"], recursive=False)
        if len(cells) < 2:
            continue
        key = cells[0].get_text(" ", strip=True).rstrip(":")
        if not key:
            continue
        val = cells[1].get_text("\n", strip=True)
        if val:
            raw.setdefault(key, val)

    for lab in soup.select("strong, b"):
        t = lab.get_text(" ", strip=True)
        if not t.endswith(":"):
            continue
        key = t.rstrip(":").strip()
        val_chunks: List[str] = []
        node = lab.next_sibling
        for _ in range(8):
            if node is None:
                break
            if getattr(node, "name", None) in {"strong", "b"}:
                break
            txt = ""
            try:
                txt = node.get_text(" ", strip=True) if hasattr(node, "get_text") else str(node).strip()
            except Exception:
                txt = str(node).strip()
            if txt:
                val_chunks.append(txt)
            node = getattr(node, "next_sibling", None)
        if val_chunks:
            raw.setdefault(key, "\n".join(val_chunks).strip())

    list_like = {"Καταθέτοντες", "Υπουργεία", "Υπουργοί"}
    raw_norm: Dict[str, object] = {}
    for k, v in raw.items():
        if isinstance(v, str) and k in list_like:
            raw_norm[k] = [x.strip() for x in v.splitlines() if x.strip()]
        else:
            raw_norm[k] = v

    mapping = {
        "Αριθμός": "protocol_number",
        "Τύπος": "type_label",
        "Συνοδος / Περίοδος": "session_period",
        "Θέμα": "subject",
        "Κοινοβουλευτική Ομάδα": "parliamentary_group",
        "Ημερομηνία": "date",
        "Ημ. Τελευταίας Τροποποίησης": "last_modified",
        "Καταθέτοντες": "submitters",
        "Υπουργεία": "ministries",
        "Υπουργοί": "ministers",
    }

    out: Dict[str, object] = {"raw_fields": raw_norm}
    for gr, en in mapping.items():
        if gr in raw_norm:
            out[en] = raw_norm[gr]

    return out


# ── URL helpers ───────────────────────────────────────────────────────────────


def _normalize_url(url: str) -> str:
    parts = urlsplit(url)
    scheme = parts.scheme.lower()
    netloc = parts.netloc.lower()
    path = parts.path
    query_items = sorted(parse_qsl(parts.query, keep_blank_values=True))
    query = urlencode(query_items, doseq=True)
    return urlunsplit((scheme, netloc, path, query, ""))


def _strip_lang_prefix(path: str) -> str:
    parts = [p for p in path.split("/") if p]
    if parts and _LANG_PREFIX_RE.match(parts[0]):
        return "/" + "/".join(parts[1:])
    return path


def _has_meaningful_query(url: str) -> bool:
    qs = dict(parse_qsl(urlsplit(url).query, keep_blank_values=True))
    return any(v.strip() for v in qs.values())


def _is_base_search_url(url: str) -> bool:
    parts = urlsplit(url)
    path_no_lang = _strip_lang_prefix(parts.path.rstrip("/"))
    target = SEARCH_PATH.rstrip("/")
    return (path_no_lang == target) and (not _has_meaningful_query(url))


def _result_page_url(page_no: int, type_id: str, date_from: str, date_to: str) -> str:
    params = [
        ("SessionPeriod", ""),
        ("datefrom", date_from),
        ("dateto", date_to),
        ("ministry", ""),
        ("mpId", ""),
        ("pageNo", str(page_no)),
        ("partyId", ""),
        ("protocol", ""),
        ("subject", ""),
        ("type", type_id),
    ]
    return SEARCH_URL + "?" + urlencode(params)


# ── Pagination helpers ────────────────────────────────────────────────────────


def extract_result_page_urls(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls: Set[str] = set()

    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        if "pageNo=" not in href and "__doPostBack" not in href:
            continue

        full = urljoin(BASE + SEARCH_PATH, href)
        if not href:
            continue

        full_n = _normalize_url(full)
        if not _is_greek_url(full_n):
            continue
        if _is_base_search_url(full_n):
            continue

        qs = dict(parse_qsl(urlsplit(full_n).query, keep_blank_values=True))
        if "pcm_id" in qs or "SortBy" in qs or "SortDirection" in qs:
            continue
        if "pageNo" not in qs:
            continue

        urls.add(full_n)

    return sorted(urls)


def extract_pager_postbacks(html: str) -> List[Tuple[str, str, int]]:
    soup = BeautifulSoup(html, "html.parser")
    out: List[Tuple[str, str, int]] = []
    seen: Set[Tuple[str, str]] = set()

    for a in soup.select("a[href*='__doPostBack']"):
        href = a.get("href") or ""
        m = _POSTBACK_RE.search(href)
        if not m:
            continue
        target = m.group("target")
        arg = m.group("arg") or ""
        if not arg.startswith("Page$"):
            continue
        try:
            page_num = int(arg.split("$", 1)[1])
        except Exception:
            continue
        key = (target, arg)
        if key in seen:
            continue
        seen.add(key)
        out.append((target, arg, page_num))

    out.sort(key=lambda t: t[2])
    return out


def parse_results_summary(html: str) -> Optional[Dict[str, int]]:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(strip=True)
    m = re.search(r"Εγγραφές:\s*(\d+)\s*-\s*(\d+)\s*από\s*(\d+)", text)
    if not m:
        return None
    start = int(m.group(1))
    end = int(m.group(2))
    total = int(m.group(3))
    page_size = max(1, end - start + 1)
    return {"start": start, "end": end, "total": total, "page_size": page_size}


def expected_pages(total: int, page_size: int) -> int:
    return (total + page_size - 1) // page_size


# ── Visited-pages CSV ─────────────────────────────────────────────────────────


def init_visited_pages_csv() -> None:
    if VISITED_PAGES_CSV.exists():
        return
    with open(VISITED_PAGES_CSV, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ts", "type_name", "type_id", "page_no", "url", "pcm_ids_on_page"])


def append_visited_page(
        *,
        type_name: str,
        type_id: str,
        page_no: Optional[int],
        url: str,
        pcm_ids_on_page: int,
) -> None:
    ts = datetime.now().isoformat(timespec="seconds")
    with open(VISITED_PAGES_CSV, "a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow([ts, type_name, type_id, page_no or "", url, pcm_ids_on_page])


# ── Scraper logic ─────────────────────────────────────────────────────────────


def run_search(
        session: requests.Session,
        *,
        type_id: str,
        date_from: str,
        date_to: str,
) -> tuple[str, requests.Session]:
    page = WebFormsPage.load(session, SEARCH_URL)

    fields = {
        "ctl00$tools1$txtSearch": "",
        "txtSubject": "",
        "txtProtocolNumber": "",
        "ddtype": type_id,
        "ddSessionPeriod": "",
        "ddPoliticalParties": "",
        "ddMps": "",
        "ddMinistries": "",
        "txtDateFrom": date_from,
        "txtDateTo": date_to,
        "ctl00$ContentPlaceHolder1$pcl1$btnSubmit": "Αναζήτηση",
        "__EVENTTARGET": "",
        "__EVENTARGUMENT": "",
    }

    result_page = page.submit(fields, referer=SEARCH_URL)
    return result_page.html, result_page.session


def crawl_results(
        session: requests.Session,
        first_html: str,
        *,
        type_name: str,
        type_id: str,
        checkpoint: CheckpointStore,
        date_from: str,
        date_to: str,
) -> tuple[Set[str], requests.Session]:
    init_visited_pages_csv()

    monitoring = not checkpoint.enabled

    all_pcm_ids: Set[str] = set() if monitoring else load_discovered_pcm_ids(type_name)
    first_page_ids = set(extract_pcm_ids(first_html))
    new_ids = first_page_ids - all_pcm_ids
    if new_ids and not monitoring:
        append_discovered_pcm_ids(type_name, new_ids)
    all_pcm_ids.update(first_page_ids)

    summary = parse_results_summary(first_html)
    if not summary:
        logger.warning("[results:{}] Could not parse summary — only page 1 scraped", type_name)
        return all_pcm_ids, session

    total = summary["total"]
    page_size = summary["page_size"]
    total_pages = expected_pages(total, page_size)

    if 1 not in checkpoint.visited_page_nos:
        append_visited_page(
            type_name=type_name, type_id=type_id,
            page_no=1, url=SEARCH_URL,
            pcm_ids_on_page=len(first_page_ids),
        )
        checkpoint.mark_page(1)

    logger.info(
        "[results:{}] total={} pages={} page_size={} | checkpoint: {}/{} pages done, {} pcm_ids known",
        type_name, total, total_pages, page_size,
        len(checkpoint.visited_page_nos), total_pages, len(all_pcm_ids),
    )

    start_time = time.monotonic()
    pages_this_run = 0

    for page_no in range(2, total_pages + 1):
        if page_no in checkpoint.visited_page_nos:
            continue

        url = _result_page_url(page_no, type_id, date_from, date_to)
        html, session = request(
            session, "GET", url, referer=SEARCH_URL,
            phase="results_page", type_name=type_name,
        )

        page_ids = set(extract_pcm_ids(html))
        new_ids = page_ids - all_pcm_ids
        if new_ids and not monitoring:
            append_discovered_pcm_ids(type_name, new_ids)
        all_pcm_ids.update(page_ids)

        append_visited_page(
            type_name=type_name, type_id=type_id,
            page_no=page_no, url=url,
            pcm_ids_on_page=len(page_ids),
        )
        checkpoint.mark_page(page_no)
        pages_this_run += 1

        elapsed = time.monotonic() - start_time
        rate = pages_this_run / elapsed if elapsed > 0 else 0
        remaining = total_pages - page_no
        eta_s = remaining / rate if rate > 0 else 0

        logger.info(
            "[results:{}] page={}/{} pcm_ids_page={} total_pcm_ids={} rate={:.1f}p/s eta={:.0f}s (~{:.1f}h)",
            type_name, page_no, total_pages,
            len(page_ids), len(all_pcm_ids),
            rate, eta_s, eta_s / 3600,
        )

    return all_pcm_ids, session


def fetch_one_detail(
        session: requests.Session,
        pcm_id: str,
        *,
        type_name: str,
        type_id: str,
) -> tuple[Dict[str, object], requests.Session]:
    detail_url = f"{SEARCH_URL}?pcm_id={pcm_id}"
    logger.debug("[detail:{}] fetching {}", type_name, detail_url)

    detail_html, session = request(
        session, "GET", detail_url, referer=SEARCH_URL,
        phase="detail", type_name=type_name, pcm_id=pcm_id,
    )
    qpdf, apdf = extract_question_answer_pdfs(detail_html)
    meta = extract_detail_fields(detail_html)

    entry_date = (meta.get("date") or extract_entry_date(detail_html) or "").strip()
    meta["date"] = entry_date

    rec: Dict[str, object] = {
        "type_name": type_name,
        "type_id": type_id,
        "pcm_id": pcm_id,
        "detail_url": detail_url,
        "question_pdfs": qpdf,
        "answer_pdfs": apdf,
        **meta,
    }
    return rec, session


def fetch_details(
        session: requests.Session,
        pcm_ids: Iterable[str],
        *,
        type_name: str,
        type_id: str,
        checkpoint: CheckpointStore,
        pg,
) -> requests.Session:
    all_ids = list(pcm_ids)
    pending = [p for p in all_ids if p not in checkpoint.done_pcm_ids]
    skipped = len(all_ids) - len(pending)

    if skipped:
        logger.info("[detail:{}] Skipping {} already-done pcm_ids", type_name, skipped)

    total = len(pending)
    start_time = time.monotonic()

    for i, pcm_id in enumerate(pending, start=1):
        detail_url = f"{SEARCH_URL}?pcm_id={pcm_id}"
        try:
            rec, session = fetch_one_detail(session, pcm_id, type_name=type_name, type_id=type_id)
            pg_upsert(pg, rec)
            checkpoint.mark_pcm_id(pcm_id)

            elapsed = time.monotonic() - start_time
            rate = i / elapsed if elapsed > 0 else 0
            eta_s = (total - i) / rate if rate > 0 else 0

            logger.info(
                "[detail:{}] {}/{} pcm_id={} num={} date={} subj={} qpdf={} apdf={} rate={:.1f}/s eta={:.0f}s (~{:.1f}h)",
                type_name, i, total, pcm_id,
                rec.get("protocol_number", "-"),
                rec.get("date") or "-",
                (rec.get("subject") or "-")[:80],
                len(rec.get("question_pdfs") or []),
                len(rec.get("answer_pdfs") or []),
                rate, eta_s, eta_s / 3600,
            )

        except Blocked403 as e:
            now = datetime.now().isoformat(timespec="seconds")
            logger.error(
                "[detail:{}] {}/{} pcm_id={} BLOCKED (403) url={}",
                type_name, i, total, pcm_id, e.url,
            )
            _append_failure_row({
                "ts": now, "phase": "detail", "type_name": type_name,
                "pcm_id": pcm_id, "url": e.url, "status_code": "403",
                "attempt": str(e.attempts), "action": "give up (max retries reached)",
                "note": "record marked blocked=true",
            })
            blocked_rec: Dict[str, object] = {
                "type_name": type_name, "type_id": type_id, "pcm_id": pcm_id,
                "detail_url": detail_url, "blocked": True,
                "block_reason": f"403 after {e.attempts} attempts",
                "question_pdfs": [], "answer_pdfs": [],
            }
            pg_upsert(pg, blocked_rec)

        except Exception:
            logger.exception("[detail:{}] {}/{} pcm_id={} ERROR", type_name, i, total, pcm_id)

    return session


# ── Main ──────────────────────────────────────────────────────────────────────


def main() -> None:
    setup_logging("scraper")

    run_once = os.environ.get("RUN_ONCE", "").lower() in ("1", "true", "yes")
    poll_interval_s = int(os.environ.get("POLL_INTERVAL_S", "3600"))
    lookback_days = int(os.environ.get("LOOKBACK_DAYS", "30"))

    logger.info(
        "Scraper starting: RUN_ONCE={} POLL_INTERVAL_S={} LOOKBACK_DAYS={}",
        run_once, poll_interval_s, lookback_days,
    )

    pg = get_pg()
    session = new_session()
    cycle = 0

    while True:
        cycle += 1

        if run_once:
            date_from_s = os.environ.get("DATE_FROM", "01/01/1995")
            date_to_s = os.environ.get("DATE_TO", datetime.now().strftime("%d/%m/%Y"))
        else:
            today = datetime.now()
            date_from_s = (today - timedelta(days=lookback_days)).strftime("%d/%m/%Y")
            date_to_s = today.strftime("%d/%m/%Y")

        logger.info("=== Cycle {} | {} → {} ===", cycle, date_from_s, date_to_s)

        for type_name in TYPES_TO_RUN:
            if type_name not in QUESTION_TYPES:
                logger.error("Unknown type_name '{}' — skipping", type_name)
                continue

            type_id = QUESTION_TYPES[type_name]
            checkpoint = CheckpointStore.load(type_name) if run_once else CheckpointStore.noop()

            logger.info(
                "[type] {} ({}) | checkpoint: {} pages done, {} details done",
                type_name, type_id,
                len(checkpoint.visited_page_nos),
                len(checkpoint.done_pcm_ids),
            )

            try:
                first_html, session = run_search(
                    session, type_id=type_id,
                    date_from=date_from_s, date_to=date_to_s,
                )
                pcm_ids, session = crawl_results(
                    session, first_html,
                    type_name=type_name, type_id=type_id,
                    checkpoint=checkpoint,
                    date_from=date_from_s, date_to=date_to_s,
                )
                session = fetch_details(
                    session, sorted(pcm_ids),
                    type_name=type_name, type_id=type_id,
                    checkpoint=checkpoint, pg=pg,
                )
            except Exception:
                logger.exception("Error processing type_name={}", type_name)
                try:
                    pg.close()
                except Exception:
                    pass
                try:
                    pg = get_pg()
                except Exception:
                    logger.error("Cannot reconnect to Postgres — will retry next cycle")

        if run_once:
            logger.info("RUN_ONCE complete — exiting")
            break

        logger.info("Cycle {} done. Sleeping {}s", cycle, poll_interval_s)
        time.sleep(poll_interval_s)


if __name__ == "__main__":
    main()
