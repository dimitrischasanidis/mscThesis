#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import csv
import json
import time
from typing import Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import parse_qs, urljoin, urlparse
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

import re
from loguru import logger
import sys
from pathlib import Path
import random
from dataclasses import dataclass


class Blocked403(RuntimeError):
    def __init__(self, url: str, attempts: int):
        super().__init__(f"403 after {attempts} attempts: {url}")
        self.url = url
        self.attempts = attempts


# ----------------------------
# Config
# ----------------------------


LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "scrape_{time:YYYY-MM-DD}.log"
VISITED_PAGES_CSV = LOG_DIR / "visited_pages.csv"

_POSTBACK_RE = re.compile(
    r"__doPostBack\('(?P<target>[^']+)','(?P<arg>[^']*)'\)",
    re.IGNORECASE
)
_LANG_PREFIX_RE = re.compile(r"^[a-z]{2}$", re.IGNORECASE)
BASE = "https://www.hellenicparliament.gr"
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

DATE_FROM = "01/01/1995"
# DATE_FROM = "15/12/2025"
DATE_TO = "18/12/2025"
ONLY_WITH_ANSWER = False

SLEEP = 0.0
OUT_JSONL = f"data_from_{DATE_FROM.replace("/", "-")}_to_{DATE_TO.replace("/", "-")}.jsonl"
OUT_CSV = f"data_from_{DATE_FROM.replace("/", "-")}_to_{DATE_TO.replace("/", "-")}.csv"

# --- Request hardening ---
REQUEST_JITTER_MIN_S = 0.2
REQUEST_JITTER_MAX_S = 1.2

MAX_403_RETRIES = 3
WAIT_ON_403_SECONDS = 600  # 10 minutes

# Where to persist “403 happened” immediately (append-only)
FAILURES_CSV = LOG_DIR / "failures.csv"

USER_AGENTS = [
    # Keep a small, realistic set; change versions occasionally
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
]


# ----------------------------
# HTTP + WebForms abstraction
# ----------------------------

def setup_logging():
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    logger.add(
        LOG_DIR / "scraper.jsonl",
        level="DEBUG",
        rotation="50 MB",
        retention="30 days",
        compression="zip",
        serialize=True,  # <-- JSON lines
        enqueue=False,  # host process; fine without queue
    )


def _append_failure_row(row: Dict[str, str]) -> None:
    """
    Append-only CSV log for operational failures (403, timeouts, etc.).
    Safe to call frequently.
    """
    header = [
        "ts",
        "phase",
        "type_name",
        "pcm_id",
        "url",
        "status_code",
        "attempt",
        "action",
        "note",
    ]
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
        # Polite pacing / jitter
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
                phase, type_name, pcm_id, attempt, MAX_403_RETRIES, url, WAIT_ON_403_SECONDS
            )

            _append_failure_row({
                "ts": now,
                "phase": phase,
                "type_name": type_name,
                "pcm_id": pcm_id,
                "url": url,
                "status_code": "403",
                "attempt": str(attempt),
                "action": f"sleep {WAIT_ON_403_SECONDS}s + refresh session + rotate UA",
                "note": "",
            })

            if attempt >= MAX_403_RETRIES:
                raise Blocked403(url=url, attempts=attempt)

            # wait + refresh session (rotate UA)
            time.sleep(WAIT_ON_403_SECONDS)
            try:
                session.close()
            except Exception:
                pass
            session = new_session()  # new UA chosen inside
            continue

        resp.raise_for_status()
        return resp.text, session

    # defensive; should not reach
    raise Blocked403(url=url, attempts=MAX_403_RETRIES)


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
            if not name:
                continue
            out[name] = inp.get("value", "")
        return out

    def submit(self, fields: Dict[str, str], *, referer: Optional[str] = None) -> "WebFormsPage":
        payload = {**self.hidden_fields(), **fields}
        html, new_sess = request(
            self.session,
            "POST",
            self.url,
            data=payload,
            referer=referer or self.url,
            phase="search_submit",
        )
        return WebFormsPage(session=new_sess, url=self.url, html=html)


# ----------------------------
# Parsing helpers
# ----------------------------

def _is_greek_url(url: str) -> bool:
    """
    Accept ONLY Greek URLs.
    Reject /en/, /fr/, /de/, etc.
    """
    path = urlsplit(url).path.lstrip("/")
    if not path:
        return True
    first_segment = path.split("/", 1)[0]
    return len(first_segment) != 2  # reject 2-letter language prefixes


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
    """
    Best-effort extraction of the entry's date from the detail page.

    The site’s detail pages typically contain a date like dd/mm/yyyy near labels
    such as "Ημερομηνία", "Κατάθεσης", etc. We:
    1) Prefer dates found in text that also contains date-ish keywords.
    2) Fall back to the first dd/mm/yyyy found anywhere on the page.
    """
    soup = BeautifulSoup(detail_html, "html.parser")
    date_re = re.compile(r"\b(\d{1,2}/\d{1,2}/\d{4})\b")
    keywords = ("Ημερομην", "Κατάθε", "Συζήτ", "Απάντη", "Ημ.")

    # 1) Prefer a date close to date-related labels
    for txt in soup.stripped_strings:
        if any(k in txt for k in keywords):
            m = date_re.search(txt)
            if m:
                return m.group(1)

    # 2) Fallback: first date anywhere
    all_text = soup.get_text(strip=True)
    m = date_re.search(all_text)
    return m.group(1) if m else ""


# ----------------------------
# Scraper logic
# ----------------------------

def run_search(session: requests.Session, *, type_id: str) -> tuple[str, requests.Session]:
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
        "txtDateFrom": DATE_FROM,
        "txtDateTo": DATE_TO,
        "ctl00$ContentPlaceHolder1$pcl1$btnSubmit": "Αναζήτηση",
        "__EVENTTARGET": "",
        "__EVENTARGUMENT": "",
    }

    result_page = page.submit(fields, referer=SEARCH_URL)
    return result_page.html, result_page.session


def fetch_one_detail(
        session: requests.Session,
        pcm_id: str,
        *,
        type_name: str,
        type_id: str,
) -> Dict[str, object]:
    log = logger.bind(type=type_name)

    detail_url = f"{SEARCH_URL}?pcm_id={pcm_id}"
    log.debug("Fetching detail {}", detail_url)
    detail_html, session = request(
        session,
        "GET",
        detail_url,
        referer=SEARCH_URL,
        phase="detail",
        type_name=type_name,
        pcm_id=pcm_id,
    )
    qpdf, apdf = extract_question_answer_pdfs(detail_html)

    meta = extract_detail_fields(detail_html)

    # Prefer the parsed date field if present, fallback to old heuristic
    entry_date = (meta.get("date") or extract_entry_date(detail_html) or "").strip()
    meta["date"] = entry_date

    rec = {
        "type_name": type_name,
        "type_id": type_id,
        "pcm_id": pcm_id,
        "detail_url": detail_url,
        "question_pdfs": qpdf,
        "answer_pdfs": apdf,
        **meta,  # merges protocol_number, subject, etc.
    }
    return rec


def fetch_details(session: requests.Session, pcm_ids: Iterable[str], *, type_name: str, type_id: str) -> List[
    Dict[str, object]]:
    ids = list(pcm_ids)
    records: List[Dict[str, object]] = []

    total = len(ids)
    for i, pcm_id in enumerate(ids, start=1):
        detail_url = f"{SEARCH_URL}?pcm_id={pcm_id}"
        try:
            rec = fetch_one_detail(session, pcm_id, type_name=type_name, type_id=type_id)
            records.append(rec)
            logger.info(
                "[detail:{}] {}/{} pcm_id={} num={} date={} subj={} qpdf={} apdf={}",
                type_name, i, total, pcm_id,
                rec.get("protocol_number", "-"),
                rec.get("date") or "-",
                (rec.get("subject") or "-")[:80],
                len(rec.get("question_pdfs") or []), len(rec.get("answer_pdfs") or []),
            )
        except Blocked403 as e:
            now = datetime.now().isoformat(timespec="seconds")
            logger.error("[detail:{}] {}/{} pcm_id={} BLOCKED (403) url={}", type_name, i, total, pcm_id, e.url)
            _append_failure_row({
                "ts": now,
                "phase": "detail",
                "type_name": type_name,
                "pcm_id": pcm_id,
                "url": e.url,
                "status_code": "403",
                "attempt": str(e.attempts),
                "action": "give up (max retries reached)",
                "note": "record marked blocked=true",
            })

            # record is preserved, but flagged
            records.append({
                "type_name": type_name,
                "type_id": type_id,
                "pcm_id": pcm_id,
                "detail_url": detail_url,
                "blocked": True,
                "block_reason": f"403 after {e.attempts} attempts",
                "question_pdfs": [],
                "answer_pdfs": [],
            })
        except Exception:
            logger.exception("[detail:{}] {}/{} pcm_id={} ERROR", type_name, i, total, pcm_id)

        if SLEEP:
            time.sleep(SLEEP)

    return records


def write_outputs(records: List[Dict[str, object]]) -> None:
    def _date_sort_key(rec: Dict[str, object]) -> tuple[bool, datetime]:
        s = (rec.get("date") or "").strip()
        try:
            d = datetime.strptime(s, "%d/%m/%Y")
            return False, d
        except ValueError:
            return True, datetime.max

    records_sorted = sorted(records, key=_date_sort_key)

    # Choose stable CSV columns
    csv_fields = [
        "type_name",
        "type_id",
        "pcm_id",
        "detail_url",
        "protocol_number",
        "type_label",
        "session_period",
        "subject",
        "parliamentary_group",
        "date",
        "last_modified",
        "submitters",
        "ministries",
        "ministers",
        "question_pdfs",
        "answer_pdfs",
        "blocked",
        "blocked_reason"
    ]

    def _join_list(v: object) -> str:
        if isinstance(v, list):
            return " | ".join(str(x) for x in v if str(x).strip())
        return str(v or "")

    with open(OUT_JSONL, "w", encoding="utf-8") as f:
        for rec in records_sorted:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    with open(OUT_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=csv_fields)
        writer.writeheader()

        for rec in records_sorted:
            writer.writerow({
                "type_name": rec.get("type_name", ""),
                "type_id": rec.get("type_id", ""),
                "pcm_id": rec.get("pcm_id", ""),
                "detail_url": rec.get("detail_url", ""),
                "protocol_number": rec.get("protocol_number", ""),
                "type_label": rec.get("type_label", ""),
                "session_period": rec.get("session_period", ""),
                "subject": rec.get("subject", ""),
                "parliamentary_group": rec.get("parliamentary_group", ""),
                "date": rec.get("date", ""),
                "last_modified": rec.get("last_modified", ""),
                "submitters": _join_list(rec.get("submitters")),
                "ministries": _join_list(rec.get("ministries")),
                "ministers": _join_list(rec.get("ministers")),
                "question_pdfs": _join_list(rec.get("question_pdfs")),
                "answer_pdfs": _join_list(rec.get("answer_pdfs")),
                "blocked": str(bool(rec.get("blocked", False))).lower(),
                "blocked_reason": rec.get("block_reason", ""),
            })


def _normalize_url(url: str) -> str:
    """
    Normalize for de-duplication:
    - resolve scheme/host casing
    - strip fragment
    - sort query params
    """
    parts = urlsplit(url)
    scheme = parts.scheme.lower()
    netloc = parts.netloc.lower()
    path = parts.path
    query_items = sorted(parse_qsl(parts.query, keep_blank_values=True))
    query = urlencode(query_items, doseq=True)
    return urlunsplit((scheme, netloc, path, query, ""))


def _strip_lang_prefix(path: str) -> str:
    # "/en/xyz" -> "/xyz", "/fr/xyz" -> "/xyz", etc.
    parts = [p for p in path.split("/") if p]
    if parts and _LANG_PREFIX_RE.match(parts[0]):
        return "/" + "/".join(parts[1:])
    return path


def _has_meaningful_query(url: str) -> bool:
    # True if there is at least one non-empty query value
    qs = dict(parse_qsl(urlsplit(url).query, keep_blank_values=True))
    return any(v.strip() for v in qs.values())


def _is_base_search_url(url: str) -> bool:
    parts = urlsplit(url)
    path_no_lang = _strip_lang_prefix(parts.path.rstrip("/"))
    target = SEARCH_PATH.rstrip("/")

    # same endpoint (allow /en prefix), and no meaningful query
    return (path_no_lang == target) and (not _has_meaningful_query(url))


def _is_result_pagination_url(url: str) -> bool:
    qs = dict(parse_qsl(urlsplit(url).query, keep_blank_values=True))

    # never follow detail links or sort toggles
    if "pcm_id" in qs:
        return False
    if "SortBy" in qs or "SortDirection" in qs:
        return False

    # Heuristic: follow only URLs that change page.
    # Adjust these keys once you inspect what the site uses.
    page_keys = {"page", "Page", "p", "Pager", "Σελίδα", "pageNo"}
    return any(k in qs for k in page_keys)


def extract_result_page_urls(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls: Set[str] = set()

    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        if "pageNo=" not in href and "__doPostBack" not in href:
            continue

        full = urljoin(BASE + SEARCH_PATH, href)  # anchor can be "?pageNo=2"
        if not href:
            continue

        full_n = _normalize_url(full)
        if not _is_greek_url(full_n):
            continue
        if _is_base_search_url(full_n):
            continue
        if not _is_result_pagination_url(full_n):
            continue

        # KEEP ONLY GREEK URLS
        if not _is_greek_url(full_n):
            continue

        urls.add(full_n)

    return sorted(urls)


def extract_pager_postbacks(html: str) -> List[Tuple[str, str, int]]:
    """
    Returns a list of (event_target, event_argument, page_num).
    Captures ASP.NET pager links like:
      href="javascript:__doPostBack('...','Page$2')"
    Also tolerates tooltip/title "Σελίδα 2".
    """
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

    # sort by page number
    out.sort(key=lambda t: t[2])
    return out


def parse_results_summary(html: str) -> Optional[Dict[str, int]]:
    """
    Parses UI summary like:
      "Εγγραφές: 1 - 10 από 6797 - Σελίδες:"
    Returns dict: {start, end, total, page_size}
    """
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(strip=True)

    # tolerant to extra spaces / punctuation
    m = re.search(
        r"Εγγραφές:\s*(\d+)\s*-\s*(\d+)\s*από\s*(\d+)",
        text
    )
    if not m:
        return None

    start = int(m.group(1))
    end = int(m.group(2))
    total = int(m.group(3))
    page_size = max(1, end - start + 1)

    return {"start": start, "end": end, "total": total, "page_size": page_size}


def expected_pages(total: int, page_size: int) -> int:
    return (total + page_size - 1) // page_size


def get_page_no(url: str) -> Optional[int]:
    qs = dict(parse_qsl(urlsplit(url).query, keep_blank_values=True))
    v = qs.get("pageNo")
    if v and v.isdigit():
        return int(v)
    return None


def extract_detail_fields(detail_html: str) -> Dict[str, object]:
    """
    Extracts key/value metadata from a detail page.

    The site typically presents fields as label/value blocks. We try common patterns:
    - dt/dd pairs
    - table rows (th/td or td/td)
    - fallback: scan for known labels and take following sibling text

    Returns a dict with normalized keys (latin snake_case) + also keeps raw Greek labels
    under 'raw_fields' for safety/debugging.
    """
    soup = BeautifulSoup(detail_html, "html.parser")

    raw: Dict[str, object] = {}

    # --- Pattern A: dt/dd definition lists ---
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

    # --- Pattern B: table rows ---
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

    # --- Pattern C: label blocks (common in these pages) ---
    # Many pages have label as plain text followed by value as next element.
    # We look for strong/b tags ending with ':'.
    for lab in soup.select("strong, b"):
        t = lab.get_text(" ", strip=True)
        if not t.endswith(":"):
            continue
        key = t.rstrip(":").strip()
        # attempt: next siblings' text
        val_chunks: List[str] = []
        node = lab.next_sibling
        # collect a small run of siblings for value
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

    # Post-process: split list-like fields by newlines into lists
    list_like = {"Καταθέτοντες", "Υπουργεία", "Υπουργοί"}
    raw_norm: Dict[str, object] = {}
    for k, v in raw.items():
        if isinstance(v, str) and k in list_like:
            items = [x.strip() for x in v.splitlines() if x.strip()]
            raw_norm[k] = items
        else:
            raw_norm[k] = v

    # Map Greek labels to stable CSV keys (extend as you find more)
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


def init_visited_pages_csv() -> None:
    """
    Create the CSV file with header if it does not exist.
    """
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
    """
    Append one visited page record. Opens file per write for safety (and simplicity).
    """
    ts = datetime.now().isoformat(timespec="seconds")
    with open(VISITED_PAGES_CSV, "a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow([ts, type_name, type_id, page_no or "", url, pcm_ids_on_page])


def crawl_results(
        session: requests.Session,
        first_html: str,
        *,
        type_name: str,
        type_id: str,
) -> tuple[Set[str], requests.Session]:
    init_visited_pages_csv()

    all_pcm_ids: Set[str] = set(extract_pcm_ids(first_html))

    summary = parse_results_summary(first_html)
    if summary:
        total = summary["total"]
        page_size = summary["page_size"]
        total_pages = expected_pages(total, page_size)
    else:
        total = page_size = total_pages = None

    # Persist page 1 as "visited"
    append_visited_page(
        type_name=type_name,
        type_id=type_id,
        page_no=1,
        url=SEARCH_URL,
        pcm_ids_on_page=len(extract_pcm_ids(first_html)),
    )

    to_visit: Set[str] = set(extract_result_page_urls(first_html))

    # Minimal in-memory dedup (do NOT drop this or you'll loop)
    visited_pages: Set[int] = {1}
    visited_urls: Set[str] = set()  # optional, but helps if pageNo absent

    # initial log
    if summary:
        exp_rows_now = min(len(visited_pages) * page_size, total)
        logger.info(
            "[results] page=1 rows_ui={}-{} page_size={} total={} expected_pages={} expected_rows_so_far={} pcm_ids={}",
            summary["start"], summary["end"], page_size, total, total_pages, exp_rows_now, len(all_pcm_ids)
        )
    else:
        logger.info("[results] page=1 pcm_ids={} (UI summary not parsed)", len(all_pcm_ids))

    while to_visit:
        url = to_visit.pop()
        url_n = _normalize_url(url)

        if not _is_greek_url(url_n) or _is_base_search_url(url_n):
            continue
        if url_n in visited_urls:
            continue

        pn = get_page_no(url_n)
        if pn is not None:
            if pn in visited_pages:
                continue
            visited_pages.add(pn)

        visited_urls.add(url_n)

        html, session = request(session, "GET", url_n, referer=SEARCH_URL, phase="results_page")

        page_pcm_ids = extract_pcm_ids(html)
        all_pcm_ids.update(page_pcm_ids)

        # Persist this visited page to CSV immediately
        append_visited_page(
            type_name=type_name,
            type_id=type_id,
            page_no=pn,
            url=url_n,
            pcm_ids_on_page=len(page_pcm_ids),
        )

        for u in extract_result_page_urls(html):
            u_n = _normalize_url(u)
            if u_n not in visited_urls and not _is_base_search_url(u_n):
                to_visit.add(u_n)

        if summary:
            exp_rows_now = min(len(visited_pages) * page_size, total)
            logger.info(
                "[results] visited_pages={}/{} expected_rows_so_far={}/{} pcm_ids={} last_url={}",
                len(visited_pages), total_pages, exp_rows_now, total, len(all_pcm_ids), url_n
            )
        else:
            logger.info(
                "[results] visited_pages={} pcm_ids={} last_url={}",
                len(visited_pages), len(all_pcm_ids), url_n
            )

        if SLEEP:
            time.sleep(SLEEP)

    return all_pcm_ids, session


def main() -> None:
    setup_logging()
    logger.info("Starting scraper: DATE_FROM={} DATE_TO={}", DATE_FROM, DATE_TO)

    session = new_session()

    all_records: List[Dict[str, object]] = []

    for type_name in TYPES_TO_RUN:
        if type_name not in QUESTION_TYPES:
            raise ValueError(f"Unknown type_name '{type_name}'. Add it to QUESTION_TYPES.")

        type_id = QUESTION_TYPES[type_name]
        logger.info("[type] {} ({})", type_name, type_id)

        first_html, session = run_search(session, type_id=type_id)
        pcm_ids, session = crawl_results(session, first_html, type_name=type_name, type_id=type_id)
        records = fetch_details(session, sorted(pcm_ids), type_name=type_name, type_id=type_id)

        all_records.extend(records)

    write_outputs(all_records)
    logger.info("Done. Wrote {} records to {} and {}.", len(all_records), OUT_JSONL, OUT_CSV)


if __name__ == "__main__":
    main()
