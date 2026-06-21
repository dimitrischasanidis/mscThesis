#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import csv
import io
import json
import sqlite3
import time
from typing import Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import parse_qs, urljoin, urlparse, urlencode
from datetime import datetime
from dataclasses import dataclass, field
from pathlib import Path
import re
import random
import sys

import requests
from bs4 import BeautifulSoup
from urllib.parse import urlsplit, urlunsplit, parse_qsl
from loguru import logger

try:
    from pdfminer.high_level import extract_text as _pdfminer_extract
    _PDFMINER_OK = True
except ImportError:
    _PDFMINER_OK = False


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
FAILURES_CSV = LOG_DIR / "failures.csv"

DB_PATH = Path("data/parliament.db")
PDF_CACHE_DIR = Path("pdfs")
# Set True to download + extract text from PDFs (significantly slower; do as separate pass)
EXTRACT_PDF_TEXT: bool = False

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
DATE_TO = "18/12/2025"
ONLY_WITH_ANSWER = False

SLEEP = 0.0
OUT_JSONL = f"data_from_{DATE_FROM.replace('/', '-')}_to_{DATE_TO.replace('/', '-')}.jsonl"
OUT_CSV = f"data_from_{DATE_FROM.replace('/', '-')}_to_{DATE_TO.replace('/', '-')}.csv"

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


# ----------------------------
# Checkpoint
# ----------------------------

@dataclass
class CheckpointStore:
    """Per-type resume state: which result pages visited, which pcm_ids detail-fetched."""
    path: Path
    visited_page_nos: Set[int] = field(default_factory=set)
    done_pcm_ids: Set[str] = field(default_factory=set)

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

    def save(self) -> None:
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


# ----------------------------
# Discovered-pcm_id file (survives restart before detail-fetch starts)
# ----------------------------

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


# ----------------------------
# SQLite storage
# ----------------------------

def init_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS records (
            pcm_id              TEXT PRIMARY KEY,
            type_name           TEXT,
            type_id             TEXT,
            date                TEXT,
            protocol_number     TEXT,
            type_label          TEXT,
            session_period      TEXT,
            subject             TEXT,
            parliamentary_group TEXT,
            last_modified       TEXT,
            submitters          TEXT,
            ministries          TEXT,
            ministers           TEXT,
            question_pdfs       TEXT,
            answer_pdfs         TEXT,
            question_text       TEXT,
            answer_text         TEXT,
            blocked             INTEGER DEFAULT 0,
            block_reason        TEXT,
            detail_url          TEXT,
            raw_fields          TEXT,
            scraped_at          TEXT
        )
    """)
    conn.commit()
    return conn


def _j(v: object) -> str:
    if isinstance(v, (list, dict)):
        return json.dumps(v, ensure_ascii=False)
    return str(v) if v is not None else ""


def upsert_record(conn: sqlite3.Connection, rec: Dict[str, object]) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO records
        (pcm_id, type_name, type_id, date, protocol_number, type_label,
         session_period, subject, parliamentary_group, last_modified,
         submitters, ministries, ministers, question_pdfs, answer_pdfs,
         question_text, answer_text, blocked, block_reason, detail_url,
         raw_fields, scraped_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            rec.get("pcm_id", ""),
            rec.get("type_name", ""),
            rec.get("type_id", ""),
            rec.get("date", ""),
            rec.get("protocol_number", ""),
            rec.get("type_label", ""),
            rec.get("session_period", ""),
            rec.get("subject", ""),
            rec.get("parliamentary_group", ""),
            rec.get("last_modified", ""),
            _j(rec.get("submitters")),
            _j(rec.get("ministries")),
            _j(rec.get("ministers")),
            _j(rec.get("question_pdfs")),
            _j(rec.get("answer_pdfs")),
            rec.get("question_text") or "",
            rec.get("answer_text") or "",
            1 if rec.get("blocked") else 0,
            rec.get("block_reason") or "",
            rec.get("detail_url", ""),
            _j(rec.get("raw_fields", {})),
            datetime.now().isoformat(timespec="seconds"),
        ),
    )
    conn.commit()


def export_to_files(conn: sqlite3.Connection) -> None:
    cur = conn.execute("SELECT * FROM records")
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, row)) for row in cur.fetchall()]

    def _date_key(r: Dict) -> tuple:
        try:
            return False, datetime.strptime((r.get("date") or "").strip(), "%d/%m/%Y")
        except ValueError:
            return True, datetime.max

    rows_sorted = sorted(rows, key=_date_key)

    def _parse_json_field(v):
        if isinstance(v, str):
            try:
                return json.loads(v)
            except Exception:
                return v
        return v

    with open(OUT_JSONL, "w", encoding="utf-8") as f:
        for r in rows_sorted:
            for k in ("submitters", "ministries", "ministers", "question_pdfs", "answer_pdfs", "raw_fields"):
                r[k] = _parse_json_field(r.get(k))
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    def _join(v) -> str:
        v = _parse_json_field(v)
        if isinstance(v, list):
            return " | ".join(str(x) for x in v if str(x).strip())
        return str(v or "")

    csv_fields = [
        "type_name", "type_id", "pcm_id", "detail_url", "protocol_number",
        "type_label", "session_period", "subject", "parliamentary_group",
        "date", "last_modified", "submitters", "ministries", "ministers",
        "question_pdfs", "answer_pdfs", "blocked", "block_reason",
    ]
    with open(OUT_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=csv_fields)
        writer.writeheader()
        for r in rows_sorted:
            writer.writerow({
                "type_name": r.get("type_name", ""),
                "type_id": r.get("type_id", ""),
                "pcm_id": r.get("pcm_id", ""),
                "detail_url": r.get("detail_url", ""),
                "protocol_number": r.get("protocol_number", ""),
                "type_label": r.get("type_label", ""),
                "session_period": r.get("session_period", ""),
                "subject": r.get("subject", ""),
                "parliamentary_group": r.get("parliamentary_group", ""),
                "date": r.get("date", ""),
                "last_modified": r.get("last_modified", ""),
                "submitters": _join(r.get("submitters")),
                "ministries": _join(r.get("ministries")),
                "ministers": _join(r.get("ministers")),
                "question_pdfs": _join(r.get("question_pdfs")),
                "answer_pdfs": _join(r.get("answer_pdfs")),
                "blocked": str(bool(r.get("blocked", 0))).lower(),
                "block_reason": r.get("block_reason", ""),
            })

    logger.info("Exported {} records → {} + {}", len(rows_sorted), OUT_JSONL, OUT_CSV)


# ----------------------------
# PDF extraction
# ----------------------------

def _pdf_cache_path(url: str) -> Path:
    import hashlib
    return PDF_CACHE_DIR / (hashlib.sha1(url.encode()).hexdigest() + ".pdf")


def fetch_pdf_bytes(url: str, session: requests.Session) -> bytes:
    cache = _pdf_cache_path(url)
    if cache.exists():
        return cache.read_bytes()
    resp = session.get(url, timeout=60, allow_redirects=True)
    resp.raise_for_status()
    PDF_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache.write_bytes(resp.content)
    return resp.content


def extract_pdf_text(url: str, session: requests.Session) -> str:
    if not _PDFMINER_OK:
        logger.warning("pdfminer not installed; run: pip install pdfminer.six")
        return ""
    try:
        data = fetch_pdf_bytes(url, session)
        return _pdfminer_extract(io.BytesIO(data)).strip()
    except Exception as e:
        logger.warning("PDF text extraction failed url={} err={}", url, e)
        return ""


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
        serialize=True,
        enqueue=False,
    )


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
                phase, type_name, pcm_id, attempt, MAX_403_RETRIES, url, WAIT_ON_403_SECONDS
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


# ----------------------------
# Parsing helpers
# ----------------------------

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


# ----------------------------
# URL helpers
# ----------------------------

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


def _result_page_url(page_no: int, type_id: str) -> str:
    """Build GET URL for a specific results page with current search params."""
    params = [
        ("SessionPeriod", ""),
        ("datefrom", DATE_FROM),
        ("dateto", DATE_TO),
        ("ministry", ""),
        ("mpId", ""),
        ("pageNo", str(page_no)),
        ("partyId", ""),
        ("protocol", ""),
        ("subject", ""),
        ("type", type_id),
    ]
    return SEARCH_URL + "?" + urlencode(params)


# ----------------------------
# Pagination helpers (kept for reference / postback fallback)
# ----------------------------

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


# ----------------------------
# Visited-pages CSV
# ----------------------------

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


def crawl_results(
        session: requests.Session,
        first_html: str,
        *,
        type_name: str,
        type_id: str,
        checkpoint: CheckpointStore,
) -> tuple[Set[str], requests.Session]:
    init_visited_pages_csv()

    # Seed from previously discovered pcm_ids (survive crash between crawl + detail phases)
    all_pcm_ids: Set[str] = load_discovered_pcm_ids(type_name)
    first_page_ids = set(extract_pcm_ids(first_html))
    new_ids = first_page_ids - all_pcm_ids
    if new_ids:
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

    pages_already_done = len(checkpoint.visited_page_nos)
    logger.info(
        "[results:{}] total={} pages={} page_size={} | checkpoint: {}/{} pages done, {} pcm_ids known",
        type_name, total, total_pages, page_size,
        pages_already_done, total_pages, len(all_pcm_ids),
    )

    start_time = time.monotonic()
    pages_this_run = 0

    for page_no in range(2, total_pages + 1):
        if page_no in checkpoint.visited_page_nos:
            continue

        url = _result_page_url(page_no, type_id)
        html, session = request(
            session, "GET", url, referer=SEARCH_URL,
            phase="results_page", type_name=type_name,
        )

        page_ids = set(extract_pcm_ids(html))
        new_ids = page_ids - all_pcm_ids
        if new_ids:
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

        if SLEEP:
            time.sleep(SLEEP)

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

    question_text = answer_text = ""
    if EXTRACT_PDF_TEXT:
        if qpdf:
            question_text = extract_pdf_text(qpdf[0], session)
        if apdf:
            answer_text = extract_pdf_text(apdf[0], session)

    rec: Dict[str, object] = {
        "type_name": type_name,
        "type_id": type_id,
        "pcm_id": pcm_id,
        "detail_url": detail_url,
        "question_pdfs": qpdf,
        "answer_pdfs": apdf,
        "question_text": question_text,
        "answer_text": answer_text,
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
        conn: sqlite3.Connection,
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
            upsert_record(conn, rec)
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
            logger.error("[detail:{}] {}/{} pcm_id={} BLOCKED (403) url={}", type_name, i, total, pcm_id, e.url)
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
            upsert_record(conn, blocked_rec)
            # Do NOT checkpoint.mark_pcm_id — allow retry on next run

        except Exception:
            logger.exception("[detail:{}] {}/{} pcm_id={} ERROR", type_name, i, total, pcm_id)

        if SLEEP:
            time.sleep(SLEEP)

    return session


# ----------------------------
# Main
# ----------------------------

def main() -> None:
    setup_logging()
    logger.info("Starting scraper: DATE_FROM={} DATE_TO={} EXTRACT_PDF_TEXT={}", DATE_FROM, DATE_TO, EXTRACT_PDF_TEXT)

    conn = init_db(DB_PATH)
    session = new_session()

    for type_name in TYPES_TO_RUN:
        if type_name not in QUESTION_TYPES:
            raise ValueError(f"Unknown type_name '{type_name}'. Add it to QUESTION_TYPES.")

        type_id = QUESTION_TYPES[type_name]
        checkpoint = CheckpointStore.load(type_name)

        logger.info(
            "[type] {} ({}) | checkpoint: {} result-pages done, {} details done",
            type_name, type_id,
            len(checkpoint.visited_page_nos),
            len(checkpoint.done_pcm_ids),
        )

        first_html, session = run_search(session, type_id=type_id)
        pcm_ids, session = crawl_results(
            session, first_html,
            type_name=type_name, type_id=type_id,
            checkpoint=checkpoint,
        )
        session = fetch_details(
            session, sorted(pcm_ids),
            type_name=type_name, type_id=type_id,
            checkpoint=checkpoint, conn=conn,
        )

    export_to_files(conn)
    conn.close()
    logger.info("Done. Data in {}", DB_PATH)


if __name__ == "__main__":
    main()
