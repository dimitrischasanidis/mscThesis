"""
Parliament Records Viewer — Streamlit app.

Browse/filter 155k+ records from the parliament Postgres DB,
view metadata + extracted text, and preview PDFs inline.
"""

import hashlib
import json
import os
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras
import requests
import streamlit as st

# ── Config ────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Parliament Viewer",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="expanded",
)

PG_DSN = os.environ.get(
    "PG_DSN",
    "host=postgres port=5432 dbname=parliament user=parliament password=parliament",
)

PDF_CACHE_DIR = Path(os.environ.get("PDF_CACHE_DIR", "/cache"))
PDF_CACHE_DIR.mkdir(parents=True, exist_ok=True)

PAGE_SIZE = 25

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}


# ── DB ────────────────────────────────────────────────────────────────────────


@st.cache_resource
def get_conn():
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    return conn


def _cur():
    return get_conn().cursor(cursor_factory=psycopg2.extras.RealDictCursor)


def query(sql: str, params=()) -> list:
    with _cur() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def query_one(sql: str, params=()):
    rows = query(sql, params)
    return rows[0] if rows else None


# ── PDF helpers ───────────────────────────────────────────────────────────────


def _pdf_cache_path(url: str) -> Path:
    return PDF_CACHE_DIR / (hashlib.sha1(url.encode()).hexdigest() + ".pdf")


@st.cache_data(show_spinner=False, ttl=3600)
def fetch_pdf_bytes(url: str) -> bytes | None:
    cached = _pdf_cache_path(url)
    if cached.exists():
        return cached.read_bytes()
    try:
        r = requests.get(url, headers=_HEADERS, timeout=30)
        r.raise_for_status()
        data = r.content
        cached.write_bytes(data)
        return data
    except Exception:
        return None


def _static_pdf_url(url: str) -> str:
    sha = hashlib.sha1(url.encode()).hexdigest()
    return f"/app/static/{sha}.pdf"


def render_pdf(url: str, key_prefix: str) -> None:
    cached = _pdf_cache_path(url)
    if not cached.exists():
        with st.spinner("Fetching PDF…"):
            fetch_pdf_bytes(url)

    btn_col, _ = st.columns([1, 3])
    with btn_col:
        if cached.exists():
            st.download_button(
                "⬇ Download",
                data=cached.read_bytes(),
                file_name=key_prefix + ".pdf",
                mime="application/pdf",
                key=f"dl_{key_prefix}_{url[-20:]}",
            )
        st.link_button("🔗 Open remote", url)

    if cached.exists():
        static_url = _static_pdf_url(url)
        st.markdown(
            f'<iframe src="{static_url}" width="100%" height="720px" '
            f'style="border:none;border-radius:4px;"></iframe>',
            unsafe_allow_html=True,
        )
    else:
        st.warning("Could not fetch PDF — use the **Open remote** link above.")


# ── Sidebar / filters ─────────────────────────────────────────────────────────


def build_sidebar() -> dict:
    with st.sidebar:
        st.title("🏛️ Parliament")
        st.markdown("---")

        type_rows = query(
            "SELECT DISTINCT type_name FROM records WHERE type_name IS NOT NULL ORDER BY 1"
        )
        type_options = [r["type_name"] for r in type_rows]
        selected_types = st.multiselect("Record type", type_options)

        text_search = st.text_input("🔍 Search", placeholder="subject, question, answer…")

        st.markdown("**Date range**")
        date_from = st.text_input("From (YYYY-MM-DD)", key="df", placeholder="2020-01-01")
        date_to = st.text_input("To (YYYY-MM-DD)", key="dt", placeholder="2024-12-31")

        st.markdown("**Filters**")
        has_qpdf = st.checkbox("Has question PDF")
        has_apdf = st.checkbox("Has answer PDF")
        downloaded = st.checkbox("Downloaded (has PDFs)")
        extracted = st.checkbox("Text has been extracted to DB")
        extraction_methods = st.multiselect(
            "Extraction method",
            ["pdfminer", "ocr", "mixed"],
            placeholder="All methods",
        )
        hide_blocked = st.checkbox("Hide blocked", value=True)

    return {
        "types": selected_types,
        "text": text_search.strip(),
        "date_from": date_from.strip(),
        "date_to": date_to.strip(),
        "has_qpdf": has_qpdf,
        "has_apdf": has_apdf,
        "downloaded": downloaded,
        "extracted": extracted,
        "extraction_methods": extraction_methods,
        "hide_blocked": hide_blocked,
    }


# ── Query builder ─────────────────────────────────────────────────────────────


def _build_where(filters: dict) -> tuple[str, list]:
    clauses, params = [], []

    if filters["types"]:
        clauses.append("type_name = ANY(%s)")
        params.append(filters["types"])

    if filters["text"]:
        clauses.append(
            "(subject ILIKE %s OR question_text ILIKE %s OR answer_text ILIKE %s)"
        )
        like = f"%{filters['text']}%"
        params += [like, like, like]

    if filters["date_from"]:
        clauses.append("date >= %s")
        params.append(filters["date_from"])

    if filters["date_to"]:
        clauses.append("date <= %s")
        params.append(filters["date_to"])

    if filters["has_qpdf"]:
        clauses.append("jsonb_array_length(coalesce(question_pdfs,'[]'::jsonb)) > 0")

    if filters["has_apdf"]:
        clauses.append("jsonb_array_length(coalesce(answer_pdfs,'[]'::jsonb)) > 0")

    if filters["downloaded"]:
        clauses.append(
            "(jsonb_array_length(coalesce(question_pdfs,'[]'::jsonb)) > 0"
            " OR jsonb_array_length(coalesce(answer_pdfs,'[]'::jsonb)) > 0)"
        )

    if filters["extracted"]:
        clauses.append(
            "(question_pdf_texts IS NOT NULL OR answer_pdf_texts IS NOT NULL)"
        )

    if filters["extraction_methods"]:
        clauses.append("pdf_extraction_method = ANY(%s)")
        params.append(filters["extraction_methods"])

    if filters["hide_blocked"]:
        clauses.append("blocked = false")

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    return where, params


@st.cache_data(show_spinner=False, ttl=60)
def count_records(where_clause: str, params_json: str) -> int:
    row = query_one(f"SELECT count(*) AS n FROM records {where_clause}", json.loads(params_json))
    return row["n"] if row else 0


@st.cache_data(show_spinner=False, ttl=60)
def fetch_page(where_clause: str, params_json: str, page: int) -> list:
    offset = page * PAGE_SIZE
    sql = f"""
        SELECT pcm_id, date, type_name, type_label, subject, blocked
        FROM records
        {where_clause}
        ORDER BY CASE WHEN date ~ '^\d{2}/\d{2}/\d{4}$'
                      THEN to_date(date, 'DD/MM/YYYY')
                 END DESC NULLS LAST, pcm_id
        LIMIT {PAGE_SIZE} OFFSET {offset}
    """
    return query(sql, json.loads(params_json))


# ── Detail panel ──────────────────────────────────────────────────────────────


def _jlist(val) -> list:
    if not val:
        return []
    if isinstance(val, list):
        return val
    try:
        return json.loads(val)
    except Exception:
        return []


def render_detail(pcm_id: str) -> None:
    rec = query_one(
        """
        SELECT pcm_id, type_name, type_label, date, protocol_number,
               subject, parliamentary_group, session_period,
               submitters, ministries, ministers,
               question_pdfs, answer_pdfs,
               question_pdf_texts, answer_pdf_texts,
               question_text, answer_text,
               detail_url, blocked, block_reason, scraped_at
        FROM records WHERE pcm_id = %s
        """,
        (pcm_id,),
    )
    if not rec:
        st.error(f"Record {pcm_id} not found.")
        return

    st.subheader(rec["subject"] or "(no subject)")

    if rec["blocked"]:
        st.error(f"🚫 Blocked: {rec['block_reason'] or 'unknown reason'}")

    # Metadata
    c1, c2, c3 = st.columns(3)
    c1.metric("Date", rec["date"] or "—")
    c2.metric("Type", rec["type_label"] or rec["type_name"] or "—")
    c3.metric("Session", rec["session_period"] or "—")

    c4, c5, c6 = st.columns(3)
    c4.metric("Protocol #", rec["protocol_number"] or "—")
    c5.metric("Group", rec["parliamentary_group"] or "—")
    c6.metric("Scraped", str(rec["scraped_at"])[:10] if rec["scraped_at"] else "—")

    if rec["detail_url"]:
        st.link_button("🔗 Parliament page", rec["detail_url"])

    # People / orgs
    for field, label in [
        ("submitters", "Submitters"),
        ("ministries", "Ministries"),
        ("ministers", "Ministers"),
    ]:
        items = _jlist(rec[field])
        if items:
            with st.expander(f"{label} ({len(items)})"):
                for item in items:
                    st.write("•", item if isinstance(item, str) else json.dumps(item, ensure_ascii=False))

    # Extracted text — per-PDF if JSONB available, fallback to legacy single column
    def _render_text_section(label: str, jsonb_val, legacy_text: str, key_prefix: str):
        entries = _jlist(jsonb_val)
        if entries:
            for i, e in enumerate(entries):
                url_short = e.get("url", "")[-40:] if e.get("url") else f"PDF {i+1}"
                with st.expander(f"📝 {label} — PDF {i+1}  `…{url_short}`"):
                    if e.get("text"):
                        st.text_area(
                            "",
                            e["text"],
                            height=250,
                            disabled=True,
                            label_visibility="collapsed",
                            key=f"{key_prefix}_{i}_{pcm_id}",
                        )
                    else:
                        st.warning("PDF fetched but no text extracted (may be scanned image).")
        elif legacy_text:
            with st.expander(f"📝 {label}"):
                st.text_area(
                    "",
                    legacy_text,
                    height=250,
                    disabled=True,
                    label_visibility="collapsed",
                    key=f"{key_prefix}_{pcm_id}",
                )
        else:
            with st.expander(f"📝 {label}"):
                st.info("Not extracted yet — extractor service will process this automatically.")

    _render_text_section(
        "Question text",
        rec["question_pdf_texts"],
        rec["question_text"] or "",
        "qt",
    )
    _render_text_section(
        "Answer text",
        rec["answer_pdf_texts"],
        rec["answer_text"] or "",
        "at",
    )

    # PDFs
    q_pdfs = _jlist(rec["question_pdfs"])
    a_pdfs = _jlist(rec["answer_pdfs"])

    if q_pdfs or a_pdfs:
        st.subheader("📄 PDFs")
        tab_labels = [f"Question {i+1}" for i in range(len(q_pdfs))] + \
                     [f"Answer {i+1}" for i in range(len(a_pdfs))]
        tab_urls = [(url, "q") for url in q_pdfs] + [(url, "a") for url in a_pdfs]

        tabs = st.tabs(tab_labels)
        for tab, (url, kind) in zip(tabs, tab_urls):
            with tab:
                st.caption(url)
                render_pdf(url, f"{kind}_{pcm_id[:8]}")
    else:
        st.info("No PDFs for this record.")


# ── Main ──────────────────────────────────────────────────────────────────────


def main():
    filters = build_sidebar()

    # Session state init
    for k, v in [("page", 0), ("selected_pcm_id", None), ("_last_filter", "")]:
        if k not in st.session_state:
            st.session_state[k] = v

    # Reset page on filter change
    filter_sig = repr(filters)
    if st.session_state["_last_filter"] != filter_sig:
        st.session_state.page = 0
        st.session_state["_last_filter"] = filter_sig

    where_clause, where_params = _build_where(filters)
    params_json = json.dumps(where_params, ensure_ascii=False, default=str)

    total = count_records(where_clause, params_json)
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    st.session_state.page = min(st.session_state.page, total_pages - 1)

    st.title("🏛️ Greek Parliament Records")
    st.caption(f"**{total:,}** records · page {st.session_state.page + 1} / {total_pages}")

    rows = fetch_page(where_clause, params_json, st.session_state.page)
    df = pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()

    if df.empty:
        st.info("No records match the current filters.")
    else:
        event = st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
            on_select="rerun",
            selection_mode="single-row",
            column_config={
                "pcm_id": st.column_config.TextColumn("ID", width="medium"),
                "date": st.column_config.TextColumn("Date", width="small"),
                "type_name": st.column_config.TextColumn("Type", width="medium"),
                "type_label": st.column_config.TextColumn("Label", width="medium"),
                "subject": st.column_config.TextColumn("Subject"),
                "blocked": st.column_config.CheckboxColumn("Blocked", width="small"),
            },
        )

        sel_rows = event.selection.rows if event.selection.rows else []
        if sel_rows:
            st.session_state.selected_pcm_id = df.iloc[sel_rows[0]]["pcm_id"]

    # Pagination
    p_prev, _, p_next = st.columns([1, 4, 1])
    with p_prev:
        if st.button("← Prev", disabled=st.session_state.page <= 0, use_container_width=True):
            st.session_state.page -= 1
            st.rerun()
    with p_next:
        if st.button("Next →", disabled=st.session_state.page >= total_pages - 1, use_container_width=True):
            st.session_state.page += 1
            st.rerun()

    # Detail panel
    if st.session_state.selected_pcm_id:
        st.divider()
        render_detail(st.session_state.selected_pcm_id)


if __name__ == "__main__":
    main()
