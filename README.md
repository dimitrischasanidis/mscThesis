# Greek Parliament Scraper

Scrapes the Hellenic Parliament's parliamentary questions database, extracts PDF text, and serves a Streamlit viewer — all with Loki/Grafana observability.

## Architecture

```
hellenicparliament.gr
        │
        ▼
  [scraper.py] ──► Postgres (records table: metadata + PDF URLs)
                        │
                        ▼
  [downloader.py] ──► pdfs/<sha1>.pdf  (or .pdf.failed on error)
                        │
                        ▼
  [extractor.py] ──► Postgres (question_pdf_texts / answer_pdf_texts)
                        │
                        ▼
  [streamlit viewer] ◄─┘

  logs/*.jsonl ──► Promtail ──► Loki ──► Grafana
```

## Services

| Service | Image | Description |
|---------|-------|-------------|
| `scraper` | `parliament-scraper` | Monitors parliament site for new questions; writes metadata to Postgres |
| `downloader` | `parliament-downloader` | Downloads PDF files to local cache (`pdfs/`) |
| `extractor` | `parliament-extractor` | Extracts text from PDFs (pdfminer first, EasyOCR GPU fallback for scanned pages) |
| `streamlit` | `parliament-viewer` | Web viewer for browsing and searching records |
| `postgres` | pgvector/pgvector:pg16 | Primary datastore |
| `loki` + `promtail` | Grafana stack | Log aggregation |
| `grafana` | grafana/grafana | Dashboards (disabled on server, use barad-dur-monitoring) |

## Environment variables

| Variable | Default | Used by |
|----------|---------|---------|
| `PG_DSN` | `host=localhost port=5433 dbname=parliament user=parliament password=parliament` | all Python services |
| `PDF_CACHE_DIR` | `pdfs` | downloader, extractor |
| `POLL_INTERVAL_S` | `3600` (scraper) / `60` (downloader, extractor) | all Python services |
| `LOOKBACK_DAYS` | `30` | scraper (monitoring mode) |
| `DATE_FROM` | `01/01/1995` | scraper (`RUN_ONCE` mode only) |
| `DATE_TO` | today | scraper (`RUN_ONCE` mode only) |
| `RUN_ONCE` | — | scraper: set to `1` for a full backfill pass then exit |

## Compose layout

All compose files live in `docker/`. All commands run from **repo root** using
`--project-directory .` so relative volume/config paths resolve correctly.

| File | Purpose |
|------|---------|
| `docker/compose.infra.yml` | postgres, pgadmin, loki, promtail, grafana |
| `docker/compose.app.yml` | scraper, downloader, extractor, streamlit |
| `docker/compose.server.yml` | server overrides (grafana off, loki ext net, streamlit bind-mount) |
| `docker/compose.prod.yml` | use registry images (disable local builds) |
| `docker/compose.override.yml` | local dev port remaps + streamlit live-reload |

## Run locally

```bash
# Start everything (builds images locally)
docker compose --project-directory . \
  -f docker/compose.infra.yml -f docker/compose.app.yml -f docker/compose.override.yml \
  up -d --build

# Tail logs
tail -f logs/scraper.jsonl logs/downloader.jsonl logs/extractor.jsonl

# Grafana: http://localhost:3001  (admin/admin)
# Streamlit: http://localhost:8501
# pgAdmin: http://localhost:5050  (admin@parliament.local / admin)
```

For a full backfill (one pass, 1995 → today):
```bash
docker compose --project-directory . \
  -f docker/compose.infra.yml -f docker/compose.app.yml \
  run --rm -e RUN_ONCE=1 scraper
```

## Deploy to server (barad-dur)

CI (GitHub Actions self-hosted runner) handles builds and deploys automatically on push to `main`/`master`. The runner **never cleans untracked files** (`clean: false`) so `pdfs/`, `data/`, and `logs/` are preserved.

Manual deploy (from `~/mscThesis`):
```bash
COMPOSE="docker compose --project-directory . \
  -f docker/compose.infra.yml -f docker/compose.app.yml \
  -f docker/compose.server.yml -f docker/compose.prod.yml"

$COMPOSE pull scraper downloader extractor streamlit
$COMPOSE up -d --no-build scraper downloader extractor streamlit
```

Start/update infra only (postgres, loki, promtail):
```bash
docker compose --project-directory . \
  -f docker/compose.infra.yml -f docker/compose.server.yml \
  up -d
```

## Monitoring (Loki / Grafana)

Grafana: `http://localhost:3001` (local) or via barad-dur-monitoring Grafana instance on the server.

Loki datasource is auto-provisioned. All Python services write structured JSONL logs to `logs/`; Promtail ships them to Loki with labels:

| Label | Values |
|-------|--------|
| `job` | `host-python-scraper` |
| `app` | `parliament-scraper` |
| `svc` | `scraper` \| `downloader` \| `extractor` |
| `level` | `DEBUG` \| `INFO` \| `WARNING` \| `ERROR` |

### LogQL queries

**All errors across services:**
```logql
{job="host-python-scraper", level="ERROR"}
```

**Per-service streams:**
```logql
{svc="scraper"}
{svc="downloader"}
{svc="extractor"}
```

**Scraper: HTTP 403 blocks:**
```logql
{svc="scraper"} |= "HTTP 403"
{svc="scraper"} |= "BLOCKED (403)"
```

**Scraper: crawl rate (results pages/sec):**
```logql
{svc="scraper"} |= "rate=" | regexp `rate=(?P<rate>[0-9.]+)p/s`
```

**PDF download failures:**
```logql
{svc="downloader"} |= "PDF download failed"
```

**PDF extraction failures:**
```logql
{svc="extractor"} |= "PDF extraction failed"
```

**OCR fallbacks (scanned PDFs):**
```logql
{svc="extractor"} |= "OCR fallback"
```

**Error rate over time:**
```logql
sum(rate({job="host-python-scraper", level="ERROR"}[5m]))
```

**Scraper cycle starts:**
```logql
{svc="scraper"} |= "=== Cycle"
```

### Other containers (not in Loki)

`postgres`, `parliament-streamlit`, `loki`, `grafana`, `promtail`, and infra containers
log to Docker stdout only — promtail does not scrape them. Use `docker logs` directly:

```bash
docker logs postgres --tail 50 -f
docker logs parliament-streamlit --tail 50 -f
docker logs loki --tail 50 -f
docker logs mscthesis-promtail-1 --tail 50 -f
docker logs grafana --tail 50 -f
```

To add a container to Loki, mount its log file into promtail's watched directory
(`/var/log/parliament-scraper/`) and add a `scrape_configs` entry in
`promtail/promtail-config.yml` with an explicit `svc` label.

### Postgres health (pgAdmin at `:5050`)

```sql
-- Records with extracted text
SELECT COUNT(*) FROM records WHERE question_pdf_texts IS NOT NULL OR answer_pdf_texts IS NOT NULL;

-- Pending extraction
SELECT COUNT(*) FROM records
WHERE blocked = FALSE
  AND (jsonb_array_length(coalesce(question_pdfs,'[]'::jsonb)) > 0 AND question_pdf_texts IS NULL
    OR jsonb_array_length(coalesce(answer_pdfs,'[]'::jsonb)) > 0 AND answer_pdf_texts IS NULL);

-- Blocked records
SELECT COUNT(*) FROM records WHERE blocked = TRUE;

-- Extraction errors
SELECT error_type, COUNT(*) FROM pdf_extraction_errors GROUP BY 1 ORDER BY 2 DESC;
```
