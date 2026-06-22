CREATE EXTENSION IF NOT EXISTS vector;

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
    submitters          JSONB,
    ministries          JSONB,
    ministers           JSONB,
    question_pdfs       JSONB,
    answer_pdfs         JSONB,
    question_text       TEXT,
    answer_text         TEXT,
    question_pdf_texts  JSONB,   -- [{url, text, method}, ...] one entry per question PDF
    answer_pdf_texts    JSONB,   -- [{url, text, method}, ...] one entry per answer PDF
    pdf_extraction_method TEXT,  -- 'pdfminer' | 'ocr' | 'mixed' | null if not extracted
    blocked             BOOLEAN DEFAULT FALSE,
    block_reason        TEXT,
    detail_url          TEXT,
    raw_fields          JSONB,
    scraped_at          TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_records_type_name ON records (type_name);
CREATE INDEX IF NOT EXISTS idx_records_date      ON records (date);
CREATE INDEX IF NOT EXISTS idx_records_blocked   ON records (blocked);

-- Embeddings table (populated after scrape, during RAG phase)
CREATE TABLE IF NOT EXISTS embeddings (
    id          BIGSERIAL PRIMARY KEY,
    pcm_id      TEXT NOT NULL REFERENCES records(pcm_id) ON DELETE CASCADE,
    chunk_type  TEXT NOT NULL CHECK (chunk_type IN ('subject', 'question', 'answer')),
    text        TEXT NOT NULL,
    embedding   vector(1024),        -- multilingual-e5-large output dim
    UNIQUE (pcm_id, chunk_type)
);

CREATE INDEX IF NOT EXISTS idx_embeddings_pcm_id ON embeddings (pcm_id);

-- PDF extraction error log
CREATE TABLE IF NOT EXISTS pdf_extraction_errors (
    id          BIGSERIAL PRIMARY KEY,
    pcm_id      TEXT REFERENCES records(pcm_id) ON DELETE CASCADE,
    url         TEXT,
    kind        TEXT CHECK (kind IN ('question', 'answer')),
    error_type  TEXT,
    error_msg   TEXT,
    traceback   TEXT,
    created_at  TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pdf_errors_pcm_id ON pdf_extraction_errors (pcm_id);
CREATE INDEX IF NOT EXISTS idx_pdf_errors_created ON pdf_extraction_errors (created_at);
-- Created after embeddings are populated (HNSW is faster for large sets):
-- CREATE INDEX idx_embeddings_hnsw ON embeddings USING hnsw (embedding vector_cosine_ops);
