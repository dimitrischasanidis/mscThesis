-- Filter & pipeline-stats indexes
-- pg_trgm for ILIKE %...% on text search fields
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_records_subject_trgm
    ON records USING gin (subject gin_trgm_ops);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_records_question_text_trgm
    ON records USING gin (question_text gin_trgm_ops);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_records_answer_text_trgm
    ON records USING gin (answer_text gin_trgm_ops);

-- has_question_pdfs / has_answer_pdfs checkboxes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_records_has_question_pdfs
    ON records (pcm_id)
    WHERE question_pdfs IS NOT NULL
      AND jsonb_array_length(question_pdfs) > 0;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_records_has_answer_pdfs
    ON records (pcm_id)
    WHERE answer_pdfs IS NOT NULL
      AND jsonb_array_length(answer_pdfs) > 0;

-- "Text has been extracted" checkbox
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_records_extracted
    ON records (pcm_id)
    WHERE question_pdf_texts IS NOT NULL
       OR answer_pdf_texts IS NOT NULL;

-- extraction_method multiselect
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_records_extraction_method
    ON records (pdf_extraction_method);

-- pipeline stats: pending extraction count
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_records_pending_extraction
    ON records (blocked)
    WHERE question_pdf_texts IS NULL
      AND answer_pdf_texts IS NULL;

-- pipeline stats: extracted count
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_records_has_method
    ON records (blocked)
    WHERE pdf_extraction_method IS NOT NULL;
