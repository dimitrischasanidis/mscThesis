ALTER TABLE records
    ADD COLUMN IF NOT EXISTS all_pdfs_cached BOOLEAN DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS idx_records_all_pdfs_cached ON records (all_pdfs_cached);
