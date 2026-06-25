-- Un-poison records whose text arrays contain download-failure entries
-- (entries with no "method" key were written by the extractor when it
-- encountered a .pdf.failed marker — prior to the fix in extractor.py).
-- Resets those sides to NULL so the extractor re-queues them.
-- Idempotent: re-running is a no-op once all poison entries are cleared.

UPDATE records
SET question_pdf_texts = NULL
WHERE question_pdf_texts IS NOT NULL
  AND EXISTS (
    SELECT 1 FROM jsonb_array_elements(question_pdf_texts) e
    WHERE NOT (e ? 'method')
  );

UPDATE records
SET answer_pdf_texts = NULL
WHERE answer_pdf_texts IS NOT NULL
  AND EXISTS (
    SELECT 1 FROM jsonb_array_elements(answer_pdf_texts) e
    WHERE NOT (e ? 'method')
  );

-- Clear extraction method where both sides are now NULL
UPDATE records
SET pdf_extraction_method = NULL
WHERE question_pdf_texts IS NULL
  AND answer_pdf_texts IS NULL
  AND pdf_extraction_method IS NOT NULL;
