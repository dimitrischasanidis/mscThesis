-- Migration: add pdf_extraction_method column to existing records table
ALTER TABLE records
    ADD COLUMN IF NOT EXISTS pdf_extraction_method TEXT
    CHECK (pdf_extraction_method IN ('pdfminer', 'ocr', 'mixed'));
