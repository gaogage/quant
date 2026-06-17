-- Phase 7 P3.7 financial quality change backfill support indexes.
-- These indexes accelerate PIT financial lookup without changing source data.

CREATE INDEX IF NOT EXISTS idx_mfi_symbol_ann_end_desc
    ON market_financial_indicator (ts_code, ann_date DESC, end_date DESC);

CREATE INDEX IF NOT EXISTS idx_mfi_symbol_end_ann_desc
    ON market_financial_indicator (ts_code, end_date, ann_date DESC);
