-- Add actual share-count fields from Tushare daily_basic.
-- These are required for PIT supply-shock research; using market cap / close as
-- a share proxy is too noisy because price moves dominate the inferred series.

ALTER TABLE public.market_stock_daily_basic
    ADD COLUMN IF NOT EXISTS total_share NUMERIC(24,6) NULL,
    ADD COLUMN IF NOT EXISTS float_share NUMERIC(24,6) NULL,
    ADD COLUMN IF NOT EXISTS free_share NUMERIC(24,6) NULL;
