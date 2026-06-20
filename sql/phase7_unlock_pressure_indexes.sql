-- Phase 7 unlock-pressure backfill/readiness support indexes.
-- market_stock_share_float is a Timescale hypertable, so use plain
-- CREATE INDEX instead of CREATE INDEX CONCURRENTLY.
-- market_stock_daily_bar_adj is a view; its base table market_stock_daily_bar
-- already has (trade_date, symbol) coverage indexes in production.

CREATE INDEX IF NOT EXISTS idx_market_stock_share_float_unlock_pressure_pit
    ON public.market_stock_share_float (symbol, float_date, available_at);
