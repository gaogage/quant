-- Phase 7 strict OOS/WFA discovery performance indexes.
--
-- This file is intentionally idempotent and non-destructive. It does not change
-- strategy logic, PIT rules, train/OOS splitting, or robustness gates.
--
-- Timescale hypertables do not support CREATE INDEX CONCURRENTLY. Run this in a
-- maintenance window for a populated research DB because it may take minutes
-- and can block writes while each index is created.

SET statement_timeout = 0;

CREATE INDEX IF NOT EXISTS idx_multi_factor_value_combo_date_score_symbol
    ON public.multi_factor_value (combo_name, version, trade_date DESC, raw_score, symbol)
    INCLUDE (available_at, normalized_score);

CREATE INDEX IF NOT EXISTS idx_market_stock_daily_date_symbol_cover
    ON public.market_stock_daily_bar (trade_date, symbol)
    INCLUDE (open, close, pre_close, amount);

CREATE INDEX IF NOT EXISTS idx_market_stock_daily_symbol_date_cover
    ON public.market_stock_daily_bar (symbol, trade_date)
    INCLUDE (open, close, pre_close, amount);
