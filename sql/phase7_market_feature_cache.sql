-- Phase 7-ER persistent market feature cache.
--
-- This file is intentionally idempotent and non-destructive. It adds an
-- optional cache layer for expensive PIT-safe market features used during
-- automated OOS/WFA discovery. Strategy logic, train/OOS splitting,
-- robustness gates, and anti-overfit rules are unchanged.

SET statement_timeout = 0;

CREATE TABLE IF NOT EXISTS public.market_feature_cache_manifest (
    cache_key TEXT PRIMARY KEY,
    feature_kind TEXT NOT NULL CHECK (feature_kind IN ('return_history', 'average_amount_history', 'pit_average_amount_matrix', 'return_risk_feature_matrix', 'return_risk_stats_feature_matrix')),
    data_version_id TEXT NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    lookback_days INTEGER NOT NULL CHECK (lookback_days > 0),
    universe_hash TEXT NOT NULL,
    symbol_count INTEGER NOT NULL CHECK (symbol_count >= 0),
    row_count BIGINT NOT NULL DEFAULT 0 CHECK (row_count >= 0),
    status TEXT NOT NULL DEFAULT 'ready' CHECK (status IN ('building', 'ready', 'failed')),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE public.market_feature_cache_manifest
    DROP CONSTRAINT IF EXISTS market_feature_cache_manifest_feature_kind_check;

ALTER TABLE public.market_feature_cache_manifest
    ADD CONSTRAINT market_feature_cache_manifest_feature_kind_check
    CHECK (feature_kind IN ('return_history', 'average_amount_history', 'pit_average_amount_matrix', 'return_risk_feature_matrix', 'return_risk_stats_feature_matrix'));

CREATE TABLE IF NOT EXISTS public.market_feature_cache_symbol (
    cache_key TEXT NOT NULL REFERENCES public.market_feature_cache_manifest(cache_key) ON DELETE CASCADE,
    symbol TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (cache_key, symbol)
);

CREATE TABLE IF NOT EXISTS public.market_feature_cache_value (
    cache_key TEXT NOT NULL REFERENCES public.market_feature_cache_manifest(cache_key) ON DELETE CASCADE,
    symbol TEXT NOT NULL,
    trade_date DATE NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (cache_key, symbol, trade_date)
);

CREATE TABLE IF NOT EXISTS public.market_feature_cache_return_risk_matrix_row (
    cache_key TEXT NOT NULL REFERENCES public.market_feature_cache_manifest(cache_key) ON DELETE CASCADE,
    score_day DATE NOT NULL,
    symbol TEXT NOT NULL,
    returns DOUBLE PRECISION[] NOT NULL CHECK (array_position(returns, NULL) IS NULL),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (cache_key, score_day, symbol)
);

CREATE TABLE IF NOT EXISTS public.market_feature_cache_return_risk_stats_row (
    cache_key TEXT NOT NULL REFERENCES public.market_feature_cache_manifest(cache_key) ON DELETE CASCADE,
    score_day DATE NOT NULL,
    symbol TEXT NOT NULL,
    return_count BIGINT NOT NULL CHECK (return_count >= 0),
    total_return DOUBLE PRECISION NULL,
    sample_volatility DOUBLE PRECISION NULL,
    kelly_mean DOUBLE PRECISION NULL,
    kelly_population_variance DOUBLE PRECISION NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (cache_key, score_day, symbol)
);

CREATE TABLE IF NOT EXISTS public.market_feature_cache_return_risk_pairwise_row (
    cache_key TEXT NOT NULL REFERENCES public.market_feature_cache_manifest(cache_key) ON DELETE CASCADE,
    score_day DATE NOT NULL,
    left_symbol TEXT NOT NULL,
    right_symbol TEXT NOT NULL,
    correlation DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (left_symbol < right_symbol),
    CHECK (correlation BETWEEN -1.000000000001 AND 1.000000000001),
    PRIMARY KEY (cache_key, score_day, left_symbol, right_symbol)
);

CREATE INDEX IF NOT EXISTS idx_market_feature_cache_manifest_exact_ready
    ON public.market_feature_cache_manifest (
        feature_kind,
        data_version_id,
        start_date,
        end_date,
        lookback_days,
        universe_hash,
        symbol_count
    )
    WHERE status = 'ready';

CREATE INDEX IF NOT EXISTS idx_market_feature_cache_symbol_symbol
    ON public.market_feature_cache_symbol (symbol, cache_key);

CREATE INDEX IF NOT EXISTS idx_market_feature_cache_value_symbol_date
    ON public.market_feature_cache_value (symbol, trade_date);

CREATE INDEX IF NOT EXISTS idx_market_feature_cache_value_cache_date
    ON public.market_feature_cache_value (cache_key, trade_date);

CREATE INDEX IF NOT EXISTS idx_market_feature_cache_return_risk_matrix_symbol_day
    ON public.market_feature_cache_return_risk_matrix_row (symbol, score_day, cache_key);

CREATE INDEX IF NOT EXISTS idx_market_feature_cache_return_risk_stats_symbol_day
    ON public.market_feature_cache_return_risk_stats_row (symbol, score_day, cache_key);

CREATE INDEX IF NOT EXISTS idx_market_feature_cache_return_risk_pairwise_left_day
    ON public.market_feature_cache_return_risk_pairwise_row (left_symbol, score_day, cache_key);

CREATE INDEX IF NOT EXISTS idx_market_feature_cache_return_risk_pairwise_right_day
    ON public.market_feature_cache_return_risk_pairwise_row (right_symbol, score_day, cache_key);
