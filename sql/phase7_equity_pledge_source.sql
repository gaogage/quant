-- Phase 7 P3.20B equity pledge pressure raw source schema.
-- This DDL is a design artifact first. Apply only after the schema contract
-- and PIT available_at policy are reviewed.

CREATE TABLE IF NOT EXISTS market_stock_pledge_stat (
    symbol TEXT NOT NULL,
    end_date DATE NOT NULL,
    pledge_count INTEGER,
    unrest_pledge NUMERIC,
    rest_pledge NUMERIC,
    total_share NUMERIC,
    pledge_ratio NUMERIC,
    available_at DATE NOT NULL,
    source_published_at TIMESTAMPTZ,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    source TEXT NOT NULL DEFAULT 'tushare',
    data_version_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, end_date),
    CONSTRAINT market_stock_pledge_stat_available_at_check CHECK (available_at >= end_date)
);

CREATE TABLE IF NOT EXISTS market_stock_pledge_detail (
    symbol TEXT NOT NULL,
    ann_date DATE NOT NULL,
    holder_name TEXT NOT NULL DEFAULT '',
    pledge_amount NUMERIC NOT NULL DEFAULT 0,
    pledge_start_date DATE,
    pledge_end_date DATE,
    is_release TEXT,
    release_date DATE,
    pledgor TEXT NOT NULL DEFAULT '',
    holding_amount NUMERIC,
    pledged_amount NUMERIC,
    p_total_ratio NUMERIC,
    h_total_ratio NUMERIC,
    is_buyback TEXT,
    available_at DATE NOT NULL,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    source TEXT NOT NULL DEFAULT 'tushare',
    data_version_id TEXT,
    source_row_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, ann_date, source_row_hash),
    CONSTRAINT market_stock_pledge_detail_available_at_check CHECK (available_at >= ann_date)
);

CREATE INDEX IF NOT EXISTS idx_market_stock_pledge_stat_available_at
    ON market_stock_pledge_stat (available_at, end_date);

CREATE INDEX IF NOT EXISTS idx_market_stock_pledge_stat_symbol_available_at
    ON market_stock_pledge_stat (symbol, available_at);

CREATE INDEX IF NOT EXISTS idx_market_stock_pledge_detail_available_at
    ON market_stock_pledge_detail (available_at, ann_date);

CREATE INDEX IF NOT EXISTS idx_market_stock_pledge_detail_symbol_available_at
    ON market_stock_pledge_detail (symbol, available_at);
