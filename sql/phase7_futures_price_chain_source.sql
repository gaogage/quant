-- Phase 7 P3.19J futures price-chain raw source schema.
-- This DDL is a design artifact first. Apply only after the schema contract
-- and PIT publication policy are reviewed.

CREATE TABLE IF NOT EXISTS market_futures_daily (
    ts_code TEXT NOT NULL,
    trade_date DATE NOT NULL,
    pre_close NUMERIC,
    pre_settle NUMERIC,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    settle NUMERIC,
    change1 NUMERIC,
    change2 NUMERIC,
    vol NUMERIC,
    amount NUMERIC,
    oi NUMERIC,
    oi_chg NUMERIC,
    delv_settle NUMERIC,
    available_at DATE NOT NULL,
    source_published_at TIMESTAMPTZ,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    source TEXT NOT NULL DEFAULT 'tushare',
    data_version_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (ts_code, trade_date),
    CONSTRAINT market_futures_daily_pit_available_at_check CHECK (available_at >= trade_date)
);

CREATE TABLE IF NOT EXISTS market_futures_warehouse_receipt (
    trade_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    exchange TEXT NOT NULL DEFAULT '',
    fut_name TEXT,
    warehouse TEXT NOT NULL DEFAULT '',
    wh_id TEXT,
    pre_vol NUMERIC,
    vol NUMERIC,
    vol_chg NUMERIC,
    area TEXT,
    year TEXT,
    grade TEXT,
    brand TEXT,
    place TEXT,
    pd NUMERIC,
    is_ct TEXT,
    unit TEXT,
    available_at DATE NOT NULL,
    source_published_at TIMESTAMPTZ,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    source TEXT NOT NULL DEFAULT 'tushare',
    data_version_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (trade_date, symbol, exchange, warehouse),
    CONSTRAINT market_futures_wsr_pit_available_at_check CHECK (available_at >= trade_date)
);

CREATE TABLE IF NOT EXISTS market_futures_holding_rank (
    trade_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    exchange TEXT NOT NULL DEFAULT '',
    broker TEXT NOT NULL DEFAULT '',
    vol NUMERIC,
    vol_chg NUMERIC,
    long_hld NUMERIC,
    long_chg NUMERIC,
    short_hld NUMERIC,
    short_chg NUMERIC,
    available_at DATE NOT NULL,
    source_published_at TIMESTAMPTZ,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    source TEXT NOT NULL DEFAULT 'tushare',
    data_version_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (trade_date, symbol, exchange, broker),
    CONSTRAINT market_futures_holding_pit_available_at_check CHECK (available_at >= trade_date)
);

CREATE TABLE IF NOT EXISTS market_futures_product_exposure_mapping_pit (
    product_symbol TEXT NOT NULL,
    exposure_type TEXT NOT NULL,
    exposure_code TEXT NOT NULL,
    direction SMALLINT NOT NULL,
    weight NUMERIC NOT NULL,
    valid_from DATE NOT NULL,
    valid_to DATE,
    available_at DATE NOT NULL,
    source TEXT NOT NULL,
    mapping_version TEXT NOT NULL,
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (product_symbol, exposure_type, exposure_code, valid_from, mapping_version),
    CONSTRAINT market_futures_product_exposure_direction_check CHECK (direction IN (-1, 1)),
    CONSTRAINT market_futures_product_exposure_weight_check CHECK (weight > 0 AND weight <= 1),
    CONSTRAINT market_futures_product_exposure_type_check CHECK (
        exposure_type IN ('sw_industry', 'stock_symbol')
    ),
    CONSTRAINT market_futures_product_exposure_interval_check CHECK (
        valid_to IS NULL OR valid_to >= valid_from
    )
);

CREATE INDEX IF NOT EXISTS idx_market_futures_daily_available_at
    ON market_futures_daily (available_at, trade_date);

CREATE INDEX IF NOT EXISTS idx_market_futures_wsr_available_at
    ON market_futures_warehouse_receipt (available_at, trade_date);

CREATE INDEX IF NOT EXISTS idx_market_futures_holding_available_at
    ON market_futures_holding_rank (available_at, trade_date);

CREATE INDEX IF NOT EXISTS idx_market_futures_exposure_available_at
    ON market_futures_product_exposure_mapping_pit (available_at, valid_from, valid_to);

ALTER TABLE market_futures_daily
    ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'tushare',
    ADD COLUMN IF NOT EXISTS data_version_id TEXT;

ALTER TABLE market_futures_warehouse_receipt
    ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'tushare',
    ADD COLUMN IF NOT EXISTS data_version_id TEXT;

ALTER TABLE market_futures_holding_rank
    ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'tushare',
    ADD COLUMN IF NOT EXISTS data_version_id TEXT;
