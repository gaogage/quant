-- Phase 7 P3.21B shareholder structure raw source schema.
-- Apply only after schema contract review. These are raw PIT tables only;
-- they do not define a trainable factor or a WFA sleeve.

CREATE TABLE IF NOT EXISTS market_stock_holder_number (
    symbol TEXT NOT NULL,
    ann_date DATE NOT NULL,
    end_date DATE NOT NULL,
    holder_num BIGINT,
    available_at DATE NOT NULL,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    source TEXT NOT NULL DEFAULT 'tushare',
    data_version_id TEXT,
    source_row_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, ann_date, end_date),
    CONSTRAINT market_stock_holder_number_available_at_check CHECK (available_at >= ann_date)
);

ALTER TABLE IF EXISTS market_stock_holder_number
    DROP CONSTRAINT IF EXISTS market_stock_holder_number_period_pit_check;

CREATE TABLE IF NOT EXISTS market_stock_top10_holders (
    symbol TEXT NOT NULL,
    ann_date DATE NOT NULL,
    end_date DATE NOT NULL,
    holder_name TEXT NOT NULL DEFAULT '',
    hold_amount NUMERIC,
    hold_ratio NUMERIC,
    hold_float_ratio NUMERIC,
    hold_change NUMERIC,
    holder_type TEXT,
    available_at DATE NOT NULL,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    source TEXT NOT NULL DEFAULT 'tushare',
    data_version_id TEXT,
    source_row_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, ann_date, end_date, source_row_hash),
    CONSTRAINT market_stock_top10_holders_available_at_check CHECK (available_at >= ann_date)
);

ALTER TABLE IF EXISTS market_stock_top10_holders
    DROP CONSTRAINT IF EXISTS market_stock_top10_holders_period_pit_check;

CREATE TABLE IF NOT EXISTS market_stock_top10_float_holders (
    symbol TEXT NOT NULL,
    ann_date DATE NOT NULL,
    end_date DATE NOT NULL,
    holder_name TEXT NOT NULL DEFAULT '',
    hold_amount NUMERIC,
    hold_ratio NUMERIC,
    hold_float_ratio NUMERIC,
    hold_change NUMERIC,
    holder_type TEXT,
    available_at DATE NOT NULL,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    source TEXT NOT NULL DEFAULT 'tushare',
    data_version_id TEXT,
    source_row_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, ann_date, end_date, source_row_hash),
    CONSTRAINT market_stock_top10_float_holders_available_at_check CHECK (available_at >= ann_date)
);

ALTER TABLE IF EXISTS market_stock_top10_float_holders
    DROP CONSTRAINT IF EXISTS market_stock_top10_float_holders_period_pit_check;

CREATE TABLE IF NOT EXISTS market_stock_holder_trade (
    symbol TEXT NOT NULL,
    ann_date DATE NOT NULL,
    holder_name TEXT NOT NULL DEFAULT '',
    holder_type TEXT,
    in_de TEXT,
    change_vol NUMERIC,
    change_ratio NUMERIC,
    after_share NUMERIC,
    after_ratio NUMERIC,
    avg_price NUMERIC,
    total_share NUMERIC,
    begin_date DATE,
    close_date DATE,
    available_at DATE NOT NULL,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    source TEXT NOT NULL DEFAULT 'tushare',
    data_version_id TEXT,
    source_row_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, ann_date, source_row_hash),
    CONSTRAINT market_stock_holder_trade_available_at_check CHECK (available_at >= ann_date),
    CONSTRAINT market_stock_holder_trade_interval_check CHECK (
        begin_date IS NULL OR close_date IS NULL OR close_date >= begin_date
    )
);

CREATE INDEX IF NOT EXISTS idx_market_stock_holder_number_available_at
    ON market_stock_holder_number (available_at, ann_date, end_date);

CREATE INDEX IF NOT EXISTS idx_market_stock_holder_number_symbol_available_at
    ON market_stock_holder_number (symbol, available_at, end_date);

CREATE INDEX IF NOT EXISTS idx_market_stock_top10_holders_available_at
    ON market_stock_top10_holders (available_at, ann_date, end_date);

CREATE INDEX IF NOT EXISTS idx_market_stock_top10_holders_symbol_available_at
    ON market_stock_top10_holders (symbol, available_at, end_date);

CREATE INDEX IF NOT EXISTS idx_market_stock_top10_float_holders_available_at
    ON market_stock_top10_float_holders (available_at, ann_date, end_date);

CREATE INDEX IF NOT EXISTS idx_market_stock_top10_float_holders_symbol_available_at
    ON market_stock_top10_float_holders (symbol, available_at, end_date);

CREATE INDEX IF NOT EXISTS idx_market_stock_holder_trade_available_at
    ON market_stock_holder_trade (available_at, ann_date);

CREATE INDEX IF NOT EXISTS idx_market_stock_holder_trade_symbol_available_at
    ON market_stock_holder_trade (symbol, available_at);
