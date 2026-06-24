-- Phase 7 P3.22C security-level margin detail raw source schema.
-- This table is a PIT-gated raw source only. It does not define a trainable
-- factor, WFA sleeve, or v19 selection candidate until coverage, correlation,
-- P3.10 diagnostics, cost/capacity and strict OOS gates pass.

CREATE TABLE IF NOT EXISTS market_stock_margin_detail (
    symbol TEXT NOT NULL,
    trade_date DATE NOT NULL,
    name TEXT,
    rzye NUMERIC,
    rqye NUMERIC,
    rzmre NUMERIC,
    rqyl NUMERIC,
    rzche NUMERIC,
    rqchl NUMERIC,
    rqmcl NUMERIC,
    rzrqye NUMERIC,
    available_at DATE NOT NULL,
    source_published_at TIMESTAMPTZ,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    source TEXT NOT NULL DEFAULT 'tushare',
    data_version_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, trade_date),
    CONSTRAINT market_stock_margin_detail_available_at_check CHECK (available_at > trade_date),
    CONSTRAINT market_stock_margin_detail_core_nonnegative_check CHECK (
        (rzye IS NULL OR rzye >= 0)
        AND (rqye IS NULL OR rqye >= 0)
        AND (rzmre IS NULL OR rzmre >= 0)
        AND (rqyl IS NULL OR rqyl >= 0)
        AND (rqmcl IS NULL OR rqmcl >= 0)
        AND (rzrqye IS NULL OR rzrqye >= 0)
    )
);

ALTER TABLE IF EXISTS market_stock_margin_detail
    DROP CONSTRAINT IF EXISTS market_stock_margin_detail_nonnegative_check;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'market_stock_margin_detail_core_nonnegative_check'
          AND conrelid = 'market_stock_margin_detail'::regclass
    ) THEN
        ALTER TABLE market_stock_margin_detail
            ADD CONSTRAINT market_stock_margin_detail_core_nonnegative_check CHECK (
                (rzye IS NULL OR rzye >= 0)
                AND (rqye IS NULL OR rqye >= 0)
                AND (rzmre IS NULL OR rzmre >= 0)
                AND (rqyl IS NULL OR rqyl >= 0)
                AND (rqmcl IS NULL OR rqmcl >= 0)
                AND (rzrqye IS NULL OR rzrqye >= 0)
            ) NOT VALID;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_market_stock_margin_detail_available_at
    ON market_stock_margin_detail (available_at, trade_date);

CREATE INDEX IF NOT EXISTS idx_market_stock_margin_detail_symbol_available_at
    ON market_stock_margin_detail (symbol, available_at, trade_date);

CREATE INDEX IF NOT EXISTS idx_market_stock_margin_detail_trade_date
    ON market_stock_margin_detail (trade_date, symbol);
