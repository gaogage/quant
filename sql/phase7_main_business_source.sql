-- P3.19E main-business raw source.
-- Tushare fina_mainbz/fina_mainbz_vip has report-period segment values but no
-- native announcement date. available_at must be joined from financial
-- statement announcement/disclosure data before a row is persisted.

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS public.market_stock_main_business (
    symbol VARCHAR(20) NOT NULL,
    end_date DATE NOT NULL,
    available_at DATE NOT NULL,
    business_type VARCHAR(4) NOT NULL,
    bz_item TEXT NOT NULL DEFAULT '',
    bz_code VARCHAR(16) NOT NULL DEFAULT '',
    bz_sales NUMERIC(24,4) NULL,
    bz_profit NUMERIC(24,4) NULL,
    bz_cost NUMERIC(24,4) NULL,
    curr_type VARCHAR(16) NOT NULL DEFAULT '',
    update_flag VARCHAR(8) NOT NULL DEFAULT '',
    source_row_hash VARCHAR(64) NOT NULL,
    raw_payload JSONB NOT NULL,
    source VARCHAR(32) NOT NULL,
    data_version_id VARCHAR(64) NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, end_date, business_type, source_row_hash, available_at)
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'market_stock_main_business_available_chk'
    ) THEN
        ALTER TABLE public.market_stock_main_business
            ADD CONSTRAINT market_stock_main_business_available_chk
            CHECK (available_at >= end_date);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_market_stock_main_business_symbol_pit
    ON public.market_stock_main_business (symbol, available_at DESC, end_date DESC);

CREATE INDEX IF NOT EXISTS idx_market_stock_main_business_period
    ON public.market_stock_main_business (end_date DESC, business_type);

CREATE INDEX IF NOT EXISTS idx_market_stock_main_business_available
    ON public.market_stock_main_business (available_at DESC);

SELECT create_hypertable(
    'public.market_stock_main_business',
    'available_at',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 month'
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_market_stock_main_business_data_version'
    ) THEN
        ALTER TABLE public.market_stock_main_business
            ADD CONSTRAINT fk_market_stock_main_business_data_version
            FOREIGN KEY (data_version_id) REFERENCES public.data_version(data_version_id)
            ON DELETE SET NULL;
    END IF;
END $$;
