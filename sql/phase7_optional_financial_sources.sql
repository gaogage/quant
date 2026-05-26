-- Phase 7-FE optional financial/event data sources.
-- Non-destructive DDL for data sources that passed the small Tushare permission smoke.

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS public.data_sync_attempt (
    source VARCHAR(64) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    task_id VARCHAR(64) NULL,
    status VARCHAR(32) NOT NULL,
    row_count BIGINT NOT NULL DEFAULT 0,
    error_message TEXT NULL,
    attempted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source, symbol, start_date, end_date)
);

CREATE INDEX IF NOT EXISTS idx_data_sync_attempt_source_status
    ON public.data_sync_attempt (source, status, symbol);
CREATE INDEX IF NOT EXISTS idx_data_sync_attempt_task
    ON public.data_sync_attempt (task_id);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_data_sync_attempt_status'
    ) THEN
        ALTER TABLE public.data_sync_attempt
            ADD CONSTRAINT chk_data_sync_attempt_status CHECK (status IN ('completed', 'failed'));
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_data_sync_attempt_task'
    ) THEN
        ALTER TABLE public.data_sync_attempt
            ADD CONSTRAINT fk_data_sync_attempt_task
            FOREIGN KEY (task_id) REFERENCES public.data_sync_task(task_id)
            ON DELETE SET NULL;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS public.market_stock_cashflow (
    symbol VARCHAR(20) NOT NULL,
    ann_date DATE NOT NULL,
    f_ann_date DATE NULL,
    end_date DATE NOT NULL,
    available_at DATE NOT NULL,
    net_profit NUMERIC(24,4) NULL,
    n_cashflow_act NUMERIC(24,4) NULL,
    c_cash_equ_end_period NUMERIC(24,4) NULL,
    raw_payload JSONB NOT NULL,
    source VARCHAR(32) NOT NULL,
    data_version_id VARCHAR(64) NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, end_date, ann_date, available_at)
);

CREATE INDEX IF NOT EXISTS idx_market_stock_cashflow_symbol_date
    ON public.market_stock_cashflow (symbol, available_at DESC);
CREATE INDEX IF NOT EXISTS idx_market_stock_cashflow_ann_date
    ON public.market_stock_cashflow (ann_date DESC);
CREATE INDEX IF NOT EXISTS idx_market_stock_cashflow_end_date
    ON public.market_stock_cashflow (end_date DESC);

SELECT create_hypertable(
    'public.market_stock_cashflow',
    'available_at',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 month'
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_market_stock_cashflow_data_version'
    ) THEN
        ALTER TABLE public.market_stock_cashflow
            ADD CONSTRAINT fk_market_stock_cashflow_data_version
            FOREIGN KEY (data_version_id) REFERENCES public.data_version(data_version_id)
            ON DELETE SET NULL;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS public.market_stock_dividend (
    symbol VARCHAR(20) NOT NULL,
    end_date DATE NOT NULL,
    ann_date DATE NOT NULL,
    div_proc VARCHAR(32) NOT NULL,
    available_at DATE NOT NULL,
    cash_div NUMERIC(24,6) NULL,
    cash_div_tax NUMERIC(24,6) NULL,
    record_date DATE NULL,
    ex_date DATE NULL,
    pay_date DATE NULL,
    imp_ann_date DATE NULL,
    raw_payload JSONB NOT NULL,
    source VARCHAR(32) NOT NULL,
    data_version_id VARCHAR(64) NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, end_date, ann_date, div_proc, available_at)
);

CREATE INDEX IF NOT EXISTS idx_market_stock_dividend_symbol_date
    ON public.market_stock_dividend (symbol, available_at DESC);
CREATE INDEX IF NOT EXISTS idx_market_stock_dividend_ann_date
    ON public.market_stock_dividend (ann_date DESC);
CREATE INDEX IF NOT EXISTS idx_market_stock_dividend_ex_date
    ON public.market_stock_dividend (ex_date DESC);

SELECT create_hypertable(
    'public.market_stock_dividend',
    'available_at',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 month'
);

CREATE TABLE IF NOT EXISTS public.market_stock_repurchase (
    symbol VARCHAR(20) NOT NULL,
    ann_date DATE NOT NULL,
    end_date DATE NOT NULL,
    proc VARCHAR(32) NOT NULL,
    available_at DATE NOT NULL,
    exp_date DATE NULL,
    vol NUMERIC(24,4) NULL,
    amount NUMERIC(24,4) NULL,
    high_limit NUMERIC(18,6) NULL,
    low_limit NUMERIC(18,6) NULL,
    raw_payload JSONB NOT NULL,
    source VARCHAR(32) NOT NULL,
    data_version_id VARCHAR(64) NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, ann_date, end_date, proc, available_at)
);

CREATE INDEX IF NOT EXISTS idx_market_stock_repurchase_symbol_date
    ON public.market_stock_repurchase (symbol, available_at DESC);
CREATE INDEX IF NOT EXISTS idx_market_stock_repurchase_ann_date
    ON public.market_stock_repurchase (ann_date DESC);
CREATE INDEX IF NOT EXISTS idx_market_stock_repurchase_end_date
    ON public.market_stock_repurchase (end_date DESC);

SELECT create_hypertable(
    'public.market_stock_repurchase',
    'available_at',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 month'
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_market_stock_dividend_data_version'
    ) THEN
        ALTER TABLE public.market_stock_dividend
            ADD CONSTRAINT fk_market_stock_dividend_data_version
            FOREIGN KEY (data_version_id) REFERENCES public.data_version(data_version_id)
            ON DELETE SET NULL;
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_market_stock_repurchase_data_version'
    ) THEN
        ALTER TABLE public.market_stock_repurchase
            ADD CONSTRAINT fk_market_stock_repurchase_data_version
            FOREIGN KEY (data_version_id) REFERENCES public.data_version(data_version_id)
            ON DELETE SET NULL;
    END IF;
END $$;
