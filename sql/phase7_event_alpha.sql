-- Phase 7-Y 事件类 Alpha 数据底座
-- 基于 docs/projects/quant/tasks/quant/05-表结构设计.md 与
-- docs/projects/quant/tasks/quant/sql/001_initial_schema.sql 的正式口径。

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS public.market_stock_forecast (
    symbol VARCHAR(20) NOT NULL,
    ann_date DATE NOT NULL,
    end_date DATE NOT NULL,
    forecast_type VARCHAR(32) NOT NULL,
    p_change_min NUMERIC(18,6) NULL,
    p_change_max NUMERIC(18,6) NULL,
    net_profit_min NUMERIC(24,4) NULL,
    net_profit_max NUMERIC(24,4) NULL,
    first_ann_date DATE NOT NULL,
    available_at DATE NOT NULL,
    summary TEXT NULL,
    change_reason TEXT NULL,
    raw_payload JSONB NOT NULL,
    source VARCHAR(32) NOT NULL,
    data_version_id VARCHAR(64) NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, ann_date, end_date, forecast_type, first_ann_date, available_at)
);

CREATE INDEX IF NOT EXISTS idx_market_stock_forecast_symbol_date
    ON public.market_stock_forecast (symbol, available_at DESC);
CREATE INDEX IF NOT EXISTS idx_market_stock_forecast_ann_date
    ON public.market_stock_forecast (ann_date DESC);
CREATE INDEX IF NOT EXISTS idx_market_stock_forecast_end_date
    ON public.market_stock_forecast (end_date DESC);

SELECT create_hypertable(
    'public.market_stock_forecast',
    'available_at',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 month'
);

CREATE TABLE IF NOT EXISTS public.market_stock_express (
    symbol VARCHAR(20) NOT NULL,
    ann_date DATE NOT NULL,
    end_date DATE NOT NULL,
    revenue NUMERIC(24,4) NULL,
    n_income NUMERIC(24,4) NULL,
    yoy_sales NUMERIC(18,6) NULL,
    yoy_dedu_np NUMERIC(18,6) NULL,
    diluted_eps NUMERIC(18,6) NULL,
    diluted_roe NUMERIC(18,6) NULL,
    is_audit INTEGER NULL,
    available_at DATE NOT NULL,
    perf_summary TEXT NULL,
    remark TEXT NULL,
    raw_payload JSONB NOT NULL,
    source VARCHAR(32) NOT NULL,
    data_version_id VARCHAR(64) NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, ann_date, end_date, available_at)
);

CREATE INDEX IF NOT EXISTS idx_market_stock_express_symbol_date
    ON public.market_stock_express (symbol, available_at DESC);
CREATE INDEX IF NOT EXISTS idx_market_stock_express_ann_date
    ON public.market_stock_express (ann_date DESC);
CREATE INDEX IF NOT EXISTS idx_market_stock_express_end_date
    ON public.market_stock_express (end_date DESC);

SELECT create_hypertable(
    'public.market_stock_express',
    'available_at',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 month'
);

CREATE TABLE IF NOT EXISTS public.market_stock_disclosure_date (
    symbol VARCHAR(20) NOT NULL,
    end_date DATE NOT NULL,
    ann_date DATE NOT NULL,
    pre_date DATE NULL,
    actual_date DATE NULL,
    modify_date DATE NULL,
    available_at DATE NOT NULL,
    raw_payload JSONB NOT NULL,
    source VARCHAR(32) NOT NULL,
    data_version_id VARCHAR(64) NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, end_date, available_at)
);

CREATE INDEX IF NOT EXISTS idx_market_stock_disclosure_date_symbol_date
    ON public.market_stock_disclosure_date (symbol, available_at DESC);
CREATE INDEX IF NOT EXISTS idx_market_stock_disclosure_date_end_date
    ON public.market_stock_disclosure_date (end_date DESC);
CREATE INDEX IF NOT EXISTS idx_market_stock_disclosure_date_ann_date
    ON public.market_stock_disclosure_date (ann_date DESC);

SELECT create_hypertable(
    'public.market_stock_disclosure_date',
    'available_at',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 month'
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_market_stock_forecast_data_version'
    ) THEN
        ALTER TABLE public.market_stock_forecast
            ADD CONSTRAINT fk_market_stock_forecast_data_version
            FOREIGN KEY (data_version_id) REFERENCES public.data_version(data_version_id)
            ON DELETE SET NULL;
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_market_stock_express_data_version'
    ) THEN
        ALTER TABLE public.market_stock_express
            ADD CONSTRAINT fk_market_stock_express_data_version
            FOREIGN KEY (data_version_id) REFERENCES public.data_version(data_version_id)
            ON DELETE SET NULL;
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_market_stock_disclosure_date_data_version'
    ) THEN
        ALTER TABLE public.market_stock_disclosure_date
            ADD CONSTRAINT fk_market_stock_disclosure_date_data_version
            FOREIGN KEY (data_version_id) REFERENCES public.data_version(data_version_id)
            ON DELETE SET NULL;
    END IF;
END $$;
