-- Phase 7 P3.14 supply-demand source: restricted-share unlock announcements.
-- PIT rule: available_at is the announcement date (ann_date). float_date may be
-- a future event date, but can only be used after ann_date is visible.

CREATE TABLE IF NOT EXISTS public.market_stock_share_float (
    symbol VARCHAR(20) NOT NULL,
    ann_date DATE NOT NULL,
    float_date DATE NOT NULL,
    available_at DATE NOT NULL,
    float_share NUMERIC(24,4) NULL,
    float_ratio NUMERIC(18,6) NULL,
    holder_name TEXT NOT NULL DEFAULT '',
    share_type VARCHAR(128) NOT NULL DEFAULT '',
    raw_payload JSONB NOT NULL,
    source VARCHAR(32) NOT NULL,
    data_version_id VARCHAR(64) NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, ann_date, float_date, holder_name, share_type, available_at)
);

CREATE INDEX IF NOT EXISTS idx_market_stock_share_float_symbol_date
    ON public.market_stock_share_float (symbol, available_at DESC);
CREATE INDEX IF NOT EXISTS idx_market_stock_share_float_ann_date
    ON public.market_stock_share_float (ann_date DESC);
CREATE INDEX IF NOT EXISTS idx_market_stock_share_float_float_date
    ON public.market_stock_share_float (float_date DESC);

SELECT create_hypertable(
    'public.market_stock_share_float',
    'available_at',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 month'
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_market_stock_share_float_data_version'
    ) THEN
        ALTER TABLE public.market_stock_share_float
            ADD CONSTRAINT fk_market_stock_share_float_data_version
            FOREIGN KEY (data_version_id) REFERENCES public.data_version(data_version_id)
            ON DELETE SET NULL;
    END IF;
END $$;
