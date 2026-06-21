-- P3.18 industry membership raw source.
-- PIT rule: Tushare index_member provides effective interval dates but no
-- independent publication timestamp. Until a better publication source is
-- proven, entry availability is conservatively set to in_date and exit
-- availability is set to out_date when present. Factor SQL must treat this as
-- an interval membership source, not as a predictive event signal.

CREATE TABLE IF NOT EXISTS public.market_stock_industry_membership_pit (
    classification_source VARCHAR(16) NOT NULL,
    industry_level VARCHAR(8) NOT NULL,
    index_code VARCHAR(20) NOT NULL,
    index_name TEXT NOT NULL DEFAULT '',
    industry_code VARCHAR(32) NOT NULL DEFAULT '',
    industry_name TEXT NOT NULL DEFAULT '',
    parent_code VARCHAR(32) NOT NULL DEFAULT '',
    symbol VARCHAR(20) NOT NULL,
    symbol_name TEXT NOT NULL DEFAULT '',
    in_date DATE NOT NULL,
    out_date DATE NULL,
    available_at DATE NOT NULL,
    exit_available_at DATE NULL,
    is_new VARCHAR(8) NOT NULL DEFAULT '',
    raw_payload JSONB NOT NULL,
    source VARCHAR(32) NOT NULL,
    data_version_id VARCHAR(64) NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (classification_source, index_code, symbol, in_date)
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'market_stock_industry_membership_interval_chk'
    ) THEN
        ALTER TABLE public.market_stock_industry_membership_pit
            ADD CONSTRAINT market_stock_industry_membership_interval_chk
            CHECK (out_date IS NULL OR out_date >= in_date);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'market_stock_industry_membership_available_chk'
    ) THEN
        ALTER TABLE public.market_stock_industry_membership_pit
            ADD CONSTRAINT market_stock_industry_membership_available_chk
            CHECK (available_at >= in_date);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'market_stock_industry_membership_exit_available_chk'
    ) THEN
        ALTER TABLE public.market_stock_industry_membership_pit
            ADD CONSTRAINT market_stock_industry_membership_exit_available_chk
            CHECK (exit_available_at IS NULL OR out_date IS NOT NULL);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_market_stock_industry_membership_symbol_pit
    ON public.market_stock_industry_membership_pit
    (symbol, classification_source, industry_level, available_at, in_date, out_date);

CREATE INDEX IF NOT EXISTS idx_market_stock_industry_membership_index
    ON public.market_stock_industry_membership_pit
    (classification_source, industry_level, index_code, in_date);

CREATE INDEX IF NOT EXISTS idx_market_stock_industry_membership_available
    ON public.market_stock_industry_membership_pit (available_at);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_market_stock_industry_membership_data_version'
    ) THEN
        ALTER TABLE public.market_stock_industry_membership_pit
            ADD CONSTRAINT fk_market_stock_industry_membership_data_version
            FOREIGN KEY (data_version_id) REFERENCES public.data_version(data_version_id)
            ON DELETE SET NULL;
    END IF;
END $$;
