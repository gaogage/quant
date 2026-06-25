-- Phase 7 P3.23C AkShare/multi-vendor analyst revision raw source schema.
-- Apply only after P3.23B history replay audit passes and schema review accepts
-- the PIT policy. This is a raw evidence table only. It does not define a
-- trainable factor, WFA sleeve, or v19 selection candidate.

CREATE TABLE IF NOT EXISTS market_vendor_analyst_revision_raw (
    vendor TEXT NOT NULL DEFAULT 'akshare',
    vendor_source TEXT NOT NULL DEFAULT 'akshare',
    vendor_endpoint TEXT NOT NULL DEFAULT 'stock_rank_forecast_cninfo',
    request_key TEXT NOT NULL,
    symbol TEXT NOT NULL,
    symbol_name TEXT,
    publication_date DATE NOT NULL,
    source_published_at TIMESTAMPTZ NOT NULL,
    available_at DATE NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    institution_name TEXT,
    analyst_name TEXT,
    rating_current TEXT,
    rating_previous TEXT,
    rating_change TEXT,
    is_first_rating TEXT,
    target_price_min NUMERIC,
    target_price_max NUMERIC,
    report_title TEXT,
    report_url TEXT,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    raw_payload_hash TEXT NOT NULL,
    data_version_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (vendor, vendor_endpoint, request_key, symbol, publication_date, raw_payload_hash),
    CONSTRAINT market_vendor_analyst_revision_available_at_check CHECK (available_at >= publication_date),
    CONSTRAINT market_vendor_analyst_revision_endpoint_check CHECK (
        vendor_endpoint IN ('stock_rank_forecast_cninfo', 'stock_research_report_em')
    )
);

CREATE INDEX IF NOT EXISTS idx_market_vendor_analyst_revision_available_at
    ON market_vendor_analyst_revision_raw (available_at, publication_date);

CREATE INDEX IF NOT EXISTS idx_market_vendor_analyst_revision_symbol_available_at
    ON market_vendor_analyst_revision_raw (symbol, available_at, publication_date);

CREATE INDEX IF NOT EXISTS idx_market_vendor_analyst_revision_publication_date
    ON market_vendor_analyst_revision_raw (publication_date, vendor_endpoint);

CREATE INDEX IF NOT EXISTS idx_market_vendor_analyst_revision_hash
    ON market_vendor_analyst_revision_raw (raw_payload_hash);
