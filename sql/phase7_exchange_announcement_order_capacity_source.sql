-- Phase 7 P3.24 exchange announcement order/capacity text raw source schema.
-- This DDL is a manual-review design artifact only. Apply only after the
-- source contract, PDF detail audit, and PIT publication policy are reviewed.
-- The table preserves raw announcement evidence. It is not a factor table and
-- does not admit P3.10, WFA, or v19 train selection by itself.

CREATE TABLE IF NOT EXISTS market_exchange_announcement_text_raw (
    vendor TEXT NOT NULL,
    vendor_endpoint TEXT NOT NULL,
    request_key TEXT NOT NULL,
    symbol TEXT NOT NULL,
    symbol_name TEXT,
    announcement_id TEXT NOT NULL,
    org_id TEXT NOT NULL DEFAULT '',
    announcement_category TEXT NOT NULL DEFAULT '',
    announcement_title TEXT NOT NULL,
    announcement_time DATE NOT NULL,
    source_published_at TEXT NOT NULL,
    source_published_at_ts TIMESTAMPTZ,
    source_published_date DATE,
    source_published_at_quality TEXT NOT NULL,
    available_at DATE NOT NULL,
    announcement_url TEXT NOT NULL,
    pdf_final_url TEXT,
    text_content TEXT,
    text_hash TEXT,
    text_hash_algorithm TEXT NOT NULL DEFAULT 'sha256',
    timestamp_candidates JSONB NOT NULL DEFAULT '[]'::jsonb,
    pdf_metadata_keys JSONB NOT NULL DEFAULT '[]'::jsonb,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    raw_payload_hash TEXT NOT NULL,
    parser_used TEXT,
    parser_version TEXT,
    parser_errors JSONB NOT NULL DEFAULT '[]'::jsonb,
    pdf_parse_status TEXT NOT NULL DEFAULT 'pending_manual_review',
    event_type TEXT,
    evidence_spans JSONB NOT NULL DEFAULT '[]'::jsonb,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    data_version_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (vendor, vendor_endpoint, announcement_id, symbol, raw_payload_hash),
    CONSTRAINT market_exchange_announcement_text_available_at_check
        CHECK (available_at >= announcement_time),
    CONSTRAINT market_exchange_announcement_text_quality_check
        CHECK (source_published_at_quality IN ('timestamp', 'date_only_next_session', 'missing')),
    CONSTRAINT market_exchange_announcement_text_hash_algo_check
        CHECK (text_hash_algorithm IN ('sha256')),
    CONSTRAINT market_exchange_announcement_text_timestamp_semantics_check
        CHECK (
            (source_published_at_quality = 'timestamp' AND source_published_at_ts IS NOT NULL)
            OR (source_published_at_quality = 'date_only_next_session' AND source_published_date IS NOT NULL)
            OR (source_published_at_quality = 'missing')
        ),
    CONSTRAINT market_exchange_announcement_text_event_type_check
        CHECK (
            event_type IS NULL
            OR event_type IN (
                'order_or_contract_signed',
                'capacity_expansion_or_commissioning',
                'product_price_adjustment',
                'major_supply_or_customer_agreement'
            )
        ),
    CONSTRAINT market_exchange_announcement_text_parse_status_check
        CHECK (
            pdf_parse_status IN (
                'ok',
                'pdf_parse_empty',
                'scanned_pdf_ocr_required',
                'error',
                'pending_manual_review'
            )
        )
);

CREATE INDEX IF NOT EXISTS idx_market_exchange_announcement_available_at
    ON market_exchange_announcement_text_raw (available_at, announcement_time);

CREATE INDEX IF NOT EXISTS idx_market_exchange_announcement_symbol_available_at
    ON market_exchange_announcement_text_raw (symbol, available_at, announcement_time);

CREATE INDEX IF NOT EXISTS idx_market_exchange_announcement_quality
    ON market_exchange_announcement_text_raw (source_published_at_quality, available_at);

CREATE INDEX IF NOT EXISTS idx_market_exchange_announcement_hash
    ON market_exchange_announcement_text_raw (text_hash, raw_payload_hash);

CREATE INDEX IF NOT EXISTS idx_market_exchange_announcement_parser_status
    ON market_exchange_announcement_text_raw (pdf_parse_status, parser_used);
