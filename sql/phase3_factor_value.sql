-- Factor value persistence (Phase 3)
-- TimescaleDB hypertable for storing computed factor values

CREATE TABLE IF NOT EXISTS factor_value (
    factor_name   VARCHAR(64)     NOT NULL,
    symbol        VARCHAR(20)     NOT NULL,
    trade_date    DATE            NOT NULL,
    raw_value     DOUBLE PRECISION,
    std_value     DOUBLE PRECISION,
    method        VARCHAR(32),
    params        JSONB,
    computed_at   TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    PRIMARY KEY (factor_name, symbol, trade_date)
);

-- Convert to hypertable partitioned by trade_date
SELECT create_hypertable('factor_value', 'trade_date', if_not_exists => TRUE);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_factor_value_name_date
    ON factor_value (factor_name, trade_date DESC);

CREATE INDEX IF NOT EXISTS idx_factor_value_symbol_date
    ON factor_value (symbol, trade_date DESC);

COMMENT ON TABLE factor_value IS 'Computed factor values with optional standardization';
