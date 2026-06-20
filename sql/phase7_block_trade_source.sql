-- P3.15 block-trade raw source.
-- Data is announced after the trading session, so available_at is conservatively set
-- to trade_date + 1 calendar day for daily PIT feature use.

CREATE TABLE IF NOT EXISTS market_stock_block_trade (
    source_row_no INTEGER NOT NULL DEFAULT 0,
    ts_code VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    price NUMERIC(18,6) NOT NULL,
    vol NUMERIC(24,6) NOT NULL,
    amount NUMERIC(24,6) NOT NULL,
    buyer TEXT NOT NULL DEFAULT '',
    seller TEXT NOT NULL DEFAULT '',
    available_at DATE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (trade_date, source_row_no)
);

ALTER TABLE market_stock_block_trade
    ADD COLUMN IF NOT EXISTS source_row_no INTEGER;

WITH ranked AS (
    SELECT ctid,
           ROW_NUMBER() OVER (
               PARTITION BY trade_date
               ORDER BY ts_code, price, vol, amount, buyer, seller, created_at
           ) AS rn
    FROM market_stock_block_trade
    WHERE source_row_no IS NULL
)
UPDATE market_stock_block_trade target
SET source_row_no = ranked.rn
FROM ranked
WHERE target.ctid = ranked.ctid;

ALTER TABLE market_stock_block_trade
    ALTER COLUMN source_row_no SET DEFAULT 0,
    ALTER COLUMN source_row_no SET NOT NULL;

ALTER TABLE market_stock_block_trade
    DROP CONSTRAINT IF EXISTS market_stock_block_trade_pkey;

ALTER TABLE market_stock_block_trade
    ADD CONSTRAINT market_stock_block_trade_pkey PRIMARY KEY (trade_date, source_row_no);

CREATE INDEX IF NOT EXISTS idx_market_stock_block_trade_date_symbol
    ON market_stock_block_trade (trade_date DESC, ts_code);

CREATE INDEX IF NOT EXISTS idx_market_stock_block_trade_pit
    ON market_stock_block_trade (ts_code, available_at, trade_date DESC);
