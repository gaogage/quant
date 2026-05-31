-- Phase 8: 沪深港通北向资金 (North-bound capital flow)
-- Tushare moneyflow_hsgt endpoint

BEGIN;

CREATE TABLE IF NOT EXISTS public.market_moneyflow_hsgt (
    trade_date        DATE NOT NULL,
    ggt_type          VARCHAR(8) NOT NULL,   -- 'N'=北向(沪股通+深股通), 'S'=南向(港股通)
    north_flow        NUMERIC(18,4),          -- 北向资金净流入(亿元)
    south_flow        NUMERIC(18,4),          -- 南向资金净流入(亿元)
    north_balance     NUMERIC(18,4),          -- 北向资金累计余额
    south_balance     NUMERIC(18,4),          -- 南向资金累计余额
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (trade_date, ggt_type)
);

-- Factor: daily north-bound net flow (signal for smart money direction)
CREATE TABLE IF NOT EXISTS public.factor_value_north_flow_daily (
    factor_code       VARCHAR(128) NOT NULL DEFAULT 'north_flow_daily_std',
    trade_date        DATE NOT NULL,
    value             DOUBLE PRECISION,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (factor_code, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_moneyflow_hsgt_date ON market_moneyflow_hsgt(trade_date DESC);

COMMIT;
