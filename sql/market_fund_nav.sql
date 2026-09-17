-- 基金净值表(ETF 溢价门禁数据源, 2026-09-17 一期)
-- 溢价率 = T 日收盘价 / 最近可得 unit_nav - 1
-- QDII 净值 T+1 上午公布: T 日 23:30 信号生成时最近可得净值通常为 T-1 日(隔夜美股误差 ±2-3%,
-- 门禁阈值 5% 已含余量, 见 docs/projects/quant/knowledge/system/ ETF溢价停牌门禁设计)
CREATE TABLE IF NOT EXISTS market_fund_nav (
    symbol      VARCHAR(20)   NOT NULL,
    nav_date    DATE          NOT NULL,
    ann_date    DATE,
    unit_nav    NUMERIC(16,6) NOT NULL,
    accum_nav   NUMERIC(16,6),
    adj_nav     NUMERIC(16,6),
    source      VARCHAR(32)   NOT NULL DEFAULT 'tushare',
    created_at  TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT market_fund_nav_pkey PRIMARY KEY (symbol, nav_date)
);

CREATE INDEX IF NOT EXISTS idx_market_fund_nav_symbol_latest
    ON market_fund_nav (symbol, nav_date DESC);
