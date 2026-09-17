-- 基金分红表(ETF 公司行动权威定性数据, 2026-09-17 复权体系 P0-2)
-- 与 fund_adj/market_adjustment_factor 因子跳变组合成 ETF 权威判定法:
--   因子跳变日 + fund_div 有 ex_date 匹配的分红记录 → 分红(div_cash 入现金)
--   因子跳变日 + 无分红记录 → 份额拆分/折算(因子比 = 拆分比例, 调份额)
CREATE TABLE IF NOT EXISTS market_fund_div (
    symbol       VARCHAR(20)   NOT NULL,
    ann_date     DATE,
    ex_date      DATE          NOT NULL,
    div_proc     VARCHAR(32)   NOT NULL DEFAULT '实施',
    record_date  DATE,
    pay_date     DATE,
    div_cash     NUMERIC(16,6) NOT NULL,
    available_at DATE          NOT NULL,
    source       VARCHAR(32)   NOT NULL DEFAULT 'tushare',
    created_at   TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT market_fund_div_pkey PRIMARY KEY (symbol, ex_date, div_proc)
);

CREATE INDEX IF NOT EXISTS idx_market_fund_div_symbol_ex ON market_fund_div (symbol, ex_date);
