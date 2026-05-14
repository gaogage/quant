-- PROTOTYPE ONLY: official Phase 3-C schema lives in docs/projects/quant/tasks/quant/sql/001_initial_schema.sql
-- Phase 3: 财务数据表（精简版 — 财务报表 + 财务指标）
-- 设计原则：EAV 风格，单表存所有财务字段，避免每种报表一张表

-- 财务报表数据（利润表 / 资产负债表 / 现金流量表）
CREATE TABLE IF NOT EXISTS market_financial_statement (
    id              BIGSERIAL PRIMARY KEY,
    ts_code         VARCHAR(16) NOT NULL,
    ann_date        DATE NOT NULL,          -- 公告日期
    end_date        DATE NOT NULL,          -- 报告期截止日期
    statement_type  VARCHAR(16) NOT NULL,   -- income / balance / cashflow
    field_name      VARCHAR(64) NOT NULL,   -- 字段名 (e.g. total_revenue, total_assets)
    field_value     NUMERIC(24,4),          -- 字段值（单位：元）
    report_type     VARCHAR(4) DEFAULT '1', -- 1=合并 2=母公司
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (ts_code, end_date, statement_type, field_name, report_type)
);

CREATE INDEX idx_fin_stmt_code ON market_financial_statement(ts_code);
CREATE INDEX idx_fin_stmt_end_date ON market_financial_statement(end_date);
CREATE INDEX idx_fin_stmt_type ON market_financial_statement(statement_type);

-- 财务指标（派生指标：EPS, ROE, ROA, 毛利率等）
CREATE TABLE IF NOT EXISTS market_financial_indicator (
    ts_code         VARCHAR(16) NOT NULL,
    ann_date        DATE NOT NULL,
    end_date        DATE NOT NULL,
    eps             NUMERIC(18,6),
    roe             NUMERIC(18,6),
    roa             NUMERIC(18,6),
    gross_margin    NUMERIC(18,6),          -- 毛利率
    netprofit_margin NUMERIC(18,6),         -- 净利率
    debt_to_assets  NUMERIC(18,6),          -- 资产负债率
    current_ratio   NUMERIC(18,6),          -- 流动比率
    quick_ratio     NUMERIC(18,6),          -- 速动比率
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (ts_code, end_date)
);

CREATE INDEX idx_fin_ind_date ON market_financial_indicator(end_date);
