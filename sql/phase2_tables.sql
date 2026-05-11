-- Phase 2 表结构 DDL
-- 2026-05-10

-- 1. backtest_task: 加 mode 字段
ALTER TABLE backtest_task ADD COLUMN IF NOT EXISTS mode VARCHAR(16) NOT NULL DEFAULT 'standard';
ALTER TABLE backtest_task ADD CONSTRAINT chk_backtest_task_mode CHECK (mode IN ('fast', 'standard', 'audit'));

-- 2. backtest_result: 加 calmar_ratio + annualized_volatility
ALTER TABLE backtest_result ADD COLUMN IF NOT EXISTS calmar_ratio NUMERIC(18,10);
ALTER TABLE backtest_result ADD COLUMN IF NOT EXISTS annualized_volatility NUMERIC(18,10);

-- 3. backtest_trade: 加归因权重字段
ALTER TABLE backtest_trade ADD COLUMN IF NOT EXISTS target_weight NUMERIC(18,10);
ALTER TABLE backtest_trade ADD COLUMN IF NOT EXISTS executed_weight NUMERIC(18,10);

-- 4. backtest_position: 加 target_weight
ALTER TABLE backtest_position ADD COLUMN IF NOT EXISTS target_weight NUMERIC(18,10);

-- 5. portfolio_policy
CREATE TABLE IF NOT EXISTS portfolio_policy (
    policy_id VARCHAR(64) PRIMARY KEY, name VARCHAR(128) NOT NULL,
    strategy_version_id VARCHAR(64) NOT NULL,
    rebalance_rule JSONB NOT NULL, constraints JSONB NOT NULL, risk_budget JSONB,
    status VARCHAR(32) NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 6. portfolio_target
CREATE TABLE IF NOT EXISTS portfolio_target (
    task_id VARCHAR(64) NOT NULL, trade_date DATE NOT NULL, symbol VARCHAR(20) NOT NULL,
    target_weight NUMERIC(18,10) NOT NULL, target_quantity NUMERIC(24,6), reason VARCHAR(128),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (task_id, trade_date, symbol)
);

-- 7. portfolio_exposure
CREATE TABLE IF NOT EXISTS portfolio_exposure (
    task_id VARCHAR(64) NOT NULL, trade_date DATE NOT NULL,
    exposure_type VARCHAR(32) NOT NULL, exposure_name VARCHAR(128) NOT NULL,
    net_exposure NUMERIC(18,10) NOT NULL, gross_exposure NUMERIC(18,10),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (task_id, trade_date, exposure_type, exposure_name)
);

-- 8. portfolio_constraint_violation
CREATE TABLE IF NOT EXISTS portfolio_constraint_violation (
    violation_id VARCHAR(64) PRIMARY KEY, task_id VARCHAR(64) NOT NULL, trade_date DATE NOT NULL,
    constraint_name VARCHAR(128) NOT NULL, limit_value NUMERIC(18,10) NOT NULL,
    actual_value NUMERIC(18,10) NOT NULL, severity VARCHAR(16) NOT NULL DEFAULT 'warning',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_portfolio_violation_task ON portfolio_constraint_violation(task_id, trade_date);
ALTER TABLE portfolio_constraint_violation ADD CONSTRAINT chk_portfolio_violation_severity CHECK (severity IN ('warning', 'hard'));

-- 9. portfolio_attribution
CREATE TABLE IF NOT EXISTS portfolio_attribution (
    task_id VARCHAR(64) NOT NULL, trade_date DATE NOT NULL,
    attribution_type VARCHAR(32) NOT NULL, attribution_name VARCHAR(128) NOT NULL,
    contribution NUMERIC(18,10) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (task_id, trade_date, attribution_type, attribution_name)
);

-- 10. benchmark_weight (TimescaleDB hypertable)
CREATE TABLE IF NOT EXISTS benchmark_weight (
    benchmark_code VARCHAR(20) NOT NULL, trade_date DATE NOT NULL,
    symbol VARCHAR(20) NOT NULL, weight NUMERIC(18,10) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (benchmark_code, trade_date, symbol)
);
SELECT create_hypertable('benchmark_weight', 'trade_date', chunk_time_interval => INTERVAL '3 months', if_not_exists => TRUE);
