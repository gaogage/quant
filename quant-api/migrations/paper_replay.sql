-- paper_replay 表：回放结果存储（一个账号一条记录）
-- 执行方式：psql -h <host> -U <user> -d <db> -f migration_paper_replay.sql

CREATE TABLE IF NOT EXISTS paper_replay (
    replay_id              VARCHAR(64) PRIMARY KEY,
    paper_account_id       VARCHAR(64) NOT NULL,
    start_date             DATE NOT NULL,
    end_date               DATE NOT NULL,
    annual_return_pct      DOUBLE PRECISION,
    cumulative_return_pct  DOUBLE PRECISION,
    sharpe_ratio           DOUBLE PRECISION,
    sortino_ratio          DOUBLE PRECISION,
    calmar_ratio           DOUBLE PRECISION,
    max_drawdown_pct       DOUBLE PRECISION,
    volatility_pct         DOUBLE PRECISION,
    win_rate_pct           DOUBLE PRECISION,
    trading_days           INTEGER,
    yearly_returns         JSONB,
    benchmarks             JSONB,
    created_by             VARCHAR(32),
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 如果表已存在但缺少 JSONB 列（升级场景）
ALTER TABLE paper_replay ADD COLUMN IF NOT EXISTS yearly_returns JSONB;
ALTER TABLE paper_replay ADD COLUMN IF NOT EXISTS benchmarks JSONB;
