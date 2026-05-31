-- Phase 8: Paper Trading — Position Tracking & NAV Monitoring
-- Design follows 05-表结构设计.md conventions:
--   PK: VARCHAR(64), audit: created_at/updated_at, money: NUMERIC(24,6), price: NUMERIC(18,10)

BEGIN;

-- ── paper_position: live holdings per account ──
CREATE TABLE IF NOT EXISTS public.paper_position (
    paper_position_id  VARCHAR(64) PRIMARY KEY,
    paper_account_id   VARCHAR(64) NOT NULL
        REFERENCES paper_account(paper_account_id) ON DELETE CASCADE,
    symbol             VARCHAR(20) NOT NULL,
    quantity           NUMERIC(24,6) NOT NULL DEFAULT 0,
    avg_cost           NUMERIC(18,6) NOT NULL DEFAULT 0,
    market_price       NUMERIC(18,6),
    market_value       NUMERIC(24,6),
    -- Portfolio construction tracking
    target_weight      NUMERIC(18,10),   -- from strategy signal
    actual_weight      NUMERIC(18,10),   -- actual allocation
    weight_drift       NUMERIC(18,10),   -- target - actual
    -- P&L
    unrealized_pnl     NUMERIC(24,6) DEFAULT 0,
    realized_pnl       NUMERIC(24,6) DEFAULT 0,
    last_trade_date    DATE,
    -- Frozen capital (T+1 settlement)
    frozen_quantity    NUMERIC(24,6) DEFAULT 0,
    frozen_amount      NUMERIC(24,6) DEFAULT 0,
    -- Audit
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (paper_account_id, symbol)
);

CREATE INDEX IF NOT EXISTS idx_paper_position_account
    ON paper_position(paper_account_id);

-- trigger_updated_at handled by trg_paper_position_updated_at below
CREATE TRIGGER trg_paper_position_updated_at
    BEFORE UPDATE ON paper_position
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ── paper_nav_snapshot: daily NAV for performance tracking ──
CREATE TABLE IF NOT EXISTS public.paper_nav_snapshot (
    nav_snapshot_id    VARCHAR(64) NOT NULL,   -- TIMEScaledb hypertable requires NOT NULL
    paper_account_id   VARCHAR(64) NOT NULL
        REFERENCES paper_account(paper_account_id) ON DELETE CASCADE,
    snapshot_date      DATE NOT NULL,
    nav                NUMERIC(24,6) NOT NULL,
    cash               NUMERIC(24,6) NOT NULL,
    market_value       NUMERIC(24,6) NOT NULL,
    frozen_amount      NUMERIC(24,6) DEFAULT 0,
    position_count     INTEGER NOT NULL DEFAULT 0,
    -- Performance metrics (since inception)
    daily_return       NUMERIC(18,10),
    cumulative_return  NUMERIC(18,10),
    benchmark_return   NUMERIC(18,10),  -- 000300.SH cumulative
    excess_return      NUMERIC(18,10),
    max_drawdown       NUMERIC(18,10),
    running_sharpe     NUMERIC(18,10),
    -- Strategy source
    strategy_version_id VARCHAR(64)
        REFERENCES strategy_version(strategy_version_id) ON DELETE SET NULL,
    prediction_set_id  VARCHAR(64),
    signal_count       INTEGER DEFAULT 0,
    trade_count        INTEGER DEFAULT 0,
    -- Audit
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (paper_account_id, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_paper_nav_snapshot_date
    ON paper_nav_snapshot(snapshot_date DESC);

-- ── paper_account: add nav tracking columns ──
ALTER TABLE public.paper_account
    ADD COLUMN IF NOT EXISTS current_nav        NUMERIC(24,6),
    ADD COLUMN IF NOT EXISTS peak_nav           NUMERIC(24,6),
    ADD COLUMN IF NOT EXISTS max_drawdown_pct   NUMERIC(18,10),
    ADD COLUMN IF NOT EXISTS total_trades       INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_signal_date   DATE,
    ADD COLUMN IF NOT EXISTS candidate_type     VARCHAR(32),  -- defensive / professional / elite
    ADD COLUMN IF NOT EXISTS strategy_version_id VARCHAR(64)
        REFERENCES strategy_version(strategy_version_id) ON DELETE SET NULL;

COMMIT;
