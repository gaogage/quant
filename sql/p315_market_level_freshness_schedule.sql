-- P3.15 market-level regime/risk-budget source freshness schedule.
-- Keeps margin/HSGT source checks in the scheduler ledger instead of manual-only operations.

INSERT INTO scheduled_task_config (
    task_name,
    task_type,
    description,
    enabled,
    schedule_cron,
    params,
    created_by,
    updated_by
)
VALUES (
    'p315_market_level_freshness_daily',
    'market_level_source_freshness',
    'P3.15 market-level regime/risk-budget source freshness sync for margin and HSGT',
    true,
    '20 18 * * 1-5',
    '{"sources":["market_margin_regime","market_moneyflow_hsgt_regime"]}'::jsonb,
    'codex',
    'codex'
)
ON CONFLICT (task_name) DO UPDATE
SET task_type = EXCLUDED.task_type,
    description = EXCLUDED.description,
    enabled = EXCLUDED.enabled,
    schedule_cron = EXCLUDED.schedule_cron,
    params = EXCLUDED.params,
    updated_by = EXCLUDED.updated_by,
    updated_at = now();
