-- factor_backfill_daily: 因子回填定时任务(调仓数据保障)
-- T+1 9:00 run_tick 内联已做 bar 同步+phase7 因子回填(主路径),
-- 此任务作为"补保险"在 T+1 之后(9:30)幂等刷新 phase7 量价因子,
-- 保证盘中调仓依赖的 factor_value/multi_factor_value 新鲜。
-- 可被 check_task_dependency_order 检查、可手动触发、可配 CRON。

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
    'factor_backfill_daily',
    'factor_backfill',
    '因子回填(phase7 量价因子),T+1 补保险,保证盘中调仓数据新鲜',
    true,
    '30 9 * * 1-5',
    '{"version":"1.0.0"}'::jsonb,
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
