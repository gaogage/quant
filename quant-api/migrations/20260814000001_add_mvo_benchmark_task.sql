-- mvo_benchmark_daily: MVO 动态基准曲线同步(21:00 EOD 后)
-- 全周期重跑 MVO 动态后复权无成本回测,落 backtest_mvo_equity_curve 作实盘偏离基准。
-- 替代 composite 静态合成曲线(曾致仪表盘"回测偏离"口径错配 +30%)。
-- run_daily_simulation 必须操作账号,故用专用 inactive 账号 pa-v24-mvo-bench 承载(scheduler 不调 inactive)。

INSERT INTO scheduled_task_config (
    task_name, task_type, description, enabled, schedule_cron, params, created_by, updated_by
)
VALUES (
    'mvo_benchmark_daily',
    'mvo_equity_curve_update',
    'MVO 动态后复权无成本基准曲线同步(21:00 EOD 后),供仪表盘偏离对比',
    true,
    '0 21 * * 1-5',
    '{"strategy_id":"v24","benchmark_account_id":"pa-v24-mvo-bench","start_date":"20160104"}'::jsonb,
    'claude',
    'claude'
)
ON CONFLICT (task_name) DO UPDATE
SET task_type = EXCLUDED.task_type,
    description = EXCLUDED.description,
    enabled = EXCLUDED.enabled,
    schedule_cron = EXCLUDED.schedule_cron,
    params = EXCLUDED.params,
    updated_by = EXCLUDED.updated_by,
    updated_at = now();

-- lev(1.5x) 基准:真带杠杆跑,供 lev 账号精确偏离对比(避免无杠杆基准线性放大低估杠杆复利)
-- 用独立 lev 基准账号 pa-v24-mvo-bench-lev,与 unlev 任务隔离避免并发 reset 冲突。
INSERT INTO scheduled_task_config (
    task_name, task_type, description, enabled, schedule_cron, params, created_by, updated_by
)
VALUES (
    'mvo_benchmark_daily_lev',
    'mvo_equity_curve_update',
    'MVO 动态有杠杆(1.5x)基准曲线同步(21:00),供 lev 账号偏离对比',
    true,
    '0 21 * * 1-5',
    '{"strategy_id":"v24_lev","benchmark_account_id":"pa-v24-mvo-bench-lev","start_date":"20160104","leverage_multiplier":1.5}'::jsonb,
    'claude',
    'claude'
)
ON CONFLICT (task_name) DO UPDATE
SET params = EXCLUDED.params,
    schedule_cron = EXCLUDED.schedule_cron,
    updated_at = now();
