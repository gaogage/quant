-- P1-4: v16 ML blend 策略参数配置化(从 scheduler.rs 硬编码改为 strategy_config 字段)
-- kelly_fraction/score_candidate_pool_size 原硬编码 0.25/200(scheduler.rs:3011, equity_curve_sync.rs:168),
-- 现字段化以支持参数敏感性扫描和 per-strategy blend 配置。
-- 金融常量(252 年交易日/MA60/MA250)不配置化(行业惯例,改之反而引入过拟合嫌疑)。
ALTER TABLE strategy_config
  ADD COLUMN IF NOT EXISTS kelly_fraction double precision DEFAULT '0.25'::numeric,
  ADD COLUMN IF NOT EXISTS score_candidate_pool_size integer DEFAULT 200;
