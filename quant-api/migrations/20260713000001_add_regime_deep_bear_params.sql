-- P1-2: regime 阈值配置化(从 scheduler.rs 硬编码改为 strategy_config 字段)
-- detect_regime_exposure 原硬编码 deep_bear_threshold=-0.10 / deep_bear_exposure=0.60,
-- 现字段化以支持参数敏感性扫描(P0-3)和 per-strategy regime 配置。
-- regime_bull_threshold/regime_bear_threshold 已存在(本 migration 不动),
-- 本 migration 仅补 deep_bear 两列(detect_regime_exposure 实际使用的)。
ALTER TABLE strategy_config
  ADD COLUMN IF NOT EXISTS deep_bear_threshold double precision DEFAULT '-0.10'::numeric,
  ADD COLUMN IF NOT EXISTS deep_bear_exposure double precision DEFAULT '0.60'::numeric;
