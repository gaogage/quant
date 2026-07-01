-- P4.0d: MVO_OBJECTIVE 字段化(从全局 env var 改为 strategy_config 字段)
-- 默认 minvariance,与原 env var 缺省分支一致。
ALTER TABLE strategy_config
  ADD COLUMN IF NOT EXISTS mvo_objective TEXT DEFAULT 'minvariance';
