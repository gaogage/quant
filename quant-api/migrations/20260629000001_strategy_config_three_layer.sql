-- 策略组合嵌套架构三层结构(strategy_config 加 strategy_type/parent_strategy_id/asset_class)
-- 对应 spec 45 Task 1: DB 迁移

-- 1. 新增 3 个字段区分层级
ALTER TABLE strategy_config
  ADD COLUMN IF NOT EXISTS strategy_type varchar(16) NOT NULL DEFAULT 'composite',
  ADD COLUMN IF NOT EXISTS parent_strategy_id varchar(32) NULL,
  ADD COLUMN IF NOT EXISTS asset_class varchar(16) NULL;

-- 2. equity_curve_task_id 改可空(非选股 asset 行 commodity/bond/us_stock 无需 backtest 选股)
ALTER TABLE strategy_config ALTER COLUMN equity_curve_task_id DROP NOT NULL;

-- 3. CHECK 约束
ALTER TABLE strategy_config
  ADD CONSTRAINT strategy_type_chk CHECK (strategy_type IN ('composite','asset')),
  ADD CONSTRAINT asset_class_chk CHECK (asset_class IS NULL OR asset_class IN ('a_share','us_stock','bond','cash','commodity')),
  ADD CONSTRAINT composite_asset_class_null_chk CHECK (strategy_type='composite' OR asset_class IS NOT NULL);
