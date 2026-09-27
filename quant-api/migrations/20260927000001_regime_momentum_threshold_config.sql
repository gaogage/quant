-- 2026-09-27 regime 动量门槛配置化（任务81 收官，用户裁决：写死值必须 DB 化）
-- 背景：mvo_weights.rs 的 adaptive min_stock 分支门槛原为代码写死
--   sleeve 3 月累计 < -0.03 → 熊市防守（regime_bear_min_stock）
--   sleeve 6 月累计 > +0.15 → bull 分支（regime_bull_min_stock）
-- 写死值违反配置化治理（任务80 精神），迁移为 strategy_config 列。
-- 默认值保持现行为（-0.03 / 0.15），存量策略零行为变化。

ALTER TABLE strategy_config
    ADD COLUMN IF NOT EXISTS regime_bear_momentum_threshold double precision DEFAULT -0.03,
    ADD COLUMN IF NOT EXISTS regime_bull_momentum_threshold double precision DEFAULT 0.15;

-- 审计注记：mvo_weights.rs 仍写死的 Kelly 族参数（基数 0.5 / 下限 0.3 / 上限 1.5）
-- 与 lookback 36 月/3 月/6 月窗口，列入硬编码审计清单待用户逐项审批后另行配置化。
