-- P4.0d: rebalance 参数字段化(regime 阈值 + slippage)
-- 将硬编码的杠杆门控阈值(0.9)和滑点(0.002)改为可配置字段

ALTER TABLE strategy_config
  ADD COLUMN IF NOT EXISTS leverage_regime_threshold DOUBLE PRECISION DEFAULT 0.9,
  ADD COLUMN IF NOT EXISTS slippage_pct DOUBLE PRECISION DEFAULT 0.002;
