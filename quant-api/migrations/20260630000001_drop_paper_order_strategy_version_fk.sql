-- spec 45 迁移补全:删除 paper_order.strategy_version_id 过时外键约束
--
-- 背景:spec 45 把策略配置从 strategy_version 表迁移到 strategy_config 三层结构
-- (composite/asset,用 strategy_id 标识)。paper_account.strategy_version_id 和
-- paper_order.strategy_version_id 语义已变为 strategy_config.strategy_id(如 "v19")。
--
-- 但 paper_order 的外键 fk_paper_order_strategy_version 仍引用旧 strategy_version 表,
-- 导致建仓时 INSERT paper_order(strategy_version_id='v19')违反外键
-- (strategy_version 表无 v19 记录)→ create_planned_trade 全失败 → 回放/在线模拟 NAV 恒定。
--
-- 修复:删除过时外键。strategy_version_id 现指 strategy_config.strategy_id,
-- 语义已与 strategy_version 表脱钩(spec 45 §30/§88)。
-- ON DELETE SET NULL 语义保留(strategy_config 行删除时 paper_order.strategy_version_id 置空),
-- 由应用层(load_resolved_strategy 查不到时 Err)保证引用完整性。

ALTER TABLE paper_order DROP CONSTRAINT IF EXISTS fk_paper_order_strategy_version;
