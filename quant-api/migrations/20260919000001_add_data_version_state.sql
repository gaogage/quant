-- P3 data_version state 列（R11 预留兑现 + Step 4a 后续）
-- 用途：数据发现问题后将版本标记 deprecated，阻断新回测/调仓引用
-- （resolve_state / deprecate / verify_registered 代码侧待本 DDL 执行后实装）。
-- 存量行全部默认 'active'（与现行为"存在即 Active"完全一致，零行为变化）。
ALTER TABLE data_version ADD COLUMN IF NOT EXISTS state VARCHAR(16) NOT NULL DEFAULT 'active';

-- 校验约束：只允许两态，防脏值
ALTER TABLE data_version DROP CONSTRAINT IF EXISTS chk_data_version_state;
ALTER TABLE data_version ADD CONSTRAINT chk_data_version_state
    CHECK (state IN ('active', 'deprecated'));

-- 辅助索引：verify_registered 批量查询 + 未来"列 active 版本"场景
CREATE INDEX IF NOT EXISTS idx_data_version_state ON data_version (state);
