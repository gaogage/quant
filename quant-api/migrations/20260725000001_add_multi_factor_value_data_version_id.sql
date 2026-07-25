-- DDD Step 5a/5b 前置：multi_factor_value 加 data_version_id 列
--
-- 目的：让因子数据加载 SQL 能按 data_version_id 过滤，从数据层保证 PIT 语义
-- （之前 multi_factor_value 无此列，因子查询无法区分数据版本，有跨版本脏读风险）。
--
-- 安全性：
-- - 加可空列无默认值，PostgreSQL 元数据级操作，不重写 1.3 亿行数据，不锁表。
-- - 不加 FK 约束（大表 FK 有插入/更新性能影响；data_version_id 为 NULL 表示
--   历史数据未关联版本，类型层 PitSeries 构造时校验非空）。
-- - 索引单独建（可选），避免加列时连带建索引锁表。
--
-- 关联：Step 5b 的 VerifiedBar::try_from_raw 校验 data_version_id 存在性；
--       Step 5a 的 PitSeries::from_verified_pit 要求 dv_id 非空。

ALTER TABLE multi_factor_value
    ADD COLUMN IF NOT EXISTS data_version_id character varying(64);

-- 为 dv_id 过滤查询建部分索引（仅非 NULL 行，避免全表索引膨胀）
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_multi_factor_value_data_version
    ON multi_factor_value (data_version_id)
    WHERE data_version_id IS NOT NULL;
