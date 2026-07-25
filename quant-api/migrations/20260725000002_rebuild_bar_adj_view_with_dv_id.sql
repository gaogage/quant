-- DDD Step 5b 前置：market_stock_daily_bar_adj 视图补 data_version_id 列
--
-- 目的：让行情查询 SQL 能按 data_version_id 过滤（之前视图只选价格列，未暴露
-- 基表 b.data_version_id，导致 load_daily_bars 等查询无法区分数据版本）。
--
-- 安全性：CREATE OR REPLACE VIEW 是元数据操作，不锁表不重写数据。
-- 视图依赖的 market_stock_daily_bar 基表已有 data_version_id 列（含 FK）。
--
-- 关联：Step 5b 的 load_daily_bars 改为返回 Vec<VerifiedBar>，SQL 加
--       AND data_version_id = $N 过滤；缓存键纳入 dv_id 防跨版本污染。

CREATE OR REPLACE VIEW market_stock_daily_bar_adj AS
SELECT
    b.symbol,
    b.trade_date,
    (b.open * COALESCE(a.adj_factor, 1.0))::numeric(20,4)  AS open,
    (b.high * COALESCE(a.adj_factor, 1.0))::numeric(20,4)  AS high,
    (b.low * COALESCE(a.adj_factor, 1.0))::numeric(20,4)   AS low,
    (b.close * COALESCE(a.adj_factor, 1.0))::numeric(20,4) AS close,
    (b.pre_close * COALESCE(a.adj_factor, 1.0))::numeric(20,4) AS pre_close,
    b.pct_change,
    b.volume,
    b.amount,
    b.data_version_id
FROM market_stock_daily_bar b
LEFT JOIN market_adjustment_factor a
    ON a.symbol::text = b.symbol::text
   AND a.trade_date = b.trade_date;

COMMENT ON VIEW market_stock_daily_bar_adj IS
    '复权日线视图（Step 5b 补 data_version_id 列，供 PIT 过滤）';
