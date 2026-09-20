-- 任务71: market_stock_daily_bar_adj 视图 stock 语义收紧(2026-09-20)
--
-- 背景: 2026-07-24~08-04 复权因子缺口 7 个交易日(199~742 只/日), 视图对缺 adj 行
-- COALESCE(adj_factor,1.0) 静默返回未复权 raw 价——错误价格混入因子/MVO/回测序列,
-- 且无任何告警(比例阈值 90% 又漏报 3 天)。用户定版: 复权是绩效评估基石,
-- 宁可缺行不可错价, 100% 完备。
--
-- 改动: LEFT JOIN market_stock 判定 instrument_type——
--   stock: 缺 adj 的行从视图消失(WHERE a.symbol IS NOT NULL 对 stock 强制生效)
--   etf:   维持放行(仅策略池有 adj 体系, 独立检查项覆盖)
--   指数(不在 market_stock): 维持放行(无复权概念, 语义正确)
--
-- 前置(已验证): stock 2014+ bar⊆adj=0(缺口 15 行已补); adj_factor 全表零 NULL;
--   market_stock.symbol 唯一。数据完备时本视图与旧版输出逐位一致。
-- 执行后须过: 视图行数/价格锚点与改前一致 + audit hash 2/2 不变。

CREATE OR REPLACE VIEW market_stock_daily_bar_adj AS
SELECT b.symbol,
       b.trade_date,
       (b.open  * COALESCE(a.adj_factor, 1.0))::numeric(20,4) AS open,
       (b.high  * COALESCE(a.adj_factor, 1.0))::numeric(20,4) AS high,
       (b.low   * COALESCE(a.adj_factor, 1.0))::numeric(20,4) AS low,
       (b.close * COALESCE(a.adj_factor, 1.0))::numeric(20,4) AS close,
       (b.pre_close * COALESCE(a.adj_factor, 1.0))::numeric(20,4) AS pre_close,
       CASE WHEN LAG(b.close * COALESCE(a.adj_factor, 1.0)) OVER (w) > 0
            THEN ROUND(((b.close * COALESCE(a.adj_factor,1.0)) /
                 LAG(b.close * COALESCE(a.adj_factor,1.0)) OVER (w) - 1) * 100, 4)
            ELSE NULL END AS pct_change,
       b.volume,
       b.amount,
       b.data_version_id
FROM market_stock_daily_bar b
LEFT JOIN market_adjustment_factor a ON a.symbol::text = b.symbol::text AND a.trade_date = b.trade_date
LEFT JOIN market_stock s ON s.symbol::text = b.symbol::text
WHERE a.symbol IS NOT NULL
   OR s.instrument_type IS DISTINCT FROM 'stock'
WINDOW w AS (PARTITION BY b.symbol ORDER BY b.trade_date);

COMMENT ON VIEW market_stock_daily_bar_adj IS
  '后复权日线视图: stock 缺 adj 行不出现(宁缺行不错价, 2026-09-20 任务71); ETF/指数放行 raw; adj 缺失由复权因子逐股 100% 门禁抓取';
