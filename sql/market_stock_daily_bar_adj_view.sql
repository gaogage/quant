-- 后复权视图(2026-09-17 P1-2 定版): OHLC×因子 + pct_change 总回报口径
-- pct_change = 复权序列日收益(与 close 同空间); raw 表保留原始价格涨跌
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
WINDOW w AS (PARTITION BY b.symbol ORDER BY b.trade_date);
