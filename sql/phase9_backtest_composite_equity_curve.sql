-- v24 回测偏离修复:P1 composite 回测曲线对标
-- 偏离监控原用纯 A 股选股回测曲线对比 composite 多资产实盘 NAV,
-- ETF 部分(债券/商品/美股)完全缺失于对标基准,导致结构性偏差。
-- 本表存储 composite 级别合成回测曲线,偏离计算直接读此表对标。
--
-- 注意:本文件仅作迁移文档参考。实际建表由 equity_curve_sync.rs 的
-- ensure_composite_curve_table() 在代码内执行(sqlx 不支持单 prepared statement
-- 多条 SQL,故 CREATE TABLE 与 CREATE INDEX 在代码里拆成两条分别执行)。
CREATE TABLE IF NOT EXISTS backtest_composite_equity_curve (
    strategy_id   VARCHAR NOT NULL,            -- composite 策略 id,如 'v24'
    trade_date    DATE NOT NULL,
    portfolio_value NUMERIC(20,4) NOT NULL,    -- composite 合成净值
    a_share_value NUMERIC(20,4),               -- A 股回测曲线值(调试用)
    etf_value     NUMERIC(20,4),               -- ETF 组合曲线值(调试用)
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (strategy_id, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_bcec_strategy_date
    ON backtest_composite_equity_curve (strategy_id, trade_date);
