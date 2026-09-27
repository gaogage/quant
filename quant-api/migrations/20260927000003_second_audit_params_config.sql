-- 2026-09-27 第二轮硬编码审计 C1-C7 全批配置化（用户裁决）
-- C1/C2/C4 业务语义 → strategy_config 列；C3 执行器级 → app_config 键；
-- C5/C6/C7 窗口/运维类 → env + 兜底（代码内实现，无 DDL）。
-- 默认值=原写死值，存量策略行为零变化。

ALTER TABLE strategy_config
    -- C1: regime<1 时现金段自动申购标的与触发阈值(原 rebalance.rs 写死 "511880.SH"/0.01)
    ADD COLUMN IF NOT EXISTS cash_park_symbol varchar(20) DEFAULT '511880.SH',
    ADD COLUMN IF NOT EXISTS cash_park_threshold double precision DEFAULT 0.01,
    -- C2: 组合止损双阈值(原 paper.rs 写死 0.10/0.25: 组合 DD>10% 且个股 DD>25% 才止损)
    ADD COLUMN IF NOT EXISTS stop_loss_portfolio_dd double precision DEFAULT 0.10,
    ADD COLUMN IF NOT EXISTS stop_loss_stock_dd double precision DEFAULT 0.25,
    -- C4: CSI300 regime 判定回看窗(原 mvo_engine.rs 写死 400 天)
    ADD COLUMN IF NOT EXISTS regime_lookback_days integer DEFAULT 400;

-- C3: PTrade 信号执行窗口(原 signal_export.rs 写死 09:35-10:30 / 10:35-11:30)
INSERT INTO app_config (config_key, config_value, description) VALUES
    ('signal.execute_window_start', '"09:35"', 'PTrade 信号执行窗口起点(正常标的)'),
    ('signal.execute_window_end', '"10:30"', 'PTrade 信号执行窗口终点'),
    ('signal.deferred_window_start', '"10:35"', 'PTrade 延迟执行窗口起点(溢价门禁命中标的)'),
    ('signal.deferred_window_end', '"11:30"', 'PTrade 延迟执行窗口终点')
ON CONFLICT (config_key) DO NOTHING;
