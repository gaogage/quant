-- 2026-09-27 ETF 动量过滤配置化（Python 脚本论证通过后的 Rust 实现配套）
-- 机制：ETF N 日动量 < 0 时该资产权重 × downscale，池内归一化（TSMOM 倾斜）
-- 论证：13 年逐年全正、全窗口稳健（任务81 文档）；组合层增量待研究账号回放验证
-- 默认 enabled=false——存量策略零行为变化，验证通过经用户批准后方启用。

ALTER TABLE strategy_config
    ADD COLUMN IF NOT EXISTS etf_momentum_filter_enabled boolean DEFAULT false,
    ADD COLUMN IF NOT EXISTS etf_momentum_window_days integer DEFAULT 20,
    ADD COLUMN IF NOT EXISTS etf_momentum_downscale double precision DEFAULT 0.4;
