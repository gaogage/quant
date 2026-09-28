-- 2026-09-29 调仓预算分配重构（任务86）：生成侧过滤 + pro-rata 均匀缩放
-- 背景见 docs/projects/quant/tasks/quant/86-预算分配重构.md

-- ① ETF 再平衡带宽配置化（原硬编码 0.25, 2026-09-08 换手优化的值延续为默认）
--    归属: 策略级——带宽=策略容忍的跟踪误差/换手权衡, 相对量纲跨账号一致
ALTER TABLE strategy_config
    ADD COLUMN IF NOT EXISTS etf_rebalance_band double precision DEFAULT 0.25;

-- ② 最小交易额（存量微调低于此值不生成单; 新建仓/清仓豁免）
--    归属: 账号级——佣金失衡阈值=券商费率+资金规模属性（同 margin_interest_rate 先例）
ALTER TABLE paper_account
    ADD COLUMN IF NOT EXISTS min_trade_amount double precision;

-- app_config 兜底（账号列 NULL 时生效; 0 = 关闭过滤）
INSERT INTO app_config (config_key, config_value, description) VALUES
    ('rebalance.min_trade_default', '5000', '最小交易额默认值(元):存量微调低于此值不生成单,账号列缺省时兜底; 0=关闭')
ON CONFLICT (config_key) DO NOTHING;
