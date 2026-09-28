-- 2026-09-28 杠杆预平衡（任务85）：账号级带宽配置
-- 实际杠杆 = (NAV+margin)/NAV 维持在目标×(1±band) 内——超上限卖出资金优先还款,
-- 低于下限预算池放宽融资。带宽 0/负 = 功能关闭(回退现状)。
-- 默认 0.15(±15%),账号可各自设置(不同券商/风险偏好)。

ALTER TABLE paper_account
    ADD COLUMN IF NOT EXISTS leverage_rebalance_band double precision;

INSERT INTO app_config (config_key, config_value, description) VALUES
    ('leverage.rebalance_band_default', '0.15', '杠杆预平衡带宽默认值(账号列缺省时兜底; 0=关闭)')
ON CONFLICT (config_key) DO NOTHING;
