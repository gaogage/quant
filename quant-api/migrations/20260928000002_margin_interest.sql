-- 2026-09-28 融资利息计提（用户批准实施；账号级利率——每个账号可对应不同券商）
-- 设计: 利率三层仲裁(对齐维保线任务80 C5 先例):
--   账户列 margin_interest_rate > app_config margin.interest_default_rate > 代码兜底 0.0835
-- 计息: 每日 EOD 盯市时 interest = margin_amount × rate / day_basis(360)
--   从 cash 扣减(保证金口径, 等价净值减), margin_amount 不变(本金不动)
-- 日志表: margin_interest_log 每账户每日一行, 幂等(唯一约束), 可审计

ALTER TABLE paper_account
    ADD COLUMN IF NOT EXISTS margin_interest_rate double precision,
    ADD COLUMN IF NOT EXISTS margin_interest_day_basis integer DEFAULT 360;

CREATE TABLE IF NOT EXISTS margin_interest_log (
    paper_account_id varchar(64) NOT NULL,
    interest_date    date NOT NULL,
    margin_amount    numeric(24,6) NOT NULL,
    annual_rate      double precision NOT NULL,
    day_basis        integer NOT NULL,
    interest_amount  numeric(24,6) NOT NULL,
    created_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (paper_account_id, interest_date)
);

INSERT INTO app_config (config_key, config_value, description) VALUES
    ('margin.interest_default_rate', '0.0835', '融资利息默认年利率(账户列缺省时; 主流券商无门槛约 8.35%)'),
    ('margin.interest_day_basis', '360', '计息天数基准(券商惯例 360)')
ON CONFLICT (config_key) DO NOTHING;

-- 生产 lev 账户: 当前无专属券商费率信息, 留空走默认 8.35%(用户提供实际费率后 UPDATE)
