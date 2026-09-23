-- 任务80: 系统级 KV 配置表（C4/C5/C6 落点）
-- DDL 与初始值已于 2026-09-23 在生产执行（本文件为固化记录，幂等可重放）
CREATE TABLE IF NOT EXISTS app_config (
    config_key   varchar(64) PRIMARY KEY,
    config_value text NOT NULL,
    description  text,
    updated_at   timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE app_config IS '系统级 KV 配置(任务80): 执行器/渠道成本口径与系统缺省线; 优先级=调用方参数>账户列>本表>代码兜底';

INSERT INTO app_config (config_key, config_value, description) VALUES
 ('backtest.commission_rate','0.0003','回测佣金率(万3,当前渠道口径)'),
 ('backtest.min_commission','5.0','回测最低佣金(元)'),
 ('backtest.stamp_tax_rate','0.0005','回测印花税(万5,卖出单边)'),
 ('backtest.slippage_rate','0.0001','回测滑点(1bp)'),
 ('backtest.impact_rate','0.05','回测冲击成本系数'),
 ('backtest.index_commission_rate','0.5','回测指数佣金口径'),
 ('backtest.transfer_fee_rate','0.00001','回转过户费(万0.1)'),
 ('margin.default_liquidation_threshold','1.3','维保平仓线缺省(paper_account 列优先)'),
 ('margin.default_warning_threshold','1.5','维保警戒线缺省(paper_account 列优先)'),
 ('executor.commission_rate','0.00025','模拟执行器佣金(万2.5)'),
 ('executor.min_commission','5.0','模拟执行器最低佣金(元)'),
 ('executor.stamp_tax_rate','0.00025','模拟执行器印花税(万2.5,卖出单边)')
ON CONFLICT (config_key) DO NOTHING;

-- 同批: strategy_config 配置化两列（任务80 批1）
ALTER TABLE strategy_config ADD COLUMN IF NOT EXISTS ind_neutral boolean NOT NULL DEFAULT false;
ALTER TABLE strategy_config ADD COLUMN IF NOT EXISTS materialize_mode varchar(32) NOT NULL DEFAULT 'pit';
COMMENT ON COLUMN strategy_config.ind_neutral IS 'combo 物化是否走行业中性化(任务80: 原 full_pit_icir_indneutral_val_v1 代码特判迁列)';
COMMENT ON COLUMN strategy_config.materialize_mode IS 'combo 物化模式: pit=PIT物化 / phase7_backfill=phase7回填路由(任务80: 原 phase7_price_volume_expanded_v1 代码特判迁列)';
