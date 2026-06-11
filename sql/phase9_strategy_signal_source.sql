-- A股选股方式下沉到策略表
-- v19 = 外层 MVO(A股 + 7 ETF) + GA + vol_target杠杆 + 体制
-- 其中"A股"大类的投资方式(因子/ML/混合)属于策略属性，不应挂在账号上
-- 账号只通过 paper_account.strategy_version_id 指向策略

ALTER TABLE strategy_config
    ADD COLUMN IF NOT EXISTS signal_source           varchar(32)  NOT NULL DEFAULT 'prediction_blend',
    ADD COLUMN IF NOT EXISTS prediction_blend_weight  double precision NOT NULL DEFAULT 0.5,
    ADD COLUMN IF NOT EXISTS combo_name               varchar(64)  NOT NULL DEFAULT 'phase7_price_volume_expanded_v1',
    ADD COLUMN IF NOT EXISTS top_n                    integer      NOT NULL DEFAULT 30,
    ADD COLUMN IF NOT EXISTS prediction_set_id        varchar(64);

COMMENT ON COLUMN strategy_config.signal_source IS 'A股大类选股方式: factor / prediction / prediction_blend';
COMMENT ON COLUMN strategy_config.prediction_blend_weight IS '因子:ML 混合比例 (0=纯因子, 1=纯ML)';
COMMENT ON COLUMN strategy_config.combo_name IS 'A股因子组合名';
COMMENT ON COLUMN strategy_config.top_n IS 'A股精选股数';
COMMENT ON COLUMN strategy_config.prediction_set_id IS 'ML预测集ID, NULL=用最新PIT集';

-- v19 生产策略: 因子+ML混合(0.5)
UPDATE strategy_config
   SET signal_source = 'prediction_blend',
       prediction_blend_weight = 0.5,
       combo_name = 'phase7_price_volume_expanded_v1',
       top_n = 30
 WHERE strategy_id = 'v19';

-- v20 同 v19 选股方式（regime router 仅改 MVO 层）
UPDATE strategy_config
   SET signal_source = 'prediction_blend',
       prediction_blend_weight = 0.5,
       combo_name = 'phase7_price_volume_expanded_v1',
       top_n = 30
 WHERE strategy_id = 'v20';
