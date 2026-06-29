-- 8 平铺策略行迁移为 composite + asset 三层结构
-- 对应 spec 45 Task 2: 数据迁移
-- 决策:asset 行标的列表复用 etf_symbols 列(非选股 asset 行 task_id=NULL)

-- ETF 阵容:["518880.SH"(黄金),"511010.SH"(国债),"513500.SH"(标普),"513100.SH"(纳指),"159980.SZ"(有色),"159985.SZ"(豆粕),"501018.SH"(原油)]
-- 归类:
--   commodity: 黄金/有色/原油/豆粕(原 MVO 权重 [0.22,0.03,0.03,0.03])
--   bond:      国债(权重 [0.28])
--   us_stock:  标普/纳指(权重 [0.05,0.10])
--   a_share:   从平铺行复制选股参数 + 个股集中度

-- 现有 v19/v21/v21_lev 三行已默认 strategy_type='composite',无需更新,仅 INSERT 12 个 asset 子行。
-- 以下对每个 composite 各插 4 asset 行。

-- ===== v19 =====
INSERT INTO strategy_config (strategy_id, name, strategy_type, parent_strategy_id, asset_class, status, etf_symbols, default_weights, signal_source, rebalance_freq)
VALUES ('v19-commodity', 'v19商品资产', 'asset', 'v19', 'commodity', 'active',
        '["518880.SH","159980.SZ","501018.SH","159985.SZ"]', '[0.22,0.03,0.03,0.03]', 'fixed', 'quarterly')
ON CONFLICT (strategy_id) DO NOTHING;
INSERT INTO strategy_config (strategy_id, name, strategy_type, parent_strategy_id, asset_class, status, etf_symbols, default_weights, signal_source, rebalance_freq)
VALUES ('v19-bond', 'v19债券资产', 'asset', 'v19', 'bond', 'active',
        '["511010.SH"]', '[0.28]', 'fixed', 'quarterly')
ON CONFLICT (strategy_id) DO NOTHING;
INSERT INTO strategy_config (strategy_id, name, strategy_type, parent_strategy_id, asset_class, status, etf_symbols, default_weights, signal_source, rebalance_freq)
VALUES ('v19-us_stock', 'v19美股资产', 'asset', 'v19', 'us_stock', 'active',
        '["513500.SH","513100.SH"]', '[0.05,0.10]', 'fixed', 'quarterly')
ON CONFLICT (strategy_id) DO NOTHING;
INSERT INTO strategy_config (strategy_id, name, strategy_type, parent_strategy_id, asset_class, status, signal_source, combo_name, top_n, prediction_blend_weight, score_direction, candidate_tier, equity_curve_task_id, max_single, max_single_bull, rebalance_freq, prediction_set_id, etf_symbols, default_weights)
SELECT 'v19-a_share', 'v19 A股资产', 'asset', 'v19', 'a_share', 'active',
       signal_source, combo_name, top_n, prediction_blend_weight, score_direction, candidate_tier,
       equity_curve_task_id, max_single, max_single_bull, 'quarterly', prediction_set_id, '[]', '[]'
FROM strategy_config WHERE strategy_id='v19'
ON CONFLICT (strategy_id) DO NOTHING;

-- ===== v21 =====
INSERT INTO strategy_config (strategy_id, name, strategy_type, parent_strategy_id, asset_class, status, etf_symbols, default_weights, signal_source, rebalance_freq)
VALUES ('v21-commodity', 'v21商品资产', 'asset', 'v21', 'commodity', 'active',
        '["518880.SH","159980.SZ","501018.SH","159985.SZ"]', '[0.22,0.03,0.03,0.03]', 'fixed', 'quarterly')
ON CONFLICT (strategy_id) DO NOTHING;
INSERT INTO strategy_config (strategy_id, name, strategy_type, parent_strategy_id, asset_class, status, etf_symbols, default_weights, signal_source, rebalance_freq)
VALUES ('v21-bond', 'v21债券资产', 'asset', 'v21', 'bond', 'active',
        '["511010.SH"]', '[0.28]', 'fixed', 'quarterly')
ON CONFLICT (strategy_id) DO NOTHING;
INSERT INTO strategy_config (strategy_id, name, strategy_type, parent_strategy_id, asset_class, status, etf_symbols, default_weights, signal_source, rebalance_freq)
VALUES ('v21-us_stock', 'v21美股资产', 'asset', 'v21', 'us_stock', 'active',
        '["513500.SH","513100.SH"]', '[0.05,0.10]', 'fixed', 'quarterly')
ON CONFLICT (strategy_id) DO NOTHING;
INSERT INTO strategy_config (strategy_id, name, strategy_type, parent_strategy_id, asset_class, status, signal_source, combo_name, top_n, prediction_blend_weight, score_direction, candidate_tier, equity_curve_task_id, max_single, max_single_bull, rebalance_freq, prediction_set_id, etf_symbols, default_weights)
SELECT 'v21-a_share', 'v21 A股资产', 'asset', 'v21', 'a_share', 'active',
       signal_source, combo_name, top_n, prediction_blend_weight, score_direction, candidate_tier,
       equity_curve_task_id, max_single, max_single_bull, 'quarterly', prediction_set_id, '[]', '[]'
FROM strategy_config WHERE strategy_id='v21'
ON CONFLICT (strategy_id) DO NOTHING;

-- ===== v21_lev =====
INSERT INTO strategy_config (strategy_id, name, strategy_type, parent_strategy_id, asset_class, status, etf_symbols, default_weights, signal_source, rebalance_freq)
VALUES ('v21_lev-commodity', 'v21_lev商品资产', 'asset', 'v21_lev', 'commodity', 'active',
        '["518880.SH","159980.SZ","501018.SH","159985.SZ"]', '[0.22,0.03,0.03,0.03]', 'fixed', 'quarterly')
ON CONFLICT (strategy_id) DO NOTHING;
INSERT INTO strategy_config (strategy_id, name, strategy_type, parent_strategy_id, asset_class, status, etf_symbols, default_weights, signal_source, rebalance_freq)
VALUES ('v21_lev-bond', 'v21_lev债券资产', 'asset', 'v21_lev', 'bond', 'active',
        '["511010.SH"]', '[0.28]', 'fixed', 'quarterly')
ON CONFLICT (strategy_id) DO NOTHING;
INSERT INTO strategy_config (strategy_id, name, strategy_type, parent_strategy_id, asset_class, status, etf_symbols, default_weights, signal_source, rebalance_freq)
VALUES ('v21_lev-us_stock', 'v21_lev美股资产', 'asset', 'v21_lev', 'us_stock', 'active',
        '["513500.SH","513100.SH"]', '[0.05,0.10]', 'fixed', 'quarterly')
ON CONFLICT (strategy_id) DO NOTHING;
INSERT INTO strategy_config (strategy_id, name, strategy_type, parent_strategy_id, asset_class, status, signal_source, combo_name, top_n, prediction_blend_weight, score_direction, candidate_tier, equity_curve_task_id, max_single, max_single_bull, rebalance_freq, prediction_set_id, etf_symbols, default_weights)
SELECT 'v21_lev-a_share', 'v21_lev A股资产', 'asset', 'v21_lev', 'a_share', 'active',
       signal_source, combo_name, top_n, prediction_blend_weight, score_direction, candidate_tier,
       equity_curve_task_id, max_single, max_single_bull, 'quarterly', prediction_set_id, '[]', '[]'
FROM strategy_config WHERE strategy_id='v21_lev'
ON CONFLICT (strategy_id) DO NOTHING;
