-- Phase 7 P3.19J futures price-chain raw source schema.
-- This DDL is a design artifact first. Apply only after the schema contract
-- and PIT publication policy are reviewed.

CREATE TABLE IF NOT EXISTS market_futures_daily (
    ts_code TEXT NOT NULL,
    trade_date DATE NOT NULL,
    pre_close NUMERIC,
    pre_settle NUMERIC,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    settle NUMERIC,
    change1 NUMERIC,
    change2 NUMERIC,
    vol NUMERIC,
    amount NUMERIC,
    oi NUMERIC,
    oi_chg NUMERIC,
    delv_settle NUMERIC,
    available_at DATE NOT NULL,
    source_published_at TIMESTAMPTZ,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    source TEXT NOT NULL DEFAULT 'tushare',
    data_version_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (ts_code, trade_date),
    CONSTRAINT market_futures_daily_pit_available_at_check CHECK (available_at >= trade_date)
);

CREATE TABLE IF NOT EXISTS market_futures_warehouse_receipt (
    trade_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    exchange TEXT NOT NULL DEFAULT '',
    fut_name TEXT,
    warehouse TEXT NOT NULL DEFAULT '',
    wh_id TEXT,
    pre_vol NUMERIC,
    vol NUMERIC,
    vol_chg NUMERIC,
    area TEXT,
    year TEXT,
    grade TEXT,
    brand TEXT,
    place TEXT,
    pd NUMERIC,
    is_ct TEXT,
    unit TEXT,
    available_at DATE NOT NULL,
    source_published_at TIMESTAMPTZ,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    source TEXT NOT NULL DEFAULT 'tushare',
    data_version_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (trade_date, symbol, exchange, warehouse),
    CONSTRAINT market_futures_wsr_pit_available_at_check CHECK (available_at >= trade_date)
);

CREATE TABLE IF NOT EXISTS market_futures_holding_rank (
    trade_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    exchange TEXT NOT NULL DEFAULT '',
    broker TEXT NOT NULL DEFAULT '',
    vol NUMERIC,
    vol_chg NUMERIC,
    long_hld NUMERIC,
    long_chg NUMERIC,
    short_hld NUMERIC,
    short_chg NUMERIC,
    available_at DATE NOT NULL,
    source_published_at TIMESTAMPTZ,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    source TEXT NOT NULL DEFAULT 'tushare',
    data_version_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (trade_date, symbol, exchange, broker),
    CONSTRAINT market_futures_holding_pit_available_at_check CHECK (available_at >= trade_date)
);

CREATE TABLE IF NOT EXISTS market_futures_product_exposure_mapping_pit (
    product_symbol TEXT NOT NULL,
    exposure_type TEXT NOT NULL,
    exposure_code TEXT NOT NULL,
    direction SMALLINT NOT NULL,
    weight NUMERIC NOT NULL,
    valid_from DATE NOT NULL,
    valid_to DATE,
    available_at DATE NOT NULL,
    source TEXT NOT NULL,
    mapping_version TEXT NOT NULL,
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (product_symbol, exposure_type, exposure_code, valid_from, mapping_version),
    CONSTRAINT market_futures_product_exposure_direction_check CHECK (direction IN (-1, 1)),
    CONSTRAINT market_futures_product_exposure_weight_check CHECK (weight > 0 AND weight <= 1),
    CONSTRAINT market_futures_product_exposure_type_check CHECK (
        exposure_type IN ('sw_industry', 'stock_symbol')
    ),
    CONSTRAINT market_futures_product_exposure_interval_check CHECK (
        valid_to IS NULL OR valid_to >= valid_from
    )
);

CREATE TABLE IF NOT EXISTS market_futures_product_exclusion_gate_pit (
    product_symbol TEXT NOT NULL,
    gate_scope TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    valid_from DATE NOT NULL,
    valid_to DATE,
    available_at DATE NOT NULL,
    source TEXT NOT NULL,
    gate_version TEXT NOT NULL,
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (product_symbol, gate_scope, valid_from, gate_version),
    CONSTRAINT market_futures_product_exclusion_scope_check CHECK (
        gate_scope IN ('futures_price_chain_factor')
    ),
    CONSTRAINT market_futures_product_exclusion_reason_check CHECK (
        reason_code IN (
            'financial_index_future',
            'interest_rate_future',
            'non_industry_derivative',
            'ambiguous_product_symbol',
            'insufficient_industry_evidence'
        )
    ),
    CONSTRAINT market_futures_product_exclusion_interval_check CHECK (
        valid_to IS NULL OR valid_to >= valid_from
    )
);

CREATE TABLE IF NOT EXISTS market_futures_product_signal_pit (
    product_symbol TEXT NOT NULL,
    signal_code TEXT NOT NULL,
    source_version TEXT NOT NULL,
    trade_date DATE NOT NULL,
    available_at DATE NOT NULL,
    raw_value DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (signal_code, source_version, product_symbol, trade_date),
    CONSTRAINT market_futures_product_signal_available_at_check CHECK (available_at >= trade_date),
    CONSTRAINT market_futures_product_signal_code_check CHECK (
        signal_code IN (
            'fpc_price_mom_20v60_std',
            'fpc_inventory_tight_20v60_std',
            'fpc_net_position_20v60_std'
        )
    )
);

CREATE INDEX IF NOT EXISTS idx_market_futures_daily_available_at
    ON market_futures_daily (available_at, trade_date);

CREATE INDEX IF NOT EXISTS idx_market_futures_wsr_available_at
    ON market_futures_warehouse_receipt (available_at, trade_date);

CREATE INDEX IF NOT EXISTS idx_market_futures_holding_available_at
    ON market_futures_holding_rank (available_at, trade_date);

CREATE INDEX IF NOT EXISTS idx_market_futures_exposure_available_at
    ON market_futures_product_exposure_mapping_pit (available_at, valid_from, valid_to);

CREATE INDEX IF NOT EXISTS idx_market_futures_exclusion_available_at
    ON market_futures_product_exclusion_gate_pit (available_at, valid_from, valid_to);

CREATE INDEX IF NOT EXISTS idx_market_futures_product_signal_available_at
    ON market_futures_product_signal_pit
    (source_version, signal_code, available_at, product_symbol, trade_date);

CREATE INDEX IF NOT EXISTS idx_market_futures_product_signal_trade_date
    ON market_futures_product_signal_pit
    (source_version, signal_code, trade_date, product_symbol);

ALTER TABLE market_futures_daily
    ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'tushare',
    ADD COLUMN IF NOT EXISTS data_version_id TEXT;

ALTER TABLE market_futures_warehouse_receipt
    ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'tushare',
    ADD COLUMN IF NOT EXISTS data_version_id TEXT;

ALTER TABLE market_futures_holding_rank
    ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'tushare',
    ADD COLUMN IF NOT EXISTS data_version_id TEXT;

WITH futures_price_chain_direct_mapping_seed (
    product_symbol,
    exposure_type,
    exposure_code,
    product_name,
    industry_name,
    valid_from,
    review_note
) AS (
    VALUES
    ('AD', 'sw_industry', '801050.SI', '铸造铝合金', '有色金属(申万)', DATE '2025-06-10', 'Direct non-ferrous metal product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.'),
    ('AG', 'sw_industry', '801050.SI', '白银', '有色金属(申万)', DATE '2014-01-02', 'Direct precious metal product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.'),
    ('AL', 'sw_industry', '801050.SI', '铝', '有色金属(申万)', DATE '2014-01-02', 'Direct non-ferrous metal product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.'),
    ('AO', 'sw_industry', '801050.SI', '氧化铝', '有色金属(申万)', DATE '2023-06-19', 'Direct aluminium-chain product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.'),
    ('AU', 'sw_industry', '801050.SI', '黄金', '有色金属(申万)', DATE '2014-01-02', 'Direct precious metal product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.'),
    ('BC', 'sw_industry', '801050.SI', '国际铜', '有色金属(申万)', DATE '2020-11-19', 'Direct copper product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.'),
    ('CU', 'sw_industry', '801050.SI', '铜', '有色金属(申万)', DATE '2014-01-02', 'Direct copper product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.'),
    ('LC', 'sw_industry', '801050.SI', '碳酸锂', '有色金属(申万)', DATE '2023-07-21', 'Direct lithium-chain product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.'),
    ('NI', 'sw_industry', '801050.SI', '镍', '有色金属(申万)', DATE '2015-03-27', 'Direct non-ferrous metal product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.'),
    ('PB', 'sw_industry', '801050.SI', '铅', '有色金属(申万)', DATE '2014-01-02', 'Direct non-ferrous metal product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.'),
    ('PD', 'sw_industry', '801050.SI', '钯', '有色金属(申万)', DATE '2025-11-27', 'Direct precious metal product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.'),
    ('PT', 'sw_industry', '801050.SI', '铂', '有色金属(申万)', DATE '2025-11-27', 'Direct precious metal product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.'),
    ('SN', 'sw_industry', '801050.SI', '锡', '有色金属(申万)', DATE '2015-03-27', 'Direct non-ferrous metal product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.'),
    ('ZN', 'sw_industry', '801050.SI', '锌', '有色金属(申万)', DATE '2014-01-02', 'Direct non-ferrous metal product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.'),
    ('HC', 'sw_industry', '801040.SI', '热轧卷板', '钢铁(申万)', DATE '2014-03-21', 'Direct steel finished product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.'),
    ('RB', 'sw_industry', '801040.SI', '螺纹钢', '钢铁(申万)', DATE '2014-01-02', 'Direct steel finished product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.'),
    ('SS', 'sw_industry', '801040.SI', '不锈钢', '钢铁(申万)', DATE '2019-09-25', 'Direct steel finished product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.'),
    ('WR', 'sw_industry', '801040.SI', '线材', '钢铁(申万)', DATE '2014-01-02', 'Direct steel finished product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.'),
    ('J', 'sw_industry', '801950.SI', '焦炭', '煤炭(申万)', DATE '2014-01-02', 'Direct coal/coke-chain product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.'),
    ('JM', 'sw_industry', '801950.SI', '焦煤', '煤炭(申万)', DATE '2014-01-02', 'Direct coal/coke-chain product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.'),
    ('ZC', 'sw_industry', '801950.SI', '动力煤', '煤炭(申万)', DATE '2015-05-18', 'Direct coal product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.'),
    ('BU', 'sw_industry', '801960.SI', '石油沥青', '石油石化(申万)', DATE '2014-01-02', 'Direct petroleum product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.'),
    ('FU', 'sw_industry', '801960.SI', '燃料油', '石油石化(申万)', DATE '2014-01-02', 'Direct petroleum product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.'),
    ('LU', 'sw_industry', '801960.SI', '低硫燃料油', '石油石化(申万)', DATE '2020-06-22', 'Direct petroleum product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.'),
    ('PG', 'sw_industry', '801960.SI', '液化石油气', '石油石化(申万)', DATE '2020-03-30', 'Direct petroleum product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.'),
    ('SC', 'sw_industry', '801960.SI', '中质含硫原油', '石油石化(申万)', DATE '2018-03-26', 'Direct crude oil product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.'),
    ('BR', 'sw_industry', '801030.SI', '丁二烯橡胶', '基础化工(申万)', DATE '2023-07-28', 'Direct chemical product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.'),
    ('BZ', 'sw_industry', '801030.SI', '纯苯', '基础化工(申万)', DATE '2025-07-08', 'Direct chemical product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.'),
    ('EB', 'sw_industry', '801030.SI', '苯乙烯', '基础化工(申万)', DATE '2019-09-26', 'Direct chemical product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.'),
    ('EG', 'sw_industry', '801030.SI', '乙二醇', '基础化工(申万)', DATE '2018-12-10', 'Direct chemical product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.'),
    ('L', 'sw_industry', '801030.SI', '聚乙烯', '基础化工(申万)', DATE '2014-01-02', 'Direct chemical product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.'),
    ('MA', 'sw_industry', '801030.SI', '甲醇', '基础化工(申万)', DATE '2014-01-02', 'Direct chemical product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.'),
    ('PF', 'sw_industry', '801030.SI', '短纤', '基础化工(申万)', DATE '2020-10-12', 'Direct chemical fiber product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.'),
    ('PL', 'sw_industry', '801030.SI', '丙烯', '基础化工(申万)', DATE '2025-07-22', 'Direct chemical product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.'),
    ('PP', 'sw_industry', '801030.SI', '聚丙烯', '基础化工(申万)', DATE '2014-02-28', 'Direct chemical product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.'),
    ('PR', 'sw_industry', '801030.SI', '瓶片', '基础化工(申万)', DATE '2024-08-30', 'Direct chemical polyester-chain product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.'),
    ('PX', 'sw_industry', '801030.SI', '对二甲苯', '基础化工(申万)', DATE '2023-09-15', 'Direct chemical product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.'),
    ('SA', 'sw_industry', '801030.SI', '纯碱', '基础化工(申万)', DATE '2019-12-06', 'Direct chemical product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.'),
    ('SH', 'sw_industry', '801030.SI', '烧碱', '基础化工(申万)', DATE '2023-09-15', 'Direct chemical product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.'),
    ('TA', 'sw_industry', '801030.SI', 'PTA', '基础化工(申万)', DATE '2014-01-02', 'Direct chemical product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.'),
    ('UR', 'sw_industry', '801030.SI', '尿素', '基础化工(申万)', DATE '2019-08-09', 'Direct chemical fertilizer product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.'),
    ('V', 'sw_industry', '801030.SI', '聚氯乙烯', '基础化工(申万)', DATE '2014-01-02', 'Direct chemical product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.'),
    ('FG', 'sw_industry', '801710.SI', '玻璃', '建筑材料(申万)', DATE '2014-01-02', 'Direct glass building-material product observed from warehouse receipt fut_name; mapped only as a commodity producer price-chain proxy.')
)
INSERT INTO market_futures_product_exposure_mapping_pit (
    product_symbol,
    exposure_type,
    exposure_code,
    direction,
    weight,
    valid_from,
    valid_to,
    available_at,
    source,
    mapping_version,
    evidence
)
SELECT
    product_symbol,
    exposure_type,
    exposure_code,
    1,
    1.0,
    valid_from,
    NULL::date,
    valid_from,
    'fut_wsr_name_and_sw2021_l1_economic_link_review',
    'p319q-product-sw2021-l1-direct-commodity-v1',
    jsonb_build_object(
        'product_name', product_name,
        'industry_name', industry_name,
        'source_documents', jsonb_build_array(
            'market_futures_warehouse_receipt.fut_name',
            'market_stock_industry_membership_pit.SW2021_L1'
        ),
        'review_note', review_note
    )
FROM futures_price_chain_direct_mapping_seed
ON CONFLICT (product_symbol, exposure_type, exposure_code, valid_from, mapping_version) DO UPDATE
SET direction = EXCLUDED.direction,
    weight = EXCLUDED.weight,
    valid_to = EXCLUDED.valid_to,
    available_at = EXCLUDED.available_at,
    source = EXCLUDED.source,
    evidence = EXCLUDED.evidence,
    updated_at = now();

WITH futures_price_chain_evidence_backed_mapping_seed (
    product_symbol,
    exposure_type,
    exposure_code,
    direction,
    product_name,
    industry_name,
    valid_from,
    review_note
) AS (
    VALUES
    ('A', 'sw_industry', '801010.SI', 1, '豆一', '农林牧渔(申万)', DATE '2014-01-02', 'Agricultural commodity observed from warehouse receipt fut_name; mapped as a producer price-chain proxy.'),
    ('AP', 'sw_industry', '801010.SI', 1, '苹果', '农林牧渔(申万)', DATE '2017-12-22', 'Agricultural commodity observed from warehouse receipt fut_name; mapped as a producer price-chain proxy.'),
    ('B', 'sw_industry', '801010.SI', 1, '豆二', '农林牧渔(申万)', DATE '2014-01-02', 'Agricultural commodity observed from warehouse receipt fut_name; mapped as a producer price-chain proxy.'),
    ('C', 'sw_industry', '801010.SI', 1, '玉米', '农林牧渔(申万)', DATE '2014-01-02', 'Agricultural commodity observed from warehouse receipt fut_name; mapped as a producer price-chain proxy.'),
    ('CJ', 'sw_industry', '801010.SI', 1, '红枣', '农林牧渔(申万)', DATE '2019-04-30', 'Agricultural commodity observed from warehouse receipt fut_name; mapped as a producer price-chain proxy.'),
    ('CS', 'sw_industry', '801010.SI', 1, '玉米淀粉', '农林牧渔(申万)', DATE '2014-12-19', 'Corn-processing agricultural chain product observed from warehouse receipt fut_name; mapped as a producer price-chain proxy before P3.10 diagnostics.'),
    ('JD', 'sw_industry', '801010.SI', 1, '鸡蛋', '农林牧渔(申万)', DATE '2014-01-02', 'Agricultural commodity observed from warehouse receipt fut_name; mapped as a producer price-chain proxy.'),
    ('JR', 'sw_industry', '801010.SI', 1, '粳稻', '农林牧渔(申万)', DATE '2014-01-02', 'Agricultural commodity observed from warehouse receipt fut_name; mapped as a producer price-chain proxy.'),
    ('LG', 'sw_industry', '801010.SI', 1, '原木', '农林牧渔(申万)', DATE '2024-11-18', 'Forestry commodity observed from warehouse receipt fut_name; mapped as a producer price-chain proxy.'),
    ('LH', 'sw_industry', '801010.SI', 1, '生猪', '农林牧渔(申万)', DATE '2021-01-08', 'Agricultural commodity observed from warehouse receipt fut_name; mapped as a producer price-chain proxy.'),
    ('LR', 'sw_industry', '801010.SI', 1, '晚籼', '农林牧渔(申万)', DATE '2014-07-08', 'Agricultural commodity observed from warehouse receipt fut_name; mapped as a producer price-chain proxy.'),
    ('M', 'sw_industry', '801010.SI', 1, '豆粕', '农林牧渔(申万)', DATE '2014-01-02', 'Soybean meal agricultural chain product observed from warehouse receipt fut_name; mapped as a producer price-chain proxy before P3.10 diagnostics.'),
    ('NR', 'sw_industry', '801010.SI', 1, '20号胶', '农林牧渔(申万)', DATE '2019-08-12', 'Natural-rubber agricultural chain product observed from warehouse receipt fut_name; mapped as a producer price-chain proxy before P3.10 diagnostics.'),
    ('OI', 'sw_industry', '801010.SI', 1, '菜油', '农林牧渔(申万)', DATE '2014-01-02', 'Oilseed agricultural chain product observed from warehouse receipt fut_name; mapped as a producer price-chain proxy before P3.10 diagnostics.'),
    ('P', 'sw_industry', '801010.SI', 1, '棕榈油', '农林牧渔(申万)', DATE '2014-01-02', 'Oilseed agricultural chain product observed from warehouse receipt fut_name; mapped as a producer price-chain proxy before P3.10 diagnostics.'),
    ('PK', 'sw_industry', '801010.SI', 1, '花生', '农林牧渔(申万)', DATE '2021-02-01', 'Agricultural commodity observed from warehouse receipt fut_name; mapped as a producer price-chain proxy.'),
    ('PM', 'sw_industry', '801010.SI', 1, '普麦', '农林牧渔(申万)', DATE '2014-01-02', 'Agricultural commodity observed from warehouse receipt fut_name; mapped as a producer price-chain proxy.'),
    ('RI', 'sw_industry', '801010.SI', 1, '早籼', '农林牧渔(申万)', DATE '2014-01-02', 'Agricultural commodity observed from warehouse receipt fut_name; mapped as a producer price-chain proxy.'),
    ('RM', 'sw_industry', '801010.SI', 1, '菜粕', '农林牧渔(申万)', DATE '2014-01-02', 'Oilseed agricultural chain product observed from warehouse receipt fut_name; mapped as a producer price-chain proxy before P3.10 diagnostics.'),
    ('RR', 'sw_industry', '801010.SI', 1, '粳米', '农林牧渔(申万)', DATE '2019-08-16', 'Agricultural commodity observed from warehouse receipt fut_name; mapped as a producer price-chain proxy.'),
    ('RS', 'sw_industry', '801010.SI', 1, '菜籽', '农林牧渔(申万)', DATE '2014-01-02', 'Agricultural commodity observed from warehouse receipt fut_name; mapped as a producer price-chain proxy.'),
    ('RU', 'sw_industry', '801010.SI', 1, '天然橡胶', '农林牧渔(申万)', DATE '2014-01-02', 'Natural-rubber agricultural chain product observed from warehouse receipt fut_name; mapped as a producer price-chain proxy before P3.10 diagnostics.'),
    ('WH', 'sw_industry', '801010.SI', 1, '强筋小麦', '农林牧渔(申万)', DATE '2014-01-02', 'Agricultural commodity observed from warehouse receipt fut_name; mapped as a producer price-chain proxy.'),
    ('Y', 'sw_industry', '801010.SI', 1, '豆油', '农林牧渔(申万)', DATE '2014-01-02', 'Oilseed agricultural chain product observed from warehouse receipt fut_name; mapped as a producer price-chain proxy before P3.10 diagnostics.'),
    ('CF', 'sw_industry', '801130.SI', 1, '棉花', '纺织服饰(申万)', DATE '2014-01-02', 'Cotton textile-chain commodity observed from warehouse receipt fut_name; mapped as a textile price-chain proxy before P3.10 diagnostics.'),
    ('CY', 'sw_industry', '801130.SI', 1, '棉纱', '纺织服饰(申万)', DATE '2017-08-18', 'Cotton-yarn textile-chain commodity observed from warehouse receipt fut_name; mapped as a textile price-chain proxy before P3.10 diagnostics.'),
    ('SR', 'sw_industry', '801120.SI', 1, '白糖', '食品饮料(申万)', DATE '2014-01-02', 'Sugar food-chain commodity observed from warehouse receipt fut_name; mapped as a food producer price-chain proxy before P3.10 diagnostics.'),
    ('BB', 'sw_industry', '801140.SI', 1, '胶合板', '轻工制造(申万)', DATE '2014-01-02', 'Wood-board light-manufacturing chain product observed from warehouse receipt fut_name; mapped as a producer price-chain proxy before P3.10 diagnostics.'),
    ('FB', 'sw_industry', '801140.SI', 1, '纤维板', '轻工制造(申万)', DATE '2014-01-02', 'Wood-board light-manufacturing chain product observed from warehouse receipt fut_name; mapped as a producer price-chain proxy before P3.10 diagnostics.'),
    ('SP', 'sw_industry', '801140.SI', -1, '纸浆', '轻工制造(申万)', DATE '2018-11-27', 'Pulp is treated as an input-cost proxy for paper/light-manufacturing exposure; direction is pre-registered negative and must be tested only in P3.10 diagnostics.'),
    ('I', 'sw_industry', '801040.SI', -1, '铁矿石', '钢铁(申万)', DATE '2014-01-02', 'Iron ore is treated as an input-cost proxy for steel exposure; direction is pre-registered negative and must be tested only in P3.10 diagnostics.'),
    ('SF', 'sw_industry', '801040.SI', 1, '硅铁', '钢铁(申万)', DATE '2014-08-08', 'Ferroalloy product observed from warehouse receipt fut_name; mapped as a steel-chain producer price proxy before P3.10 diagnostics.'),
    ('SM', 'sw_industry', '801040.SI', 1, '锰硅', '钢铁(申万)', DATE '2014-08-08', 'Ferroalloy product observed from warehouse receipt fut_name; mapped as a steel-chain producer price proxy before P3.10 diagnostics.'),
    ('SI', 'sw_industry', '801050.SI', 1, '工业硅', '有色金属(申万)', DATE '2022-12-22', 'Industrial silicon commodity observed from warehouse receipt fut_name; mapped as a non-ferrous/small-metal producer price-chain proxy before P3.10 diagnostics.'),
    ('PS', 'sw_industry', '801730.SI', 1, '多晶硅', '电力设备(申万)', DATE '2024-12-26', 'Polysilicon commodity observed from warehouse receipt fut_name; mapped as a photovoltaic supply-chain price proxy before P3.10 diagnostics.')
)
INSERT INTO market_futures_product_exposure_mapping_pit (
    product_symbol,
    exposure_type,
    exposure_code,
    direction,
    weight,
    valid_from,
    valid_to,
    available_at,
    source,
    mapping_version,
    evidence
)
SELECT
    product_symbol,
    exposure_type,
    exposure_code,
    direction,
    1.0,
    valid_from,
    NULL::date,
    valid_from,
    'fut_wsr_name_and_sw2021_l1_economic_link_review',
    'p319q-product-sw2021-l1-evidence-backed-v1',
    jsonb_build_object(
        'product_name', product_name,
        'industry_name', industry_name,
        'source_documents', jsonb_build_array(
            'market_futures_warehouse_receipt.fut_name',
            'market_stock_industry_membership_pit.SW2021_L1'
        ),
        'review_note', review_note
    )
FROM futures_price_chain_evidence_backed_mapping_seed
ON CONFLICT (product_symbol, exposure_type, exposure_code, valid_from, mapping_version) DO UPDATE
SET direction = EXCLUDED.direction,
    weight = EXCLUDED.weight,
    valid_to = EXCLUDED.valid_to,
    available_at = EXCLUDED.available_at,
    source = EXCLUDED.source,
    evidence = EXCLUDED.evidence,
    updated_at = now();

WITH futures_price_chain_residual_mapping_seed (
    product_symbol,
    exposure_type,
    exposure_code,
    direction,
    product_name,
    industry_name,
    valid_from,
    source_url,
    review_note
) AS (
    VALUES
    ('EC', 'sw_industry', '801170.SI', 1, '集运指数（欧线）', '交通运输(申万)', DATE '2023-08-18', 'https://www.ine.cn/regulation/ineregulation/rules/202308/t20230811_814255.html', 'INE standard contract identifies EC as Shanghai export container settlement freight index Europe route; mapped as a transport freight-rate price proxy before P3.10 diagnostics.'),
    ('OP', 'sw_industry', '801140.SI', 1, '胶版印刷纸', '轻工制造(申万)', DATE '2025-09-10', 'https://www.shfe.com.cn/products/futures/energyandchemical/op_f/', 'SHFE product page identifies OP as offset printing paper futures; mapped as a light-manufacturing paper-chain price proxy before P3.10 diagnostics.'),
    ('ME', 'sw_industry', '801030.SI', 1, '甲醇', '基础化工(申万)', DATE '2014-01-02', 'https://www.scqh.com.cn/content/show/13/2265', 'Historical methanol code ME is treated as methanol economic exposure based on public rule-change evidence that later contracts use MA; mapped as a chemical price proxy before P3.10 diagnostics.'),
    ('TC', 'sw_industry', '801950.SI', 1, '动力煤', '煤炭(申万)', DATE '2014-01-02', 'https://www.czce.com.cn/cn/sspz/dlm/H077002012index_1.htm', 'Historical TC raw symbol is treated as thermal-coal economic exposure; mapped as a coal price proxy before P3.10 diagnostics.'),
    ('ER', 'sw_industry', '801010.SI', 1, '早籼稻', '农林牧渔(申万)', DATE '2014-01-02', 'http://futures.pingan.com/pinganqihuogonggao/122688.shtml', 'Historical ER code is treated as early indica rice exposure based on public code-change notice evidence; mapped as an agricultural price proxy before P3.10 diagnostics.'),
    ('WS', 'sw_industry', '801010.SI', 1, '强麦', '农林牧渔(申万)', DATE '2014-01-02', 'http://futures.pingan.com/pinganqihuogonggao/122688.shtml', 'Historical WS code is treated as strong wheat exposure based on public code-change notice evidence; mapped as an agricultural price proxy before P3.10 diagnostics.')
)
INSERT INTO market_futures_product_exposure_mapping_pit (
    product_symbol,
    exposure_type,
    exposure_code,
    direction,
    weight,
    valid_from,
    valid_to,
    available_at,
    source,
    mapping_version,
    evidence
)
SELECT
    product_symbol,
    exposure_type,
    exposure_code,
    direction,
    1.0,
    valid_from,
    NULL::date,
    valid_from,
    'official_contract_or_public_code_change_review',
    'p319q-product-sw2021-l1-official-residual-v1',
    jsonb_build_object(
        'product_name', product_name,
        'industry_name', industry_name,
        'source_urls', jsonb_build_array(source_url),
        'review_note', review_note
    )
FROM futures_price_chain_residual_mapping_seed
ON CONFLICT (product_symbol, exposure_type, exposure_code, valid_from, mapping_version) DO UPDATE
SET direction = EXCLUDED.direction,
    weight = EXCLUDED.weight,
    valid_to = EXCLUDED.valid_to,
    available_at = EXCLUDED.available_at,
    source = EXCLUDED.source,
    evidence = EXCLUDED.evidence,
    updated_at = now();

INSERT INTO market_futures_product_exclusion_gate_pit (
    product_symbol,
    gate_scope,
    reason_code,
    valid_from,
    valid_to,
    available_at,
    source,
    gate_version,
    evidence
) VALUES
    ('IF', 'futures_price_chain_factor', 'financial_index_future', DATE '2014-01-01', NULL, DATE '2014-01-01', 'cffex_official_product_category_review', 'p319o-cffex-financial-rate-exclusion-v1', '{"product_name":"沪深300股指期货","source_urls":["http://www.cffex.com.cn/cn/hs300.html","http://www.cffex.com.cn/cn/jycs.html"],"review_note":"CFFEX classifies IF as an equity index future. It is not a physical commodity or supply-chain price proxy, so it must not be mapped to a SW industry for futures_price_chain factors."}'::jsonb),
    ('IH', 'futures_price_chain_factor', 'financial_index_future', DATE '2014-01-01', NULL, DATE '2014-01-01', 'cffex_official_product_category_review', 'p319o-cffex-financial-rate-exclusion-v1', '{"product_name":"上证50股指期货","source_urls":["http://www.cffex.com.cn/cn/sz50gzqh.html","http://www.cffex.com.cn/cn/jycs.html"],"review_note":"CFFEX classifies IH as an equity index future. It is not a physical commodity or supply-chain price proxy, so it must not be mapped to a SW industry for futures_price_chain factors."}'::jsonb),
    ('IC', 'futures_price_chain_factor', 'financial_index_future', DATE '2014-01-01', NULL, DATE '2014-01-01', 'cffex_official_product_category_review', 'p319o-cffex-financial-rate-exclusion-v1', '{"product_name":"中证500股指期货","source_urls":["http://www.cffex.com.cn/cn/zz500.html","http://www.cffex.com.cn/cn/jycs.html"],"review_note":"CFFEX classifies IC as an equity index future. It is not a physical commodity or supply-chain price proxy, so it must not be mapped to a SW industry for futures_price_chain factors."}'::jsonb),
    ('TS', 'futures_price_chain_factor', 'interest_rate_future', DATE '2014-01-01', NULL, DATE '2014-01-01', 'cffex_official_product_category_review', 'p319o-cffex-financial-rate-exclusion-v1', '{"product_name":"2年期国债期货","source_urls":["http://www.cffex.com.cn/cn/2ts.html","http://www.cffex.com.cn/cn/jycs.html"],"review_note":"CFFEX classifies TS as a treasury bond / interest-rate future. It is not a physical commodity or supply-chain price proxy, so it must not be mapped to a SW industry for futures_price_chain factors."}'::jsonb),
    ('TF', 'futures_price_chain_factor', 'interest_rate_future', DATE '2014-01-01', NULL, DATE '2014-01-01', 'cffex_official_product_category_review', 'p319o-cffex-financial-rate-exclusion-v1', '{"product_name":"5年期国债期货","source_urls":["http://www.cffex.com.cn/cn/5tf.html","http://www.cffex.com.cn/cn/jycs.html"],"review_note":"CFFEX classifies TF as a treasury bond / interest-rate future. It is not a physical commodity or supply-chain price proxy, so it must not be mapped to a SW industry for futures_price_chain factors."}'::jsonb),
    ('T', 'futures_price_chain_factor', 'interest_rate_future', DATE '2014-01-01', NULL, DATE '2014-01-01', 'cffex_official_product_category_review', 'p319o-cffex-financial-rate-exclusion-v1', '{"product_name":"10年期国债期货","source_urls":["http://www.cffex.com.cn/cn/10t.html","http://www.cffex.com.cn/cn/jycs.html"],"review_note":"CFFEX classifies T as a treasury bond / interest-rate future. It is not a physical commodity or supply-chain price proxy, so it must not be mapped to a SW industry for futures_price_chain factors."}'::jsonb),
    ('TL', 'futures_price_chain_factor', 'interest_rate_future', DATE '2014-01-01', NULL, DATE '2014-01-01', 'cffex_official_product_category_review', 'p319o-cffex-financial-rate-exclusion-v1', '{"product_name":"30年期国债期货","source_urls":["http://www.cffex.com.cn/cn/30tl.html","http://www.cffex.com.cn/cn/jycs.html"],"review_note":"CFFEX classifies TL as a treasury bond / interest-rate future. It is not a physical commodity or supply-chain price proxy, so it must not be mapped to a SW industry for futures_price_chain factors."}'::jsonb),
    ('IM', 'futures_price_chain_factor', 'financial_index_future', DATE '2014-01-01', NULL, DATE '2014-01-01', 'cffex_official_product_category_review', 'p319p-cffex-derivative-exclusion-v1', '{"product_name":"中证1000股指期货","source_urls":["http://www.cffex.com.cn/cn/zz1000.html","http://www.cffex.com.cn/cn/jycs.html"],"review_note":"CFFEX classifies IM as an equity index future. It is not a physical commodity or supply-chain price proxy, so it must not be mapped to a SW industry for futures_price_chain factors."}'::jsonb),
    ('IO', 'futures_price_chain_factor', 'non_industry_derivative', DATE '2014-01-01', NULL, DATE '2014-01-01', 'cffex_official_product_category_review', 'p319p-cffex-derivative-exclusion-v1', '{"product_name":"沪深300股指期权","source_urls":["http://www.cffex.com.cn/cp/index.html","http://www.cffex.com.cn/cn/jycs.html"],"review_note":"CFFEX classifies IO as an equity index option. It is a derivative on an equity index rather than a physical commodity or supply-chain price proxy, so it must not be mapped to a SW industry for futures_price_chain factors."}'::jsonb),
    ('SCTAS', 'futures_price_chain_factor', 'non_industry_derivative', DATE '2020-10-12', NULL, DATE '2020-10-12', 'ine_tas_symbol_pattern_review', 'p319p-tas-derivative-exclusion-v1', '{"product_name":"原油 TAS","source_urls":["https://www.shfe.com.cn/publicnotice/notice/911404207.html","https://www.cmegroup.com/education/courses/introduction-to-trading-at-settlement.html"],"review_note":"SCTAS is treated as a crude-oil TAS contract/order-type symbol observed in INE raw data, not as an independent physical commodity product. Keep it out of futures_price_chain product-to-SW-industry mapping to avoid mixing TAS rows into the SC main commodity proxy."}'::jsonb)
ON CONFLICT (product_symbol, gate_scope, valid_from, gate_version) DO UPDATE
SET reason_code = EXCLUDED.reason_code,
    valid_to = EXCLUDED.valid_to,
    available_at = EXCLUDED.available_at,
    source = EXCLUDED.source,
    evidence = EXCLUDED.evidence,
    updated_at = now();
