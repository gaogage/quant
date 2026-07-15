-- A股交易规则模块5: market_stock 加 instrument_type 字段区分股票/ETF
--
-- 背景:原 sync_fund_basic 把 ETF 写入 market_stock 时 fund_type 只拼进 name,
-- 无独立字段区分股票 vs ETF/LOF。回测/实盘只能靠 symbol 前缀+name 启发式判断,
-- 不可靠(用户明确要求"不要用前缀+name判断ETF,应查系统字段")。
--
-- instrument_type 取值:
--   'stock'   A股股票(主板/创业板/科创板)
--   'etf'     ETF/LOF(场内基金)
--   'lof'     LOF(目前与 etf 同处理,保留区分)
--   'fund'    其他基金(货基/债基等,场外)
-- NULL 表示未回填(兼容历史数据),代码用 is_etf_symbol 兜底。

ALTER TABLE market_stock
    ADD COLUMN IF NOT EXISTS instrument_type varchar(16);

-- 回填:根据 symbol 代码段批量更新
-- ETF/LOF:沪市 51x/56x、深市 159xxx
UPDATE market_stock SET instrument_type = 'etf'
WHERE instrument_type IS NULL
  AND (symbol LIKE '51%' OR symbol LIKE '56%' OR symbol LIKE '159%');

-- 货币基金 511xxx/511990、债券基金 51xxxx 单独标(简化归 etf,场内交易)
-- 已被上面 51% 覆盖,无需额外

-- 剩余未标的默认 stock(A股股票)
UPDATE market_stock SET instrument_type = 'stock'
WHERE instrument_type IS NULL;

-- 索引:按品种查询
CREATE INDEX IF NOT EXISTS idx_market_stock_instrument_type
    ON market_stock(instrument_type);

-- ─────────────────────────────────────────────────────────────────
-- market_fund:基金专有完整信息(Tushare fund_basic 24 字段全量接入)
--
-- 背景:原 sync_fund_basic 把 fund_type 拼进 market_stock.name(如"沪深300ETF(股票型)"),
-- 丢失管理人/托管人/管理费/托管费/投资风格/业绩基准/成立日/发行份额等全部专有信息。
-- 用户要求"信息尽量完整接入,方便未来使用"。
--
-- 设计:market_stock.instrument_type 做快速股票/ETF 区分(回测/实盘查单字段);
--       market_fund 存基金专有完整字段,通过 symbol FK 关联 market_stock(1:1)。
--       两者分离:股票不占 fund 字段,fund 不污染 stock 表;回测只查 market_stock,
--       需要基金详情(费率/管理人/风格)时 JOIN market_fund。
--
-- 字段对应 Tushare fund_basic:
--   ts_code→symbol, name, management, custodian, fund_type(投资类型),
--   found_date, due_date, list_date, issue_date, delist_date, issue_amount,
--   m_fee(管理费), c_fee(托管费), duration_year, p_value, min_amount,
--   exp_return, benchmark, status, invest_type(投资风格), type(基金类型),
--   trustee, purc_startdate, redm_startdate, market(E场内/O场外)
CREATE TABLE IF NOT EXISTS market_fund (
    symbol          varchar(20)   PRIMARY KEY REFERENCES market_stock(symbol) ON DELETE CASCADE,
    name            varchar(100)  NOT NULL,
    management      varchar(128),                       -- 管理人
    custodian       varchar(128),                       -- 托管人
    fund_type       varchar(32),                        -- 投资类型(股票型/债券型/混合型/货币市场型/商品型/QDII...)
    found_date      date,                               -- 成立日期
    due_date        date,                               -- 到期日期
    list_date       date,                               -- 上市时间
    issue_date      date,                               -- 发行日期
    delist_date     date,                               -- 退市日期
    issue_amount    numeric(20,4),                      -- 发行份额(亿)
    m_fee           numeric(10,6),                      -- 管理费
    c_fee           numeric(10,6),                      -- 托管费
    duration_year   numeric(10,2),                      -- 存续期
    p_value         numeric(20,8),                      -- 面值
    min_amount      numeric(20,8),                      -- 起点金额(万元)
    exp_return      varchar(64),                        -- 预期收益率
    benchmark       text,                               -- 业绩比较基准
    status          varchar(8),                         -- 存续状态(D摘牌 I发行 L已上市)
    invest_type     varchar(32),                        -- 投资风格
    type            varchar(32),                        -- 基金类型
    trustee         varchar(128),                       -- 受托人
    purc_startdate  date,                               -- 日常申购起始日
    redm_startdate  date,                               -- 日常赎回起始日
    market          varchar(8),                         -- E场内 O场外
    created_at      timestamptz  NOT NULL DEFAULT now(),
    updated_at      timestamptz  NOT NULL DEFAULT now()
);

-- 索引:按投资类型/基金类型/管理人查询
CREATE INDEX IF NOT EXISTS idx_market_fund_fund_type   ON market_fund(fund_type);
CREATE INDEX IF NOT EXISTS idx_market_fund_type        ON market_fund(type);
CREATE INDEX IF NOT EXISTS idx_market_fund_management  ON market_fund(management);
CREATE INDEX IF NOT EXISTS idx_market_fund_market      ON market_fund(market);

-- updated_at 触发器(与 market_stock 一致)
CREATE TRIGGER trg_market_fund_updated_at
    BEFORE UPDATE ON market_fund
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

