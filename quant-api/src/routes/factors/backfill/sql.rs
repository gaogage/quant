//! phase7 因子回填 SQL 构建器：各因子族的 INSERT...SELECT PIT 回填语句生成。
use super::*;

pub(crate) fn phase7_factor_backfill_sql(spec: &Phase7BackfillFactorSpec) -> String {
    match spec.kind {
        Phase7BackfillFactorKind::Reversal => phase7_reversal_backfill_sql(spec.period),
        Phase7BackfillFactorKind::DownsideVolatility => {
            phase7_downside_volatility_backfill_sql(spec.period)
        }
        Phase7BackfillFactorKind::AmihudIlliquidity => phase7_amihud_backfill_sql(spec.period),
        Phase7BackfillFactorKind::AmountIntensity => {
            phase7_amount_intensity_backfill_sql(spec.period)
        }
        Phase7BackfillFactorKind::MarketRelativeMomentum => {
            phase7_relative_momentum_backfill_sql(spec.period, false)
        }
        Phase7BackfillFactorKind::IndustryRelativeMomentum => {
            phase7_relative_momentum_backfill_sql(spec.period, true)
        }
        Phase7BackfillFactorKind::FinancialLatest {
            source_column,
            higher_is_better,
        } => phase7_financial_latest_backfill_sql(source_column, higher_is_better),
        Phase7BackfillFactorKind::IndustryRelativeFinancialLatest {
            source_column,
            higher_is_better,
        } => {
            phase7_industry_relative_financial_latest_backfill_sql(source_column, higher_is_better)
        }
        Phase7BackfillFactorKind::DailyBasicLatest {
            source_column,
            higher_is_better,
            positive_only,
        } => phase7_daily_basic_latest_backfill_sql(source_column, higher_is_better, positive_only),
        Phase7BackfillFactorKind::MoneyflowRolling {
            amount_expression,
            higher_is_better,
        } => phase7_moneyflow_backfill_sql(spec.period, amount_expression, higher_is_better),
        Phase7BackfillFactorKind::MoneyflowCongestionInteraction { flow_expression } => {
            phase7_moneyflow_congestion_backfill_sql(spec.period, flow_expression)
        }
        Phase7BackfillFactorKind::SupplyFloatShock {
            share_expression,
            horizon_days,
            mode,
        } => phase7_supply_float_shock_backfill_sql(share_expression, horizon_days, mode),
        Phase7BackfillFactorKind::CashflowLatest {
            value_expression,
            required_filter,
            higher_is_better,
        } => {
            phase7_cashflow_latest_backfill_sql(value_expression, required_filter, higher_is_better)
        }
        Phase7BackfillFactorKind::DividendRollingQuality {
            value_expression,
            higher_is_better,
        } => phase7_dividend_rolling_quality_backfill_sql(value_expression, higher_is_better),
        Phase7BackfillFactorKind::EventLatest {
            source_table,
            value_expression,
            higher_is_better,
        } => phase7_event_latest_backfill_sql(source_table, value_expression, higher_is_better),
        Phase7BackfillFactorKind::BlockTradeWindow {
            value_expression,
            higher_is_better,
            window_days,
            decay_days,
        } => phase7_block_trade_window_backfill_sql(
            value_expression,
            higher_is_better,
            window_days,
            decay_days,
        ),
        Phase7BackfillFactorKind::UnlockPressure { horizon_days } => {
            phase7_unlock_pressure_backfill_sql(horizon_days)
        }
        Phase7BackfillFactorKind::LiquidityQuality {
            signal,
            short_window,
            long_window,
        } => phase7_liquidity_quality_backfill_sql(signal, short_window, long_window),
        Phase7BackfillFactorKind::MarketResidualRisk {
            signal,
            short_window,
            long_window,
        } => phase7_market_residual_risk_backfill_sql(signal, short_window, long_window),
        Phase7BackfillFactorKind::IndustryProsperity {
            signal,
            short_window,
            long_window,
        } => phase7_industry_prosperity_backfill_sql(signal, short_window, long_window),
        Phase7BackfillFactorKind::FuturesPriceChain { signal } => {
            phase7_futures_price_chain_backfill_sql(signal)
        }
        Phase7BackfillFactorKind::EquityPledgePressure => {
            panic!("equity_pledge_pressure must use the dedicated PIT combo builder")
        }
        Phase7BackfillFactorKind::ShareholderStructure => {
            panic!(
                "shareholder_structure must use the dedicated strict PIT low-fanout combo builder"
            )
        }
        Phase7BackfillFactorKind::MarginDetailLeverageCrowding => {
            panic!("margin_detail must use the dedicated next-session PIT combo builder")
        }
        Phase7BackfillFactorKind::AnalystRevision {
            value_expression,
            higher_is_better,
            window_days,
            decay_days,
        } => phase7_analyst_revision_backfill_sql(
            value_expression,
            higher_is_better,
            window_days,
            decay_days,
        ),
        Phase7BackfillFactorKind::ForecastRevision {
            value_expression,
            higher_is_better,
            max_event_age_days,
        } => phase7_forecast_revision_backfill_sql(
            value_expression,
            higher_is_better,
            max_event_age_days,
        ),
        Phase7BackfillFactorKind::EventWindow {
            source_table,
            value_expression,
            higher_is_better,
            window_days,
            decay_days,
        } => phase7_event_window_backfill_sql(
            source_table,
            value_expression,
            higher_is_better,
            window_days,
            decay_days,
        ),
        Phase7BackfillFactorKind::EventPostReturnCurve {
            source_table,
            event_filter_expression,
            higher_is_better,
            window_days,
            industry_relative,
            min_event_age_days,
            max_event_age_days,
        } => phase7_event_post_return_curve_backfill_sql(
            source_table,
            event_filter_expression,
            higher_is_better,
            window_days,
            industry_relative,
            min_event_age_days,
            max_event_age_days,
        ),
        Phase7BackfillFactorKind::FinancialAnnualChange {
            source_column,
            mode,
        } => phase7_financial_annual_change_backfill_sql(source_column, mode),
        Phase7BackfillFactorKind::FinancialAnnualAcceleration {
            source_column,
            mode,
        } => phase7_financial_annual_acceleration_backfill_sql(source_column, mode),
        Phase7BackfillFactorKind::FinancialAnnualPersistence {
            source_column,
            mode,
        } => phase7_financial_annual_persistence_backfill_sql(source_column, mode),
        Phase7BackfillFactorKind::LargeCapMomentumReversal {
            reversal_period,
            momentum_period,
            large_cap_threshold_yi,
        } => phase7_large_cap_momentum_reversal_backfill_sql(
            reversal_period,
            momentum_period,
            large_cap_threshold_yi,
        ),
        Phase7BackfillFactorKind::DefensiveLowVolQuality {
            volatility_period,
            industries,
        } => phase7_defensive_low_vol_quality_backfill_sql(volatility_period, industries),
    }
}

pub(crate) fn phase7_reversal_backfill_sql(period: i32) -> String {
    format!(
        "WITH priced AS (
            SELECT
                symbol,
                trade_date,
                close::double precision AS close,
                LAG(close::double precision, {period}) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                ) AS prev_close
            FROM market_stock_daily_bar_adj
            WHERE close IS NOT NULL
              AND trade_date <= $4
        ),
        raw AS (
            SELECT
                symbol,
                trade_date,
                (prev_close - close) / NULLIF(prev_close, 0.0) AS raw_value
            FROM priced
            WHERE trade_date BETWEEN $3 AND $4
              AND prev_close IS NOT NULL
              AND prev_close <> 0.0
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                raw_value,
                percent_rank() OVER (PARTITION BY trade_date ORDER BY raw_value) AS normalized_value
            FROM raw
            WHERE raw_value IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, trade_date
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

/// P4.2b 大盘动量反转交互特征 backfill SQL。
/// 大盘股池(total_mv > 阈值,PIT)内,reversal × momentum 交互:
/// raw_value = CUME_DIST(reversal) × CUME_DIST(momentum),捕获"既超跌又强势"的非线性协同。
/// reversal = (prev_rev_close - close)/prev_rev_close(超跌为正),
/// momentum = (close - prev_mom_close)/prev_mom_close(强势为正)。
/// 两个 CUME_DIST ∈ [0,1],乘积 ∈ [0,1],高=同时超跌+强势。
/// PIT:量价当日已知;市值取 trade_date<=当日 最近值;available_at = trade_date。
pub(crate) fn phase7_large_cap_momentum_reversal_backfill_sql(
    reversal_period: i32,
    momentum_period: i32,
    large_cap_threshold_yi: i32,
) -> String {
    // total_mv 单位为万元,阈值亿元 → 万元 = ×1e4
    let threshold_wan: i64 = (large_cap_threshold_yi as i64) * 10_000;
    format!(
        "WITH priced AS (
            SELECT
                symbol,
                trade_date,
                close::double precision AS close,
                LAG(close::double precision, {reversal_period}) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                ) AS prev_rev_close,
                LAG(close::double precision, {momentum_period}) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                ) AS prev_mom_close
            FROM market_stock_daily_bar_adj
            WHERE close IS NOT NULL AND trade_date <= $4
              AND trade_date >= ($3::date - INTERVAL '120 days')
        ),
        -- 大盘股池:用 start_date($3) 当日市值固定分桶(PIT,与 P4.1c 口径一致),
        -- 避免逐日 LATERAL 市值查询的性能开销。大盘股池相对稳定,固定分桶是可接受的简化。
        large_cap_symbols AS (
            SELECT DISTINCT ON (symbol) symbol
            FROM market_stock_daily_basic
            WHERE trade_date <= $3 AND total_mv > {threshold_wan}
            ORDER BY symbol, trade_date DESC
        ),
        large_cap AS (
            SELECT p.symbol, p.trade_date, p.close, p.prev_rev_close, p.prev_mom_close
            FROM priced p
            JOIN large_cap_symbols l ON p.symbol = l.symbol
            WHERE p.trade_date BETWEEN $3 AND $4
              AND p.prev_rev_close IS NOT NULL AND p.prev_mom_close IS NOT NULL
              AND p.prev_rev_close <> 0.0 AND p.prev_mom_close <> 0.0
        ),
        raw AS (
            SELECT
                symbol,
                trade_date,
                (prev_rev_close - close) / NULLIF(prev_rev_close, 0.0) AS reversal,
                (close - prev_mom_close) / NULLIF(prev_mom_close, 0.0) AS momentum
            FROM large_cap
            WHERE prev_rev_close <> 0.0 AND prev_mom_close <> 0.0
        ),
        interaction AS (
            SELECT
                symbol,
                trade_date,
                CUME_DIST() OVER (PARTITION BY trade_date ORDER BY reversal) AS rev_dist,
                CUME_DIST() OVER (PARTITION BY trade_date ORDER BY momentum) AS mom_dist
            FROM raw
            WHERE reversal IS NOT NULL AND momentum IS NOT NULL
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                rev_dist * mom_dist AS raw_value,
                percent_rank() OVER (PARTITION BY trade_date ORDER BY rev_dist * mom_dist) AS normalized_value
            FROM interaction
            WHERE rev_dist IS NOT NULL AND mom_dist IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, trade_date
        FROM ranked
        WHERE raw_value IS NOT NULL
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

/// P4.2b 防御板块低波质量交互特征 backfill SQL。
/// 防御行业池(银行/保险/白酒/黄金/机场/电信运营/啤酒)内,low_volatility × fin_roe 交互:
/// raw_value = CUME_DIST(-volatility) × CUME_DIST(fin_roe),捕获"既低波又高质量"的非线性协同。
/// 低波(防御性)+ 高质量(高 ROE)是防御板块的核心选股逻辑,补偿 ascending 在此失效。
/// PIT:量价当日已知;fin_roe 用 available_at <= trade_date;available_at = trade_date。
pub(crate) fn phase7_defensive_low_vol_quality_backfill_sql(
    volatility_period: i32,
    industries: &[&str],
) -> String {
    let preceding = volatility_period - 1;
    // 行业列表展开为 SQL IN 列表:'银行','白酒',...
    let industry_list: String = industries
        .iter()
        .map(|s| format!("'{}'", s))
        .collect::<Vec<_>>()
        .join(",");
    // 任务80: C类特许 → env 化（默认=原写死值）
    let factor_version = crate::routes::shared::factor_version();
    format!(
        "WITH ret AS (
            SELECT
                symbol,
                trade_date,
                CASE
                    WHEN prev_close > 0.0 THEN (close - prev_close) / prev_close
                    ELSE NULL
                END AS ret
            FROM (
                SELECT
                    symbol,
                    trade_date,
                    close::double precision AS close,
                    LAG(close::double precision) OVER (
                        PARTITION BY symbol ORDER BY trade_date
                    ) AS prev_close
                FROM market_stock_daily_bar_adj
                WHERE close IS NOT NULL
                  AND trade_date <= $4
                  AND trade_date >= ($3::date - INTERVAL '120 days')
            ) bars
        ),
        vol AS (
            SELECT
                symbol,
                trade_date,
                SQRT(
                    AVG(POWER(ret, 2)) OVER (
                        PARTITION BY symbol ORDER BY trade_date
                        ROWS BETWEEN {preceding} PRECEDING AND CURRENT ROW
                    ) * 252.0
                ) AS volatility
            FROM ret
            WHERE ret IS NOT NULL
        ),
        defensive AS (
            SELECT v.symbol, v.trade_date, v.volatility, fv.normalized_value AS fin_roe
            FROM vol v
            JOIN market_stock ms ON v.symbol = ms.symbol
            JOIN LATERAL (
                SELECT normalized_value FROM factor_value
                WHERE factor_code = 'fin_roe_daily_std'
                  AND factor_version = '{factor_version}'
                  AND symbol = v.symbol
                  AND trade_date = v.trade_date
                  AND normalized_value IS NOT NULL
                  AND available_at <= v.trade_date
                ORDER BY available_at DESC LIMIT 1
            ) fv ON true
            WHERE v.trade_date BETWEEN $3 AND $4
              AND v.volatility IS NOT NULL AND v.volatility > 0.0
              AND ms.industry IN ({industry_list})
        ),
        interaction AS (
            SELECT
                symbol,
                trade_date,
                volatility,
                fin_roe,
                CUME_DIST() OVER (PARTITION BY trade_date ORDER BY -volatility) AS lowvol_dist,
                CUME_DIST() OVER (PARTITION BY trade_date ORDER BY fin_roe) AS quality_dist
            FROM defensive
            WHERE fin_roe IS NOT NULL
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                lowvol_dist * quality_dist AS raw_value,
                percent_rank() OVER (PARTITION BY trade_date ORDER BY lowvol_dist * quality_dist) AS normalized_value
            FROM interaction
            WHERE lowvol_dist IS NOT NULL AND quality_dist IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, trade_date
        FROM ranked
        WHERE raw_value IS NOT NULL
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_downside_volatility_backfill_sql(period: i32) -> String {
    let preceding = period - 1;
    format!(
        "WITH returns AS (
            SELECT
                symbol,
                trade_date,
                CASE
                    WHEN prev_close > 0.0 THEN (close - prev_close) / prev_close
                    ELSE NULL
                END AS ret
            FROM (
                SELECT
                    symbol,
                    trade_date,
                    close::double precision AS close,
                    LAG(close::double precision) OVER (
                        PARTITION BY symbol ORDER BY trade_date
                    ) AS prev_close
                FROM market_stock_daily_bar_adj
                WHERE close IS NOT NULL
                  AND trade_date <= $4
            ) bars
        ),
        raw AS (
            SELECT
                symbol,
                trade_date,
                SQRT(
                    AVG(POWER(LEAST(ret, 0.0), 2)) OVER (
                        PARTITION BY symbol ORDER BY trade_date
                        ROWS BETWEEN {preceding} PRECEDING AND CURRENT ROW
                    ) * 252.0
                ) AS raw_value,
                COUNT(ret) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {preceding} PRECEDING AND CURRENT ROW
                ) AS obs_count
            FROM returns
            WHERE ret IS NOT NULL
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                raw_value,
                percent_rank() OVER (PARTITION BY trade_date ORDER BY raw_value) AS normalized_value
            FROM raw
            WHERE trade_date BETWEEN $3 AND $4
              AND obs_count = {period}
              AND raw_value IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, trade_date
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_amihud_backfill_sql(period: i32) -> String {
    let preceding = period - 1;
    format!(
        "WITH observations AS (
            SELECT
                symbol,
                trade_date,
                CASE
                    WHEN prev_close > 0.0 AND amount > 0.0
                        THEN ABS((close - prev_close) / prev_close) / amount * 1000000000.0
                    ELSE NULL
                END AS illiquidity
            FROM (
                SELECT
                    symbol,
                    trade_date,
                    close::double precision AS close,
                    amount::double precision AS amount,
                    LAG(close::double precision) OVER (
                        PARTITION BY symbol ORDER BY trade_date
                    ) AS prev_close
                FROM market_stock_daily_bar_adj
                WHERE close IS NOT NULL
                  AND amount IS NOT NULL
                  AND trade_date <= $4
            ) bars
        ),
        raw AS (
            SELECT
                symbol,
                trade_date,
                AVG(illiquidity) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {preceding} PRECEDING AND CURRENT ROW
                ) AS raw_value,
                COUNT(illiquidity) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {preceding} PRECEDING AND CURRENT ROW
                ) AS obs_count
            FROM observations
            WHERE illiquidity IS NOT NULL
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                raw_value,
                percent_rank() OVER (PARTITION BY trade_date ORDER BY raw_value) AS normalized_value
            FROM raw
            WHERE trade_date BETWEEN $3 AND $4
              AND obs_count = {period}
              AND raw_value IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, trade_date
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_amount_intensity_backfill_sql(period: i32) -> String {
    format!(
        "WITH raw AS (
            SELECT
                symbol,
                trade_date,
                amount::double precision
                    / NULLIF(
                        AVG(amount::double precision) OVER (
                            PARTITION BY symbol ORDER BY trade_date
                            ROWS BETWEEN {period} PRECEDING AND 1 PRECEDING
                        ),
                        0.0
                    ) AS raw_value,
                COUNT(amount) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {period} PRECEDING AND 1 PRECEDING
                ) AS obs_count
            FROM market_stock_daily_bar_adj
            WHERE amount IS NOT NULL
              AND trade_date <= $4
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                raw_value,
                percent_rank() OVER (PARTITION BY trade_date ORDER BY raw_value) AS normalized_value
            FROM raw
            WHERE trade_date BETWEEN $3 AND $4
              AND obs_count = {period}
              AND raw_value IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, trade_date
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_relative_momentum_backfill_sql(
    period: i32,
    industry_relative: bool,
) -> String {
    let baseline_select = if industry_relative {
        "trade_date, industry, AVG(stock_return) AS baseline_return"
    } else {
        "trade_date, AVG(stock_return) AS baseline_return"
    };
    let baseline_group_by = if industry_relative {
        "trade_date, industry"
    } else {
        "trade_date"
    };
    let baseline_join = if industry_relative {
        "bl.trade_date = sr.trade_date AND bl.industry = sr.industry"
    } else {
        "bl.trade_date = sr.trade_date"
    };

    format!(
        "WITH stock_returns AS (
            SELECT
                bars.symbol,
                bars.trade_date,
                COALESCE(NULLIF(ms.industry, ''), 'UNKNOWN') AS industry,
                CASE
                    WHEN bars.prev_close > 0.0 THEN (bars.close - bars.prev_close) / bars.prev_close
                    ELSE NULL
                END AS stock_return
            FROM (
                SELECT
                    symbol,
                    trade_date,
                    close::double precision AS close,
                    LAG(close::double precision, {period}) OVER (
                        PARTITION BY symbol ORDER BY trade_date
                    ) AS prev_close
                FROM market_stock_daily_bar_adj
                WHERE close IS NOT NULL
                  AND trade_date <= $4
            ) bars
            JOIN market_stock ms ON ms.symbol = bars.symbol
            WHERE ms.list_status = 'L'
        ),
        baseline AS (
            SELECT {baseline_select}
            FROM stock_returns
            WHERE stock_return IS NOT NULL
            GROUP BY {baseline_group_by}
        ),
        raw AS (
            SELECT
                sr.symbol,
                sr.trade_date,
                sr.stock_return - bl.baseline_return AS raw_value
            FROM stock_returns sr
            JOIN baseline bl ON {baseline_join}
            WHERE sr.trade_date BETWEEN $3 AND $4
              AND sr.stock_return IS NOT NULL
              AND bl.baseline_return IS NOT NULL
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                raw_value,
                percent_rank() OVER (PARTITION BY trade_date ORDER BY raw_value) AS normalized_value
            FROM raw
            WHERE raw_value IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, trade_date
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_financial_latest_backfill_sql(
    source_column: &'static str,
    higher_is_better: bool,
) -> String {
    let rank_order = if higher_is_better {
        "raw_value"
    } else {
        "raw_value DESC"
    };
    format!(
        "WITH trade_days AS (
            SELECT trade_date
            FROM market_trade_calendar
            WHERE exchange = 'SSE'
              AND is_open = true
              AND trade_date BETWEEN $3 AND $4
        ),
        symbols AS (
            SELECT DISTINCT ts_code AS symbol
            FROM market_financial_indicator
            WHERE {source_column} IS NOT NULL
        ),
        latest AS (
            SELECT
                symbols.symbol,
                td.trade_date,
                fi.ann_date,
                fi.raw_value
            FROM symbols
            JOIN trade_days td ON true
            JOIN LATERAL (
                SELECT
                    ann_date,
                    {source_column}::double precision AS raw_value
                FROM market_financial_indicator fi
                WHERE fi.ts_code = symbols.symbol
                  AND fi.ann_date <= td.trade_date
                  AND fi.{source_column} IS NOT NULL
                ORDER BY fi.ann_date DESC, fi.end_date DESC
                LIMIT 1
            ) fi ON true
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                ann_date,
                raw_value,
                percent_rank() OVER (PARTITION BY trade_date ORDER BY {rank_order}) AS normalized_value
            FROM latest
            WHERE raw_value IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, ann_date
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_industry_relative_financial_latest_backfill_sql(
    source_column: &'static str,
    higher_is_better: bool,
) -> String {
    let rank_order = if higher_is_better {
        "raw_value"
    } else {
        "raw_value DESC"
    };
    format!(
        "WITH trade_days AS (
            SELECT trade_date
            FROM market_trade_calendar
            WHERE exchange = 'SSE'
              AND is_open = true
              AND trade_date BETWEEN $3 AND $4
        ),
        symbols AS (
            SELECT DISTINCT ts_code AS symbol
            FROM market_financial_indicator
            WHERE {source_column} IS NOT NULL
        ),
        latest AS (
            SELECT
                symbols.symbol,
                td.trade_date,
                COALESCE(NULLIF(ms.industry, ''), 'UNKNOWN') AS industry,
                fi.ann_date,
                fi.raw_value
            FROM symbols
            JOIN trade_days td ON true
            JOIN market_stock ms ON ms.symbol = symbols.symbol
            JOIN LATERAL (
                SELECT
                    ann_date,
                    {source_column}::double precision AS raw_value
                FROM market_financial_indicator fi
                WHERE fi.ts_code = symbols.symbol
                  AND fi.ann_date <= td.trade_date
                  AND fi.{source_column} IS NOT NULL
                ORDER BY fi.ann_date DESC, fi.end_date DESC
                LIMIT 1
            ) fi ON true
        ),
        residualized AS (
            SELECT
                symbol,
                trade_date,
                ann_date,
                raw_value - AVG(raw_value) OVER (
                    PARTITION BY trade_date, industry
                ) AS raw_value
            FROM latest
            WHERE raw_value IS NOT NULL
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                ann_date,
                raw_value,
                percent_rank() OVER (PARTITION BY trade_date ORDER BY {rank_order}) AS normalized_value
            FROM residualized
            WHERE raw_value IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, ann_date
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_financial_annual_change_backfill_sql(
    source_column: &'static str,
    mode: FinancialAnnualChangeMode,
) -> String {
    let raw_expression = match mode {
        FinancialAnnualChangeMode::PercentChange => {
            "(latest.raw_value - prev.raw_value) / NULLIF(ABS(prev.raw_value), 0.0)"
        }
        FinancialAnnualChangeMode::Difference => "latest.raw_value - prev.raw_value",
        FinancialAnnualChangeMode::Decrease => "prev.raw_value - latest.raw_value",
    };

    format!(
        "WITH trade_days AS (
            SELECT trade_date
            FROM market_trade_calendar
            WHERE exchange = 'SSE'
              AND is_open = true
              AND trade_date BETWEEN $3 AND $4
        ),
        symbols AS (
            SELECT DISTINCT ts_code AS symbol
            FROM market_financial_indicator
            WHERE {source_column} IS NOT NULL
        ),
        latest AS (
            SELECT
                symbols.symbol,
                td.trade_date,
                fi.ann_date,
                fi.end_date,
                fi.raw_value
            FROM symbols
            JOIN trade_days td ON true
            JOIN LATERAL (
                SELECT
                    ann_date,
                    end_date,
                    {source_column}::double precision AS raw_value
                FROM market_financial_indicator fi
                WHERE fi.ts_code = symbols.symbol
                  AND fi.ann_date <= td.trade_date
                  AND fi.{source_column} IS NOT NULL
                ORDER BY fi.ann_date DESC, fi.end_date DESC
                LIMIT 1
            ) fi ON true
        ),
        matched AS (
            SELECT
                latest.symbol,
                latest.trade_date,
                latest.ann_date,
                latest.end_date,
                latest.raw_value AS current_raw_value,
                prev.ann_date AS prev_ann_date,
                prev.raw_value AS previous_raw_value,
                {raw_expression} AS raw_value
            FROM latest
            JOIN LATERAL (
                SELECT
                    ann_date,
                    end_date,
                    {source_column}::double precision AS raw_value
                FROM market_financial_indicator prev
                WHERE prev.ts_code = latest.symbol
                  AND prev.end_date = (latest.end_date - INTERVAL '1 year')::date
                  AND prev.ann_date <= latest.trade_date
                  AND prev.{source_column} IS NOT NULL
                ORDER BY prev.ann_date DESC
                LIMIT 1
            ) prev ON true
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                GREATEST(ann_date, prev_ann_date) AS available_at,
                raw_value,
                percent_rank() OVER (PARTITION BY trade_date ORDER BY raw_value) AS normalized_value
            FROM matched
            WHERE raw_value IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, available_at
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_financial_annual_acceleration_backfill_sql(
    source_column: &'static str,
    mode: FinancialAnnualChangeMode,
) -> String {
    let latest_yoy_expression = match mode {
        FinancialAnnualChangeMode::PercentChange => {
            "(latest.raw_value - latest_prev.raw_value) / NULLIF(ABS(latest_prev.raw_value), 0.0)"
        }
        FinancialAnnualChangeMode::Difference | FinancialAnnualChangeMode::Decrease => {
            "latest.raw_value - latest_prev.raw_value"
        }
    };
    let prior_yoy_expression = match mode {
        FinancialAnnualChangeMode::PercentChange => {
            "(prior_latest.raw_value - prior_prev.raw_value) / NULLIF(ABS(prior_prev.raw_value), 0.0)"
        }
        FinancialAnnualChangeMode::Difference | FinancialAnnualChangeMode::Decrease => {
            "prior_latest.raw_value - prior_prev.raw_value"
        }
    };
    let raw_expression = match mode {
        FinancialAnnualChangeMode::Decrease => "prior_yoy.raw_value - latest_yoy.raw_value",
        FinancialAnnualChangeMode::PercentChange | FinancialAnnualChangeMode::Difference => {
            "latest_yoy.raw_value - prior_yoy.raw_value"
        }
    };

    format!(
        "WITH trade_days AS (
            SELECT trade_date
            FROM market_trade_calendar
            WHERE exchange = 'SSE'
              AND is_open = true
              AND trade_date BETWEEN $3 AND $4
        ),
        source_reports AS (
            SELECT
                latest.ts_code AS symbol,
                latest.ann_date,
                latest.end_date,
                latest.{source_column}::double precision AS raw_value
            FROM market_financial_indicator latest
            WHERE latest.{source_column} IS NOT NULL
              AND latest.ann_date <= $4
        ),
        yoy_points AS (
            SELECT
                latest.symbol,
                latest.ann_date AS latest_ann_date,
                latest.end_date,
                GREATEST(latest.ann_date, latest_prev.ann_date, prior_latest.ann_date, prior_prev.ann_date) AS available_at,
                {raw_expression} AS raw_value
            FROM source_reports latest
            JOIN LATERAL (
                SELECT
                    ann_date,
                    end_date,
                    {source_column}::double precision AS raw_value
                FROM market_financial_indicator latest_prev
                WHERE latest_prev.ts_code = latest.symbol
                  AND latest_prev.end_date = (latest.end_date - INTERVAL '1 year')::date
                  AND latest_prev.{source_column} IS NOT NULL
                ORDER BY latest_prev.ann_date DESC
                LIMIT 1
            ) latest_prev ON true
            JOIN LATERAL (
                SELECT
                    ann_date,
                    end_date,
                    {source_column}::double precision AS raw_value
                FROM market_financial_indicator prior_latest
                WHERE prior_latest.ts_code = latest.symbol
                  AND prior_latest.ann_date < latest.ann_date
                  AND prior_latest.end_date < latest.end_date
                  AND prior_latest.{source_column} IS NOT NULL
                ORDER BY prior_latest.ann_date DESC, prior_latest.end_date DESC
                LIMIT 1
            ) prior_latest ON true
            JOIN LATERAL (
                SELECT
                    ann_date,
                    end_date,
                    {source_column}::double precision AS raw_value
                FROM market_financial_indicator prior_prev
                WHERE prior_prev.ts_code = latest.symbol
                  AND prior_prev.end_date = (prior_latest.end_date - INTERVAL '1 year')::date
                  AND prior_prev.{source_column} IS NOT NULL
                ORDER BY prior_prev.ann_date DESC
                LIMIT 1
            ) prior_prev ON true
            CROSS JOIN LATERAL (
                SELECT {latest_yoy_expression} AS raw_value
            ) latest_yoy
            CROSS JOIN LATERAL (
                SELECT {prior_yoy_expression} AS raw_value
            ) prior_yoy
            WHERE latest_yoy.raw_value IS NOT NULL
              AND prior_yoy.raw_value IS NOT NULL
        ),
        deduped_points AS (
            SELECT DISTINCT ON (symbol, available_at)
                symbol,
                latest_ann_date,
                end_date,
                available_at,
                raw_value
            FROM yoy_points
            WHERE available_at <= $4
              AND raw_value IS NOT NULL
            ORDER BY symbol, available_at, latest_ann_date DESC, end_date DESC
        ),
        yoy_intervals AS (
            SELECT
                symbol,
                available_at,
                LEAD(available_at) OVER (
                    PARTITION BY symbol ORDER BY available_at
                ) AS next_available_at,
                raw_value
            FROM deduped_points
        ),
        raw AS (
            SELECT
                yi.symbol,
                td.trade_date,
                yi.available_at,
                yi.raw_value
            FROM yoy_intervals yi
            JOIN trade_days td
              ON td.trade_date >= yi.available_at
             AND (yi.next_available_at IS NULL OR td.trade_date < yi.next_available_at)
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                available_at,
                raw_value,
                percent_rank() OVER (PARTITION BY trade_date ORDER BY raw_value) AS normalized_value
            FROM raw
            WHERE raw_value IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, available_at
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_financial_annual_persistence_backfill_sql(
    source_column: &'static str,
    mode: FinancialAnnualChangeMode,
) -> String {
    let yoy_expression = match mode {
        FinancialAnnualChangeMode::PercentChange => {
            "(current_report.raw_value - previous_report.raw_value) / NULLIF(ABS(previous_report.raw_value), 0.0)"
        }
        FinancialAnnualChangeMode::Difference => "current_report.raw_value - previous_report.raw_value",
        FinancialAnnualChangeMode::Decrease => "previous_report.raw_value - current_report.raw_value",
    };
    let raw_expression = "yoy_value + 0.5 * prior_yoy_value + 0.25 * second_yoy_value";

    format!(
        "WITH trade_days AS (
            SELECT trade_date
            FROM market_trade_calendar
            WHERE exchange = 'SSE'
              AND is_open = true
              AND trade_date BETWEEN $3 AND $4
        ),
        source_reports AS (
            SELECT
                latest.ts_code AS symbol,
                latest.ann_date,
                latest.end_date,
                latest.{source_column}::double precision AS raw_value
            FROM market_financial_indicator latest
            WHERE latest.{source_column} IS NOT NULL
              AND latest.ann_date <= $4
        ),
        annual_yoy_points AS (
            SELECT
                current_report.symbol,
                current_report.ann_date AS current_ann_date,
                current_report.end_date,
                GREATEST(current_report.ann_date, previous_report.ann_date) AS yoy_available_at,
                {yoy_expression} AS raw_value
            FROM source_reports current_report
            JOIN source_reports previous_report
              ON previous_report.symbol = current_report.symbol
             AND previous_report.end_date = (current_report.end_date - INTERVAL '1 year')::date
            WHERE {yoy_expression} IS NOT NULL
        ),
        sequenced_yoy AS (
            SELECT
                symbol,
                current_ann_date,
                end_date,
                yoy_available_at,
                raw_value AS yoy_value,
                LAG(yoy_available_at, 1) OVER (
                    PARTITION BY symbol ORDER BY current_ann_date, end_date
                ) AS prior_yoy_available_at,
                LAG(raw_value, 1) OVER (
                    PARTITION BY symbol ORDER BY current_ann_date, end_date
                ) AS prior_yoy_value,
                LAG(yoy_available_at, 2) OVER (
                    PARTITION BY symbol ORDER BY current_ann_date, end_date
                ) AS second_yoy_available_at,
                LAG(raw_value, 2) OVER (
                    PARTITION BY symbol ORDER BY current_ann_date, end_date
                ) AS second_yoy_value
            FROM annual_yoy_points
        ),
        persistence_points AS (
            SELECT
                symbol,
                current_ann_date AS latest_ann_date,
                end_date,
                GREATEST(yoy_available_at, prior_yoy_available_at, second_yoy_available_at) AS available_at,
                {raw_expression} AS raw_value
            FROM sequenced_yoy
            WHERE prior_yoy_value IS NOT NULL
              AND second_yoy_value IS NOT NULL
              AND prior_yoy_available_at IS NOT NULL
              AND second_yoy_available_at IS NOT NULL
        ),
        deduped_points AS (
            SELECT DISTINCT ON (symbol, available_at)
                symbol,
                latest_ann_date,
                end_date,
                available_at,
                raw_value
            FROM persistence_points
            WHERE available_at <= $4
              AND raw_value IS NOT NULL
            ORDER BY symbol, available_at, latest_ann_date DESC, end_date DESC
        ),
        persistence_intervals AS (
            SELECT
                symbol,
                available_at,
                LEAD(available_at) OVER (
                    PARTITION BY symbol ORDER BY available_at
                ) AS next_available_at,
                raw_value
            FROM deduped_points
        ),
        raw AS (
            SELECT
                yi.symbol,
                td.trade_date,
                yi.available_at,
                yi.raw_value
            FROM persistence_intervals yi
            JOIN trade_days td
              ON td.trade_date >= yi.available_at
             AND (yi.next_available_at IS NULL OR td.trade_date < yi.next_available_at)
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                available_at,
                raw_value,
                percent_rank() OVER (PARTITION BY trade_date ORDER BY raw_value) AS normalized_value
            FROM raw
            WHERE raw_value IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, available_at
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_daily_basic_latest_backfill_sql(
    source_column: &'static str,
    higher_is_better: bool,
    positive_only: bool,
) -> String {
    let rank_order = if higher_is_better {
        "raw_value"
    } else {
        "raw_value DESC"
    };
    let positive_filter = if positive_only {
        "AND raw_value > 0.0"
    } else {
        ""
    };

    format!(
        "WITH raw AS (
            SELECT
                symbol,
                trade_date,
                {source_column}::double precision AS raw_value
            FROM market_stock_daily_basic
            WHERE trade_date BETWEEN $3 AND $4
              AND {source_column} IS NOT NULL
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                raw_value,
                percent_rank() OVER (PARTITION BY trade_date ORDER BY {rank_order}) AS normalized_value
            FROM raw
            WHERE raw_value IS NOT NULL
              {positive_filter}
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, trade_date
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_moneyflow_backfill_sql(
    period: i32,
    amount_expression: &'static str,
    higher_is_better: bool,
) -> String {
    let preceding = period - 1;
    let rank_order = if higher_is_better {
        "raw_value"
    } else {
        "raw_value DESC"
    };

    format!(
        "WITH observations AS (
            SELECT
                mf.symbol,
                mf.trade_date,
                {amount_expression} AS flow_amount,
                bar.amount::double precision AS traded_amount
            FROM market_stock_moneyflow mf
            JOIN market_stock_daily_bar_adj bar
              ON bar.symbol = mf.symbol
             AND bar.trade_date = mf.trade_date
            WHERE mf.trade_date <= $4
              AND bar.amount IS NOT NULL
              AND bar.amount > 0
        ),
        raw AS (
            SELECT
                symbol,
                trade_date,
                SUM(flow_amount) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {preceding} PRECEDING AND CURRENT ROW
                ) / NULLIF(
                    SUM(traded_amount) OVER (
                        PARTITION BY symbol ORDER BY trade_date
                        ROWS BETWEEN {preceding} PRECEDING AND CURRENT ROW
                    ),
                    0.0
                ) AS raw_value,
                COUNT(traded_amount) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {preceding} PRECEDING AND CURRENT ROW
                ) AS obs_count
            FROM observations
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                raw_value,
                percent_rank() OVER (PARTITION BY trade_date ORDER BY {rank_order}) AS normalized_value
            FROM raw
            WHERE trade_date BETWEEN $3 AND $4
              AND obs_count = {period}
              AND raw_value IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, trade_date
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_moneyflow_congestion_backfill_sql(
    period: i32,
    flow_expression: &'static str,
) -> String {
    let preceding = period - 1;

    format!(
        "WITH observations AS (
            SELECT
                mf.symbol,
                mf.trade_date,
                {flow_expression} AS flow_amount,
                bar.amount::double precision AS traded_amount,
                basic.circ_mv::double precision AS float_market_value,
                AVG(bar.amount::double precision) OVER (
                    PARTITION BY mf.symbol ORDER BY mf.trade_date
                    ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING
                ) AS prior_traded_amount_avg_60
            FROM market_stock_moneyflow mf
            JOIN market_stock_daily_bar_adj bar
              ON bar.symbol = mf.symbol
             AND bar.trade_date = mf.trade_date
            JOIN market_stock_daily_basic basic
              ON basic.symbol = mf.symbol
             AND basic.trade_date = mf.trade_date
            WHERE mf.trade_date <= $4
              AND mf.trade_date >= ($3::date - INTERVAL '180 days')
              AND bar.amount IS NOT NULL
              AND bar.amount > 0
              AND basic.circ_mv IS NOT NULL
              AND basic.circ_mv > 0
        ),
        daily_crowding AS (
            SELECT
                symbol,
                trade_date,
                flow_amount,
                traded_amount,
                float_market_value,
                traded_amount / NULLIF(prior_traded_amount_avg_60, 0.0) AS amount_crowding
            FROM observations
            WHERE prior_traded_amount_avg_60 IS NOT NULL
              AND prior_traded_amount_avg_60 > 0.0
              AND flow_amount IS NOT NULL
        ),
        rolling AS (
            SELECT
                symbol,
                trade_date,
                SUM(flow_amount) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {preceding} PRECEDING AND CURRENT ROW
                ) / NULLIF(
                    SUM(traded_amount) OVER (
                        PARTITION BY symbol ORDER BY trade_date
                        ROWS BETWEEN {preceding} PRECEDING AND CURRENT ROW
                    ),
                    0.0
                ) AS flow_intensity,
                AVG(amount_crowding) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {preceding} PRECEDING AND CURRENT ROW
                ) AS amount_crowding,
                SUM(traded_amount) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {preceding} PRECEDING AND CURRENT ROW
                ) / NULLIF(
                    SUM(float_market_value) OVER (
                        PARTITION BY symbol ORDER BY trade_date
                        ROWS BETWEEN {preceding} PRECEDING AND CURRENT ROW
                    ),
                    0.0
                ) AS capacity_pressure,
                COUNT(traded_amount) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {preceding} PRECEDING AND CURRENT ROW
                ) AS obs_count
            FROM daily_crowding
        ),
        raw AS (
            SELECT
                symbol,
                trade_date,
                flow_intensity,
                GREATEST(COALESCE(amount_crowding, 0.0) - 1.0, 0.0)
                    + GREATEST(COALESCE(capacity_pressure, 0.0), 0.0) AS crowding_penalty
            FROM rolling
            WHERE trade_date BETWEEN $3 AND $4
              AND obs_count = {period}
              AND flow_intensity IS NOT NULL
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                flow_intensity / (1.0 + crowding_penalty) AS raw_value,
                percent_rank() OVER (
                    PARTITION BY trade_date
                    ORDER BY flow_intensity / (1.0 + crowding_penalty)
                ) AS normalized_value
            FROM raw
            WHERE crowding_penalty IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, trade_date
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_supply_float_shock_backfill_sql(
    share_expression: &'static str,
    horizon_days: i32,
    mode: SupplyFloatShockMode,
) -> String {
    let horizon_days = horizon_days.max(1);
    let delta_preceding = horizon_days - 1;
    let warmup_days = (horizon_days * 3).max(90);
    let raw_projection = match mode {
        SupplyFloatShockMode::GrowthInverse => {
            format!("-LN(share_value / NULLIF(prev_share_{horizon_days}, 0.0)) AS raw_value")
        }
        SupplyFloatShockMode::ChurnInverse => {
            format!(
                "-STDDEV_POP(one_day_share_delta) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {delta_preceding} PRECEDING AND CURRENT ROW
                ) AS raw_value"
            )
        }
    };
    let raw_filter = match mode {
        SupplyFloatShockMode::GrowthInverse => {
            format!("obs_count_{horizon_days} = {}", horizon_days + 1)
        }
        SupplyFloatShockMode::ChurnInverse => {
            format!("delta_count_{horizon_days} = {horizon_days}")
        }
    };

    format!(
        "WITH supply AS (
            SELECT
                basic.symbol,
                basic.trade_date,
                {share_expression} AS share_value
            FROM market_stock_daily_basic basic
            WHERE basic.trade_date <= $4
              AND basic.trade_date >= ($3::date - INTERVAL '{warmup_days} days')
        ),
        observations AS (
            SELECT
                symbol,
                trade_date,
                share_value,
                LAG(share_value, {horizon_days}) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                ) AS prev_share_{horizon_days},
                CASE
                    WHEN LAG(share_value) OVER (PARTITION BY symbol ORDER BY trade_date) > 0.0
                         AND share_value > 0.0
                    THEN LN(share_value / NULLIF(
                        LAG(share_value) OVER (PARTITION BY symbol ORDER BY trade_date),
                        0.0
                    ))
                    ELSE NULL
                END AS one_day_share_delta,
                COUNT(share_value) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {horizon_days} PRECEDING AND CURRENT ROW
                ) AS obs_count_{horizon_days}
            FROM supply
            WHERE share_value IS NOT NULL
              AND share_value > 0.0
        ),
        raw AS (
            SELECT
                symbol,
                trade_date,
                obs_count_{horizon_days},
                {raw_projection},
                COUNT(one_day_share_delta) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {delta_preceding} PRECEDING AND CURRENT ROW
                ) AS delta_count_{horizon_days}
            FROM observations
        ),
        filtered AS (
            SELECT
                symbol,
                trade_date,
                trade_date AS available_at,
                raw_value
            FROM raw
            WHERE trade_date BETWEEN $3 AND $4
              AND {raw_filter}
              AND raw_value IS NOT NULL
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                available_at,
                raw_value,
                percent_rank() OVER (PARTITION BY trade_date ORDER BY raw_value) AS normalized_value
            FROM filtered
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, available_at
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_liquidity_quality_backfill_sql(
    signal: LiquidityQualitySignal,
    short_window: i32,
    long_window: i32,
) -> String {
    let short_window = short_window.max(2);
    let long_window = long_window.max(short_window + 1);
    let short_preceding = short_window - 1;
    let long_preceding = long_window - 1;
    // warmup 必须 > 120 个交易日的自然日长度(≈168 天, 含春节等长假可达 175+)。
    // 2026-09-18 事故: 原 max(180) 临界不足, 7-01 回看仅 ~118 交易日, 叠加
    // daily_basic 个股零星缺行后 obs_count_120 恒差 1~6 行 → raw_filter 全过滤,
    // 回填 completed 但 0 行写入(静默), 断档永久无法补回。240 天 ≈ 172 交易日,
    // 余量 43%, 覆盖长假与个股缺行。
    let warmup_days = (long_window * 2).max(240);
    let raw_value_expression = match signal {
        LiquidityQualitySignal::ImpactImprovement => {
            "LN((long_illiq + 1e-12) / (short_illiq + 1e-12)) AS raw_value".to_string()
        }
        LiquidityQualitySignal::AmountTrend => {
            "LN((short_amount + 1.0) / (long_amount + 1.0)) AS raw_value".to_string()
        }
        LiquidityQualitySignal::AmountStability => {
            format!("-COALESCE(std_log_amount_{short_window}, 0.0) AS raw_value")
        }
        LiquidityQualitySignal::TurnoverStability => {
            format!(
                "-ABS((turnover_proxy - avg_turnover_{short_window}) / NULLIF(std_turnover_{short_window}, 0.0))
                 - COALESCE(std_turnover_{short_window}, 0.0) AS raw_value"
            )
        }
    };
    let raw_filter = match signal {
        LiquidityQualitySignal::ImpactImprovement => {
            format!(
                "illiq_obs_count_{short_window} = {short_window}
              AND illiq_obs_count_{long_window} = {long_window}
              AND short_illiq IS NOT NULL
              AND long_illiq IS NOT NULL"
            )
        }
        LiquidityQualitySignal::AmountTrend => {
            format!(
                "amount_obs_count_{short_window} = {short_window}
              AND amount_obs_count_{long_window} = {long_window}
              AND short_amount IS NOT NULL
              AND long_amount IS NOT NULL"
            )
        }
        LiquidityQualitySignal::AmountStability => {
            format!(
                "amount_obs_count_{short_window} = {short_window}
              AND std_log_amount_{short_window} IS NOT NULL"
            )
        }
        LiquidityQualitySignal::TurnoverStability => {
            format!(
                "turnover_obs_count_{short_window} = {short_window}
              AND avg_turnover_{short_window} IS NOT NULL
              AND std_turnover_{short_window} IS NOT NULL"
            )
        }
    };

    format!(
        "WITH joined AS (
            SELECT
                bar.symbol,
                bar.trade_date,
                bar.close::double precision AS close,
                bar.amount::double precision AS amount,
                basic.circ_mv::double precision AS circ_mv,
                LAG(close::double precision) OVER (
                    PARTITION BY bar.symbol ORDER BY bar.trade_date
                ) AS prev_close
            FROM market_stock_daily_bar_adj bar
            JOIN market_stock_daily_basic basic
              ON basic.symbol = bar.symbol
             AND basic.trade_date = bar.trade_date
            WHERE bar.trade_date <= $4
              AND bar.trade_date >= ($3::date - INTERVAL '{warmup_days} days')
              AND bar.close IS NOT NULL
              AND bar.amount IS NOT NULL
              AND basic.circ_mv IS NOT NULL
        ),
        observations AS (
            SELECT
                symbol,
                trade_date,
                amount,
                CASE
                    WHEN prev_close > 0.0 AND close > 0.0 AND amount > 0.0
                    THEN ABS((close - prev_close) / prev_close) / amount * 1000000000.0
                    ELSE NULL
                END AS illiquidity,
                CASE WHEN amount > 0.0 THEN LN(1.0 + amount) ELSE NULL END AS log_amount,
                CASE
                    WHEN amount > 0.0 AND circ_mv > 0.0 THEN amount / NULLIF(circ_mv, 0.0)
                    ELSE NULL
                END AS turnover_proxy
            FROM joined
        ),
        rolling AS (
            SELECT
                symbol,
                trade_date,
                amount,
                turnover_proxy,
                AVG(illiquidity) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {short_preceding} PRECEDING AND CURRENT ROW
                ) AS short_illiq,
                AVG(illiquidity) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {long_preceding} PRECEDING AND CURRENT ROW
                ) AS long_illiq,
                COUNT(illiquidity) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {short_preceding} PRECEDING AND CURRENT ROW
                ) AS illiq_obs_count_{short_window},
                COUNT(illiquidity) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {long_preceding} PRECEDING AND CURRENT ROW
                ) AS illiq_obs_count_{long_window},
                AVG(amount) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {short_preceding} PRECEDING AND CURRENT ROW
                ) AS short_amount,
                AVG(amount) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {long_preceding} PRECEDING AND CURRENT ROW
                ) AS long_amount,
                COUNT(amount) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {short_preceding} PRECEDING AND CURRENT ROW
                ) AS amount_obs_count_{short_window},
                COUNT(amount) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {long_preceding} PRECEDING AND CURRENT ROW
                ) AS amount_obs_count_{long_window},
                STDDEV_SAMP(log_amount) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {short_preceding} PRECEDING AND CURRENT ROW
                ) AS std_log_amount_{short_window},
                AVG(turnover_proxy) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {short_preceding} PRECEDING AND CURRENT ROW
                ) AS avg_turnover_{short_window},
                STDDEV_SAMP(turnover_proxy) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {short_preceding} PRECEDING AND CURRENT ROW
                ) AS std_turnover_{short_window},
                COUNT(turnover_proxy) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {short_preceding} PRECEDING AND CURRENT ROW
                ) AS turnover_obs_count_{short_window}
            FROM observations
        ),
        raw AS (
            SELECT
                symbol,
                trade_date,
                trade_date AS available_at,
                {raw_value_expression}
            FROM rolling
            WHERE trade_date BETWEEN $3 AND $4
              AND {raw_filter}
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                available_at,
                raw_value,
                percent_rank() OVER (PARTITION BY trade_date ORDER BY raw_value) AS normalized_value
            FROM raw
            WHERE raw_value IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, available_at
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_market_residual_risk_backfill_sql(
    signal: MarketResidualRiskSignal,
    short_window: i32,
    long_window: i32,
) -> String {
    let short_window = short_window.max(2);
    let long_window = long_window.max(short_window + 1);
    let short_preceding = short_window - 1;
    let long_preceding = long_window - 1;
    let warmup_days = (long_window * 4).max(480);
    let min_downside_observations = (long_window / 4).max(20);
    let raw_value_expression = match signal {
        MarketResidualRiskSignal::LowBeta => format!("-beta_{long_window} AS raw_value"),
        MarketResidualRiskSignal::LowDownsideBeta => {
            format!("-downside_beta_{long_window} AS raw_value")
        }
        MarketResidualRiskSignal::LowResidualVolatility => {
            format!("-residual_vol_{long_window} AS raw_value")
        }
        MarketResidualRiskSignal::ResidualReversal => format!(
            "-residual_mean_{short_window} / NULLIF(residual_vol_{long_window}, 0.0) AS raw_value"
        ),
    };
    let raw_filter = match signal {
        MarketResidualRiskSignal::LowBeta => {
            format!("obs_count_{long_window} = {long_window} AND beta_{long_window} IS NOT NULL")
        }
        MarketResidualRiskSignal::LowDownsideBeta => format!(
            "downside_obs_count_{long_window} >= {min_downside_observations}
             AND downside_beta_{long_window} IS NOT NULL"
        ),
        MarketResidualRiskSignal::LowResidualVolatility => format!(
            "residual_obs_count_{long_window} = {long_window}
             AND residual_vol_{long_window} IS NOT NULL"
        ),
        MarketResidualRiskSignal::ResidualReversal => format!(
            "residual_obs_count_{short_window} = {short_window}
             AND residual_vol_{long_window} IS NOT NULL
             AND residual_mean_{short_window} IS NOT NULL"
        ),
    };

    format!(
        "WITH market AS (
            SELECT
                idx.trade_date,
                CASE
                    WHEN idx.close > 0 AND idx.pre_close > 0
                    THEN (idx.close::double precision / idx.pre_close::double precision) - 1.0
                    ELSE NULL
                END AS market_return
            FROM market_index_daily_bar idx
            WHERE idx.symbol = '000300.SH'
              AND idx.trade_date <= $4
              AND idx.trade_date >= ($3::date - INTERVAL '{warmup_days} days')
              AND idx.close IS NOT NULL
              AND idx.pre_close IS NOT NULL
        ),
        stock AS (
            SELECT
                bar.symbol,
                bar.trade_date,
                bar.close::double precision AS close,
                LAG(bar.close::double precision) OVER (
                    PARTITION BY bar.symbol ORDER BY bar.trade_date
                ) AS prev_close
            FROM market_stock_daily_bar_adj bar
            WHERE bar.trade_date <= $4
              AND bar.trade_date >= ($3::date - INTERVAL '{warmup_days} days')
              AND bar.close IS NOT NULL
        ),
        joined AS (
            SELECT
                stock.symbol,
                stock.trade_date,
                CASE
                    WHEN stock.prev_close > 0.0 AND stock.close > 0.0
                    THEN stock.close / stock.prev_close - 1.0
                    ELSE NULL
                END AS stock_return,
                market.market_return
            FROM stock
            JOIN market
              ON market.trade_date = stock.trade_date
            WHERE market.market_return IS NOT NULL
        ),
        rolling_beta AS (
            SELECT
                symbol,
                trade_date,
                stock_return,
                market_return,
                REGR_SLOPE(stock_return, market_return) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {long_preceding} PRECEDING AND CURRENT ROW
                ) AS beta_{long_window},
                REGR_COUNT(stock_return, market_return) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {long_preceding} PRECEDING AND CURRENT ROW
                ) AS obs_count_{long_window},
                REGR_SLOPE(
                    CASE WHEN market_return < 0.0 THEN stock_return ELSE NULL END,
                    CASE WHEN market_return < 0.0 THEN market_return ELSE NULL END
                ) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {long_preceding} PRECEDING AND CURRENT ROW
                ) AS downside_beta_{long_window},
                REGR_COUNT(
                    CASE WHEN market_return < 0.0 THEN stock_return ELSE NULL END,
                    CASE WHEN market_return < 0.0 THEN market_return ELSE NULL END
                ) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {long_preceding} PRECEDING AND CURRENT ROW
                ) AS downside_obs_count_{long_window}
            FROM joined
            WHERE stock_return IS NOT NULL
              AND market_return IS NOT NULL
        ),
        residualized AS (
            SELECT
                symbol,
                trade_date,
                stock_return,
                market_return,
                beta_{long_window},
                downside_beta_{long_window},
                obs_count_{long_window},
                downside_obs_count_{long_window},
                stock_return - beta_{long_window} * market_return AS residual_return
            FROM rolling_beta
            WHERE obs_count_{long_window} = {long_window}
              AND beta_{long_window} IS NOT NULL
        ),
        rolling_residual AS (
            SELECT
                symbol,
                trade_date,
                beta_{long_window},
                downside_beta_{long_window},
                obs_count_{long_window},
                downside_obs_count_{long_window},
                residual_return,
                STDDEV_SAMP(residual_return) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {long_preceding} PRECEDING AND CURRENT ROW
                ) AS residual_vol_{long_window},
                COUNT(residual_return) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {long_preceding} PRECEDING AND CURRENT ROW
                ) AS residual_obs_count_{long_window},
                AVG(residual_return) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {short_preceding} PRECEDING AND CURRENT ROW
                ) AS residual_mean_{short_window},
                COUNT(residual_return) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {short_preceding} PRECEDING AND CURRENT ROW
                ) AS residual_obs_count_{short_window}
            FROM residualized
        ),
        raw AS (
            SELECT
                symbol,
                trade_date,
                trade_date AS available_at,
                {raw_value_expression}
            FROM rolling_residual
            WHERE trade_date BETWEEN $3 AND $4
              AND {raw_filter}
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                available_at,
                raw_value,
                percent_rank() OVER (PARTITION BY trade_date ORDER BY raw_value) AS normalized_value
            FROM raw
            WHERE raw_value IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, available_at
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_cashflow_latest_backfill_sql(
    value_expression: &'static str,
    required_filter: &'static str,
    higher_is_better: bool,
) -> String {
    let rank_order = if higher_is_better {
        "raw_value"
    } else {
        "raw_value DESC"
    };

    format!(
        "WITH trade_days AS (
            SELECT trade_date
            FROM market_trade_calendar
            WHERE exchange = 'SSE'
              AND is_open = true
              AND trade_date BETWEEN $3 AND $4
        ),
        event_points AS (
            SELECT DISTINCT ON (cf.symbol, cf.available_at)
                cf.symbol,
                cf.available_at,
                {value_expression} AS raw_value
            FROM market_stock_cashflow cf
            WHERE cf.symbol IS NOT NULL
              AND cf.available_at IS NOT NULL
              AND {required_filter}
            ORDER BY cf.symbol, cf.available_at, cf.end_date DESC, cf.ann_date DESC
        ),
        version_intervals AS (
            SELECT
                symbol,
                available_at,
                LEAD(available_at) OVER (
                    PARTITION BY symbol ORDER BY available_at
                ) AS next_available_at,
                raw_value
            FROM event_points
        ),
        latest AS (
            SELECT
                vi.symbol,
                td.trade_date,
                vi.available_at,
                vi.raw_value
            FROM version_intervals vi
            JOIN trade_days td
              ON td.trade_date >= vi.available_at
             AND (vi.next_available_at IS NULL OR td.trade_date < vi.next_available_at)
            WHERE vi.raw_value IS NOT NULL
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                available_at,
                raw_value,
                percent_rank() OVER (PARTITION BY trade_date ORDER BY {rank_order}) AS normalized_value
            FROM latest
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, available_at
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_dividend_rolling_quality_backfill_sql(
    value_expression: &'static str,
    higher_is_better: bool,
) -> String {
    let rank_order = if higher_is_better {
        "raw_value"
    } else {
        "raw_value DESC"
    };

    format!(
        "WITH trade_days AS (
            SELECT trade_date
            FROM market_trade_calendar
            WHERE exchange = 'SSE'
              AND is_open = true
              AND trade_date BETWEEN $3 AND $4
        ),
        symbols AS (
            SELECT DISTINCT symbol
            FROM market_stock_dividend
            WHERE COALESCE(cash_div_tax, cash_div) IS NOT NULL
        ),
        history AS (
            SELECT
                symbols.symbol,
                td.trade_date,
                MAX(div.ann_date) AS available_at,
                COUNT(DISTINCT div.end_date) FILTER (WHERE div.cash_div_value > 0.0) AS dividend_years,
                SUM(div.cash_div_value) FILTER (WHERE div.cash_div_value > 0.0) AS cash_div_sum,
                AVG(div.cash_div_value) FILTER (WHERE div.cash_div_value > 0.0) AS cash_div_avg,
                STDDEV_POP(cash_div_value) FILTER (WHERE div.cash_div_value > 0.0) AS cash_div_stdev,
                MAX(
                    CASE
                        WHEN div.end_date >= (td.trade_date - INTERVAL '18 months')::date
                         AND div.cash_div_value > 0.0
                        THEN 1
                        ELSE 0
                    END
                ) AS recent_positive_dividend
            FROM symbols
            JOIN trade_days td ON true
            LEFT JOIN LATERAL (
                SELECT
                    div.ann_date,
                    div.end_date,
                    COALESCE(div.cash_div_tax, div.cash_div)::double precision AS cash_div_value
                FROM market_stock_dividend div
                WHERE div.symbol = symbols.symbol
                  AND div.ann_date <= td.trade_date
                  AND div.end_date >= (td.trade_date - INTERVAL '4 years')::date
                  AND COALESCE(div.cash_div_tax, div.cash_div) IS NOT NULL
            ) div ON true
            GROUP BY symbols.symbol, td.trade_date
        ),
        raw AS (
            SELECT
                symbol,
                trade_date,
                available_at,
                {value_expression} AS raw_value
            FROM history
            WHERE available_at IS NOT NULL
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                available_at,
                raw_value,
                percent_rank() OVER (PARTITION BY trade_date ORDER BY {rank_order}) AS normalized_value
            FROM raw
            WHERE raw_value IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, available_at
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_event_latest_backfill_sql(
    source_table: &'static str,
    value_expression: &'static str,
    higher_is_better: bool,
) -> String {
    let rank_order = if higher_is_better {
        "raw_value"
    } else {
        "raw_value DESC"
    };

    format!(
        "WITH trade_days AS (
            SELECT trade_date
            FROM market_trade_calendar
            WHERE exchange = 'SSE'
              AND is_open = true
              AND trade_date BETWEEN $3 AND $4
        ),
        ranked_events AS (
            SELECT
                event.symbol,
                event.available_at,
                {value_expression} AS raw_value,
                ROW_NUMBER() OVER (
                    PARTITION BY event.symbol, event.available_at
                    ORDER BY event.end_date DESC NULLS LAST, event.created_at DESC NULLS LAST
                ) AS event_rank
            FROM {source_table} event
            WHERE event.symbol IS NOT NULL
              AND event.available_at IS NOT NULL
              AND event.available_at <= $4
        ),
        deduped_events AS (
            SELECT
                symbol,
                available_at,
                raw_value
            FROM ranked_events
            WHERE event_rank = 1
        ),
        event_intervals AS (
            SELECT
                symbol,
                available_at,
                LEAD(available_at) OVER (PARTITION BY symbol ORDER BY available_at) AS next_available_at,
                raw_value
            FROM deduped_events
        ),
        latest AS (
            SELECT
                event_intervals.symbol,
                td.trade_date,
                event_intervals.available_at,
                event_intervals.raw_value
            FROM event_intervals
            JOIN trade_days td
              ON td.trade_date >= event_intervals.available_at
             AND td.trade_date < COALESCE(event_intervals.next_available_at, $4 + 1)
             AND td.trade_date BETWEEN $3 AND $4
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                available_at,
                raw_value,
                COUNT(*) OVER (PARTITION BY trade_date) AS symbol_count,
                CASE
                    WHEN COUNT(*) OVER (PARTITION BY trade_date) = 1 THEN 1.0
                    ELSE percent_rank() OVER (PARTITION BY trade_date ORDER BY {rank_order})
                END AS normalized_value
            FROM latest
            WHERE raw_value IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, available_at
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_block_trade_window_backfill_sql(
    value_expression: &'static str,
    higher_is_better: bool,
    window_days: i32,
    decay_days: i32,
) -> String {
    let rank_order = if higher_is_better {
        "raw_value"
    } else {
        "raw_value DESC"
    };
    let window_days = window_days.clamp(1, 120);
    let decay_days = decay_days.clamp(1, window_days);

    format!(
        "WITH trade_days AS (
            SELECT trade_date
            FROM market_trade_calendar
            WHERE exchange = 'SSE'
              AND is_open = true
              AND trade_date BETWEEN $3 AND $4
        ),
        events AS (
            SELECT
                event.ts_code AS symbol,
                event.trade_date AS event_trade_date,
                event.available_at,
                event.source_row_no,
                event.created_at,
                event.price,
                event.vol,
                event.amount,
                event.buyer,
                event.seller,
                {value_expression} AS event_raw_value
            FROM market_stock_block_trade event
            LEFT JOIN market_stock_daily_bar_adj bar
              ON bar.symbol = event.ts_code
             AND bar.trade_date = event.trade_date
            WHERE event.ts_code IS NOT NULL
              AND event.available_at IS NOT NULL
              AND event.available_at > event.trade_date
              AND event.available_at <= $4
              AND event.available_at >= $3 - INTERVAL '{window_days} days'
        ),
        expanded AS (
            SELECT
                events.symbol,
                td.trade_date,
                events.available_at,
                events.event_raw_value,
                GREATEST(
                    0.0,
                    1.0 - ((td.trade_date - events.available_at)::double precision / {decay_days}.0)
                ) AS decay_weight
            FROM events
            JOIN trade_days td
              ON td.trade_date >= events.available_at
             AND td.trade_date <= events.available_at + INTERVAL '{window_days} days'
            WHERE events.event_raw_value IS NOT NULL
              AND events.event_raw_value <> 0.0
        ),
        raw AS (
            SELECT
                symbol,
                trade_date,
                MAX(available_at) AS available_at,
                SUM(event_raw_value * decay_weight) AS raw_value
            FROM expanded
            WHERE decay_weight > 0.0
            GROUP BY symbol, trade_date
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                available_at,
                raw_value,
                COUNT(*) OVER (PARTITION BY trade_date) AS symbol_count,
                CASE
                    WHEN COUNT(*) OVER (PARTITION BY trade_date) = 1 THEN 1.0
                    ELSE percent_rank() OVER (PARTITION BY trade_date ORDER BY {rank_order})
                END AS normalized_value
            FROM raw
            WHERE raw_value IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, available_at
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_unlock_pressure_backfill_sql(horizon_days: i32) -> String {
    let horizon_days = horizon_days.clamp(1, 365);
    format!(
        "WITH trade_days AS (
            SELECT trade_date
            FROM market_trade_calendar
            WHERE exchange = 'SSE'
              AND is_open = true
              AND trade_date BETWEEN $3 AND $4
        ),
        universe AS (
            SELECT DISTINCT
                universe.symbol,
                universe.trade_date
            FROM market_stock_daily_bar_adj universe
            JOIN trade_days td
              ON td.trade_date = universe.trade_date
            WHERE universe.symbol IS NOT NULL
        ),
        raw_pressure AS (
            SELECT
                universe.symbol,
                universe.trade_date,
                COALESCE(SUM(COALESCE(event.float_ratio::double precision, 0.0)), 0.0) AS raw_unlock_ratio,
                COALESCE(MAX(event.available_at), universe.trade_date) AS available_at
            FROM universe
            LEFT JOIN market_stock_share_float event
              ON event.symbol = universe.symbol
             AND event.available_at <= universe.trade_date
             AND event.float_date >= universe.trade_date
             AND event.float_date <= universe.trade_date + INTERVAL '{horizon_days} days'
            GROUP BY universe.symbol, universe.trade_date
        ),
        scored AS (
            SELECT
                symbol,
                trade_date,
                available_at,
                -raw_unlock_ratio AS raw_value
            FROM raw_pressure
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                available_at,
                raw_value,
                CASE
                    WHEN COUNT(*) OVER (PARTITION BY trade_date) = 1 THEN 1.0
                    ELSE percent_rank() OVER (PARTITION BY trade_date ORDER BY raw_value)
                END AS normalized_value
            FROM scored
            WHERE raw_value IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, available_at
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_industry_prosperity_backfill_sql(
    signal: IndustryProsperitySignal,
    short_window: i32,
    long_window: i32,
) -> String {
    let short_window = short_window.max(2);
    let long_window = long_window.max(short_window + 1);
    let short_preceding = short_window - 1;
    let long_preceding = long_window - 1;
    let warmup_days = (long_window * 3).max(240);
    let min_members = 20;
    let raw_value_expression = match signal {
        IndustryProsperitySignal::ReturnMomentum => {
            "industry_ret_short - industry_ret_long AS raw_value"
        }
        IndustryProsperitySignal::PositiveBreadth => "positive_breadth_short AS raw_value",
        IndustryProsperitySignal::AmountTrend => {
            "LN((industry_amount_short + 1.0) / (industry_amount_long + 1.0)) AS raw_value"
        }
    };
    let raw_filter = match signal {
        IndustryProsperitySignal::ReturnMomentum => {
            format!(
                "ret_short_member_count >= {min_members}
              AND ret_long_member_count >= {min_members}
              AND industry_ret_short IS NOT NULL
              AND industry_ret_long IS NOT NULL"
            )
        }
        IndustryProsperitySignal::PositiveBreadth => {
            format!(
                "ret_short_member_count >= {min_members}
              AND positive_breadth_short IS NOT NULL"
            )
        }
        IndustryProsperitySignal::AmountTrend => {
            format!(
                "amount_member_count >= {min_members}
              AND industry_amount_short IS NOT NULL
              AND industry_amount_long IS NOT NULL"
            )
        }
    };

    format!(
        "WITH eligible_universe AS (
            SELECT
                bar.symbol,
                bar.trade_date,
                bar.close::double precision AS close,
                bar.amount::double precision AS amount
            FROM market_stock_daily_bar_adj bar
            JOIN market_stock ms
              ON ms.symbol = bar.symbol
            JOIN market_stock_daily_basic basic
              ON basic.symbol = bar.symbol
             AND basic.trade_date = bar.trade_date
            WHERE bar.trade_date <= $4
              AND bar.trade_date >= ($3::date - INTERVAL '{warmup_days} days')
              AND bar.close IS NOT NULL
              AND bar.close > 0
              AND bar.amount IS NOT NULL
              AND bar.amount > 0
              AND basic.circ_mv IS NOT NULL
              AND basic.circ_mv > 0
              AND ms.list_status = 'L'
              AND COALESCE(ms.is_st, false) = false
              AND ms.exchange IN ('SSE', 'SZSE')
              AND ms.market IN ('主板', '创业板')
              AND ms.symbol NOT LIKE '688%SH'
              AND COALESCE(ms.market, '') NOT ILIKE '%科创%'
              AND COALESCE(ms.market, '') NOT ILIKE '%北交%'
        ),
        universe AS (
            SELECT
                universe.symbol,
                universe.trade_date,
                universe.close,
                universe.amount,
                membership.index_code,
                membership.industry_name
            FROM eligible_universe universe
            JOIN market_stock_industry_membership_pit membership
              ON membership.symbol = universe.symbol
             AND membership.industry_level = 'L1'
             AND membership.classification_source = CASE
                 WHEN universe.trade_date < DATE '2021-12-13' THEN 'SW2014'
                 ELSE 'SW2021'
             END
             AND membership.available_at <= universe.trade_date
             AND membership.in_date <= universe.trade_date
             AND (
                 membership.exit_available_at IS NULL
                 OR membership.exit_available_at > universe.trade_date
             )
        ),
        stock_roll AS (
            SELECT
                symbol,
                trade_date,
                index_code,
                industry_name,
                close,
                amount,
                LAG(close, {short_window}) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                ) AS prev_close_short,
                LAG(close, {long_window}) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                ) AS prev_close_long,
                AVG(amount) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {short_preceding} PRECEDING AND CURRENT ROW
                ) AS amount_short,
                AVG(amount) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {long_preceding} PRECEDING AND CURRENT ROW
                ) AS amount_long,
                COUNT(amount) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {short_preceding} PRECEDING AND CURRENT ROW
                ) AS amount_obs_count_short,
                COUNT(amount) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN {long_preceding} PRECEDING AND CURRENT ROW
                ) AS amount_obs_count_long
            FROM universe
        ),
        stock_features AS (
            SELECT
                symbol,
                trade_date,
                index_code,
                industry_name,
                CASE
                    WHEN prev_close_short > 0.0 AND close > 0.0
                    THEN close / prev_close_short - 1.0
                    ELSE NULL
                END AS ret_short,
                CASE
                    WHEN prev_close_long > 0.0 AND close > 0.0
                    THEN close / prev_close_long - 1.0
                    ELSE NULL
                END AS ret_long,
                CASE
                    WHEN amount_obs_count_short = {short_window} THEN amount_short
                    ELSE NULL
                END AS amount_short,
                CASE
                    WHEN amount_obs_count_long = {long_window} THEN amount_long
                    ELSE NULL
                END AS amount_long
            FROM stock_roll
        ),
        industry_daily AS (
            SELECT
                index_code,
                industry_name,
                trade_date,
                AVG(ret_short) FILTER (WHERE ret_short IS NOT NULL) AS industry_ret_short,
                AVG(ret_long) FILTER (WHERE ret_long IS NOT NULL) AS industry_ret_long,
                AVG(
                    CASE
                        WHEN ret_short IS NULL THEN NULL
                        WHEN ret_short > 0.0 THEN 1.0
                        ELSE 0.0
                    END
                ) AS positive_breadth_short,
                AVG(amount_short) FILTER (WHERE amount_short IS NOT NULL) AS industry_amount_short,
                AVG(amount_long) FILTER (WHERE amount_long IS NOT NULL) AS industry_amount_long,
                COUNT(*) FILTER (WHERE ret_short IS NOT NULL) AS ret_short_member_count,
                COUNT(*) FILTER (WHERE ret_long IS NOT NULL) AS ret_long_member_count,
                COUNT(*) FILTER (
                    WHERE amount_short IS NOT NULL AND amount_long IS NOT NULL
                ) AS amount_member_count
            FROM stock_features
            GROUP BY index_code, industry_name, trade_date
        ),
        raw AS (
            SELECT
                stock_features.symbol,
                stock_features.trade_date,
                stock_features.trade_date AS available_at,
                {raw_value_expression}
            FROM stock_features
            JOIN industry_daily
              ON industry_daily.index_code = stock_features.index_code
             AND industry_daily.trade_date = stock_features.trade_date
            WHERE stock_features.trade_date BETWEEN $3 AND $4
              AND {raw_filter}
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                available_at,
                raw_value,
                percent_rank() OVER (PARTITION BY trade_date ORDER BY raw_value) AS normalized_value
            FROM raw
            WHERE raw_value IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, available_at
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_industry_prosperity_multi_backfill_sql() -> String {
    let warmup_days = 360;
    let min_members = 20;
    format!(
        "WITH eligible_universe AS (
            SELECT
                bar.symbol,
                bar.trade_date,
                bar.close::double precision AS close,
                bar.amount::double precision AS amount
            FROM market_stock_daily_bar_adj bar
            JOIN market_stock ms
              ON ms.symbol = bar.symbol
            JOIN market_stock_daily_basic basic
              ON basic.symbol = bar.symbol
             AND basic.trade_date = bar.trade_date
            WHERE bar.trade_date <= $6
              AND bar.trade_date >= ($5::date - INTERVAL '{warmup_days} days')
              AND bar.close IS NOT NULL
              AND bar.close > 0
              AND bar.amount IS NOT NULL
              AND bar.amount > 0
              AND basic.circ_mv IS NOT NULL
              AND basic.circ_mv > 0
              AND ms.list_status = 'L'
              AND COALESCE(ms.is_st, false) = false
              AND ms.exchange IN ('SSE', 'SZSE')
              AND ms.market IN ('主板', '创业板')
              AND ms.symbol NOT LIKE '688%SH'
              AND COALESCE(ms.market, '') NOT ILIKE '%科创%'
              AND COALESCE(ms.market, '') NOT ILIKE '%北交%'
        ),
        universe AS (
            SELECT
                universe.symbol,
                universe.trade_date,
                universe.close,
                universe.amount,
                membership.index_code,
                membership.industry_name
            FROM eligible_universe universe
            JOIN market_stock_industry_membership_pit membership
              ON membership.symbol = universe.symbol
             AND membership.industry_level = 'L1'
             AND membership.classification_source = CASE
                 WHEN universe.trade_date < DATE '2021-12-13' THEN 'SW2014'
                 ELSE 'SW2021'
             END
             AND membership.available_at <= universe.trade_date
             AND membership.in_date <= universe.trade_date
             AND (
                 membership.exit_available_at IS NULL
                 OR membership.exit_available_at > universe.trade_date
             )
        ),
        stock_roll AS (
            SELECT
                symbol,
                trade_date,
                index_code,
                industry_name,
                close,
                amount,
                LAG(close, 20) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                ) AS prev_close_20,
                LAG(close, 60) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                ) AS prev_close_60,
                LAG(close, 120) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                ) AS prev_close_120,
                AVG(amount) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) AS amount_20,
                AVG(amount) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN 119 PRECEDING AND CURRENT ROW
                ) AS amount_120,
                COUNT(amount) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) AS amount_obs_count_20,
                COUNT(amount) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN 119 PRECEDING AND CURRENT ROW
                ) AS amount_obs_count_120
            FROM universe
        ),
        stock_features AS (
            SELECT
                symbol,
                trade_date,
                index_code,
                industry_name,
                CASE
                    WHEN prev_close_20 > 0.0 AND close > 0.0
                    THEN close / prev_close_20 - 1.0
                    ELSE NULL
                END AS ret_20,
                CASE
                    WHEN prev_close_60 > 0.0 AND close > 0.0
                    THEN close / prev_close_60 - 1.0
                    ELSE NULL
                END AS ret_60,
                CASE
                    WHEN prev_close_120 > 0.0 AND close > 0.0
                    THEN close / prev_close_120 - 1.0
                    ELSE NULL
                END AS ret_120,
                CASE
                    WHEN amount_obs_count_20 = 20 THEN amount_20
                    ELSE NULL
                END AS amount_20,
                CASE
                    WHEN amount_obs_count_120 = 120 THEN amount_120
                    ELSE NULL
                END AS amount_120
            FROM stock_roll
        ),
        industry_daily AS (
            SELECT
                index_code,
                industry_name,
                trade_date,
                AVG(ret_20) FILTER (WHERE ret_20 IS NOT NULL) AS industry_ret_20,
                AVG(ret_120) FILTER (WHERE ret_120 IS NOT NULL) AS industry_ret_120,
                AVG(
                    CASE
                        WHEN ret_60 IS NULL THEN NULL
                        WHEN ret_60 > 0.0 THEN 1.0
                        ELSE 0.0
                    END
                ) AS positive_breadth_60,
                AVG(amount_20) FILTER (WHERE amount_20 IS NOT NULL) AS industry_amount_20,
                AVG(amount_120) FILTER (WHERE amount_120 IS NOT NULL) AS industry_amount_120,
                COUNT(*) FILTER (WHERE ret_20 IS NOT NULL) AS ret_20_member_count,
                COUNT(*) FILTER (WHERE ret_60 IS NOT NULL) AS ret_60_member_count,
                COUNT(*) FILTER (WHERE ret_120 IS NOT NULL) AS ret_120_member_count,
                COUNT(*) FILTER (
                    WHERE amount_20 IS NOT NULL AND amount_120 IS NOT NULL
                ) AS amount_member_count
            FROM stock_features
            GROUP BY index_code, industry_name, trade_date
        ),
        raw AS (
            SELECT
                stock_features.symbol,
                stock_features.trade_date,
                stock_features.trade_date AS available_at,
                signals.factor_code,
                signals.raw_value
            FROM stock_features
            JOIN industry_daily
              ON industry_daily.index_code = stock_features.index_code
             AND industry_daily.trade_date = stock_features.trade_date
            CROSS JOIN LATERAL (
                VALUES
                    (
                        $1::varchar,
                        CASE
                            WHEN ret_20_member_count >= {min_members}
                             AND ret_120_member_count >= {min_members}
                             AND industry_ret_20 IS NOT NULL
                             AND industry_ret_120 IS NOT NULL
                            THEN industry_ret_20 - industry_ret_120
                            ELSE NULL
                        END
                    ),
                    (
                        $2::varchar,
                        CASE
                            WHEN ret_60_member_count >= {min_members}
                             AND positive_breadth_60 IS NOT NULL
                            THEN positive_breadth_60
                            ELSE NULL
                        END
                    ),
                    (
                        $3::varchar,
                        CASE
                            WHEN amount_member_count >= {min_members}
                             AND industry_amount_20 IS NOT NULL
                             AND industry_amount_120 IS NOT NULL
                            THEN LN((industry_amount_20 + 1.0) / (industry_amount_120 + 1.0))
                            ELSE NULL
                        END
                    )
            ) AS signals(factor_code, raw_value)
            WHERE stock_features.trade_date BETWEEN $5 AND $6
        ),
        ranked AS (
            SELECT
                factor_code,
                symbol,
                trade_date,
                available_at,
                raw_value,
                percent_rank() OVER (
                    PARTITION BY factor_code, trade_date ORDER BY raw_value
                ) AS normalized_value
            FROM raw
            WHERE raw_value IS NOT NULL
        ),
        inserted AS (
            INSERT INTO factor_value
                (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
            SELECT factor_code, $4, symbol, trade_date, raw_value, normalized_value, available_at
            FROM ranked
            ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
                raw_value = EXCLUDED.raw_value,
                normalized_value = EXCLUDED.normalized_value,
                available_at = EXCLUDED.available_at,
                created_at = NOW()
            RETURNING factor_code
        )
        SELECT factor_code, COUNT(*)::int8 AS row_count
        FROM inserted
        GROUP BY factor_code
        ORDER BY factor_code"
    )
}

pub(crate) fn phase7_futures_price_chain_backfill_sql(_signal: FuturesPriceChainSignal) -> String {
    panic!("futures_price_chain factors must use the shared multi-signal builder to avoid repeated raw-table scans")
}

pub(crate) fn phase7_futures_price_chain_product_signal_backfill_sql() -> &'static str {
    "WITH daily_raw_symbol AS (
        SELECT
            ts_code,
            upper(substring(ts_code from '^([A-Za-z]+)[0-9]{4}\\.')) AS product_symbol_raw,
            trade_date,
            available_at,
            close::double precision AS close,
            amount::double precision AS amount,
            oi::double precision AS oi
        FROM market_futures_daily
        WHERE trade_date >= ($5::date - INTERVAL '180 days')
          AND trade_date <= $6
          AND available_at <= $6
          AND close IS NOT NULL
          AND close > 0
          AND substring(ts_code from '^([A-Za-z]+)[0-9]{4}\\.') IS NOT NULL
    ),
    daily_contracts AS (
        SELECT
            CASE
                WHEN product_symbol_raw = 'PTA' THEN 'TA'
                WHEN length(product_symbol_raw) > 4 AND right(product_symbol_raw, 4) = 'ACTV'
                THEN left(product_symbol_raw, length(product_symbol_raw) - 4)
                WHEN length(product_symbol_raw) > 2 AND right(product_symbol_raw, 1) = 'L'
                THEN left(product_symbol_raw, length(product_symbol_raw) - 1)
                ELSE product_symbol_raw
            END AS product_symbol,
            trade_date,
            available_at,
            close,
            amount,
            oi,
            ROW_NUMBER() OVER (
                PARTITION BY
                    CASE
                        WHEN product_symbol_raw = 'PTA' THEN 'TA'
                        WHEN length(product_symbol_raw) > 4 AND right(product_symbol_raw, 4) = 'ACTV'
                        THEN left(product_symbol_raw, length(product_symbol_raw) - 4)
                        WHEN length(product_symbol_raw) > 2 AND right(product_symbol_raw, 1) = 'L'
                        THEN left(product_symbol_raw, length(product_symbol_raw) - 1)
                        ELSE product_symbol_raw
                    END,
                    trade_date
                ORDER BY COALESCE(amount, 0.0) DESC, COALESCE(oi, 0.0) DESC, ts_code
            ) AS contract_rank
        FROM daily_raw_symbol
    ),
    main_contract AS (
        SELECT product_symbol, trade_date, available_at, close
        FROM daily_contracts
        WHERE contract_rank = 1
          AND product_symbol IS NOT NULL
          AND product_symbol <> ''
    ),
    price_roll AS (
        SELECT
            product_symbol,
            trade_date,
            available_at,
            close,
            LAG(close, 20) OVER (PARTITION BY product_symbol ORDER BY trade_date) AS close_20,
            LAG(close, 60) OVER (PARTITION BY product_symbol ORDER BY trade_date) AS close_60
        FROM main_contract
    ),
    price_signal AS (
        SELECT
            product_symbol,
            trade_date,
            available_at,
            CASE
                WHEN close_20 > 0.0 AND close_60 > 0.0
                THEN (close / close_20 - 1.0) - (close / close_60 - 1.0)
                ELSE NULL
            END AS raw_value
        FROM price_roll
    ),
    wsr_raw_symbol AS (
        SELECT
            upper(substring(symbol from '^[A-Za-z]+')) AS product_symbol_raw,
            trade_date,
            available_at,
            vol::double precision AS vol
        FROM market_futures_warehouse_receipt
        WHERE trade_date >= ($5::date - INTERVAL '180 days')
          AND trade_date <= $6
          AND available_at <= $6
          AND vol IS NOT NULL
          AND vol >= 0
          AND substring(symbol from '^[A-Za-z]+') IS NOT NULL
    ),
    wsr_daily AS (
        SELECT
            CASE
                WHEN product_symbol_raw = 'PTA' THEN 'TA'
                WHEN length(product_symbol_raw) > 4 AND right(product_symbol_raw, 4) = 'ACTV'
                THEN left(product_symbol_raw, length(product_symbol_raw) - 4)
                WHEN length(product_symbol_raw) > 2 AND right(product_symbol_raw, 1) = 'L'
                THEN left(product_symbol_raw, length(product_symbol_raw) - 1)
                ELSE product_symbol_raw
            END AS product_symbol,
            trade_date,
            MAX(available_at) AS available_at,
            SUM(vol) AS inventory_vol
        FROM wsr_raw_symbol
        GROUP BY 1, trade_date
    ),
    wsr_roll AS (
        SELECT
            product_symbol,
            trade_date,
            available_at,
            AVG(inventory_vol) OVER (
                PARTITION BY product_symbol ORDER BY trade_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) AS inventory_20,
            AVG(inventory_vol) OVER (
                PARTITION BY product_symbol ORDER BY trade_date
                ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
            ) AS inventory_60,
            COUNT(inventory_vol) OVER (
                PARTITION BY product_symbol ORDER BY trade_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) AS inventory_obs_20,
            COUNT(inventory_vol) OVER (
                PARTITION BY product_symbol ORDER BY trade_date
                ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
            ) AS inventory_obs_60
        FROM wsr_daily
        WHERE product_symbol IS NOT NULL
          AND product_symbol <> ''
    ),
    inventory_signal AS (
        SELECT
            product_symbol,
            trade_date,
            available_at,
            CASE
                WHEN inventory_obs_20 = 20
                 AND inventory_obs_60 = 60
                 AND inventory_20 IS NOT NULL
                 AND inventory_60 IS NOT NULL
                THEN -LN((inventory_20 + 1.0) / (inventory_60 + 1.0))
                ELSE NULL
            END AS raw_value
        FROM wsr_roll
    ),
    holding_raw_symbol AS (
        SELECT
            upper(substring(symbol from '^[A-Za-z]+')) AS product_symbol_raw,
            trade_date,
            available_at,
            long_hld::double precision AS long_hld,
            short_hld::double precision AS short_hld
        FROM market_futures_holding_rank
        WHERE trade_date >= ($5::date - INTERVAL '180 days')
          AND trade_date <= $6
          AND available_at <= $6
          AND substring(symbol from '^[A-Za-z]+') IS NOT NULL
          AND (long_hld IS NOT NULL OR short_hld IS NOT NULL)
    ),
    holding_daily AS (
        SELECT
            CASE
                WHEN product_symbol_raw = 'PTA' THEN 'TA'
                WHEN length(product_symbol_raw) > 4 AND right(product_symbol_raw, 4) = 'ACTV'
                THEN left(product_symbol_raw, length(product_symbol_raw) - 4)
                WHEN length(product_symbol_raw) > 2 AND right(product_symbol_raw, 1) = 'L'
                THEN left(product_symbol_raw, length(product_symbol_raw) - 1)
                ELSE product_symbol_raw
            END AS product_symbol,
            trade_date,
            MAX(available_at) AS available_at,
            SUM(COALESCE(long_hld, 0.0)) AS long_hld,
            SUM(COALESCE(short_hld, 0.0)) AS short_hld
        FROM holding_raw_symbol
        GROUP BY 1, trade_date
    ),
    holding_features AS (
        SELECT
            product_symbol,
            trade_date,
            available_at,
            CASE
                WHEN long_hld + short_hld > 0.0
                THEN (long_hld - short_hld) / NULLIF(long_hld + short_hld, 0.0)
                ELSE NULL
            END AS net_position_ratio
        FROM holding_daily
        WHERE product_symbol IS NOT NULL
          AND product_symbol <> ''
    ),
    holding_roll AS (
        SELECT
            product_symbol,
            trade_date,
            available_at,
            AVG(net_position_ratio) OVER (
                PARTITION BY product_symbol ORDER BY trade_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) AS net_ratio_20,
            AVG(net_position_ratio) OVER (
                PARTITION BY product_symbol ORDER BY trade_date
                ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
            ) AS net_ratio_60,
            COUNT(net_position_ratio) OVER (
                PARTITION BY product_symbol ORDER BY trade_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) AS net_obs_20,
            COUNT(net_position_ratio) OVER (
                PARTITION BY product_symbol ORDER BY trade_date
                ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
            ) AS net_obs_60
        FROM holding_features
    ),
    holding_signal AS (
        SELECT
            product_symbol,
            trade_date,
            available_at,
            CASE
                WHEN net_obs_20 = 20
                 AND net_obs_60 = 60
                 AND net_ratio_20 IS NOT NULL
                 AND net_ratio_60 IS NOT NULL
                THEN net_ratio_20 - net_ratio_60
                ELSE NULL
            END AS raw_value
        FROM holding_roll
    ),
    product_signal AS (
        SELECT $1::varchar AS factor_code, product_symbol, trade_date, available_at, raw_value
        FROM price_signal
        WHERE raw_value IS NOT NULL
        UNION ALL
        SELECT $2::varchar AS factor_code, product_symbol, trade_date, available_at, raw_value
        FROM inventory_signal
        WHERE raw_value IS NOT NULL
        UNION ALL
        SELECT $3::varchar AS factor_code, product_symbol, trade_date, available_at, raw_value
        FROM holding_signal
        WHERE raw_value IS NOT NULL
    ),
    inserted AS (
        INSERT INTO market_futures_product_signal_pit
            (signal_code, source_version, product_symbol, trade_date, available_at, raw_value)
        SELECT factor_code, $4, product_symbol, trade_date, available_at, raw_value
        FROM product_signal
        WHERE product_symbol IS NOT NULL
          AND product_symbol <> ''
          AND trade_date >= ($5::date - INTERVAL '180 days')
          AND trade_date <= $6
          AND available_at IS NOT NULL
          AND available_at >= trade_date
          AND available_at <= ($6::date + INTERVAL '7 days')
        ON CONFLICT (signal_code, source_version, product_symbol, trade_date) DO UPDATE SET
            available_at = EXCLUDED.available_at,
            raw_value = EXCLUDED.raw_value,
            updated_at = NOW()
        RETURNING signal_code
    )
    SELECT signal_code, COUNT(*)::int8 AS row_count
    FROM inserted
    GROUP BY signal_code
    ORDER BY signal_code"
}

pub(crate) fn phase7_futures_price_chain_combo_backfill_sql() -> &'static str {
    "WITH weights AS (
        SELECT key AS factor_code, value::double precision AS weight
        FROM jsonb_each_text($3::jsonb)
    ),
    stock_days AS (
        SELECT trade_date
        FROM market_trade_calendar
        WHERE exchange = 'SSE'
          AND is_open = true
          AND trade_date BETWEEN $4 AND $5
    ),
    eligible_universe AS MATERIALIZED (
        SELECT
            bar.symbol,
            bar.trade_date
        FROM stock_days td
        JOIN market_stock_daily_bar bar
          ON bar.trade_date = td.trade_date
         AND bar.trade_date BETWEEN $4 AND $5
        JOIN market_stock ms
          ON ms.symbol = bar.symbol
        JOIN market_stock_daily_basic basic
          ON basic.symbol = bar.symbol
         AND basic.trade_date = bar.trade_date
         AND basic.trade_date BETWEEN $4 AND $5
        WHERE bar.close IS NOT NULL
          AND bar.close > 0
          AND basic.circ_mv IS NOT NULL
          AND basic.circ_mv > 0
          AND ms.list_date IS NOT NULL
          AND ms.list_date <= bar.trade_date
          AND (
              ms.delist_date IS NULL
              OR ms.delist_date >= bar.trade_date
          )
          AND ms.exchange IN ('SSE', 'SZSE')
          AND ms.market IN ('主板', '创业板')
          AND ms.symbol NOT LIKE '688%SH'
          AND COALESCE(ms.market, '') NOT ILIKE '%科创%'
          AND COALESCE(ms.market, '') NOT ILIKE '%北交%'
          AND NOT EXISTS (
              SELECT 1
              FROM market_stock_name_history st_name
              WHERE st_name.symbol = bar.symbol
                AND st_name.is_st = true
                AND st_name.start_date <= bar.trade_date
                AND COALESCE(st_name.end_date, DATE '9999-12-31') >= bar.trade_date
          )
    ),
    stock_membership AS MATERIALIZED (
        SELECT
            universe.symbol,
            universe.trade_date,
            membership.index_code,
            membership.available_at AS membership_available_at
        FROM eligible_universe universe
        JOIN market_stock_industry_membership_pit membership
          ON membership.symbol = universe.symbol
         AND membership.industry_level = 'L1'
         AND membership.classification_source = CASE
             WHEN universe.trade_date < DATE '2021-12-13' THEN 'SW2014'
             ELSE 'SW2021'
         END
         AND membership.available_at <= universe.trade_date
         AND membership.in_date <= universe.trade_date
         AND (
             membership.exit_available_at IS NULL
             OR membership.exit_available_at > universe.trade_date
         )
    ),
    signal_intervals AS (
        SELECT
            signal_code AS factor_code,
            product_symbol,
            trade_date,
            available_at,
            LEAD(available_at) OVER (
                PARTITION BY signal_code, product_symbol ORDER BY available_at, trade_date
            ) AS next_available_at,
            raw_value
        FROM market_futures_product_signal_pit
        WHERE source_version = $2
          AND signal_code IN (SELECT factor_code FROM weights)
          AND trade_date >= ($4::date - INTERVAL '180 days')
          AND trade_date <= $5
          AND available_at <= $5
          AND raw_value IS NOT NULL
          AND product_symbol IS NOT NULL
          AND product_symbol <> ''
          AND available_at >= trade_date
    ),
    signal_window AS (
        SELECT *
        FROM signal_intervals
        WHERE available_at IS NOT NULL
          AND available_at <= $5
    ),
    industry_signal AS MATERIALIZED (
        SELECT
            signal_window.factor_code,
            td.trade_date,
            mapping.exposure_code AS index_code,
            GREATEST(MAX(signal_window.available_at), MAX(mapping.available_at)) AS available_at,
            SUM(signal_window.raw_value * mapping.direction::double precision * mapping.weight::double precision)
                / NULLIF(SUM(ABS(mapping.weight::double precision)), 0.0) AS raw_value
        FROM stock_days td
        JOIN signal_window
          ON td.trade_date >= signal_window.available_at
         AND td.trade_date < COALESCE(signal_window.next_available_at, ($5::date + INTERVAL '1 day'))
        JOIN market_futures_product_exposure_mapping_pit mapping
          ON upper(mapping.product_symbol) = signal_window.product_symbol
         AND mapping.exposure_type = 'sw_industry'
         AND mapping.available_at <= td.trade_date
         AND mapping.valid_from <= signal_window.trade_date
         AND (
             mapping.valid_to IS NULL
             OR mapping.valid_to >= signal_window.trade_date
         )
        WHERE td.trade_date BETWEEN $4 AND $5
        GROUP BY signal_window.factor_code, td.trade_date, mapping.exposure_code
    ),
    raw AS (
        SELECT
            stock_membership.symbol,
            stock_membership.trade_date,
            GREATEST(industry_signal.available_at, stock_membership.membership_available_at) AS available_at,
            industry_signal.factor_code,
            industry_signal.raw_value
        FROM stock_membership
        JOIN industry_signal
          ON industry_signal.index_code = stock_membership.index_code
         AND industry_signal.trade_date = stock_membership.trade_date
        WHERE industry_signal.raw_value IS NOT NULL
          AND industry_signal.available_at <= stock_membership.trade_date
    ),
    ranked AS (
        SELECT
            factor_code,
            symbol,
            trade_date,
            available_at,
            raw_value,
            CASE
                WHEN COUNT(*) OVER (PARTITION BY factor_code, trade_date) = 1 THEN 1.0
                ELSE percent_rank() OVER (
                    PARTITION BY factor_code, trade_date ORDER BY raw_value
                )
            END AS normalized_value
        FROM raw
        WHERE raw_value IS NOT NULL
          AND available_at <= trade_date
    ),
    scores AS (
        SELECT
            ranked.symbol,
            ranked.trade_date,
            SUM(ranked.normalized_value::double precision * weights.weight)
                / NULLIF(SUM(weights.weight), 0.0) AS raw_score,
            MAX(ranked.available_at) AS available_at
        FROM ranked
        JOIN weights
          ON weights.factor_code = ranked.factor_code
        GROUP BY ranked.symbol, ranked.trade_date
        HAVING COUNT(DISTINCT ranked.factor_code) >= $6
    ),
    deleted AS (
        DELETE FROM multi_factor_value
        WHERE combo_name = $1
          AND version = $2
          AND trade_date BETWEEN $4 AND $5
        RETURNING 1
    )
    INSERT INTO multi_factor_value
        (combo_name, version, symbol, trade_date, raw_score, normalized_score, available_at)
    SELECT $1, $2, symbol, trade_date, raw_score, raw_score, available_at
    FROM scores
    WHERE raw_score IS NOT NULL
      AND available_at <= trade_date
    ON CONFLICT (combo_name, version, symbol, trade_date) DO UPDATE SET
        raw_score = EXCLUDED.raw_score,
        normalized_score = EXCLUDED.normalized_score,
        available_at = EXCLUDED.available_at,
        created_at = NOW()"
}

pub(crate) fn phase7_equity_pledge_pressure_backfill_sql() -> &'static str {
    "WITH weights AS (
        SELECT key AS factor_code, value::double precision AS weight
        FROM jsonb_each_text($3::jsonb)
        WHERE key = 'eq_pledge_low_ratio_std'
    ),
    stock_days AS (
        SELECT trade_date
        FROM market_trade_calendar
        WHERE exchange = 'SSE'
          AND is_open = true
          AND trade_date BETWEEN $4 AND $5
    ),
    eligible_universe AS MATERIALIZED (
        SELECT
            bar.symbol,
            bar.trade_date
        FROM stock_days td
        JOIN market_stock_daily_bar bar
          ON bar.trade_date = td.trade_date
         AND bar.trade_date BETWEEN $4 AND $5
        JOIN market_stock ms
          ON ms.symbol = bar.symbol
        JOIN market_stock_daily_basic basic
          ON basic.symbol = bar.symbol
         AND basic.trade_date = bar.trade_date
         AND basic.trade_date BETWEEN $4 AND $5
        WHERE bar.close IS NOT NULL
          AND bar.close > 0
          AND basic.circ_mv IS NOT NULL
          AND basic.circ_mv > 0
          AND ms.list_date IS NOT NULL
          AND ms.list_date <= bar.trade_date
          AND (
              ms.delist_date IS NULL
              OR ms.delist_date >= bar.trade_date
          )
          AND ms.exchange IN ('SSE', 'SZSE')
          AND ms.market IN ('主板', '创业板')
          AND ms.symbol NOT LIKE '688%SH'
          AND COALESCE(ms.market, '') NOT ILIKE '%科创%'
          AND COALESCE(ms.market, '') NOT ILIKE '%北交%'
          AND NOT EXISTS (
              SELECT 1
              FROM market_stock_name_history st_name
              WHERE st_name.symbol = bar.symbol
                AND st_name.is_st = true
                AND st_name.start_date <= bar.trade_date
                AND COALESCE(st_name.end_date, DATE '9999-12-31') >= bar.trade_date
          )
    ),
    latest_stat AS MATERIALIZED (
        SELECT
            universe.symbol,
            universe.trade_date,
            stat.available_at,
            COALESCE(
                stat.pledge_ratio::double precision,
                (
                    (COALESCE(stat.unrest_pledge, 0)::double precision
                     + COALESCE(stat.rest_pledge, 0)::double precision)
                    / NULLIF(stat.total_share::double precision, 0.0)
                ) * 100.0
            ) AS pledge_ratio
        FROM eligible_universe universe
        JOIN LATERAL (
            SELECT
                stat.end_date,
                stat.available_at,
                stat.pledge_ratio,
                stat.unrest_pledge,
                stat.rest_pledge,
                stat.total_share
            FROM market_stock_pledge_stat stat
            WHERE stat.symbol = universe.symbol
              AND stat.available_at <= universe.trade_date
              AND stat.end_date <= universe.trade_date
            ORDER BY stat.available_at DESC, stat.end_date DESC
            LIMIT 1
        ) stat ON true
    ),
    raw AS (
        SELECT
            symbol,
            trade_date,
            available_at,
            'eq_pledge_low_ratio_std'::varchar AS factor_code,
            -pledge_ratio AS raw_value
        FROM latest_stat
        WHERE pledge_ratio BETWEEN 0.0 AND 100.0
          AND available_at <= trade_date
    ),
    ranked AS (
        SELECT
            factor_code,
            symbol,
            trade_date,
            available_at,
            raw_value,
            CASE
                WHEN COUNT(*) OVER (PARTITION BY factor_code, trade_date) = 1 THEN 1.0
                ELSE percent_rank() OVER (
                    PARTITION BY factor_code, trade_date ORDER BY raw_value
                )
            END AS normalized_score
        FROM raw
        WHERE raw_value IS NOT NULL
          AND available_at <= trade_date
    ),
    scores AS (
        SELECT
            ranked.symbol,
            ranked.trade_date,
            SUM(ranked.raw_value::double precision * weights.weight)
                / NULLIF(SUM(weights.weight), 0.0) AS raw_score,
            SUM(ranked.normalized_score::double precision * weights.weight)
                / NULLIF(SUM(weights.weight), 0.0) AS normalized_score,
            MAX(ranked.available_at) AS available_at
        FROM ranked
        JOIN weights
          ON weights.factor_code = ranked.factor_code
        GROUP BY ranked.symbol, ranked.trade_date
    ),
    deleted AS (
        DELETE FROM multi_factor_value
        WHERE combo_name = $1
          AND version = $2
          AND trade_date BETWEEN $4 AND $5
        RETURNING 1
    )
    INSERT INTO multi_factor_value
        (combo_name, version, symbol, trade_date, raw_score, normalized_score, available_at)
    SELECT $1, $2, symbol, trade_date, raw_score, normalized_score, available_at
    FROM scores
    WHERE raw_score IS NOT NULL
      AND normalized_score IS NOT NULL
      AND available_at <= trade_date
    ON CONFLICT (combo_name, version, symbol, trade_date) DO UPDATE SET
        raw_score = EXCLUDED.raw_score,
        normalized_score = EXCLUDED.normalized_score,
        available_at = EXCLUDED.available_at,
        created_at = NOW()"
}

pub(crate) fn phase7_shareholder_structure_backfill_sql() -> &'static str {
    "WITH weights AS (
        SELECT key AS factor_code, value::double precision AS weight
        FROM jsonb_each_text($3::jsonb)
        WHERE key IN (
            'sh_holder_count_decline_1y_std',
            'sh_holder_count_decline_prev_std',
            'sh_holder_trade_net_increase_120d_std'
        )
    ),
    stock_days AS (
        SELECT trade_date
        FROM market_trade_calendar
        WHERE exchange = 'SSE'
          AND is_open = true
          AND trade_date BETWEEN $4 AND $5
    ),
    eligible_universe AS MATERIALIZED (
        SELECT
            bar.symbol,
            bar.trade_date
        FROM stock_days td
        JOIN market_stock_daily_bar bar
          ON bar.trade_date = td.trade_date
         AND bar.trade_date BETWEEN $4 AND $5
        JOIN market_stock ms
          ON ms.symbol = bar.symbol
        JOIN market_stock_daily_basic basic
          ON basic.symbol = bar.symbol
         AND basic.trade_date = bar.trade_date
         AND basic.trade_date BETWEEN $4 AND $5
        WHERE bar.close IS NOT NULL
          AND bar.close > 0
          AND basic.circ_mv IS NOT NULL
          AND basic.circ_mv > 0
          AND ms.list_date IS NOT NULL
          AND ms.list_date <= bar.trade_date
          AND (
              ms.delist_date IS NULL
              OR ms.delist_date >= bar.trade_date
          )
          AND ms.exchange IN ('SSE', 'SZSE')
          AND ms.market IN ('主板', '创业板')
          AND ms.symbol NOT LIKE '688%SH'
          AND COALESCE(ms.market, '') NOT ILIKE '%科创%'
          AND COALESCE(ms.market, '') NOT ILIKE '%北交%'
          AND NOT EXISTS (
              SELECT 1
              FROM market_stock_name_history st_name
              WHERE st_name.symbol = bar.symbol
                AND st_name.is_st = true
                AND st_name.start_date <= bar.trade_date
                AND COALESCE(st_name.end_date, DATE '9999-12-31') >= bar.trade_date
          )
    ),
    holder_number_candidates AS MATERIALIZED (
        SELECT DISTINCT ON (hn.symbol)
            hn.symbol,
            hn.available_at,
            hn.end_date,
            hn.holder_num::double precision AS holder_num,
            hn.source_row_hash
        FROM market_stock_holder_number hn
        WHERE hn.available_at < $4
          AND hn.available_at <= $5
          AND hn.available_at >= hn.ann_date
          AND hn.available_at >= hn.end_date
          AND hn.holder_num IS NOT NULL
          AND hn.holder_num > 0
        ORDER BY hn.symbol, hn.available_at DESC, hn.end_date DESC, hn.source_row_hash DESC
    ),
    holder_number_segment_events AS MATERIALIZED (
        SELECT
            hn.symbol,
            hn.available_at,
            hn.end_date,
            hn.holder_num::double precision AS holder_num,
            hn.source_row_hash
        FROM market_stock_holder_number hn
        WHERE hn.available_at >= $4
          AND hn.available_at <= $5
          AND hn.available_at >= hn.ann_date
          AND hn.available_at >= hn.end_date
          AND hn.holder_num IS NOT NULL
          AND hn.holder_num > 0
    ),
    holder_number_base AS MATERIALIZED (
        SELECT symbol, available_at, end_date, holder_num, source_row_hash
        FROM holder_number_candidates
        UNION ALL
        SELECT symbol, available_at, end_date, holder_num, source_row_hash
        FROM holder_number_segment_events
    ),
    holder_number_events AS MATERIALIZED (
        SELECT
            base.symbol,
            base.available_at,
            base.end_date,
            LEAD(base.available_at) OVER (
                PARTITION BY base.symbol
                ORDER BY base.available_at, base.end_date, base.source_row_hash
            ) AS next_available_at,
            base.holder_num,
            prev.holder_num AS prev_holder_num,
            prior_yoy.holder_num AS prior_yoy_holder_num
        FROM holder_number_base base
        LEFT JOIN LATERAL (
            SELECT hn.holder_num::double precision AS holder_num
            FROM market_stock_holder_number hn
            WHERE hn.symbol = base.symbol
              AND hn.available_at <= base.available_at
              AND hn.available_at >= hn.ann_date
              AND hn.available_at >= hn.end_date
              AND hn.holder_num IS NOT NULL
              AND hn.holder_num > 0
              AND (
                  hn.end_date < base.end_date
                  OR (
                      hn.end_date = base.end_date
                      AND hn.available_at < base.available_at
                  )
              )
            ORDER BY hn.end_date DESC, hn.available_at DESC, hn.source_row_hash DESC
            LIMIT 1
        ) prev ON true
        LEFT JOIN LATERAL (
            SELECT hn.holder_num::double precision AS holder_num
            FROM market_stock_holder_number hn
            WHERE hn.symbol = base.symbol
              AND hn.end_date <= base.end_date - INTERVAL '300 days'
              AND hn.available_at <= base.available_at
              AND hn.available_at >= hn.ann_date
              AND hn.available_at >= hn.end_date
              AND hn.holder_num IS NOT NULL
              AND hn.holder_num > 0
            ORDER BY hn.end_date DESC, hn.available_at DESC, hn.source_row_hash DESC
            LIMIT 1
        ) prior_yoy ON true
    ),
    holder_number_raw AS (
        SELECT
            universe.symbol,
            universe.trade_date,
            event.available_at,
            factor.factor_code,
            factor.raw_value
        FROM eligible_universe universe
        JOIN holder_number_events event
          ON event.symbol = universe.symbol
         AND event.available_at <= universe.trade_date
         AND universe.trade_date < COALESCE(event.next_available_at, DATE '9999-12-31')
        CROSS JOIN LATERAL (
            VALUES
                (
                    'sh_holder_count_decline_prev_std'::varchar,
                    CASE
                        WHEN event.prev_holder_num > 0
                        THEN -1.0 * ((event.holder_num / NULLIF(event.prev_holder_num, 0.0)) - 1.0)
                        ELSE NULL
                    END
                ),
                (
                    'sh_holder_count_decline_1y_std'::varchar,
                    CASE
                        WHEN event.prior_yoy_holder_num > 0
                        THEN -1.0 * ((event.holder_num / NULLIF(event.prior_yoy_holder_num, 0.0)) - 1.0)
                        ELSE NULL
                    END
                )
        ) AS factor(factor_code, raw_value)
        WHERE event.available_at <= universe.trade_date
    ),
    holder_trade_raw AS (
        SELECT
            universe.symbol,
            universe.trade_date,
            MAX(trade.available_at) AS available_at,
            'sh_holder_trade_net_increase_120d_std'::varchar AS factor_code,
            SUM(
                CASE
                    WHEN UPPER(COALESCE(trade.in_de, '')) = 'IN'
                        THEN ABS(COALESCE(trade.change_ratio::double precision, 0.0))
                    WHEN UPPER(COALESCE(trade.in_de, '')) = 'DE'
                        THEN -ABS(COALESCE(trade.change_ratio::double precision, 0.0))
                    ELSE COALESCE(trade.change_ratio::double precision, 0.0)
                END
            ) AS raw_value
        FROM eligible_universe universe
        JOIN market_stock_holder_trade trade
          ON trade.symbol = universe.symbol
         AND trade.available_at <= universe.trade_date
         AND trade.available_at > universe.trade_date - INTERVAL '120 days'
        WHERE trade.available_at >= trade.ann_date
          AND (trade.change_ratio IS NULL OR (trade.change_ratio >= -100 AND trade.change_ratio <= 100))
          AND (trade.after_ratio IS NULL OR (trade.after_ratio >= 0 AND trade.after_ratio <= 100))
          AND (trade.begin_date IS NULL OR trade.close_date IS NULL OR trade.close_date >= trade.begin_date)
        GROUP BY universe.symbol, universe.trade_date
    ),
    raw AS (
        SELECT symbol, trade_date, available_at, factor_code, raw_value
        FROM holder_number_raw
        UNION ALL
        SELECT symbol, trade_date, available_at, factor_code, raw_value
        FROM holder_trade_raw
    ),
    ranked AS (
        SELECT
            factor_code,
            symbol,
            trade_date,
            available_at,
            raw_value,
            CASE
                WHEN COUNT(*) OVER (PARTITION BY factor_code, trade_date) = 1 THEN 1.0
                ELSE percent_rank() OVER (
                    PARTITION BY factor_code, trade_date ORDER BY raw_value
                )
            END AS normalized_score
        FROM raw
        WHERE raw_value IS NOT NULL
          AND available_at <= trade_date
    ),
    scores AS (
        SELECT
            ranked.symbol,
            ranked.trade_date,
            SUM(ranked.raw_value::double precision * weights.weight)
                / NULLIF(SUM(weights.weight), 0.0) AS raw_score,
            SUM(ranked.normalized_score::double precision * weights.weight)
                / NULLIF(SUM(weights.weight), 0.0) AS normalized_score,
            MAX(ranked.available_at) AS available_at
        FROM ranked
        JOIN weights
          ON weights.factor_code = ranked.factor_code
        GROUP BY ranked.symbol, ranked.trade_date
    ),
    deleted AS (
        DELETE FROM multi_factor_value
        WHERE combo_name = $1
          AND version = $2
          AND trade_date BETWEEN $4 AND $5
        RETURNING 1
    )
    INSERT INTO multi_factor_value
        (combo_name, version, symbol, trade_date, raw_score, normalized_score, available_at)
    SELECT $1, $2, symbol, trade_date, raw_score, normalized_score, available_at
    FROM scores
    WHERE raw_score IS NOT NULL
      AND normalized_score IS NOT NULL
      AND available_at <= trade_date
    ON CONFLICT (combo_name, version, symbol, trade_date) DO UPDATE SET
        raw_score = EXCLUDED.raw_score,
        normalized_score = EXCLUDED.normalized_score,
        available_at = EXCLUDED.available_at,
        created_at = NOW()"
}

pub(crate) fn phase7_margin_detail_backfill_sql() -> &'static str {
    "WITH weights AS (
        SELECT key AS factor_code, value::double precision AS weight
        FROM jsonb_each_text($3::jsonb)
        WHERE key IN (
            'md_financing_buy_intensity_20d_std',
            'md_financing_balance_chg_20d_std',
            'md_short_sell_pressure_relief_20d_std'
        )
    ),
    weight_params AS (
        SELECT
            COALESCE(
                MAX(weight) FILTER (
                    WHERE factor_code = 'md_financing_buy_intensity_20d_std'
                ),
                0.0
            ) AS financing_buy_weight,
            COALESCE(
                MAX(weight) FILTER (
                    WHERE factor_code = 'md_financing_balance_chg_20d_std'
                ),
                0.0
            ) AS financing_balance_weight,
            COALESCE(
                MAX(weight) FILTER (
                    WHERE factor_code = 'md_short_sell_pressure_relief_20d_std'
                ),
                0.0
            ) AS short_sell_relief_weight
        FROM weights
    ),
    stock_days AS (
        SELECT trade_date
        FROM market_trade_calendar
        WHERE exchange = 'SSE'
          AND is_open = true
          AND trade_date BETWEEN $4 AND $5
    ),
    eligible_universe AS MATERIALIZED (
        SELECT
            bar.symbol,
            bar.trade_date
        FROM stock_days td
        JOIN market_stock_daily_bar bar
          ON bar.trade_date = td.trade_date
         AND bar.trade_date BETWEEN $4 AND $5
        JOIN market_stock ms
          ON ms.symbol = bar.symbol
        JOIN market_stock_daily_basic basic
          ON basic.symbol = bar.symbol
         AND basic.trade_date = bar.trade_date
         AND basic.trade_date BETWEEN $4 AND $5
        WHERE bar.close IS NOT NULL
          AND bar.close > 0
          AND basic.circ_mv IS NOT NULL
          AND basic.circ_mv > 0
          AND ms.list_date IS NOT NULL
          AND ms.list_date <= bar.trade_date
          AND (
              ms.delist_date IS NULL
              OR ms.delist_date >= bar.trade_date
          )
          AND ms.exchange IN ('SSE', 'SZSE')
          AND ms.market IN ('主板', '创业板')
          AND ms.symbol NOT LIKE '688%SH'
          AND COALESCE(ms.market, '') NOT ILIKE '%科创%'
          AND COALESCE(ms.market, '') NOT ILIKE '%北交%'
          AND NOT EXISTS (
              SELECT 1
              FROM market_stock_name_history st_name
              WHERE st_name.symbol = bar.symbol
                AND st_name.is_st = true
                AND st_name.start_date <= bar.trade_date
                AND COALESCE(st_name.end_date, DATE '9999-12-31') >= bar.trade_date
          )
    ),
    margin_observations AS MATERIALIZED (
        SELECT
            md.symbol,
            md.trade_date,
            md.available_at,
            md.rzmre::double precision AS rzmre,
            md.rzye::double precision AS rzye,
            md.rqmcl::double precision AS rqmcl,
            bar.volume::double precision AS volume,
            bar.amount::double precision AS amount,
            basic.circ_mv::double precision AS circ_mv
        FROM market_stock_margin_detail md
        JOIN market_stock_daily_bar bar
          ON bar.symbol = md.symbol
         AND bar.trade_date = md.trade_date
        JOIN market_stock_daily_basic basic
          ON basic.symbol = md.symbol
         AND basic.trade_date = md.trade_date
        WHERE md.trade_date >= ($4::date - INTERVAL '90 days')
          AND md.trade_date <= $5
          AND md.available_at <= $5
          AND md.available_at > md.trade_date
          AND md.available_at IS NOT NULL
          AND md.rzye IS NOT NULL
          AND md.rzmre IS NOT NULL
          AND bar.amount IS NOT NULL
          AND bar.amount > 0
          AND bar.volume IS NOT NULL
          AND bar.volume > 0
          AND basic.circ_mv IS NOT NULL
          AND basic.circ_mv > 0
    ),
    rolling AS MATERIALIZED (
        SELECT
            symbol,
            trade_date,
            available_at,
            SUM(md.rzmre::double precision) OVER (
                PARTITION BY symbol ORDER BY trade_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) / NULLIF(
                SUM(md.amount::double precision) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ),
                0.0
            ) AS financing_buy_intensity_20d,
            (
                rzye::double precision
                - LAG(md.rzye::double precision, 20) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                )
            ) / NULLIF(circ_mv::double precision, 0.0) AS financing_balance_chg_20d,
            -SUM(COALESCE(md.rqmcl::double precision, 0.0)) OVER (
                PARTITION BY symbol ORDER BY trade_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) / NULLIF(
                SUM(volume::double precision) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ),
                0.0
            ) AS short_sell_pressure_relief_20d,
            COUNT(md.rzmre) OVER (
                PARTITION BY symbol ORDER BY trade_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) AS obs_count
        FROM margin_observations md
    ),
    latest_signals AS (
        SELECT DISTINCT ON (universe.symbol, universe.trade_date)
            universe.symbol,
            universe.trade_date,
            rolling.available_at,
            rolling.financing_buy_intensity_20d,
            rolling.financing_balance_chg_20d,
            rolling.short_sell_pressure_relief_20d,
            GREATEST(
                0,
                (CASE WHEN rolling.financing_buy_intensity_20d IS NOT NULL THEN 1 ELSE 0 END)
                + (CASE WHEN rolling.financing_balance_chg_20d IS NOT NULL THEN 1 ELSE 0 END)
                + (CASE WHEN rolling.short_sell_pressure_relief_20d IS NOT NULL THEN 1 ELSE 0 END)
            ) AS valid_signal_count
        FROM eligible_universe universe
        JOIN rolling
          ON rolling.symbol = universe.symbol
         AND rolling.available_at = universe.trade_date
        WHERE rolling.obs_count >= 20
          AND rolling.available_at <= universe.trade_date
          AND GREATEST(
                0,
                (CASE WHEN rolling.financing_buy_intensity_20d IS NOT NULL THEN 1 ELSE 0 END)
                + (CASE WHEN rolling.financing_balance_chg_20d IS NOT NULL THEN 1 ELSE 0 END)
                + (CASE WHEN rolling.short_sell_pressure_relief_20d IS NOT NULL THEN 1 ELSE 0 END)
          ) >= 3
        ORDER BY universe.symbol, universe.trade_date, rolling.available_at DESC
    ),
    ranked_signals AS (
        SELECT
            symbol,
            trade_date,
            available_at,
            financing_buy_intensity_20d,
            financing_balance_chg_20d,
            short_sell_pressure_relief_20d,
            CASE
                WHEN COUNT(*) OVER (PARTITION BY trade_date) = 1 THEN 1.0
                ELSE percent_rank() OVER (
                    PARTITION BY trade_date ORDER BY financing_buy_intensity_20d
                )
            END AS financing_buy_intensity_rank,
            CASE
                WHEN COUNT(*) OVER (PARTITION BY trade_date) = 1 THEN 1.0
                ELSE percent_rank() OVER (
                    PARTITION BY trade_date ORDER BY financing_balance_chg_20d
                )
            END AS financing_balance_chg_rank,
            CASE
                WHEN COUNT(*) OVER (PARTITION BY trade_date) = 1 THEN 1.0
                ELSE percent_rank() OVER (
                    PARTITION BY trade_date ORDER BY short_sell_pressure_relief_20d
                )
            END AS short_sell_pressure_relief_rank
        FROM latest_signals
        WHERE valid_signal_count >= 3
          AND available_at <= trade_date
    ),
    scores AS (
        SELECT
            ranked_signals.symbol,
            ranked_signals.trade_date,
            (
                ranked_signals.financing_buy_intensity_20d
                    * weight_params.financing_buy_weight
                + ranked_signals.financing_balance_chg_20d
                    * weight_params.financing_balance_weight
                + ranked_signals.short_sell_pressure_relief_20d
                    * weight_params.short_sell_relief_weight
            ) / NULLIF(
                weight_params.financing_buy_weight
                + weight_params.financing_balance_weight
                + weight_params.short_sell_relief_weight,
                0.0
            ) AS raw_score,
            (
                ranked_signals.financing_buy_intensity_rank
                    * weight_params.financing_buy_weight
                + ranked_signals.financing_balance_chg_rank
                    * weight_params.financing_balance_weight
                + ranked_signals.short_sell_pressure_relief_rank
                    * weight_params.short_sell_relief_weight
            ) / NULLIF(
                weight_params.financing_buy_weight
                + weight_params.financing_balance_weight
                + weight_params.short_sell_relief_weight,
                0.0
            ) AS normalized_score,
            ranked_signals.available_at AS available_at
        FROM ranked_signals
        CROSS JOIN weight_params
        WHERE (
                weight_params.financing_buy_weight
                + weight_params.financing_balance_weight
                + weight_params.short_sell_relief_weight
            ) > 0.0
    ),
    deleted AS (
        DELETE FROM multi_factor_value
        WHERE combo_name = $1
          AND version = $2
          AND trade_date BETWEEN $4 AND $5
        RETURNING 1
    )
    INSERT INTO multi_factor_value
        (combo_name, version, symbol, trade_date, raw_score, normalized_score, available_at)
    SELECT $1, $2, symbol, trade_date, raw_score, normalized_score, available_at
    FROM scores
    WHERE raw_score IS NOT NULL
      AND normalized_score IS NOT NULL
      AND available_at <= trade_date
    ON CONFLICT (combo_name, version, symbol, trade_date) DO UPDATE SET
        raw_score = EXCLUDED.raw_score,
        normalized_score = EXCLUDED.normalized_score,
        available_at = EXCLUDED.available_at,
        created_at = NOW()"
}

pub(crate) fn phase7_analyst_revision_backfill_sql(
    value_expression: &'static str,
    higher_is_better: bool,
    window_days: i32,
    decay_days: i32,
) -> String {
    let rank_order = if higher_is_better {
        "raw_value"
    } else {
        "raw_value DESC"
    };
    let window_days = window_days.max(1);
    let decay_days = decay_days.max(1);

    format!(
        "WITH trade_days AS (
            SELECT trade_date
            FROM market_trade_calendar
            WHERE exchange = 'SSE'
              AND is_open = true
              AND trade_date BETWEEN $3 AND $4
        ),
        eligible_universe AS MATERIALIZED (
            SELECT
                bar.symbol,
                bar.trade_date
            FROM trade_days td
            JOIN market_stock_daily_bar bar
              ON bar.trade_date = td.trade_date
            JOIN market_stock ms
              ON ms.symbol = bar.symbol
            JOIN market_stock_daily_basic basic
              ON basic.symbol = bar.symbol
             AND basic.trade_date = bar.trade_date
            WHERE bar.close IS NOT NULL
              AND bar.close > 0
              AND basic.circ_mv IS NOT NULL
              AND basic.circ_mv > 0
              AND ms.list_date IS NOT NULL
              AND ms.list_date <= bar.trade_date
              AND (
                  ms.delist_date IS NULL
                  OR ms.delist_date >= bar.trade_date
              )
              AND ms.exchange IN ('SSE', 'SZSE')
              AND ms.market IN ('主板', '创业板')
              AND ms.symbol NOT LIKE '688%SH'
              AND COALESCE(ms.market, '') NOT ILIKE '%科创%'
              AND COALESCE(ms.market, '') NOT ILIKE '%北交%'
              AND NOT EXISTS (
                  SELECT 1
                  FROM market_stock_name_history st_name
                  WHERE st_name.symbol = bar.symbol
                    AND st_name.is_st = true
                    AND st_name.start_date <= bar.trade_date
                    AND COALESCE(st_name.end_date, DATE '9999-12-31') >= bar.trade_date
              )
        ),
        raw_events AS MATERIALIZED (
            SELECT
                ms.symbol,
                raw.available_at,
                raw.publication_date,
                CASE
                    WHEN raw.rating_change = '调高' THEN 1.0
                    WHEN raw.rating_change = '调低' THEN -1.0
                    ELSE NULL
                END AS rating_change_score,
                CASE
                    WHEN raw.is_first_rating = '是首次评级'
                     AND COALESCE(raw.rating_current, '') ~* '(买入|增持|推荐|强烈推荐|谨慎买入|谨慎增持|审慎推荐|BUY|OVERWEIGHT)'
                    THEN 1.0
                    WHEN raw.is_first_rating = '是首次评级' THEN 0.0
                    ELSE NULL
                END AS bullish_first_rating_score
            FROM market_vendor_analyst_revision_raw raw
            JOIN market_stock ms
              ON LEFT(ms.symbol, 6) = raw.symbol
            WHERE raw.vendor = 'akshare'
              AND raw.vendor_endpoint = 'stock_rank_forecast_cninfo'
              AND raw.available_at IS NOT NULL
              AND raw.available_at <= $4
              AND raw.available_at >= ($3::date - INTERVAL '{window_days} days')
              AND raw.available_at >= raw.publication_date
              AND raw.source_published_at IS NOT NULL
              AND raw.rating_previous IS NOT NULL
              AND raw.rating_change IS NOT NULL
              AND ms.exchange IN ('SSE', 'SZSE')
              AND ms.market IN ('主板', '创业板')
              AND ms.symbol NOT LIKE '688%SH'
              AND COALESCE(ms.market, '') NOT ILIKE '%科创%'
              AND COALESCE(ms.market, '') NOT ILIKE '%北交%'
        ),
        events AS (
            SELECT
                symbol,
                available_at,
                {value_expression} AS event_raw_value
            FROM raw_events
        ),
        expanded AS (
            SELECT
                universe.symbol,
                universe.trade_date,
                events.available_at,
                events.event_raw_value,
                GREATEST(
                    0.0,
                    1.0 - ((universe.trade_date - events.available_at)::double precision / {decay_days}.0)
                ) AS decay_weight
            FROM eligible_universe universe
            JOIN events
              ON events.symbol = universe.symbol
             AND universe.trade_date >= events.available_at
             AND universe.trade_date <= events.available_at + INTERVAL '{window_days} days'
            WHERE events.event_raw_value IS NOT NULL
              AND events.available_at <= universe.trade_date
        ),
        raw AS (
            SELECT
                symbol,
                trade_date,
                MAX(available_at) AS available_at,
                SUM(event_raw_value * decay_weight) AS raw_value
            FROM expanded
            WHERE decay_weight > 0.0
            GROUP BY symbol, trade_date
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                available_at,
                raw_value,
                CASE
                    WHEN COUNT(*) OVER (PARTITION BY trade_date) = 1 THEN 1.0
                    ELSE percent_rank() OVER (
                        PARTITION BY trade_date ORDER BY {rank_order}
                    )
                END AS normalized_value
            FROM raw
            WHERE raw_value IS NOT NULL
              AND available_at <= trade_date
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, available_at
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_analyst_revision_multi_backfill_sql() -> String {
    "WITH trade_days AS (
        SELECT trade_date
        FROM market_trade_calendar
        WHERE exchange = 'SSE'
          AND is_open = true
          AND trade_date BETWEEN $6 AND $7
    ),
    eligible_universe AS MATERIALIZED (
        SELECT
            bar.symbol,
            bar.trade_date
        FROM trade_days td
        JOIN market_stock_daily_bar bar
          ON bar.trade_date = td.trade_date
        JOIN market_stock ms
          ON ms.symbol = bar.symbol
        JOIN market_stock_daily_basic basic
          ON basic.symbol = bar.symbol
         AND basic.trade_date = bar.trade_date
        WHERE bar.close IS NOT NULL
          AND bar.close > 0
          AND basic.circ_mv IS NOT NULL
          AND basic.circ_mv > 0
          AND ms.list_date IS NOT NULL
          AND ms.list_date <= bar.trade_date
          AND (
              ms.delist_date IS NULL
              OR ms.delist_date >= bar.trade_date
          )
          AND ms.exchange IN ('SSE', 'SZSE')
          AND ms.market IN ('主板', '创业板')
          AND ms.symbol NOT LIKE '688%SH'
          AND COALESCE(ms.market, '') NOT ILIKE '%科创%'
          AND COALESCE(ms.market, '') NOT ILIKE '%北交%'
          AND NOT EXISTS (
              SELECT 1
              FROM market_stock_name_history st_name
              WHERE st_name.symbol = bar.symbol
                AND st_name.is_st = true
                AND st_name.start_date <= bar.trade_date
                AND COALESCE(st_name.end_date, DATE '9999-12-31') >= bar.trade_date
          )
    ),
    raw_events AS MATERIALIZED (
        SELECT
            ms.symbol,
            raw.available_at,
            raw.publication_date,
            CASE
                WHEN raw.rating_change = '调高' THEN 1.0
                WHEN raw.rating_change = '调低' THEN -1.0
                ELSE NULL
            END AS rating_change_score,
            CASE
                WHEN raw.is_first_rating = '是首次评级'
                 AND COALESCE(raw.rating_current, '') ~* '(买入|增持|推荐|强烈推荐|谨慎买入|谨慎增持|审慎推荐|BUY|OVERWEIGHT)'
                THEN 1.0
                WHEN raw.is_first_rating = '是首次评级' THEN 0.0
                ELSE NULL
            END AS bullish_first_rating_score
        FROM market_vendor_analyst_revision_raw raw
        JOIN market_stock ms
          ON LEFT(ms.symbol, 6) = raw.symbol
        WHERE raw.vendor = 'akshare'
          AND raw.vendor_endpoint = 'stock_rank_forecast_cninfo'
          AND raw.available_at IS NOT NULL
          AND raw.available_at <= $7
          AND raw.available_at >= ($6::date - INTERVAL '60 days')
          AND raw.available_at >= raw.publication_date
          AND raw.source_published_at IS NOT NULL
          AND raw.rating_previous IS NOT NULL
          AND raw.rating_change IS NOT NULL
          AND ms.exchange IN ('SSE', 'SZSE')
          AND ms.market IN ('主板', '创业板')
          AND ms.symbol NOT LIKE '688%SH'
          AND COALESCE(ms.market, '') NOT ILIKE '%科创%'
          AND COALESCE(ms.market, '') NOT ILIKE '%北交%'
    ),
    events AS MATERIALIZED (
        SELECT $1::varchar AS factor_code,
               symbol,
               available_at,
               rating_change_score AS event_raw_value,
               20::int AS window_days,
               20::int AS decay_days
        FROM raw_events
        WHERE rating_change_score IS NOT NULL
        UNION ALL
        SELECT $2::varchar AS factor_code,
               symbol,
               available_at,
               1.0 AS event_raw_value,
               20::int AS window_days,
               20::int AS decay_days
        FROM raw_events
        WHERE rating_change_score > 0.0
        UNION ALL
        SELECT $3::varchar AS factor_code,
               symbol,
               available_at,
               -1.0 AS event_raw_value,
               20::int AS window_days,
               20::int AS decay_days
        FROM raw_events
        WHERE rating_change_score < 0.0
        UNION ALL
        SELECT $4::varchar AS factor_code,
               symbol,
               available_at,
               bullish_first_rating_score AS event_raw_value,
               60::int AS window_days,
               60::int AS decay_days
        FROM raw_events
        WHERE bullish_first_rating_score IS NOT NULL
    ),
    expanded AS (
        SELECT
            events.factor_code,
            universe.symbol,
            universe.trade_date,
            events.available_at,
            events.event_raw_value,
            GREATEST(
                0.0,
                1.0 - ((universe.trade_date - events.available_at)::double precision / events.decay_days::double precision)
            ) AS decay_weight
        FROM eligible_universe universe
        JOIN events
          ON events.symbol = universe.symbol
         AND universe.trade_date >= events.available_at
         AND universe.trade_date <= events.available_at + events.window_days * INTERVAL '1 day'
        WHERE events.event_raw_value IS NOT NULL
          AND events.available_at <= universe.trade_date
    ),
    raw AS (
        SELECT
            factor_code,
            symbol,
            trade_date,
            MAX(available_at) AS available_at,
            SUM(event_raw_value * decay_weight) AS raw_value
        FROM expanded
        WHERE decay_weight > 0.0
        GROUP BY factor_code, symbol, trade_date
    ),
    ranked AS (
        SELECT
            factor_code,
            symbol,
            trade_date,
            available_at,
            raw_value,
            CASE
                WHEN COUNT(*) OVER (PARTITION BY factor_code, trade_date) = 1 THEN 1.0
                ELSE percent_rank() OVER (
                    PARTITION BY factor_code, trade_date ORDER BY raw_value
                )
            END AS normalized_value
        FROM raw
        WHERE raw_value IS NOT NULL
          AND available_at <= trade_date
    ),
    deleted AS (
        DELETE FROM factor_value
        WHERE factor_code IN ($1, $2, $3, $4)
          AND factor_version = $5
          AND trade_date BETWEEN $6 AND $7
        RETURNING 1
    ),
    upserted AS (
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT factor_code, $5, symbol, trade_date, raw_value, normalized_value, available_at
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()
        RETURNING factor_code
    )
    SELECT factor_code, COUNT(*)::int8 AS row_count
    FROM upserted
    GROUP BY factor_code
    ORDER BY factor_code"
        .to_string()
}

pub(crate) fn phase7_forecast_revision_backfill_sql(
    value_expression: &'static str,
    higher_is_better: bool,
    max_event_age_days: i32,
) -> String {
    let rank_order = if higher_is_better {
        "raw_value"
    } else {
        "raw_value DESC"
    };
    let max_event_age_days = max_event_age_days.max(1);

    format!(
        "WITH trade_days AS (
            SELECT trade_date
            FROM market_trade_calendar
            WHERE exchange = 'SSE'
              AND is_open = true
              AND trade_date BETWEEN $3 AND $4
        ),
        revision_events AS (
            SELECT
                latest.symbol,
                GREATEST(latest.available_at, previous.available_at) AS available_at,
                latest.end_date,
                latest.created_at,
                {value_expression} AS raw_value
            FROM market_stock_forecast latest
            JOIN LATERAL (
                SELECT
                    previous.available_at,
                    previous.ann_date,
                    previous.end_date,
                    previous.forecast_type,
                    previous.p_change_min,
                    previous.p_change_max,
                    previous.net_profit_min,
                    previous.net_profit_max,
                    previous.created_at
                FROM market_stock_forecast previous
                WHERE previous.symbol = latest.symbol
                  AND previous.end_date = latest.end_date
                  AND previous.available_at < latest.available_at
                  AND previous.available_at IS NOT NULL
                  AND (
                      previous.p_change_min IS NOT NULL
                      OR previous.p_change_max IS NOT NULL
                      OR previous.net_profit_min IS NOT NULL
                      OR previous.net_profit_max IS NOT NULL
                      OR previous.forecast_type IS NOT NULL
                  )
                ORDER BY previous.available_at DESC, previous.created_at DESC NULLS LAST
                LIMIT 1
            ) previous ON true
            WHERE latest.symbol IS NOT NULL
              AND latest.available_at IS NOT NULL
              AND latest.available_at <= $4
              AND (
                  latest.p_change_min IS NOT NULL
                  OR latest.p_change_max IS NOT NULL
                  OR latest.net_profit_min IS NOT NULL
                  OR latest.net_profit_max IS NOT NULL
                  OR latest.forecast_type IS NOT NULL
              )
        ),
        ranked_events AS (
            SELECT
                symbol,
                available_at,
                raw_value,
                ROW_NUMBER() OVER (
                    PARTITION BY symbol, available_at
                    ORDER BY end_date DESC NULLS LAST, created_at DESC NULLS LAST
                ) AS event_rank
            FROM revision_events
            WHERE raw_value IS NOT NULL
        ),
        deduped_events AS (
            SELECT symbol, available_at, raw_value
            FROM ranked_events
            WHERE event_rank = 1
        ),
        event_intervals AS (
            SELECT
                symbol,
                available_at,
                LEAD(available_at) OVER (PARTITION BY symbol ORDER BY available_at) AS next_available_at,
                raw_value
            FROM deduped_events
        ),
        latest AS (
            SELECT
                event_intervals.symbol,
                td.trade_date,
                event_intervals.available_at,
                event_intervals.raw_value
            FROM event_intervals
            JOIN trade_days td
              ON td.trade_date >= event_intervals.available_at
             AND td.trade_date <= event_intervals.available_at + INTERVAL '{max_event_age_days} days'
             AND td.trade_date < COALESCE(event_intervals.next_available_at, $4 + 1)
             AND td.trade_date BETWEEN $3 AND $4
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                available_at,
                raw_value,
                COUNT(*) OVER (PARTITION BY trade_date) AS symbol_count,
                CASE
                    WHEN COUNT(*) OVER (PARTITION BY trade_date) = 1 THEN 1.0
                    ELSE percent_rank() OVER (PARTITION BY trade_date ORDER BY {rank_order})
                END AS normalized_value
            FROM latest
            WHERE raw_value IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, available_at
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_event_window_backfill_sql(
    source_table: &'static str,
    value_expression: &'static str,
    higher_is_better: bool,
    window_days: i32,
    decay_days: i32,
) -> String {
    let rank_order = if higher_is_better {
        "raw_value"
    } else {
        "raw_value DESC"
    };
    let window_days = window_days.max(1);
    let decay_days = decay_days.max(1);

    format!(
        "WITH trade_days AS (
            SELECT trade_date
            FROM market_trade_calendar
            WHERE exchange = 'SSE'
              AND is_open = true
              AND trade_date BETWEEN $3 AND $4
        ),
        events AS (
            SELECT
                event.symbol,
                event.available_at,
                event.end_date,
                event.created_at,
                {value_expression} AS event_raw_value
            FROM {source_table} event
            WHERE event.available_at <= $4
              AND event.available_at >= $3 - INTERVAL '{window_days} days'
        ),
        expanded AS (
            SELECT
                events.symbol,
                td.trade_date,
                events.available_at,
                events.end_date,
                events.created_at,
                events.event_raw_value,
                GREATEST(0.0, 1.0 - ((td.trade_date - events.available_at)::double precision / {decay_days}.0)) AS decay_weight,
                ROW_NUMBER() OVER (
                    PARTITION BY events.symbol, td.trade_date
                    ORDER BY events.available_at DESC, events.end_date DESC, events.created_at DESC
                ) AS event_rank
            FROM events
            JOIN trade_days td
              ON td.trade_date >= events.available_at
             AND td.trade_date <= events.available_at + INTERVAL '{window_days} days'
            WHERE events.event_raw_value IS NOT NULL
        ),
        raw AS (
            SELECT
                symbol,
                trade_date,
                available_at,
                event_raw_value * decay_weight AS raw_value
            FROM expanded
            WHERE event_rank = 1
              AND decay_weight > 0.0
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                available_at,
                raw_value,
                COUNT(*) OVER (PARTITION BY trade_date) AS symbol_count,
                CASE
                    WHEN COUNT(*) OVER (PARTITION BY trade_date) = 1 THEN 1.0
                    ELSE percent_rank() OVER (PARTITION BY trade_date ORDER BY {rank_order})
                END AS normalized_value
            FROM raw
            WHERE raw_value IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, available_at
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_event_post_return_curve_backfill_sql(
    source_table: &'static str,
    event_filter_expression: &'static str,
    higher_is_better: bool,
    window_days: i32,
    industry_relative: bool,
    min_event_age_days: i32,
    max_event_age_days: i32,
) -> String {
    let rank_order = if higher_is_better {
        "raw_value"
    } else {
        "raw_value DESC"
    };
    let window_days = window_days.max(1);
    let min_event_age_days = min_event_age_days.clamp(0, window_days);
    let max_event_age_days = max_event_age_days.clamp(min_event_age_days, window_days);
    let raw_value_expression = if industry_relative {
        "raw_event_return - AVG(raw_event_return) OVER (PARTITION BY trade_date, industry)"
    } else {
        "raw_event_return"
    };

    format!(
        "WITH trade_days AS (
            SELECT trade_date
            FROM market_trade_calendar
            WHERE exchange = 'SSE'
              AND is_open = true
              AND trade_date BETWEEN $3 AND $4
        ),
        events AS (
            SELECT
                event.symbol,
                event.available_at,
                event.end_date,
                event.created_at
            FROM {source_table} event
            WHERE event.available_at <= $4
              AND event.available_at >= $3 - INTERVAL '{window_days} days'
              AND ({event_filter_expression})
        ),
        expanded AS (
            SELECT
                events.symbol,
                td.trade_date,
                events.available_at,
                COALESCE(NULLIF(ms.industry, ''), 'UNKNOWN') AS industry,
                (
                    current_bar.close::double precision
                    / NULLIF(anchor_bar.close::double precision, 0.0)
                    - 1.0
                )
                * GREATEST(
                    0.0,
                    1.0 - ((td.trade_date - events.available_at)::double precision / {window_days}.0)
                ) AS raw_event_return,
                ROW_NUMBER() OVER (
                    PARTITION BY events.symbol, td.trade_date
                    ORDER BY events.available_at DESC, events.end_date DESC, events.created_at DESC
                ) AS event_rank
            FROM events
            JOIN trade_days td
              ON td.trade_date >= events.available_at + INTERVAL '{min_event_age_days} days'
             AND td.trade_date <= events.available_at + INTERVAL '{max_event_age_days} days'
            JOIN market_stock ms
              ON ms.symbol = events.symbol
            JOIN LATERAL (
                SELECT close, trade_date
                FROM market_stock_daily_bar_adj anchor_bar
                WHERE anchor_bar.symbol = events.symbol
                  AND anchor_bar.trade_date <= events.available_at
                  AND anchor_bar.close IS NOT NULL
                  AND anchor_bar.close > 0
                ORDER BY anchor_bar.trade_date DESC
                LIMIT 1
            ) anchor_bar ON true
            JOIN market_stock_daily_bar_adj current_bar
              ON current_bar.symbol = events.symbol
             AND current_bar.trade_date = td.trade_date
             AND current_bar.close IS NOT NULL
             AND current_bar.close > 0
        ),
        latest_event AS (
            SELECT
                symbol,
                trade_date,
                available_at,
                industry,
                raw_event_return
            FROM expanded
            WHERE event_rank = 1
              AND raw_event_return IS NOT NULL
        ),
        residualized AS (
            SELECT
                symbol,
                trade_date,
                available_at,
                {raw_value_expression} AS raw_value
            FROM latest_event
        ),
        ranked AS (
            SELECT
                symbol,
                trade_date,
                available_at,
                raw_value,
                COUNT(*) OVER (PARTITION BY trade_date) AS symbol_count,
                CASE
                    WHEN COUNT(*) OVER (PARTITION BY trade_date) = 1 THEN 1.0
                    ELSE percent_rank() OVER (PARTITION BY trade_date ORDER BY {rank_order})
                END AS normalized_value
            FROM residualized
            WHERE raw_value IS NOT NULL
        )
        INSERT INTO factor_value
            (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
        SELECT $1, $2, symbol, trade_date, raw_value, normalized_value, available_at
        FROM ranked
        ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            normalized_value = EXCLUDED.normalized_value,
            available_at = EXCLUDED.available_at,
            created_at = NOW()"
    )
}

pub(crate) fn phase7_combo_backfill_sql() -> &'static str {
    "WITH weights AS (
        SELECT key AS factor_code, value::double precision AS weight
        FROM jsonb_each_text($3::jsonb)
    ),
    scores AS (
        SELECT
            fv.symbol,
            fv.trade_date,
            SUM(fv.normalized_value::double precision * weights.weight)
                / NULLIF(SUM(weights.weight), 0.0) AS raw_score,
            MAX(COALESCE(fv.available_at, fv.trade_date)) AS available_at
        FROM weights
        JOIN factor_value fv
          ON fv.factor_code = weights.factor_code
         AND fv.factor_version = $2
         AND fv.trade_date BETWEEN $4 AND $5
         AND fv.normalized_value IS NOT NULL
        GROUP BY fv.symbol, fv.trade_date
        HAVING COUNT(DISTINCT fv.factor_code) >= $6
    )
    INSERT INTO multi_factor_value
        (combo_name, version, symbol, trade_date, raw_score, normalized_score, available_at)
    SELECT $1, $2, symbol, trade_date, raw_score, raw_score, available_at
    FROM scores
    ON CONFLICT (combo_name, version, symbol, trade_date) DO UPDATE SET
        raw_score = EXCLUDED.raw_score,
        normalized_score = EXCLUDED.normalized_score,
        available_at = EXCLUDED.available_at,
        created_at = NOW()"
}

pub(crate) fn phase7_alpha_blend_backfill_sql(combo_method: &str) -> &'static str {
    match combo_method {
        "weighted_combo_optional_overlay" => phase7_optional_overlay_blend_backfill_sql(),
        _ => phase7_strict_alpha_blend_backfill_sql(),
    }
}

pub(crate) fn phase7_strict_alpha_blend_backfill_sql() -> &'static str {
    "WITH sources AS (
        SELECT combo_name, version, weight
        FROM jsonb_to_recordset($3::jsonb)
             AS sources(combo_name text, version text, weight double precision)
    ),
    scores AS (
        SELECT
            mfv.symbol,
            mfv.trade_date,
            SUM(mfv.normalized_score::double precision * sources.weight) AS raw_score,
            MAX(COALESCE(mfv.available_at, mfv.trade_date)) AS available_at
        FROM multi_factor_value mfv
        JOIN sources
          ON sources.combo_name = mfv.combo_name
         AND sources.version = mfv.version
        WHERE mfv.trade_date BETWEEN $4 AND $5
          AND mfv.normalized_score IS NOT NULL
        GROUP BY mfv.symbol, mfv.trade_date
        HAVING COUNT(DISTINCT (sources.combo_name, sources.version)) = $6
    )
    INSERT INTO multi_factor_value
        (combo_name, version, symbol, trade_date, raw_score, normalized_score, available_at)
    SELECT $1, $2, symbol, trade_date, raw_score, raw_score, available_at
    FROM scores
    ON CONFLICT (combo_name, version, symbol, trade_date) DO UPDATE SET
        raw_score = EXCLUDED.raw_score,
        normalized_score = EXCLUDED.normalized_score,
        available_at = EXCLUDED.available_at,
        created_at = NOW()"
}

pub(crate) fn phase7_optional_overlay_blend_backfill_sql() -> &'static str {
    "WITH sources AS (
        SELECT
            source_record.item ->> 'combo_name' AS combo_name,
            source_record.item ->> 'version' AS version,
            (source_record.item ->> 'weight')::double precision AS weight,
            source_record.ordinality
        FROM jsonb_array_elements($3::jsonb) WITH ORDINALITY AS source_record(item, ordinality)
    ),
    required_sources AS (
        SELECT combo_name, version, weight
        FROM sources
        WHERE ordinality = 1
    ),
    optional_sources AS (
        SELECT combo_name, version, weight
        FROM sources
        WHERE ordinality > 1
    ),
    required_scores AS (
        SELECT
            mfv.symbol,
            mfv.trade_date,
            SUM(mfv.normalized_score::double precision * required_sources.weight) AS required_score,
            MAX(COALESCE(mfv.available_at, mfv.trade_date)) AS required_available_at
        FROM multi_factor_value mfv
        JOIN required_sources
          ON required_sources.combo_name = mfv.combo_name
         AND required_sources.version = mfv.version
        WHERE mfv.trade_date BETWEEN $4 AND $5
          AND mfv.normalized_score IS NOT NULL
        GROUP BY mfv.symbol, mfv.trade_date
        HAVING COUNT(DISTINCT (required_sources.combo_name, required_sources.version)) = $6
    ),
    scores AS (
        SELECT
            required_scores.symbol,
            required_scores.trade_date,
            required_scores.required_score
                + COALESCE(SUM(optional_mfv.normalized_score::double precision * optional_sources.weight), 0.0)
                AS raw_score,
            GREATEST(
                required_scores.required_available_at,
                COALESCE(MAX(COALESCE(optional_mfv.available_at, optional_mfv.trade_date)), required_scores.required_available_at)
            ) AS available_at
        FROM required_scores
        LEFT JOIN optional_sources ON true
        LEFT JOIN multi_factor_value optional_mfv
          ON optional_mfv.combo_name = optional_sources.combo_name
         AND optional_mfv.version = optional_sources.version
         AND optional_mfv.symbol = required_scores.symbol
         AND optional_mfv.trade_date = required_scores.trade_date
         AND optional_mfv.normalized_score IS NOT NULL
        GROUP BY required_scores.symbol, required_scores.trade_date, required_scores.required_score, required_scores.required_available_at
    )
    INSERT INTO multi_factor_value
        (combo_name, version, symbol, trade_date, raw_score, normalized_score, available_at)
    SELECT $1, $2, symbol, trade_date, raw_score, raw_score, available_at
    FROM scores
    ON CONFLICT (combo_name, version, symbol, trade_date) DO UPDATE SET
        raw_score = EXCLUDED.raw_score,
        normalized_score = EXCLUDED.normalized_score,
        available_at = EXCLUDED.available_at,
        created_at = NOW()"
}
