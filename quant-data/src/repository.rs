//! 数据仓储层
//!
//! 使用 SQLx 裸 SQL 查询（时序/批量走 SQLx + TimescaleDB）。
//! 遵循 05-表结构设计.md 中的表结构。

use chrono::NaiveDate;
use sqlx::PgPool;
use std::collections::HashSet;
use tracing::{debug, info};

use crate::model::entities::{
    MarketAdjustmentFactor, MarketFuturesDaily, MarketFuturesHoldingRank,
    MarketFuturesWarehouseReceipt, MarketIndexDailyBar, MarketStock, MarketStockCashflow,
    MarketStockDailyBar, MarketStockDailyBasic, MarketStockDisclosureDate, MarketStockDividend,
    MarketStockExpress, MarketStockForecast, MarketStockIndustryMembershipPit,
    MarketStockMainBusiness, MarketStockMarginDetail, MarketStockMoneyflow, MarketStockRepurchase,
    MarketStockShareFloat, MarketTradeCalendar,
};

// ─── market_stock ────────────────────────────────────────────────

pub async fn upsert_stock(pool: &PgPool, stock: &MarketStock) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"INSERT INTO market_stock (symbol, name, exchange, market, industry, list_status, list_date, delist_date, is_st)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
           ON CONFLICT (symbol) DO UPDATE SET
             name = EXCLUDED.name,
             market = EXCLUDED.market,
             industry = EXCLUDED.industry,
             list_status = EXCLUDED.list_status,
             list_date = EXCLUDED.list_date,
             delist_date = EXCLUDED.delist_date,
             is_st = EXCLUDED.is_st,
             updated_at = now()"#,
    )
    .bind(&stock.symbol)
    .bind(&stock.name)
    .bind(&stock.exchange)
    .bind(&stock.market)
    .bind(&stock.industry)
    .bind(&stock.list_status)
    .bind(stock.list_date)
    .bind(stock.delist_date)
    .bind(stock.is_st)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn upsert_stocks_batch(
    pool: &PgPool,
    stocks: &[MarketStock],
) -> Result<usize, sqlx::Error> {
    let mut count = 0;
    for stock in stocks {
        upsert_stock(pool, stock).await?;
        count += 1;
    }
    debug!("批量 upsert {} 条 stock_basic", count);
    Ok(count)
}

pub async fn count_stocks(pool: &PgPool) -> Result<i64, sqlx::Error> {
    let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM market_stock")
        .fetch_one(pool)
        .await?;
    Ok(row.0)
}

pub async fn list_listed_stock_symbols(pool: &PgPool) -> Result<Vec<String>, sqlx::Error> {
    let rows: Vec<(String,)> =
        sqlx::query_as("SELECT symbol FROM market_stock WHERE list_status = 'L' ORDER BY symbol")
            .fetch_all(pool)
            .await?;
    Ok(rows.into_iter().map(|(symbol,)| symbol).collect())
}

// ─── market_stock_daily_bar ──────────────────────────────────────

pub async fn upsert_daily_bar(
    pool: &PgPool,
    bar: &MarketStockDailyBar,
    data_version_id: &str,
    source: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"INSERT INTO market_stock_daily_bar
             (symbol, trade_date, open, high, low, close, pre_close, pct_change,
              volume, amount, source, data_version_id)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
           ON CONFLICT (symbol, trade_date) DO UPDATE SET
             open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
             close = EXCLUDED.close, pre_close = EXCLUDED.pre_close,
             pct_change = EXCLUDED.pct_change,
             volume = EXCLUDED.volume, amount = EXCLUDED.amount,
             source = EXCLUDED.source, data_version_id = EXCLUDED.data_version_id"#,
    )
    .bind(&bar.symbol)
    .bind(bar.trade_date)
    .bind(bar.open)
    .bind(bar.high)
    .bind(bar.low)
    .bind(bar.close)
    .bind(bar.pre_close)
    .bind(bar.change_pct)
    .bind(bar.volume)
    .bind(bar.amount)
    .bind(source)
    .bind(data_version_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn upsert_daily_bars_batch(
    pool: &PgPool,
    bars: &[MarketStockDailyBar],
    data_version_id: &str,
    source: &str,
) -> Result<usize, sqlx::Error> {
    let mut count = 0;
    for bar in bars {
        upsert_daily_bar(pool, bar, data_version_id, source).await?;
        count += 1;
    }
    info!("批量 upsert {} 条日线", count);
    Ok(count)
}

pub async fn get_daily_date_range(
    pool: &PgPool,
    symbol: &str,
) -> Result<(Option<NaiveDate>, Option<NaiveDate>), sqlx::Error> {
    let row: Option<(Option<NaiveDate>, Option<NaiveDate>)> = sqlx::query_as(
        "SELECT MIN(trade_date), MAX(trade_date) FROM market_stock_daily_bar WHERE symbol = $1",
    )
    .bind(symbol)
    .fetch_optional(pool)
    .await?;
    Ok(row.unwrap_or((None, None)))
}

// ─── market_stock_daily_basic ────────────────────────────────────

pub async fn upsert_daily_basic(
    pool: &PgPool,
    row: &MarketStockDailyBasic,
    data_version_id: &str,
    source: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"INSERT INTO market_stock_daily_basic
             (symbol, trade_date, pe_ttm, pb, ps_ttm, dv_ttm, total_share, float_share, free_share,
              total_mv, circ_mv,
              source, data_version_id)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
           ON CONFLICT (symbol, trade_date) DO UPDATE SET
             pe_ttm = EXCLUDED.pe_ttm,
             pb = EXCLUDED.pb,
             ps_ttm = EXCLUDED.ps_ttm,
             dv_ttm = EXCLUDED.dv_ttm,
             total_share = EXCLUDED.total_share,
             float_share = EXCLUDED.float_share,
             free_share = EXCLUDED.free_share,
             total_mv = EXCLUDED.total_mv,
             circ_mv = EXCLUDED.circ_mv,
             source = EXCLUDED.source,
             data_version_id = EXCLUDED.data_version_id"#,
    )
    .bind(&row.symbol)
    .bind(row.trade_date)
    .bind(row.pe_ttm)
    .bind(row.pb)
    .bind(row.ps_ttm)
    .bind(row.dv_ttm)
    .bind(row.total_share)
    .bind(row.float_share)
    .bind(row.free_share)
    .bind(row.total_mv)
    .bind(row.circ_mv)
    .bind(source)
    .bind(data_version_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn upsert_daily_basic_batch(
    pool: &PgPool,
    rows: &[MarketStockDailyBasic],
    data_version_id: &str,
    source: &str,
) -> Result<usize, sqlx::Error> {
    if rows.is_empty() {
        return Ok(0);
    }

    let mut saved = 0usize;
    for chunk in rows.chunks(2_000) {
        let mut builder = sqlx::QueryBuilder::<sqlx::Postgres>::new(
            "INSERT INTO market_stock_daily_basic \
             (symbol, trade_date, pe_ttm, pb, ps_ttm, dv_ttm, total_share, float_share, free_share, \
              total_mv, circ_mv, \
              source, data_version_id) ",
        );

        builder.push_values(chunk, |mut row_builder, item| {
            row_builder
                .push_bind(&item.symbol)
                .push_bind(item.trade_date)
                .push_bind(item.pe_ttm)
                .push_bind(item.pb)
                .push_bind(item.ps_ttm)
                .push_bind(item.dv_ttm)
                .push_bind(item.total_share)
                .push_bind(item.float_share)
                .push_bind(item.free_share)
                .push_bind(item.total_mv)
                .push_bind(item.circ_mv)
                .push_bind(source)
                .push_bind(data_version_id);
        });

        builder.push(
            " ON CONFLICT (symbol, trade_date) DO UPDATE SET \
              pe_ttm = EXCLUDED.pe_ttm, \
              pb = EXCLUDED.pb, \
              ps_ttm = EXCLUDED.ps_ttm, \
              dv_ttm = EXCLUDED.dv_ttm, \
              total_share = EXCLUDED.total_share, \
              float_share = EXCLUDED.float_share, \
              free_share = EXCLUDED.free_share, \
              total_mv = EXCLUDED.total_mv, \
              circ_mv = EXCLUDED.circ_mv, \
              source = EXCLUDED.source, \
              data_version_id = EXCLUDED.data_version_id",
        );

        let result = builder.build().execute(pool).await?;
        saved += result.rows_affected() as usize;
    }

    info!("批量 upsert {} 条每日估值基础数据", saved);
    Ok(saved)
}

// ─── market_stock_moneyflow ─────────────────────────────────────

pub async fn upsert_moneyflow(
    pool: &PgPool,
    row: &MarketStockMoneyflow,
    data_version_id: &str,
    source: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"INSERT INTO market_stock_moneyflow
             (symbol, trade_date,
              buy_sm_vol, buy_sm_amount, sell_sm_vol, sell_sm_amount,
              buy_md_vol, buy_md_amount, sell_md_vol, sell_md_amount,
              buy_lg_vol, buy_lg_amount, sell_lg_vol, sell_lg_amount,
              buy_elg_vol, buy_elg_amount, sell_elg_vol, sell_elg_amount,
              net_mf_vol, net_mf_amount, source, data_version_id)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                   $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
           ON CONFLICT (symbol, trade_date) DO UPDATE SET
             buy_sm_vol = EXCLUDED.buy_sm_vol,
             buy_sm_amount = EXCLUDED.buy_sm_amount,
             sell_sm_vol = EXCLUDED.sell_sm_vol,
             sell_sm_amount = EXCLUDED.sell_sm_amount,
             buy_md_vol = EXCLUDED.buy_md_vol,
             buy_md_amount = EXCLUDED.buy_md_amount,
             sell_md_vol = EXCLUDED.sell_md_vol,
             sell_md_amount = EXCLUDED.sell_md_amount,
             buy_lg_vol = EXCLUDED.buy_lg_vol,
             buy_lg_amount = EXCLUDED.buy_lg_amount,
             sell_lg_vol = EXCLUDED.sell_lg_vol,
             sell_lg_amount = EXCLUDED.sell_lg_amount,
             buy_elg_vol = EXCLUDED.buy_elg_vol,
             buy_elg_amount = EXCLUDED.buy_elg_amount,
             sell_elg_vol = EXCLUDED.sell_elg_vol,
             sell_elg_amount = EXCLUDED.sell_elg_amount,
             net_mf_vol = EXCLUDED.net_mf_vol,
             net_mf_amount = EXCLUDED.net_mf_amount,
             source = EXCLUDED.source,
             data_version_id = EXCLUDED.data_version_id"#,
    )
    .bind(&row.symbol)
    .bind(row.trade_date)
    .bind(row.buy_sm_vol)
    .bind(row.buy_sm_amount)
    .bind(row.sell_sm_vol)
    .bind(row.sell_sm_amount)
    .bind(row.buy_md_vol)
    .bind(row.buy_md_amount)
    .bind(row.sell_md_vol)
    .bind(row.sell_md_amount)
    .bind(row.buy_lg_vol)
    .bind(row.buy_lg_amount)
    .bind(row.sell_lg_vol)
    .bind(row.sell_lg_amount)
    .bind(row.buy_elg_vol)
    .bind(row.buy_elg_amount)
    .bind(row.sell_elg_vol)
    .bind(row.sell_elg_amount)
    .bind(row.net_mf_vol)
    .bind(row.net_mf_amount)
    .bind(source)
    .bind(data_version_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn upsert_moneyflow_batch(
    pool: &PgPool,
    rows: &[MarketStockMoneyflow],
    data_version_id: &str,
    source: &str,
) -> Result<usize, sqlx::Error> {
    if rows.is_empty() {
        return Ok(0);
    }

    let mut saved = 0usize;
    for chunk in rows.chunks(1_000) {
        let mut builder = sqlx::QueryBuilder::<sqlx::Postgres>::new(
            "INSERT INTO market_stock_moneyflow \
             (symbol, trade_date, \
              buy_sm_vol, buy_sm_amount, sell_sm_vol, sell_sm_amount, \
              buy_md_vol, buy_md_amount, sell_md_vol, sell_md_amount, \
              buy_lg_vol, buy_lg_amount, sell_lg_vol, sell_lg_amount, \
              buy_elg_vol, buy_elg_amount, sell_elg_vol, sell_elg_amount, \
              net_mf_vol, net_mf_amount, source, data_version_id) ",
        );

        builder.push_values(chunk, |mut row_builder, item| {
            row_builder
                .push_bind(&item.symbol)
                .push_bind(item.trade_date)
                .push_bind(item.buy_sm_vol)
                .push_bind(item.buy_sm_amount)
                .push_bind(item.sell_sm_vol)
                .push_bind(item.sell_sm_amount)
                .push_bind(item.buy_md_vol)
                .push_bind(item.buy_md_amount)
                .push_bind(item.sell_md_vol)
                .push_bind(item.sell_md_amount)
                .push_bind(item.buy_lg_vol)
                .push_bind(item.buy_lg_amount)
                .push_bind(item.sell_lg_vol)
                .push_bind(item.sell_lg_amount)
                .push_bind(item.buy_elg_vol)
                .push_bind(item.buy_elg_amount)
                .push_bind(item.sell_elg_vol)
                .push_bind(item.sell_elg_amount)
                .push_bind(item.net_mf_vol)
                .push_bind(item.net_mf_amount)
                .push_bind(source)
                .push_bind(data_version_id);
        });

        builder.push(
            " ON CONFLICT (symbol, trade_date) DO UPDATE SET \
              buy_sm_vol = EXCLUDED.buy_sm_vol, \
              buy_sm_amount = EXCLUDED.buy_sm_amount, \
              sell_sm_vol = EXCLUDED.sell_sm_vol, \
              sell_sm_amount = EXCLUDED.sell_sm_amount, \
              buy_md_vol = EXCLUDED.buy_md_vol, \
              buy_md_amount = EXCLUDED.buy_md_amount, \
              sell_md_vol = EXCLUDED.sell_md_vol, \
              sell_md_amount = EXCLUDED.sell_md_amount, \
              buy_lg_vol = EXCLUDED.buy_lg_vol, \
              buy_lg_amount = EXCLUDED.buy_lg_amount, \
              sell_lg_vol = EXCLUDED.sell_lg_vol, \
              sell_lg_amount = EXCLUDED.sell_lg_amount, \
              buy_elg_vol = EXCLUDED.buy_elg_vol, \
              buy_elg_amount = EXCLUDED.buy_elg_amount, \
              sell_elg_vol = EXCLUDED.sell_elg_vol, \
              sell_elg_amount = EXCLUDED.sell_elg_amount, \
              net_mf_vol = EXCLUDED.net_mf_vol, \
              net_mf_amount = EXCLUDED.net_mf_amount, \
              source = EXCLUDED.source, \
              data_version_id = EXCLUDED.data_version_id",
        );

        let result = builder.build().execute(pool).await?;
        saved += result.rows_affected() as usize;
    }

    info!("批量 upsert {} 条每日资金流数据", saved);
    Ok(saved)
}

// ─── market_stock_margin_detail ─────────────────────────────────

pub async fn upsert_margin_detail_batch(
    pool: &PgPool,
    rows: &[MarketStockMarginDetail],
    data_version_id: &str,
    source: &str,
) -> Result<usize, sqlx::Error> {
    if rows.is_empty() {
        return Ok(0);
    }

    let mut saved = 0usize;
    for chunk in rows.chunks(1_000) {
        let mut builder = sqlx::QueryBuilder::<sqlx::Postgres>::new(
            "INSERT INTO market_stock_margin_detail \
             (symbol, trade_date, name, rzye, rqye, rzmre, rqyl, rzche, rqchl, rqmcl, rzrqye, \
              available_at, source_published_at, raw_payload, source, data_version_id) ",
        );

        builder.push_values(chunk, |mut row_builder, item| {
            row_builder
                .push_bind(&item.symbol)
                .push_bind(item.trade_date)
                .push_bind(&item.name)
                .push_bind(item.rzye)
                .push_bind(item.rqye)
                .push_bind(item.rzmre)
                .push_bind(item.rqyl)
                .push_bind(item.rzche)
                .push_bind(item.rqchl)
                .push_bind(item.rqmcl)
                .push_bind(item.rzrqye)
                .push_bind(item.available_at)
                .push_bind(item.source_published_at)
                .push_bind(&item.raw_payload)
                .push_bind(source)
                .push_bind(data_version_id);
        });

        builder.push(
            " ON CONFLICT (symbol, trade_date) DO UPDATE SET \
              name = EXCLUDED.name, \
              rzye = EXCLUDED.rzye, \
              rqye = EXCLUDED.rqye, \
              rzmre = EXCLUDED.rzmre, \
              rqyl = EXCLUDED.rqyl, \
              rzche = EXCLUDED.rzche, \
              rqchl = EXCLUDED.rqchl, \
              rqmcl = EXCLUDED.rqmcl, \
              rzrqye = EXCLUDED.rzrqye, \
              available_at = EXCLUDED.available_at, \
              source_published_at = EXCLUDED.source_published_at, \
              raw_payload = EXCLUDED.raw_payload, \
              source = EXCLUDED.source, \
              data_version_id = EXCLUDED.data_version_id, \
              updated_at = now()",
        );

        let result = builder.build().execute(pool).await?;
        saved += result.rows_affected() as usize;
    }

    info!("批量 upsert {} 条个股融资融券明细", saved);
    Ok(saved)
}

// ─── market_stock_forecast ───────────────────────────────────────

pub async fn upsert_forecast(
    pool: &PgPool,
    row: &MarketStockForecast,
    data_version_id: &str,
    source: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"INSERT INTO market_stock_forecast
             (symbol, ann_date, end_date, forecast_type, p_change_min, p_change_max,
              net_profit_min, net_profit_max, first_ann_date, available_at, summary,
              change_reason, raw_payload, source, data_version_id)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
           ON CONFLICT (symbol, ann_date, end_date, forecast_type, first_ann_date, available_at) DO UPDATE SET
             p_change_min = EXCLUDED.p_change_min,
             p_change_max = EXCLUDED.p_change_max,
             net_profit_min = EXCLUDED.net_profit_min,
             net_profit_max = EXCLUDED.net_profit_max,
             available_at = EXCLUDED.available_at,
             summary = EXCLUDED.summary,
             change_reason = EXCLUDED.change_reason,
             raw_payload = EXCLUDED.raw_payload,
             source = EXCLUDED.source,
             data_version_id = EXCLUDED.data_version_id"#,
    )
    .bind(&row.symbol)
    .bind(row.ann_date)
    .bind(row.end_date)
    .bind(&row.forecast_type)
    .bind(row.p_change_min)
    .bind(row.p_change_max)
    .bind(row.net_profit_min)
    .bind(row.net_profit_max)
    .bind(row.first_ann_date)
    .bind(row.available_at)
    .bind(&row.summary)
    .bind(&row.change_reason)
    .bind(&row.raw_payload)
    .bind(source)
    .bind(data_version_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn upsert_forecast_batch(
    pool: &PgPool,
    rows: &[MarketStockForecast],
    data_version_id: &str,
    source: &str,
) -> Result<usize, sqlx::Error> {
    if rows.is_empty() {
        return Ok(0);
    }

    let mut seen = HashSet::new();
    let unique_rows: Vec<MarketStockForecast> = rows
        .iter()
        .filter(|row| {
            seen.insert((
                row.symbol.clone(),
                row.ann_date,
                row.end_date,
                row.forecast_type.clone(),
                row.first_ann_date,
                row.available_at,
            ))
        })
        .cloned()
        .collect();
    let mut saved = 0usize;
    for chunk in unique_rows.chunks(1_000) {
        let mut builder = sqlx::QueryBuilder::<sqlx::Postgres>::new(
            "INSERT INTO market_stock_forecast \
             (symbol, ann_date, end_date, forecast_type, p_change_min, p_change_max, \
              net_profit_min, net_profit_max, first_ann_date, available_at, summary, \
              change_reason, raw_payload, source, data_version_id) ",
        );
        builder.push_values(chunk, |mut row_builder, item| {
            row_builder
                .push_bind(&item.symbol)
                .push_bind(item.ann_date)
                .push_bind(item.end_date)
                .push_bind(&item.forecast_type)
                .push_bind(item.p_change_min)
                .push_bind(item.p_change_max)
                .push_bind(item.net_profit_min)
                .push_bind(item.net_profit_max)
                .push_bind(item.first_ann_date)
                .push_bind(item.available_at)
                .push_bind(&item.summary)
                .push_bind(&item.change_reason)
                .push_bind(&item.raw_payload)
                .push_bind(source)
                .push_bind(data_version_id);
        });
        builder.push(
            " ON CONFLICT (symbol, ann_date, end_date, forecast_type, first_ann_date, available_at) DO UPDATE SET \
              p_change_min = EXCLUDED.p_change_min, \
              p_change_max = EXCLUDED.p_change_max, \
              net_profit_min = EXCLUDED.net_profit_min, \
              net_profit_max = EXCLUDED.net_profit_max, \
              available_at = EXCLUDED.available_at, \
              summary = EXCLUDED.summary, \
              change_reason = EXCLUDED.change_reason, \
              raw_payload = EXCLUDED.raw_payload, \
              source = EXCLUDED.source, \
              data_version_id = EXCLUDED.data_version_id",
        );
        let result = builder.build().execute(pool).await?;
        saved += result.rows_affected() as usize;
    }
    info!(
        "批量 upsert {} 条业绩预告数据，去重 {} 条",
        saved,
        rows.len().saturating_sub(unique_rows.len())
    );
    Ok(saved)
}

// ─── market_stock_express ────────────────────────────────────────

pub async fn upsert_express(
    pool: &PgPool,
    row: &MarketStockExpress,
    data_version_id: &str,
    source: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"INSERT INTO market_stock_express
             (symbol, ann_date, end_date, revenue, n_income, yoy_sales, yoy_dedu_np,
              diluted_eps, diluted_roe, is_audit, available_at, perf_summary, remark,
              raw_payload, source, data_version_id)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
           ON CONFLICT (symbol, ann_date, end_date, available_at) DO UPDATE SET
             revenue = EXCLUDED.revenue,
             n_income = EXCLUDED.n_income,
             yoy_sales = EXCLUDED.yoy_sales,
             yoy_dedu_np = EXCLUDED.yoy_dedu_np,
             diluted_eps = EXCLUDED.diluted_eps,
             diluted_roe = EXCLUDED.diluted_roe,
             is_audit = EXCLUDED.is_audit,
             available_at = EXCLUDED.available_at,
             perf_summary = EXCLUDED.perf_summary,
             remark = EXCLUDED.remark,
             raw_payload = EXCLUDED.raw_payload,
             source = EXCLUDED.source,
             data_version_id = EXCLUDED.data_version_id"#,
    )
    .bind(&row.symbol)
    .bind(row.ann_date)
    .bind(row.end_date)
    .bind(row.revenue)
    .bind(row.n_income)
    .bind(row.yoy_sales)
    .bind(row.yoy_dedu_np)
    .bind(row.diluted_eps)
    .bind(row.diluted_roe)
    .bind(row.is_audit)
    .bind(row.available_at)
    .bind(&row.perf_summary)
    .bind(&row.remark)
    .bind(&row.raw_payload)
    .bind(source)
    .bind(data_version_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn upsert_express_batch(
    pool: &PgPool,
    rows: &[MarketStockExpress],
    data_version_id: &str,
    source: &str,
) -> Result<usize, sqlx::Error> {
    if rows.is_empty() {
        return Ok(0);
    }

    let mut seen = HashSet::new();
    let unique_rows: Vec<MarketStockExpress> = rows
        .iter()
        .filter(|row| {
            seen.insert((
                row.symbol.clone(),
                row.ann_date,
                row.end_date,
                row.available_at,
            ))
        })
        .cloned()
        .collect();
    let mut saved = 0usize;
    for chunk in unique_rows.chunks(1_000) {
        let mut builder = sqlx::QueryBuilder::<sqlx::Postgres>::new(
            "INSERT INTO market_stock_express \
             (symbol, ann_date, end_date, revenue, n_income, yoy_sales, yoy_dedu_np, \
              diluted_eps, diluted_roe, is_audit, available_at, perf_summary, remark, \
              raw_payload, source, data_version_id) ",
        );
        builder.push_values(chunk, |mut row_builder, item| {
            row_builder
                .push_bind(&item.symbol)
                .push_bind(item.ann_date)
                .push_bind(item.end_date)
                .push_bind(item.revenue)
                .push_bind(item.n_income)
                .push_bind(item.yoy_sales)
                .push_bind(item.yoy_dedu_np)
                .push_bind(item.diluted_eps)
                .push_bind(item.diluted_roe)
                .push_bind(item.is_audit)
                .push_bind(item.available_at)
                .push_bind(&item.perf_summary)
                .push_bind(&item.remark)
                .push_bind(&item.raw_payload)
                .push_bind(source)
                .push_bind(data_version_id);
        });
        builder.push(
            " ON CONFLICT (symbol, ann_date, end_date, available_at) DO UPDATE SET \
              revenue = EXCLUDED.revenue, \
              n_income = EXCLUDED.n_income, \
              yoy_sales = EXCLUDED.yoy_sales, \
              yoy_dedu_np = EXCLUDED.yoy_dedu_np, \
              diluted_eps = EXCLUDED.diluted_eps, \
              diluted_roe = EXCLUDED.diluted_roe, \
              is_audit = EXCLUDED.is_audit, \
              available_at = EXCLUDED.available_at, \
              perf_summary = EXCLUDED.perf_summary, \
              remark = EXCLUDED.remark, \
              raw_payload = EXCLUDED.raw_payload, \
              source = EXCLUDED.source, \
              data_version_id = EXCLUDED.data_version_id",
        );
        let result = builder.build().execute(pool).await?;
        saved += result.rows_affected() as usize;
    }
    info!(
        "批量 upsert {} 条业绩快报数据，去重 {} 条",
        saved,
        rows.len().saturating_sub(unique_rows.len())
    );
    Ok(saved)
}

// ─── market_stock_disclosure_date ───────────────────────────────

pub async fn upsert_disclosure_date(
    pool: &PgPool,
    row: &MarketStockDisclosureDate,
    data_version_id: &str,
    source: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"INSERT INTO market_stock_disclosure_date
             (symbol, end_date, ann_date, pre_date, actual_date, modify_date,
              available_at, raw_payload, source, data_version_id)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
           ON CONFLICT (symbol, end_date, available_at) DO UPDATE SET
             ann_date = EXCLUDED.ann_date,
             pre_date = EXCLUDED.pre_date,
             actual_date = EXCLUDED.actual_date,
             modify_date = EXCLUDED.modify_date,
             available_at = EXCLUDED.available_at,
             raw_payload = EXCLUDED.raw_payload,
             source = EXCLUDED.source,
             data_version_id = EXCLUDED.data_version_id"#,
    )
    .bind(&row.symbol)
    .bind(row.end_date)
    .bind(row.ann_date)
    .bind(row.pre_date)
    .bind(row.actual_date)
    .bind(row.modify_date)
    .bind(row.available_at)
    .bind(&row.raw_payload)
    .bind(source)
    .bind(data_version_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn upsert_disclosure_date_batch(
    pool: &PgPool,
    rows: &[MarketStockDisclosureDate],
    data_version_id: &str,
    source: &str,
) -> Result<usize, sqlx::Error> {
    if rows.is_empty() {
        return Ok(0);
    }

    let mut seen = HashSet::new();
    let unique_rows: Vec<MarketStockDisclosureDate> = rows
        .iter()
        .filter(|row| seen.insert((row.symbol.clone(), row.end_date, row.available_at)))
        .cloned()
        .collect();
    let mut saved = 0usize;
    for chunk in unique_rows.chunks(1_000) {
        let mut builder = sqlx::QueryBuilder::<sqlx::Postgres>::new(
            "INSERT INTO market_stock_disclosure_date \
             (symbol, end_date, ann_date, pre_date, actual_date, modify_date, \
              available_at, raw_payload, source, data_version_id) ",
        );
        builder.push_values(chunk, |mut row_builder, item| {
            row_builder
                .push_bind(&item.symbol)
                .push_bind(item.end_date)
                .push_bind(item.ann_date)
                .push_bind(item.pre_date)
                .push_bind(item.actual_date)
                .push_bind(item.modify_date)
                .push_bind(item.available_at)
                .push_bind(&item.raw_payload)
                .push_bind(source)
                .push_bind(data_version_id);
        });
        builder.push(
            " ON CONFLICT (symbol, end_date, available_at) DO UPDATE SET \
              ann_date = EXCLUDED.ann_date, \
              pre_date = EXCLUDED.pre_date, \
              actual_date = EXCLUDED.actual_date, \
              modify_date = EXCLUDED.modify_date, \
              available_at = EXCLUDED.available_at, \
              raw_payload = EXCLUDED.raw_payload, \
              source = EXCLUDED.source, \
              data_version_id = EXCLUDED.data_version_id",
        );
        let result = builder.build().execute(pool).await?;
        saved += result.rows_affected() as usize;
    }
    info!(
        "批量 upsert {} 条财报披露日期数据，去重 {} 条",
        saved,
        rows.len().saturating_sub(unique_rows.len())
    );
    Ok(saved)
}

// ─── market_stock_cashflow ──────────────────────────────────────

pub async fn upsert_cashflow_batch(
    pool: &PgPool,
    rows: &[MarketStockCashflow],
    data_version_id: &str,
    source: &str,
) -> Result<usize, sqlx::Error> {
    if rows.is_empty() {
        return Ok(0);
    }

    let mut seen = HashSet::new();
    let unique_rows: Vec<MarketStockCashflow> = rows
        .iter()
        .filter(|row| {
            seen.insert((
                row.symbol.clone(),
                row.end_date,
                row.ann_date,
                row.available_at,
            ))
        })
        .cloned()
        .collect();
    let mut saved = 0usize;
    for chunk in unique_rows.chunks(1_000) {
        let mut builder = sqlx::QueryBuilder::<sqlx::Postgres>::new(
            "INSERT INTO market_stock_cashflow \
             (symbol, ann_date, f_ann_date, end_date, available_at, net_profit, \
              n_cashflow_act, c_cash_equ_end_period, raw_payload, source, data_version_id) ",
        );
        builder.push_values(chunk, |mut row_builder, item| {
            row_builder
                .push_bind(&item.symbol)
                .push_bind(item.ann_date)
                .push_bind(item.f_ann_date)
                .push_bind(item.end_date)
                .push_bind(item.available_at)
                .push_bind(item.net_profit)
                .push_bind(item.n_cashflow_act)
                .push_bind(item.c_cash_equ_end_period)
                .push_bind(&item.raw_payload)
                .push_bind(source)
                .push_bind(data_version_id);
        });
        builder.push(
            " ON CONFLICT (symbol, end_date, ann_date, available_at) DO UPDATE SET \
              f_ann_date = EXCLUDED.f_ann_date, \
              net_profit = EXCLUDED.net_profit, \
              n_cashflow_act = EXCLUDED.n_cashflow_act, \
              c_cash_equ_end_period = EXCLUDED.c_cash_equ_end_period, \
              raw_payload = EXCLUDED.raw_payload, \
              source = EXCLUDED.source, \
              data_version_id = EXCLUDED.data_version_id",
        );
        let result = builder.build().execute(pool).await?;
        saved += result.rows_affected() as usize;
    }
    info!(
        "批量 upsert {} 条现金流量表数据，去重 {} 条",
        saved,
        rows.len().saturating_sub(unique_rows.len())
    );
    Ok(saved)
}

// ─── market_stock_dividend ──────────────────────────────────────

pub async fn upsert_dividend_batch(
    pool: &PgPool,
    rows: &[MarketStockDividend],
    data_version_id: &str,
    source: &str,
) -> Result<usize, sqlx::Error> {
    if rows.is_empty() {
        return Ok(0);
    }

    let mut seen = HashSet::new();
    let unique_rows: Vec<MarketStockDividend> = rows
        .iter()
        .filter(|row| {
            seen.insert((
                row.symbol.clone(),
                row.end_date,
                row.ann_date,
                row.div_proc.clone(),
                row.available_at,
            ))
        })
        .cloned()
        .collect();
    let mut saved = 0usize;
    for chunk in unique_rows.chunks(1_000) {
        let mut builder = sqlx::QueryBuilder::<sqlx::Postgres>::new(
            "INSERT INTO market_stock_dividend \
             (symbol, end_date, ann_date, div_proc, available_at, cash_div, cash_div_tax, \
              record_date, ex_date, pay_date, imp_ann_date, raw_payload, source, data_version_id) ",
        );
        builder.push_values(chunk, |mut row_builder, item| {
            row_builder
                .push_bind(&item.symbol)
                .push_bind(item.end_date)
                .push_bind(item.ann_date)
                .push_bind(&item.div_proc)
                .push_bind(item.available_at)
                .push_bind(item.cash_div)
                .push_bind(item.cash_div_tax)
                .push_bind(item.record_date)
                .push_bind(item.ex_date)
                .push_bind(item.pay_date)
                .push_bind(item.imp_ann_date)
                .push_bind(&item.raw_payload)
                .push_bind(source)
                .push_bind(data_version_id);
        });
        builder.push(
            " ON CONFLICT (symbol, end_date, ann_date, div_proc, available_at) DO UPDATE SET \
              cash_div = EXCLUDED.cash_div, \
              cash_div_tax = EXCLUDED.cash_div_tax, \
              record_date = EXCLUDED.record_date, \
              ex_date = EXCLUDED.ex_date, \
              pay_date = EXCLUDED.pay_date, \
              imp_ann_date = EXCLUDED.imp_ann_date, \
              raw_payload = EXCLUDED.raw_payload, \
              source = EXCLUDED.source, \
              data_version_id = EXCLUDED.data_version_id",
        );
        let result = builder.build().execute(pool).await?;
        saved += result.rows_affected() as usize;
    }
    info!(
        "批量 upsert {} 条分红送股数据，去重 {} 条",
        saved,
        rows.len().saturating_sub(unique_rows.len())
    );
    Ok(saved)
}

// ─── market_stock_repurchase ────────────────────────────────────

pub async fn upsert_repurchase_batch(
    pool: &PgPool,
    rows: &[MarketStockRepurchase],
    data_version_id: &str,
    source: &str,
) -> Result<usize, sqlx::Error> {
    if rows.is_empty() {
        return Ok(0);
    }

    let mut seen = HashSet::new();
    let unique_rows: Vec<MarketStockRepurchase> = rows
        .iter()
        .filter(|row| {
            seen.insert((
                row.symbol.clone(),
                row.ann_date,
                row.end_date,
                row.proc.clone(),
                row.available_at,
            ))
        })
        .cloned()
        .collect();
    let mut saved = 0usize;
    for chunk in unique_rows.chunks(1_000) {
        let mut builder = sqlx::QueryBuilder::<sqlx::Postgres>::new(
            "INSERT INTO market_stock_repurchase \
             (symbol, ann_date, end_date, proc, available_at, exp_date, vol, amount, \
              high_limit, low_limit, raw_payload, source, data_version_id) ",
        );
        builder.push_values(chunk, |mut row_builder, item| {
            row_builder
                .push_bind(&item.symbol)
                .push_bind(item.ann_date)
                .push_bind(item.end_date)
                .push_bind(&item.proc)
                .push_bind(item.available_at)
                .push_bind(item.exp_date)
                .push_bind(item.vol)
                .push_bind(item.amount)
                .push_bind(item.high_limit)
                .push_bind(item.low_limit)
                .push_bind(&item.raw_payload)
                .push_bind(source)
                .push_bind(data_version_id);
        });
        builder.push(
            " ON CONFLICT (symbol, ann_date, end_date, proc, available_at) DO UPDATE SET \
              exp_date = EXCLUDED.exp_date, \
              vol = EXCLUDED.vol, \
              amount = EXCLUDED.amount, \
              high_limit = EXCLUDED.high_limit, \
              low_limit = EXCLUDED.low_limit, \
              raw_payload = EXCLUDED.raw_payload, \
              source = EXCLUDED.source, \
              data_version_id = EXCLUDED.data_version_id",
        );
        let result = builder.build().execute(pool).await?;
        saved += result.rows_affected() as usize;
    }
    info!(
        "批量 upsert {} 条股票回购数据，去重 {} 条",
        saved,
        rows.len().saturating_sub(unique_rows.len())
    );
    Ok(saved)
}

// ─── market_stock_share_float ───────────────────────────────────

pub async fn upsert_share_float_batch(
    pool: &PgPool,
    rows: &[MarketStockShareFloat],
    data_version_id: &str,
    source: &str,
) -> Result<usize, sqlx::Error> {
    if rows.is_empty() {
        return Ok(0);
    }

    let mut seen = HashSet::new();
    let unique_rows: Vec<MarketStockShareFloat> = rows
        .iter()
        .filter(|row| {
            seen.insert((
                row.symbol.clone(),
                row.ann_date,
                row.float_date,
                row.holder_name.clone(),
                row.share_type.clone(),
                row.available_at,
            ))
        })
        .cloned()
        .collect();
    let mut saved = 0usize;
    for chunk in unique_rows.chunks(1_000) {
        let mut builder = sqlx::QueryBuilder::<sqlx::Postgres>::new(
            "INSERT INTO market_stock_share_float \
             (symbol, ann_date, float_date, available_at, float_share, float_ratio, \
              holder_name, share_type, raw_payload, source, data_version_id) ",
        );
        builder.push_values(chunk, |mut row_builder, item| {
            row_builder
                .push_bind(&item.symbol)
                .push_bind(item.ann_date)
                .push_bind(item.float_date)
                .push_bind(item.available_at)
                .push_bind(item.float_share)
                .push_bind(item.float_ratio)
                .push_bind(&item.holder_name)
                .push_bind(&item.share_type)
                .push_bind(&item.raw_payload)
                .push_bind(source)
                .push_bind(data_version_id);
        });
        builder.push(
            " ON CONFLICT (symbol, ann_date, float_date, holder_name, share_type, available_at) DO UPDATE SET \
              float_share = EXCLUDED.float_share, \
              float_ratio = EXCLUDED.float_ratio, \
              raw_payload = EXCLUDED.raw_payload, \
              source = EXCLUDED.source, \
              data_version_id = EXCLUDED.data_version_id",
        );
        let result = builder.build().execute(pool).await?;
        saved += result.rows_affected() as usize;
    }
    info!(
        "批量 upsert {} 条限售股解禁数据，去重 {} 条",
        saved,
        rows.len().saturating_sub(unique_rows.len())
    );
    Ok(saved)
}

// ─── market_stock_main_business ─────────────────────────────────

pub async fn upsert_main_business_batch(
    pool: &PgPool,
    rows: &[MarketStockMainBusiness],
    data_version_id: &str,
    source: &str,
) -> Result<usize, sqlx::Error> {
    if rows.is_empty() {
        return Ok(0);
    }

    let mut seen = HashSet::new();
    let unique_rows: Vec<MarketStockMainBusiness> = rows
        .iter()
        .filter(|row| {
            seen.insert((
                row.symbol.clone(),
                row.end_date,
                row.business_type.clone(),
                row.source_row_hash.clone(),
                row.available_at,
            ))
        })
        .cloned()
        .collect();
    let mut saved = 0usize;
    for chunk in unique_rows.chunks(1_000) {
        let mut builder = sqlx::QueryBuilder::<sqlx::Postgres>::new(
            "INSERT INTO market_stock_main_business \
             (symbol, end_date, available_at, business_type, bz_item, bz_code, bz_sales, bz_profit, \
              bz_cost, curr_type, update_flag, source_row_hash, raw_payload, source, data_version_id) ",
        );
        builder.push_values(chunk, |mut row_builder, item| {
            row_builder
                .push_bind(&item.symbol)
                .push_bind(item.end_date)
                .push_bind(item.available_at)
                .push_bind(&item.business_type)
                .push_bind(&item.bz_item)
                .push_bind(&item.bz_code)
                .push_bind(item.bz_sales)
                .push_bind(item.bz_profit)
                .push_bind(item.bz_cost)
                .push_bind(&item.curr_type)
                .push_bind(&item.update_flag)
                .push_bind(&item.source_row_hash)
                .push_bind(&item.raw_payload)
                .push_bind(source)
                .push_bind(data_version_id);
        });
        builder.push(
            " ON CONFLICT (symbol, end_date, business_type, source_row_hash, available_at) DO UPDATE SET \
              bz_item = EXCLUDED.bz_item, \
              bz_code = EXCLUDED.bz_code, \
              bz_sales = EXCLUDED.bz_sales, \
              bz_profit = EXCLUDED.bz_profit, \
              bz_cost = EXCLUDED.bz_cost, \
              curr_type = EXCLUDED.curr_type, \
              update_flag = EXCLUDED.update_flag, \
              raw_payload = EXCLUDED.raw_payload, \
              source = EXCLUDED.source, \
              data_version_id = EXCLUDED.data_version_id, \
              updated_at = now()",
        );
        let result = builder.build().execute(pool).await?;
        saved += result.rows_affected() as usize;
    }
    info!(
        "批量 upsert {} 条主营业务构成数据，去重 {} 条",
        saved,
        rows.len().saturating_sub(unique_rows.len())
    );
    Ok(saved)
}

// ─── market_futures_* raw price-chain sources ───────────────────

pub async fn upsert_futures_daily_batch(
    pool: &PgPool,
    rows: &[MarketFuturesDaily],
    data_version_id: &str,
    source: &str,
) -> Result<usize, sqlx::Error> {
    if rows.is_empty() {
        return Ok(0);
    }

    let mut seen = HashSet::new();
    let unique_rows: Vec<MarketFuturesDaily> = rows
        .iter()
        .filter(|row| seen.insert((row.ts_code.clone(), row.trade_date)))
        .cloned()
        .collect();
    let mut saved = 0usize;
    for chunk in unique_rows.chunks(1_000) {
        let mut builder = sqlx::QueryBuilder::<sqlx::Postgres>::new(
            "INSERT INTO market_futures_daily \
             (ts_code, trade_date, pre_close, pre_settle, open, high, low, close, settle, change1, \
              change2, vol, amount, oi, oi_chg, delv_settle, available_at, source_published_at, \
              raw_payload, data_version_id, source) ",
        );
        builder.push_values(chunk, |mut row_builder, item| {
            row_builder
                .push_bind(&item.ts_code)
                .push_bind(item.trade_date)
                .push_bind(item.pre_close)
                .push_bind(item.pre_settle)
                .push_bind(item.open)
                .push_bind(item.high)
                .push_bind(item.low)
                .push_bind(item.close)
                .push_bind(item.settle)
                .push_bind(item.change1)
                .push_bind(item.change2)
                .push_bind(item.vol)
                .push_bind(item.amount)
                .push_bind(item.oi)
                .push_bind(item.oi_chg)
                .push_bind(item.delv_settle)
                .push_bind(item.available_at)
                .push_bind(item.source_published_at)
                .push_bind(&item.raw_payload)
                .push_bind(data_version_id)
                .push_bind(source);
        });
        builder.push(
            " ON CONFLICT (ts_code, trade_date) DO UPDATE SET \
              pre_close = EXCLUDED.pre_close, \
              pre_settle = EXCLUDED.pre_settle, \
              open = EXCLUDED.open, \
              high = EXCLUDED.high, \
              low = EXCLUDED.low, \
              close = EXCLUDED.close, \
              settle = EXCLUDED.settle, \
              change1 = EXCLUDED.change1, \
              change2 = EXCLUDED.change2, \
              vol = EXCLUDED.vol, \
              amount = EXCLUDED.amount, \
              oi = EXCLUDED.oi, \
              oi_chg = EXCLUDED.oi_chg, \
              delv_settle = EXCLUDED.delv_settle, \
              available_at = EXCLUDED.available_at, \
              source_published_at = EXCLUDED.source_published_at, \
              raw_payload = EXCLUDED.raw_payload, \
              data_version_id = EXCLUDED.data_version_id, \
              source = EXCLUDED.source, \
              updated_at = now()",
        );
        let result = builder.build().execute(pool).await?;
        saved += result.rows_affected() as usize;
    }
    info!(
        "批量 upsert {} 条期货日线数据，去重 {} 条",
        saved,
        rows.len().saturating_sub(unique_rows.len())
    );
    Ok(saved)
}

pub async fn upsert_futures_warehouse_receipt_batch(
    pool: &PgPool,
    rows: &[MarketFuturesWarehouseReceipt],
    data_version_id: &str,
    source: &str,
) -> Result<usize, sqlx::Error> {
    if rows.is_empty() {
        return Ok(0);
    }

    let mut seen = HashSet::new();
    let unique_rows: Vec<MarketFuturesWarehouseReceipt> = rows
        .iter()
        .filter(|row| {
            seen.insert((
                row.trade_date,
                row.symbol.clone(),
                row.exchange.clone(),
                row.warehouse.clone(),
            ))
        })
        .cloned()
        .collect();
    let mut saved = 0usize;
    for chunk in unique_rows.chunks(1_000) {
        let mut builder = sqlx::QueryBuilder::<sqlx::Postgres>::new(
            "INSERT INTO market_futures_warehouse_receipt \
             (trade_date, symbol, exchange, fut_name, warehouse, wh_id, pre_vol, vol, vol_chg, \
              area, year, grade, brand, place, pd, is_ct, unit, available_at, source_published_at, \
              raw_payload, data_version_id, source) ",
        );
        builder.push_values(chunk, |mut row_builder, item| {
            row_builder
                .push_bind(item.trade_date)
                .push_bind(&item.symbol)
                .push_bind(&item.exchange)
                .push_bind(&item.fut_name)
                .push_bind(&item.warehouse)
                .push_bind(&item.wh_id)
                .push_bind(item.pre_vol)
                .push_bind(item.vol)
                .push_bind(item.vol_chg)
                .push_bind(&item.area)
                .push_bind(&item.year)
                .push_bind(&item.grade)
                .push_bind(&item.brand)
                .push_bind(&item.place)
                .push_bind(item.pd)
                .push_bind(&item.is_ct)
                .push_bind(&item.unit)
                .push_bind(item.available_at)
                .push_bind(item.source_published_at)
                .push_bind(&item.raw_payload)
                .push_bind(data_version_id)
                .push_bind(source);
        });
        builder.push(
            " ON CONFLICT (trade_date, symbol, exchange, warehouse) DO UPDATE SET \
              fut_name = EXCLUDED.fut_name, \
              wh_id = EXCLUDED.wh_id, \
              pre_vol = EXCLUDED.pre_vol, \
              vol = EXCLUDED.vol, \
              vol_chg = EXCLUDED.vol_chg, \
              area = EXCLUDED.area, \
              year = EXCLUDED.year, \
              grade = EXCLUDED.grade, \
              brand = EXCLUDED.brand, \
              place = EXCLUDED.place, \
              pd = EXCLUDED.pd, \
              is_ct = EXCLUDED.is_ct, \
              unit = EXCLUDED.unit, \
              available_at = EXCLUDED.available_at, \
              source_published_at = EXCLUDED.source_published_at, \
              raw_payload = EXCLUDED.raw_payload, \
              data_version_id = EXCLUDED.data_version_id, \
              source = EXCLUDED.source, \
              updated_at = now()",
        );
        let result = builder.build().execute(pool).await?;
        saved += result.rows_affected() as usize;
    }
    info!(
        "批量 upsert {} 条期货仓单数据，去重 {} 条",
        saved,
        rows.len().saturating_sub(unique_rows.len())
    );
    Ok(saved)
}

pub async fn upsert_futures_holding_rank_batch(
    pool: &PgPool,
    rows: &[MarketFuturesHoldingRank],
    data_version_id: &str,
    source: &str,
) -> Result<usize, sqlx::Error> {
    if rows.is_empty() {
        return Ok(0);
    }

    let mut seen = HashSet::new();
    let unique_rows: Vec<MarketFuturesHoldingRank> = rows
        .iter()
        .filter(|row| {
            seen.insert((
                row.trade_date,
                row.symbol.clone(),
                row.exchange.clone(),
                row.broker.clone(),
            ))
        })
        .cloned()
        .collect();
    let mut saved = 0usize;
    for chunk in unique_rows.chunks(1_000) {
        let mut builder = sqlx::QueryBuilder::<sqlx::Postgres>::new(
            "INSERT INTO market_futures_holding_rank \
             (trade_date, symbol, exchange, broker, vol, vol_chg, long_hld, long_chg, short_hld, \
              short_chg, available_at, source_published_at, raw_payload, data_version_id, source) ",
        );
        builder.push_values(chunk, |mut row_builder, item| {
            row_builder
                .push_bind(item.trade_date)
                .push_bind(&item.symbol)
                .push_bind(&item.exchange)
                .push_bind(&item.broker)
                .push_bind(item.vol)
                .push_bind(item.vol_chg)
                .push_bind(item.long_hld)
                .push_bind(item.long_chg)
                .push_bind(item.short_hld)
                .push_bind(item.short_chg)
                .push_bind(item.available_at)
                .push_bind(item.source_published_at)
                .push_bind(&item.raw_payload)
                .push_bind(data_version_id)
                .push_bind(source);
        });
        builder.push(
            " ON CONFLICT (trade_date, symbol, exchange, broker) DO UPDATE SET \
              vol = EXCLUDED.vol, \
              vol_chg = EXCLUDED.vol_chg, \
              long_hld = EXCLUDED.long_hld, \
              long_chg = EXCLUDED.long_chg, \
              short_hld = EXCLUDED.short_hld, \
              short_chg = EXCLUDED.short_chg, \
              available_at = EXCLUDED.available_at, \
              source_published_at = EXCLUDED.source_published_at, \
              raw_payload = EXCLUDED.raw_payload, \
              data_version_id = EXCLUDED.data_version_id, \
              source = EXCLUDED.source, \
              updated_at = now()",
        );
        let result = builder.build().execute(pool).await?;
        saved += result.rows_affected() as usize;
    }
    info!(
        "批量 upsert {} 条期货持仓排名数据，去重 {} 条",
        saved,
        rows.len().saturating_sub(unique_rows.len())
    );
    Ok(saved)
}

// ─── market_stock_industry_membership_pit ───────────────────────

pub async fn upsert_industry_membership_batch(
    pool: &PgPool,
    rows: &[MarketStockIndustryMembershipPit],
    data_version_id: &str,
    source: &str,
) -> Result<usize, sqlx::Error> {
    if rows.is_empty() {
        return Ok(0);
    }

    let mut seen = HashSet::new();
    let unique_rows: Vec<MarketStockIndustryMembershipPit> = rows
        .iter()
        .filter(|row| {
            seen.insert((
                row.classification_source.clone(),
                row.index_code.clone(),
                row.symbol.clone(),
                row.in_date,
            ))
        })
        .cloned()
        .collect();
    let mut saved = 0usize;
    for chunk in unique_rows.chunks(1_000) {
        let mut builder = sqlx::QueryBuilder::<sqlx::Postgres>::new(
            "INSERT INTO market_stock_industry_membership_pit \
             (classification_source, industry_level, index_code, index_name, industry_code, \
              industry_name, parent_code, symbol, symbol_name, in_date, out_date, available_at, \
              exit_available_at, is_new, raw_payload, source, data_version_id) ",
        );
        builder.push_values(chunk, |mut row_builder, item| {
            row_builder
                .push_bind(&item.classification_source)
                .push_bind(&item.industry_level)
                .push_bind(&item.index_code)
                .push_bind(&item.index_name)
                .push_bind(&item.industry_code)
                .push_bind(&item.industry_name)
                .push_bind(&item.parent_code)
                .push_bind(&item.symbol)
                .push_bind(&item.symbol_name)
                .push_bind(item.in_date)
                .push_bind(item.out_date)
                .push_bind(item.available_at)
                .push_bind(item.exit_available_at)
                .push_bind(&item.is_new)
                .push_bind(&item.raw_payload)
                .push_bind(source)
                .push_bind(data_version_id);
        });
        builder.push(
            " ON CONFLICT (classification_source, index_code, symbol, in_date) DO UPDATE SET \
              industry_level = EXCLUDED.industry_level, \
              index_name = EXCLUDED.index_name, \
              industry_code = EXCLUDED.industry_code, \
              industry_name = EXCLUDED.industry_name, \
              parent_code = EXCLUDED.parent_code, \
              symbol_name = EXCLUDED.symbol_name, \
              out_date = EXCLUDED.out_date, \
              available_at = EXCLUDED.available_at, \
              exit_available_at = EXCLUDED.exit_available_at, \
              is_new = EXCLUDED.is_new, \
              raw_payload = EXCLUDED.raw_payload, \
              source = EXCLUDED.source, \
              data_version_id = EXCLUDED.data_version_id, \
              updated_at = now()",
        );
        let result = builder.build().execute(pool).await?;
        saved += result.rows_affected() as usize;
    }
    info!(
        "批量 upsert {} 条行业成员 PIT 数据，去重 {} 条",
        saved,
        rows.len().saturating_sub(unique_rows.len())
    );
    Ok(saved)
}

// ─── market_index_daily_bar ──────────────────────────────────────

pub async fn upsert_index_daily_bar(
    pool: &PgPool,
    bar: &MarketIndexDailyBar,
    data_version_id: &str,
    source: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"INSERT INTO market_index_daily_bar
             (symbol, trade_date, open, high, low, close, pre_close, pct_change,
              volume, amount, source, data_version_id)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
           ON CONFLICT (symbol, trade_date) DO UPDATE SET
             open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
             close = EXCLUDED.close, pre_close = EXCLUDED.pre_close,
             pct_change = EXCLUDED.pct_change,
             volume = EXCLUDED.volume, amount = EXCLUDED.amount,
             source = EXCLUDED.source, data_version_id = EXCLUDED.data_version_id"#,
    )
    .bind(&bar.symbol)
    .bind(bar.trade_date)
    .bind(bar.open)
    .bind(bar.high)
    .bind(bar.low)
    .bind(bar.close)
    .bind(bar.pre_close)
    .bind(bar.change_pct)
    .bind(bar.volume)
    .bind(bar.amount)
    .bind(source)
    .bind(data_version_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn upsert_index_daily_bars_batch(
    pool: &PgPool,
    bars: &[MarketIndexDailyBar],
    data_version_id: &str,
    source: &str,
) -> Result<usize, sqlx::Error> {
    let mut count = 0;
    for bar in bars {
        upsert_index_daily_bar(pool, bar, data_version_id, source).await?;
        count += 1;
    }
    info!("批量 upsert {} 条指数日线", count);
    Ok(count)
}

// ─── market_trade_calendar ───────────────────────────────────────

pub async fn upsert_trade_calendar(
    pool: &PgPool,
    cal: &MarketTradeCalendar,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"INSERT INTO market_trade_calendar (exchange, trade_date, is_open, pre_trade_date)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (exchange, trade_date) DO UPDATE SET
             is_open = EXCLUDED.is_open,
             pre_trade_date = EXCLUDED.pre_trade_date"#,
    )
    .bind(&cal.exchange)
    .bind(cal.trade_date)
    .bind(cal.is_open)
    .bind(cal.pre_trade_date)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn bulk_upsert_calendars(
    pool: &PgPool,
    cals: &[MarketTradeCalendar],
) -> Result<usize, sqlx::Error> {
    let mut count = 0;
    for cal in cals {
        upsert_trade_calendar(pool, cal).await?;
        count += 1;
    }
    info!("批量 upsert {} 条交易日历", count);
    Ok(count)
}

// ─── market_adjustment_factor ────────────────────────────────────

pub async fn upsert_adj_factors_batch(
    pool: &PgPool,
    factors: &[MarketAdjustmentFactor],
    data_version_id: &str,
    source: &str,
) -> Result<usize, sqlx::Error> {
    let mut count = 0;
    for f in factors {
        sqlx::query(
            r#"INSERT INTO market_adjustment_factor (symbol, trade_date, adj_factor, source, data_version_id)
               VALUES ($1, $2, $3, $4, $5)
               ON CONFLICT (symbol, trade_date) DO UPDATE SET
                 adj_factor = EXCLUDED.adj_factor,
                 source = EXCLUDED.source,
                 data_version_id = EXCLUDED.data_version_id"#,
        )
        .bind(&f.symbol)
        .bind(f.trade_date)
        .bind(f.adj_factor)
        .bind(source)
        .bind(data_version_id)
        .execute(pool)
        .await?;
        count += 1;
    }
    info!("批量 upsert {} 条复权因子", count);
    Ok(count)
}

// ─── data_version ─────────────────────────────────────────────────

/// 创建数据版本记录（同步开始前调用）
pub async fn create_data_version(
    pool: &PgPool,
    dv_id: &str,
    name: &str,
    source: &str,
    tables: &[&str],
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"INSERT INTO data_version (data_version_id, name, source, start_date, end_date, tables, snapshot_hash)
           VALUES ($1, $2, $3, $4, $5, $6, '')
           ON CONFLICT (data_version_id) DO NOTHING"#,
    )
    .bind(dv_id)
    .bind(name)
    .bind(source)
    .bind(start_date)
    .bind(end_date)
    .bind(tables)
    .execute(pool)
    .await?;
    Ok(())
}

// ─── data_sync_task ──────────────────────────────────────────────

pub async fn create_sync_task(
    pool: &PgPool,
    task_id: &str,
    task_type: &str,
    status: &str,
) -> Result<(), sqlx::Error> {
    create_sync_task_with_context(
        pool, task_id, task_type, "tushare", None, None, None, status, None,
    )
    .await
}

pub async fn create_sync_task_with_context(
    pool: &PgPool,
    task_id: &str,
    task_type: &str,
    source: &str,
    symbols: Option<&[String]>,
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
    status: &str,
    retry_of_task_id: Option<&str>,
) -> Result<(), sqlx::Error> {
    let symbols_vec = symbols.map(|items| items.to_vec());
    sqlx::query(
        r#"INSERT INTO data_sync_task
           (task_id, task_type, source, symbols, start_date, end_date, status,
            retry_of_task_id, started_at, last_heartbeat_at, heartbeat_timeout_seconds)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8,
            CASE WHEN $7 = 'running' THEN now() ELSE NULL END,
            CASE WHEN $7 = 'running' THEN now() ELSE NULL END,
            600)
           ON CONFLICT (task_id) DO UPDATE SET
             task_type = EXCLUDED.task_type,
             source = EXCLUDED.source,
             symbols = EXCLUDED.symbols,
             start_date = EXCLUDED.start_date,
             end_date = EXCLUDED.end_date,
             status = EXCLUDED.status,
             retry_of_task_id = EXCLUDED.retry_of_task_id,
             started_at = COALESCE(data_sync_task.started_at, EXCLUDED.started_at),
             last_heartbeat_at = COALESCE(EXCLUDED.last_heartbeat_at, data_sync_task.last_heartbeat_at),
             heartbeat_timeout_seconds = COALESCE(data_sync_task.heartbeat_timeout_seconds, EXCLUDED.heartbeat_timeout_seconds)"#,
    )
    .bind(task_id)
    .bind(task_type)
    .bind(source)
    .bind(symbols_vec.as_deref())
    .bind(start_date)
    .bind(end_date)
    .bind(status)
    .bind(retry_of_task_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub fn update_sync_task_sql() -> &'static str {
    r#"UPDATE data_sync_task
           SET status = $2, total_count = $3, success_count = $4, failed_count = $5,
               error_message = CASE WHEN $2 = 'completed' THEN NULL ELSE error_message END,
               progress = CASE WHEN $3 > 0 THEN ($4 * 100 / $3) ELSE 0 END,
               last_heartbeat_at = now(),
               started_at = COALESCE(started_at, now()),
               completed_at = CASE WHEN $2 IN ('completed','partial','failed') THEN now() ELSE completed_at END
           WHERE task_id = $1"#
}

pub async fn update_sync_task(
    pool: &PgPool,
    task_id: &str,
    status: &str,
    total: i32,
    success: i32,
    failed: i32,
) -> Result<(), sqlx::Error> {
    sqlx::query(update_sync_task_sql())
        .bind(task_id)
        .bind(status)
        .bind(total)
        .bind(success)
        .bind(failed)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn update_sync_task_with_error(
    pool: &PgPool,
    task_id: &str,
    status: &str,
    total: i32,
    success: i32,
    failed: i32,
    error_message: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"UPDATE data_sync_task
           SET status = $2, total_count = $3, success_count = $4, failed_count = $5,
               error_message = $6,
               progress = CASE WHEN $3 > 0 THEN ($4 * 100 / $3) ELSE 0 END,
               last_heartbeat_at = now(),
               started_at = COALESCE(started_at, now()),
               completed_at = CASE WHEN $2 IN ('completed','partial','failed') THEN now() ELSE completed_at END
           WHERE task_id = $1"#,
    )
    .bind(task_id)
    .bind(status)
    .bind(total)
    .bind(success)
    .bind(failed)
    .bind(error_message)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn heartbeat_sync_task(
    pool: &PgPool,
    task_id: &str,
    total: i32,
    success: i32,
    failed: i32,
    progress: i32,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"UPDATE data_sync_task
           SET status = 'running',
               total_count = $2,
               success_count = $3,
               failed_count = $4,
               progress = LEAST(GREATEST($5, 0), 99),
               last_heartbeat_at = now(),
               started_at = COALESCE(started_at, now())
           WHERE task_id = $1
             AND status = 'running'"#,
    )
    .bind(task_id)
    .bind(total)
    .bind(success)
    .bind(failed)
    .bind(progress)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn fail_sync_task(
    pool: &PgPool,
    task_id: &str,
    error_message: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"UPDATE data_sync_task
           SET status = 'failed', failed_count = GREATEST(failed_count, 1),
               error_message = $2, progress = 0, last_heartbeat_at = now(), completed_at = now()
           WHERE task_id = $1"#,
    )
    .bind(task_id)
    .bind(error_message)
    .execute(pool)
    .await?;
    Ok(())
}

pub fn sync_attempt_upsert_sql() -> &'static str {
    r#"INSERT INTO data_sync_attempt
       (source, symbol, start_date, end_date, task_id, status, row_count, error_message,
        attempted_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now(), now())
       ON CONFLICT (source, symbol, start_date, end_date) DO UPDATE SET
         task_id = EXCLUDED.task_id,
         status = EXCLUDED.status,
         row_count = EXCLUDED.row_count,
         error_message = EXCLUDED.error_message,
         attempted_at = EXCLUDED.attempted_at,
         updated_at = now()"#
}

pub fn sync_attempt_success_filter_sql() -> &'static str {
    r#"SELECT attempt.symbol
       FROM data_sync_attempt attempt
       WHERE attempt.start_date <= $1
         AND attempt.end_date >= $2
         AND attempt.source = $3
         AND attempt.status = 'completed'"#
}

pub async fn upsert_sync_attempt(
    pool: &PgPool,
    source: &str,
    symbol: &str,
    start_date: NaiveDate,
    end_date: NaiveDate,
    task_id: &str,
    status: &str,
    row_count: i64,
    error_message: Option<&str>,
) -> Result<(), sqlx::Error> {
    sqlx::query(sync_attempt_upsert_sql())
        .bind(source)
        .bind(symbol)
        .bind(start_date)
        .bind(end_date)
        .bind(task_id)
        .bind(status)
        .bind(row_count)
        .bind(error_message)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn list_successfully_attempted_symbols(
    pool: &PgPool,
    source: &str,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<HashSet<String>, sqlx::Error> {
    let symbols: Vec<String> = sqlx::query_scalar(sync_attempt_success_filter_sql())
        .bind(start_date)
        .bind(end_date)
        .bind(source)
        .fetch_all(pool)
        .await?;
    Ok(symbols.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sync_attempt_upsert_sql_records_zero_row_successes() {
        let sql = sync_attempt_upsert_sql();

        assert!(sql.contains("INSERT INTO data_sync_attempt"));
        assert!(sql.contains("row_count"));
        assert!(sql.contains("status"));
        assert!(sql.contains("ON CONFLICT (source, symbol, start_date, end_date)"));
        assert!(sql.contains("task_id = EXCLUDED.task_id"));
    }

    #[test]
    fn update_sync_task_sql_clears_stale_error_on_completed_status() {
        let sql = update_sync_task_sql();

        assert!(sql.contains("error_message = CASE WHEN $2 = 'completed' THEN NULL"));
        assert!(sql.contains("status = $2"));
        assert!(sql.contains("completed_at = CASE WHEN $2 IN ('completed','partial','failed')"));
    }

    #[test]
    fn sync_attempt_query_filters_successful_attempts_by_source() {
        let sql = sync_attempt_success_filter_sql();

        assert!(sql.contains("data_sync_attempt attempt"));
        assert!(sql.contains("attempt.source = $3"));
        assert!(sql.contains("attempt.status = 'completed'"));
        assert!(sql.contains("attempt.start_date <= $1"));
        assert!(sql.contains("attempt.end_date >= $2"));
    }
}
