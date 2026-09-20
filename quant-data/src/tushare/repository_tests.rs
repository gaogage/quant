//! repository.rs 的真实 PG 直测（本机 quant 库）。
//!
//! 惯例与 quant-factor 测试一致：非 ignored、秒级、测试键一律 zzz 语义。
//! 并行安全策略：**每个测试函数独占一组 symbol 键**（互不重叠），
//! task_id/dv_id 用 zzz-test- 前缀；前置 + 结尾精确键 DELETE，与真实数据零碰撞。

use chrono::NaiveDate;
use rust_decimal::Decimal;
use sqlx::PgPool;

use crate::model::entities::{
    MarketAdjustmentFactor, MarketFundNav, MarketStock, MarketStockDailyBar, MarketStockDailyBasic,
    MarketStockMoneyflow, MarketTradeCalendar,
};
use crate::repository;

use super::local_pool;

// 每测试独占键（symbol 列宽 20，全部 ≤20 字符）
const ZZZ_STOCK: &str = "ZZZTSTA.SH"; // upsert_stock 幂等测试
const ZZZ_BATCH1: &str = "ZZZTSTB1.SH"; // stocks_batch 测试
const ZZZ_BATCH2: &str = "ZZZTSTB2.SH";
const ZZZ_BAR: &str = "ZZZTSTC.SH"; // daily_bar 测试
const ZZZ_NAV: &str = "ZZZTSTD.SH"; // fund_nav 测试
const ZZZ_BASIC: &str = "ZZZTSTE.SH"; // daily_basic 测试
const ZZZ_MF: &str = "ZZZTSTF.SH"; // moneyflow 测试
const ZZZ_ADJ: &str = "ZZZTSTG.SH"; // adj_factor 测试
const ZZZ_ATTEMPT_OK: &str = "ZZZTSTH.SH"; // sync_attempt 成功键
const ZZZ_ATTEMPT_FAIL: &str = "ZZZTSTI.SH"; // sync_attempt 失败键
                                             // 每测试独占 dv（FK 前置用）——共用一个 dv 时，并行测试的全量清理
                                             // 会删掉他人正在写入依赖的 data_version 行，触发 FK 违反
const ZZZ_DV_BAR: &str = "zzz-test-dv-bar";
const ZZZ_DV_BASIC: &str = "zzz-test-dv-basic";
const ZZZ_DV_MF: &str = "zzz-test-dv-mf";
const ZZZ_DV_ADJ: &str = "zzz-test-dv-adj";

fn d(y: i32, m: u32, day: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, day).unwrap()
}

fn dec(x: f64) -> Decimal {
    Decimal::from_f64_retain(x).expect("f64→Decimal")
}

/// 浮点容差断言：落库 numeric 列按列 scale 舍入，与 from_f64_retain
/// 保留的二进制近似（如 1.149999...91）数值不等，精确 == 会假失败。
fn assert_dec_close(actual: Decimal, expected: f64, ctx: &str) {
    let diff = (actual - dec(expected)).abs();
    assert!(
        diff < dec(1e-9),
        "{}: 实际 {} 与期望 {} 差 {}",
        ctx,
        actual,
        expected,
        diff
    );
}

/// 清理指定 symbol 在六张表中的行（只动传入键，并行安全：
/// 全量清理会删掉并行中其它测试正在断言/写入依赖的行）
async fn cleanup_syms(pool: &PgPool, syms: &[&str]) {
    for sym in syms {
        for table in [
            "market_stock",
            "market_stock_daily_bar",
            "market_fund_nav",
            "market_stock_daily_basic",
            "market_stock_moneyflow",
            "market_adjustment_factor",
        ] {
            let _ = sqlx::query(&format!("DELETE FROM {} WHERE symbol = $1", table))
                .bind(sym)
                .execute(pool)
                .await;
        }
    }
}

/// 清理本文件写入的两条日历键（2026-01-05/06；sync_tests 用 02 月键）
async fn cleanup_cal(pool: &PgPool) {
    let _ = sqlx::query(
        "DELETE FROM market_trade_calendar WHERE exchange = 'ZZZ' \
         AND trade_date IN ('2026-01-05', '2026-01-06')",
    )
    .execute(pool)
    .await;
}

/// 清理指定任务行
async fn cleanup_task(pool: &PgPool, task_id: &str) {
    let _ = sqlx::query("DELETE FROM data_sync_task WHERE task_id = $1")
        .bind(task_id)
        .execute(pool)
        .await;
}

/// 清理 attempt 测试键（仅 sync_attempt 测试调用，按 source 前缀）
async fn cleanup_attempts(pool: &PgPool) {
    let _ = sqlx::query("DELETE FROM data_sync_attempt WHERE source LIKE 'zzz-test-%'")
        .execute(pool)
        .await;
}

/// 清理指定数据版本（下游表 FK 均 ON DELETE SET NULL）
async fn cleanup_dv(pool: &PgPool, dv_id: &str) {
    let _ = sqlx::query("DELETE FROM data_version WHERE data_version_id = $1")
        .bind(dv_id)
        .execute(pool)
        .await;
}

fn stock(symbol: &str, name: &str, exchange: &str) -> MarketStock {
    MarketStock {
        symbol: symbol.to_string(),
        name: name.to_string(),
        exchange: exchange.to_string(),
        market: Some("主板".to_string()),
        industry: Some("银行".to_string()),
        list_status: "L".to_string(),
        list_date: Some(d(2026, 1, 5)),
        delist_date: None,
        is_st: false,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    }
}

// ─── market_stock 族 ─────────────────────────────────────────────

#[tokio::test]
async fn upsert_stock_is_idempotent_and_second_write_wins() {
    let pool = local_pool().await;
    cleanup_syms(&pool, &[ZZZ_STOCK]).await;

    repository::upsert_stock(&pool, &stock(ZZZ_STOCK, "测试银行", "SSE"))
        .await
        .expect("首次 upsert");

    // 幂等重跑：同 symbol 改名 + 改状态 → 仍是 1 行，字段取第二次值
    let mut second = stock(ZZZ_STOCK, "测试银行改", "SSE");
    second.list_status = "D".to_string();
    repository::upsert_stock(&pool, &second)
        .await
        .expect("二次 upsert");

    let row: (String, String, Option<NaiveDate>) =
        sqlx::query_as("SELECT name, list_status, list_date FROM market_stock WHERE symbol = $1")
            .bind(ZZZ_STOCK)
            .fetch_one(&pool)
            .await
            .expect("查询 zzz 行");
    assert_eq!(row.0, "测试银行改", "二次写入应胜出");
    assert_eq!(row.1, "D");
    assert_eq!(row.2, Some(d(2026, 1, 5)));

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM market_stock WHERE symbol = $1")
        .bind(ZZZ_STOCK)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1, "幂等：重复 upsert 不增行");

    cleanup_syms(&pool, &[ZZZ_STOCK]).await;
}

#[tokio::test]
async fn upsert_stocks_batch_counts_and_feeds_symbol_listing() {
    let pool = local_pool().await;
    cleanup_syms(&pool, &[ZZZ_BATCH1, ZZZ_BATCH2]).await;

    let n = repository::upsert_stocks_batch(
        &pool,
        &[
            stock(ZZZ_BATCH1, "测试甲", "SSE"),
            stock(ZZZ_BATCH2, "测试乙", "SZSE"),
        ],
    )
    .await
    .expect("批量 upsert");
    assert_eq!(n, 2, "返回值 = 处理行数");

    // list_listed_stock_symbols 只返回 list_status='L'，按 symbol 排序
    let listed = repository::list_listed_stock_symbols(&pool)
        .await
        .expect("list");
    assert!(
        listed.contains(&ZZZ_BATCH1.to_string()),
        "L 状态应出现在列表"
    );
    assert!(
        listed.contains(&ZZZ_BATCH2.to_string()),
        "两个 zzz 键都应在列"
    );

    // count_stocks 全表计数：只断言包含性（避免与真实数据/并发写入耦合）
    let total = repository::count_stocks(&pool).await.expect("count");
    assert!(total >= 2, "至少包含两条 zzz 测试行，实际 {}", total);

    cleanup_syms(&pool, &[ZZZ_BATCH1, ZZZ_BATCH2]).await;
}

// ─── market_stock_daily_bar 族 ───────────────────────────────────

#[tokio::test]
async fn daily_bar_upsert_batch_and_date_range_roundtrip() {
    let pool = local_pool().await;
    cleanup_syms(&pool, &[ZZZ_BAR]).await;
    cleanup_dv(&pool, ZZZ_DV_BAR).await;

    // FK 前置：daily_bar.data_version_id → data_version
    repository::create_data_version(
        &pool,
        ZZZ_DV_BAR,
        "zzz test dv",
        "tushare",
        &["market_stock_daily_bar"],
        d(2026, 1, 1),
        d(2026, 1, 31),
    )
    .await
    .expect("create dv");

    let bar = |day: u32, close: f64| MarketStockDailyBar {
        symbol: ZZZ_BAR.to_string(),
        trade_date: d(2026, 1, day),
        open: Decimal::from_f64_retain(10.0).expect("f64→Decimal"),
        high: Decimal::from_f64_retain(close + 0.5).expect("f64→Decimal"),
        low: Decimal::from_f64_retain(9.5).expect("f64→Decimal"),
        close: Decimal::from_f64_retain(close).expect("f64→Decimal"),
        pre_close: Some(Decimal::from_f64_retain(close - 0.1).expect("f64→Decimal")),
        change_pct: Some(Decimal::from_f64_retain(1.23).expect("f64→Decimal")),
        volume: Decimal::from_f64_retain(100_000.0).expect("f64→Decimal"),
        amount: Decimal::from_f64_retain(1_050_000.0).expect("f64→Decimal"),
    };

    // 单条 + 批量两条：1/5 与 1/9（1/5 在批量中二次覆盖）
    repository::upsert_daily_bar(&pool, &bar(5, 10.0), ZZZ_DV_BAR, "tushare")
        .await
        .expect("单条 upsert");
    let n = repository::upsert_daily_bars_batch(
        &pool,
        &[bar(9, 11.0), bar(5, 10.5)],
        ZZZ_DV_BAR,
        "tushare",
    )
    .await
    .expect("批量 upsert");
    assert_eq!(n, 2, "批量返回处理行数");

    // 幂等：1/5 被批量以 10.5 覆盖 → COUNT 仍 2 行、close=10.5
    let (count, close_5): (i64, Option<Decimal>) = sqlx::query_as(
        "SELECT COUNT(*), MAX(close) FILTER (WHERE trade_date = '2026-01-05') \
         FROM market_stock_daily_bar WHERE symbol = $1",
    )
    .bind(ZZZ_BAR)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, 2);
    assert_eq!(
        close_5,
        Some(Decimal::from_f64_retain(10.5).expect("f64→Decimal")),
        "幂等覆盖取最新值"
    );

    // get_daily_date_range：min=1/5, max=1/9；补 1/7 不改变两端
    let (lo, hi) = repository::get_daily_date_range(&pool, ZZZ_BAR)
        .await
        .expect("date range");
    assert_eq!(lo, Some(d(2026, 1, 5)));
    assert_eq!(hi, Some(d(2026, 1, 9)));

    repository::upsert_daily_bar(&pool, &bar(7, 10.8), ZZZ_DV_BAR, "tushare")
        .await
        .expect("补中间日");
    let (lo, hi) = repository::get_daily_date_range(&pool, ZZZ_BAR)
        .await
        .expect("date range 2");
    assert_eq!(lo, Some(d(2026, 1, 5)), "min 不变");
    assert_eq!(hi, Some(d(2026, 1, 9)), "max 不变");

    // 无数据 symbol → (None, None)
    let (lo, hi) = repository::get_daily_date_range(&pool, "ZZZNOEXIST.SH")
        .await
        .expect("空 range");
    assert_eq!((lo, hi), (None, None));

    cleanup_syms(&pool, &[ZZZ_BAR]).await;
    cleanup_dv(&pool, ZZZ_DV_BAR).await;
}

// ─── market_fund_nav 族 ──────────────────────────────────────────

#[tokio::test]
async fn fund_nav_upsert_and_latest_nav_picks_max_date() {
    let pool = local_pool().await;
    cleanup_syms(&pool, &[ZZZ_NAV]).await;

    let navs = vec![
        MarketFundNav {
            symbol: ZZZ_NAV.to_string(),
            nav_date: d(2026, 1, 5),
            ann_date: Some(d(2026, 1, 6)),
            unit_nav: Decimal::from_f64_retain(1.0).expect("f64→Decimal"),
            accum_nav: Some(Decimal::from_f64_retain(1.0).expect("f64→Decimal")),
            adj_nav: None,
        },
        MarketFundNav {
            symbol: ZZZ_NAV.to_string(),
            nav_date: d(2026, 1, 8),
            ann_date: Some(d(2026, 1, 9)),
            unit_nav: Decimal::from_f64_retain(1.5).expect("f64→Decimal"),
            accum_nav: Some(Decimal::from_f64_retain(1.5).expect("f64→Decimal")),
            adj_nav: None,
        },
    ];
    let n = repository::upsert_fund_navs(&pool, &navs)
        .await
        .expect("upsert navs");
    assert_eq!(n, 2);

    // latest_fund_navs：DISTINCT ON (symbol) ORDER BY nav_date DESC → 1/8 的 1.5
    let latest = repository::latest_fund_navs(&pool, &[ZZZ_NAV.to_string()])
        .await
        .expect("latest");
    assert_eq!(latest.len(), 1, "每 symbol 只留一条：{:?}", latest);
    let (date, nav) = latest.get(ZZZ_NAV).expect("应含 zzz 键");
    assert_eq!(*date, d(2026, 1, 8));
    assert_eq!(*nav, Decimal::from_f64_retain(1.5).expect("f64→Decimal"));

    // 查询集合含不存在的 symbol → 只返回有数据的
    let latest =
        repository::latest_fund_navs(&pool, &[ZZZ_NAV.to_string(), "ZZZNOEXIST.SH".to_string()])
            .await
            .expect("latest 2");
    assert_eq!(latest.len(), 1);

    cleanup_syms(&pool, &[ZZZ_NAV]).await;
}

// ─── market_stock_daily_basic 族 ─────────────────────────────────

#[tokio::test]
async fn daily_basic_single_and_batch_upsert_roundtrip() {
    let pool = local_pool().await;
    cleanup_syms(&pool, &[ZZZ_BASIC]).await;
    cleanup_dv(&pool, ZZZ_DV_BASIC).await;

    // FK 前置：daily_basic.data_version_id → data_version
    repository::create_data_version(
        &pool,
        ZZZ_DV_BASIC,
        "zzz test dv",
        "tushare",
        &["market_stock_daily_basic"],
        d(2026, 1, 1),
        d(2026, 1, 31),
    )
    .await
    .expect("create dv");

    let row = |day: u32, pe: f64| MarketStockDailyBasic {
        symbol: ZZZ_BASIC.to_string(),
        trade_date: d(2026, 1, day),
        pe_ttm: Some(Decimal::from_f64_retain(pe).expect("f64→Decimal")),
        pb: Some(Decimal::from_f64_retain(0.72).expect("f64→Decimal")),
        ps_ttm: Some(Decimal::from_f64_retain(1.15).expect("f64→Decimal")),
        dv_ttm: Some(Decimal::from_f64_retain(4.2).expect("f64→Decimal")),
        total_share: Some(Decimal::from_f64_retain(1_940_591.819_8).expect("f64→Decimal")),
        float_share: Some(Decimal::from_f64_retain(1_940_554.467_5).expect("f64→Decimal")),
        free_share: Some(Decimal::from_f64_retain(1_800_000.25).expect("f64→Decimal")),
        total_mv: Some(Decimal::from_f64_retain(123_456.7).expect("f64→Decimal")),
        circ_mv: Some(Decimal::from_f64_retain(98_765.4).expect("f64→Decimal")),
    };

    repository::upsert_daily_basic(&pool, &row(5, 6.8), ZZZ_DV_BASIC, "tushare")
        .await
        .expect("单条 daily_basic");

    // 批量（QueryBuilder 路径）：1/5 覆盖 + 1/6 新增 → rows_affected=2
    let n = repository::upsert_daily_basic_batch(
        &pool,
        &[row(5, 7.0), row(6, 9.0)],
        ZZZ_DV_BASIC,
        "tushare",
    )
    .await
    .expect("批量 daily_basic");
    assert_eq!(n, 2, "PG 对 upsert 每行都计 rows_affected");

    let (count, pe5): (i64, Option<Decimal>) = sqlx::query_as(
        "SELECT COUNT(*), MAX(pe_ttm) FILTER (WHERE trade_date = '2026-01-05') \
         FROM market_stock_daily_basic WHERE symbol = $1",
    )
    .bind(ZZZ_BASIC)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, 2);
    assert_eq!(
        pe5,
        Some(Decimal::from_f64_retain(7.0).expect("f64→Decimal")),
        "覆盖写入取最新"
    );

    // 空数组短路分支：返回 0 且不发 SQL
    let n = repository::upsert_daily_basic_batch(&pool, &[], ZZZ_DV_BASIC, "tushare")
        .await
        .expect("空批量");
    assert_eq!(n, 0);

    cleanup_syms(&pool, &[ZZZ_BASIC]).await;
    cleanup_dv(&pool, ZZZ_DV_BASIC).await;
}

// ─── market_stock_moneyflow 族（QueryBuilder 大绑定）──────────────

#[tokio::test]
async fn moneyflow_batch_upsert_roundtrip() {
    let pool = local_pool().await;
    cleanup_syms(&pool, &[ZZZ_MF]).await;
    cleanup_dv(&pool, ZZZ_DV_MF).await;

    // FK 前置：moneyflow.data_version_id → data_version
    repository::create_data_version(
        &pool,
        ZZZ_DV_MF,
        "zzz test dv",
        "tushare",
        &["market_stock_moneyflow"],
        d(2026, 1, 1),
        d(2026, 1, 31),
    )
    .await
    .expect("create dv");

    let row = MarketStockMoneyflow {
        symbol: ZZZ_MF.to_string(),
        trade_date: d(2026, 1, 5),
        buy_sm_vol: Some(Decimal::from_f64_retain(100.0).expect("f64→Decimal")),
        buy_sm_amount: Some(Decimal::from_f64_retain(1_000.0).expect("f64→Decimal")),
        sell_sm_vol: None, // 部分字段 NULL 走 Option 绑定分支
        sell_sm_amount: None,
        buy_md_vol: Some(Decimal::from_f64_retain(200.0).expect("f64→Decimal")),
        buy_md_amount: Some(Decimal::from_f64_retain(2_000.0).expect("f64→Decimal")),
        sell_md_vol: None,
        sell_md_amount: None,
        buy_lg_vol: Some(Decimal::from_f64_retain(300.0).expect("f64→Decimal")),
        buy_lg_amount: Some(Decimal::from_f64_retain(3_000.0).expect("f64→Decimal")),
        sell_lg_vol: None,
        sell_lg_amount: None,
        buy_elg_vol: Some(Decimal::from_f64_retain(400.0).expect("f64→Decimal")),
        buy_elg_amount: Some(Decimal::from_f64_retain(4_000.0).expect("f64→Decimal")),
        sell_elg_vol: None,
        sell_elg_amount: None,
        net_mf_vol: Some(Decimal::from_f64_retain(500.0).expect("f64→Decimal")),
        net_mf_amount: Some(Decimal::from_f64_retain(5_000.0).expect("f64→Decimal")),
    };

    let n = repository::upsert_moneyflow_batch(&pool, &[row], ZZZ_DV_MF, "tushare")
        .await
        .expect("批量 moneyflow");
    assert_eq!(n, 1);

    let (net_amount, sell_sm): (Option<Decimal>, Option<Decimal>) = sqlx::query_as(
        "SELECT net_mf_amount, sell_sm_amount FROM market_stock_moneyflow \
         WHERE symbol = $1 AND trade_date = '2026-01-05'",
    )
    .bind(ZZZ_MF)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        net_amount,
        Some(Decimal::from_f64_retain(5_000.0).expect("f64→Decimal"))
    );
    assert_eq!(sell_sm, None, "未提供的字段应保持 NULL");

    // 空数组短路
    let n = repository::upsert_moneyflow_batch(&pool, &[], ZZZ_DV_MF, "tushare")
        .await
        .expect("空批量");
    assert_eq!(n, 0);

    cleanup_syms(&pool, &[ZZZ_MF]).await;
    cleanup_dv(&pool, ZZZ_DV_MF).await;
}

// ─── market_trade_calendar 族 ────────────────────────────────────

#[tokio::test]
async fn trade_calendar_upsert_and_bulk_roundtrip() {
    let pool = local_pool().await;
    cleanup_cal(&pool).await;

    repository::upsert_trade_calendar(
        &pool,
        &MarketTradeCalendar {
            exchange: "ZZZ".to_string(),
            trade_date: d(2026, 1, 5),
            is_open: true,
            pre_trade_date: Some(d(2026, 1, 2)),
        },
    )
    .await
    .expect("单条日历");

    let n = repository::bulk_upsert_calendars(
        &pool,
        &[
            // 幂等：1/5 改 pre_trade_date → 覆盖
            MarketTradeCalendar {
                exchange: "ZZZ".to_string(),
                trade_date: d(2026, 1, 5),
                is_open: true,
                pre_trade_date: Some(d(2025, 12, 31)),
            },
            MarketTradeCalendar {
                exchange: "ZZZ".to_string(),
                trade_date: d(2026, 1, 6),
                is_open: false,
                pre_trade_date: None,
            },
        ],
    )
    .await
    .expect("批量日历");
    assert_eq!(n, 2);

    let (count, pre5, open6): (i64, Option<NaiveDate>, Option<i32>) = sqlx::query_as(
        "SELECT COUNT(*), \
                MAX(pre_trade_date) FILTER (WHERE trade_date = '2026-01-05'), \
                MAX(is_open::int) FILTER (WHERE trade_date = '2026-01-06') \
         FROM market_trade_calendar WHERE exchange = 'ZZZ'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, 2, "幂等：1/5 重复 upsert 不增行");
    assert_eq!(pre5, Some(d(2025, 12, 31)), "覆盖取最新");
    assert_eq!(open6, Some(0), "休市日 is_open=false");

    cleanup_cal(&pool).await;
}

// ─── market_adjustment_factor 族 ─────────────────────────────────

#[tokio::test]
async fn adj_factors_batch_upsert_is_idempotent() {
    let pool = local_pool().await;
    cleanup_syms(&pool, &[ZZZ_ADJ]).await;
    cleanup_dv(&pool, ZZZ_DV_ADJ).await;

    repository::create_data_version(
        &pool,
        ZZZ_DV_ADJ,
        "zzz test dv",
        "tushare",
        &["market_adjustment_factor"],
        d(2026, 1, 1),
        d(2026, 1, 31),
    )
    .await
    .expect("create dv");

    let f = |day: u32, factor: f64| MarketAdjustmentFactor {
        symbol: ZZZ_ADJ.to_string(),
        trade_date: d(2026, 1, day),
        adj_factor: Decimal::from_f64_retain(factor).expect("f64→Decimal"),
    };

    let n =
        repository::upsert_adj_factors_batch(&pool, &[f(5, 1.1), f(6, 1.2)], ZZZ_DV_ADJ, "tushare")
            .await
            .expect("首次批量");
    assert_eq!(n, 2);

    // 幂等重跑 + 1/5 值更新
    let n = repository::upsert_adj_factors_batch(
        &pool,
        &[f(5, 1.15), f(6, 1.2)],
        ZZZ_DV_ADJ,
        "tushare",
    )
    .await
    .expect("二次批量");
    assert_eq!(n, 2);

    let (count, f5): (i64, Option<Decimal>) = sqlx::query_as(
        "SELECT COUNT(*), MAX(adj_factor) FILTER (WHERE trade_date = '2026-01-05') \
         FROM market_adjustment_factor WHERE symbol = $1",
    )
    .bind(ZZZ_ADJ)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, 2);
    // 1.15 非二进制精确：落库 numeric(24,10) 舍入值与 from_f64_retain 近似不等 → 容差
    assert_dec_close(f5.expect("1/5 应有值"), 1.15, "adj_factor 覆盖取最新");

    cleanup_syms(&pool, &[ZZZ_ADJ]).await;
    cleanup_dv(&pool, ZZZ_DV_ADJ).await;
}

// ─── data_sync_task 族 ───────────────────────────────────────────

#[tokio::test]
async fn sync_task_lifecycle_running_to_completed_with_progress_math() {
    let pool = local_pool().await;
    cleanup_task(&pool, "zzz-test-task-lifecycle").await;
    let task_id = "zzz-test-task-lifecycle";

    // 创建 running 任务：started_at/heartbeat 立即落值（CASE WHEN 分支）
    repository::create_sync_task_with_context(
        &pool,
        task_id,
        "daily",
        "tushare",
        Some(&[ZZZ_STOCK.to_string()]),
        Some(d(2026, 1, 1)),
        Some(d(2026, 1, 31)),
        "running",
        None,
    )
    .await
    .expect("create task");

    let (status, started_at, heartbeat): (
        String,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<chrono::DateTime<chrono::Utc>>,
    ) = sqlx::query_as(
        "SELECT status, started_at, last_heartbeat_at FROM data_sync_task WHERE task_id = $1",
    )
    .bind(task_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(status, "running");
    assert!(started_at.is_some(), "running 创建即写 started_at");
    assert!(heartbeat.is_some());

    // 先制造一个错误状态，验证 completed 清 error_message 的 CASE 分支
    repository::update_sync_task_with_error(&pool, task_id, "partial", 4, 2, 1, "部分失败：限流")
        .await
        .expect("with error");
    let (progress, err): (i32, Option<String>) =
        sqlx::query_as("SELECT progress, error_message FROM data_sync_task WHERE task_id = $1")
            .bind(task_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(progress, 50, "手算：success(2)*100/total(4)=50");
    assert_eq!(err.as_deref(), Some("部分失败：限流"));

    // update_sync_task → completed：progress 重算 + error_message 清 NULL
    repository::update_sync_task(&pool, task_id, "completed", 4, 4, 0)
        .await
        .expect("complete");
    let (progress, err, completed_at): (
        i32,
        Option<String>,
        Option<chrono::DateTime<chrono::Utc>>,
    ) = sqlx::query_as(
        "SELECT progress, error_message, completed_at FROM data_sync_task WHERE task_id = $1",
    )
    .bind(task_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(progress, 100, "手算：4*100/4=100");
    assert_eq!(err, None, "completed 状态应清空 error_message");
    assert!(completed_at.is_some(), "终态应写 completed_at");

    cleanup_task(&pool, "zzz-test-task-lifecycle").await;
}

#[tokio::test]
async fn heartbeat_clamps_progress_to_99_and_fail_path() {
    let pool = local_pool().await;
    cleanup_task(&pool, "zzz-test-task-hb").await;
    cleanup_task(&pool, "zzz-test-task-fail").await;

    // 心跳测试任务
    let hb_task = "zzz-test-task-hb";
    repository::create_sync_task(&pool, hb_task, "daily", "running")
        .await
        .expect("create hb task");

    // progress=150 → LEAST(GREATEST(150,0),99)=99
    repository::heartbeat_sync_task(&pool, hb_task, 10, 5, 0, 150)
        .await
        .expect("heartbeat");
    let progress: i32 =
        sqlx::query_scalar("SELECT progress FROM data_sync_task WHERE task_id = $1")
            .bind(hb_task)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(progress, 99, "手算：150 收敛到上限 99");

    // fail_sync_task：failed_count 至少 1 + 错误信息 + completed_at
    let fail_task = "zzz-test-task-fail";
    repository::create_sync_task(&pool, fail_task, "daily", "running")
        .await
        .expect("create fail task");
    repository::fail_sync_task(&pool, fail_task, "上游 40101")
        .await
        .expect("fail");
    let (status, failed, err): (String, i32, Option<String>) = sqlx::query_as(
        "SELECT status, failed_count, error_message FROM data_sync_task WHERE task_id = $1",
    )
    .bind(fail_task)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(status, "failed");
    assert_eq!(failed, 1, "GREATEST(初始0, 1)=1");
    assert_eq!(err.as_deref(), Some("上游 40101"));

    cleanup_task(&pool, "zzz-test-task-hb").await;
    cleanup_task(&pool, "zzz-test-task-fail").await;
}

// ─── data_sync_attempt 族 ────────────────────────────────────────

#[tokio::test]
async fn sync_attempt_upsert_and_success_window_filter() {
    let pool = local_pool().await;
    cleanup_task(&pool, "zzz-test-task-attempt").await;
    cleanup_attempts(&pool).await;

    // FK 前置：data_sync_attempt.task_id → data_sync_task
    repository::create_sync_task(&pool, "zzz-test-task-attempt", "fund_daily", "running")
        .await
        .expect("create attempt host task");

    // 1 条成功 attempt（区间覆盖 2026 全年）+ 1 条失败 + 1 条异源
    repository::upsert_sync_attempt(
        &pool,
        "zzz-test-src",
        ZZZ_ATTEMPT_OK,
        d(2026, 1, 1),
        d(2026, 12, 31),
        "zzz-test-task-attempt",
        "completed",
        42,
        None,
    )
    .await
    .expect("attempt ok");
    repository::upsert_sync_attempt(
        &pool,
        "zzz-test-src",
        ZZZ_ATTEMPT_FAIL,
        d(2026, 1, 1),
        d(2026, 12, 31),
        "zzz-test-task-attempt",
        "failed",
        0,
        Some("40101"),
    )
    .await
    .expect("attempt failed");
    repository::upsert_sync_attempt(
        &pool,
        "zzz-test-other-src",
        ZZZ_ATTEMPT_OK,
        d(2026, 1, 1),
        d(2026, 12, 31),
        "zzz-test-task-attempt",
        "completed",
        1,
        None,
    )
    .await
    .expect("attempt other source");

    // 幂等 upsert：同键 (source, symbol, start, end) 重写 row_count
    repository::upsert_sync_attempt(
        &pool,
        "zzz-test-src",
        ZZZ_ATTEMPT_OK,
        d(2026, 1, 1),
        d(2026, 12, 31),
        "zzz-test-task-attempt",
        "completed",
        99,
        None,
    )
    .await
    .expect("attempt rewrite");
    let rows: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM data_sync_attempt WHERE source LIKE 'zzz-test-%'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(rows, 3, "幂等重写不增行");

    // 窗口命中：查询 [end=2026-06-30, start=2026-01-01]，
    // 条件 attempt.start<=0630 && attempt.end>=0101 → 命中 zzz 成功键
    let hits = repository::list_successfully_attempted_symbols(
        &pool,
        "zzz-test-src",
        d(2026, 6, 30),
        d(2026, 1, 1),
    )
    .await
    .expect("filter hit");
    assert!(hits.iter().any(|h| h == ZZZ_ATTEMPT_OK), "命中={hits:?}");
    assert!(!hits.iter().any(|h| h == ZZZ_ATTEMPT_FAIL), "failed 不入选");
    assert_eq!(hits.len(), 1, "异源不入选");

    // 窗口未命中：查询 2025 年窗口（attempt.start 2026-01-01 <= 2025-06-30 不成立）
    let miss = repository::list_successfully_attempted_symbols(
        &pool,
        "zzz-test-src",
        d(2025, 6, 30),
        d(2025, 1, 1),
    )
    .await
    .expect("filter miss");
    assert!(miss.is_empty(), "窗口外应空，实际 {:?}", miss);

    cleanup_task(&pool, "zzz-test-task-attempt").await;
    cleanup_attempts(&pool).await;
}
