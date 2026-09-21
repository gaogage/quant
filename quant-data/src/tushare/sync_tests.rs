//! sync.rs 主链测试：mock Tushare client + 真实本机 PG 组合。
//!
//! 覆盖 6 条代表性同步链（stock_basic 成败两分支 / daily_bars 单页+4000 翻页 /
//! fund_daily 含 attempt 精确记账 / fund_div 脏行过滤 / trade_calendar /
//! adj_factor 逐只并发路径），键位 ZZZSYNC* 与真实数据零碰撞。
//!
//! 并行安全纪律（8 失败修复核心）：**每个测试只清理自己的键**——
//! 全量清理会在并行测试间互踩（A 的开头清理删掉 B 正在断言的行）。

use chrono::NaiveDate;
use rust_decimal::Decimal;
use serde_json::{json, Value};
use sqlx::PgPool;

use crate::sync;

use super::{client_for, local_pool, spawn_mock_tushare, MockResponse};

// 每测试独占 symbol 键（与 repository_tests 的 ZZZTST* 系互不重叠）
const ZZZ_BASIC_STOCK: &str = "ZZZSYNC1.SH";
const ZZZ_BAR: &str = "ZZZSYNC2.SH";
const ZZZ_PAGE: &str = "ZZZSYNC3.SH";
const ZZZ_FUND: &str = "ZZZSYNC4.SH";
const ZZZ_DIV: &str = "ZZZSYNC5.SH";
const ZZZ_ADJ: &str = "ZZZSYNC6.SH";
/// stock_basic 失败分支专用的"零写入"断言键（不得与成功分支共用：
/// 否则并行时成功分支的写入会让失败分支的 count==0 断言随机失败）
const ZZZ_NEVER_WRITTEN: &str = "ZZZSYNC7.SH";
/// fund_div 失败分支专用键（不得与成功分支共用：两测试对同一 symbol 的
/// data_sync_attempt 行互相 upsert 覆盖，fetch_one 断言会拿到对方状态）
const ZZZ_DIV_FAIL: &str = "ZZZSYNC8.SH";

fn d(y: i32, m: u32, day: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, day).unwrap()
}

fn dec(x: f64) -> Decimal {
    Decimal::from_f64_retain(x).expect("f64→Decimal")
}

/// 浮点容差断言：落库 numeric 列按列 scale 四舍五入，与 from_f64_retain
/// 保留的二进制近似（如 0.149999...99）数值不等，精确 == 会假失败。
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

/// 清理指定 symbol 在四张同步写入表中的行（只动传入键，并行安全）
async fn cleanup_symbols(pool: &PgPool, syms: &[&str]) {
    for sym in syms {
        for table in [
            "market_stock",
            "market_stock_daily_bar",
            "market_fund_div",
            "market_adjustment_factor",
        ] {
            let _ = sqlx::query(&format!("DELETE FROM {} WHERE symbol = $1", table))
                .bind(sym)
                .execute(pool)
                .await;
        }
    }
}

/// 清理指定 symbol 的 attempt 行（source 是真实值 fund_daily/fund_div，按 symbol 清）
async fn cleanup_attempt(pool: &PgPool, sym: &str) {
    let _ = sqlx::query("DELETE FROM data_sync_attempt WHERE symbol = $1")
        .bind(sym)
        .execute(pool)
        .await;
}

/// 清理指定 task_id 的任务行
async fn cleanup_task(pool: &PgPool, task_id: &str) {
    let _ = sqlx::query("DELETE FROM data_sync_task WHERE task_id = $1")
        .bind(task_id)
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

/// sync_trade_calendar 专用的两天日历键（避开 repository_tests 的 01 月键）
async fn cleanup_cal(pool: &PgPool) {
    let _ = sqlx::query(
        "DELETE FROM market_trade_calendar WHERE exchange = 'ZZZ' \
         AND trade_date IN ('2026-02-09', '2026-02-10')",
    )
    .execute(pool)
    .await;
}

/// fund_div 的 task_id 由 sync 内部按运行日生成（fund-div-sync-{yyyymmdd}），
/// 会覆盖当天真实 scheduler 的同名任务行——测试结尾删掉避免残留误导。
async fn cleanup_fund_div_task(pool: &PgPool) {
    let today = chrono::Local::now().date_naive();
    let task_id = format!("fund-div-sync-{}", today.format("%Y%m%d"));
    let _ = sqlx::query("DELETE FROM data_sync_task WHERE task_id = $1")
        .bind(task_id)
        .execute(pool)
        .await;
}

// ═══════════════════════════════════════════════════════════════
// 第二批：重型链批产（任务一）。
//
// 键位续用 ZZZSYNC 系列（9 起步），与既有 1-8 及 repository_tests 的
// ZZZTST* 互不重叠；日期维度键（2027-01 与 2026-10/08-03）均选在
// 真实数据边界之外（真实数据 max 见各测试注释），前置+结尾精确清理。
// ═══════════════════════════════════════════════════════════════

/// sync_moneyflow 成功分支独占键
const ZZZ_MF: &str = "ZZZSYNC9.SH";
/// sync_moneyflow 失败分支零写入断言键（不得与成功分支共用）
const ZZZ_MF_FAIL: &str = "ZZZSYNC10.SH";
/// sync_margin_detail 成功分支
const ZZZ_MD: &str = "ZZZSYNC11.SH";
/// sync_margin_detail 失败分支零写入断言键
const ZZZ_MD_FAIL: &str = "ZZZSYNC12.SH";
/// sync_equity_pledge_pressure（stat + detail 双表共用，两表独立清理）
const ZZZ_PLEDGE: &str = "ZZZSYNC13.SH";
/// sync_shareholder_structure（holder_number + top10 双源）
const ZZZ_SHS: &str = "ZZZSYNC14.SH";
/// sync_financial_data（三接口组合）
const ZZZ_FIN: &str = "ZZZSYNC15.SH";
/// sync_forecast 逐只路径
const ZZZ_FC: &str = "ZZZSYNC16.SH";
/// sync_forecast_by_day 全市场单日路径（落表按 symbol 独占）
const ZZZ_FC_DAY: &str = "ZZZSYNC17.SH";
/// sync_express
const ZZZ_EXP: &str = "ZZZSYNC18.SH";
/// sync_disclosure_date
const ZZZ_DD: &str = "ZZZSYNC19.SH";
/// sync_cashflow 成功分支
const ZZZ_CF: &str = "ZZZSYNC20.SH";
/// sync_cashflow 失败分支（attempt failed 记账断言，须独立键避免
/// 同 symbol attempt 行互相 upsert 覆盖）
const ZZZ_CF_FAIL: &str = "ZZZSYNC21.SH";
/// sync_dividend
const ZZZ_DVD: &str = "ZZZSYNC22.SH";
/// sync_repurchase
const ZZZ_RP: &str = "ZZZSYNC23.SH";
/// sync_share_float
const ZZZ_SF: &str = "ZZZSYNC24.SH";
/// sync_namechange
const ZZZ_NC: &str = "ZZZSYNC25.SH";
/// sync_main_business
const ZZZ_MB: &str = "ZZZSYNC26.SH";
/// sync_industry_membership
const ZZZ_IND: &str = "ZZZSYNC27.SH";
/// sync_block_trade 的 ts_code 维度键
const ZZZ_BT: &str = "ZZZSYNC28.SH";
/// sync_suspension 单日
const ZZZ_SUSP: &str = "ZZZSYNC29.SH";
/// sync_suspension_range 范围
const ZZZ_SUSP_R: &str = "ZZZSYNC30.SH";
/// sync_limit_list 单日
const ZZZ_LIM: &str = "ZZZSYNC31.SH";
/// sync_limit_list_range 范围
const ZZZ_LIM_R: &str = "ZZZSYNC32.SH";
/// sync_futures_price_chain 成功分支合约（futures ts_code 形如 RB2609.SHFE）
const ZZZ_FUT: &str = "ZZZ2609.SHFE";
/// sync_futures_price_chain 失败分支合约（独立合约避免 attempt 键互踩）
const ZZZ_FUT_FAIL: &str = "ZZZ2701.SHFE";

/// 按 symbol 精确清理多张表（只动传入键，并行安全）
async fn cleanup_symbol_tables(pool: &PgPool, tables: &[&str], sym: &str) {
    for table in tables {
        let _ = sqlx::query(&format!("DELETE FROM {} WHERE symbol = $1", table))
            .bind(sym)
            .execute(pool)
            .await;
    }
}

/// 按 ts_code 清理（financial_statement / futures_daily 用 ts_code 列名）
async fn cleanup_ts_code_tables(pool: &PgPool, tables: &[&str], code: &str) {
    for table in tables {
        let _ = sqlx::query(&format!("DELETE FROM {} WHERE ts_code = $1", table))
            .bind(code)
            .execute(pool)
            .await;
    }
}

/// 按 task_id 清 attempt——第二批链的 attempt symbol 位可能是
/// "__ALL__"/"period:xxx"/attempt_key 等非 symbol 值，按 task_id 清最精确
async fn cleanup_attempt_by_task(pool: &PgPool, task_id: &str) {
    let _ = sqlx::query("DELETE FROM data_sync_attempt WHERE task_id = $1")
        .bind(task_id)
        .execute(pool)
        .await;
}

/// 任务+数据版本一并清理（第二批链 dv_id == task_id 传入值）
async fn cleanup_task_and_dv(pool: &PgPool, task_id: &str) {
    cleanup_task(pool, task_id).await;
    cleanup_dv(pool, task_id).await;
}

/// 读取任务终态（status, total, ok, failed）——多链复用
async fn task_state(pool: &PgPool, task_id: &str) -> (String, i32, i32, i32) {
    sqlx::query_as(
        "SELECT status, total_count, success_count, failed_count \
         FROM data_sync_task WHERE task_id = $1",
    )
    .bind(task_id)
    .fetch_one(pool)
    .await
    .expect("任务行应存在")
}

// ─── sync_stock_basic ────────────────────────────────────────────

#[tokio::test]
async fn sync_stock_basic_upserts_both_exchanges_and_completes_task() {
    let pool = local_pool().await;
    cleanup_symbols(&pool, &[ZZZ_BASIC_STOCK]).await;
    cleanup_task(&pool, "zzz-test-sync-basic").await;

    let mock = spawn_mock_tushare(vec![(
        "stock_basic",
        MockResponse::Rows {
            fields: vec![
                "ts_code",
                "name",
                "market",
                "industry",
                "list_status",
                "list_date",
                "delist_date",
                "is_st",
            ],
            items: vec![vec![
                json!(ZZZ_BASIC_STOCK),
                json!("测试同步银行"),
                json!("主板"),
                json!("银行"),
                json!("L"),
                json!("20260105"),
                Value::Null,
                json!("0"),
            ]],
        },
    )])
    .await;
    let client = client_for(&mock.base_url);

    let n = sync::sync_stock_basic(&pool, &client, "zzz-test-sync-basic")
        .await
        .expect("sync_stock_basic 应成功");

    // SSE/SZSE 两个交易所循环各拿一次 mock 响应（同 symbol）→ all.len()=2
    // upsert_stocks_batch 返回处理行数 2（同 symbol 第二次覆盖）
    assert_eq!(n, 2, "返回值 = 两交易所循环累计行数");

    // 两交易所各调一次 API
    let reqs = mock.requests_for("stock_basic");
    assert_eq!(reqs.len(), 2, "SSE + SZSE 各一次");
    let exchanges: Vec<&Value> = reqs
        .iter()
        .map(|r| r.params.get("exchange").unwrap())
        .collect();
    assert!(exchanges.contains(&&json!("SSE")));
    assert!(exchanges.contains(&&json!("SZSE")));

    // 落库断言：1 行（幂等）；exchange 终值是迭代序最后一档，断言双值之一
    // （不绑死循环顺序）
    let row: (String, String, Option<NaiveDate>, bool) = sqlx::query_as(
        "SELECT name, exchange, list_date, is_st FROM market_stock WHERE symbol = $1",
    )
    .bind(ZZZ_BASIC_STOCK)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.0, "测试同步银行");
    assert!(
        row.1 == "SSE" || row.1 == "SZSE",
        "exchange 应为两档之一，实际 {}",
        row.1
    );
    assert_eq!(row.2, Some(d(2026, 1, 5)), "yyyymmdd → NaiveDate");
    assert!(!row.3, "is_st='0' → false");

    // 任务终态 completed
    let status: String = sqlx::query_scalar(
        "SELECT status FROM data_sync_task WHERE task_id = 'zzz-test-sync-basic'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(status, "completed");

    mock.shutdown();
    cleanup_symbols(&pool, &[ZZZ_BASIC_STOCK]).await;
    cleanup_task(&pool, "zzz-test-sync-basic").await;
}

#[tokio::test]
async fn sync_stock_basic_marks_task_failed_on_upstream_error() {
    let pool = local_pool().await;
    // 只清理本测试的键：失败分支不写股票，断言键用独占的 ZZZ_NEVER_WRITTEN
    // （不可全量清理——会删掉并行中成功分支刚写入的行）
    cleanup_symbols(&pool, &[ZZZ_NEVER_WRITTEN]).await;
    cleanup_task(&pool, "zzz-test-sync-basic-fail").await;

    let mock = spawn_mock_tushare(vec![(
        "stock_basic",
        MockResponse::ApiErr {
            code: 40101,
            msg: "抱歉，您没有访问该接口的权限".to_string(),
        },
    )])
    .await;
    let client = client_for(&mock.base_url);

    let err = sync::sync_stock_basic(&pool, &client, "zzz-test-sync-basic-fail")
        .await
        .expect_err("上游 40101 应传播为 Err");
    assert!(err.to_string().contains("40101"), "err={}", err);

    // 失败路径：task 标 failed（update_sync_task(0,0,1) 分支）
    let (status, failed): (String, i32) = sqlx::query_as(
        "SELECT status, failed_count FROM data_sync_task \
         WHERE task_id = 'zzz-test-sync-basic-fail'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(status, "failed");
    assert_eq!(failed, 1);

    // 未写任何股票（独占键，不受并行成功分支影响）
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM market_stock WHERE symbol = $1")
        .bind(ZZZ_NEVER_WRITTEN)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);

    mock.shutdown();
    cleanup_symbols(&pool, &[ZZZ_NEVER_WRITTEN]).await;
    cleanup_task(&pool, "zzz-test-sync-basic-fail").await;
}

// ─── sync_daily_bars ─────────────────────────────────────────────

#[tokio::test]
async fn sync_daily_bars_single_page_skips_invalid_rows() {
    let pool = local_pool().await;
    cleanup_symbols(&pool, &[ZZZ_BAR]).await;
    cleanup_task(&pool, "zzz-test-sync-bar").await;
    cleanup_dv(&pool, "zzz-test-sync-bar").await;

    // 3 行：2 有效 + 1 行 trade_date 为空（filter_map 静默跳过）
    let bar_row = |code: &str, date: &str, close: f64| {
        vec![
            json!(code),
            json!(date),
            json!(10.0),
            json!(close + 0.5),
            json!(9.8),
            json!(close),
            json!(close - 0.1),
            json!(1.5),
            json!(1000.0),
            json!(10200.0),
        ]
    };
    let mock = spawn_mock_tushare(vec![(
        "daily",
        MockResponse::Rows {
            fields: vec![
                "ts_code",
                "trade_date",
                "open",
                "high",
                "low",
                "close",
                "pre_close",
                "pct_chg",
                "vol",
                "amount",
            ],
            items: vec![
                bar_row(ZZZ_BAR, "20260105", 10.0),
                bar_row(ZZZ_BAR, "20260106", 10.5),
                bar_row(ZZZ_BAR, "", 11.0), // 无效行：日期解析失败 → 跳过
            ],
        },
    )])
    .await;
    let client = client_for(&mock.base_url);

    let n = sync::sync_daily_bars(
        &pool,
        &client,
        &[ZZZ_BAR.to_string()],
        "20260105",
        "20260131", // 单月窗口 → 一个月块
        "zzz-test-sync-bar",
    )
    .await
    .expect("sync_daily_bars 应成功");

    // 返回有效行数（无效行不计入 total_rows）
    assert_eq!(n, 2, "无效行被过滤，有效 2 行");

    // 落库 2 行 + 字段映射（pct_chg 1.5% → change_pct 0.015）
    let (count, close_5): (i64, Option<Decimal>) = sqlx::query_as(
        "SELECT COUNT(*), MAX(close) FILTER (WHERE trade_date = '2026-01-05') \
         FROM market_stock_daily_bar WHERE symbol = $1",
    )
    .bind(ZZZ_BAR)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, 2);
    assert_eq!(close_5, Some(dec(10.0)), "10.0 二进制精确，可直接 == ");

    // 任务 completed + dv 已注册（FK 链）
    let status: String =
        sqlx::query_scalar("SELECT status FROM data_sync_task WHERE task_id = 'zzz-test-sync-bar'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(status, "completed");
    let dv: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM data_version WHERE data_version_id = 'zzz-test-sync-bar'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(dv, 1, "同步应注册 data_version");

    // 单页即停：row_count(3) < page_limit(4000)，只发一次请求
    assert_eq!(mock.requests_for("daily").len(), 1);

    mock.shutdown();
    cleanup_symbols(&pool, &[ZZZ_BAR]).await;
    cleanup_task(&pool, "zzz-test-sync-bar").await;
    cleanup_dv(&pool, "zzz-test-sync-bar").await;
}

#[tokio::test]
async fn sync_daily_bars_paginates_with_offset_until_short_page() {
    let pool = local_pool().await;
    cleanup_symbols(&pool, &[ZZZ_PAGE]).await;
    cleanup_task(&pool, "zzz-test-sync-page").await;
    cleanup_dv(&pool, "zzz-test-sync-page").await;

    // 4001 行 > sync 内部 page_limit(4000)：第一页 4000 行（== limit → 继续翻页），
    // 第二页 offset=4000 返回 1 行（< limit → 停止）。日期从 2010-01-01 连续 4001 天。
    let fields = vec![
        "ts_code",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "pct_chg",
        "vol",
        "amount",
    ];
    let base = d(2010, 1, 1);
    let items: Vec<Vec<Value>> = (0..4001u32)
        .map(|i| {
            let date = base + chrono::Duration::days(i as i64);
            vec![
                json!(ZZZ_PAGE),
                json!(date.format("%Y%m%d").to_string()),
                json!(10.0),
                json!(11.0),
                json!(9.5),
                json!(10.5),
                json!(10.4),
                json!(0.5),
                json!(100.0),
                json!(1050.0),
            ]
        })
        .collect();

    let mock = spawn_mock_tushare(vec![(
        "daily",
        MockResponse::Paged {
            fields,
            items,
            page_size: 4000,
        },
    )])
    .await;
    let client = client_for(&mock.base_url);

    let n = sync::sync_daily_bars(
        &pool,
        &client,
        &[ZZZ_PAGE.to_string()],
        "20100101",
        "20100131", // 单月窗口：全部 4001 行在一个月块内翻页拿完
        "zzz-test-sync-page",
    )
    .await
    .expect("分页同步应成功");

    assert_eq!(n, 4001, "两页合计 4000 + 1 行");

    // 翻页请求序列：offset 0 → 4000，恰好 2 次请求
    let reqs = mock.requests_for("daily");
    assert_eq!(reqs.len(), 2, "4000+1 行 → 2 页");
    assert_eq!(reqs[0].params.get("offset"), Some(&json!("0")));
    assert_eq!(reqs[0].params.get("limit"), Some(&json!("4000")));
    assert_eq!(reqs[1].params.get("offset"), Some(&json!("4000")));

    // 落库 4001 行（(symbol, trade_date) 唯一键去重）
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM market_stock_daily_bar WHERE symbol = $1")
            .bind(ZZZ_PAGE)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 4001);

    let status: String = sqlx::query_scalar(
        "SELECT status FROM data_sync_task WHERE task_id = 'zzz-test-sync-page'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(status, "completed");

    mock.shutdown();
    cleanup_symbols(&pool, &[ZZZ_PAGE]).await;
    cleanup_task(&pool, "zzz-test-sync-page").await;
    cleanup_dv(&pool, "zzz-test-sync-page").await;
}

// ─── sync_fund_daily ─────────────────────────────────────────────

#[tokio::test]
async fn sync_fund_daily_writes_bars_and_exact_zero_day_attempts() {
    let pool = local_pool().await;
    cleanup_symbols(&pool, &[ZZZ_FUND]).await;
    cleanup_attempt(&pool, ZZZ_FUND).await;
    cleanup_task(&pool, "zzz-test-sync-fund").await;
    cleanup_dv(&pool, "zzz-test-sync-fund").await;

    // 单 symbol（<=32 → record_exact_zero_days=true）；
    // 窗口 2026-09-14~18：库内真实日历该周 5 天全开市
    let mock = spawn_mock_tushare(vec![(
        "fund_daily",
        MockResponse::Rows {
            fields: vec![
                "ts_code",
                "trade_date",
                "open",
                "high",
                "low",
                "close",
                "pre_close",
                "pct_chg",
                "vol",
                "amount",
            ],
            items: vec![vec![
                json!(ZZZ_FUND),
                json!("20260914"), // 5 个开市日只回 1 天
                json!(1.0),
                json!(1.1),
                json!(0.99),
                json!(1.05),
                json!(1.04),
                json!(0.6),
                json!(10000.0),
                json!(10500.0),
            ]],
        },
    )])
    .await;
    let client = client_for(&mock.base_url);

    let n = sync::sync_fund_daily(
        &pool,
        &client,
        &[ZZZ_FUND.to_string()],
        "20260914",
        "20260918",
        "zzz-test-sync-fund",
    )
    .await
    .expect("sync_fund_daily 应成功");

    assert_eq!(n, 1, "mock 只回 1 根 K 线");

    // 落库 1 行；close 1.05 非二进制精确 → 容差断言
    let (count, close): (i64, Option<Decimal>) =
        sqlx::query_as("SELECT COUNT(*), MAX(close) FROM market_stock_daily_bar WHERE symbol = $1")
            .bind(ZZZ_FUND)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 1);
    assert_dec_close(close.expect("close 应有值"), 1.05, "fund_daily close");

    // attempt 精确记账（手算）：
    //   区间级 1 条（[0914,0918], completed, row_count=1）
    //   日级 0 行 4 条（0915/0916/0917/0918 开市但上游无行）
    let (total, zero_day): (i64, i64) = sqlx::query_as(
        "SELECT COUNT(*), COUNT(*) FILTER (WHERE error_message IS NOT NULL) \
         FROM data_sync_attempt WHERE symbol = $1 AND source = 'fund_daily'",
    )
    .bind(ZZZ_FUND)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(total, 5, "1 区间 + 4 个缺失开市日");
    assert_eq!(zero_day, 4, "4 条日级 0 行补记");

    let range_attempt: Option<i64> = sqlx::query_scalar(
        "SELECT row_count FROM data_sync_attempt \
         WHERE symbol = $1 AND source = 'fund_daily' AND start_date = '2026-09-14' \
           AND end_date = '2026-09-18'",
    )
    .bind(ZZZ_FUND)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(range_attempt, Some(1), "区间级 attempt 记 1 行");

    let status: String = sqlx::query_scalar(
        "SELECT status FROM data_sync_task WHERE task_id = 'zzz-test-sync-fund'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(status, "completed");

    mock.shutdown();
    cleanup_symbols(&pool, &[ZZZ_FUND]).await;
    cleanup_attempt(&pool, ZZZ_FUND).await;
    cleanup_task(&pool, "zzz-test-sync-fund").await;
    cleanup_dv(&pool, "zzz-test-sync-fund").await;
}

// ─── sync_fund_div ───────────────────────────────────────────────

#[tokio::test]
async fn sync_fund_div_keeps_only_implemented_positive_cash_rows() {
    let pool = local_pool().await;
    cleanup_symbols(&pool, &[ZZZ_DIV]).await;
    cleanup_attempt(&pool, ZZZ_DIV).await;
    cleanup_fund_div_task(&pool).await;

    // 3 行：合法实施分红 + 预研阶段（div_proc 过滤）+ 0 现金（cash<=0 过滤）
    let mock = spawn_mock_tushare(vec![(
        "fund_div",
        MockResponse::Rows {
            fields: vec![
                "ts_code",
                "ann_date",
                "imp_anndate",
                "div_proc",
                "record_date",
                "ex_date",
                "pay_date",
                "div_cash",
            ],
            items: vec![
                vec![
                    json!(ZZZ_DIV),
                    json!("20260110"),
                    json!("20260108"),
                    json!("实施"),
                    json!("20260114"),
                    json!("20260115"),
                    json!("20260120"),
                    json!(0.15),
                ],
                vec![
                    json!(ZZZ_DIV),
                    json!("20260110"),
                    json!("20260108"),
                    json!("预研"),
                    json!("20260113"),
                    json!("20260115"),
                    json!("20260120"),
                    json!(0.2),
                ],
                vec![
                    json!(ZZZ_DIV),
                    json!("20260110"),
                    json!("20260108"),
                    json!("实施"),
                    json!("20260114"),
                    json!("20260115"),
                    json!("20260120"),
                    json!(0.0),
                ],
            ],
        },
    )])
    .await;
    let client = client_for(&mock.base_url);

    let n = sync::sync_fund_div(&pool, &client, &[ZZZ_DIV.to_string()])
        .await
        .expect("sync_fund_div 应成功");
    assert_eq!(n, 1, "3 行中只有 1 行通过 div_proc=实施 且 cash>0 过滤");

    // 落库 1 行；available_at = COALESCE(ann_date, ex_date) = 2026-01-10；
    // div_cash 0.15 落库按 numeric(16,6) 舍入，与 from_f64_retain 的二进制近似不等 → 容差
    let row: (Decimal, NaiveDate) =
        sqlx::query_as("SELECT div_cash, available_at FROM market_fund_div WHERE symbol = $1")
            .bind(ZZZ_DIV)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_dec_close(row.0, 0.15, "fund_div div_cash");
    assert_eq!(row.1, d(2026, 1, 10), "available_at 取 ann_date");

    // attempt 记 completed 且 row_count=1（日期为运行日，只断言行数与状态）
    let (status, rc): (String, i64) = sqlx::query_as(
        "SELECT status, row_count FROM data_sync_attempt \
         WHERE symbol = $1 AND source = 'fund_div'",
    )
    .bind(ZZZ_DIV)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(status, "completed");
    assert_eq!(rc, 1);

    mock.shutdown();
    cleanup_symbols(&pool, &[ZZZ_DIV]).await;
    cleanup_attempt(&pool, ZZZ_DIV).await;
    cleanup_fund_div_task(&pool).await;
}

#[tokio::test]
async fn sync_fund_div_upstream_error_records_failed_attempt() {
    let pool = local_pool().await;
    // 独占键：与成功分支分键，避免同 symbol 的 attempt 行互相覆盖
    cleanup_symbols(&pool, &[ZZZ_DIV_FAIL]).await;
    cleanup_attempt(&pool, ZZZ_DIV_FAIL).await;
    cleanup_fund_div_task(&pool).await;

    let mock = spawn_mock_tushare(vec![(
        "fund_div",
        MockResponse::ApiErr {
            code: 40101,
            msg: "权限不足".to_string(),
        },
    )])
    .await;
    let client = client_for(&mock.base_url);

    // 非 40203 错误不重试，直接记 failed attempt；函数本身返回 Ok（单标的失败不中断）
    let n = sync::sync_fund_div(&pool, &client, &[ZZZ_DIV_FAIL.to_string()])
        .await
        .expect("整体应 Ok");
    assert_eq!(n, 0, "失败标的贡献 0 行");

    let (status, err): (String, Option<String>) = sqlx::query_as(
        "SELECT status, error_message FROM data_sync_attempt \
         WHERE symbol = $1 AND source = 'fund_div'",
    )
    .bind(ZZZ_DIV_FAIL)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(status, "failed");
    assert!(
        err.unwrap_or_default().contains("40101"),
        "错误信息应含 40101"
    );

    // 未写分红行
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM market_fund_div WHERE symbol = $1")
        .bind(ZZZ_DIV_FAIL)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);

    mock.shutdown();
    cleanup_symbols(&pool, &[ZZZ_DIV_FAIL]).await;
    cleanup_attempt(&pool, ZZZ_DIV_FAIL).await;
    cleanup_fund_div_task(&pool).await;
}

// ─── sync_trade_calendar ─────────────────────────────────────────

#[tokio::test]
async fn sync_trade_calendar_maps_cal_date_and_pretrade() {
    let pool = local_pool().await;
    cleanup_cal(&pool).await;
    cleanup_task(&pool, "zzz-test-sync-cal").await;

    let mock = spawn_mock_tushare(vec![(
        "trade_cal",
        MockResponse::Rows {
            fields: vec!["exchange", "cal_date", "is_open", "pretrade_date"],
            items: vec![
                vec![json!("ZZZ"), json!("20260209"), json!(1), json!("20260206")],
                vec![json!("ZZZ"), json!("20260210"), json!(0), json!("")],
            ],
        },
    )])
    .await;
    let client = client_for(&mock.base_url);

    let n = sync::sync_trade_calendar_with_task(&pool, &client, "ZZZ", "zzz-test-sync-cal")
        .await
        .expect("日历同步应成功");
    assert_eq!(n, 2);

    // 落库断言：cal_date → trade_date；is_open 数值转 bool；空 pretrade → None
    let rows: Vec<(NaiveDate, bool, Option<NaiveDate>)> = sqlx::query_as(
        "SELECT trade_date, is_open, pre_trade_date FROM market_trade_calendar \
         WHERE exchange = 'ZZZ' AND trade_date IN ('2026-02-09', '2026-02-10') \
         ORDER BY trade_date",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (d(2026, 2, 9), true, Some(d(2026, 2, 6))));
    assert_eq!(rows[1], (d(2026, 2, 10), false, None));

    // exchange 参数必传
    let reqs = mock.requests_for("trade_cal");
    assert_eq!(reqs.len(), 1);
    assert_eq!(reqs[0].params.get("exchange"), Some(&json!("ZZZ")));

    let status: String =
        sqlx::query_scalar("SELECT status FROM data_sync_task WHERE task_id = 'zzz-test-sync-cal'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(status, "completed");

    mock.shutdown();
    cleanup_cal(&pool).await;
    cleanup_task(&pool, "zzz-test-sync-cal").await;
}

// ─── sync_adj_factor（逐只 8 并发路径）────────────────────────────

#[tokio::test]
async fn sync_adj_factor_per_symbol_path_upserts_factors() {
    let pool = local_pool().await;
    cleanup_symbols(&pool, &[ZZZ_ADJ]).await;
    cleanup_task(&pool, "zzz-test-sync-adj").await;
    cleanup_dv(&pool, "zzz-test-sync-adj").await;

    let mock = spawn_mock_tushare(vec![(
        "adj_factor",
        MockResponse::Rows {
            fields: vec!["ts_code", "trade_date", "adj_factor"],
            items: vec![
                vec![json!(ZZZ_ADJ), json!("20260105"), json!(10.5)],
                vec![json!(ZZZ_ADJ), json!("20260106"), json!(11.0)],
            ],
        },
    )])
    .await;
    let client = client_for(&mock.base_url);

    // 非空 symbols → 逐只并发路径（for_each_concurrent(8)）
    let n = sync::sync_adj_factor(
        &pool,
        &client,
        &[ZZZ_ADJ.to_string()],
        "20260105",
        "20260131",
        "zzz-test-sync-adj",
    )
    .await
    .expect("sync_adj_factor 应成功");

    // 返回 ok 计数（1 只成功 = 1），不是行数
    assert_eq!(n, 1, "逐只路径返回成功 symbol 数");

    // 落库 2 行复权因子（10.5/11.0 二进制精确，直接 ==）
    let rows: Vec<(NaiveDate, Decimal)> = sqlx::query_as(
        "SELECT trade_date, adj_factor FROM market_adjustment_factor \
         WHERE symbol = $1 ORDER BY trade_date",
    )
    .bind(ZZZ_ADJ)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (d(2026, 1, 5), dec(10.5)));
    assert_eq!(rows[1], (d(2026, 1, 6), dec(11.0)));

    // 逐只路径请求形态：ts_code + start/end，无 limit/offset（无分页）
    let reqs = mock.requests_for("adj_factor");
    assert_eq!(reqs.len(), 1);
    assert_eq!(reqs[0].params.get("ts_code"), Some(&json!(ZZZ_ADJ)));
    assert_eq!(reqs[0].params.get("start_date"), Some(&json!("20260105")));
    assert!(!reqs[0].params.contains_key("limit"));

    let status: String =
        sqlx::query_scalar("SELECT status FROM data_sync_task WHERE task_id = 'zzz-test-sync-adj'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(status, "completed", "fail=0 → completed");

    mock.shutdown();
    cleanup_symbols(&pool, &[ZZZ_ADJ]).await;
    cleanup_task(&pool, "zzz-test-sync-adj").await;
    cleanup_dv(&pool, "zzz-test-sync-adj").await;
}

// ─── sync_moneyflow（个股资金流向，逐只路径）──────────────────────

#[tokio::test]
async fn sync_moneyflow_per_symbol_writes_rows_and_completes() {
    let pool = local_pool().await;
    cleanup_symbol_tables(&pool, &["market_stock_moneyflow"], ZZZ_MF).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-mf").await;

    // 2 行：1 有效 + 1 行 trade_date 为空（filter_map 静默跳过）
    let mock = spawn_mock_tushare(vec![(
        "moneyflow",
        MockResponse::Rows {
            fields: vec![
                "ts_code",
                "trade_date",
                "buy_sm_vol",
                "net_mf_vol",
                "net_mf_amount",
            ],
            items: vec![
                vec![
                    json!(ZZZ_MF),
                    json!("20260105"),
                    json!(100.5),
                    json!(200.0),
                    json!(3000.5),
                ],
                vec![json!(ZZZ_MF), json!(""), json!(1.0), json!(1.0), json!(1.0)],
            ],
        },
    )])
    .await;
    let client = client_for(&mock.base_url);

    let n = sync::sync_moneyflow(
        &pool,
        &client,
        &[ZZZ_MF.to_string()],
        "20260105",
        "20260131",
        "zzz-test-sync-mf",
    )
    .await
    .expect("sync_moneyflow 应成功");
    assert_eq!(n, 1, "脏行被过滤，有效 1 行");

    // 落库 1 行；net_mf_amount 3000.5 非二进制精确 → 容差断言
    let (count, net_amount): (i64, Option<Decimal>) = sqlx::query_as(
        "SELECT COUNT(*), MAX(net_mf_amount) FROM market_stock_moneyflow WHERE symbol = $1",
    )
    .bind(ZZZ_MF)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, 1);
    assert_dec_close(
        net_amount.expect("net_mf_amount 应有值"),
        3000.5,
        "moneyflow net_mf_amount",
    );

    // 逐只路径请求形态：ts_code + start/end + limit=6000 + offset=0（单页即停）
    let reqs = mock.requests_for("moneyflow");
    assert_eq!(reqs.len(), 1);
    assert_eq!(reqs[0].params.get("ts_code"), Some(&json!(ZZZ_MF)));
    assert_eq!(reqs[0].params.get("start_date"), Some(&json!("20260105")));
    assert_eq!(reqs[0].params.get("end_date"), Some(&json!("20260131")));
    assert_eq!(reqs[0].params.get("limit"), Some(&json!("6000")));
    assert_eq!(reqs[0].params.get("offset"), Some(&json!("0")));

    // 任务终态 completed（failed=0）
    let (status, _, _, failed) = task_state(&pool, "zzz-test-sync-mf").await;
    assert_eq!(status, "completed");
    assert_eq!(failed, 0);

    mock.shutdown();
    cleanup_symbol_tables(&pool, &["market_stock_moneyflow"], ZZZ_MF).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-mf").await;
}

#[tokio::test]
async fn sync_moneyflow_upstream_error_marks_task_partial() {
    let pool = local_pool().await;
    // 失败分支零写入断言用独占键（非 40203 不重试）
    cleanup_symbol_tables(&pool, &["market_stock_moneyflow"], ZZZ_MF_FAIL).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-mf-fail").await;

    let mock = spawn_mock_tushare(vec![(
        "moneyflow",
        MockResponse::ApiErr {
            code: 40101,
            msg: "权限不足".to_string(),
        },
    )])
    .await;
    let client = client_for(&mock.base_url);

    // 单标的失败不中断：函数返回 Ok(0)，task 标 partial
    let n = sync::sync_moneyflow(
        &pool,
        &client,
        &[ZZZ_MF_FAIL.to_string()],
        "20260105",
        "20260131",
        "zzz-test-sync-mf-fail",
    )
    .await
    .expect("整体应 Ok");
    assert_eq!(n, 0, "失败标的贡献 0 行");

    let (status, _, _, failed) = task_state(&pool, "zzz-test-sync-mf-fail").await;
    assert_eq!(status, "partial");
    assert_eq!(failed, 1);

    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM market_stock_moneyflow WHERE symbol = $1")
            .bind(ZZZ_MF_FAIL)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 0, "失败路径不应落库");

    mock.shutdown();
    cleanup_symbol_tables(&pool, &["market_stock_moneyflow"], ZZZ_MF_FAIL).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-mf-fail").await;
}

// ─── sync_margin_detail（个股两融明细，PIT available_at 推导）─────

#[tokio::test]
async fn sync_margin_detail_derives_available_at_from_next_open_date() {
    let pool = local_pool().await;
    cleanup_symbol_tables(&pool, &["market_stock_margin_detail"], ZZZ_MD).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-md").await;

    let mock = spawn_mock_tushare(vec![(
        "margin_detail",
        MockResponse::Rows {
            fields: vec![
                "ts_code",
                "trade_date",
                "name",
                "rzye",
                "rqye",
                "rzmre",
                "rqyl",
                "rzche",
                "rqchl",
                "rqmcl",
                "rzrqye",
            ],
            items: vec![vec![
                json!(ZZZ_MD),
                json!("20260105"), // 真实日历开市日（周一）
                json!("测试两融"),
                json!(1500.5),
                json!(250.0),
                json!(100.0),
                json!(5.5),
                json!(60.0),
                json!(10.0),
                json!(30.0),
                json!(1750.5),
            ]],
        },
    )])
    .await;
    let client = client_for(&mock.base_url);

    let n = sync::sync_margin_detail(
        &pool,
        &client,
        &[ZZZ_MD.to_string()],
        "20260105",
        "20260105", // 单日窗口
        "zzz-test-sync-md",
    )
    .await
    .expect("sync_margin_detail 应成功");
    assert_eq!(n, 1);

    // 落库断言：rzye 容差；available_at = trade_date 之后第一个开市日
    // （真实日历 2026-01-06 周二开市）；source_published_at = available_at 00:30Z
    let row: (
        Option<Decimal>,
        NaiveDate,
        Option<chrono::DateTime<chrono::Utc>>,
    ) = sqlx::query_as(
        "SELECT rzye, available_at, source_published_at \
         FROM market_stock_margin_detail WHERE symbol = $1",
    )
    .bind(ZZZ_MD)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_dec_close(row.0.expect("rzye 应有值"), 1500.5, "margin_detail rzye");
    assert_eq!(row.1, d(2026, 1, 6), "available_at = 下一开市日");
    let published = row.2.expect("source_published_at 应有值");
    assert_eq!(
        published,
        chrono::NaiveDate::from_ymd_opt(2026, 1, 6)
            .unwrap()
            .and_hms_opt(0, 30, 0)
            .unwrap()
            .and_utc(),
        "published_at = available_at 当日 00:30 UTC"
    );

    // 逐只路径请求形态：ts_code + start/end + limit=6000
    let reqs = mock.requests_for("margin_detail");
    assert_eq!(reqs.len(), 1);
    assert_eq!(reqs[0].params.get("ts_code"), Some(&json!(ZZZ_MD)));
    assert_eq!(reqs[0].params.get("limit"), Some(&json!("6000")));

    let (status, _, _, failed) = task_state(&pool, "zzz-test-sync-md").await;
    assert_eq!(status, "completed");
    assert_eq!(failed, 0);

    mock.shutdown();
    cleanup_symbol_tables(&pool, &["market_stock_margin_detail"], ZZZ_MD).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-md").await;
}

#[tokio::test]
async fn sync_margin_detail_error_marks_partial_without_writes() {
    let pool = local_pool().await;
    cleanup_symbol_tables(&pool, &["market_stock_margin_detail"], ZZZ_MD_FAIL).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-md-fail").await;

    let mock = spawn_mock_tushare(vec![(
        "margin_detail",
        MockResponse::ApiErr {
            code: 40101,
            msg: "权限不足".to_string(),
        },
    )])
    .await;
    let client = client_for(&mock.base_url);

    let n = sync::sync_margin_detail(
        &pool,
        &client,
        &[ZZZ_MD_FAIL.to_string()],
        "20260105",
        "20260131",
        "zzz-test-sync-md-fail",
    )
    .await
    .expect("单标的失败不中断，整体 Ok");
    assert_eq!(n, 0);

    let (status, _, _, failed) = task_state(&pool, "zzz-test-sync-md-fail").await;
    assert_eq!(status, "partial");
    assert_eq!(failed, 1);

    mock.shutdown();
    cleanup_symbol_tables(&pool, &["market_stock_margin_detail"], ZZZ_MD_FAIL).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-md-fail").await;
}

// ─── sync_moneyflow_hsgt + sync_margin（市场级日线，按位置解析）───

#[tokio::test]
async fn sync_moneyflow_hsgt_and_margin_upsert_by_date() {
    let pool = local_pool().await;
    // 真实数据边界：hsgt max=2026-05-29 / margin max=2026-09-08，
    // 2027-01-04 无真实行，upsert 不覆盖任何真实数据
    let _ = sqlx::query("DELETE FROM market_moneyflow_hsgt WHERE trade_date = '2027-01-04'")
        .execute(&pool)
        .await;
    let _ = sqlx::query("DELETE FROM market_margin WHERE trade_date = '2027-01-04'")
        .execute(&pool)
        .await;

    // hsgt：按位置解析（[0]date [1]north [2]south [3]nb [4]sb），字符串数字形态；
    // 含 1 行 date 为空的脏行（跳过）
    let mock = spawn_mock_tushare(vec![
        (
            "moneyflow_hsgt",
            MockResponse::Rows {
                fields: vec![
                    "trade_date",
                    "north_flow",
                    "south_flow",
                    "north_balance",
                    "south_balance",
                ],
                items: vec![
                    vec![
                        json!("20270104"),
                        json!("100.5"),
                        json!("-20.1"),
                        json!("30000.25"),
                        json!("-4000.0"),
                    ],
                    vec![
                        json!(""),
                        json!("1.0"),
                        json!("1.0"),
                        json!("1.0"),
                        json!("1.0"),
                    ],
                ],
            },
        ),
        (
            "margin",
            MockResponse::Rows {
                fields: vec!["trade_date", "exchange_id", "rzye", "rqye", "rzrqye"],
                items: vec![vec![
                    json!("20270104"),
                    json!("SSE"),
                    json!(1000.5),   // 数值形态
                    json!("2000.5"), // 字符串形态（as_f64 回退 parse 分支）
                    json!("3001.0"),
                ]],
            },
        ),
    ])
    .await;
    let client = client_for(&mock.base_url);

    // 窗口 2 天 ≤ 92 天 → 单批单次调用
    let n_hsgt = sync::sync_moneyflow_hsgt(&pool, &client, "20270104", "20270105")
        .await
        .expect("sync_moneyflow_hsgt 应成功");
    assert_eq!(n_hsgt, 1, "脏行跳过后 1 行");

    let n_margin = sync::sync_margin(&pool, &client, "20270104", "20270105")
        .await
        .expect("sync_margin 应成功");
    assert_eq!(n_margin, 1);

    // hsgt 落库：north_flow 是 double precision 列 → f64 容差断言
    // （parse→写库→读出往返，用绝对误差 < 1e-9 规避二进制精确比较）
    let hsgt: (f64, f64) = sqlx::query_as(
        "SELECT north_flow, south_balance FROM market_moneyflow_hsgt \
         WHERE trade_date = '2027-01-04'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!((hsgt.0 - 100.5).abs() < 1e-9, "north_flow 实际 {}", hsgt.0);
    assert!(
        (hsgt.1 - (-4000.0)).abs() < 1e-9,
        "south_balance 实际 {}",
        hsgt.1
    );

    // margin 落库（numeric 列 → Decimal 容差）
    let margin: (Decimal, Decimal) = sqlx::query_as(
        "SELECT rzye, rqye FROM market_margin WHERE trade_date = '2027-01-04' AND exchange = 'SSE'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_dec_close(margin.0, 1000.5, "margin rzye");
    assert_dec_close(margin.1, 2000.5, "margin rqye（字符串数字）");

    // 请求形态：start/end 成对
    let hsgt_reqs = mock.requests_for("moneyflow_hsgt");
    assert_eq!(hsgt_reqs.len(), 1);
    assert_eq!(
        hsgt_reqs[0].params.get("start_date"),
        Some(&json!("20270104"))
    );
    assert_eq!(
        hsgt_reqs[0].params.get("end_date"),
        Some(&json!("20270105"))
    );

    mock.shutdown();
    let _ = sqlx::query("DELETE FROM market_moneyflow_hsgt WHERE trade_date = '2027-01-04'")
        .execute(&pool)
        .await;
    let _ = sqlx::query("DELETE FROM market_margin WHERE trade_date = '2027-01-04'")
        .execute(&pool)
        .await;
}

// ─── sync_block_trade（大宗交易，日历空窗口 fallback 逐日）────────

#[tokio::test]
async fn sync_block_trade_falls_back_to_daily_iteration_without_calendar() {
    let pool = local_pool().await;
    // 真实 block_trade max=2026-09-18；窗口选 2027-01（日历只到 2026-12-31，
    // SSE 开市日查询为空 → fallback 逐日迭代）
    let _ = sqlx::query(
        "DELETE FROM market_stock_block_trade WHERE trade_date IN ('2027-01-08', '2027-01-09')",
    )
    .execute(&pool)
    .await;
    cleanup_task(&pool, "zzz-test-sync-bt").await;

    // 每天 2 行：1 有效 + 1 空 ts_code（跳过）→ 两天各落 1 行
    let mock = spawn_mock_tushare(vec![(
        "block_trade",
        MockResponse::Rows {
            fields: vec![
                "ts_code",
                "trade_date",
                "price",
                "vol",
                "amount",
                "buyer",
                "seller",
            ],
            items: vec![
                vec![
                    json!(ZZZ_BT),
                    json!("20270108"),
                    json!(10.5),
                    json!(100.0),
                    json!(1050.0),
                    json!("买方营业部"),
                    json!("卖方营业部"),
                ],
                vec![
                    json!(""),
                    json!("20270108"),
                    json!(1.0),
                    json!(1.0),
                    json!(1.0),
                    json!(""),
                    json!(""),
                ],
            ],
        },
    )])
    .await;
    let client = client_for(&mock.base_url);

    let n = sync::sync_block_trade(&pool, &client, "zzz-test-sync-bt", "20270108", "20270109")
        .await
        .expect("sync_block_trade 应成功");

    // 手算：mock 恒定返回"1 有效 + 1 脏"（行内 trade_date 固定为 20270108），
    // 两个 fallback 日各处理一次 → 计数 2；但落库行内 date 是权威键，
    // 两天 upsert 同一 (2027-01-08, row_no=1) → 表内幂等为 1 行
    assert_eq!(n, 2);

    // 落库：source_row_no 按 idx+1（有效行是第 1 行 → no=1）；
    // available_at = 行内 trade_date + 1 天
    let rows: Vec<(NaiveDate, i32, Decimal, NaiveDate)> = sqlx::query_as(
        "SELECT trade_date, source_row_no, price, available_at \
         FROM market_stock_block_trade WHERE ts_code = $1 ORDER BY trade_date",
    )
    .bind(ZZZ_BT)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "行内 date 权威：两日重复行 upsert 幂等为 1 行"
    );
    assert_eq!(rows[0].0, d(2027, 1, 8));
    assert_eq!(rows[0].1, 1, "有效行在响应中排第 1 → source_row_no=1");
    assert_dec_close(rows[0].2, 10.5, "block_trade price");
    assert_eq!(rows[0].3, d(2027, 1, 9), "available_at = 行内 trade_date+1");

    // 日历空 → fallback 逐日恰好 2 次请求（每天一次，按 trade_date 参数）
    let reqs = mock.requests_for("block_trade");
    assert_eq!(reqs.len(), 2);
    assert_eq!(reqs[0].params.get("trade_date"), Some(&json!("20270108")));
    assert_eq!(reqs[1].params.get("trade_date"), Some(&json!("20270109")));

    mock.shutdown();
    let _ = sqlx::query(
        "DELETE FROM market_stock_block_trade WHERE trade_date IN ('2027-01-08', '2027-01-09')",
    )
    .execute(&pool)
    .await;
    cleanup_task(&pool, "zzz-test-sync-bt").await;
}

// ─── sync_equity_pledge_pressure（质押统计 + 明细双接口）──────────

#[tokio::test]
async fn sync_equity_pledge_pressure_writes_stat_and_detail_tables() {
    let pool = local_pool().await;
    cleanup_symbol_tables(
        &pool,
        &["market_stock_pledge_stat", "market_stock_pledge_detail"],
        ZZZ_PLEDGE,
    )
    .await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-pledge").await;
    cleanup_attempt_by_task(&pool, "zzz-test-sync-pledge").await;

    let mock = spawn_mock_tushare(vec![
        (
            "pledge_stat",
            MockResponse::Rows {
                fields: vec![
                    "ts_code",
                    "end_date",
                    "pledge_count",
                    "unrest_pledge",
                    "rest_pledge",
                    "total_share",
                    "pledge_ratio",
                ],
                items: vec![vec![
                    json!(ZZZ_PLEDGE),
                    json!("20251231"),
                    json!(5),
                    json!(1000.5),
                    json!(2000.5),
                    json!(30000.0),
                    json!(10.5),
                ]],
            },
        ),
        (
            "pledge_detail",
            MockResponse::Rows {
                fields: vec![
                    "ts_code",
                    "ann_date",
                    "holder_name",
                    "pledge_amount",
                    "start_date",
                    "end_date",
                    "is_release",
                    "release_date",
                    "pledgor",
                ],
                items: vec![vec![
                    json!(ZZZ_PLEDGE),
                    json!("20251110"),
                    json!("张某"),
                    json!(500.5),
                    json!("20250101"),
                    json!(""),
                    json!(""),
                    Value::Null,
                    json!("李某"),
                ]],
            },
        ),
    ])
    .await;
    let client = client_for(&mock.base_url);

    // 逐只路径 + 单季度窗口（2025Q4）→ stat 1 单元 + detail 1 窗口
    let n = sync::sync_equity_pledge_pressure(
        &pool,
        &client,
        "zzz-test-sync-pledge",
        &[ZZZ_PLEDGE.to_string()],
        "20251001",
        "20251231",
    )
    .await
    .expect("sync_equity_pledge_pressure 应成功");

    // 手算：stat 1 行 + detail 1 行 = 2
    assert_eq!(n, 2);

    // stat 表：available_at = end_date + 1 天；pledge_ratio 容差
    let stat: (NaiveDate, Decimal) = sqlx::query_as(
        "SELECT available_at, pledge_ratio FROM market_stock_pledge_stat WHERE symbol = $1",
    )
    .bind(ZZZ_PLEDGE)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(stat.0, d(2026, 1, 1), "stat available_at = end_date+1");
    assert_dec_close(stat.1, 10.5, "pledge_ratio");

    // detail 表：available_at = ann_date（原生 PIT）；空 end_date → None
    let detail: (NaiveDate, Option<NaiveDate>) = sqlx::query_as(
        "SELECT available_at, pledge_end_date FROM market_stock_pledge_detail WHERE symbol = $1",
    )
    .bind(ZZZ_PLEDGE)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(detail.0, d(2025, 11, 10), "detail available_at = ann_date");
    assert_eq!(detail.1, None, "空 end_date → None");

    // 请求形态：stat 按逐只 ts_code；detail 按季度窗口 start/end + 分页
    let stat_reqs = mock.requests_for("pledge_stat");
    assert_eq!(stat_reqs.len(), 1);
    assert_eq!(stat_reqs[0].params.get("ts_code"), Some(&json!(ZZZ_PLEDGE)));
    assert_eq!(stat_reqs[0].params.get("limit"), Some(&json!("5000")));
    let detail_reqs = mock.requests_for("pledge_detail");
    assert_eq!(detail_reqs.len(), 1);
    assert_eq!(
        detail_reqs[0].params.get("start_date"),
        Some(&json!("20251001"))
    );
    assert_eq!(
        detail_reqs[0].params.get("end_date"),
        Some(&json!("20251231"))
    );

    let (status, _, _, failed) = task_state(&pool, "zzz-test-sync-pledge").await;
    assert_eq!(status, "completed");
    assert_eq!(failed, 0);

    mock.shutdown();
    cleanup_symbol_tables(
        &pool,
        &["market_stock_pledge_stat", "market_stock_pledge_detail"],
        ZZZ_PLEDGE,
    )
    .await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-pledge").await;
    cleanup_attempt_by_task(&pool, "zzz-test-sync-pledge").await;
}

// ─── sync_shareholder_structure（holder_number + top10 双源）──────

#[tokio::test]
async fn sync_shareholder_structure_writes_two_sources_with_attempt_audit() {
    let pool = local_pool().await;
    cleanup_symbol_tables(
        &pool,
        &["market_stock_holder_number", "market_stock_top10_holders"],
        ZZZ_SHS,
    )
    .await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-shs").await;
    cleanup_attempt_by_task(&pool, "zzz-test-sync-shs").await;

    let mock = spawn_mock_tushare(vec![
        (
            "stk_holdernumber",
            MockResponse::Rows {
                fields: vec!["ts_code", "ann_date", "end_date", "holder_num"],
                items: vec![vec![
                    json!(ZZZ_SHS),
                    json!("20251110"),
                    json!("20250930"),
                    json!(123456),
                ]],
            },
        ),
        (
            "top10_holders",
            MockResponse::Rows {
                fields: vec![
                    "ts_code",
                    "ann_date",
                    "end_date",
                    "holder_name",
                    "hold_amount",
                    "hold_ratio",
                    "hold_float_ratio",
                    "hold_change",
                    "holder_type",
                ],
                items: vec![vec![
                    json!(ZZZ_SHS),
                    json!("20251110"),
                    json!("20250930"),
                    json!("控股股东"),
                    json!(1000000.5),
                    json!(35.5),
                    json!(45.0),
                    json!(1000.0),
                    json!("G"),
                ]],
            },
        ),
    ])
    .await;
    let client = client_for(&mock.base_url);

    // 双源 filter：holder_number（全局）+ top10_holders（逐只）；单季度窗口
    let n = sync::sync_shareholder_structure(
        &pool,
        &client,
        "zzz-test-sync-shs",
        &[ZZZ_SHS.to_string()],
        &["holder_number".to_string(), "top10_holders".to_string()],
        "20251001",
        "20251231",
    )
    .await
    .expect("sync_shareholder_structure 应成功");

    // 手算：holder_number 1 行 + top10 1 行 = 2
    assert_eq!(n, 2);

    // holder_number 表：available_at = ann_date
    let hn: (i64, NaiveDate) = sqlx::query_as(
        "SELECT holder_num, available_at FROM market_stock_holder_number WHERE symbol = $1",
    )
    .bind(ZZZ_SHS)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(hn.0, 123456);
    assert_eq!(hn.1, d(2025, 11, 10));

    // top10 表：hold_ratio 容差；holder_type 保留
    let t10: (Decimal, Option<String>) = sqlx::query_as(
        "SELECT hold_ratio, holder_type FROM market_stock_top10_holders WHERE symbol = $1",
    )
    .bind(ZZZ_SHS)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_dec_close(t10.0, 35.5, "top10 hold_ratio");
    assert_eq!(t10.1.as_deref(), Some("G"));

    // 请求形态：holder_number 全局（无 ts_code，按季度窗口）；top10 逐只
    let hn_reqs = mock.requests_for("stk_holdernumber");
    assert_eq!(hn_reqs.len(), 1);
    assert!(!hn_reqs[0].params.contains_key("ts_code"));
    assert_eq!(
        hn_reqs[0].params.get("start_date"),
        Some(&json!("20251001"))
    );
    let t10_reqs = mock.requests_for("top10_holders");
    assert_eq!(t10_reqs.len(), 1);
    assert_eq!(t10_reqs[0].params.get("ts_code"), Some(&json!(ZZZ_SHS)));

    // attempt 审计：两条 completed（holder_number 的 symbol 位是 __ALL__）
    let (attempts, completed): (i64, i64) = sqlx::query_as(
        "SELECT COUNT(*), COUNT(*) FILTER (WHERE status = 'completed') \
         FROM data_sync_attempt WHERE task_id = $1",
    )
    .bind("zzz-test-sync-shs")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!((attempts, completed), (2, 2));

    let (status, _, _, failed) = task_state(&pool, "zzz-test-sync-shs").await;
    assert_eq!(status, "completed");
    assert_eq!(failed, 0);

    mock.shutdown();
    cleanup_symbol_tables(
        &pool,
        &["market_stock_holder_number", "market_stock_top10_holders"],
        ZZZ_SHS,
    )
    .await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-shs").await;
    cleanup_attempt_by_task(&pool, "zzz-test-sync-shs").await;
}

// ─── sync_financial_data（income + balancesheet + fina_indicator）─

#[tokio::test]
async fn sync_financial_data_expands_income_fields_and_filters_balance_whitelist() {
    let pool = local_pool().await;
    cleanup_ts_code_tables(
        &pool,
        &["market_financial_statement", "market_financial_indicator"],
        ZZZ_FIN,
    )
    .await;
    cleanup_task(&pool, "zzz-test-sync-fin").await;
    cleanup_attempt(&pool, ZZZ_FIN).await;

    let mock = spawn_mock_tushare(vec![
        // income：非黑名单数值字段逐个展开（revenue/total_profit 2 个），
        // name 字符串跳过
        (
            "income",
            MockResponse::Rows {
                fields: vec![
                    "ts_code",
                    "end_date",
                    "ann_date",
                    "revenue",
                    "total_profit",
                    "name",
                ],
                items: vec![vec![
                    json!(ZZZ_FIN),
                    json!("20251231"),
                    json!("20260110"),
                    json!(100.5),
                    json!(20.5),
                    json!("某公司"),
                ]],
            },
        ),
        // balancesheet：白名单 9 字段中的 2 个（total_assets/total_liab），
        // other_field 不在白名单 → 跳过
        (
            "balancesheet",
            MockResponse::Rows {
                fields: vec![
                    "ts_code",
                    "end_date",
                    "ann_date",
                    "total_assets",
                    "total_liab",
                    "other_field",
                ],
                items: vec![vec![
                    json!(ZZZ_FIN),
                    json!("20251231"),
                    json!("20260110"),
                    json!(1000.5),
                    json!(600.5),
                    json!("不在白名单"),
                ]],
            },
        ),
        (
            "fina_indicator",
            MockResponse::Rows {
                fields: vec!["ts_code", "end_date", "ann_date", "eps", "roe"],
                items: vec![vec![
                    json!(ZZZ_FIN),
                    json!("20251231"),
                    json!("20260110"),
                    json!(1.5),
                    json!(12.5),
                ]],
            },
        ),
    ])
    .await;
    let client = client_for(&mock.base_url);

    let (stmt_count, ind_count) = sync::sync_financial_data_with_task(
        &pool,
        &client,
        &[ZZZ_FIN.to_string()],
        "zzz-test-sync-fin",
    )
    .await
    .expect("sync_financial_data 应成功");

    // 手算：income 展开 2 行（revenue+total_profit）+ balance 白名单 2 行
    // （total_assets+total_liab）= 4 statements；indicator 1 行
    assert_eq!(stmt_count, 4);
    assert_eq!(ind_count, 1);

    // statement 表按 (statement_type, field_name) 断言 4 行
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT statement_type, field_name FROM market_financial_statement \
         WHERE ts_code = $1 ORDER BY statement_type, field_name",
    )
    .bind(ZZZ_FIN)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 4);
    assert!(rows.contains(&("balance".into(), "total_assets".into())));
    assert!(rows.contains(&("balance".into(), "total_liab".into())));
    assert!(rows.contains(&("income".into(), "revenue".into())));
    assert!(rows.contains(&("income".into(), "total_profit".into())));
    // 白名单外字段不应出现
    assert!(!rows.iter().any(|(_, f)| f == "other_field"));

    // indicator 表 1 行：ann_date 缺省回退 end_date 的分支未触发（ann_date 有值）
    let ind: (Option<Decimal>, NaiveDate) =
        sqlx::query_as("SELECT eps, ann_date FROM market_financial_indicator WHERE ts_code = $1")
            .bind(ZZZ_FIN)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_dec_close(ind.0.expect("eps 应有值"), 1.5, "fina_indicator eps");
    assert_eq!(ind.1, d(2026, 1, 10));

    // attempt：financial 源 completed，row_count = 4 stmt + 1 ind = 5
    let (status, rc): (String, i64) = sqlx::query_as(
        "SELECT status, row_count FROM data_sync_attempt \
         WHERE symbol = $1 AND source = 'financial'",
    )
    .bind(ZZZ_FIN)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(status, "completed");
    assert_eq!(rc, 5);

    let (task_status, _, _, failed) = task_state(&pool, "zzz-test-sync-fin").await;
    assert_eq!(task_status, "completed");
    assert_eq!(failed, 0);

    mock.shutdown();
    cleanup_ts_code_tables(
        &pool,
        &["market_financial_statement", "market_financial_indicator"],
        ZZZ_FIN,
    )
    .await;
    cleanup_task(&pool, "zzz-test-sync-fin").await;
    cleanup_attempt(&pool, ZZZ_FIN).await;
}

// ─── sync_forecast（逐只路径 + by_day 全市场路径）─────────────────

fn forecast_mock_rows(symbol: &str) -> MockResponse {
    // 2 行：1 有效（预增 50~80%）+ 1 脏行（ann_date 空 → 过滤）
    MockResponse::Rows {
        fields: vec![
            "ts_code",
            "ann_date",
            "end_date",
            "type",
            "p_change_min",
            "p_change_max",
            "net_profit_min",
            "net_profit_max",
            "first_ann_date",
        ],
        items: vec![
            vec![
                json!(symbol),
                json!("20260110"),
                json!("20251231"),
                json!("预增"),
                json!(50.0),
                json!(80.0),
                json!(1.0),
                json!(2.0),
                json!("20260110"),
            ],
            vec![
                json!(symbol),
                json!(""),
                json!("20251231"),
                json!("预增"),
                json!(1.0),
                json!(2.0),
                json!(0.0),
                json!(0.0),
                json!(""),
            ],
        ],
    }
}

#[tokio::test]
async fn sync_forecast_per_symbol_filters_dirty_rows_and_records_attempt() {
    let pool = local_pool().await;
    cleanup_symbol_tables(&pool, &["market_stock_forecast"], ZZZ_FC).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-fc").await;
    cleanup_attempt(&pool, ZZZ_FC).await;

    let mock = spawn_mock_tushare(vec![("forecast", forecast_mock_rows(ZZZ_FC))]).await;
    let client = client_for(&mock.base_url);

    let n = sync::sync_forecast(
        &pool,
        &client,
        &[ZZZ_FC.to_string()],
        "20260101",
        "20260131",
        "zzz-test-sync-fc",
    )
    .await
    .expect("sync_forecast 应成功");
    assert_eq!(n, 1, "脏行（ann_date 空）被过滤");

    // 落库：type → forecast_type；available_at = ann_date
    let row: (String, NaiveDate, Option<Decimal>) = sqlx::query_as(
        "SELECT forecast_type, available_at, p_change_max \
         FROM market_stock_forecast WHERE symbol = $1",
    )
    .bind(ZZZ_FC)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.0, "预增");
    assert_eq!(row.1, d(2026, 1, 10));
    assert_dec_close(
        row.2.expect("p_change_max 应有值"),
        80.0,
        "forecast p_change_max",
    );

    // 逐只请求形态：ts_code + start/end + limit=2000 + offset=0
    let reqs = mock.requests_for("forecast");
    assert_eq!(reqs.len(), 1);
    assert_eq!(reqs[0].params.get("ts_code"), Some(&json!(ZZZ_FC)));
    assert_eq!(reqs[0].params.get("limit"), Some(&json!("2000")));

    // attempt：forecast 源 completed row_count=1
    let (status, rc): (String, i64) = sqlx::query_as(
        "SELECT status, row_count FROM data_sync_attempt \
         WHERE symbol = $1 AND source = 'forecast'",
    )
    .bind(ZZZ_FC)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(status, "completed");
    assert_eq!(rc, 1);

    let (task_status, _, _, failed) = task_state(&pool, "zzz-test-sync-fc").await;
    assert_eq!(task_status, "completed");
    assert_eq!(failed, 0);

    mock.shutdown();
    cleanup_symbol_tables(&pool, &["market_stock_forecast"], ZZZ_FC).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-fc").await;
    cleanup_attempt(&pool, ZZZ_FC).await;
}

#[tokio::test]
async fn sync_forecast_by_day_pulls_whole_market_for_single_date() {
    let pool = local_pool().await;
    cleanup_symbol_tables(&pool, &["market_stock_forecast"], ZZZ_FC_DAY).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-fc-day").await;

    let mock = spawn_mock_tushare(vec![("forecast", forecast_mock_rows(ZZZ_FC_DAY))]).await;
    let client = client_for(&mock.base_url);

    let n = sync::sync_forecast_by_day(&pool, &client, "20260110", "zzz-test-sync-fc-day")
        .await
        .expect("sync_forecast_by_day 应成功");
    assert_eq!(n, 1, "脏行被过滤，有效 1 行");

    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM market_stock_forecast WHERE symbol = $1")
            .bind(ZZZ_FC_DAY)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 1);

    // by_day 请求形态：ann_date 单日全市场（无 ts_code）+ 分页参数
    let reqs = mock.requests_for("forecast");
    assert_eq!(reqs.len(), 1);
    assert!(!reqs[0].params.contains_key("ts_code"));
    assert_eq!(reqs[0].params.get("ann_date"), Some(&json!("20260110")));
    assert_eq!(reqs[0].params.get("limit"), Some(&json!("2000")));

    // by_day 无失败路径 → 直接 completed（total/success 均为行数）
    let (status, total, ok, failed) = task_state(&pool, "zzz-test-sync-fc-day").await;
    assert_eq!(status, "completed");
    assert_eq!((total, ok, failed), (1, 1, 0));

    mock.shutdown();
    cleanup_symbol_tables(&pool, &["market_stock_forecast"], ZZZ_FC_DAY).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-fc-day").await;
}

// ─── sync_express（业绩快报）──────────────────────────────────────

#[tokio::test]
async fn sync_express_writes_express_rows_with_available_at() {
    let pool = local_pool().await;
    cleanup_symbol_tables(&pool, &["market_stock_express"], ZZZ_EXP).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-exp").await;
    cleanup_attempt(&pool, ZZZ_EXP).await;

    let mock = spawn_mock_tushare(vec![(
        "express",
        MockResponse::Rows {
            fields: vec![
                "ts_code",
                "ann_date",
                "end_date",
                "revenue",
                "n_income",
                "diluted_eps",
                "is_audit",
            ],
            items: vec![vec![
                json!(ZZZ_EXP),
                json!("20260120"),
                json!("20251231"),
                json!(2000000000.5),
                json!(300000000.0),
                json!(0.5),
                json!(1),
            ]],
        },
    )])
    .await;
    let client = client_for(&mock.base_url);

    let n = sync::sync_express(
        &pool,
        &client,
        &[ZZZ_EXP.to_string()],
        "20260101",
        "20260131",
        "zzz-test-sync-exp",
    )
    .await
    .expect("sync_express 应成功");
    assert_eq!(n, 1);

    let row: (Option<Decimal>, NaiveDate, Option<i32>) = sqlx::query_as(
        "SELECT revenue, available_at, is_audit FROM market_stock_express WHERE symbol = $1",
    )
    .bind(ZZZ_EXP)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_dec_close(
        row.0.expect("revenue 应有值"),
        2000000000.5,
        "express revenue",
    );
    assert_eq!(row.1, d(2026, 1, 20), "available_at = ann_date");
    assert_eq!(row.2, Some(1), "is_audit 数值转 i32");

    // express 的 ts_code 是必填参数（签名非 Option）
    let reqs = mock.requests_for("express");
    assert_eq!(reqs.len(), 1);
    assert_eq!(reqs[0].params.get("ts_code"), Some(&json!(ZZZ_EXP)));
    assert_eq!(reqs[0].params.get("start_date"), Some(&json!("20260101")));

    let (status, _, _, failed) = task_state(&pool, "zzz-test-sync-exp").await;
    assert_eq!(status, "completed");
    assert_eq!(failed, 0);

    mock.shutdown();
    cleanup_symbol_tables(&pool, &["market_stock_express"], ZZZ_EXP).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-exp").await;
    cleanup_attempt(&pool, ZZZ_EXP).await;
}

// ─── sync_disclosure_date（按报告期全市场拉取 + symbol 过滤）─────

#[tokio::test]
async fn sync_disclosure_date_filters_symbols_and_takes_max_available_at() {
    let pool = local_pool().await;
    cleanup_symbol_tables(&pool, &["market_stock_disclosure_date"], ZZZ_DD).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-dd").await;
    cleanup_attempt(&pool, ZZZ_DD).await;

    // 2 行：ZZZ_DD 有效 + 非请求 symbol（symbol_filter 过滤）
    let mock = spawn_mock_tushare(vec![(
        "disclosure_date",
        MockResponse::Rows {
            fields: vec![
                "ts_code",
                "end_date",
                "ann_date",
                "pre_date",
                "actual_date",
                "modify_date",
            ],
            items: vec![
                vec![
                    json!(ZZZ_DD),
                    json!("20251231"),
                    json!("20260120"),
                    json!("20260115"),
                    json!("20260121"),
                    json!("20260122"),
                ],
                vec![
                    json!("000000.SZ"),
                    json!("20251231"),
                    json!("20260120"),
                    json!("20260115"),
                    json!("20260121"),
                    json!(""),
                ],
            ],
        },
    )])
    .await;
    let client = client_for(&mock.base_url);

    // 窗口单季度 → periods=[20251231] 一次全市场拉取
    let n = sync::sync_disclosure_date(
        &pool,
        &client,
        &[ZZZ_DD.to_string()],
        "20251201",
        "20251231",
        "zzz-test-sync-dd",
    )
    .await
    .expect("sync_disclosure_date 应成功");
    assert_eq!(n, 1, "非请求 symbol 被过滤");

    // available_at = max(ann, actual, modify) = 2026-01-22
    let row: (NaiveDate, NaiveDate, NaiveDate) = sqlx::query_as(
        "SELECT pre_date, actual_date, available_at \
         FROM market_stock_disclosure_date WHERE symbol = $1",
    )
    .bind(ZZZ_DD)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.0, d(2026, 1, 15));
    assert_eq!(row.1, d(2026, 1, 21));
    assert_eq!(row.2, d(2026, 1, 22), "available_at = 三日期最大值");

    // 请求形态：按 period（end_date 参数）+ limit=3000，无 ts_code
    let reqs = mock.requests_for("disclosure_date");
    assert_eq!(reqs.len(), 1);
    assert_eq!(reqs[0].params.get("end_date"), Some(&json!("20251231")));
    assert_eq!(reqs[0].params.get("limit"), Some(&json!("3000")));
    assert!(!reqs[0].params.contains_key("ts_code"));

    // attempt：请求 symbol 记账 row_count=1
    let (status, rc): (String, i64) = sqlx::query_as(
        "SELECT status, row_count FROM data_sync_attempt \
         WHERE symbol = $1 AND source = 'disclosure_date'",
    )
    .bind(ZZZ_DD)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(status, "completed");
    assert_eq!(rc, 1);

    mock.shutdown();
    cleanup_symbol_tables(&pool, &["market_stock_disclosure_date"], ZZZ_DD).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-dd").await;
    cleanup_attempt(&pool, ZZZ_DD).await;
}

// ─── sync_cashflow（现金流：成功 + 失败 attempt 记账）────────────

#[tokio::test]
async fn sync_cashflow_uses_f_ann_date_for_available_at() {
    let pool = local_pool().await;
    cleanup_symbol_tables(&pool, &["market_stock_cashflow"], ZZZ_CF).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-cf").await;
    cleanup_attempt(&pool, ZZZ_CF).await;

    // f_ann_date 空 → available_at 回退 ann_date（COALESCE 语义）
    let mock = spawn_mock_tushare(vec![(
        "cashflow",
        MockResponse::Rows {
            fields: vec![
                "ts_code",
                "ann_date",
                "f_ann_date",
                "end_date",
                "net_profit",
                "n_cashflow_act",
                "c_cash_equ_end_period",
            ],
            items: vec![vec![
                json!(ZZZ_CF),
                json!("20260115"),
                json!(""),
                json!("20251231"),
                json!(1000000000.0),
                json!(500000000.5),
                json!(200000000.0),
            ]],
        },
    )])
    .await;
    let client = client_for(&mock.base_url);

    let n = sync::sync_cashflow(
        &pool,
        &client,
        &[ZZZ_CF.to_string()],
        "20260101",
        "20260131",
        "zzz-test-sync-cf",
    )
    .await
    .expect("sync_cashflow 应成功");
    assert_eq!(n, 1);

    let row: (NaiveDate, Option<NaiveDate>, Option<Decimal>) = sqlx::query_as(
        "SELECT available_at, f_ann_date, n_cashflow_act \
         FROM market_stock_cashflow WHERE symbol = $1",
    )
    .bind(ZZZ_CF)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        row.0,
        d(2026, 1, 15),
        "f_ann_date 空 → available_at 回退 ann_date"
    );
    assert_eq!(row.1, None);
    assert_dec_close(
        row.2.expect("n_cashflow_act 应有值"),
        500000000.5,
        "cashflow",
    );

    let reqs = mock.requests_for("cashflow");
    assert_eq!(reqs.len(), 1);
    assert_eq!(reqs[0].params.get("ts_code"), Some(&json!(ZZZ_CF)));
    assert_eq!(reqs[0].params.get("start_date"), Some(&json!("20260101")));
    assert_eq!(reqs[0].params.get("limit"), Some(&json!("2000")));

    let (status, rc): (String, i64) = sqlx::query_as(
        "SELECT status, row_count FROM data_sync_attempt \
         WHERE symbol = $1 AND source = 'cashflow'",
    )
    .bind(ZZZ_CF)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(status, "completed");
    assert_eq!(rc, 1);

    mock.shutdown();
    cleanup_symbol_tables(&pool, &["market_stock_cashflow"], ZZZ_CF).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-cf").await;
    cleanup_attempt(&pool, ZZZ_CF).await;
}

#[tokio::test]
async fn sync_cashflow_upstream_error_records_failed_attempt() {
    let pool = local_pool().await;
    // 独立键：失败 attempt 的 upsert 会与成功分支的同 (source,symbol) 键互踩
    cleanup_symbol_tables(&pool, &["market_stock_cashflow"], ZZZ_CF_FAIL).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-cf-fail").await;
    cleanup_attempt(&pool, ZZZ_CF_FAIL).await;

    let mock = spawn_mock_tushare(vec![(
        "cashflow",
        MockResponse::ApiErr {
            code: 40101,
            msg: "权限不足".to_string(),
        },
    )])
    .await;
    let client = client_for(&mock.base_url);

    // 单标的失败不中断：Ok(0) + attempt failed + task partial
    let n = sync::sync_cashflow(
        &pool,
        &client,
        &[ZZZ_CF_FAIL.to_string()],
        "20260101",
        "20260131",
        "zzz-test-sync-cf-fail",
    )
    .await
    .expect("整体应 Ok");
    assert_eq!(n, 0);

    let (status, err): (String, Option<String>) = sqlx::query_as(
        "SELECT status, error_message FROM data_sync_attempt \
         WHERE symbol = $1 AND source = 'cashflow'",
    )
    .bind(ZZZ_CF_FAIL)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(status, "failed");
    assert!(err.unwrap_or_default().contains("40101"));

    let (task_status, _, _, failed) = task_state(&pool, "zzz-test-sync-cf-fail").await;
    assert_eq!(task_status, "partial");
    assert_eq!(failed, 1);

    mock.shutdown();
    cleanup_symbol_tables(&pool, &["market_stock_cashflow"], ZZZ_CF_FAIL).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-cf-fail").await;
    cleanup_attempt(&pool, ZZZ_CF_FAIL).await;
}

// ─── sync_dividend（分红送股：imp_ann_date 优先 + 窗口过滤）───────

#[tokio::test]
async fn sync_dividend_prefers_imp_ann_date_and_filters_window() {
    let pool = local_pool().await;
    cleanup_symbol_tables(&pool, &["market_stock_dividend"], ZZZ_DVD).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-dvd").await;
    cleanup_attempt(&pool, ZZZ_DVD).await;

    // 3 行：合法实施（imp_ann_date 优先）+ 脏行（ann_date 空）+ 窗口外（available_at
    // = imp_ann_date=20260215 不在 [20260101,20260131] → date_in_range 过滤）
    let mock = spawn_mock_tushare(vec![(
        "dividend",
        MockResponse::Rows {
            fields: vec![
                "ts_code",
                "end_date",
                "ann_date",
                "div_proc",
                "cash_div",
                "cash_div_tax",
                "stk_div",
                "stk_bo_rate",
                "stk_co_rate",
                "record_date",
                "ex_date",
                "pay_date",
                "imp_ann_date",
            ],
            items: vec![
                vec![
                    json!(ZZZ_DVD),
                    json!("20251231"),
                    json!("20260115"),
                    json!("实施"),
                    json!(0.15),
                    json!(0.0),
                    json!(0.0),
                    json!(0.0),
                    json!(0.0),
                    json!("20260120"),
                    json!("20260121"),
                    json!("20260122"),
                    json!("20260118"),
                ],
                vec![
                    json!(ZZZ_DVD),
                    json!("20251231"),
                    json!(""),
                    json!("实施"),
                    json!(0.1),
                    json!(0.0),
                    json!(0.0),
                    json!(0.0),
                    json!(0.0),
                    json!(""),
                    json!(""),
                    json!(""),
                    json!(""),
                ],
                vec![
                    json!(ZZZ_DVD),
                    json!("20251231"),
                    json!("20260115"),
                    json!("实施"),
                    json!(0.2),
                    json!(0.0),
                    json!(0.0),
                    json!(0.0),
                    json!(0.0),
                    json!(""),
                    json!(""),
                    json!(""),
                    json!("20260215"),
                ],
            ],
        },
    )])
    .await;
    let client = client_for(&mock.base_url);

    let n = sync::sync_dividend(
        &pool,
        &client,
        &[ZZZ_DVD.to_string()],
        "20260101",
        "20260131",
        "zzz-test-sync-dvd",
    )
    .await
    .expect("sync_dividend 应成功");
    assert_eq!(n, 1, "脏行 + 窗口外行均被过滤");

    // available_at = imp_ann_date（2026-01-18 优先于 ann_date 2026-01-15）
    let row: (NaiveDate, Option<Decimal>) = sqlx::query_as(
        "SELECT available_at, cash_div FROM market_stock_dividend WHERE symbol = $1",
    )
    .bind(ZZZ_DVD)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.0, d(2026, 1, 18), "available_at = imp_ann_date");
    assert_dec_close(row.1.expect("cash_div 应有值"), 0.15, "dividend cash_div");

    // dividend 调用不传日期参数（接口按 symbol 全量），只传分页
    let reqs = mock.requests_for("dividend");
    assert_eq!(reqs.len(), 1);
    assert_eq!(reqs[0].params.get("ts_code"), Some(&json!(ZZZ_DVD)));
    assert!(!reqs[0].params.contains_key("ann_date"));

    mock.shutdown();
    cleanup_symbol_tables(&pool, &["market_stock_dividend"], ZZZ_DVD).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-dvd").await;
    cleanup_attempt(&pool, ZZZ_DVD).await;
}

// ─── sync_repurchase（全局拉取 + symbol 过滤）─────────────────────

#[tokio::test]
async fn sync_repurchase_filters_requested_symbols_from_global_pull() {
    let pool = local_pool().await;
    cleanup_symbol_tables(&pool, &["market_stock_repurchase"], ZZZ_RP).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-rp").await;

    // 2 行：ZZZ_RP 有效 + 其他 symbol（symbol_filter 过滤）
    let mock = spawn_mock_tushare(vec![(
        "repurchase",
        MockResponse::Rows {
            fields: vec![
                "ts_code",
                "ann_date",
                "end_date",
                "proc",
                "exp_date",
                "vol",
                "amount",
                "high_limit",
                "low_limit",
            ],
            items: vec![
                vec![
                    json!(ZZZ_RP),
                    json!("20260115"),
                    json!("20260131"),
                    json!("实施"),
                    json!("20260630"),
                    json!(100.5),
                    json!(1050.0),
                    json!(11.0),
                    json!(9.0),
                ],
                vec![
                    json!("000000.SZ"),
                    json!("20260115"),
                    json!("20260131"),
                    json!("实施"),
                    json!(""),
                    json!(1.0),
                    json!(10.0),
                    json!(""),
                    json!(""),
                ],
            ],
        },
    )])
    .await;
    let client = client_for(&mock.base_url);

    let n = sync::sync_repurchase(
        &pool,
        &client,
        &[ZZZ_RP.to_string()],
        "20260101",
        "20260131",
        "zzz-test-sync-rp",
    )
    .await
    .expect("sync_repurchase 应成功");
    assert_eq!(n, 1, "非请求 symbol 被过滤");

    let row: (String, Option<Decimal>, Option<Decimal>) =
        sqlx::query_as("SELECT proc, vol, amount FROM market_stock_repurchase WHERE symbol = $1")
            .bind(ZZZ_RP)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(row.0, "实施");
    assert_dec_close(row.1.expect("vol 应有值"), 100.5, "repurchase vol");
    assert_dec_close(row.2.expect("amount 应有值"), 1050.0, "repurchase amount");

    // repurchase 官方参数不含 ts_code：按 start/end 窗口全局拉取
    let reqs = mock.requests_for("repurchase");
    assert_eq!(reqs.len(), 1);
    assert!(!reqs[0].params.contains_key("ts_code"));
    assert_eq!(reqs[0].params.get("start_date"), Some(&json!("20260101")));
    assert_eq!(reqs[0].params.get("end_date"), Some(&json!("20260131")));

    let (status, _, _, failed) = task_state(&pool, "zzz-test-sync-rp").await;
    assert_eq!(status, "completed");
    assert_eq!(failed, 0);

    mock.shutdown();
    cleanup_symbol_tables(&pool, &["market_stock_repurchase"], ZZZ_RP).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-rp").await;
}

// ─── sync_share_float（限售解禁：全局拉取 + attempt 记账）─────────

#[tokio::test]
async fn sync_share_float_writes_unlock_rows_with_attempt() {
    let pool = local_pool().await;
    cleanup_symbol_tables(&pool, &["market_stock_share_float"], ZZZ_SF).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-sf").await;
    cleanup_attempt(&pool, ZZZ_SF).await;

    let mock = spawn_mock_tushare(vec![(
        "share_float",
        MockResponse::Rows {
            fields: vec![
                "ts_code",
                "ann_date",
                "float_date",
                "float_share",
                "float_ratio",
                "holder_name",
                "share_type",
            ],
            items: vec![vec![
                json!(ZZZ_SF),
                json!("20260110"),
                json!("20260201"),
                json!(1000.5),
                json!(5.5),
                json!("控股股东"),
                json!("限售股"),
            ]],
        },
    )])
    .await;
    let client = client_for(&mock.base_url);

    let n = sync::sync_share_float(
        &pool,
        &client,
        &[ZZZ_SF.to_string()],
        "20260101",
        "20260131",
        "zzz-test-sync-sf",
    )
    .await
    .expect("sync_share_float 应成功");
    assert_eq!(n, 1);

    let row: (NaiveDate, NaiveDate, Option<Decimal>) = sqlx::query_as(
        "SELECT available_at, float_date, float_share \
         FROM market_stock_share_float WHERE symbol = $1",
    )
    .bind(ZZZ_SF)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.0, d(2026, 1, 10), "available_at = ann_date");
    assert_eq!(row.1, d(2026, 2, 1));
    assert_dec_close(row.2.expect("float_share 应有值"), 1000.5, "share_float");

    // 全局拉取按 ann_date 窗口 + 分页
    let reqs = mock.requests_for("share_float");
    assert_eq!(reqs.len(), 1);
    assert!(!reqs[0].params.contains_key("ts_code"));
    assert_eq!(reqs[0].params.get("start_date"), Some(&json!("20260101")));
    assert_eq!(reqs[0].params.get("limit"), Some(&json!("2000")));

    // attempt：share_float 源 completed row_count=1
    let (status, rc): (String, i64) = sqlx::query_as(
        "SELECT status, row_count FROM data_sync_attempt \
         WHERE symbol = $1 AND source = 'share_float'",
    )
    .bind(ZZZ_SF)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(status, "completed");
    assert_eq!(rc, 1);

    mock.shutdown();
    cleanup_symbol_tables(&pool, &["market_stock_share_float"], ZZZ_SF).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-sf").await;
    cleanup_attempt(&pool, ZZZ_SF).await;
}

// ─── sync_namechange（ST 名称历史 PIT）───────────────────────────

#[tokio::test]
async fn sync_namechange_detects_st_periods_and_writes_history() {
    let pool = local_pool().await;
    cleanup_symbol_tables(&pool, &["market_stock_name_history"], ZZZ_NC).await;

    // 3 行：ST 名（is_st=true）+ 普通更名（end_date 空 → NULL）+ 空 ts_code（跳过）
    let mock = spawn_mock_tushare(vec![(
        "namechange",
        MockResponse::Rows {
            fields: vec!["ts_code", "name", "start_date", "end_date", "change_reason"],
            items: vec![
                vec![
                    json!(ZZZ_NC),
                    json!("ST测试股"),
                    json!("20260101"),
                    json!("20260601"),
                    json!("其他风险警示"),
                ],
                vec![
                    json!(ZZZ_NC),
                    json!("恢复正常名"),
                    json!("20260602"),
                    json!(""),
                    json!("摘帽"),
                ],
                vec![
                    json!(""),
                    json!("某股"),
                    json!("20260101"),
                    json!(""),
                    json!("x"),
                ],
            ],
        },
    )])
    .await;
    let client = client_for(&mock.base_url);

    let total = sync::sync_namechange(&pool, &client)
        .await
        .expect("sync_namechange 应成功");
    assert_eq!(total, 2, "空 ts_code 行被跳过");

    // 落库：is_st 判定（含 ST 且非"退市"开头）；空 end_date → NULL
    let rows: Vec<(String, bool, Option<NaiveDate>)> = sqlx::query_as(
        "SELECT name, is_st, end_date FROM market_stock_name_history \
         WHERE symbol = $1 ORDER BY start_date",
    )
    .bind(ZZZ_NC)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], ("ST测试股".into(), true, Some(d(2026, 6, 1))));
    assert_eq!(rows[1], ("恢复正常名".into(), false, None));

    // 请求形态：1990 年至今全量（start_date=19900101）
    let reqs = mock.requests_for("namechange");
    assert_eq!(reqs.len(), 1);
    assert_eq!(reqs[0].params.get("start_date"), Some(&json!("19900101")));

    mock.shutdown();
    cleanup_symbol_tables(&pool, &["market_stock_name_history"], ZZZ_NC).await;
}

// ─── sync_suspension（停牌：单日 + 范围）─────────────────────────

#[tokio::test]
async fn sync_suspension_replaces_day_rows_and_records_completion() {
    let pool = local_pool().await;
    // 真实 suspension 数据 max=2026-09-18；2027-01-04 无真实行，
    // 函数开头的"清除当日旧数据"不会误删真实数据
    let _ = sqlx::query(
        "DELETE FROM market_stock_suspension WHERE symbol = $1 AND trade_date = '2027-01-04'",
    )
    .bind(ZZZ_SUSP)
    .execute(&pool)
    .await;
    cleanup_task(&pool, "suspension_daily-20270104").await;

    // 2 行：1 有效 + 1 空 ts_code（跳过）
    let mock = spawn_mock_tushare(vec![(
        "suspend_d",
        MockResponse::Rows {
            fields: vec!["ts_code", "suspend_type"],
            items: vec![
                vec![json!(ZZZ_SUSP), json!("S")],
                vec![json!(""), json!("S")],
            ],
        },
    )])
    .await;
    let client = client_for(&mock.base_url);

    let total = sync::sync_suspension(&pool, &client, "20270104")
        .await
        .expect("sync_suspension 应成功");
    assert_eq!(total, 1, "空 ts_code 行被跳过");

    // 落库：suspend_type 原样写入
    let row: (String,) = sqlx::query_as(
        "SELECT suspend_type FROM market_stock_suspension \
         WHERE symbol = $1 AND trade_date = '2027-01-04'",
    )
    .bind(ZZZ_SUSP)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.0, "S");

    // 完成标记：suspension_daily-20270104 completed
    let (status, total_count): (String, i32) = sqlx::query_as(
        "SELECT status, total_count FROM data_sync_task \
         WHERE task_id = 'suspension_daily-20270104'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(status, "completed");
    assert_eq!(total_count, 1);

    mock.shutdown();
    let _ = sqlx::query(
        "DELETE FROM market_stock_suspension WHERE symbol = $1 AND trade_date = '2027-01-04'",
    )
    .bind(ZZZ_SUSP)
    .execute(&pool)
    .await;
    cleanup_task(&pool, "suspension_daily-20270104").await;
}

#[tokio::test]
async fn sync_suspension_range_iterates_calendar_open_dates() {
    let pool = local_pool().await;
    // 真实日历 2026-10-08/09 开市（国庆后）；suspension 真实数据 max=2026-09-18，
    // 该窗口无真实行
    let _ = sqlx::query(
        "DELETE FROM market_stock_suspension WHERE symbol = $1 AND trade_date IN ('2026-10-08', '2026-10-09')",
    )
    .bind(ZZZ_SUSP_R)
    .execute(&pool)
    .await;
    cleanup_task(&pool, "suspension_daily-20261008").await;
    cleanup_task(&pool, "suspension_daily-20261009").await;

    // 恒定 1 行 → 每个开市日 1 行
    let mock = spawn_mock_tushare(vec![(
        "suspend_d",
        MockResponse::Rows {
            fields: vec!["ts_code", "suspend_type"],
            items: vec![vec![json!(ZZZ_SUSP_R), json!("S")]],
        },
    )])
    .await;
    let client = client_for(&mock.base_url);

    let total = sync::sync_suspension_range(&pool, &client, "20261008", "20261009")
        .await
        .expect("sync_suspension_range 应成功");

    // 手算：日历 2 个开市日 × 每天 1 行 = 2
    assert_eq!(total, 2);

    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM market_stock_suspension \
         WHERE symbol = $1 AND trade_date IN ('2026-10-08', '2026-10-09')",
    )
    .bind(ZZZ_SUSP_R)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, 2);

    // 按开市日逐日调用（非 range 参数）
    let reqs = mock.requests_for("suspend_d");
    assert_eq!(reqs.len(), 2);
    assert_eq!(reqs[0].params.get("trade_date"), Some(&json!("20261008")));
    assert_eq!(reqs[1].params.get("trade_date"), Some(&json!("20261009")));

    mock.shutdown();
    let _ = sqlx::query(
        "DELETE FROM market_stock_suspension WHERE symbol = $1 AND trade_date IN ('2026-10-08', '2026-10-09')",
    )
    .bind(ZZZ_SUSP_R)
    .execute(&pool)
    .await;
    cleanup_task(&pool, "suspension_daily-20261008").await;
    cleanup_task(&pool, "suspension_daily-20261009").await;
}

// ─── sync_limit_list（涨跌停：单日 + 范围）───────────────────────

#[tokio::test]
async fn sync_limit_list_writes_symbols_with_null_direction() {
    let pool = local_pool().await;
    // 真实 limit 数据 max=2026-09-19；2027-01-05 无真实行
    let _ = sqlx::query(
        "DELETE FROM market_stock_limit WHERE symbol = $1 AND trade_date = '2027-01-05'",
    )
    .bind(ZZZ_LIM)
    .execute(&pool)
    .await;
    cleanup_task(&pool, "limit_daily-20270105").await;

    let mock = spawn_mock_tushare(vec![(
        "limit_list_d",
        MockResponse::Rows {
            fields: vec!["ts_code"],
            items: vec![vec![json!(ZZZ_LIM)]],
        },
    )])
    .await;
    let client = client_for(&mock.base_url);

    let total = sync::sync_limit_list(&pool, &client, "20270105")
        .await
        .expect("sync_limit_list 应成功");
    assert_eq!(total, 1);

    // 接口无方向字段：只写 symbol，limit_type 留空待 derive 补全
    let row: (Option<String>,) = sqlx::query_as(
        "SELECT limit_type FROM market_stock_limit \
         WHERE symbol = $1 AND trade_date = '2027-01-05'",
    )
    .bind(ZZZ_LIM)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.0, None, "limit_type 由 derive 补全，此处应为 NULL");

    // 完成标记 + 请求形态（trade_date 单日参数）
    let (status, total_count): (String, i32) = sqlx::query_as(
        "SELECT status, total_count FROM data_sync_task \
         WHERE task_id = 'limit_daily-20270105'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(status, "completed");
    assert_eq!(total_count, 1);

    let reqs = mock.requests_for("limit_list_d");
    assert_eq!(reqs.len(), 1);
    assert_eq!(reqs[0].params.get("trade_date"), Some(&json!("20270105")));

    mock.shutdown();
    let _ = sqlx::query(
        "DELETE FROM market_stock_limit WHERE symbol = $1 AND trade_date = '2027-01-05'",
    )
    .bind(ZZZ_LIM)
    .execute(&pool)
    .await;
    cleanup_task(&pool, "limit_daily-20270105").await;
}

#[tokio::test]
async fn sync_limit_list_range_deletes_chunk_then_inserts_from_rows() {
    let pool = local_pool().await;
    let _ = sqlx::query(
        "DELETE FROM market_stock_limit WHERE symbol = $1 AND trade_date IN ('2027-01-06', '2027-01-07')",
    )
    .bind(ZZZ_LIM_R)
    .execute(&pool)
    .await;

    // 行自带 trade_date 字段（range 模式与单日模式的数据差异点）
    let mock = spawn_mock_tushare(vec![(
        "limit_list_d",
        MockResponse::Rows {
            fields: vec!["ts_code", "trade_date"],
            items: vec![
                vec![json!(ZZZ_LIM_R), json!("20270106")],
                vec![json!(ZZZ_LIM_R), json!("20270107")],
                vec![json!(""), json!("20270106")], // 空 ts_code 跳过
            ],
        },
    )])
    .await;
    let client = client_for(&mock.base_url);

    let total = sync::sync_limit_list_range(&pool, &client, "20270106", "20270107")
        .await
        .expect("sync_limit_list_range 应成功");
    assert_eq!(total, 2, "空 ts_code 行被跳过");

    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM market_stock_limit \
         WHERE symbol = $1 AND trade_date IN ('2027-01-06', '2027-01-07')",
    )
    .bind(ZZZ_LIM_R)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, 2);

    // 2 天窗口 ≤ 3 天分块 → 单次 range 调用（start/end 成对，无 trade_date）
    let reqs = mock.requests_for("limit_list_d");
    assert_eq!(reqs.len(), 1);
    assert_eq!(reqs[0].params.get("start_date"), Some(&json!("20270106")));
    assert_eq!(reqs[0].params.get("end_date"), Some(&json!("20270107")));
    assert!(!reqs[0].params.contains_key("trade_date"));

    mock.shutdown();
    let _ = sqlx::query(
        "DELETE FROM market_stock_limit WHERE symbol = $1 AND trade_date IN ('2027-01-06', '2027-01-07')",
    )
    .bind(ZZZ_LIM_R)
    .execute(&pool)
    .await;
}

// ─── sync_futures_price_chain（期货三接口：日线/仓单/持仓排名）────

#[tokio::test]
async fn sync_futures_price_chain_writes_three_tables_for_open_date() {
    let pool = local_pool().await;
    // 真实 futures 三表数据 max=2026-06-18；选真实日历开市日 2026-08-03（周一），
    // 该日无真实期货数据，三表 upsert 零碰撞。单日窗口 = 1 交易日 × 3 接口。
    cleanup_ts_code_tables(&pool, &["market_futures_daily"], ZZZ_FUT).await;
    let _ = sqlx::query(
        "DELETE FROM market_futures_warehouse_receipt \
         WHERE symbol = 'ZZZFUT' AND trade_date = '2026-08-03'",
    )
    .execute(&pool)
    .await;
    let _ = sqlx::query(
        "DELETE FROM market_futures_holding_rank \
         WHERE symbol = 'ZZZFUT' AND trade_date = '2026-08-03'",
    )
    .execute(&pool)
    .await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-fpc").await;
    cleanup_attempt_by_task(&pool, "zzz-test-sync-fpc").await;

    let mock = spawn_mock_tushare(vec![
        (
            "fut_daily",
            MockResponse::Rows {
                fields: vec![
                    "ts_code",
                    "trade_date",
                    "pre_close",
                    "pre_settle",
                    "open",
                    "high",
                    "low",
                    "close",
                    "settle",
                    "change1",
                    "change2",
                    "vol",
                    "amount",
                    "oi",
                    "oi_chg",
                    "delv_settle",
                ],
                items: vec![vec![
                    json!(ZZZ_FUT),
                    json!("20260803"),
                    json!(3400.0),
                    json!(3410.0),
                    json!(3450.0),
                    json!(3520.0),
                    json!(3380.0),
                    json!(3500.5),
                    json!(3510.0),
                    json!(100.5),
                    json!(90.5),
                    json!(12345.0),
                    json!(4321000.0),
                    json!(123456.0),
                    json!(-100.0),
                    json!(3520.0),
                ]],
            },
        ),
        (
            "fut_wsr",
            MockResponse::Rows {
                fields: vec![
                    "trade_date",
                    "symbol",
                    "fut_name",
                    "warehouse",
                    "wh_id",
                    "pre_vol",
                    "vol",
                    "vol_chg",
                    "area",
                    "year",
                    "grade",
                    "brand",
                    "place",
                    "pd",
                    "is_ct",
                    "unit",
                    "exchange",
                ],
                items: vec![vec![
                    json!("20260803"),
                    json!("ZZZFUT"),
                    json!("测试锌"),
                    json!("测试仓库"),
                    json!("1"),
                    json!(100.0),
                    json!(120.5),
                    json!(20.5),
                    json!("华东"),
                    json!("2026"),
                    json!("标准级"),
                    json!(""),
                    json!(""),
                    json!(""),
                    json!("0"),
                    json!("吨"),
                    json!("SHFE"),
                ]],
            },
        ),
        (
            "fut_holding",
            MockResponse::Rows {
                fields: vec![
                    "trade_date",
                    "symbol",
                    "broker",
                    "vol",
                    "vol_chg",
                    "long_hld",
                    "long_chg",
                    "short_hld",
                    "short_chg",
                    "exchange",
                ],
                items: vec![vec![
                    json!("20260803"),
                    json!("ZZZFUT"),
                    json!("测试期货"),
                    json!(5000.0),
                    json!(100.0),
                    json!(3000.5),
                    json!(50.0),
                    json!(2800.0),
                    json!(-30.0),
                    json!("SHFE"),
                ]],
            },
        ),
    ])
    .await;
    let client = client_for(&mock.base_url);

    let n = sync::sync_futures_price_chain(
        &pool,
        &client,
        "zzz-test-sync-fpc",
        &[ZZZ_FUT.to_string()],
        &["SHFE".to_string()],
        "20260803",
        "20260803", // 单日窗口：1 交易日 × 1 symbol × 1 exchange × 3 接口
    )
    .await
    .expect("sync_futures_price_chain 应成功");

    // 手算：daily 1 行 + wsr 1 行 + holding 1 行 = 3
    assert_eq!(n, 3);

    // 日线表：available_at = trade_date + 1
    let daily: (Option<Decimal>, NaiveDate) =
        sqlx::query_as("SELECT close, available_at FROM market_futures_daily WHERE ts_code = $1")
            .bind(ZZZ_FUT)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_dec_close(daily.0.expect("close 应有值"), 3500.5, "fut_daily close");
    assert_eq!(daily.1, d(2026, 8, 4), "available_at = trade_date+1");

    // 仓单表：uk (trade_date, symbol, exchange, warehouse)
    let wsr: (Option<Decimal>,) = sqlx::query_as(
        "SELECT vol FROM market_futures_warehouse_receipt \
         WHERE symbol = 'ZZZFUT' AND trade_date = '2026-08-03'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_dec_close(wsr.0.expect("vol 应有值"), 120.5, "fut_wsr vol");

    // 持仓排名表：uk (trade_date, symbol, exchange, broker)
    let holding: (Option<Decimal>,) = sqlx::query_as(
        "SELECT long_hld FROM market_futures_holding_rank \
         WHERE symbol = 'ZZZFUT' AND trade_date = '2026-08-03'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_dec_close(
        holding.0.expect("long_hld 应有值"),
        3000.5,
        "fut_holding long_hld",
    );

    // 请求形态：三接口各 1 次，ts_code/trade_date/exchange 过滤器齐备
    let daily_reqs = mock.requests_for("fut_daily");
    assert_eq!(daily_reqs.len(), 1);
    assert_eq!(daily_reqs[0].params.get("ts_code"), Some(&json!(ZZZ_FUT)));
    assert_eq!(
        daily_reqs[0].params.get("trade_date"),
        Some(&json!("20260803"))
    );
    assert_eq!(daily_reqs[0].params.get("exchange"), Some(&json!("SHFE")));
    let wsr_reqs = mock.requests_for("fut_wsr");
    assert_eq!(wsr_reqs.len(), 1);
    assert_eq!(wsr_reqs[0].params.get("symbol"), Some(&json!(ZZZ_FUT)));
    let holding_reqs = mock.requests_for("fut_holding");
    assert_eq!(holding_reqs.len(), 1);
    assert_eq!(holding_reqs[0].params.get("symbol"), Some(&json!(ZZZ_FUT)));

    // attempt：3 条 completed（daily/wsr/holding 各一）
    let (attempts, completed): (i64, i64) = sqlx::query_as(
        "SELECT COUNT(*), COUNT(*) FILTER (WHERE status = 'completed') \
         FROM data_sync_attempt WHERE task_id = $1",
    )
    .bind("zzz-test-sync-fpc")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!((attempts, completed), (3, 3));

    let (status, _, _, failed) = task_state(&pool, "zzz-test-sync-fpc").await;
    assert_eq!(status, "completed");
    assert_eq!(failed, 0);

    mock.shutdown();
    cleanup_ts_code_tables(&pool, &["market_futures_daily"], ZZZ_FUT).await;
    let _ = sqlx::query(
        "DELETE FROM market_futures_warehouse_receipt \
         WHERE symbol = 'ZZZFUT' AND trade_date = '2026-08-03'",
    )
    .execute(&pool)
    .await;
    let _ = sqlx::query(
        "DELETE FROM market_futures_holding_rank \
         WHERE symbol = 'ZZZFUT' AND trade_date = '2026-08-03'",
    )
    .execute(&pool)
    .await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-fpc").await;
    cleanup_attempt_by_task(&pool, "zzz-test-sync-fpc").await;
}

#[tokio::test]
async fn sync_futures_price_chain_daily_error_fails_task_with_attempt() {
    let pool = local_pool().await;
    cleanup_ts_code_tables(&pool, &["market_futures_daily"], ZZZ_FUT_FAIL).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-fpc-fail").await;
    cleanup_attempt_by_task(&pool, "zzz-test-sync-fpc-fail").await;

    // fut_daily 恒 40101 → 第一个单元失败即返回 Err（与逐标的容错链不同，
    // 本链任一单元失败直接终止任务）
    let mock = spawn_mock_tushare(vec![(
        "fut_daily",
        MockResponse::ApiErr {
            code: 40101,
            msg: "权限不足".to_string(),
        },
    )])
    .await;
    let client = client_for(&mock.base_url);

    let err = sync::sync_futures_price_chain(
        &pool,
        &client,
        "zzz-test-sync-fpc-fail",
        &[ZZZ_FUT_FAIL.to_string()],
        &["SHFE".to_string()],
        "20260803",
        "20260803",
    )
    .await
    .expect_err("fut_daily 失败应传播为 Err");
    assert!(err.to_string().contains("fut_daily"), "err={}", err);

    // attempt 记 failed + 错误信息；task 标 partial
    let (status, err_msg): (String, Option<String>) = sqlx::query_as(
        "SELECT status, error_message FROM data_sync_attempt \
         WHERE task_id = $1 AND source = 'futures_price_chain_daily'",
    )
    .bind("zzz-test-sync-fpc-fail")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(status, "failed");
    assert!(err_msg.unwrap_or_default().contains("40101"));

    let (task_status, _, _, failed) = task_state(&pool, "zzz-test-sync-fpc-fail").await;
    assert_eq!(task_status, "partial");
    assert_eq!(failed, 1);

    mock.shutdown();
    cleanup_ts_code_tables(&pool, &["market_futures_daily"], ZZZ_FUT_FAIL).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-fpc-fail").await;
    cleanup_attempt_by_task(&pool, "zzz-test-sync-fpc-fail").await;
}

// ─── sync_main_business（available_at 依赖财务/披露链路 join）────

#[tokio::test]
async fn sync_main_business_joins_available_at_and_filters_universe() {
    let pool = local_pool().await;
    cleanup_symbol_tables(&pool, &["market_stock_main_business"], ZZZ_MB).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-mb").await;
    cleanup_attempt_by_task(&pool, "zzz-test-sync-mb").await;
    // 预插 financial_statement 作为 available_at 源（load_main_business_available_at_for_period
    // 的 join 事实）：ann_date >= period 且 end_date = period
    let _ = sqlx::query(
        "DELETE FROM market_financial_statement WHERE ts_code = $1 AND field_name = 'zzz_seed'",
    )
    .bind(ZZZ_MB)
    .execute(&pool)
    .await;

    let mock = spawn_mock_tushare(vec![(
        "fina_mainbz_vip",
        MockResponse::Rows {
            fields: vec![
                "ts_code",
                "end_date",
                "bz_item",
                "bz_code",
                "bz_sales",
                "bz_profit",
                "bz_cost",
                "curr_type",
                "update_flag",
            ],
            items: vec![
                vec![
                    json!(ZZZ_MB),
                    json!("20251231"),
                    json!("主营锌锭"),
                    json!("P"),
                    json!(1000.5),
                    json!(200.5),
                    json!(600.0),
                    json!("人民币"),
                    json!("1"),
                ],
                // 非请求 symbol → out_of_universe 过滤（不查 available_at）
                vec![
                    json!("ZZZMBX.SH"),
                    json!("20251231"),
                    json!("其他业务"),
                    json!("P"),
                    json!(1.0),
                    json!(1.0),
                    json!(1.0),
                    json!("人民币"),
                    json!("1"),
                ],
            ],
        },
    )])
    .await;
    let client = client_for(&mock.base_url);

    // 先预插 available_at 源行（period=20251231，公告日 2026-01-10）
    sqlx::query(
        "INSERT INTO market_financial_statement \
         (ts_code, ann_date, end_date, statement_type, field_name, field_value) \
         VALUES ($1, '2026-01-10', '2025-12-31', 'income', 'zzz_seed', 1.0)",
    )
    .bind(ZZZ_MB)
    .execute(&pool)
    .await
    .unwrap();

    let n = sync::sync_main_business(
        &pool,
        &client,
        "zzz-test-sync-mb",
        &[ZZZ_MB.to_string()],
        "20251001",
        "20251231", // 单季度 → periods=[20251231]
        "P",
    )
    .await
    .expect("sync_main_business 应成功");
    assert_eq!(n, 1, "out_of_universe 行被过滤");

    // 落库：available_at 来自 financial_statement join（2026-01-10）；
    // business_type 默认取 bz_code
    let row: (NaiveDate, String, Option<Decimal>) = sqlx::query_as(
        "SELECT available_at, business_type, bz_sales \
         FROM market_stock_main_business WHERE symbol = $1",
    )
    .bind(ZZZ_MB)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.0, d(2026, 1, 10), "available_at = 财务公告日 join 结果");
    assert_eq!(row.1, "P");
    assert_dec_close(
        row.2.expect("bz_sales 应有值"),
        1000.5,
        "main_business bz_sales",
    );

    // 请求形态：按 period 拉全市场（period + type + 分页），无 ts_code
    let reqs = mock.requests_for("fina_mainbz_vip");
    assert_eq!(reqs.len(), 1);
    assert_eq!(reqs[0].params.get("period"), Some(&json!("20251231")));
    assert_eq!(reqs[0].params.get("type"), Some(&json!("P")));
    assert!(!reqs[0].params.contains_key("ts_code"));

    // attempt：note 记录 out_of_universe_rows=1（审计过滤事实）
    let (status, note): (String, Option<String>) =
        sqlx::query_as("SELECT status, error_message FROM data_sync_attempt WHERE task_id = $1")
            .bind("zzz-test-sync-mb")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(status, "completed");
    assert!(
        note.unwrap_or_default().contains("out_of_universe_rows=1"),
        "note 应含 out_of_universe_rows=1"
    );

    mock.shutdown();
    cleanup_symbol_tables(&pool, &["market_stock_main_business"], ZZZ_MB).await;
    let _ = sqlx::query(
        "DELETE FROM market_financial_statement WHERE ts_code = $1 AND field_name = 'zzz_seed'",
    )
    .bind(ZZZ_MB)
    .execute(&pool)
    .await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-mb").await;
    cleanup_attempt_by_task(&pool, "zzz-test-sync-mb").await;
}

// ─── sync_industry_membership（双分类源 + index_member_all 成分）──

#[tokio::test]
async fn sync_industry_membership_syncs_both_classification_sources() {
    let pool = local_pool().await;
    let _ = sqlx::query("DELETE FROM market_stock_industry_membership_pit WHERE symbol = $1")
        .bind(ZZZ_IND)
        .execute(&pool)
        .await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-ind").await;

    // index_classify 对 SW2014/SW2021 两源各拉一次：mock 恒返回同一行且
    // 不带 src 字段 → 第一源回退 fallback "SW2014"、第二源回退 "SW2021"，
    // metas 得到同 index_code 的两条分类元数据
    let mock = spawn_mock_tushare(vec![
        (
            "index_classify",
            MockResponse::Rows {
                fields: vec![
                    "index_code",
                    "industry_name",
                    "parent_code",
                    "level",
                    "industry_code",
                    "is_pub",
                ],
                items: vec![vec![
                    json!("801ZZZ.SI"),
                    json!("测试业"),
                    json!(""),
                    json!("L1"),
                    json!("801000"),
                    json!(1),
                ]],
            },
        ),
        (
            "index_member_all",
            MockResponse::Rows {
                fields: vec![
                    "l1_code", "l2_code", "l3_code", "ts_code", "name", "in_date", "out_date",
                    "is_new",
                ],
                items: vec![vec![
                    json!("801ZZZ.SI"),
                    json!(""),
                    json!(""),
                    json!(ZZZ_IND),
                    json!("测试股"),
                    json!("20250101"),
                    json!(""),
                    json!("1"),
                ]],
            },
        ),
    ])
    .await;
    let client = client_for(&mock.base_url);

    let n = sync::sync_industry_membership(
        &pool,
        &client,
        "zzz-test-sync-ind",
        &["801ZZZ.SI".to_string()],
        "20250101",
        "20251231",
    )
    .await
    .expect("sync_industry_membership 应成功");

    // 手算：两分类源各 1 条成员 → 2 行
    assert_eq!(n, 2);

    // 落库：classification_source 双值；uk (classification_source, index_code, symbol, in_date)
    let rows: Vec<(String, String, NaiveDate)> = sqlx::query_as(
        "SELECT classification_source, industry_level, in_date \
         FROM market_stock_industry_membership_pit WHERE symbol = $1 \
         ORDER BY classification_source",
    )
    .bind(ZZZ_IND)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, "SW2014");
    assert_eq!(rows[1].0, "SW2021");
    assert_eq!(rows[0].1, "L1");
    assert_eq!(rows[0].2, d(2025, 1, 1), "available_at = in_date");

    // 请求形态：classify 按源循环（level=L1 + src 参数）；
    // member 的 801 前缀码映射到 l1_code 参数位
    let classify_reqs = mock.requests_for("index_classify");
    assert_eq!(classify_reqs.len(), 2, "SW2014 + SW2021 各一次");
    assert_eq!(classify_reqs[0].params.get("level"), Some(&json!("L1")));
    assert_eq!(classify_reqs[0].params.get("src"), Some(&json!("SW2014")));
    assert_eq!(classify_reqs[1].params.get("src"), Some(&json!("SW2021")));
    let member_reqs = mock.requests_for("index_member_all");
    assert_eq!(member_reqs.len(), 2);
    assert_eq!(
        member_reqs[0].params.get("l1_code"),
        Some(&json!("801ZZZ.SI")),
        "801 前缀分类码应映射到 l1_code 参数"
    );
    assert!(!member_reqs[0].params.contains_key("l2_code"));

    mock.shutdown();
    let _ = sqlx::query("DELETE FROM market_stock_industry_membership_pit WHERE symbol = $1")
        .bind(ZZZ_IND)
        .execute(&pool)
        .await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-ind").await;
}

// ═══════════════════════════════════════════════════════════════
// 第四批：sync.rs 尾部零覆盖链补齐。
//
// - sync_daily_basic（逐只/全市场月度/日级回退/限流中断/checkpoint 心跳）
// - sync_fund_basic / sync_fund_nav / sync_fund_adj / sync_index_daily
// - run_quality_check / derive_limit_list_from_daily_bars /
//   backfill_limit_completion_markers / get_st_symbols_at_date /
//   get_pit_main_board_non_st_symbols
//
// 键位续用 ZZZSYNC 系列（33 起步）；derive 涨跌停受 ^[036][0-9]{5}\.(SH|SZ)$
// 正则约束，用 099998/099999/369999 形态键（真实市场无此代码）。
// 日期统一选未来开市周 2026-12-28~30（真实日历已同步到年末、真实 derive
// 任务键未跑过，与生产数据零碰撞）；run_quality_check 用真实开市周
// 2026-09-14~18（该周 5 天全开市，zzz symbol 行零碰撞）。
// ═══════════════════════════════════════════════════════════════

/// sync_daily_basic 逐只路径
const ZZZ_DB1: &str = "ZZZSYNC33.SH";
/// sync_daily_basic 全市场月度路径
const ZZZ_DB2: &str = "ZZZSYNC34.SH";
/// sync_daily_basic checkpoint 心跳路径（51 只共用同一 mock 行）
const ZZZ_DB5: &str = "ZZZSYNC37.SH";
/// sync_fund_basic 有效行
const ZZZ_FB1: &str = "ZZZSYNC38.SH";
/// sync_fund_basic status=D 退市跳过行
const ZZZ_FB2: &str = "ZZZSYNC39.SH";
/// sync_fund_nav 成功/失败/最新跳过三场景
const ZZZ_FN1: &str = "ZZZSYNC40.SH";
const ZZZ_FN2: &str = "ZZZSYNC41.SH";
const ZZZ_FN3: &str = "ZZZSYNC42.SH";
/// sync_fund_adj 成功/失败/取消
const ZZZ_FA1: &str = "ZZZSYNC43.SH";
const ZZZ_FA2: &str = "ZZZSYNC44.SH";
const ZZZ_FA3: &str = "ZZZSYNC45.SH";
/// sync_index_daily 成功/失败
const ZZZ_IX1: &str = "ZZZSYNC46.SH";
const ZZZ_IX2: &str = "ZZZSYNC47.SH";
/// run_quality_check
const ZZZ_QC: &str = "ZZZSYNC48.SH";
/// get_st_symbols_at_date：闭区间/开区间/非 ST 行
const ZZZ_ST1: &str = "ZZZSYNC49.SH";
const ZZZ_ST2: &str = "ZZZSYNC50.SH";
const ZZZ_ST3: &str = "ZZZSYNC52.SH";
/// get_pit_main_board：主板正常/ST/创业板形态/未上市/退市
const ZZZ_MB1: &str = "ZZZSYNC51.SH";
const ZZZ_MB2: &str = "ZZZSYNC53.SH";
const ZZZ_MB_FUT: &str = "ZZZSYNC54.SH";
const ZZZ_MB_DLQ: &str = "ZZZSYNC55.SH";
const ZZZ_GEM: &str = "300ZZZ.SZ";
/// derive_limit_list 主链（非 ST：U/D/无涨停三天）
const ZZZ_DL1: &str = "099999.SZ";
/// derive_limit_list 次新股过滤（list_date 距窗口不足 5 个开市日）
const ZZZ_DL2: &str = "099998.SZ";
/// derive_limit_list ST 5% 板判定
const ZZZ_DL3: &str = "369999.SH";

/// daily_basic 接口的估值字段集（sync_daily_basic 行映射所需全集）
const DAILY_BASIC_FIELDS: &[&str] = &[
    "ts_code",
    "trade_date",
    "pe_ttm",
    "pb",
    "ps_ttm",
    "dv_ttm",
    "total_share",
    "float_share",
    "free_share",
    "total_mv",
    "circ_mv",
];

/// 清理第四批写过的表（按 symbol 精确键，逐表 DELETE，并行安全）
async fn cleanup_zzz_tables(pool: &PgPool, syms: &[&str]) {
    for sym in syms {
        for table in [
            "market_stock",
            "market_stock_daily_bar",
            "market_fund",
            "market_fund_nav",
            "market_adjustment_factor",
            "market_index_daily_bar",
            "market_stock_daily_basic",
            "market_stock_limit",
            "market_stock_name_history",
        ] {
            let _ = sqlx::query(&format!("DELETE FROM {} WHERE symbol = $1", table))
                .bind(sym)
                .execute(pool)
                .await;
        }
    }
}

/// derive/backfill 写入的完成标记任务键（2026-12-28~30 三个开市日）
// 实现（backfill_event_sync_completion_markers）生成 task_id = "{task_type}-{date}"，
// derive_limit_list 传 task_type="limit_daily"、source="derived:daily_limit"——
// task_id 与 source 的值不要搞反（原臆测 derived:* 为 task_id 已修正）
const DERIVE_TASK_KEYS: [&str; 3] = [
    "limit_daily-20261228",
    "limit_daily-20261229",
    "limit_daily-20261230",
];

async fn cleanup_derive_tasks(pool: &PgPool) {
    for task_id in DERIVE_TASK_KEYS {
        cleanup_task(pool, task_id).await;
    }
}

// ─── sync_daily_basic ────────────────────────────────────────────

#[tokio::test]
async fn sync_daily_basic_per_symbol_writes_valuation_rows() {
    let pool = local_pool().await;
    cleanup_zzz_tables(&pool, &[ZZZ_DB1]).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-db1").await;

    let mock = spawn_mock_tushare(vec![(
        "daily_basic",
        MockResponse::Rows {
            fields: DAILY_BASIC_FIELDS.to_vec(),
            items: vec![vec![
                json!(ZZZ_DB1),
                json!("20261228"),
                json!(12.5),
                json!(1.8),
                json!(2.4),
                json!(0.5),
                json!(25000.0),
                json!(18000.0),
                json!(15000.0),
                json!(950000.0),
                json!(700000.0),
            ]],
        },
    )])
    .await;
    let client = client_for(&mock.base_url);

    let n = sync::sync_daily_basic(
        &pool,
        &client,
        &[ZZZ_DB1.to_string()],
        "20261228",
        "20261230",
        "zzz-test-sync-db1",
    )
    .await
    .expect("逐只路径应成功");

    assert_eq!(n, 1, "mock 单行估值数据");

    // 请求形态：ts_code + 全窗口 start/end + 分页参数
    let reqs = mock.requests_for("daily_basic");
    assert_eq!(reqs.len(), 1, "单页即停");
    assert_eq!(reqs[0].params.get("ts_code"), Some(&json!(ZZZ_DB1)));
    assert_eq!(reqs[0].params.get("start_date"), Some(&json!("20261228")));
    assert_eq!(reqs[0].params.get("end_date"), Some(&json!("20261230")));
    assert_eq!(reqs[0].params.get("limit"), Some(&json!("6000")));
    assert_eq!(reqs[0].params.get("offset"), Some(&json!("0")));

    // 落库：估值字段映射 + data_version_id 关联
    let row: (Option<Decimal>, Option<Decimal>, Option<Decimal>, String) = sqlx::query_as(
        "SELECT pe_ttm, circ_mv, pb, data_version_id FROM market_stock_daily_basic \
         WHERE symbol = $1 AND trade_date = '2026-12-28'",
    )
    .bind(ZZZ_DB1)
    .fetch_one(&pool)
    .await
    .expect("估值行应落库");
    assert_dec_close(row.0.expect("pe_ttm"), 12.5, "daily_basic pe_ttm");
    assert_dec_close(row.1.expect("circ_mv"), 700000.0, "daily_basic circ_mv");
    assert_dec_close(row.2.expect("pb"), 1.8, "daily_basic pb");
    assert_eq!(row.3, "zzz-test-sync-db1");

    // 任务终态：逐只路径 total/ok 按 symbol 数计
    assert_eq!(
        task_state(&pool, "zzz-test-sync-db1").await,
        ("completed".to_string(), 1, 1, 0)
    );

    mock.shutdown();
    cleanup_zzz_tables(&pool, &[ZZZ_DB1]).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-db1").await;
}

#[tokio::test]
async fn sync_daily_basic_monthly_market_path_writes_rows_and_completes() {
    let pool = local_pool().await;
    cleanup_zzz_tables(&pool, &[ZZZ_DB2]).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-db2").await;

    // 全市场（symbols 空）按月循环：窗口 2026-12-01~03 恰一个月块；
    // 两行 < page_limit 6000 → 单页停止
    let valuation_row = |day: &str| {
        vec![
            json!(ZZZ_DB2),
            json!(day),
            json!(10.0),
            json!(1.0),
            json!(1.5),
            json!(0.3),
            json!(1000.0),
            json!(800.0),
            json!(600.0),
            json!(50000.0),
            json!(40000.0),
        ]
    };
    let mock = spawn_mock_tushare(vec![(
        "daily_basic",
        MockResponse::Paged {
            fields: DAILY_BASIC_FIELDS.to_vec(),
            items: vec![valuation_row("20261201"), valuation_row("20261202")],
            page_size: 6000,
        },
    )])
    .await;
    let client = client_for(&mock.base_url);

    let n = sync::sync_daily_basic(
        &pool,
        &client,
        &[],
        "20261201",
        "20261203",
        "zzz-test-sync-db2",
    )
    .await
    .expect("全市场月度路径应成功");

    assert_eq!(n, 2, "两行估值数据");

    // 请求形态：月块边界 start=12-01 / end=12-03（无 ts_code）
    let reqs = mock.requests_for("daily_basic");
    assert_eq!(reqs.len(), 1, "单页即停");
    assert!(!reqs[0].params.contains_key("ts_code"));
    assert_eq!(reqs[0].params.get("start_date"), Some(&json!("20261201")));
    assert_eq!(reqs[0].params.get("end_date"), Some(&json!("20261203")));

    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM market_stock_daily_basic WHERE symbol = $1")
            .bind(ZZZ_DB2)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 2);

    // 任务终态：月度路径 total/ok 按月块数计（窗口 1 个月 → 1/1）
    assert_eq!(
        task_state(&pool, "zzz-test-sync-db2").await,
        ("completed".to_string(), 1, 1, 0)
    );

    mock.shutdown();
    cleanup_zzz_tables(&pool, &[ZZZ_DB2]).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-db2").await;
}

#[tokio::test]
async fn sync_daily_basic_hourly_limit_error_aborts_as_partial() {
    let pool = local_pool().await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-db3").await;

    let mock = spawn_mock_tushare(vec![(
        "daily_basic",
        MockResponse::ApiErr {
            code: 40203,
            msg: "每小时最多访问该接口4000次".to_string(),
        },
    )])
    .await;
    let client = client_for(&mock.base_url);

    let result = sync::sync_daily_basic(
        &pool,
        &client,
        &[],
        "20261201",
        "20261203",
        "zzz-test-sync-db3",
    )
    .await;

    let err = result.expect_err("限流错误应中断返回 Err");
    assert!(
        err.to_string().contains("rate limited"),
        "错误应标记限流语义，实际: {}",
        err
    );

    // 任务先落 partial 再返回（total=月块数1, ok=0, failed=1）
    assert_eq!(
        task_state(&pool, "zzz-test-sync-db3").await,
        ("partial".to_string(), 1, 0, 1)
    );

    // 零写入断言：按本测试独占 dv_id 精确计数
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM market_stock_daily_basic WHERE data_version_id = $1",
    )
    .bind("zzz-test-sync-db3")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, 0, "限流中断不应写入估值行");

    mock.shutdown();
    cleanup_task_and_dv(&pool, "zzz-test-sync-db3").await;
}

#[tokio::test]
async fn sync_daily_basic_daily_fallback_accumulates_failures() {
    let pool = local_pool().await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-db4").await;

    // 非限流错误（40101 权限）：月度调用失败 → 逐日回退，回退也全失败
    //（mock 按 api_name 路由，月度/日级拿到同一错误响应）
    let mock = spawn_mock_tushare(vec![(
        "daily_basic",
        MockResponse::ApiErr {
            code: 40101,
            msg: "抱歉，您没有访问该接口的权限".to_string(),
        },
    )])
    .await;
    let client = client_for(&mock.base_url);

    let n = sync::sync_daily_basic(
        &pool,
        &client,
        &[],
        "20261201",
        "20261203",
        "zzz-test-sync-db4",
    )
    .await
    .expect("非限流错误不中断整体，走完回退后正常收尾");

    assert_eq!(n, 0, "无任何成功行");

    // 请求形态：1 次月度（start/end）+ 3 次日级回退（trade_date）
    let reqs = mock.requests_for("daily_basic");
    assert_eq!(reqs.len(), 4, "月度 1 次 + 日级 3 次");
    let day_reqs: Vec<&Value> = reqs
        .iter()
        .filter_map(|r| r.params.get("trade_date"))
        .collect();
    assert_eq!(day_reqs.len(), 3);
    assert!(day_reqs.contains(&&json!("20261201")));
    assert!(day_reqs.contains(&&json!("20261202")));
    assert!(day_reqs.contains(&&json!("20261203")));

    // 回退月块收尾：ok=1（月块计 1）、failed=3（每日累计）
    assert_eq!(
        task_state(&pool, "zzz-test-sync-db4").await,
        ("partial".to_string(), 1, 1, 3)
    );

    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM market_stock_daily_basic WHERE data_version_id = $1",
    )
    .bind("zzz-test-sync-db4")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, 0);

    mock.shutdown();
    cleanup_task_and_dv(&pool, "zzz-test-sync-db4").await;
}

#[tokio::test]
async fn sync_daily_basic_checkpoint_heartbeats_every_50_symbols() {
    let pool = local_pool().await;
    cleanup_zzz_tables(&pool, &[ZZZ_DB5]).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-db5").await;

    let mock = spawn_mock_tushare(vec![(
        "daily_basic",
        MockResponse::Rows {
            fields: DAILY_BASIC_FIELDS.to_vec(),
            items: vec![vec![
                json!(ZZZ_DB5),
                json!("20261228"),
                json!(10.0),
                json!(1.0),
                json!(1.0),
                json!(0.1),
                json!(100.0),
                json!(80.0),
                json!(60.0),
                json!(1000.0),
                json!(800.0),
            ]],
        },
    )])
    .await;
    let client = client_for(&mock.base_url);

    // 51 只：i=50 时命中 checkpoint（50 的倍数）→ 心跳分支；
    // i=0 命中早退分支（不查库直接 false）
    let symbols = vec![ZZZ_DB5.to_string(); 51];
    let n = sync::sync_daily_basic(
        &pool,
        &client,
        &symbols,
        "20261228",
        "20261230",
        "zzz-test-sync-db5",
    )
    .await
    .expect("51 只逐只同步应成功");

    assert_eq!(n, 51, "每只各拿到 1 行 mock 数据");
    assert_eq!(mock.requests_for("daily_basic").len(), 51);

    // 同 (symbol, trade_date) 幂等 → 落库仅 1 行
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM market_stock_daily_basic WHERE symbol = $1")
            .bind(ZZZ_DB5)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 1);

    assert_eq!(
        task_state(&pool, "zzz-test-sync-db5").await,
        ("completed".to_string(), 51, 51, 0)
    );

    mock.shutdown();
    cleanup_zzz_tables(&pool, &[ZZZ_DB5]).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-db5").await;
}

// ─── sync_fund_basic ─────────────────────────────────────────────

#[tokio::test]
async fn sync_fund_basic_upserts_stock_and_fund_skips_delisted() {
    let pool = local_pool().await;
    cleanup_zzz_tables(&pool, &[ZZZ_FB1, ZZZ_FB2]).await;

    let mock = spawn_mock_tushare(vec![(
        "fund_basic",
        MockResponse::Rows {
            fields: vec![
                "ts_code",
                "name",
                "management",
                "custodian",
                "fund_type",
                "found_date",
                "due_date",
                "list_date",
                "issue_date",
                "delist_date",
                "issue_amount",
                "m_fee",
                "c_fee",
                "duration_year",
                "p_value",
                "min_amount",
                "exp_return",
                "benchmark",
                "status",
                "invest_type",
                "type",
                "trustee",
                "purc_startdate",
                "redm_startdate",
                "market",
            ],
            items: vec![
                vec![
                    json!(ZZZ_FB1),
                    json!("测试同步ETF"),
                    json!("测试基金公司"),
                    json!("测试托管行"),
                    json!("ETF"),
                    json!("20250101"),
                    json!(""),
                    json!("20260105"),
                    json!(""),
                    json!(""),
                    json!(150000000.0),
                    json!(0.5),
                    json!(0.6),
                    json!(3.5),
                    json!(1.0),
                    json!(1000.0),
                    json!(""),
                    json!(""),
                    json!("O"),
                    json!("被动指数型"),
                    json!("契约型开放式"),
                    json!(""),
                    json!("20260110"),
                    json!("20260111"),
                    json!("E"),
                ],
                vec![
                    json!(ZZZ_FB2),
                    json!("退市测试ETF"),
                    json!("x"),
                    json!("x"),
                    json!("ETF"),
                    json!(""),
                    json!(""),
                    json!(""),
                    json!(""),
                    json!(""),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    json!(""),
                    json!(""),
                    json!("D"),
                    json!(""),
                    json!(""),
                    json!(""),
                    json!(""),
                    json!(""),
                    json!("E"),
                ],
            ],
        },
    )])
    .await;
    let client = client_for(&mock.base_url);

    let n = sync::sync_fund_basic(&pool, &client)
        .await
        .expect("sync_fund_basic 应成功");

    // markets = [E, L] 两轮循环，每轮 1 有效行（D 行跳过）→ total=2
    assert_eq!(n, 2, "两市场循环累计，退市行跳过");

    let reqs = mock.requests_for("fund_basic");
    assert_eq!(reqs.len(), 2, "E + L 各一次");
    let markets: Vec<&Value> = reqs
        .iter()
        .map(|r| r.params.get("market").unwrap())
        .collect();
    assert!(markets.contains(&&json!("E")));
    assert!(markets.contains(&&json!("L")));

    // market_stock：instrument_type=etf；market 终值是循环最后一档 L
    let stock: (String, String, Option<NaiveDate>) =
        sqlx::query_as("SELECT name, market, list_date FROM market_stock WHERE symbol = $1")
            .bind(ZZZ_FB1)
            .fetch_one(&pool)
            .await
            .expect("market_stock 应有行");
    assert_eq!(stock.0, "测试同步ETF");
    assert_eq!(stock.1, "L", "市场循环终值为 L");
    assert_eq!(stock.2, Some(d(2026, 1, 5)));

    // market_fund：24 字段全量 upsert 的代表列
    let fund: (String, String, NaiveDate) = sqlx::query_as(
        "SELECT fund_type, management, list_date FROM market_fund WHERE symbol = $1",
    )
    .bind(ZZZ_FB1)
    .fetch_one(&pool)
    .await
    .expect("market_fund 应有行");
    assert_eq!(fund.0, "ETF");
    assert_eq!(fund.1, "测试基金公司");
    assert_eq!(fund.2, d(2026, 1, 5));

    // status=D 行两表都不落
    let (s, f): (i64, i64) = sqlx::query_as(
        "SELECT (SELECT COUNT(*) FROM market_stock WHERE symbol = $1), \
                (SELECT COUNT(*) FROM market_fund WHERE symbol = $1)",
    )
    .bind(ZZZ_FB2)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!((s, f), (0, 0), "退市基金应被跳过");

    mock.shutdown();
    cleanup_zzz_tables(&pool, &[ZZZ_FB1, ZZZ_FB2]).await;
}

// ─── sync_fund_nav ───────────────────────────────────────────────

#[tokio::test]
async fn sync_fund_nav_success_failure_and_up_to_date_scenarios() {
    let pool = local_pool().await;
    cleanup_zzz_tables(&pool, &[ZZZ_FN1, ZZZ_FN2, ZZZ_FN3]).await;
    cleanup_attempt(&pool, ZZZ_FN1).await;
    cleanup_attempt(&pool, ZZZ_FN2).await;
    cleanup_attempt(&pool, ZZZ_FN3).await;
    // task_id 按运行日生成，会覆盖当天真实 scheduler 同名任务行 → 结尾删除
    let today = chrono::Local::now().date_naive();
    let nav_task = format!("fund-nav-sync-{}", today.format("%Y%m%d"));
    cleanup_task(&pool, &nav_task).await;
    let yesterday = (today - chrono::Duration::days(1))
        .format("%Y%m%d")
        .to_string();

    // 场景 A：成功——unit_nav 必填；ann_date 是 ISO 格式（FromStr 要求 YYYY-MM-DD）
    let mock = spawn_mock_tushare(vec![(
        "fund_nav",
        MockResponse::Rows {
            fields: vec![
                "ts_code",
                "ann_date",
                "nav_date",
                "unit_nav",
                "accum_nav",
                "adj_nav",
            ],
            items: vec![vec![
                json!(ZZZ_FN1),
                json!("2026-09-15"),
                json!(yesterday),
                json!(1.234),
                json!(2.5),
                json!(3.5),
            ]],
        },
    )])
    .await;
    let client = client_for(&mock.base_url);
    let n = sync::sync_fund_nav(&pool, &client, &[ZZZ_FN1.to_string()])
        .await
        .expect("场景A应成功");
    assert_eq!(n, 1);
    mock.shutdown();

    let nav: (Decimal, Option<NaiveDate>, Option<Decimal>, String) = sqlx::query_as(
        "SELECT unit_nav, ann_date, accum_nav, source FROM market_fund_nav WHERE symbol = $1",
    )
    .bind(ZZZ_FN1)
    .fetch_one(&pool)
    .await
    .expect("净值行应落库");
    assert_dec_close(nav.0, 1.234, "fund_nav unit_nav");
    assert_eq!(nav.1, Some(d(2026, 9, 15)), "ISO ann_date → NaiveDate");
    assert_dec_close(nav.2.expect("accum_nav"), 2.5, "fund_nav accum_nav");
    assert_eq!(nav.3, "tushare");

    let attempt: (String, i64) = sqlx::query_as(
        "SELECT status, row_count FROM data_sync_attempt \
         WHERE symbol = $1 AND source = 'fund_nav' AND start_date < end_date",
    )
    .bind(ZZZ_FN1)
    .fetch_one(&pool)
    .await
    .expect("区间级 attempt 应记录");
    assert_eq!(attempt.0, "completed");
    assert_eq!(attempt.1, 1);

    // 场景 B：单标的失败（非限流错误）→ attempt failed，不中断
    let mock_b = spawn_mock_tushare(vec![(
        "fund_nav",
        MockResponse::ApiErr {
            code: 40101,
            msg: "抱歉，您没有访问该接口的权限".to_string(),
        },
    )])
    .await;
    let client_b = client_for(&mock_b.base_url);
    let n = sync::sync_fund_nav(&pool, &client_b, &[ZZZ_FN2.to_string()])
        .await
        .expect("单标的失败不中断整体");
    assert_eq!(n, 0, "失败标的不计行");
    mock_b.shutdown();

    let fail_attempt: (String, Option<String>) = sqlx::query_as(
        "SELECT status, error_message FROM data_sync_attempt \
         WHERE symbol = $1 AND source = 'fund_nav'",
    )
    .bind(ZZZ_FN2)
    .fetch_one(&pool)
    .await
    .expect("失败 attempt 应记录");
    assert_eq!(fail_attempt.0, "failed");
    assert!(
        fail_attempt.1.as_deref().unwrap_or("").contains("40101"),
        "错误消息应保留上游 code，实际 {:?}",
        fail_attempt.1
    );

    // 场景 C：净值已是最新（max(nav_date)=今天）→ 增量起点溢出窗口，跳过
    sqlx::query(
        "INSERT INTO market_fund_nav (symbol, nav_date, unit_nav, source) VALUES ($1, $2, 1.0, 'tushare')",
    )
    .bind(ZZZ_FN3)
    .bind(today)
    .execute(&pool)
    .await
    .unwrap();
    let mock_c = spawn_mock_tushare(vec![(
        "fund_nav",
        MockResponse::Rows {
            fields: vec!["ts_code", "ann_date", "nav_date", "unit_nav"],
            items: vec![],
        },
    )])
    .await;
    let client_c = client_for(&mock_c.base_url);
    let n = sync::sync_fund_nav(&pool, &client_c, &[ZZZ_FN3.to_string()])
        .await
        .expect("最新跳过路径应成功");
    assert_eq!(n, 0);
    assert_eq!(
        mock_c.requests_for("fund_nav").len(),
        0,
        "起点>今天直接 continue，不发请求"
    );
    mock_c.shutdown();

    let cnt: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM data_sync_attempt WHERE symbol = $1 AND source = 'fund_nav'",
    )
    .bind(ZZZ_FN3)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(cnt, 0, "跳过标的不记 attempt");

    // 整体任务终态 completed
    let status: String = sqlx::query_scalar("SELECT status FROM data_sync_task WHERE task_id = $1")
        .bind(&nav_task)
        .fetch_one(&pool)
        .await
        .expect("任务行应存在");
    assert_eq!(status, "completed");

    cleanup_zzz_tables(&pool, &[ZZZ_FN1, ZZZ_FN2, ZZZ_FN3]).await;
    cleanup_attempt(&pool, ZZZ_FN1).await;
    cleanup_attempt(&pool, ZZZ_FN2).await;
    cleanup_attempt(&pool, ZZZ_FN3).await;
    cleanup_task(&pool, &nav_task).await;
}

// ─── sync_fund_adj ───────────────────────────────────────────────

#[tokio::test]
async fn sync_fund_adj_writes_factors_and_completes_task() {
    let pool = local_pool().await;
    cleanup_zzz_tables(&pool, &[ZZZ_FA1]).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-fadj1").await;

    let mock = spawn_mock_tushare(vec![(
        "fund_adj",
        MockResponse::Rows {
            fields: vec!["trade_date", "adj_factor"],
            items: vec![vec![json!("20261228"), json!(1.5)]],
        },
    )])
    .await;
    let client = client_for(&mock.base_url);

    let n = sync::sync_fund_adj(
        &pool,
        &client,
        &[ZZZ_FA1.to_string()],
        "20261228",
        "20261230",
        "zzz-test-sync-fadj1",
    )
    .await
    .expect("sync_fund_adj 应成功");

    assert_eq!(n, 1, "ok 计数 = 有因子的标的数");

    let factor: (NaiveDate, Decimal) = sqlx::query_as(
        "SELECT trade_date, adj_factor FROM market_adjustment_factor WHERE symbol = $1",
    )
    .bind(ZZZ_FA1)
    .fetch_one(&pool)
    .await
    .expect("复权因子行应落库");
    assert_eq!(factor.0, d(2026, 12, 28));
    assert_dec_close(factor.1, 1.5, "fund_adj adj_factor");

    let reqs = mock.requests_for("fund_adj");
    assert_eq!(reqs.len(), 1);
    assert_eq!(reqs[0].params.get("ts_code"), Some(&json!(ZZZ_FA1)));
    assert_eq!(reqs[0].params.get("start_date"), Some(&json!("20261228")));
    assert_eq!(reqs[0].params.get("end_date"), Some(&json!("20261230")));

    assert_eq!(
        task_state(&pool, "zzz-test-sync-fadj1").await,
        ("completed".to_string(), 1, 1, 0)
    );

    mock.shutdown();
    cleanup_zzz_tables(&pool, &[ZZZ_FA1]).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-fadj1").await;
}

#[tokio::test]
async fn sync_fund_adj_marks_partial_on_upstream_error() {
    let pool = local_pool().await;
    cleanup_zzz_tables(&pool, &[ZZZ_FA2]).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-fadj2").await;

    let mock = spawn_mock_tushare(vec![(
        "fund_adj",
        MockResponse::ApiErr {
            code: 40101,
            msg: "抱歉，您没有访问该接口的权限".to_string(),
        },
    )])
    .await;
    let client = client_for(&mock.base_url);

    // 上游失败不返回 Err：fail 计数 → partial
    let n = sync::sync_fund_adj(
        &pool,
        &client,
        &[ZZZ_FA2.to_string()],
        "20261228",
        "20261230",
        "zzz-test-sync-fadj2",
    )
    .await
    .expect("单标的失败不应中断");
    assert_eq!(n, 0);

    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM market_adjustment_factor WHERE symbol = $1")
            .bind(ZZZ_FA2)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 0, "失败标的零写入");

    assert_eq!(
        task_state(&pool, "zzz-test-sync-fadj2").await,
        ("partial".to_string(), 1, 0, 1)
    );

    mock.shutdown();
    cleanup_zzz_tables(&pool, &[ZZZ_FA2]).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-fadj2").await;
}

#[tokio::test]
async fn sync_fund_adj_stops_when_cancel_requested() {
    let pool = local_pool().await;
    cleanup_zzz_tables(&pool, &[ZZZ_FA3]).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-fadj3").await;

    // SlowOk 让第一只耗时 500ms；后台延迟 150ms 再置 cancel_requested，
    // 确保第一只的取消检查（开跑后 ~10ms 内）已通过、置位落在 SlowOk
    // 窗口内 → 第二只循环开头查询到取消 → break → 任务终态 cancelled
    let mock = spawn_mock_tushare(vec![("fund_adj", MockResponse::SlowOk { delay_ms: 500 })]).await;
    let client = client_for(&mock.base_url);

    let task_id = "zzz-test-sync-fadj3".to_string();
    let canceller = {
        let pool = pool.clone();
        let task_id = task_id.clone();
        tokio::spawn(async move {
            // 延迟启动避开第一只的取消检查，再轮询直到 task 行被置为 running 后置位
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            for _ in 0..20 {
                let n = sqlx::query(
                    "UPDATE data_sync_task SET status = 'cancel_requested' \
                     WHERE task_id = $1 AND status = 'running'",
                )
                .bind(&task_id)
                .execute(&pool)
                .await
                .map(|r| r.rows_affected())
                .unwrap_or(0);
                if n > 0 {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        })
    };

    let n = sync::sync_fund_adj(
        &pool,
        &client,
        &[ZZZ_FA3.to_string(), "ZZZSYNC45B.SH".to_string()],
        "20261228",
        "20261230",
        &task_id,
    )
    .await
    .expect("取消路径应正常收尾");
    canceller.await.expect("canceller join");

    assert_eq!(n, 0, "SlowOk 空数据不计 ok");
    assert_eq!(
        mock.requests_for("fund_adj").len(),
        1,
        "第二只在取消检查处 break"
    );

    assert_eq!(
        task_state(&pool, "zzz-test-sync-fadj3").await,
        ("cancelled".to_string(), 2, 0, 0)
    );

    mock.shutdown();
    cleanup_zzz_tables(&pool, &[ZZZ_FA3]).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-fadj3").await;
}

// ─── sync_index_daily ────────────────────────────────────────────

#[tokio::test]
async fn sync_index_daily_writes_bars_and_completes_task() {
    let pool = local_pool().await;
    cleanup_zzz_tables(&pool, &[ZZZ_IX1]).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-idx1").await;

    let mock = spawn_mock_tushare(vec![(
        "index_daily",
        MockResponse::Rows {
            fields: vec![
                "trade_date",
                "open",
                "high",
                "low",
                "close",
                "pre_close",
                "pct_chg",
                "vol",
                "amount",
            ],
            items: vec![vec![
                json!("20261228"),
                json!(3900.5),
                json!(3950.0),
                json!(3880.0),
                json!(3920.8),
                json!(3899.0),
                json!(0.56),
                json!(123000.0),
                json!(481500.0),
            ]],
        },
    )])
    .await;
    let client = client_for(&mock.base_url);

    let n = sync::sync_index_daily(
        &pool,
        &client,
        &[ZZZ_IX1.to_string()],
        "20261228",
        "20261230",
        "zzz-test-sync-idx1",
    )
    .await
    .expect("sync_index_daily 应成功");

    assert_eq!(n, 1);

    let bar: (Decimal, Decimal, String) = sqlx::query_as(
        "SELECT close, pct_change, data_version_id FROM market_index_daily_bar WHERE symbol = $1",
    )
    .bind(ZZZ_IX1)
    .fetch_one(&pool)
    .await
    .expect("指数日线应落库");
    assert_dec_close(bar.0, 3920.8, "index_daily close");
    assert_dec_close(bar.1, 0.0056, "pct_chg 百分比 → 小数");
    assert_eq!(bar.2, "zzz-test-sync-idx1");

    let reqs = mock.requests_for("index_daily");
    assert_eq!(reqs.len(), 1);
    assert_eq!(reqs[0].params.get("ts_code"), Some(&json!(ZZZ_IX1)));
    assert_eq!(reqs[0].params.get("start_date"), Some(&json!("20261228")));
    assert_eq!(reqs[0].params.get("end_date"), Some(&json!("20261230")));

    assert_eq!(
        task_state(&pool, "zzz-test-sync-idx1").await,
        ("completed".to_string(), 1, 1, 0)
    );

    mock.shutdown();
    cleanup_zzz_tables(&pool, &[ZZZ_IX1]).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-idx1").await;
}

#[tokio::test]
async fn sync_index_daily_marks_partial_on_upstream_error() {
    let pool = local_pool().await;
    cleanup_zzz_tables(&pool, &[ZZZ_IX2]).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-idx2").await;

    let mock = spawn_mock_tushare(vec![(
        "index_daily",
        MockResponse::ApiErr {
            code: 40101,
            msg: "抱歉，您没有访问该接口的权限".to_string(),
        },
    )])
    .await;
    let client = client_for(&mock.base_url);

    let n = sync::sync_index_daily(
        &pool,
        &client,
        &[ZZZ_IX2.to_string()],
        "20261228",
        "20261230",
        "zzz-test-sync-idx2",
    )
    .await
    .expect("上游失败不中断");
    assert_eq!(n, 0);

    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM market_index_daily_bar WHERE symbol = $1")
            .bind(ZZZ_IX2)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 0);

    assert_eq!(
        task_state(&pool, "zzz-test-sync-idx2").await,
        ("partial".to_string(), 1, 0, 1)
    );

    mock.shutdown();
    cleanup_zzz_tables(&pool, &[ZZZ_IX2]).await;
    cleanup_task_and_dv(&pool, "zzz-test-sync-idx2").await;
}

// ─── run_quality_check ───────────────────────────────────────────

#[tokio::test]
async fn run_quality_check_scores_completeness_with_outliers() {
    let pool = local_pool().await;
    cleanup_zzz_tables(&pool, &[ZZZ_QC]).await;

    // 真实开市周 2026-09-14~18（5 天）：写满 5 行，其中 1 行 close=0 是异常值
    // score = 100 - 1/5*30 = 94
    for (i, day) in ["20260914", "20260915", "20260916", "20260917", "20260918"]
        .iter()
        .enumerate()
    {
        let close = if i == 2 { 0.0 } else { 10.5 };
        sqlx::query(
            "INSERT INTO market_stock_daily_bar \
             (symbol, trade_date, open, high, low, close, pre_close, volume, amount, source, created_at) \
             VALUES ($1, $2, 10.0, 11.0, 9.5, $3, 10.4, 1000.0, 10500.0, 'zzz-test', now()) \
             ON CONFLICT (symbol, trade_date) DO NOTHING",
        )
        .bind(ZZZ_QC)
        .bind(NaiveDate::parse_from_str(day, "%Y%m%d").unwrap())
        .bind(dec(close))
        .execute(&pool)
        .await
        .unwrap();
    }

    let report = sync::run_quality_check(&pool, &[ZZZ_QC.to_string()], "20260914", "20260918")
        .await
        .expect("质量检查应成功");

    assert_eq!(
        report["expected_records"].as_i64(),
        Some(5),
        "5 开市日 × 1 标的"
    );
    assert_eq!(report["actual_records"].as_i64(), Some(5));
    assert_eq!(report["missing"].as_i64(), Some(0));
    assert_eq!(report["duplicates"].as_i64(), Some(0));
    assert_eq!(report["outliers"].as_i64(), Some(1), "close=0 计异常");
    let score = report["quality_score"].as_f64().expect("score 数值");
    assert!(
        (score - 94.0).abs() < 0.01,
        "100 - 30%*1/5 = 94，实际 {}",
        score
    );

    // 检查结果落库（按返回 check_id 精确校验并清理）
    let check_id = report["check_id"].as_str().expect("check_id").to_string();
    let persisted: (i64, i64) = sqlx::query_as(
        "SELECT total_records, outlier_count FROM data_quality_check WHERE check_id = $1",
    )
    .bind(&check_id)
    .fetch_one(&pool)
    .await
    .expect("检查行应落库");
    assert_eq!(persisted, (5, 1));

    let _ = sqlx::query("DELETE FROM data_quality_check WHERE check_id = $1")
        .bind(&check_id)
        .execute(&pool)
        .await;
    cleanup_zzz_tables(&pool, &[ZZZ_QC]).await;
}

// ─── derive_limit_list_from_daily_bars ───────────────────────────

/// 直插 market_stock 基础行（derive 的 stock_profile CTE 所需）
async fn insert_stock_for_derive(pool: &PgPool, symbol: &str, list_date: &str) {
    sqlx::query(
        "INSERT INTO market_stock (symbol, name, exchange, market, list_status, list_date) \
         VALUES ($1, '测试派生股', $2, '主板', 'L', $3) \
         ON CONFLICT (symbol) DO UPDATE SET list_date = EXCLUDED.list_date",
    )
    .bind(symbol)
    .bind(if symbol.ends_with(".SH") {
        "SSE"
    } else {
        "SZSE"
    })
    .bind(NaiveDate::parse_from_str(list_date, "%Y%m%d").unwrap())
    .execute(pool)
    .await
    .unwrap();
}

/// 直插日线行（derive 的 bars CTE 所需）
async fn insert_bar_for_derive(pool: &PgPool, symbol: &str, day: &str, close: f64, pre_close: f64) {
    sqlx::query(
        "INSERT INTO market_stock_daily_bar \
         (symbol, trade_date, open, high, low, close, pre_close, volume, amount, source, created_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, 1000.0, 10500.0, 'zzz-test', now()) \
         ON CONFLICT (symbol, trade_date) DO NOTHING",
    )
    .bind(symbol)
    .bind(NaiveDate::parse_from_str(day, "%Y%m%d").unwrap())
    .bind(dec(close * 0.98))
    .bind(dec(close * 1.01))
    .bind(dec(pre_close * 0.97))
    .bind(dec(close))
    .bind(dec(pre_close))
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn derive_limit_list_classifies_u_d_st_band_and_filters_recent_listing() {
    let pool = local_pool().await;
    cleanup_zzz_tables(&pool, &[ZZZ_DL1, ZZZ_DL2, ZZZ_DL3]).await;
    cleanup_derive_tasks(&pool).await;

    // 阶段一（非 ST 10% 板 + 次新过滤）：老股 U/D/无三天 + 次新股被过滤
    insert_stock_for_derive(&pool, ZZZ_DL1, "20000101").await;
    insert_bar_for_derive(&pool, ZZZ_DL1, "20261228", 11.0, 10.0).await; // +10% → U
    insert_bar_for_derive(&pool, ZZZ_DL1, "20261229", 9.0, 10.0).await; // -10% → D
    insert_bar_for_derive(&pool, ZZZ_DL1, "20261230", 10.0, 10.0).await; // 平盘 → 无

    // 次新股：list_date=2026-12-22（周二），12-28 恰第 5 个开市日(22,23,24,25,28)。
    // 实现口径 trade_seq - list_seq + 1 > 5 才判定（第 6 开市日起），第 5 日过滤 ✓。
    // （原注释误算 12-21 起数——25 日周五也开市，28 日实为第 6 日不被过滤）
    insert_stock_for_derive(&pool, ZZZ_DL2, "20261222").await;
    insert_bar_for_derive(&pool, ZZZ_DL2, "20261228", 11.0, 10.0).await; // 本应 U，被过滤

    let inserted = sync::derive_limit_list_from_daily_bars(&pool, "20261228", "20261230")
        .await
        .expect("派生涨跌停应成功");
    assert_eq!(inserted, 2, "老股 U+D 两行，次新股被过滤");

    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT trade_date::text, limit_type FROM market_stock_limit \
         WHERE symbol = $1 ORDER BY trade_date",
    )
    .bind(ZZZ_DL1)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], ("2026-12-28".to_string(), "U".to_string()));
    assert_eq!(rows[1], ("2026-12-29".to_string(), "D".to_string()));

    let recent: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM market_stock_limit WHERE symbol = $1")
            .bind(ZZZ_DL2)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(recent, 0, "上市 5 日内的涨停不落库");

    // 幂等：重跑行数不增
    let inserted_again = sync::derive_limit_list_from_daily_bars(&pool, "20261228", "20261230")
        .await
        .expect("二次派生应成功");
    assert_eq!(inserted_again, 2, "upsert 幂等，仍是 2 行（覆盖计数）");

    // 阶段二（ST 5% 板）：ST 区间开到无穷 → 窗口内按 5% 板判定。
    // +5% 收 10.5：5% 板 U 阈 10.499 → U；若误按 10% 板（阈 10.999）则不落库
    // → 有行即证明 ST 阈值生效（本阶段与阶段一同窗口串行，避免并行 inserted 计数互踩）
    insert_stock_for_derive(&pool, ZZZ_DL3, "20000101").await;
    sqlx::query(
        "INSERT INTO market_stock_name_history (symbol, name, start_date, end_date, is_st) \
         VALUES ($1, 'ST测试派生', '2026-01-01', NULL, true)",
    )
    .bind(ZZZ_DL3)
    .execute(&pool)
    .await
    .unwrap();
    insert_bar_for_derive(&pool, ZZZ_DL3, "20261228", 10.5, 10.0).await;
    insert_bar_for_derive(&pool, ZZZ_DL3, "20261229", 10.96, 10.0).await; // 远超阈值的伴随 U

    let inserted_st = sync::derive_limit_list_from_daily_bars(&pool, "20261228", "20261230")
        .await
        .expect("ST 派生应成功");
    assert_eq!(inserted_st, 4, "本轮 upsert 命中 4 行（老股 2 + ST 股 2）");

    let st_rows: Vec<String> = sqlx::query_scalar(
        "SELECT limit_type FROM market_stock_limit WHERE symbol = $1 ORDER BY trade_date",
    )
    .bind(ZZZ_DL3)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        st_rows,
        vec!["U".to_string(), "U".to_string()],
        "5% 板两行均 U"
    );

    // 完成标记任务补齐（source=derived:daily_limit）
    for day in ["20261228", "20261229", "20261230"] {
        let task_id = format!("limit_daily-{}", day);
        let (status, source): (String, String) =
            sqlx::query_as("SELECT status, source FROM data_sync_task WHERE task_id = $1")
                .bind(&task_id)
                .fetch_one(&pool)
                .await
                .unwrap_or_else(|_| panic!("完成标记 {} 应存在", task_id));
        assert_eq!(status, "completed");
        assert_eq!(source, "derived:daily_limit");
    }

    cleanup_zzz_tables(&pool, &[ZZZ_DL1, ZZZ_DL2, ZZZ_DL3]).await;
    cleanup_derive_tasks(&pool).await;
}

#[tokio::test]
async fn derive_limit_list_rejects_bad_date_windows() {
    let pool = local_pool().await;

    let err = sync::derive_limit_list_from_daily_bars(&pool, "20261230", "20261228")
        .await
        .expect_err("倒置窗口应报错");
    assert!(err.contains("不能晚于"), "实际: {}", err);

    let err = sync::derive_limit_list_from_daily_bars(&pool, "bad-date", "20261228")
        .await
        .expect_err("非法日期应报错");
    assert!(err.contains("解析"), "实际: {}", err);
}

// ─── backfill_limit_completion_markers ───────────────────────────

#[tokio::test]
async fn backfill_limit_markers_writes_daily_completed_tasks() {
    let pool = local_pool().await;
    // 窗口 12-21~24 独立于 derive 测试（12-28~30）：并行时 limit 表行数断言互不污染
    let days = ["20261221", "20261222", "20261223", "20261224"];
    for day in days {
        cleanup_task(&pool, &format!("limit_daily-{}", day)).await;
    }

    // 参数校验分支
    let err =
        sync::backfill_limit_completion_markers(&pool, "20261224", "20261221", "zzz-test-blcm")
            .await
            .expect_err("倒置窗口应报错");
    assert!(err.contains("不能晚于"), "实际: {}", err);

    let n = sync::backfill_limit_completion_markers(&pool, "20261221", "20261224", "zzz-test-blcm")
        .await
        .expect("补齐完成标记应成功");
    assert_eq!(n, 4, "四个开市日各补一条任务");

    // 每个开市日一条 completed 任务，source 透传；limit 表该窗口无行 → total=0
    let rows: Vec<(String, String, i32)> = sqlx::query_as(
        "SELECT task_id, source, total_count FROM data_sync_task \
         WHERE task_id IN ('limit_daily-20261221', 'limit_daily-20261222', \
                           'limit_daily-20261223', 'limit_daily-20261224') \
         ORDER BY task_id",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 4);
    for (task_id, source, total) in &rows {
        assert!(task_id.starts_with("limit_daily-2026122"));
        assert_eq!(source, "zzz-test-blcm");
        assert_eq!(*total, 0, "limit 表该窗口无行 → COALESCE 0");
    }

    for day in days {
        cleanup_task(&pool, &format!("limit_daily-{}", day)).await;
    }
}

// ─── get_st_symbols_at_date / get_pit_main_board_non_st_symbols ──

#[tokio::test]
async fn st_symbol_query_uses_pit_window_semantics() {
    let pool = local_pool().await;
    cleanup_zzz_tables(&pool, &[ZZZ_ST1, ZZZ_ST2, ZZZ_ST3]).await;

    sqlx::query(
        "INSERT INTO market_stock_name_history (symbol, name, start_date, end_date, is_st) VALUES \
         ($1, 'ST闭区间', '2026-01-01', '2026-06-01', true), \
         ($2, 'ST开区间', '2026-05-01', NULL, true), \
         ($3, '非ST行', '2026-01-01', NULL, false)",
    )
    .bind(ZZZ_ST1)
    .bind(ZZZ_ST2)
    .bind(ZZZ_ST3)
    .execute(&pool)
    .await
    .unwrap();

    // 2026-03-01：闭区间命中，开区间未开始
    let syms = sync::get_st_symbols_at_date(&pool, d(2026, 3, 1))
        .await
        .expect("PIT ST 查询应成功");
    assert!(
        syms.contains(&ZZZ_ST1.to_string()),
        "闭区间命中: {:?}",
        syms
    );
    assert!(!syms.contains(&ZZZ_ST2.to_string()));
    assert!(!syms.contains(&ZZZ_ST3.to_string()), "非 ST 行不入选");

    // 2026-07-01：闭区间已结束，开区间仍在
    let syms = sync::get_st_symbols_at_date(&pool, d(2026, 7, 1))
        .await
        .expect("PIT ST 查询 2 应成功");
    assert!(!syms.contains(&ZZZ_ST1.to_string()), "闭区间已出窗");
    assert!(
        syms.contains(&ZZZ_ST2.to_string()),
        "开区间仍在: {:?}",
        syms
    );

    cleanup_zzz_tables(&pool, &[ZZZ_ST1, ZZZ_ST2, ZZZ_ST3]).await;
}

#[tokio::test]
async fn main_board_universe_query_excludes_growth_st_and_unlisted() {
    let pool = local_pool().await;
    let all_keys = [ZZZ_MB1, ZZZ_MB2, ZZZ_GEM, ZZZ_MB_FUT, ZZZ_MB_DLQ];
    cleanup_zzz_tables(&pool, &all_keys).await;

    sqlx::query(
        "INSERT INTO market_stock (symbol, name, exchange, list_status, list_date) VALUES \
         ($1, '主板正常', 'SSE', 'L', '2020-01-01'), \
         ($2, '主板ST', 'SSE', 'L', '2020-01-01'), \
         ($3, '创业板形态', 'SZSE', 'L', '2020-01-01'), \
         ($4, '未上市', 'SSE', 'L', '2027-01-01'), \
         ($5, '已退市', 'SSE', 'D', '2020-01-01')",
    )
    .bind(ZZZ_MB1)
    .bind(ZZZ_MB2)
    .bind(ZZZ_GEM)
    .bind(ZZZ_MB_FUT)
    .bind(ZZZ_MB_DLQ)
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO market_stock_name_history (symbol, name, start_date, end_date, is_st) \
         VALUES ($1, 'ST宇宙股', '2026-01-01', NULL, true)",
    )
    .bind(ZZZ_MB2)
    .execute(&pool)
    .await
    .unwrap();

    let syms = sync::get_pit_main_board_non_st_symbols(&pool, d(2026, 6, 1))
        .await
        .expect("主板 universe 查询应成功");

    // 全市场查询（含真实股票）：只断言自家键的进出，不做全量精确比较
    assert!(
        syms.contains(&ZZZ_MB1.to_string()),
        "主板正常股应入选: {:?}",
        syms.len()
    );
    assert!(!syms.contains(&ZZZ_MB2.to_string()), "ST 股应被排除");
    assert!(!syms.contains(&ZZZ_GEM.to_string()), "创业板形态应被排除");
    assert!(
        !syms.contains(&ZZZ_MB_FUT.to_string()),
        "list_date 晚于 as_of 应被排除"
    );
    assert!(!syms.contains(&ZZZ_MB_DLQ.to_string()), "退市股应被排除");
    // 排序不变式
    let mut sorted = syms.clone();
    sorted.sort();
    assert_eq!(syms, sorted, "结果应按 symbol 有序");

    // as_of 早于 ST 起点 → ST 股恢复入选（PIT 语义）
    let syms = sync::get_pit_main_board_non_st_symbols(&pool, d(2025, 12, 31))
        .await
        .expect("PIT 前置查询应成功");
    assert!(syms.contains(&ZZZ_MB1.to_string()));
    assert!(
        syms.contains(&ZZZ_MB2.to_string()),
        "ST 起点前主板 ST 股应入选"
    );

    cleanup_zzz_tables(&pool, &all_keys).await;
}
