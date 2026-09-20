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
