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
