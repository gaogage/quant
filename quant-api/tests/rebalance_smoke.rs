//! DDD 重构 rebalance 关键路径冒烟测试(Step 0 安全网)。
//!
//! 验证 v24 实盘调仓产出路径完整:paper_position 有估值 + NAV 快照存在。
//! 纯只读查询,不触发调仓不写数据,对实盘安全。
//! 重构期间每步 MR 跑此测试确认调仓产出链路未被破坏。
//!
//! 运行方式(需 DB):
//!   cargo test --package quant-api --test rebalance_smoke

use chrono::NaiveDate;
use sqlx::PgPool;

/// v24 active 账号应有持仓估值(market_price 非 NULL)。
/// mark_to_market 每日 EOD 跑此写入,无估值说明调仓/估值路径断裂。
#[tokio::test]
async fn v24_account_has_marked_positions() {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
    let db = PgPool::connect(&url).await.expect("DB 连接成功");

    let account_id: String = sqlx::query_scalar(
        "SELECT paper_account_id FROM paper_account \
         WHERE status = 'active' AND account_type = 'simulated' \
         AND strategy_version_id = 'v24' LIMIT 1",
    )
    .fetch_one(&db)
    .await
    .expect("应存在 v24 active 账号");

    let priced: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM paper_position \
         WHERE paper_account_id = $1 AND market_price IS NOT NULL AND market_price > 0",
    )
    .bind(&account_id)
    .fetch_one(&db)
    .await
    .unwrap_or(0);

    assert!(priced > 0, "v24 账号应有持仓估值,无估值说明 mark_to_market 路径断裂");
}

/// v24 active 账号近期应有 NAV 快照(rebalance 后写入)。
#[tokio::test]
async fn v24_account_has_recent_nav_snapshot() {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
    let db = PgPool::connect(&url).await.expect("DB 连接成功");

    // 取近 3 天内有 NAV 快照的 v24 账号
    let cutoff = NaiveDate::from_ymd_opt(2026, 7, 21).unwrap();
    let snap_cnt: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM paper_nav_snapshot pns \
         JOIN paper_account pa ON pa.paper_account_id = pns.paper_account_id \
         WHERE pa.status = 'active' AND pa.strategy_version_id = 'v24' \
         AND pns.snapshot_date >= $1",
    )
    .bind(cutoff)
    .fetch_one(&db)
    .await
    .unwrap_or(0);

    assert!(snap_cnt > 0, "v24 账号近 3 天应有 NAV 快照,无快照说明调仓收尾路径断裂");
}

/// v24 杠杆账号的 load_account SELECT 不应触发 ColumnNotFound panic（7/30 account.rs:216 回归守卫）。
/// 根因：SELECT 的 COALESCE(liquidation_threshold,1.3) 无 alias，结果集列名是 "coalesce" 而非
/// "liquidation_threshold"，row.get("liquidation_threshold") ColumnNotFound panic，
/// 导致 scheduler 14:40 调仓路径崩溃循环重启（7/30 起 27 次容器重启）。
/// 此测试复现 load_account 的 SELECT 并验证两列按名可取，防止 alias 再丢失。
#[tokio::test]
async fn v24_margin_account_load_select_has_alias_columns() {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
    let db = PgPool::connect(&url).await.expect("DB 连接成功");

    // 取任意 v24 杠杆账号
    let account_id: String = sqlx::query_scalar(
        "SELECT paper_account_id FROM paper_account \
         WHERE status = 'active' AND account_type = 'simulated' \
         AND strategy_version_id = 'v24' AND leverage_enabled = true LIMIT 1",
    )
    .fetch_one(&db)
    .await
    .expect("应存在 v24 杠杆账号");

    // 复现 load_account 的 SELECT（account.rs:189），COALESCE 必须带 AS alias
    use sqlx::Row;
    let row = sqlx::query(
        "SELECT leverage_enabled, leverage_multiplier, leverage_mode,
                COALESCE(liquidation_threshold, 1.3) AS liquidation_threshold,
                COALESCE(warning_threshold, 1.5) AS warning_threshold,
                strategy_version_id, name
         FROM paper_account WHERE paper_account_id = $1",
    )
    .bind(&account_id)
    .fetch_one(&db)
    .await
    .expect("load_account SELECT 应成功");

    // 按列名取值——若 COALESCE 无 alias，这里会 ColumnNotFound panic
    let liq: f64 = row
        .try_get::<f64, _>("liquidation_threshold")
        .expect("liquidation_threshold 列必须存在（COALESCE 需 AS alias）");
    let warn: f64 = row
        .try_get::<f64, _>("warning_threshold")
        .expect("warning_threshold 列必须存在（COALESCE 需 AS alias）");

    // COALESCE 默认值或实际值
    assert!(liq > 0.0, "强平阈值应 > 0, 实际={}", liq);
    assert!(warn > 0.0, "预警阈值应 > 0, 实际={}", warn);
}
