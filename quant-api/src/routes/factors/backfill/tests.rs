//! phase7 因子回填测试（stale 重算补数工具）。
use super::*;

/// 停更因子族统一重算（2026-09-05，数据已补齐后手动触发研究期管道）：
/// 覆盖估值/行业残差财务/增长恢复/解禁/股本/量价派生五族停更因子。
/// 运行：set -a; source ../.env; source ../.env.quant; set +a;
///       cargo test --release -p quant-api stale_recompute -- --ignored --nocapture
#[tokio::test]
#[ignore = "DB 集成测试(本机 PG),显式跑: cargo test -- --ignored"]
async fn stale_factor_families_recompute() {
    let db = sqlx::PgPool::connect(
        &std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into()),
    )
    .await
    .expect("db");
    let start = NaiveDate::from_ymd_opt(2026, 5, 1).unwrap();
    let end = NaiveDate::from_ymd_opt(2026, 9, 4).unwrap();
    let mk = |task: &'static str,
              src: &'static str,
              bundle: &'static str,
              cat: &'static str,
              phase: &'static str,
              deps: &'static [&'static str],
              method: &'static str| {
        SetBasedFactorBackfillPlan {
            start_date: start,
            end_date: end,
            version: "1.0.0".into(),
            combo_name: format!("{}_v1", bundle),
            statement_timeout_ms: 0,
            task_type: task,
            source: src,
            heartbeat_timeout_seconds: 3600,
            bundle_name: bundle,
            category: cat,
            phase,
            dependencies: deps,
            combo_method: method,
            experiment_type: "stale_recompute_20260905",
            source_combos: vec![],
        }
    };
    // 估值 4 因子（val_pe/pb/ps/dividend_yield）
    let r = run_phase7_valuation_backfill(
        &db,
        "stale-rc-valuation-20260905",
        &mk(
            "phase7_valuation_backfill",
            "factor",
            "phase7_valuation_v1",
            "valuation",
            "7-B/7-J",
            &["market_stock_daily_basic"],
            "equal_weight_valuation",
        ),
    )
    .await;
    println!("[stale-rc] valuation: {:?}", r.map(|c| c.task_status()));
    // 行业残差财务族（fin_*_indrel）
    let r = run_phase7_industry_residual_quality_backfill(
        &db,
        "stale-rc-indrel-20260905",
        &mk(
            "phase7_industry_residual_quality_backfill",
            "factor",
            "phase7_industry_residual_quality_v1",
            "industry_residual_quality",
            "7-B",
            &["market_financial_indicator"],
            "equal_weight",
        ),
    )
    .await;
    println!("[stale-rc] indrel: {:?}", r.map(|c| c.task_status()));
    // 增长恢复族（fin_*_recovery_persist）
    let r = run_phase7_growth_recovery_backfill(
        &db,
        "stale-rc-growth-20260905",
        &mk(
            "phase7_growth_recovery_backfill",
            "factor",
            "phase7_growth_recovery_v1",
            "growth_recovery",
            "7-B",
            &["market_financial_indicator"],
            "equal_weight",
        ),
    )
    .await;
    println!("[stale-rc] growth: {:?}", r.map(|c| c.task_status()));
    // 解禁压力族（unlock_pressure）
    let r = run_phase7_unlock_supply_pressure_backfill(
        &db,
        "stale-rc-unlock-20260905",
        &mk(
            "phase7_unlock_supply_pressure_backfill",
            "factor",
            "phase7_unlock_supply_pressure_v1",
            "unlock_supply_pressure",
            "7-B",
            &["market_stock_share_float"],
            "equal_weight",
        ),
    )
    .await;
    println!("[stale-rc] unlock: {:?}", r.map(|c| c.task_status()));
    // 股本变动族（float/total_share_growth）
    let r = run_phase7_supply_float_shock_backfill(
        &db,
        "stale-rc-float-20260905",
        &mk(
            "phase7_supply_float_shock_backfill",
            "factor",
            "phase7_supply_float_shock_v1",
            "supply_float_shock",
            "7-B",
            &["market_stock_share_float"],
            "equal_weight",
        ),
    )
    .await;
    println!("[stale-rc] float: {:?}", r.map(|c| c.task_status()));
    // 业绩预告修正族（forecast_*_120d_std，2026-09-05 补加）
    let r = run_phase7_forecast_revision_surprise_backfill(
        &db,
        "stale-rc-fc-20260905",
        &mk(
            "phase7_forecast_revision_surprise_backfill",
            "factor",
            "phase7_forecast_revision_surprise_v1",
            "forecast_revision_surprise",
            "7-B",
            &["market_stock_forecast", "market_trade_calendar"],
            "equal_weight",
        ),
    )
    .await;
    println!("[stale-rc] fc: {:?}", r.map(|c| c.task_status()));
    // 流动性质量族（liq_*_trend/impact）
    let r = run_phase7_liquidity_quality_backfill(
        &db,
        "stale-rc-liq-20260905",
        &mk(
            "phase7_liquidity_quality_backfill",
            "factor",
            "phase7_liquidity_quality_v1",
            "liquidity_quality",
            "7-B",
            &["market_stock_daily_bar"],
            "equal_weight",
        ),
    )
    .await;
    println!("[stale-rc] liq: {:?}", r.map(|c| c.task_status()));
    // 恢复持续性族（fin_*_recovery_persist）
    let r = run_phase7_earnings_recovery_persistence_backfill(
        &db,
        "stale-rc-erp-20260905",
        &mk(
            "phase7_earnings_recovery_persistence_backfill",
            "factor",
            "phase7_earnings_recovery_persistence_v1",
            "earnings_recovery_persistence",
            "7-B",
            &["market_financial_indicator"],
            "equal_weight",
        ),
    )
    .await;
    println!("[stale-rc] erp: {:?}", r.map(|c| c.task_status()));
    // 量价派生（mom_5d_std/vol_20d_std 等在 price_volume specs 里）
    let r = run_phase7_price_volume_backfill(
        &db,
        "stale-rc-pv-20260905",
        &mk(
            "phase7_price_volume_backfill",
            "factor",
            "phase7_price_volume_expanded_v1",
            "price_volume",
            "7-B",
            &["market_stock_daily_bar"],
            "equal_weight",
        ),
    )
    .await;
    println!("[stale-rc] price_volume: {:?}", r.map(|c| c.task_status()));
}
