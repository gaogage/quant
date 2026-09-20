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

// ─── 第二批补充测试（非 ignored，秒级，真实本机 PG）───────────────────────
//
// 安全边界：backfill_*_background handler 只直调**拒绝分支**——
// into_plan 校验失败在写 data_sync_task / spawn 重回填之前短路返回，
// 不触库不启动后台任务。成功分支会拉全市场因子重算（分钟级），绝不直调。
// 写路径仅用 zzz_test_api_bf_ 前缀键（data_sync_task / experiment_run），自造自清理。

mod second_batch {
    use super::*;
    use axum::extract::State;
    use axum::response::IntoResponse;
    use axum::Json;
    use std::sync::Arc;

    async fn test_db() -> sqlx::PgPool {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        sqlx::PgPool::connect(&url).await.expect("test db connect")
    }

    /// 构造直调用 AppState（拒绝分支不触 Tushare/库，客户端仅初始化不发请求）。
    async fn test_state() -> Arc<crate::AppState> {
        let _ = dotenv::from_filename("../.env");
        let _ = dotenv::dotenv();
        let db = test_db().await;
        let tushare = quant_data::tushare::client::TushareClient::from_env()
            .expect("Tushare client init (需 TUSHARE_TOKEN: source ../.env)");
        Arc::new(crate::AppState {
            start_time: chrono::Utc::now(),
            db,
            tushare,
            sync_tasks: crate::sync_task_registry::new_registry(),
        })
    }

    /// handler 返回的 Json 响应体解析为 serde_json::Value。
    async fn resp_json(resp: impl IntoResponse) -> serde_json::Value {
        let body = resp.into_response().into_body();
        let bytes = axum::body::to_bytes(body, usize::MAX)
            .await
            .expect("response body");
        serde_json::from_slice(&bytes).expect("json response body")
    }

    fn inverted_dates() -> (Option<String>, Option<String>) {
        // start > end → into_plan 在任何写库动作之前拒绝
        (Some("2026-06-10".into()), Some("2026-06-01".into()))
    }

    fn assert_rejected(value: &serde_json::Value, expect_fragment: &str, handler: &str) {
        assert_eq!(
            value["code"], 1,
            "[{handler}] 拒绝分支应返回 code 1: {value}"
        );
        let message = value["message"].as_str().unwrap_or_default();
        assert!(
            message.contains(expect_fragment),
            "[{handler}] message 应含 {expect_fragment:?}: {message}"
        );
    }

    // ── 拒绝分支直调：基本面/量价族（8 handler）──

    #[tokio::test]
    async fn fundamental_family_handlers_reject_inverted_dates_without_scheduling() {
        let state = test_state().await;
        let (start, end) = inverted_dates();

        let v = resp_json(
            backfill_phase7_price_volume_background(
                State(state.clone()),
                Json(Phase7PriceVolumeBackfillRequest {
                    start_date: start.clone(),
                    end_date: end.clone(),
                    version: None,
                    combo_name: None,
                    statement_timeout_ms: None,
                }),
            )
            .await,
        )
        .await;
        assert_rejected(&v, "start_date must be <= end_date", "price_volume");

        let v = resp_json(
            backfill_phase7_financial_quality_background(
                State(state.clone()),
                Json(Phase7FinancialQualityBackfillRequest {
                    start_date: start.clone(),
                    end_date: end.clone(),
                    version: None,
                    combo_name: None,
                    statement_timeout_ms: None,
                }),
            )
            .await,
        )
        .await;
        assert_rejected(&v, "start_date must be <= end_date", "financial_quality");

        let v = resp_json(
            backfill_phase7_financial_quality_change_background(
                State(state.clone()),
                Json(Phase7FinancialQualityChangeBackfillRequest {
                    start_date: start.clone(),
                    end_date: end.clone(),
                    version: None,
                    combo_name: None,
                    statement_timeout_ms: None,
                }),
            )
            .await,
        )
        .await;
        assert_rejected(
            &v,
            "start_date must be <= end_date",
            "financial_quality_change",
        );

        let v = resp_json(
            backfill_phase7_earnings_recovery_persistence_background(
                State(state.clone()),
                Json(Phase7EarningsRecoveryPersistenceBackfillRequest {
                    start_date: start.clone(),
                    end_date: end.clone(),
                    version: None,
                    combo_name: None,
                    statement_timeout_ms: None,
                }),
            )
            .await,
        )
        .await;
        assert_rejected(
            &v,
            "start_date must be <= end_date",
            "earnings_recovery_persistence",
        );

        let v = resp_json(
            backfill_phase7_industry_residual_quality_background(
                State(state.clone()),
                Json(Phase7IndustryResidualQualityBackfillRequest {
                    start_date: start.clone(),
                    end_date: end.clone(),
                    version: None,
                    combo_name: None,
                    statement_timeout_ms: None,
                }),
            )
            .await,
        )
        .await;
        assert_rejected(
            &v,
            "start_date must be <= end_date",
            "industry_residual_quality",
        );

        let v = resp_json(
            backfill_phase7_relative_strength_background(
                State(state.clone()),
                Json(Phase7RelativeStrengthBackfillRequest {
                    start_date: start.clone(),
                    end_date: end.clone(),
                    version: None,
                    combo_name: None,
                    statement_timeout_ms: None,
                }),
            )
            .await,
        )
        .await;
        assert_rejected(&v, "start_date must be <= end_date", "relative_strength");

        let v = resp_json(
            backfill_phase7_quality_relative_strength_background(
                State(state.clone()),
                Json(Phase7QualityRelativeStrengthBackfillRequest {
                    start_date: start.clone(),
                    end_date: end.clone(),
                    version: None,
                    combo_name: None,
                    statement_timeout_ms: None,
                }),
            )
            .await,
        )
        .await;
        assert_rejected(
            &v,
            "start_date must be <= end_date",
            "quality_relative_strength",
        );

        let v = resp_json(
            backfill_phase7_growth_recovery_background(
                State(state.clone()),
                Json(Phase7GrowthRecoveryBackfillRequest {
                    start_date: start,
                    end_date: end,
                    version: None,
                    combo_name: None,
                    statement_timeout_ms: None,
                }),
            )
            .await,
        )
        .await;
        assert_rejected(&v, "start_date must be <= end_date", "growth_recovery");
    }

    // ── 拒绝分支直调：估值/资金流/供给/流动性族 + P4.2b（10 handler）──

    #[tokio::test]
    async fn valuation_supply_family_handlers_reject_inverted_dates_without_scheduling() {
        let state = test_state().await;
        let (start, end) = inverted_dates();

        macro_rules! reject {
            ($handler:expr, $req:expr, $name:literal) => {{
                let v = resp_json($handler(State(state.clone()), Json($req)).await).await;
                assert_rejected(&v, "start_date must be <= end_date", $name);
            }};
        }

        reject!(
            backfill_phase7_valuation_background,
            Phase7ValuationBackfillRequest {
                start_date: start.clone(),
                end_date: end.clone(),
                version: None,
                combo_name: None,
                statement_timeout_ms: None,
            },
            "valuation"
        );
        reject!(
            backfill_phase7_moneyflow_background,
            Phase7MoneyflowBackfillRequest {
                start_date: start.clone(),
                end_date: end.clone(),
                version: None,
                combo_name: None,
                statement_timeout_ms: None,
            },
            "moneyflow"
        );
        reject!(
            backfill_phase7_moneyflow_congestion_background,
            Phase7MoneyflowCongestionBackfillRequest {
                start_date: start.clone(),
                end_date: end.clone(),
                version: None,
                combo_name: None,
                statement_timeout_ms: None,
            },
            "moneyflow_congestion"
        );
        reject!(
            backfill_phase7_supply_float_shock_background,
            Phase7SupplyFloatShockBackfillRequest {
                start_date: start.clone(),
                end_date: end.clone(),
                version: None,
                combo_name: None,
                statement_timeout_ms: None,
            },
            "supply_float_shock"
        );
        reject!(
            backfill_phase7_cashflow_quality_background,
            Phase7CashflowQualityBackfillRequest {
                start_date: start.clone(),
                end_date: end.clone(),
                version: None,
                combo_name: None,
                statement_timeout_ms: None,
            },
            "cashflow_quality"
        );
        reject!(
            backfill_phase7_dividend_quality_background,
            Phase7DividendQualityBackfillRequest {
                start_date: start.clone(),
                end_date: end.clone(),
                version: None,
                combo_name: None,
                statement_timeout_ms: None,
            },
            "dividend_quality"
        );
        reject!(
            backfill_phase7_liquidity_quality_background,
            Phase7LiquidityQualityBackfillRequest {
                start_date: start.clone(),
                end_date: end.clone(),
                version: None,
                combo_name: None,
                statement_timeout_ms: None,
            },
            "liquidity_quality"
        );
        reject!(
            backfill_phase7_market_residual_risk_background,
            Phase7MarketResidualRiskBackfillRequest {
                start_date: start.clone(),
                end_date: end.clone(),
                version: None,
                combo_name: None,
                statement_timeout_ms: None,
            },
            "market_residual_risk"
        );
        reject!(
            backfill_p42b_large_cap_momentum_reversal_background,
            P42bLargeCapMomentumReversalBackfillRequest {
                start_date: start.clone(),
                end_date: end.clone(),
                version: None,
                combo_name: None,
                statement_timeout_ms: None,
            },
            "p42b_large_cap_momentum_reversal"
        );
        reject!(
            backfill_p42b_defensive_low_vol_quality_background,
            P42bDefensiveLowVolQualityBackfillRequest {
                start_date: start,
                end_date: end,
                version: None,
                combo_name: None,
                statement_timeout_ms: None,
            },
            "p42b_defensive_low_vol_quality"
        );
    }

    // ── 拒绝分支直调：事件族（7 handler）──

    #[tokio::test]
    async fn event_family_handlers_reject_inverted_dates_without_scheduling() {
        let state = test_state().await;
        let (start, end) = inverted_dates();

        macro_rules! reject {
            ($handler:expr, $req:expr, $name:literal) => {{
                let v = resp_json($handler(State(state.clone()), Json($req)).await).await;
                assert_rejected(&v, "start_date must be <= end_date", $name);
            }};
        }

        reject!(
            backfill_phase7_event_alpha_background,
            Phase7EventAlphaBackfillRequest {
                start_date: start.clone(),
                end_date: end.clone(),
                version: None,
                combo_name: None,
                statement_timeout_ms: None,
            },
            "event_alpha"
        );
        reject!(
            backfill_phase7_event_window_alpha_background,
            Phase7EventWindowAlphaBackfillRequest {
                start_date: start.clone(),
                end_date: end.clone(),
                version: None,
                combo_name: None,
                statement_timeout_ms: None,
            },
            "event_window_alpha"
        );
        reject!(
            backfill_phase7_event_surprise_background,
            Phase7EventSurpriseBackfillRequest {
                start_date: start.clone(),
                end_date: end.clone(),
                version: None,
                combo_name: None,
                statement_timeout_ms: None,
            },
            "event_surprise"
        );
        reject!(
            backfill_phase7_forecast_revision_surprise_background,
            Phase7ForecastRevisionSurpriseBackfillRequest {
                start_date: start.clone(),
                end_date: end.clone(),
                version: None,
                combo_name: None,
                statement_timeout_ms: None,
            },
            "forecast_revision_surprise"
        );
        reject!(
            backfill_phase7_repurchase_supply_shock_background,
            Phase7RepurchaseSupplyShockBackfillRequest {
                start_date: start.clone(),
                end_date: end.clone(),
                version: None,
                combo_name: None,
                statement_timeout_ms: None,
            },
            "repurchase_supply_shock"
        );
        reject!(
            backfill_phase7_block_trade_supply_demand_background,
            Phase7BlockTradeSupplyDemandBackfillRequest {
                start_date: start.clone(),
                end_date: end.clone(),
                version: None,
                combo_name: None,
                statement_timeout_ms: None,
            },
            "block_trade_supply_demand"
        );
        reject!(
            backfill_phase7_unlock_supply_pressure_background,
            Phase7UnlockSupplyPressureBackfillRequest {
                start_date: start,
                end_date: end,
                version: None,
                combo_name: None,
                statement_timeout_ms: None,
            },
            "unlock_supply_pressure"
        );
    }

    // ── 拒绝分支直调：admission gate 族（6 handler，缺 gate 即拒，日期合法也不触库）──

    #[tokio::test]
    async fn alpha_admission_gated_handlers_reject_without_market_scope_gate() {
        let state = test_state().await;
        // gate 缺失时 admission 校验先于日期解析拒绝——成功路径完全不触库
        let (start, end) = (
            Some("2017-01-03".to_string()),
            Some("2026-06-30".to_string()),
        );

        macro_rules! reject_gate {
            ($handler:expr, $req:expr, $name:literal) => {{
                let v = resp_json($handler(State(state.clone()), Json($req)).await).await;
                assert_rejected(&v, "alpha_admission_gate_id", $name);
            }};
        }

        reject_gate!(
            backfill_phase7_industry_prosperity_background,
            Phase7IndustryProsperityBackfillRequest {
                start_date: start.clone(),
                end_date: end.clone(),
                version: None,
                combo_name: None,
                statement_timeout_ms: None,
                alpha_admission_gate_id: None,
                universe_profile: None,
            },
            "industry_prosperity"
        );
        reject_gate!(
            backfill_phase7_futures_price_chain_background,
            Phase7FuturesPriceChainBackfillRequest {
                start_date: start.clone(),
                end_date: end.clone(),
                version: None,
                combo_name: None,
                statement_timeout_ms: None,
                alpha_admission_gate_id: None,
                universe_profile: None,
            },
            "futures_price_chain"
        );
        reject_gate!(
            backfill_phase7_equity_pledge_pressure_background,
            Phase7EquityPledgePressureBackfillRequest {
                start_date: start.clone(),
                end_date: end.clone(),
                version: None,
                combo_name: None,
                statement_timeout_ms: None,
                alpha_admission_gate_id: None,
                universe_profile: None,
            },
            "equity_pledge_pressure"
        );
        reject_gate!(
            backfill_phase7_shareholder_structure_background,
            Phase7ShareholderStructureBackfillRequest {
                start_date: start.clone(),
                end_date: end.clone(),
                version: None,
                combo_name: None,
                statement_timeout_ms: None,
                alpha_admission_gate_id: None,
                universe_profile: None,
            },
            "shareholder_structure"
        );
        reject_gate!(
            backfill_phase7_margin_detail_background,
            Phase7MarginDetailBackfillRequest {
                start_date: start.clone(),
                end_date: end.clone(),
                version: None,
                combo_name: None,
                statement_timeout_ms: None,
                alpha_admission_gate_id: None,
                universe_profile: None,
            },
            "margin_detail"
        );
        reject_gate!(
            backfill_phase7_analyst_revision_background,
            Phase7AnalystRevisionBackfillRequest {
                start_date: start,
                end_date: end,
                version: None,
                combo_name: None,
                statement_timeout_ms: None,
                alpha_admission_gate_id: None,
                universe_profile: None,
            },
            "analyst_revision"
        );
    }

    // ── 拒绝分支直调：alpha_blend / profiles 载荷校验 ──

    #[tokio::test]
    async fn alpha_blend_handlers_reject_invalid_payloads_without_scheduling() {
        let state = test_state().await;
        let (start, end) = inverted_dates();

        // sources < 2
        let v = resp_json(
            backfill_phase7_alpha_blend_background(
                State(state.clone()),
                Json(Phase7AlphaBlendBackfillRequest {
                    start_date: None,
                    end_date: None,
                    version: None,
                    combo_name: None,
                    statement_timeout_ms: None,
                    allow_signed_weights: false,
                    sources: vec![Phase7AlphaBlendSourceRequest {
                        combo_name: "phase7_financial_quality_v1".into(),
                        version: None,
                        weight: 1.0,
                    }],
                }),
            )
            .await,
        )
        .await;
        assert_rejected(&v, "at least 2 combo sources", "alpha_blend_sources");

        // 权重和 != 1
        let v = resp_json(
            backfill_phase7_alpha_blend_background(
                State(state.clone()),
                Json(Phase7AlphaBlendBackfillRequest {
                    start_date: None,
                    end_date: None,
                    version: None,
                    combo_name: None,
                    statement_timeout_ms: None,
                    allow_signed_weights: false,
                    sources: vec![
                        Phase7AlphaBlendSourceRequest {
                            combo_name: "phase7_financial_quality_v1".into(),
                            version: None,
                            weight: 0.3,
                        },
                        Phase7AlphaBlendSourceRequest {
                            combo_name: "phase7_valuation_v1".into(),
                            version: None,
                            weight: 0.3,
                        },
                    ],
                }),
            )
            .await,
        )
        .await;
        assert_rejected(&v, "weights must sum to 1.0", "alpha_blend_weight_sum");

        // 日期倒置
        let v = resp_json(
            backfill_phase7_alpha_blend_background(
                State(state.clone()),
                Json(Phase7AlphaBlendBackfillRequest {
                    start_date: start,
                    end_date: end,
                    version: None,
                    combo_name: None,
                    statement_timeout_ms: None,
                    allow_signed_weights: false,
                    sources: vec![
                        Phase7AlphaBlendSourceRequest {
                            combo_name: "phase7_financial_quality_v1".into(),
                            version: None,
                            weight: 0.5,
                        },
                        Phase7AlphaBlendSourceRequest {
                            combo_name: "phase7_valuation_v1".into(),
                            version: None,
                            weight: 0.5,
                        },
                    ],
                }),
            )
            .await,
        )
        .await;
        assert_rejected(&v, "start_date must be <= end_date", "alpha_blend_dates");

        // profiles 日期倒置
        let v = resp_json(
            backfill_phase7_alpha_blend_profiles_background(
                State(state),
                Json(Phase7AlphaBlendProfilesBackfillRequest {
                    start_date: Some("2026-06-10".into()),
                    end_date: Some("2026-06-01".into()),
                    version: None,
                    profile_names: None,
                    statement_timeout_ms: None,
                }),
            )
            .await,
        )
        .await;
        assert_rejected(
            &v,
            "start_date must be <= end_date",
            "alpha_blend_profiles_dates",
        );
    }

    // ── 共享纯函数（mod.rs）──

    #[test]
    fn alpha_blend_required_source_count_branches() {
        let mut plan = zzz_alpha_blend_plan();
        plan.combo_method = "weighted_combo_optional_overlay";
        assert_eq!(phase7_alpha_blend_required_source_count(&plan), 1);
        plan.combo_method = "weighted_combo_blend";
        plan.source_combos = vec![
            Phase7AlphaBlendSourcePlan {
                combo_name: "a".into(),
                version: "1.0.0".into(),
                weight: 0.5,
            },
            Phase7AlphaBlendSourcePlan {
                combo_name: "b".into(),
                version: "1.0.0".into(),
                weight: 0.5,
            },
            Phase7AlphaBlendSourcePlan {
                combo_name: "c".into(),
                version: "1.0.0".into(),
                weight: 0.0,
            },
        ];
        assert_eq!(phase7_alpha_blend_required_source_count(&plan), 3);
    }

    #[test]
    fn factor_backfill_combo_weights_json_maps_factor_codes_to_weights() {
        let specs = vec![
            zzz_spec("zzz_test_api_bf_a", 0.25),
            zzz_spec("zzz_test_api_bf_b", 0.75),
        ];
        let weights = factor_backfill_combo_weights_json(&specs).expect("weights json");
        assert_eq!(weights["zzz_test_api_bf_a"], json!(0.25));
        assert_eq!(weights["zzz_test_api_bf_b"], json!(0.75));
    }

    #[test]
    fn alpha_blend_source_records_and_weights_json_encode_combo_keys() {
        let sources = vec![
            Phase7AlphaBlendSourcePlan {
                combo_name: "phase7_financial_quality_v1".into(),
                version: "1.0.0".into(),
                weight: 0.6,
            },
            Phase7AlphaBlendSourcePlan {
                combo_name: "phase7_valuation_v1".into(),
                version: "2.0.0".into(),
                weight: 0.4,
            },
        ];
        let records = alpha_blend_source_records_json(&sources).expect("records json");
        assert_eq!(records.as_array().expect("array").len(), 2);
        assert_eq!(
            records[0]["combo_name"],
            json!("phase7_financial_quality_v1")
        );
        assert_eq!(records[0]["weight"], json!(0.6));

        let weights = alpha_blend_weights_json(&sources).expect("weights json");
        assert_eq!(weights["phase7_financial_quality_v1@1.0.0"], json!(0.6));
        assert_eq!(weights["phase7_valuation_v1@2.0.0"], json!(0.4));
    }

    #[test]
    fn backfill_report_total_rows_saturates_and_completion_accessors() {
        let report = SetBasedFactorBackfillReport {
            factor_rows: usize::MAX / 2,
            combo_rows: usize::MAX,
            factor_rows_by_code: vec![("zzz_test_api_bf_a".into(), 10)],
        };
        // 饱和加法不 panic
        assert_eq!(report.total_rows(), usize::MAX);

        let small = SetBasedFactorBackfillReport {
            factor_rows: 120,
            combo_rows: 30,
            factor_rows_by_code: vec![],
        };
        assert_eq!(small.total_rows(), 150);

        let completed = SetBasedFactorBackfillCompletion::completed_with(small.clone(), 42);
        assert_eq!(completed.task_status(), "completed");
        assert_eq!(completed.experiment_status(), "completed");
        assert_eq!(completed.report().total_rows(), 150);
        assert_eq!(completed.elapsed_ms(), 42);

        let cancelled = SetBasedFactorBackfillCompletion::cancelled_with(small, 7);
        assert_eq!(cancelled.task_status(), "cancelled");
        assert_eq!(cancelled.experiment_status(), "partial");
        assert_eq!(cancelled.elapsed_ms(), 7);
    }

    #[test]
    fn elapsed_millis_reports_measured_duration() {
        let started = std::time::Instant::now();
        std::thread::sleep(std::time::Duration::from_millis(5));
        let elapsed = elapsed_millis(started);
        assert!(
            elapsed >= 5,
            "elapsed_millis 至少覆盖 sleep 时长: {elapsed}"
        );
    }

    // ── 连库写路径（zzz 键化，自造自清理）──

    #[tokio::test]
    async fn update_factor_backfill_progress_updates_zzz_task_counters() {
        let db = test_db().await;
        let task_id = "zzz_test_api_bf_progress";
        let _ = sqlx::query("DELETE FROM data_sync_task WHERE task_id = $1")
            .bind(task_id)
            .execute(&db)
            .await;
        sqlx::query("INSERT INTO data_sync_task (task_id, task_type, source, status) VALUES ($1, 'phase7_backfill', 'factor', 'running')")
            .bind(task_id)
            .execute(&db)
            .await
            .expect("insert zzz task");

        update_factor_backfill_progress(&db, task_id, 1, 4, 250)
            .await
            .expect("progress update");

        let (progress, success): (i32, i32) =
            sqlx::query_as("SELECT progress, success_count FROM data_sync_task WHERE task_id = $1")
                .bind(task_id)
                .fetch_one(&db)
                .await
                .expect("zzz task row");
        assert_eq!(progress, 25, "1/4 步 → 25%");
        assert_eq!(success, 250);

        // 完成步数超过总步数时 progress 封顶 100（min 保护）
        update_factor_backfill_progress(&db, task_id, 9, 4, 500)
            .await
            .expect("progress update clamped");
        let progress: i32 =
            sqlx::query_scalar("SELECT progress FROM data_sync_task WHERE task_id = $1")
                .bind(task_id)
                .fetch_one(&db)
                .await
                .expect("zzz task row");
        assert_eq!(progress, 100, "超出总步数封顶 100");

        let _ = sqlx::query("DELETE FROM data_sync_task WHERE task_id = $1")
            .bind(task_id)
            .execute(&db)
            .await;
    }

    #[tokio::test]
    async fn factor_backfill_cancel_requested_detects_cancel_states_only() {
        let db = test_db().await;
        // 不存在任务 → false
        assert!(
            !factor_backfill_cancel_requested(&db, "zzz_test_api_bf_none")
                .await
                .expect("cancel check")
        );

        let task_id = "zzz_test_api_bf_cancel";
        for status in ["cancel_requested", "cancelled", "running", "completed"] {
            let _ = sqlx::query("DELETE FROM data_sync_task WHERE task_id = $1")
                .bind(task_id)
                .execute(&db)
                .await;
            sqlx::query("INSERT INTO data_sync_task (task_id, task_type, source, status) VALUES ($1, 'phase7_backfill', 'factor', $2)")
                .bind(task_id)
                .bind(status)
                .execute(&db)
                .await
                .expect("insert zzz cancel task");
            let requested = factor_backfill_cancel_requested(&db, task_id)
                .await
                .expect("cancel check");
            assert_eq!(
                requested,
                status == "cancel_requested" || status == "cancelled",
                "status={status} 取消判定错误"
            );
        }
        let _ = sqlx::query("DELETE FROM data_sync_task WHERE task_id = $1")
            .bind(task_id)
            .execute(&db)
            .await;
    }

    #[tokio::test]
    async fn persist_factor_backfill_experiment_run_records_zzz_profile() {
        let db = test_db().await;
        let task_id = "zzz_test_api_bf_exp";
        let _ = sqlx::query("DELETE FROM experiment_run WHERE related_entity_id = $1")
            .bind(task_id)
            .execute(&db)
            .await;

        let plan = zzz_set_based_plan();
        let specs = vec![
            zzz_spec("zzz_test_api_bf_a", 0.5),
            zzz_spec("zzz_test_api_bf_b", 0.5),
        ];
        let completion = SetBasedFactorBackfillCompletion::completed_with(
            SetBasedFactorBackfillReport {
                factor_rows: 100,
                combo_rows: 50,
                factor_rows_by_code: vec![
                    ("zzz_test_api_bf_a".into(), 60),
                    ("zzz_test_api_bf_b".into(), 40),
                ],
            },
            1234,
        );
        persist_factor_backfill_experiment_run(&db, task_id, &plan, &specs, &completion)
            .await
            .expect("persist experiment run");

        let (experiment_type, status, metrics_factor_rows, config_bundle): (
            String,
            String,
            serde_json::Value,
            serde_json::Value,
        ) = sqlx::query_as(
            "SELECT experiment_type, status, metrics->'factor_rows', config->'bundle_name'
             FROM experiment_run WHERE related_entity_id = $1",
        )
        .bind(task_id)
        .fetch_one(&db)
        .await
        .expect("zzz experiment_run row");
        assert_eq!(experiment_type, plan.experiment_type);
        assert_eq!(status, "completed");
        assert_eq!(metrics_factor_rows, json!(100));
        assert_eq!(config_bundle, json!(plan.bundle_name));

        let _ = sqlx::query("DELETE FROM experiment_run WHERE related_entity_id = $1")
            .bind(task_id)
            .execute(&db)
            .await;
    }

    #[tokio::test]
    async fn set_local_statement_timeout_scopes_to_current_transaction() {
        let db = test_db().await;
        // SET LOCAL 在事务结束时自动还原——rollback 后全局设置不受影响，零残留
        let mut tx = db.begin().await.expect("begin tx");
        set_local_statement_timeout(&mut tx, 12_345)
            .await
            .expect("set local timeout");
        let current: String = sqlx::query_scalar("SHOW statement_timeout")
            .fetch_one(&mut *tx)
            .await
            .expect("show timeout");
        // PG 返回 '12345ms' 形态
        assert_eq!(current, "12345ms", "事务内 statement_timeout 应为 12345ms");
        tx.rollback().await.expect("rollback");
    }

    // ── zzz 造数构造器 ──

    fn zzz_spec(code: &'static str, weight: f64) -> SetBasedFactorSpec {
        SetBasedFactorSpec {
            factor_code: code,
            name: code,
            period: 20,
            kind: Phase7BackfillFactorKind::Reversal,
            weight,
        }
    }

    fn zzz_set_based_plan() -> SetBasedFactorBackfillPlan {
        SetBasedFactorBackfillPlan {
            start_date: NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(),
            end_date: NaiveDate::from_ymd_opt(2026, 6, 30).unwrap(),
            version: "zzz-1.0.0".into(),
            combo_name: "zzz_test_api_bf_combo".into(),
            statement_timeout_ms: 0,
            task_type: "phase7_backfill",
            source: "factor",
            heartbeat_timeout_seconds: 3600,
            bundle_name: "zzz_test_api_bf_bundle",
            category: "zzz_test",
            phase: "7-J",
            dependencies: &["market_stock_daily_bar"],
            combo_method: "equal_weight",
            experiment_type: "phase7_factor_backfill_profile",
            source_combos: vec![],
        }
    }

    fn zzz_alpha_blend_plan() -> Phase7AlphaBlendBackfillPlan {
        Phase7AlphaBlendBackfillPlan {
            start_date: NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(),
            end_date: NaiveDate::from_ymd_opt(2026, 6, 30).unwrap(),
            version: "zzz-1.0.0".into(),
            combo_name: "zzz_test_api_bf_blend".into(),
            statement_timeout_ms: 0,
            task_type: "phase7_alpha_blend_backfill",
            source: "factor",
            heartbeat_timeout_seconds: 3600,
            bundle_name: "zzz_test_api_bf_bundle",
            category: "composite_alpha",
            phase: "7-B/7-J",
            dependencies: &["multi_factor_value"],
            combo_method: "weighted_combo_blend",
            experiment_type: "phase7_factor_backfill_profile",
            source_combos: vec![],
        }
    }
}
