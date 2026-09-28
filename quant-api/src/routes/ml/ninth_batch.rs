// ─── 第九批 A 线（ML 域）ml.rs 补测 ─────────────────────────
//
// 归因：本文件 40.20% 覆盖，既有 38 个测试集中在标签计算/训练纯函数；
// 差集分两路补：
// - 纯逻辑面：LabelObjective 全变体谓词 pin、市场状态特征/分桶、RegimeSplitModel
//   守卫、cache economics 归一化与建议分支、readiness 纯工具族（ratio/gate/
//   日期解析/分布）、evaluation 状态聚合、非线性请求边界、杂项工具；
// - DB CRUD 面：ensure_data_version_exists、readiness 报告全门禁（绿/红/泄漏）、
//   evaluate_prediction_set_inner 全链门禁与落库、cache economics persist 开关、
//   create_linear_prediction_set_inner 无因子错误路径。
//
// 隔离纪律（对齐第五/八批先例）：
// - 共享父行 data_version/strategy_definition/strategy_version/model_registry 固定
//   zzz 键 ON CONFLICT DO NOTHING 幂等插入，结尾不删（防并行测试互拆）；
// - 每测试专属键 zzz_api9m_<场景>_*，前置 + 结尾精确清理；
// - model_prediction 是 3393 万行大表：只写 zzz prediction_set_id 少量行，
//   清理先按 set 精确 DELETE 再删 prediction_set。
use super::*;

use chrono::{Duration, NaiveDate};
use serde_json::Value;
use std::time::Duration as StdDuration;

async fn test_db() -> sqlx::PgPool {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
    sqlx::PgPool::connect(&url).await.expect("test db connect")
}

/// 共享父行：data_version（prediction_set FK）+ strategy 链（backtest_task FK）
/// + model_registry（prediction_set FK）。幂等插入，结尾不删。
async fn seed_shared_parents(db: &sqlx::PgPool) {
    sqlx::query(
        "INSERT INTO data_version
               (data_version_id, name, source, start_date, end_date, tables, snapshot_hash)
             VALUES ('zzz_api9m_dv', 'zzz 第九批ML数据', 'zzz', '2026-01-01', '2026-01-31',
                     '{}', 'zzz-api9m')
             ON CONFLICT DO NOTHING",
    )
    .execute(db)
    .await
    .expect("insert zzz data_version");
    sqlx::query(
        "INSERT INTO strategy_definition
               (strategy_id, strategy_code, name, strategy_type, status)
             VALUES (999999902, 'zzz_api9m_strategy', 'zzz 第九批ML', 'zzz', 'active')
             ON CONFLICT DO NOTHING",
    )
    .execute(db)
    .await
    .expect("insert zzz strategy_definition");
    sqlx::query(
        "INSERT INTO strategy_version
               (strategy_version_id, strategy_code, version, parameter_schema,
                default_parameters, status)
             VALUES ('zzz_api9m_sv', 'zzz_api9m_strategy', 'v9', '{}', '{}', 'active')
             ON CONFLICT DO NOTHING",
    )
    .execute(db)
    .await
    .expect("insert zzz strategy_version");
    sqlx::query(
        "INSERT INTO model_registry
               (model_version_id, model_code, model_type, version, label_definition,
                training_window, artifact_path, artifact_hash, status)
             VALUES ('zzz_api9m_mr', 'zzz_api9m_model', 'zzz', 'v9', '{}',
                     '{}', 'artifact://zzz', 'zzz-api9m', 'active')
             ON CONFLICT DO NOTHING",
    )
    .execute(db)
    .await
    .expect("insert zzz model_registry");
}

/// 按场景前缀清理本测试专属行（model_prediction 先删，防大表 CASCADE 依赖）。
async fn cleanup_scoped(db: &sqlx::PgPool, scope: &str) {
    let prefix = format!("zzz_api9m_{}%", scope);
    let _ = sqlx::query("DELETE FROM experiment_run WHERE related_entity_id LIKE $1")
        .bind(&prefix)
        .execute(db)
        .await;
    let _ = sqlx::query("DELETE FROM model_prediction WHERE prediction_set_id LIKE $1")
        .bind(&prefix)
        .execute(db)
        .await;
    let _ = sqlx::query("DELETE FROM portfolio_target WHERE task_id LIKE $1")
        .bind(&prefix)
        .execute(db)
        .await;
    let _ = sqlx::query("DELETE FROM backtest_result WHERE task_id LIKE $1")
        .bind(&prefix)
        .execute(db)
        .await;
    let _ = sqlx::query("DELETE FROM backtest_equity_curve WHERE task_id LIKE $1")
        .bind(&prefix)
        .execute(db)
        .await;
    let _ = sqlx::query("DELETE FROM backtest_task WHERE task_id LIKE $1")
        .bind(&prefix)
        .execute(db)
        .await;
    let _ = sqlx::query("DELETE FROM prediction_set WHERE prediction_set_id LIKE $1")
        .bind(&prefix)
        .execute(db)
        .await;
    let _ = sqlx::query("DELETE FROM model_registry WHERE model_version_id LIKE $1")
        .bind(&prefix)
        .execute(db)
        .await;
    let _ = sqlx::query("DELETE FROM training_dataset WHERE training_dataset_id LIKE $1")
        .bind(&prefix)
        .execute(db)
        .await;
}

/// 造 zzz prediction_set（默认 ready）。
async fn insert_zzz_prediction_set(
    db: &sqlx::PgPool,
    set_id: &str,
    status: &str,
    start: NaiveDate,
    end: NaiveDate,
    training_end: Option<NaiveDate>,
    metadata: Value,
) {
    sqlx::query(
        "INSERT INTO prediction_set
               (prediction_set_id, model_version_id, feature_set_version_id, data_version_id,
                start_date, end_date, prediction_hash, status, metadata, training_end_date)
             VALUES ($1, 'zzz_api9m_mr', 'zzz_api9m_fsv', 'zzz_api9m_dv',
                     $2, $3, 'zzz-api9m-hash', $4, $5, $6)",
    )
    .bind(set_id)
    .bind(start)
    .bind(end)
    .bind(status)
    .bind(metadata)
    .bind(training_end)
    .execute(db)
    .await
    .expect("insert zzz prediction_set");
}

/// 造 zzz model_prediction 行（PIT：available_at = trade_date）。
async fn insert_zzz_predictions(
    db: &sqlx::PgPool,
    set_id: &str,
    days: &[NaiveDate],
    symbols: &[&str],
) {
    for (day_idx, day) in days.iter().enumerate() {
        for (sym_idx, symbol) in symbols.iter().enumerate() {
            sqlx::query(
                    "INSERT INTO model_prediction
                       (prediction_set_id, trade_date, symbol, score, probability, rank, available_at)
                     VALUES ($1, $2, $3, $4, 0.5, $5, $2)",
                )
                .bind(set_id)
                .bind(day)
                .bind(symbol)
                .bind((day_idx * 10 + sym_idx) as f64)
                .bind((sym_idx + 1) as i32)
                .execute(db)
                .await
                .expect("insert zzz model_prediction");
        }
    }
}

/// 造 zzz backtest_task（evaluate 测试的 FK 父行）。
async fn insert_zzz_backtest_task(
    db: &sqlx::PgPool,
    task_id: &str,
    prediction_set_id: Option<&str>,
    status: &str,
) {
    sqlx::query(
        "INSERT INTO backtest_task
               (task_id, strategy_version_id, data_version_id, prediction_set_id,
                benchmark_symbol, symbols, start_date, end_date, initial_capital,
                rebalance_frequency, cost_model, slippage_model, execution_rules,
                parameters, status)
             VALUES ($1, 'zzz_api9m_sv', 'zzz_api9m_dv', $2, '000300.SH', ARRAY['ZZZ901.SH'],
                '2026-01-05', '2026-01-09', 100000, 'monthly',
                '{}', '{}', '{}', '{}', $3)",
    )
    .bind(task_id)
    .bind(prediction_set_id)
    .bind(status)
    .execute(db)
    .await
    .expect("insert zzz backtest_task");
}

fn date(y: i32, m: u32, d: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, d).expect("合法日期")
}

fn econ_input(
    id: &str,
    rows: i64,
    symbols: i64,
    days: i64,
    metadata: Value,
) -> PredictionSetCacheEconomicsInput {
    PredictionSetCacheEconomicsInput {
        prediction_set_id: id.to_string(),
        status: "ready".to_string(),
        start_date: date(2026, 1, 5),
        end_date: date(2026, 1, 9),
        metadata,
        prediction_rows: rows,
        symbol_count: symbols,
        trading_day_count: days,
    }
}

/// 三类遥测全备的 metadata（cache economics complete 分支用）。
fn full_cache_metadata() -> Value {
    json!({
        "feature_matrix_cache": {"scope": "zzz", "row_count": 100},
        "prediction_insert_telemetry": {"mode": "bulk_insert", "row_count": 100},
        "prediction_generation_telemetry": {"elapsed_ms": 120, "progress_pct": 100.0}
    })
}

// ── 纯逻辑：LabelObjective 全变体谓词 ──

#[test]
fn label_objective_variants_parse_roundtrip_and_predicate_flags() {
    // parse：None / 空白 → 默认 FutureReturn；全 10 个字符串逐一命中；非法串报错
    assert_eq!(
        LabelObjective::parse(None).unwrap(),
        LabelObjective::FutureReturn
    );
    assert_eq!(
        LabelObjective::parse(Some("  ")).unwrap(),
        LabelObjective::FutureReturn
    );
    for raw in [
        "future_return",
        "future_excess_return",
        "risk_adjusted_excess_return",
        "quality_adjusted_excess_return",
        "quality_adjusted_risk_adjusted_excess_return",
        "fundamental_quality_adjusted_excess_return",
        "regime_conditional_excess_return",
        "gradient_boosting_excess_return",
        "mlp_excess_return",
        "asymmetric_excess_return",
    ] {
        let parsed = LabelObjective::parse(Some(raw)).unwrap_or_else(|e| panic!("{}", e));
        // as_str 与 parse 互逆
        assert_eq!(parsed.as_str(), raw, "roundtrip 失败：{}", raw);
    }
    let err = LabelObjective::parse(Some("bogus")).unwrap_err();
    assert!(
        err.contains("label_objective must be one of"),
        "实际错误 {}",
        err
    );

    // 谓词矩阵：除 FutureReturn 外全部需要基准
    assert!(!LabelObjective::FutureReturn.requires_benchmark());
    for raw in [
        "future_excess_return",
        "risk_adjusted_excess_return",
        "quality_adjusted_excess_return",
        "quality_adjusted_risk_adjusted_excess_return",
        "fundamental_quality_adjusted_excess_return",
        "regime_conditional_excess_return",
        "gradient_boosting_excess_return",
        "mlp_excess_return",
        "asymmetric_excess_return",
    ] {
        assert!(LabelObjective::parse(Some(raw))
            .unwrap()
            .requires_benchmark());
    }
    // 质量调整 5 变体
    for raw in [
        "quality_adjusted_excess_return",
        "quality_adjusted_risk_adjusted_excess_return",
        "fundamental_quality_adjusted_excess_return",
        "regime_conditional_excess_return",
        "gradient_boosting_excess_return",
    ] {
        assert!(LabelObjective::parse(Some(raw))
            .unwrap()
            .is_quality_adjusted());
    }
    assert!(!LabelObjective::parse(Some("mlp_excess_return"))
        .unwrap()
        .is_quality_adjusted());
    // 单一开关谓词
    assert!(LabelObjective::FundamentalQualityAdjustedExcessReturn.uses_fundamental_quality());
    assert!(!LabelObjective::FutureReturn.uses_fundamental_quality());
    assert!(LabelObjective::RegimeConditionalExcessReturn.uses_regime_conditioning());
    assert!(LabelObjective::GradientBoostingExcessReturn.uses_gradient_boosting());
    assert!(LabelObjective::MlpExcessReturn.uses_mlp());
}

// ── 纯逻辑：市场状态特征与分桶 ──

#[test]
fn benchmark_trailing_regime_feature_scales_clamps_and_guards() {
    // 61 根基准收盘：lookback 取 60 根前的值
    let mut closes = Vec::new();
    for idx in 0..61 {
        closes.push((date(2025, 1, 2) + Duration::days(idx), 100.0));
    }
    let trade_date = closes[60].0;

    // +30% → 0.3/0.1 = 3.0（恰好触顶不裁剪）
    closes[60].1 = 130.0;
    assert!((benchmark_trailing_regime_feature(&closes, trade_date) - 3.0).abs() < 1e-9);
    // +40% → 裁剪到 3.0
    closes[60].1 = 140.0;
    assert!((benchmark_trailing_regime_feature(&closes, trade_date) - 3.0).abs() < 1e-9);
    // -40% → 裁剪到 -3.0
    closes[60].1 = 60.0;
    assert!((benchmark_trailing_regime_feature(&closes, trade_date) + 3.0).abs() < 1e-9);
    // +10% → 1.0
    closes[60].1 = 110.0;
    assert!((benchmark_trailing_regime_feature(&closes, trade_date) - 1.0).abs() < 1e-9);

    // 日期不在序列 → 0
    assert_eq!(
        benchmark_trailing_regime_feature(&closes, date(2030, 1, 1)),
        0.0
    );
    // 当根收盘非正 → 不参与匹配 → 0
    let mut zero_close = closes.clone();
    zero_close[60].1 = 0.0;
    assert_eq!(
        benchmark_trailing_regime_feature(&zero_close, trade_date),
        0.0
    );
    // 历史不足 60 根：回看下标饱和到 0，用首元素计算
    let short = vec![
        (date(2025, 6, 2), 100.0),
        (date(2025, 6, 3), 102.0),
        (date(2025, 6, 4), 101.0),
        (date(2025, 6, 5), 105.0),
        (date(2025, 6, 6), 105.0),
    ];
    // 首元素 100 → 当根 105：+5% → 0.5
    assert!((benchmark_trailing_regime_feature(&short, short[4].0) - 0.5).abs() < 1e-9);
}

#[test]
fn market_regime_tag_partitions_by_trailing_sign() {
    let mut closes = Vec::new();
    for idx in 0..61 {
        closes.push((date(2025, 1, 2) + Duration::days(idx), 100.0));
    }
    let trade_date = closes[60].0;
    closes[60].1 = 120.0;
    assert_eq!(
        MarketRegimeTag::from_benchmark_trailing(&closes, trade_date),
        MarketRegimeTag::Bull
    );
    closes[60].1 = 80.0;
    assert_eq!(
        MarketRegimeTag::from_benchmark_trailing(&closes, trade_date),
        MarketRegimeTag::Bear
    );
    closes[60].1 = 100.0;
    assert_eq!(
        MarketRegimeTag::from_benchmark_trailing(&closes, trade_date),
        MarketRegimeTag::Sideways
    );
}

#[test]
fn split_samples_by_regime_routes_tags_to_buckets() {
    let samples = vec![
        TrainingSample {
            features: vec![1.0],
            label: 0.1,
            regime_tag: Some(MarketRegimeTag::Bull),
        },
        TrainingSample {
            features: vec![2.0],
            label: 0.2,
            regime_tag: Some(MarketRegimeTag::Bear),
        },
        TrainingSample {
            features: vec![3.0],
            label: 0.3,
            regime_tag: Some(MarketRegimeTag::Sideways),
        },
        TrainingSample {
            features: vec![4.0],
            label: 0.4,
            regime_tag: None,
        },
    ];
    let (bull, bear, sideways) = split_samples_by_regime(&samples);
    assert_eq!(bull.len(), 1);
    assert_eq!(bear.len(), 1);
    // 无标签样本归入横盘桶
    assert_eq!(sideways.len(), 2);
    assert_eq!(bull[0].label, 0.1);
    assert_eq!(sideways[1].label, 0.4);
}

#[test]
fn regime_split_model_any_and_score_row_guards() {
    // 小样本训练一个两因子模型（沿用既有测试的数据形态）
    let train_rows: Vec<TrainingSample> = [
        (vec![-1.0, 0.2], -0.02),
        (vec![-0.8, 0.1], -0.01),
        (vec![0.1, 0.5], 0.01),
        (vec![0.2, 0.4], 0.02),
        (vec![0.8, -0.3], 0.08),
        (vec![1.0, -0.2], 0.10),
    ]
    .into_iter()
    .map(|(features, label)| TrainingSample {
        features,
        label,
        regime_tag: None,
    })
    .collect();
    let model = fit_nonlinear_quantile_ranker(&train_rows, 2, 3, 2).expect("训练小模型");

    // 全空模型：any_model false，任意状态打分 None
    let empty_split = RegimeSplitModel {
        bull: None,
        bear: None,
        sideways: None,
        bull_samples: 0,
        bear_samples: 0,
        sideways_samples: 0,
        factor_count: 2,
    };
    assert!(!empty_split.any_model());
    assert_eq!(
        empty_split.score_row(&[0.5, 0.1], MarketRegimeTag::Bull),
        None
    );

    // 仅牛市有模型
    let split = RegimeSplitModel {
        bull: Some(model),
        bear: None,
        sideways: None,
        bull_samples: train_rows.len(),
        bear_samples: 0,
        sideways_samples: 0,
        factor_count: 2,
    };
    assert!(split.any_model());
    // 牛市状态：合法特征出分
    let score = split
        .score_row(&[0.9, -0.25], MarketRegimeTag::Bull)
        .expect("牛市应出分");
    assert!(score.is_finite());
    // 熊市/横盘无模型 → None
    assert_eq!(split.score_row(&[0.9, -0.25], MarketRegimeTag::Bear), None);
    assert_eq!(
        split.score_row(&[0.9, -0.25], MarketRegimeTag::Sideways),
        None
    );
    // 维度不符 / 非有限值 → None
    assert_eq!(split.score_row(&[0.9], MarketRegimeTag::Bull), None);
    assert_eq!(
        split.score_row(&[f64::NAN, 0.1], MarketRegimeTag::Bull),
        None
    );
}

// ── 纯逻辑：cache economics 归一化与工具 ──

#[test]
fn cache_economics_request_trims_dedups_and_caps_at_twenty() {
    let make_req = |ids: Vec<&str>| PredictionSetCacheEconomicsReportRequest {
        prediction_set_ids: ids.into_iter().map(String::from).collect(),
        persist_report: None,
    };
    // trim + 去重 + 丢弃空白
    let req = make_req(vec![" a ", "a", "", "b "]);
    let ids = normalize_prediction_set_cache_economics_request(&req).expect("normalized");
    assert_eq!(ids, vec!["a".to_string(), "b".to_string()]);
    // 全空白 → Err
    let err =
        normalize_prediction_set_cache_economics_request(&make_req(vec!["  ", ""])).unwrap_err();
    assert!(err.contains("must not be empty"), "实际错误 {}", err);
    // 21 个 → Err
    let too_many: Vec<String> = (0..21).map(|idx| format!("s{}", idx)).collect();
    let req = PredictionSetCacheEconomicsReportRequest {
        prediction_set_ids: too_many,
        persist_report: None,
    };
    let err = normalize_prediction_set_cache_economics_request(&req).unwrap_err();
    assert!(err.contains("at most 20"), "实际错误 {}", err);
    // 20 个恰好通过
    let twenty: Vec<String> = (0..20).map(|idx| format!("s{}", idx)).collect();
    let req = PredictionSetCacheEconomicsReportRequest {
        prediction_set_ids: twenty,
        persist_report: None,
    };
    let ids = normalize_prediction_set_cache_economics_request(&req).expect("normalized");
    assert_eq!(ids.len(), 20);
}

#[test]
fn rows_per_trading_day_and_symbol_round_and_guard_zero() {
    let zero_days = econ_input("s1", 100, 10, 0, json!({}));
    assert_eq!(rows_per_trading_day(&zero_days), 0.0);
    let zero_symbols = econ_input("s1", 100, 0, 10, json!({}));
    assert_eq!(rows_per_symbol(&zero_symbols), 0.0);
    let normal = econ_input("s1", 100, 3, 10, json!({}));
    assert_eq!(rows_per_trading_day(&normal), 10.0);
    assert_eq!(rows_per_symbol(&normal), 33.0);
    // 3.5 四舍五入到 4
    let half = econ_input("s1", 7, 2, 2, json!({}));
    assert_eq!(rows_per_trading_day(&half), 4.0);
}

#[test]
fn cache_economics_recommendation_branches_beyond_audit() {
    // 既有测试覆盖 audit_uncached 分支；此处补另两个分支
    // 密集集：10 万行/日 → 建议窗口缓存 + 后台插入
    let dense = econ_input("s-dense", 1_000_000, 3000, 10, full_cache_metadata());
    let report = prediction_set_cache_economics_report_json(&[dense]);
    assert_eq!(
        report["economics"]["recommendation"],
        json!("prefer_window_cache_and_background_insert")
    );
    assert_eq!(
        report["economics"]["max_rows_per_trading_day"],
        json!(100_000.0)
    );
    // 完备小集 → cache_metadata_complete
    let complete = econ_input("s-ok", 30, 3, 10, full_cache_metadata());
    let report = prediction_set_cache_economics_report_json(std::slice::from_ref(&complete));
    assert_eq!(
        report["economics"]["recommendation"],
        json!("cache_metadata_complete")
    );
    // 集合级字段
    assert_eq!(report["prediction_set_count"], json!(1));
    assert_eq!(report["total_prediction_rows"], json!(30));
    // 最大生成耗时跨集聚合
    let slow = econ_input(
        "s-slow",
        10,
        2,
        5,
        json!({"prediction_generation_telemetry": {"elapsed_ms": 900}}),
    );
    let report = prediction_set_cache_economics_report_json(&[complete, slow]);
    assert_eq!(report["economics"]["max_generation_elapsed_ms"], json!(900));
    // 慢集缺缓存元数据 → 建议回落 audit 分支（审计优先级最高）
    assert_eq!(
        report["economics"]["recommendation"],
        json!("audit_uncached_prediction_sets")
    );
}

// ── 纯逻辑：readiness 工具族 ──

#[test]
fn percentile_disc_i64_empty_bounds_and_interpolation_index() {
    assert_eq!(percentile_disc_i64(&[], 0.95), 0);
    let sorted = [10, 20, 30];
    assert_eq!(percentile_disc_i64(&sorted, 0.0), 10);
    // ceil(3*0.5)-1 = 1
    assert_eq!(percentile_disc_i64(&sorted, 0.5), 20);
    assert_eq!(percentile_disc_i64(&sorted, 0.95), 30);
    assert_eq!(percentile_disc_i64(&sorted, 1.0), 30);
}

#[test]
fn readiness_ratio_guards_denominator_and_negative_numerator() {
    // 分母非正 → 视为全覆盖（1.0），负分子截 0
    assert_eq!(readiness_ratio(0, 0), 1.0);
    assert_eq!(readiness_ratio(5, 0), 1.0);
    assert_eq!(readiness_ratio(3, -2), 1.0);
    assert_eq!(readiness_ratio(-3, 10), 0.0);
    assert!((readiness_ratio(3, 10) - 0.3).abs() < 1e-9);
}

#[test]
fn readiness_gate_emits_complete_shape() {
    let gate = readiness_gate("zzz_gate", true, json!(5), json!(3), "zzz 说明");
    assert_eq!(gate["gate"], json!("zzz_gate"));
    assert_eq!(gate["passed"], json!(true));
    assert_eq!(gate["actual"], json!(5));
    assert_eq!(gate["expected"], json!(3));
    assert_eq!(gate["detail"], json!("zzz 说明"));
}

#[test]
fn parse_readiness_date_accepts_compact_and_iso_only() {
    assert_eq!(parse_readiness_date(None, "start_date").unwrap(), None);
    assert_eq!(
        parse_readiness_date(Some("20260105"), "start_date").unwrap(),
        Some(date(2026, 1, 5))
    );
    assert_eq!(
        parse_readiness_date(Some(" 2026-01-05 "), "start_date").unwrap(),
        Some(date(2026, 1, 5))
    );
    let err = parse_readiness_date(Some("2026/01/05"), "start_date").unwrap_err();
    assert!(err.contains("YYYYMMDD or YYYY-MM-DD"), "实际错误 {}", err);
    assert!(err.contains("start_date"));
}

#[test]
fn prediction_readiness_passed_extracts_bool_strictly() {
    assert!(prediction_readiness_passed(&json!({"passed": true})));
    assert!(!prediction_readiness_passed(&json!({"passed": false})));
    assert!(!prediction_readiness_passed(&json!({})));
    assert!(!prediction_readiness_passed(&json!({"passed": "yes"})));
}

// ── 纯逻辑：evaluation 状态聚合 ──

#[test]
fn prediction_evaluation_status_requires_all_gates_passed() {
    let all_passed = json!([
        {"gate": "min_trade_count", "passed": true},
        {"gate": "max_drawdown", "passed": true}
    ]);
    assert_eq!(
        prediction_evaluation_status(&all_passed),
        "approved_candidate"
    );
    let one_failed = json!([
        {"gate": "min_trade_count", "passed": true},
        {"gate": "max_drawdown", "passed": false}
    ]);
    assert_eq!(prediction_evaluation_status(&one_failed), "review_required");
    // 非数组 / 缺 passed 字段 → review_required
    assert_eq!(prediction_evaluation_status(&json!({})), "review_required");
    let no_flag = json!([{"gate": "min_trade_count"}]);
    assert_eq!(prediction_evaluation_status(&no_flag), "review_required");
}

// ── 纯逻辑：非线性请求边界 ──

fn base_nonlinear_req() -> TrainNonlinearQuantileRankerRequest {
    TrainNonlinearQuantileRankerRequest {
        model_code: "zzz_ranker".into(),
        model_version: "v9".into(),
        model_version_id: None,
        training_task_id: None,
        prediction_set_id: None,
        data_version_id: "zzz_api9m_dv".into(),
        feature_set_version_id: "zzz_fsv".into(),
        training_dataset_id: "zzz_ds".into(),
        train_start_date: "20250101".into(),
        train_end_date: "20251231".into(),
        prediction_start_date: "20260105".into(),
        prediction_end_date: "20260109".into(),
        label_horizon_days: None,
        label_objective: None,
        bucket_count: None,
        min_samples_per_bucket: None,
        factors: vec![LinearFactorRef {
            factor_code: "zzz_factor".into(),
            factor_version: "1.0.0".into(),
        }],
    }
}

#[test]
fn nonlinear_ranker_request_bucket_bounds_and_defaults() {
    // 默认：bucket 5 / min_samples 100，id 继承 linear 默认拼接
    let normalized =
        normalize_nonlinear_quantile_ranker_request(&base_nonlinear_req()).expect("normalized");
    assert_eq!(normalized.bucket_count, 5);
    assert_eq!(normalized.min_samples_per_bucket, 100);
    assert_eq!(normalized.model_version_id, "zzz_ranker@v9");
    assert_eq!(
        normalized.prediction_set_id,
        "pred-zzz_ranker-v9-20260105-20260109"
    );
    // bucket_count 边界：1 与 21 拒绝；2 与 20 接受
    let mut req = base_nonlinear_req();
    req.bucket_count = Some(1);
    let err = normalize_nonlinear_quantile_ranker_request(&req).unwrap_err();
    assert!(err.contains("between 2 and 20"), "实际错误 {}", err);
    let mut req = base_nonlinear_req();
    req.bucket_count = Some(21);
    assert!(normalize_nonlinear_quantile_ranker_request(&req).is_err());
    let mut req = base_nonlinear_req();
    req.bucket_count = Some(2);
    let normalized = normalize_nonlinear_quantile_ranker_request(&req).expect("normalized");
    assert_eq!(normalized.bucket_count, 2);
    let mut req = base_nonlinear_req();
    req.bucket_count = Some(20);
    let normalized = normalize_nonlinear_quantile_ranker_request(&req).expect("normalized");
    assert_eq!(normalized.bucket_count, 20);
    // min_samples_per_bucket = 0 拒绝
    let mut req = base_nonlinear_req();
    req.min_samples_per_bucket = Some(0);
    let err = normalize_nonlinear_quantile_ranker_request(&req).unwrap_err();
    assert!(
        err.contains("min_samples_per_bucket must be positive"),
        "实际错误 {}",
        err
    );
    // linear 侧错误透传（日期非法）
    let mut req = base_nonlinear_req();
    req.prediction_end_date = "not-a-date".into();
    assert!(normalize_nonlinear_quantile_ranker_request(&req).is_err());
}

#[test]
fn nonlinear_ranker_experiment_config_and_metrics_shape() {
    let normalized =
        normalize_nonlinear_quantile_ranker_request(&base_nonlinear_req()).expect("normalized");
    let config = nonlinear_quantile_ranker_experiment_config(&normalized);
    assert_eq!(config["trainer"], json!("nonlinear_quantile_ranker_v1"));
    assert_eq!(config["bucket_count"], json!(5));
    assert_eq!(config["min_samples_per_bucket"], json!(100));
    assert_eq!(config["label"]["type"], json!("future_return"));
    assert_eq!(config["label"]["benchmark"], json!(null));
    assert_eq!(config["data_version_id"], json!("zzz_api9m_dv"));

    let model = json!({"factor_count": 1});
    let metrics = nonlinear_quantile_ranker_experiment_metrics(
        120,
        60,
        &model,
        "hash-a",
        "hash-p",
        &json!({"row_count": 90}),
        &json!({"row_count": 60}),
        &json!({"elapsed_ms": 5}),
    );
    assert_eq!(metrics["sample_count"], json!(120));
    assert_eq!(metrics["prediction_rows"], json!(60));
    assert_eq!(metrics["artifact_hash"], json!("hash-a"));
    assert_eq!(metrics["prediction_hash"], json!("hash-p"));
    assert_eq!(
        metrics["status"],
        json!("training_and_prediction_completed")
    );
}

#[test]
fn walk_forward_nonlinear_config_nests_linear_fields_and_label() {
    let req = WalkForwardNonlinearQuantileRankerRequest {
        model_code: "zzz_wf".into(),
        model_version: "v9".into(),
        model_version_id: Some("zzz_api9m_wf_mv".into()),
        training_task_id: None,
        prediction_set_id: None,
        data_version_id: "zzz_api9m_dv".into(),
        feature_set_version_id: "zzz_fsv".into(),
        training_dataset_id: "zzz_ds".into(),
        prediction_start_date: "20260105".into(),
        prediction_end_date: "20260430".into(),
        train_lookback_days: Some(252),
        prediction_step_days: Some(21),
        label_horizon_days: Some(5),
        label_objective: Some("risk_adjusted_excess_return".into()),
        min_training_samples: Some(50),
        max_windows: Some(3),
        bucket_count: Some(4),
        min_samples_per_bucket: Some(40),
        factors: vec![LinearFactorRef {
            factor_code: "zzz_factor".into(),
            factor_version: "1.0.0".into(),
        }],
    };
    let normalized =
        normalize_walk_forward_nonlinear_quantile_ranker_request(&req).expect("normalized");
    // 嵌套 linear：lookback/step/horizon/min_samples 透传
    assert_eq!(normalized.linear.train_lookback_days, 252);
    assert_eq!(normalized.linear.prediction_step_days, 21);
    assert_eq!(normalized.linear.label_horizon_days, 5);
    assert_eq!(normalized.linear.min_training_samples, 50);
    assert_eq!(normalized.bucket_count, 4);
    assert_eq!(normalized.min_samples_per_bucket, 40);

    let config = walk_forward_nonlinear_quantile_ranker_experiment_config(&normalized);
    assert_eq!(
        config["trainer"],
        json!("walk_forward_nonlinear_quantile_ranker_v1")
    );
    assert_eq!(config["model_version_id"], json!("zzz_api9m_wf_mv"));
    assert_eq!(config["train_lookback_days"], json!(252));
    assert_eq!(config["prediction_step_days"], json!(21));
    assert_eq!(config["bucket_count"], json!(4));
    assert_eq!(config["min_samples_per_bucket"], json!(40));
    // label 定义含风险调整与基准
    assert_eq!(
        config["label"]["label"],
        json!("risk_adjusted_excess_return")
    );
    assert_eq!(config["label"]["benchmark"], json!("000300.SH"));
    assert_eq!(
        config["label"]["risk_adjustment"],
        json!("forward_downside_volatility_floor_1pct")
    );
}

// ── 纯逻辑：杂项工具 ──

#[test]
fn progress_pct_zero_total_is_complete_and_rounds_two_decimals() {
    assert_eq!(progress_pct(0, 0), 100.0);
    assert_eq!(progress_pct(10, 3), 30.0);
    // 完成数超总数 → 饱和 100
    assert_eq!(progress_pct(10, 15), 100.0);
    assert!((progress_pct(3, 1) - 33.33).abs() < 1e-9);
    // 阶段遥测：stage 形态 + 进度联动
    let stage = prediction_progress_stage("zzz_stage", 4, 1, 30, StdDuration::from_millis(25));
    assert_eq!(stage["stage"], json!("zzz_stage"));
    assert_eq!(stage["total_units"], json!(4));
    assert_eq!(stage["completed_units"], json!(1));
    assert_eq!(stage["row_count"], json!(30));
    assert_eq!(stage["elapsed_ms"], json!(25));
    assert!((stage["progress_pct"].as_f64().unwrap() - 25.0).abs() < 1e-9);
}

#[test]
fn parse_yyyymmdd_is_compact_only_and_prediction_row_requires_iso() {
    // 训练/预测请求侧：严格 YYYYMMDD
    assert_eq!(
        parse_yyyymmdd("20260105", "start_date").unwrap(),
        date(2026, 1, 5)
    );
    let err = parse_yyyymmdd("2026-01-05", "start_date").unwrap_err();
    assert!(err.contains("YYYYMMDD"), "实际错误 {}", err);
    // 预测行构建侧：严格 YYYY-MM-DD（walk-forward 内部日期序列化形态）
    let row = build_prediction_row("ps", "000001.SZ", "2026-01-05", 0.5, 1).expect("row");
    assert_eq!(row.trade_date, date(2026, 1, 5));
    let err = build_prediction_row("ps", "000001.SZ", "20260105", 0.5, 1).unwrap_err();
    assert!(err.contains("YYYY-MM-DD"), "实际错误 {}", err);
}

// ── DB：data_version 存在性 ──

#[tokio::test]
async fn ensure_data_version_exists_ok_and_actionable_error() {
    let db = test_db().await;
    seed_shared_parents(&db).await;
    ensure_data_version_exists(&db, "zzz_api9m_dv")
        .await
        .expect("存在的 data_version 应通过");
    let err = ensure_data_version_exists(&db, "zzz_api9m_missing_dv")
        .await
        .unwrap_err();
    assert!(err.contains("zzz_api9m_missing_dv"), "实际错误 {}", err);
    assert!(err.contains("does not exist in data_version"));
}

// ── DB：readiness 期望开市日 ──

#[tokio::test]
async fn readiness_expected_open_day_count_calendar_and_empty_fallback() {
    let db = test_db().await;
    // 真实日历命中：2026-01 上旬连续窗口的 DISTINCT 开市日数
    let days: Vec<NaiveDate> = sqlx::query_scalar(
        "SELECT DISTINCT trade_date FROM market_trade_calendar \
             WHERE is_open AND trade_date >= '2026-01-05' AND trade_date <= '2026-01-31' \
             ORDER BY trade_date LIMIT 5",
    )
    .fetch_all(&db)
    .await
    .expect("查询真实交易日");
    assert_eq!(days.len(), 5, "2026-01 上旬应有至少 5 个开市日");
    let count = readiness_expected_open_day_count(&db, days[0], days[4])
        .await
        .expect("calendar 分支");
    assert_eq!(count, 5, "窗口边界即首末日 → 期望恰好 5 天");
    // 远未来：日历与指数 bar 双双无数据 → 0（分母保护前提）
    let count = readiness_expected_open_day_count(&db, date(2099, 1, 1), date(2099, 1, 31))
        .await
        .expect("空窗口兜底");
    assert_eq!(count, 0);
}

// ── DB：readiness 报告全门禁 ──

#[tokio::test]
async fn prediction_set_readiness_report_green_then_red_gates() {
    let db = test_db().await;
    seed_shared_parents(&db).await;
    let scope = "readiness";
    cleanup_scoped(&db, scope).await;

    // 动态取真实开市日，保证覆盖率分母精确
    let days: Vec<NaiveDate> = sqlx::query_scalar(
        "SELECT DISTINCT trade_date FROM market_trade_calendar \
             WHERE is_open AND trade_date >= '2026-01-05' AND trade_date <= '2026-01-31' \
             ORDER BY trade_date LIMIT 5",
    )
    .fetch_all(&db)
    .await
    .expect("查询真实交易日");
    let (start, end) = (days[0], days[4]);

    let set_id = "zzz_api9m_readiness_ps1";
    insert_zzz_prediction_set(
        &db,
        set_id,
        "ready",
        start,
        end,
        Some(start - Duration::days(1)),
        json!({"zzz": "readiness"}),
    )
    .await;
    insert_zzz_predictions(&db, set_id, &days, &["ZZZR01.SH", "ZZZR02.SH", "ZZZR03.SH"]).await;

    let make_req = |set: &str| {
        serde_json::from_value::<PredictionSetReadinessRequest>(json!({
            "prediction_set_id": set,
            "start_date": start.to_string(),
            "end_date": end.to_string(),
            "min_daily_rows": 1,
            "min_p95_daily_row_ratio": 0.05,
            "persist_report": false
        }))
        .expect("反序列化 readiness 请求")
    };
    let result = build_prediction_set_readiness_report_from_request(&db, &make_req(set_id))
        .await
        .expect("readiness 报告");
    let report = &result["report"];
    // 七道门禁全绿
    assert_eq!(report["passed"], json!(true), "报告：{}", report);
    assert_eq!(report["level"], json!("green"));
    assert_eq!(report["readiness_type"], json!("prediction_set"));
    let gates = report["gates"].as_array().expect("gates");
    assert_eq!(gates.len(), 7);
    for gate in gates {
        assert_eq!(gate["passed"], json!(true), "未过门禁：{}", gate);
    }
    // 汇总计数：5 天 × 3 symbol
    assert_eq!(report["summary"]["expected_open_days"], json!(5));
    assert_eq!(report["summary"]["actual_prediction_days"], json!(5));
    assert_eq!(report["summary"]["day_coverage_ratio"], json!(1.0));
    assert_eq!(report["summary"]["prediction_rows"], json!(15));
    assert_eq!(report["summary"]["symbol_count"], json!(3));
    assert_eq!(report["summary"]["future_leak_rows"], json!(0));
    // persist_report=false → experiment_run_id 为 null（不落库）
    assert_eq!(result["experiment_run_id"], json!(null));

    // 红门禁一：泄漏行（available_at 晚于 trade_date）
    sqlx::query(
        "UPDATE model_prediction SET available_at = trade_date + 1 \
                     WHERE prediction_set_id = $1 AND trade_date = $2",
    )
    .bind(set_id)
    .bind(end)
    .execute(&db)
    .await
    .expect("注入泄漏行");
    let result = build_prediction_set_readiness_report_from_request(&db, &make_req(set_id))
        .await
        .expect("readiness 报告");
    let report = &result["report"];
    assert_eq!(report["passed"], json!(false));
    assert_eq!(report["level"], json!("red"));
    assert_eq!(report["summary"]["future_leak_rows"], json!(3));
    let leak_gate = report["gates"]
        .as_array()
        .expect("gates")
        .iter()
        .find(|g| g["gate"] == json!("prediction_future_leak_rows"))
        .expect("泄漏门禁");
    assert_eq!(leak_gate["passed"], json!(false));

    // 红门禁二：status 非 ready
    let set2 = "zzz_api9m_readiness_ps2";
    insert_zzz_prediction_set(&db, set2, "pending", start, end, None, json!({})).await;
    insert_zzz_predictions(&db, set2, &days, &["ZZZR01.SH"]).await;
    let result = build_prediction_set_readiness_report_from_request(&db, &make_req(set2))
        .await
        .expect("readiness 报告");
    let report = &result["report"];
    let status_gate = report["gates"]
        .as_array()
        .expect("gates")
        .iter()
        .find(|g| g["gate"] == json!("prediction_set_status_ready"))
        .expect("状态门禁");
    assert_eq!(status_gate["passed"], json!(false));
    assert_eq!(report["passed"], json!(false));
    // 单 symbol/天 → p50=1 >= min_daily_rows 1 过，但 p95 弱日阈值仍 1 → cliff 门过
    // （此集只验证 status 门禁的独立性）
    assert_eq!(status_gate["actual"], json!("pending"));

    // 红门禁三：窗口倒置直接 Err
    let inverted = serde_json::from_value::<PredictionSetReadinessRequest>(json!({
        "prediction_set_id": set_id,
        "start_date": end.to_string(),
        "end_date": start.to_string(),
        "persist_report": false
    }))
    .expect("反序列化");
    let err = build_prediction_set_readiness_report_from_request(&db, &inverted)
        .await
        .unwrap_err();
    assert!(err.contains("cannot be after"), "实际错误 {}", err);

    // 空 prediction_set_id
    let empty_req = serde_json::from_value::<PredictionSetReadinessRequest>(json!({
        "prediction_set_id": "  ",
        "persist_report": false
    }))
    .expect("反序列化");
    let err = build_prediction_set_readiness_report_from_request(&db, &empty_req)
        .await
        .unwrap_err();
    assert!(err.contains("must not be empty"), "实际错误 {}", err);

    // 不存在的 set
    let err = build_prediction_set_readiness_report_from_request(
        &db,
        &make_req("zzz_api9m_readiness_missing"),
    )
    .await
    .unwrap_err();
    assert!(err.contains("not found"), "实际错误 {}", err);

    // persist=true：报告落 experiment_run（type/related 指向 prediction_set）
    sqlx::query(
        "UPDATE model_prediction SET available_at = trade_date \
                     WHERE prediction_set_id = $1 AND trade_date = $2",
    )
    .bind(set_id)
    .bind(end)
    .execute(&db)
    .await
    .expect("恢复 PIT 行");
    let persist_req = serde_json::from_value::<PredictionSetReadinessRequest>(json!({
        "prediction_set_id": set_id,
        "persist_report": true
    }))
    .expect("反序列化");
    let result = build_prediction_set_readiness_report_from_request(&db, &persist_req)
        .await
        .expect("persist 报告");
    let run_id = result["experiment_run_id"].as_str().expect("run id");
    let (run_type, related, status): (String, String, String) = sqlx::query_as(
        "SELECT experiment_type, related_entity_id, status \
             FROM experiment_run WHERE experiment_run_id = $1",
    )
    .bind(run_id)
    .fetch_one(&db)
    .await
    .expect("回读 experiment_run");
    assert_eq!(run_type, "prediction_set_readiness_report");
    assert_eq!(related, set_id);
    assert_eq!(status, "completed");

    cleanup_scoped(&db, scope).await;
}

// ── DB：evaluation 全链门禁 ──

#[tokio::test]
async fn evaluate_prediction_set_inner_gates_and_experiment_run() {
    let db = test_db().await;
    seed_shared_parents(&db).await;
    let scope = "eval";
    cleanup_scoped(&db, scope).await;

    let set_id = "zzz_api9m_eval_ps1";
    let bt_task = "zzz_api9m_eval_bt1";
    insert_zzz_prediction_set(
        &db,
        set_id,
        "ready",
        date(2026, 1, 5),
        date(2026, 1, 9),
        None,
        json!({}),
    )
    .await;
    insert_zzz_predictions(
        &db,
        set_id,
        &[date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7)],
        &["ZZZE01.SH", "ZZZE02.SH"],
    )
    .await;
    insert_zzz_backtest_task(&db, bt_task, Some(set_id), "completed").await;
    sqlx::query(
        "INSERT INTO backtest_result
               (result_id, task_id, total_return, excess_return, max_drawdown,
                total_trades, reproducibility_hash)
             VALUES ('zzz_api9m_eval_r1', $1, 0.20, 0.05, 0.10, 50, 'zzz-api9m')",
    )
    .bind(bt_task)
    .execute(&db)
    .await
    .expect("insert zzz backtest_result");
    for (idx, symbol) in ["ZZZE01.SH", "ZZZE02.SH"].iter().enumerate() {
        sqlx::query(
            "INSERT INTO portfolio_target
                   (task_id, trade_date, symbol, target_weight)
                 VALUES ($1, '2026-01-05', $2, $3)",
        )
        .bind(bt_task)
        .bind(symbol)
        .bind(0.5 - idx as f64 * 0.1)
        .execute(&db)
        .await
        .expect("insert zzz portfolio_target");
    }

    let req = EvaluatePredictionSetRequest {
        prediction_set_id: set_id.into(),
        backtest_task_id: bt_task.into(),
        min_trade_count: None,
        max_drawdown: None,
        min_excess_return: None,
    };
    let result = evaluate_prediction_set_inner(&db, req)
        .await
        .expect("evaluation");
    // 默认门禁：50 笔 >= 1、回撤 0.10 <= 0.20、超额 0.05 >= 0 → approved
    assert_eq!(result["status"], json!("approved_candidate"));
    assert_eq!(result["metrics"]["prediction_rows"], json!(6));
    assert_eq!(result["metrics"]["prediction_symbol_count"], json!(2));
    assert_eq!(result["metrics"]["trade_count"], json!(50));
    assert_eq!(result["metrics"]["target_rows"], json!(2));
    assert_eq!(result["metrics"]["backtest_status"], json!("completed"));
    // experiment_run 落库（type 固定、related 指向 prediction_set）
    let run_id = result["experiment_run_id"].as_str().expect("run id");
    let (run_type, related, status): (String, String, String) = sqlx::query_as(
        "SELECT experiment_type, related_entity_id, status \
             FROM experiment_run WHERE experiment_run_id = $1",
    )
    .bind(run_id)
    .fetch_one(&db)
    .await
    .expect("回读 experiment_run");
    assert_eq!(run_type, "ml_prediction_backtest_gate");
    assert_eq!(related, set_id);
    assert_eq!(status, "completed");

    // 守卫：backtest_task 不存在
    let req = EvaluatePredictionSetRequest {
        prediction_set_id: set_id.into(),
        backtest_task_id: "zzz_api9m_eval_missing_bt".into(),
        min_trade_count: None,
        max_drawdown: None,
        min_excess_return: None,
    };
    let err = evaluate_prediction_set_inner(&db, req).await.unwrap_err();
    assert_eq!(err, "backtest_task not found");

    // 守卫：backtest_task 关联的 prediction_set 不匹配
    let bt_other = "zzz_api9m_eval_bt2";
    insert_zzz_backtest_task(&db, bt_other, None, "completed").await;
    let req = EvaluatePredictionSetRequest {
        prediction_set_id: set_id.into(),
        backtest_task_id: bt_other.into(),
        min_trade_count: None,
        max_drawdown: None,
        min_excess_return: None,
    };
    let err = evaluate_prediction_set_inner(&db, req).await.unwrap_err();
    assert!(err.contains("does not match"), "实际错误 {}", err);

    // 门禁收紧 → review_required（超额门槛抬到 0.10 > 实际 0.05）
    let req = EvaluatePredictionSetRequest {
        prediction_set_id: set_id.into(),
        backtest_task_id: bt_task.into(),
        min_trade_count: None,
        max_drawdown: None,
        min_excess_return: Some(0.10),
    };
    let result = evaluate_prediction_set_inner(&db, req)
        .await
        .expect("evaluation");
    assert_eq!(result["status"], json!("review_required"));

    cleanup_scoped(&db, scope).await;
}

// ── DB：cache economics persist 开关 ──

#[tokio::test]
async fn cache_economics_report_persist_toggle_and_missing_set() {
    let db = test_db().await;
    seed_shared_parents(&db).await;
    let scope = "cache";
    cleanup_scoped(&db, scope).await;

    let set_id = "zzz_api9m_cache_ps1";
    insert_zzz_prediction_set(
        &db,
        set_id,
        "ready",
        date(2026, 1, 5),
        date(2026, 1, 9),
        None,
        full_cache_metadata(),
    )
    .await;

    // persist=false：出报告不落库
    let req = PredictionSetCacheEconomicsReportRequest {
        prediction_set_ids: vec![set_id.into()],
        persist_report: Some(false),
    };
    let result = build_prediction_set_cache_economics_report(&db, &req)
        .await
        .expect("cache economics 报告");
    assert_eq!(result["experiment_run_id"], json!(null));
    assert_eq!(
        result["report"]["economics"]["recommendation"],
        json!("cache_metadata_complete")
    );
    let runs: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM experiment_run WHERE related_entity_id = $1")
            .bind(set_id)
            .fetch_one(&db)
            .await
            .expect("count experiment_run");
    assert_eq!(runs, 0, "persist=false 不得写 experiment_run");

    // persist=true（默认）：落库 completed
    let req = PredictionSetCacheEconomicsReportRequest {
        prediction_set_ids: vec![set_id.into()],
        persist_report: None,
    };
    let result = build_prediction_set_cache_economics_report(&db, &req)
        .await
        .expect("cache economics 报告");
    let run_id = result["experiment_run_id"].as_str().expect("run id");
    let (run_type, related, status): (String, String, String) = sqlx::query_as(
        "SELECT experiment_type, related_entity_id, status \
             FROM experiment_run WHERE experiment_run_id = $1",
    )
    .bind(run_id)
    .fetch_one(&db)
    .await
    .expect("回读 experiment_run");
    assert_eq!(run_type, "prediction_set_cache_economics_report");
    assert_eq!(related, set_id);
    assert_eq!(status, "completed");

    // 不存在的 set → Err 指明 id
    let req = PredictionSetCacheEconomicsReportRequest {
        prediction_set_ids: vec!["zzz_api9m_cache_missing".into()],
        persist_report: Some(false),
    };
    let err = build_prediction_set_cache_economics_report(&db, &req)
        .await
        .unwrap_err();
    assert!(err.contains("prediction_set not found"), "实际错误 {}", err);
    assert!(err.contains("zzz_api9m_cache_missing"));

    cleanup_scoped(&db, scope).await;
}

// ── DB：linear prediction 错误路径 ──

#[tokio::test]
async fn create_linear_prediction_set_errors_on_missing_version_and_factors() {
    let db = test_db().await;
    seed_shared_parents(&db).await;
    let scope = "linsmoke";
    cleanup_scoped(&db, scope).await;

    let make_req = |dv: &str, factor: &str| LinearPredictionSetRequest {
        model_code: "zzz_lin".into(),
        model_version: "v9".into(),
        model_version_id: None,
        prediction_set_id: Some("zzz_api9m_linsmoke_ps1".into()),
        data_version_id: dv.into(),
        feature_set_version_id: "zzz_fsv".into(),
        training_dataset_id: "zzz_api9m_linsmoke_ds1".into(),
        start_date: "20260105".into(),
        end_date: "20260109".into(),
        factors: vec![LinearFactorWeight {
            factor_code: factor.into(),
            factor_version: "1.0.0".into(),
            weight: 0.5,
        }],
    };

    // data_version 不存在：在写任何表之前被拦
    let err =
        create_linear_prediction_set_inner(&db, make_req("zzz_api9m_missing_dv", "zzz_factor"))
            .await
            .unwrap_err();
    assert!(
        err.contains("does not exist in data_version"),
        "实际错误 {}",
        err
    );

    // data_version 存在但因子无数据：rows 为空 → 报错且零表写入
    let err = create_linear_prediction_set_inner(
        &db,
        make_req("zzz_api9m_dv", "zzz_api9m_no_such_factor"),
    )
    .await
    .unwrap_err();
    assert!(err.contains("no factor values"), "实际错误 {}", err);
    // 三张登记表均未落行（错误路径在事务开始前返回）
    for sql in [
            "SELECT COUNT(*) FROM prediction_set WHERE prediction_set_id = 'zzz_api9m_linsmoke_ps1'",
            "SELECT COUNT(*) FROM model_registry WHERE model_version_id LIKE 'zzz_lin@v9'",
            "SELECT COUNT(*) FROM training_dataset WHERE training_dataset_id = 'zzz_api9m_linsmoke_ds1'",
        ] {
            let count: i64 = sqlx::query_scalar(sql)
                .fetch_one(&db)
                .await
                .expect("count");
            assert_eq!(count, 0, "错误路径不应落库：{}", sql);
        }

    cleanup_scoped(&db, scope).await;
}
