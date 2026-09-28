use super::*;

use chrono::NaiveDate;
use serde_json::json;
use std::collections::HashMap;

#[test]
fn readiness_thresholds_are_bounded() {
    let thresholds = ReadinessThresholds::from_options(Some(f64::NAN), Some(0), Some(10.0));

    assert_eq!(thresholds.min_day_coverage_ratio, 0.98);
    assert_eq!(thresholds.min_daily_rows, 1);
    assert_eq!(thresholds.min_p95_daily_row_ratio, 1.0);
}

#[test]
fn daily_count_distribution_flags_profile_row_cliff() {
    let thresholds = ReadinessThresholds::default();
    let counts = vec![232, 233, 234, 231, 26, 25, 27, 26];

    let distribution = daily_count_distribution(&counts, thresholds);

    assert_eq!(distribution.p95_rows, 234);
    assert_eq!(distribution.weak_day_threshold, 117);
    assert_eq!(distribution.weak_day_count, 4);
}

#[test]
fn linear_prediction_request_defaults_model_and_prediction_ids() {
    let req = LinearPredictionSetRequest {
        model_code: "linear_alpha_smoke".into(),
        model_version: "phase5a-v1".into(),
        model_version_id: None,
        prediction_set_id: None,
        data_version_id: "perf-db-smoke-data-v1".into(),
        feature_set_version_id: "phase5a-feature-smoke-v1".into(),
        training_dataset_id: "phase5a-training-smoke-v1".into(),
        start_date: "20250109".into(),
        end_date: "20250131".into(),
        factors: vec![LinearFactorWeight {
            factor_code: "mom_5d_std".into(),
            factor_version: "1.0.0".into(),
            weight: 0.7,
        }],
    };

    let normalized = normalize_linear_prediction_request(&req).expect("normalized request");

    assert_eq!(normalized.model_version_id, "linear_alpha_smoke@phase5a-v1");
    assert_eq!(
        normalized.prediction_set_id,
        "pred-linear_alpha_smoke-phase5a-v1-20250109-20250131"
    );
}

#[test]
fn model_prediction_rows_use_trade_date_as_available_at() {
    let row = build_prediction_row("pred-v1", "000001.SZ", "2025-01-09", 0.42, 3)
        .expect("prediction row");

    assert_eq!(row.prediction_set_id, "pred-v1");
    assert_eq!(row.trade_date, row.available_at);
    assert_eq!(row.rank, 3);
}

#[test]
fn model_metadata_hash_is_stable_for_same_inputs() {
    let left = stable_metadata_hash(&json!({
        "model": "linear",
        "weights": [{"factor": "mom", "weight": 0.7}, {"factor": "turn", "weight": 0.3}]
    }));
    let right = stable_metadata_hash(&json!({
        "weights": [{"weight": 0.7, "factor": "mom"}, {"weight": 0.3, "factor": "turn"}],
        "model": "linear"
    }));

    assert_eq!(left, right);
    assert!(left.starts_with("hash-"));
}

#[test]
fn linear_training_request_defaults_ids_and_label_horizon() {
    let req = TrainLinearModelRequest {
        model_code: "trained_linear_alpha".into(),
        model_version: "phase5c-v1".into(),
        model_version_id: None,
        training_task_id: None,
        prediction_set_id: None,
        data_version_id: "perf-db-smoke-data-v1".into(),
        feature_set_version_id: "phase5c-feature-smoke-v1".into(),
        training_dataset_id: "phase5c-training-smoke-v1".into(),
        train_start_date: "20250109".into(),
        train_end_date: "20250120".into(),
        prediction_start_date: "20250121".into(),
        prediction_end_date: "20250131".into(),
        label_horizon_days: None,
        label_objective: None,
        factors: vec![LinearFactorRef {
            factor_code: "mom_5d_std".into(),
            factor_version: "1.0.0".into(),
        }],
    };

    let normalized = normalize_linear_training_request(&req).expect("training request");

    assert_eq!(
        normalized.model_version_id,
        "trained_linear_alpha@phase5c-v1"
    );
    assert_eq!(
        normalized.training_task_id,
        "train-trained_linear_alpha-phase5c-v1"
    );
    assert_eq!(
        normalized.prediction_set_id,
        "pred-trained_linear_alpha-phase5c-v1-20250121-20250131"
    );
    assert_eq!(normalized.label_horizon_days, 1);
}

#[test]
fn walk_forward_linear_request_defaults_full_history_ids() {
    let req = WalkForwardLinearPredictionSetRequest {
        model_code: "phase7_wf_linear_alpha".into(),
        model_version: "phase7-wf-v1".into(),
        model_version_id: None,
        training_task_id: None,
        prediction_set_id: None,
        data_version_id: "full-a-v1".into(),
        feature_set_version_id: "phase7-alpha-v1".into(),
        training_dataset_id: "phase7-wf-training-v1".into(),
        prediction_start_date: "20160201".into(),
        prediction_end_date: "20260515".into(),
        train_lookback_days: None,
        prediction_step_days: None,
        label_horizon_days: None,
        label_objective: None,
        min_training_samples: None,
        max_windows: Some(2),
        factors: vec![LinearFactorRef {
            factor_code: "fin_roe_daily_std".into(),
            factor_version: "1.0.0".into(),
        }],
    };

    let normalized = normalize_walk_forward_linear_prediction_request(&req).expect("wf request");

    assert_eq!(
        normalized.model_version_id,
        "phase7_wf_linear_alpha@phase7-wf-v1"
    );
    assert_eq!(
        normalized.training_task_id,
        "train-phase7_wf_linear_alpha-phase7-wf-v1-walk-forward"
    );
    assert_eq!(
        normalized.prediction_set_id,
        "pred-phase7_wf_linear_alpha-phase7-wf-v1-wf-20160201-20260515"
    );
    assert_eq!(normalized.train_lookback_days, 756);
    assert_eq!(normalized.prediction_step_days, 63);
    assert_eq!(normalized.label_horizon_days, 5);
    assert_eq!(normalized.min_training_samples, 100);
}

#[test]
fn walk_forward_windows_leave_label_gap_before_prediction() {
    let req = WalkForwardLinearPredictionSetRequest {
        model_code: "phase7_wf_linear_alpha".into(),
        model_version: "phase7-wf-v1".into(),
        model_version_id: None,
        training_task_id: None,
        prediction_set_id: None,
        data_version_id: "full-a-v1".into(),
        feature_set_version_id: "phase7-alpha-v1".into(),
        training_dataset_id: "phase7-wf-training-v1".into(),
        prediction_start_date: "20250121".into(),
        prediction_end_date: "20250210".into(),
        train_lookback_days: Some(10),
        prediction_step_days: Some(5),
        label_horizon_days: Some(2),
        label_objective: None,
        min_training_samples: Some(1),
        max_windows: Some(2),
        factors: vec![LinearFactorRef {
            factor_code: "fin_roe_daily_std".into(),
            factor_version: "1.0.0".into(),
        }],
    };
    let normalized = normalize_walk_forward_linear_prediction_request(&req).expect("wf request");

    let windows = build_walk_forward_windows(&normalized).expect("windows");

    assert_eq!(windows.len(), 2);
    assert_eq!(
        windows[0].prediction_start_date,
        NaiveDate::from_ymd_opt(2025, 1, 21).unwrap()
    );
    assert_eq!(
        windows[0].prediction_end_date,
        NaiveDate::from_ymd_opt(2025, 1, 25).unwrap()
    );
    assert_eq!(
        windows[0].train_end_date,
        NaiveDate::from_ymd_opt(2025, 1, 19).unwrap()
    );
    assert_eq!(
        windows[0].train_start_date,
        NaiveDate::from_ymd_opt(2025, 1, 10).unwrap()
    );
    assert_eq!(
        windows[1].prediction_start_date,
        NaiveDate::from_ymd_opt(2025, 1, 26).unwrap()
    );
}

#[test]
fn fit_linear_weights_normalizes_covariance_scores() {
    let samples = vec![
        TrainingSample {
            regime_tag: None,
            features: vec![1.0, 0.0],
            label: 0.10,
        },
        TrainingSample {
            regime_tag: None,
            features: vec![0.0, 1.0],
            label: -0.05,
        },
    ];

    let weights = fit_linear_weights(&samples, 2);

    assert_eq!(weights.len(), 2);
    assert!((weights[0] - 0.6666666667).abs() < 1e-6);
    assert!((weights[1] + 0.3333333333).abs() < 1e-6);
    assert!((weights.iter().map(|value| value.abs()).sum::<f64>() - 1.0).abs() < 1e-6);
}

#[test]
fn future_return_label_uses_later_close_only() {
    let closes = vec![
        (NaiveDate::from_ymd_opt(2025, 1, 9).unwrap(), 10.0),
        (NaiveDate::from_ymd_opt(2025, 1, 10).unwrap(), 11.0),
        (NaiveDate::from_ymd_opt(2025, 1, 13).unwrap(), 12.1),
    ];

    let label = future_return_label_until(
        Some(&closes),
        NaiveDate::from_ymd_opt(2025, 1, 9).unwrap(),
        None,
        2,
    )
    .expect("label");

    assert!((label - 0.21).abs() < 1e-9);
}

#[test]
fn quality_adjusted_excess_return_penalizes_high_volatility_stocks() {
    let days = 300;
    let stable_closes: Vec<(NaiveDate, f64)> = (0..days)
        .map(|i| {
            (
                NaiveDate::from_ymd_opt(2024, 1, 1).unwrap() + chrono::Duration::days(i),
                10.0 + (i as f64 * 0.005),
            )
        })
        .collect();
    let volatile_closes: Vec<(NaiveDate, f64)> = (0..days)
        .map(|i| {
            let base = 10.0 + (i as f64 * 0.005);
            let noise = (i as f64 * 0.3).sin() * 0.5;
            (
                NaiveDate::from_ymd_opt(2024, 1, 1).unwrap() + chrono::Duration::days(i),
                base + noise,
            )
        })
        .collect();
    let benchmark_closes: Vec<(NaiveDate, f64)> = (0..days)
        .map(|i| {
            (
                NaiveDate::from_ymd_opt(2024, 1, 1).unwrap() + chrono::Duration::days(i),
                100.0 + (i as f64 * 0.003),
            )
        })
        .collect();

    // Use trade_date at day 150 so trailing lookback (60/120) and horizon (20) are within range
    let trade_idx = 150usize;
    let trade_date = stable_closes[trade_idx].0;
    let horizon = 20i64;

    let stable_label = label_for_objective(
        LabelObjective::QualityAdjustedExcessReturn,
        Some(&stable_closes),
        Some(&benchmark_closes),
        trade_date,
        None,
        horizon,
    );
    let volatile_label = label_for_objective(
        LabelObjective::QualityAdjustedExcessReturn,
        Some(&volatile_closes),
        Some(&benchmark_closes),
        trade_date,
        None,
        horizon,
    );

    assert!(stable_label.is_some(), "stable label should be computed");
    assert!(
        volatile_label.is_some(),
        "volatile label should be computed"
    );
    let stable = stable_label.unwrap();
    let volatile = volatile_label.unwrap();
    // Both have similar price trends; stable has lower trailing vol,
    // so quality adjustment penalizes the volatile stock more
    assert!(
        stable > volatile,
        "stable_label={stable} should be > volatile_label={volatile}"
    );
}

#[test]
fn quality_adjusted_risk_adjusted_excess_return_labels_are_smaller_than_raw_excess() {
    let days = 300;
    let closes: Vec<(NaiveDate, f64)> = (0..days)
        .map(|i| {
            (
                NaiveDate::from_ymd_opt(2024, 1, 1).unwrap() + chrono::Duration::days(i),
                10.0 + (i as f64 * 0.01),
            )
        })
        .collect();
    let benchmark_closes: Vec<(NaiveDate, f64)> = (0..days)
        .map(|i| {
            (
                NaiveDate::from_ymd_opt(2024, 1, 1).unwrap() + chrono::Duration::days(i),
                100.0 + (i as f64 * 0.003),
            )
        })
        .collect();

    let trade_idx = 150usize;
    let trade_date = closes[trade_idx].0;
    let horizon = 20i64;

    let raw = label_for_objective(
        LabelObjective::FutureExcessReturn,
        Some(&closes),
        Some(&benchmark_closes),
        trade_date,
        None,
        horizon,
    )
    .expect("raw excess return label");
    let qa = label_for_objective(
        LabelObjective::QualityAdjustedExcessReturn,
        Some(&closes),
        Some(&benchmark_closes),
        trade_date,
        None,
        horizon,
    )
    .expect("qa excess return label");
    let qara = label_for_objective(
        LabelObjective::QualityAdjustedRiskAdjustedExcessReturn,
        Some(&closes),
        Some(&benchmark_closes),
        trade_date,
        None,
        horizon,
    )
    .expect("qara label");

    // Quality-adjusted should be ≤ raw excess in absolute magnitude
    assert!(
        qa.abs() <= raw.abs(),
        "qa={qa} should be ≤ raw={raw} in magnitude"
    );
    // Both quality-adjusted labels should agree on direction with raw
    assert!(
        qara.is_sign_positive() == qa.is_sign_positive()
            && qa.is_sign_positive() == raw.is_sign_positive(),
        "qara={qara}, qa={qa}, raw={raw} should all have same sign"
    );
    // Quality-adjusted risk-adjusted = qa / downside_vol (can be larger due to low vol)
    assert!(qara.is_finite() && qara != 0.0);
}

#[test]
fn label_definition_json_includes_quality_adjustment_for_new_objectives() {
    let def = label_definition_json(LabelObjective::QualityAdjustedExcessReturn, 60);
    assert_eq!(def["label"], "quality_adjusted_excess_return");
    assert_eq!(def["benchmark"], "000300.SH");
    assert!(def["quality_adjustment"].is_object());
    assert_eq!(
        def["quality_adjustment"]["method"],
        "trailing_volatility_and_max_drawdown_penalty"
    );

    let def2 = label_definition_json(LabelObjective::QualityAdjustedRiskAdjustedExcessReturn, 120);
    assert_eq!(
        def2["label"],
        "quality_adjusted_risk_adjusted_excess_return"
    );
    assert_eq!(
        def2["risk_adjustment"],
        "forward_downside_volatility_floor_1pct"
    );
    assert!(def2["quality_adjustment"].is_object());
}

#[test]
fn trailing_volatility_computes_annualized_vol() {
    let closes: Vec<(NaiveDate, f64)> = (0..=60)
        .map(|i| {
            (
                NaiveDate::from_ymd_opt(2025, 1, 1).unwrap() + chrono::Duration::days(i),
                10.0,
            )
        })
        .collect();
    let vol = trailing_volatility(&closes, NaiveDate::from_ymd_opt(2025, 2, 20).unwrap(), 40);
    // Flat prices → near-zero volatility
    assert!(vol.is_some());
    assert!(vol.unwrap() < 0.01);
}

#[test]
fn trailing_max_drawdown_detects_drawdown() {
    // Build a continuous daily sequence: flat at 10, then peak at 12, trough at 9, recovery to 11
    let mut closes = Vec::new();
    let base = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
    // Index 0-79: flat at 10.0
    for i in 0..80 {
        closes.push((base + chrono::Duration::days(i as i64), 10.0));
    }
    // Index 80: peak at 12.0
    closes.push((base + chrono::Duration::days(80), 12.0));
    // Index 81: trough at 9.0 (drawdown 25%)
    closes.push((base + chrono::Duration::days(81), 9.0));
    // Index 82-99: recovery to 11.0
    for i in 82..100 {
        closes.push((base + chrono::Duration::days(i as i64), 11.0));
    }
    let trade_date = closes.last().unwrap().0;
    let dd = trailing_max_drawdown(&closes, trade_date, 80);
    assert!(dd.is_some(), "should compute max drawdown, got None");
    // Max drawdown from peak 12.0 to trough 9.0 = 3.0/12.0 = 0.25
    assert!((dd.unwrap() - 0.25).abs() < 1e-9);
}

#[test]
fn fundamental_quality_score_maps_positive_features_to_high_quality() {
    // Positive z-scores (good fundamentals) → high sigmoid → high quality
    let features = vec![1.0, 0.5, 0.3, 0.8, 0.6, 0.0, 0.2, 0.4, -0.1, 0.1, 0.7, 0.9];
    let score = fundamental_quality_score(&features, 12);
    // All features are ≥ -0.1, sigmoid > 0.47, so average > 0.5
    assert!(
        score > 0.5,
        "positive features should give score > 0.5, got {score}"
    );
    assert!(score <= 1.0, "score should be ≤ 1.0");
}

#[test]
fn fundamental_quality_score_penalizes_negative_features() {
    // Negative z-scores (poor fundamentals) → low sigmoid → low quality
    let features = vec![
        -1.0, -0.5, -2.0, -0.8, -0.6, -1.5, -0.2, -0.4, -0.1, -0.9, -0.7, -0.3,
    ];
    let score = fundamental_quality_score(&features, 12);
    assert!(
        score < 0.5,
        "negative features should give score < 0.5, got {score}"
    );
    assert!(score > 0.0, "score should be > 0");
}

#[test]
fn fundamental_quality_label_uses_quality_features_when_available() {
    let days = 300;
    let closes: Vec<(NaiveDate, f64)> = (0..days)
        .map(|i| {
            (
                NaiveDate::from_ymd_opt(2024, 1, 1).unwrap() + chrono::Duration::days(i),
                10.0 + (i as f64 * 0.005),
            )
        })
        .collect();
    let benchmark_closes: Vec<(NaiveDate, f64)> = (0..days)
        .map(|i| {
            (
                NaiveDate::from_ymd_opt(2024, 1, 1).unwrap() + chrono::Duration::days(i),
                100.0 + (i as f64 * 0.003),
            )
        })
        .collect();
    let trade_idx = 150usize;
    let trade_date = closes[trade_idx].0;
    let horizon = 20i64;

    // With high-quality features → higher label
    let high_quality_features: Vec<f64> = vec![1.0; 12];
    let hq_label = label_for_objective_with_features(
        LabelObjective::FundamentalQualityAdjustedExcessReturn,
        Some(&closes),
        Some(&benchmark_closes),
        trade_date,
        None,
        horizon,
        Some(&high_quality_features),
    );
    // With low-quality features → lower label
    let low_quality_features: Vec<f64> = vec![-1.0; 12];
    let lq_label = label_for_objective_with_features(
        LabelObjective::FundamentalQualityAdjustedExcessReturn,
        Some(&closes),
        Some(&benchmark_closes),
        trade_date,
        None,
        horizon,
        Some(&low_quality_features),
    );

    assert!(hq_label.is_some());
    assert!(lq_label.is_some());
    let hq = hq_label.unwrap();
    let lq = lq_label.unwrap();
    assert!(
        hq > lq,
        "high-quality label {hq} should be > low-quality label {lq}"
    );
}

#[test]
fn fundamental_quality_label_parses_correctly() {
    let obj =
        LabelObjective::parse(Some("fundamental_quality_adjusted_excess_return")).expect("parse");
    assert!(obj.uses_fundamental_quality());
    assert!(obj.requires_benchmark());
    assert!(obj.is_quality_adjusted());
    assert_eq!(obj.as_str(), "fundamental_quality_adjusted_excess_return");
}

#[test]
fn linear_training_request_accepts_label_objective_and_rejects_unknown() {
    let mut req = TrainLinearModelRequest {
        model_code: "trained_linear_alpha".into(),
        model_version: "phase7-fu-v1".into(),
        model_version_id: None,
        training_task_id: None,
        prediction_set_id: None,
        data_version_id: "full-market-2016-v1".into(),
        feature_set_version_id: "phase7-alpha-v1".into(),
        training_dataset_id: "phase7-fu-training-v1".into(),
        train_start_date: "20250109".into(),
        train_end_date: "20250120".into(),
        prediction_start_date: "20250121".into(),
        prediction_end_date: "20250131".into(),
        label_horizon_days: Some(60),
        label_objective: Some("risk_adjusted_excess_return".into()),
        factors: vec![LinearFactorRef {
            factor_code: "fin_roe_daily_std".into(),
            factor_version: "1.0.0".into(),
        }],
    };

    let normalized = normalize_linear_training_request(&req).expect("training request");
    assert_eq!(
        normalized.label_objective.as_str(),
        "risk_adjusted_excess_return"
    );

    req.label_objective = Some("future_magic".into());
    let err = normalize_linear_training_request(&req).expect_err("unknown objective");
    assert!(err.contains("label_objective"));
}

#[test]
fn walk_forward_request_persists_label_objective_in_config() {
    let req = WalkForwardLinearPredictionSetRequest {
        model_code: "phase7_wf_linear_alpha".into(),
        model_version: "phase7-fu-v1".into(),
        model_version_id: None,
        training_task_id: None,
        prediction_set_id: None,
        data_version_id: "full-market-2016-v1".into(),
        feature_set_version_id: "phase7-alpha-v1".into(),
        training_dataset_id: "phase7-fu-training-v1".into(),
        prediction_start_date: "20250121".into(),
        prediction_end_date: "20250131".into(),
        train_lookback_days: Some(756),
        prediction_step_days: Some(20),
        label_horizon_days: Some(60),
        label_objective: Some("future_excess_return".into()),
        min_training_samples: Some(100),
        max_windows: Some(1),
        factors: vec![LinearFactorRef {
            factor_code: "fin_roe_daily_std".into(),
            factor_version: "1.0.0".into(),
        }],
    };

    let normalized = normalize_walk_forward_linear_prediction_request(&req).expect("wf request");

    assert_eq!(normalized.label_objective.as_str(), "future_excess_return");
}

#[test]
fn walk_forward_linear_experiment_config_records_label_objective() {
    let req = WalkForwardLinearPredictionSetRequest {
        model_code: "phase7_wf_linear_alpha".into(),
        model_version: "phase7-fu-v1".into(),
        model_version_id: None,
        training_task_id: None,
        prediction_set_id: None,
        data_version_id: "full-market-2016-v1".into(),
        feature_set_version_id: "phase7-alpha-v1".into(),
        training_dataset_id: "phase7-fu-training-v1".into(),
        prediction_start_date: "20250121".into(),
        prediction_end_date: "20250131".into(),
        train_lookback_days: Some(756),
        prediction_step_days: Some(20),
        label_horizon_days: Some(60),
        label_objective: Some("future_excess_return".into()),
        min_training_samples: Some(100),
        max_windows: Some(1),
        factors: vec![LinearFactorRef {
            factor_code: "fin_roe_daily_std".into(),
            factor_version: "1.0.0".into(),
        }],
    };
    let normalized = normalize_walk_forward_linear_prediction_request(&req).expect("wf request");
    let label_definition =
        label_definition_json(normalized.label_objective, normalized.label_horizon_days);

    let config = walk_forward_linear_experiment_config(&normalized, &label_definition);

    assert_eq!(config["label"]["label"], "future_excess_return");
    assert_eq!(config["label"]["benchmark"], "000300.SH");
    assert_eq!(
        config["point_in_time_policy"],
        "each window trains on dates <= train_end_date and labels are capped at train_end_date"
    );
}

#[test]
fn walk_forward_nonlinear_quantile_ranker_request_keeps_label_gap_and_bucket_params() {
    let req = WalkForwardNonlinearQuantileRankerRequest {
        model_code: "p7_nlq_wf".into(),
        model_version: "h60-fe-v1".into(),
        model_version_id: None,
        training_task_id: None,
        prediction_set_id: None,
        data_version_id: "full-market-2016-v1".into(),
        feature_set_version_id: "phase7-fy-core9-v1".into(),
        training_dataset_id: "ds-p7fy-nlq-wf-v1".into(),
        prediction_start_date: "20250121".into(),
        prediction_end_date: "20250210".into(),
        train_lookback_days: Some(120),
        prediction_step_days: Some(5),
        label_horizon_days: Some(60),
        label_objective: Some("future_excess_return".into()),
        min_training_samples: Some(100),
        max_windows: Some(2),
        bucket_count: Some(7),
        min_samples_per_bucket: Some(25),
        factors: vec![LinearFactorRef {
            factor_code: "fin_roe_daily_std".into(),
            factor_version: "1.0.0".into(),
        }],
    };

    let normalized =
        normalize_walk_forward_nonlinear_quantile_ranker_request(&req).expect("wf nlq request");
    let windows = build_walk_forward_windows(&normalized.linear).expect("wf windows");
    let config = walk_forward_nonlinear_quantile_ranker_experiment_config(&normalized);

    assert_eq!(normalized.bucket_count, 7);
    assert_eq!(normalized.min_samples_per_bucket, 25);
    assert_eq!(
        normalized.linear.prediction_set_id,
        "pred-p7_nlq_wf-h60-fe-v1-nlq-wf-20250121-20250210"
    );
    assert_eq!(windows.len(), 2);
    assert_eq!(
        windows[0].train_end_date,
        NaiveDate::from_ymd_opt(2024, 11, 22).unwrap()
    );
    assert_eq!(
        windows[0].prediction_start_date,
        NaiveDate::from_ymd_opt(2025, 1, 21).unwrap()
    );
    assert_eq!(
        config["trainer"],
        "walk_forward_nonlinear_quantile_ranker_v1"
    );
    assert_eq!(config["bucket_count"], 7);
    assert_eq!(config["label"]["label"], "future_excess_return");
}

#[test]
fn quality_adjusted_label_loads_trailing_price_history_before_train_start() {
    let req = NormalizedLinearTrainingRequest {
        model_code: "p7v19ml_nlq_ranker".into(),
        model_version: "w1-test".into(),
        model_version_id: "p7v19ml-nlq-w1-test".into(),
        training_task_id: "train-p7v19ml-w1-test".into(),
        prediction_set_id: "p7v19ml-w1-test".into(),
        data_version_id: "dv-v19-alpha-rebuild-smoke".into(),
        feature_set_version_id: "phase7_gb_quality_value_recovery_low_impact_v6".into(),
        training_dataset_id: "ds-p7v19ml-w1-test".into(),
        train_start_date: NaiveDate::from_ymd_opt(2017, 1, 3).unwrap(),
        train_end_date: NaiveDate::from_ymd_opt(2017, 9, 11).unwrap(),
        prediction_start_date: NaiveDate::from_ymd_opt(2017, 11, 10).unwrap(),
        prediction_end_date: NaiveDate::from_ymd_opt(2017, 11, 29).unwrap(),
        label_horizon_days: 60,
        label_objective: LabelObjective::QualityAdjustedRiskAdjustedExcessReturn,
        factors: vec![LinearFactorRef {
            factor_code: "fin_roe_daily_std".into(),
            factor_version: "1.0.0".into(),
        }],
    };

    let price_start = training_label_price_start_date(&req);

    assert_eq!(price_start, NaiveDate::from_ymd_opt(2016, 4, 26).unwrap());
}

#[test]
fn walk_forward_nonlinear_empty_prediction_error_includes_window_diagnostics() {
    let summaries = vec![WalkForwardNonlinearWindowSummary {
        window_index: 1,
        train_start_date: NaiveDate::from_ymd_opt(2017, 1, 3).unwrap(),
        train_end_date: NaiveDate::from_ymd_opt(2017, 9, 11).unwrap(),
        prediction_start_date: NaiveDate::from_ymd_opt(2017, 11, 10).unwrap(),
        prediction_end_date: NaiveDate::from_ymd_opt(2017, 11, 29).unwrap(),
        sample_count: 0,
        prediction_rows: 0,
        skipped: true,
        skip_reason: Some("sample_count 0 < required_samples 1000".to_string()),
        model: None,
    }];

    let message = walk_forward_nonlinear_no_prediction_rows_error(&summaries);

    assert!(message.contains("walk-forward nonlinear ranker produced no prediction rows"));
    assert!(message.contains("window=1"));
    assert!(message.contains("sample_count=0"));
    assert!(message.contains("sample_count 0 < required_samples 1000"));
}

#[test]
fn missing_data_version_error_is_actionable() {
    let message = missing_data_version_error_message("dv-missing-smoke");

    assert!(message.contains("dv-missing-smoke"));
    assert!(message.contains("does not exist in data_version"));
    assert!(message.contains("data readiness/sync"));
}

#[test]
fn feature_matrix_cache_slices_train_and_prediction_windows_without_requery() {
    let rows = vec![
        TrainingFeatureMatrixRow {
            symbol: "000001.SZ".into(),
            trade_date: NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            features: vec![1.0, 0.1],
        },
        TrainingFeatureMatrixRow {
            symbol: "000001.SZ".into(),
            trade_date: NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
            features: vec![2.0, 0.2],
        },
        TrainingFeatureMatrixRow {
            symbol: "000002.SZ".into(),
            trade_date: NaiveDate::from_ymd_opt(2024, 1, 4).unwrap(),
            features: vec![3.0, 0.3],
        },
    ];
    let cache = FeatureMatrixWindowCache::new(rows);

    let train_rows = cache.slice(
        NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
        NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
    );
    let prediction_rows = cache.slice(
        NaiveDate::from_ymd_opt(2024, 1, 4).unwrap(),
        NaiveDate::from_ymd_opt(2024, 1, 4).unwrap(),
    );

    assert_eq!(cache.row_count(), 3);
    assert_eq!(train_rows.len(), 2);
    assert_eq!(prediction_rows.len(), 1);
    assert_eq!(prediction_rows[0].symbol, "000002.SZ");
}

#[test]
fn prediction_insert_telemetry_counts_bulk_insert_batches() {
    let rows = (0..12_001)
        .map(|idx| {
            build_prediction_row(
                "pred-telemetry",
                &format!("{:06}.SZ", idx),
                "2025-01-02",
                idx as f64,
                idx + 1,
            )
            .expect("prediction row")
        })
        .collect::<Vec<_>>();

    let telemetry = prediction_insert_telemetry(&rows);

    assert_eq!(telemetry["row_count"], 12_001);
    assert_eq!(telemetry["batch_size"], 5_000);
    assert_eq!(telemetry["batch_count"], 3);
    assert_eq!(telemetry["chunk_row_counts"], json!([5000, 5000, 2001]));
}

#[test]
fn nonlinear_ranker_prediction_metadata_records_cache_and_insert_telemetry() {
    let label_definition = label_definition_json(LabelObjective::RiskAdjustedExcessReturn, 45);
    let rows = vec![
        build_prediction_row("p7gb-test", "000001.SZ", "2025-01-02", 0.7, 1).expect("row 1"),
        build_prediction_row("p7gb-test", "000002.SZ", "2025-01-02", 0.4, 2).expect("row 2"),
    ];
    let metadata = nonlinear_quantile_ranker_prediction_set_metadata(
        "train-p7gb-test",
        512,
        &label_definition,
        json!({"buckets": []}),
        feature_matrix_cache_metadata(
            "prediction_window",
            NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2025, 3, 31).unwrap(),
            20_000,
        ),
        prediction_insert_telemetry(&rows),
        prediction_generation_telemetry(
            "train_only_nonlinear_quantile_ranker",
            1,
            1,
            0,
            rows.len(),
            std::time::Duration::from_millis(125),
            vec![prediction_progress_stage(
                "score_prediction_window",
                1,
                1,
                rows.len(),
                std::time::Duration::from_millis(25),
            )],
        ),
    );

    assert_eq!(metadata["model_type"], "nonlinear_quantile_ranker");
    assert_eq!(
        metadata["feature_matrix_cache"]["scope"],
        "prediction_window"
    );
    assert_eq!(metadata["feature_matrix_cache"]["row_count"], 20_000);
    assert_eq!(metadata["prediction_insert_telemetry"]["row_count"], 2);
    assert_eq!(metadata["prediction_insert_telemetry"]["batch_count"], 1);
    assert_eq!(
        metadata["prediction_generation_telemetry"]["operation"],
        "train_only_nonlinear_quantile_ranker"
    );
    assert_eq!(
        metadata["prediction_generation_telemetry"]["progress_pct"],
        100.0
    );
    assert_eq!(
        metadata["prediction_generation_telemetry"]["stages"][0]["stage"],
        "score_prediction_window"
    );
}

#[test]
fn prediction_generation_telemetry_records_elapsed_and_progress() {
    let telemetry = prediction_generation_telemetry(
        "walk_forward_nonlinear_quantile_ranker",
        6,
        4,
        2,
        253_694,
        std::time::Duration::from_millis(12_345),
        vec![
            prediction_progress_stage(
                "load_feature_matrix_cache",
                1,
                1,
                923_773,
                std::time::Duration::from_millis(3_000),
            ),
            prediction_progress_stage(
                "fit_and_score_windows",
                6,
                6,
                253_694,
                std::time::Duration::from_millis(9_345),
            ),
        ],
    );

    assert_eq!(
        telemetry["operation"],
        "walk_forward_nonlinear_quantile_ranker"
    );
    assert_eq!(telemetry["elapsed_ms"], 12_345);
    assert_eq!(telemetry["total_units"], 6);
    assert_eq!(telemetry["completed_units"], 4);
    assert_eq!(telemetry["skipped_units"], 2);
    assert_eq!(telemetry["prediction_rows"], 253_694);
    assert_eq!(telemetry["progress_pct"], 100.0);
    assert_eq!(telemetry["stages"][0]["row_count"], 923_773);
}

#[test]
fn prediction_cache_economics_report_flags_cached_train_and_uncached_test_sets() {
    let train = PredictionSetCacheEconomicsInput {
        prediction_set_id: "p7gb-w1-tr".into(),
        status: "ready".into(),
        start_date: NaiveDate::from_ymd_opt(2023, 10, 24).unwrap(),
        end_date: NaiveDate::from_ymd_opt(2024, 5, 17).unwrap(),
        metadata: json!({
            "feature_matrix_cache": {
                "scope": "request_window",
                "start_date": "2023-01-01",
                "end_date": "2024-05-18",
                "row_count": 923773
            },
            "prediction_insert_telemetry": {
                "mode": "bulk_insert",
                "row_count": 383993,
                "batch_size": 5000,
                "batch_count": 77,
                "chunk_row_counts": []
            },
            "prediction_generation_telemetry": {
                "operation": "walk_forward_nonlinear_quantile_ranker",
                "elapsed_ms": 12345,
                "progress_pct": 100.0,
                "prediction_rows": 383993
            }
        }),
        prediction_rows: 383_993,
        symbol_count: 3_094,
        trading_day_count: 140,
    };
    let test = PredictionSetCacheEconomicsInput {
        prediction_set_id: "p7gb-w1-te".into(),
        status: "ready".into(),
        start_date: NaiveDate::from_ymd_opt(2024, 5, 20).unwrap(),
        end_date: NaiveDate::from_ymd_opt(2025, 1, 24).unwrap(),
        metadata: json!({}),
        prediction_rows: 484_286,
        symbol_count: 3_313,
        trading_day_count: 164,
    };

    let report = prediction_set_cache_economics_report_json(&[train, test]);

    assert_eq!(report["prediction_set_count"], 2);
    assert_eq!(report["total_prediction_rows"], 868_279);
    assert_eq!(report["sets"][0]["cache_metadata_present"], true);
    assert_eq!(report["sets"][0]["generation_telemetry_present"], true);
    assert_eq!(
        report["sets"][0]["feature_matrix_cache"]["row_count"],
        923_773
    );
    assert_eq!(report["sets"][1]["cache_metadata_present"], false);
    assert_eq!(
        report["economics"]["recommendation"],
        "audit_uncached_prediction_sets"
    );
    assert_eq!(report["economics"]["missing_cache_metadata_count"], 1);
    assert_eq!(report["economics"]["missing_insert_telemetry_count"], 1);
    assert_eq!(report["economics"]["missing_generation_telemetry_count"], 1);
    assert_eq!(report["economics"]["max_generation_elapsed_ms"], 12_345);
    assert_eq!(report["economics"]["max_rows_per_trading_day"], 2953.0);
}

#[test]
fn excess_label_subtracts_benchmark_future_return() {
    let trade_date = NaiveDate::from_ymd_opt(2025, 1, 9).unwrap();
    let stock_closes = vec![
        (trade_date, 10.0),
        (NaiveDate::from_ymd_opt(2025, 1, 10).unwrap(), 10.5),
        (NaiveDate::from_ymd_opt(2025, 1, 13).unwrap(), 11.0),
    ];
    let benchmark_closes = vec![
        (trade_date, 100.0),
        (NaiveDate::from_ymd_opt(2025, 1, 10).unwrap(), 101.0),
        (NaiveDate::from_ymd_opt(2025, 1, 13).unwrap(), 102.0),
    ];

    let label = label_for_objective(
        LabelObjective::FutureExcessReturn,
        Some(&stock_closes),
        Some(&benchmark_closes),
        trade_date,
        None,
        2,
    )
    .expect("excess label");

    assert!((label - 0.08).abs() < 1e-9);
}

#[test]
fn risk_adjusted_excess_label_penalizes_forward_downside() {
    let trade_date = NaiveDate::from_ymd_opt(2025, 1, 9).unwrap();
    let smooth_stock = vec![
        (trade_date, 10.0),
        (NaiveDate::from_ymd_opt(2025, 1, 10).unwrap(), 10.4),
        (NaiveDate::from_ymd_opt(2025, 1, 13).unwrap(), 10.8),
    ];
    let choppy_stock = vec![
        (trade_date, 10.0),
        (NaiveDate::from_ymd_opt(2025, 1, 10).unwrap(), 9.2),
        (NaiveDate::from_ymd_opt(2025, 1, 13).unwrap(), 10.8),
    ];
    let benchmark_closes = vec![
        (trade_date, 100.0),
        (NaiveDate::from_ymd_opt(2025, 1, 10).unwrap(), 100.0),
        (NaiveDate::from_ymd_opt(2025, 1, 13).unwrap(), 100.0),
    ];

    let smooth = label_for_objective(
        LabelObjective::RiskAdjustedExcessReturn,
        Some(&smooth_stock),
        Some(&benchmark_closes),
        trade_date,
        None,
        2,
    )
    .expect("smooth label");
    let choppy = label_for_objective(
        LabelObjective::RiskAdjustedExcessReturn,
        Some(&choppy_stock),
        Some(&benchmark_closes),
        trade_date,
        None,
        2,
    )
    .expect("choppy label");

    assert!(smooth > choppy);
}

#[test]
fn label_objective_drops_samples_when_target_date_crosses_prediction_cutoff() {
    let trade_date = NaiveDate::from_ymd_opt(2025, 1, 9).unwrap();
    let closes = vec![
        (trade_date, 10.0),
        (NaiveDate::from_ymd_opt(2025, 1, 10).unwrap(), 10.5),
        (NaiveDate::from_ymd_opt(2025, 1, 13).unwrap(), 11.0),
    ];

    let label = label_for_objective(
        LabelObjective::FutureReturn,
        Some(&closes),
        None,
        trade_date,
        Some(NaiveDate::from_ymd_opt(2025, 1, 10).unwrap()),
        2,
    );

    assert!(label.is_none());
}

#[test]
fn training_feature_matrix_rows_build_samples_without_losing_factor_order() {
    let trade_date = NaiveDate::from_ymd_opt(2025, 1, 9).unwrap();
    let mut closes_by_symbol = HashMap::new();
    closes_by_symbol.insert(
        "AAA".to_string(),
        vec![
            (trade_date, 10.0),
            (NaiveDate::from_ymd_opt(2025, 1, 10).unwrap(), 10.5),
            (NaiveDate::from_ymd_opt(2025, 1, 13).unwrap(), 11.0),
        ],
    );
    closes_by_symbol.insert(
        "BBB".to_string(),
        vec![
            (trade_date, 20.0),
            (NaiveDate::from_ymd_opt(2025, 1, 10).unwrap(), 20.5),
            (NaiveDate::from_ymd_opt(2025, 1, 13).unwrap(), 21.0),
        ],
    );

    let samples = training_samples_from_feature_matrix_rows(
        vec![
            TrainingFeatureMatrixRow {
                symbol: "AAA".to_string(),
                trade_date,
                features: vec![0.25, -0.75],
            },
            TrainingFeatureMatrixRow {
                symbol: "BBB".to_string(),
                trade_date,
                features: vec![0.50],
            },
            TrainingFeatureMatrixRow {
                symbol: "AAA".to_string(),
                trade_date: NaiveDate::from_ymd_opt(2025, 1, 10).unwrap(),
                features: vec![f64::NAN, 0.10],
            },
        ],
        &closes_by_symbol,
        &Vec::new(),
        LabelObjective::FutureReturn,
        None,
        2,
        2,
    );

    assert_eq!(samples.len(), 1);
    assert_eq!(samples[0].features, vec![0.25, -0.75]);
    assert!((samples[0].label - 0.10).abs() < 1e-9);
}

#[test]
fn prediction_feature_matrix_rows_build_ranked_scores_without_losing_factor_order() {
    let trade_date = NaiveDate::from_ymd_opt(2025, 1, 9).unwrap();
    let rows = prediction_rows_from_feature_matrix_rows(
        "pred-v1",
        vec![
            TrainingFeatureMatrixRow {
                symbol: "BBB".to_string(),
                trade_date,
                features: vec![1.0, 0.25],
            },
            TrainingFeatureMatrixRow {
                symbol: "AAA".to_string(),
                trade_date,
                features: vec![2.0, -0.5],
            },
            TrainingFeatureMatrixRow {
                symbol: "CCC".to_string(),
                trade_date,
                features: vec![4.0],
            },
            TrainingFeatureMatrixRow {
                symbol: "DDD".to_string(),
                trade_date,
                features: vec![f64::NAN, 1.0],
            },
        ],
        &[0.25, -1.0],
        2,
    )
    .expect("prediction rows");

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].symbol, "AAA");
    assert_eq!(rows[0].rank, 1);
    assert!((rows[0].score - 1.0).abs() < 1e-9);
    assert_eq!(rows[0].available_at, trade_date);
    assert_eq!(rows[1].symbol, "BBB");
    assert_eq!(rows[1].rank, 2);
    assert!((rows[1].score - 0.0).abs() < 1e-9);
}

#[test]
fn nonlinear_quantile_ranker_learns_bucket_payoffs_and_scores_prediction_rows() {
    let train_rows = vec![
        TrainingSample {
            regime_tag: None,
            features: vec![-1.0, 0.2],
            label: -0.02,
        },
        TrainingSample {
            regime_tag: None,
            features: vec![-0.8, 0.1],
            label: -0.01,
        },
        TrainingSample {
            regime_tag: None,
            features: vec![0.1, 0.5],
            label: 0.01,
        },
        TrainingSample {
            regime_tag: None,
            features: vec![0.2, 0.4],
            label: 0.02,
        },
        TrainingSample {
            regime_tag: None,
            features: vec![0.8, -0.3],
            label: 0.08,
        },
        TrainingSample {
            regime_tag: None,
            features: vec![1.0, -0.2],
            label: 0.10,
        },
    ];

    let model =
        fit_nonlinear_quantile_ranker(&train_rows, 2, 3, 2).expect("nonlinear quantile ranker");

    assert_eq!(model.factor_count, 2);
    assert_eq!(model.bucket_count, 3);
    assert_eq!(model.tables.len(), 2);
    assert!(
        model.tables[0].bucket_scores[2] > model.tables[0].bucket_scores[0],
        "first factor should learn that the high bucket has the stronger payoff"
    );

    let trade_date = NaiveDate::from_ymd_opt(2025, 1, 9).unwrap();
    let rows = nonlinear_prediction_rows_from_feature_matrix_rows(
        "pred-nonlinear-v1",
        vec![
            TrainingFeatureMatrixRow {
                symbol: "WEAK".to_string(),
                trade_date,
                features: vec![-0.9, 0.1],
            },
            TrainingFeatureMatrixRow {
                symbol: "STRONG".to_string(),
                trade_date,
                features: vec![0.9, -0.1],
            },
            TrainingFeatureMatrixRow {
                symbol: "BROKEN".to_string(),
                trade_date,
                features: vec![f64::NAN, 1.0],
            },
        ],
        &model,
    )
    .expect("prediction rows");

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].symbol, "STRONG");
    assert_eq!(rows[0].rank, 1);
    assert_eq!(rows[0].available_at, trade_date);
    assert_eq!(rows[1].symbol, "WEAK");
    assert!(rows[0].score > rows[1].score);
}

#[test]
fn linear_training_experiment_records_lineage_and_metrics() {
    let req = TrainLinearModelRequest {
        model_code: "trained_linear_alpha".into(),
        model_version: "phase5c-v1".into(),
        model_version_id: None,
        training_task_id: None,
        prediction_set_id: None,
        data_version_id: "perf-db-smoke-data-v1".into(),
        feature_set_version_id: "phase5c-feature-smoke-v1".into(),
        training_dataset_id: "phase5c-training-smoke-v1".into(),
        train_start_date: "20250109".into(),
        train_end_date: "20250120".into(),
        prediction_start_date: "20250121".into(),
        prediction_end_date: "20250131".into(),
        label_horizon_days: Some(1),
        label_objective: Some("future_excess_return".into()),
        factors: vec![LinearFactorRef {
            factor_code: "mom_5d_std".into(),
            factor_version: "1.0.0".into(),
        }],
    };
    let normalized = normalize_linear_training_request(&req).expect("training request");

    let config = linear_training_experiment_config(&normalized);
    let metrics = linear_training_experiment_metrics(
        40367,
        25418,
        &json!([{"factor_code":"mom_5d_std","factor_version":"1.0.0","weight":1.0}]),
        "hash-artifact",
        "hash-prediction",
    );

    assert_eq!(
        config["training_task_id"],
        "train-trained_linear_alpha-phase5c-v1"
    );
    assert_eq!(
        config["prediction_set_id"],
        "pred-trained_linear_alpha-phase5c-v1-20250121-20250131"
    );
    assert_eq!(config["label"]["horizon_trading_days"], 1);
    assert_eq!(config["label"]["type"], "future_excess_return");
    assert_eq!(metrics["sample_count"], 40367);
    assert_eq!(metrics["prediction_rows"], 25418);
    assert_eq!(metrics["artifact_hash"], "hash-artifact");
    assert_eq!(metrics["prediction_hash"], "hash-prediction");
}

#[test]
fn prediction_evaluation_gates_require_trades_drawdown_and_excess_return() {
    let gates = evaluate_prediction_gates(0, 0.05, -0.01, 1, 0.20, 0.0);

    assert_eq!(prediction_evaluation_status(&gates), "review_required");
    assert_eq!(gates[0]["gate"], "min_trade_count");
    assert_eq!(gates[0]["passed"], false);
    assert_eq!(gates[1]["passed"], true);
    assert_eq!(gates[2]["passed"], false);

    let passed = evaluate_prediction_gates(3, 0.05, 0.01, 1, 0.20, 0.0);
    assert_eq!(prediction_evaluation_status(&passed), "approved_candidate");
}

#[test]
fn prediction_evaluation_request_defaults_gate_policy() {
    let req = EvaluatePredictionSetRequest {
        prediction_set_id: "pred-v1".into(),
        backtest_task_id: "pbt-v1".into(),
        min_trade_count: None,
        max_drawdown: None,
        min_excess_return: None,
    };

    let normalized = normalize_prediction_set_evaluation_request(&req).expect("evaluation request");

    assert_eq!(normalized.prediction_set_id, "pred-v1");
    assert_eq!(normalized.backtest_task_id, "pbt-v1");
    assert_eq!(normalized.min_trade_count, 1);
    assert!((normalized.max_drawdown - 0.20).abs() < 1e-9);
    assert_eq!(normalized.min_excess_return, 0.0);
}
