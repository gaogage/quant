//! Tests for signal generator modules.

use super::*;
use crate::engine::{BacktestConfig, BacktestEngine, BacktestOutput, MarketDay};
use crate::signal_generator::matrix_view::ReturnHistoryMatrixView;
use crate::metrics::BacktestMetrics;

#[test]
fn score_day_uses_previous_trading_day_by_default() {
    let trading_days = vec![
        NaiveDate::from_ymd_opt(2026, 4, 28).unwrap(),
        NaiveDate::from_ymd_opt(2026, 4, 29).unwrap(),
        NaiveDate::from_ymd_opt(2026, 4, 30).unwrap(),
    ];
    let config = SignalConfig {
        rebalance_freq_days: 1,
        entry_delay_days: 0,
        ..Default::default()
    };

    let score_day = score_day_for_signal(&trading_days, 2, &config).unwrap();

    assert_eq!(score_day, NaiveDate::from_ymd_opt(2026, 4, 29).unwrap());
}

#[test]
fn score_day_honors_extra_entry_delay() {
    let trading_days = vec![
        NaiveDate::from_ymd_opt(2026, 4, 27).unwrap(),
        NaiveDate::from_ymd_opt(2026, 4, 28).unwrap(),
        NaiveDate::from_ymd_opt(2026, 4, 29).unwrap(),
        NaiveDate::from_ymd_opt(2026, 4, 30).unwrap(),
    ];
    let config = SignalConfig {
        rebalance_freq_days: 1,
        entry_delay_days: 1,
        ..Default::default()
    };

    let score_day = score_day_for_signal(&trading_days, 3, &config).unwrap();

    assert_eq!(score_day, NaiveDate::from_ymd_opt(2026, 4, 28).unwrap());
}

#[test]
fn pit_average_amounts_by_date_never_uses_future_amount_rows() {
    let as_of = NaiveDate::from_ymd_opt(2026, 1, 6).unwrap();
    let future_day = NaiveDate::from_ymd_opt(2026, 1, 7).unwrap();
    let amount_history = HashMap::from([
        (
            "FUTURE_LIQUID".to_string(),
            vec![(as_of, 1_000_000.0), (future_day, 1_000_000_000.0)],
        ),
        (
            "LIQUID_NOW".to_string(),
            vec![
                (NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(), 900_000_000.0),
                (as_of, 800_000_000.0),
            ],
        ),
    ]);

    let average_amounts_by_date =
        build_pit_average_amounts_by_date(&amount_history, &[as_of], 2);
    let average_amounts = average_amounts_by_date
        .get(&as_of)
        .expect("as-of liquidity snapshot");

    assert_eq!(average_amounts["FUTURE_LIQUID"], 1_000_000.0);
    assert!(average_amounts["LIQUID_NOW"] > average_amounts["FUTURE_LIQUID"]);
}

#[test]
fn factor_signals_use_score_day_pit_capacity_snapshot() {
    let trading_days = vec![
        NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(),
        NaiveDate::from_ymd_opt(2026, 1, 6).unwrap(),
        NaiveDate::from_ymd_opt(2026, 1, 7).unwrap(),
    ];
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 6).unwrap();
    let mut scores_by_date = HashMap::new();
    scores_by_date.insert(
        score_day,
        vec![
            ("FUTURE_LIQUID_ALPHA".to_string(), 100.0),
            ("LIQUID_NEAR_ALPHA".to_string(), 99.0),
            ("LIQUID_BACKUP".to_string(), 98.0),
            ("THIN_BACKUP".to_string(), 97.0),
        ],
    );
    let average_amounts_by_date = HashMap::from([
        (
            score_day,
            HashMap::from([
                ("FUTURE_LIQUID_ALPHA".to_string(), 1_000_000.0),
                ("LIQUID_NEAR_ALPHA".to_string(), 900_000_000.0),
                ("LIQUID_BACKUP".to_string(), 800_000_000.0),
                ("THIN_BACKUP".to_string(), 1_000_000.0),
            ]),
        ),
        (
            NaiveDate::from_ymd_opt(2026, 1, 7).unwrap(),
            HashMap::from([("FUTURE_LIQUID_ALPHA".to_string(), 1_000_000_000.0)]),
        ),
    ]);
    let config = SignalConfig {
        top_n: 2,
        rebalance_freq_days: 1,
        max_position_pct: Decimal::new(50, 2),
        candidate_ranking_profile: CandidateRankingProfile::CapacityAwareAlphaLiquidityV1,
        ..Default::default()
    };

    let signals = build_rebalance_factor_signals(
        &trading_days,
        &scores_by_date,
        &config,
        &HashMap::new(),
        &average_amounts_by_date,
        &HashMap::new(),
        |_day, base| base.clone(),
    )
    .expect("factor signals");
    let signal = signals
        .get(&NaiveDate::from_ymd_opt(2026, 1, 7).unwrap())
        .expect("signal from score day");

    assert!(!signal.target_weights.contains_key("FUTURE_LIQUID_ALPHA"));
    assert!(signal.target_weights.contains_key("LIQUID_NEAR_ALPHA"));
    assert!(signal.target_weights.contains_key("LIQUID_BACKUP"));
}

#[test]
fn factor_signals_emit_first_available_rebalance_inside_short_oos_window() {
    let trading_days = vec![
        NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(),
        NaiveDate::from_ymd_opt(2026, 1, 6).unwrap(),
        NaiveDate::from_ymd_opt(2026, 1, 7).unwrap(),
    ];
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
    let scores_by_date = HashMap::from([(
        score_day,
        vec![
            ("AAA".to_string(), 3.0),
            ("BBB".to_string(), 2.0),
            ("CCC".to_string(), 1.0),
        ],
    )]);
    let config = SignalConfig {
        top_n: 1,
        rebalance_freq_days: 60,
        max_position_pct: Decimal::ONE,
        ..Default::default()
    };

    let signals = build_rebalance_factor_signals(
        &trading_days,
        &scores_by_date,
        &config,
        &HashMap::new(),
        &HashMap::new(),
        &HashMap::new(),
        |_day, base| base.clone(),
    )
    .expect("factor signals");

    let first_signal_day = NaiveDate::from_ymd_opt(2026, 1, 6).unwrap();
    let signal = signals
        .get(&first_signal_day)
        .expect("first eligible rebalance should not wait 60 trading days");
    assert_eq!(signal.target_weights.get("AAA"), Some(&Decimal::ONE));
}

#[test]
fn factor_signals_prefer_preloaded_return_risk_matrix() {
    let trading_days = vec![
        NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(),
        NaiveDate::from_ymd_opt(2026, 1, 6).unwrap(),
    ];
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
    let scores_by_date = HashMap::from([(
        score_day,
        vec![
            ("AAA".to_string(), 3.0),
            ("BBB".to_string(), 2.0),
            ("CCC".to_string(), 1.0),
        ],
    )]);
    let raw_return_history = HashMap::from([
        (
            "AAA".to_string(),
            dated_returns(&[0.12, -0.11, 0.10, -0.09, 0.08]),
        ),
        (
            "BBB".to_string(),
            dated_returns(&[0.004, 0.003, 0.005, 0.004, 0.003]),
        ),
        (
            "CCC".to_string(),
            dated_returns(&[0.005, 0.004, 0.003, 0.004, 0.005]),
        ),
    ]);
    let preloaded_return_history = HashMap::from([
        (
            "AAA".to_string(),
            dated_returns(&[0.004, 0.003, 0.005, 0.004, 0.003]),
        ),
        (
            "BBB".to_string(),
            dated_returns(&[0.005, 0.004, 0.003, 0.004, 0.005]),
        ),
        (
            "CCC".to_string(),
            dated_returns(&[0.12, -0.11, 0.10, -0.09, 0.08]),
        ),
    ]);
    let symbols = vec!["AAA".to_string(), "BBB".to_string(), "CCC".to_string()];
    let preloaded_matrices = HashMap::from([(
        5,
        Arc::new(build_score_date_return_risk_matrix(
            &preloaded_return_history,
            &[score_day],
            &symbols,
            5,
        )),
    )]);
    let config = SignalConfig {
        top_n: 2,
        rebalance_freq_days: 1,
        max_position_pct: Decimal::new(60, 2),
        candidate_risk_filter_profile: CandidateRiskFilterProfile::LowVolatilityV1,
        risk_budget_lookback_days: 5,
        ..Default::default()
    };

    let signals = build_rebalance_factor_signals_with_return_risk_matrices(
        &trading_days,
        &scores_by_date,
        &config,
        &raw_return_history,
        &HashMap::new(),
        &HashMap::new(),
        &preloaded_matrices,
        |_day, base| base.clone(),
    )
    .expect("factor signals");
    let signal = signals
        .get(&NaiveDate::from_ymd_opt(2026, 1, 6).unwrap())
        .expect("rebalance signal");

    assert!(signal.target_weights.contains_key("AAA"));
    assert!(signal.target_weights.contains_key("BBB"));
    assert!(!signal.target_weights.contains_key("CCC"));
}

#[test]
fn factor_signals_can_use_preloaded_candidate_scoped_return_risk_stats_matrix() {
    let trading_days = vec![
        NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(),
        NaiveDate::from_ymd_opt(2026, 1, 6).unwrap(),
    ];
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
    let scores_by_date = HashMap::from([(
        score_day,
        vec![
            ("AAA".to_string(), 3.0),
            ("BBB".to_string(), 2.0),
            ("CCC".to_string(), 1.0),
        ],
    )]);
    let raw_return_history = HashMap::from([
        (
            "AAA".to_string(),
            dated_returns(&[0.12, -0.11, 0.10, -0.09, 0.08]),
        ),
        (
            "BBB".to_string(),
            dated_returns(&[0.004, 0.003, 0.005, 0.004, 0.003]),
        ),
        (
            "CCC".to_string(),
            dated_returns(&[0.005, 0.004, 0.003, 0.004, 0.005]),
        ),
    ]);
    let preloaded_return_history = HashMap::from([
        (
            "AAA".to_string(),
            dated_returns(&[0.004, 0.003, 0.005, 0.004, 0.003]),
        ),
        (
            "BBB".to_string(),
            dated_returns(&[0.005, 0.004, 0.003, 0.004, 0.005]),
        ),
        (
            "CCC".to_string(),
            dated_returns(&[0.12, -0.11, 0.10, -0.09, 0.08]),
        ),
    ]);
    let symbols = vec!["AAA".to_string(), "BBB".to_string(), "CCC".to_string()];
    let pairwise_scope = return_risk_stats_pairwise_scope_for_portfolio_candidate_pool(
        score_day, &symbols, &symbols,
    );
    let preloaded_stats_matrices = HashMap::from([(
        5,
        Arc::new(
            build_score_date_return_risk_stats_matrix_with_pairwise_scope(
                &preloaded_return_history,
                &[score_day],
                &symbols,
                5,
                &pairwise_scope,
            ),
        ),
    )]);
    let config = SignalConfig {
        top_n: 2,
        rebalance_freq_days: 1,
        max_position_pct: Decimal::new(60, 2),
        candidate_risk_filter_profile: CandidateRiskFilterProfile::LowVolatilityV1,
        risk_budget_lookback_days: 5,
        ..Default::default()
    };

    let signals = build_rebalance_factor_signals_with_return_risk_stats_matrices(
        &trading_days,
        &scores_by_date,
        &config,
        &raw_return_history,
        &HashMap::new(),
        &HashMap::new(),
        &preloaded_stats_matrices,
        |_day, base| base.clone(),
    )
    .expect("factor signals");
    let signal = signals
        .get(&NaiveDate::from_ymd_opt(2026, 1, 6).unwrap())
        .expect("rebalance signal");

    assert!(signal.target_weights.contains_key("AAA"));
    assert!(signal.target_weights.contains_key("BBB"));
    assert!(!signal.target_weights.contains_key("CCC"));
}

#[test]
fn stats_matrix_signal_builder_completed_oos_smoke_has_no_metric_drift_from_raw_matrix_path() {
    let smoke = completed_oos_no_drift_smoke_for_stats_matrix_signal_builder();

    assert!(smoke.signal_count > 0);
    assert_eq!(smoke.raw_equity_curve.len(), smoke.oos_day_count);
    assert_eq!(smoke.stats_equity_curve.len(), smoke.oos_day_count);
    assert_eq!(smoke.raw_signal_count, smoke.stats_signal_count);
    assert_eq!(smoke.raw_equity_curve, smoke.stats_equity_curve);
    assert_eq!(
        smoke.raw_metrics.annual_return_pct,
        smoke.stats_metrics.annual_return_pct
    );
    assert_eq!(
        smoke.raw_metrics.sharpe_ratio,
        smoke.stats_metrics.sharpe_ratio
    );
    assert_eq!(
        smoke.raw_metrics.sortino_ratio,
        smoke.stats_metrics.sortino_ratio
    );
    assert_eq!(
        smoke.raw_metrics.calmar_ratio,
        smoke.stats_metrics.calmar_ratio
    );
    assert_eq!(
        smoke.raw_metrics.max_drawdown_pct,
        smoke.stats_metrics.max_drawdown_pct
    );
    assert_eq!(
        smoke.raw_metrics.final_execution_fill_ratio,
        smoke.stats_metrics.final_execution_fill_ratio
    );
}

#[test]
fn market_feature_snapshot_scope_only_uses_stats_return_risk_cache_when_opted_in() {
    let scope = MarketFeatureSnapshotScope::new(
        "data-v1",
        NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(),
        NaiveDate::from_ymd_opt(2026, 1, 31).unwrap(),
        NaiveDate::from_ymd_opt(2026, 2, 1).unwrap(),
        NaiveDate::from_ymd_opt(2026, 2, 28).unwrap(),
    );

    assert!(!scope.prefer_return_risk_stats_cache());
    assert!(scope
        .clone()
        .with_return_risk_stats_cache_experiment()
        .prefer_return_risk_stats_cache());
}

#[test]
fn return_risk_stats_pairwise_scope_for_factor_scores_is_score_day_specific_and_budgeted() {
    let day1 = NaiveDate::from_ymd_opt(2026, 1, 7).unwrap();
    let day2 = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let symbols = vec![
        "AAA".to_string(),
        "BBB".to_string(),
        "CCC".to_string(),
        "DDD".to_string(),
    ];
    let scores_by_date = HashMap::from([
        (
            day1,
            vec![("AAA".to_string(), 2.0), ("BBB".to_string(), 1.0)],
        ),
        (
            day2,
            vec![("CCC".to_string(), 2.0), ("DDD".to_string(), 1.0)],
        ),
    ]);
    let config = SignalConfig {
        top_n: 2,
        ..Default::default()
    };

    let plan = return_risk_stats_pairwise_scope_for_factor_scores(
        &[day1, day2],
        &symbols,
        &scores_by_date,
        &config,
        2,
    )
    .expect("pairwise scope within budget");

    assert_eq!(plan.pair_count(), 2);
    assert!(plan.contains(day1, "AAA", "BBB"));
    assert!(plan.contains(day2, "CCC", "DDD"));
    assert!(!plan.contains(day1, "CCC", "DDD"));
    assert!(!plan.contains(day2, "AAA", "BBB"));
    assert!(return_risk_stats_pairwise_scope_for_factor_scores(
        &[day1, day2],
        &symbols,
        &scores_by_date,
        &config,
        1,
    )
    .is_none());
}

#[test]
fn raw_return_risk_matrix_load_is_skipped_when_stats_cache_is_loaded() {
    assert!(!should_load_raw_return_risk_matrices(true, true));
    assert!(should_load_raw_return_risk_matrices(true, false));
    assert!(should_load_raw_return_risk_matrices(false, false));
    assert!(should_load_raw_return_risk_matrices(false, true));
}

#[test]
fn stats_return_risk_mode_does_not_prewarm_raw_return_risk_matrix() {
    assert!(should_prewarm_raw_return_risk_matrix(
        ReturnRiskFeatureCacheMode::RawMatrix,
        true
    ));
    assert!(!should_prewarm_raw_return_risk_matrix(
        ReturnRiskFeatureCacheMode::RawMatrix,
        false
    ));
    assert!(!should_prewarm_raw_return_risk_matrix(
        ReturnRiskFeatureCacheMode::StatsMatrixExperimental,
        true
    ));
}

struct StatsMatrixSignalBuilderOosNoDriftSmoke {
    oos_day_count: usize,
    signal_count: usize,
    raw_signal_count: usize,
    stats_signal_count: usize,
    raw_equity_curve: Vec<(NaiveDate, Decimal)>,
    stats_equity_curve: Vec<(NaiveDate, Decimal)>,
    raw_metrics: BacktestMetrics,
    stats_metrics: BacktestMetrics,
}

fn completed_oos_no_drift_smoke_for_stats_matrix_signal_builder(
) -> StatsMatrixSignalBuilderOosNoDriftSmoke {
    let trading_days = (0..8)
        .map(|idx| NaiveDate::from_ymd_opt(2026, 1, 7).unwrap() + Duration::days(idx))
        .collect::<Vec<_>>();
    let symbols = vec![
        "AAA".to_string(),
        "BBB".to_string(),
        "CCC".to_string(),
        "DDD".to_string(),
        "EEE".to_string(),
    ];
    let config = SignalConfig {
        top_n: 3,
        rebalance_freq_days: 1,
        max_position_pct: Decimal::new(40, 2),
        max_pairwise_correlation: Some(0.99),
        correlation_lookback_days: 5,
        portfolio_method: PortfolioConstructionMethod::RiskBudget,
        risk_budget_lookback_days: 5,
        candidate_risk_filter_profile: CandidateRiskFilterProfile::LowVolatilityV1,
        ..Default::default()
    };
    let score_days = rebalance_score_days(&trading_days, &config, |_day, base| base.clone());
    let scores_by_date = score_days
        .iter()
        .map(|score_day| {
            (
                *score_day,
                vec![
                    ("AAA".to_string(), 5.0),
                    ("BBB".to_string(), 4.0),
                    ("CCC".to_string(), 3.0),
                    ("DDD".to_string(), 2.0),
                    ("EEE".to_string(), 1.0),
                ],
            )
        })
        .collect::<HashMap<_, _>>();
    let return_history = HashMap::from([
        (
            "AAA".to_string(),
            dated_returns(&[
                0.010, -0.010, 0.015, -0.005, 0.020, 0.011, -0.006, 0.014, 0.008, -0.003,
                0.012, 0.004,
            ]),
        ),
        (
            "BBB".to_string(),
            dated_returns(&[
                0.006, 0.004, 0.005, 0.007, 0.006, 0.005, 0.004, 0.006, 0.005, 0.007, 0.006,
                0.005,
            ]),
        ),
        (
            "CCC".to_string(),
            dated_returns(&[
                -0.012, 0.009, -0.010, 0.011, -0.008, 0.010, -0.006, 0.009, -0.005, 0.008,
                -0.004, 0.007,
            ]),
        ),
        (
            "DDD".to_string(),
            dated_returns(&[
                0.080, -0.070, 0.090, -0.085, 0.075, -0.065, 0.070, -0.060, 0.065, -0.055,
                0.060, -0.050,
            ]),
        ),
        (
            "EEE".to_string(),
            dated_returns(&[
                0.004, 0.006, 0.005, 0.004, 0.006, 0.005, 0.006, 0.004, 0.005, 0.006, 0.004,
                0.005,
            ]),
        ),
    ]);
    let raw_matrices = HashMap::from([(
        5,
        Arc::new(build_score_date_return_risk_matrix(
            &return_history,
            &score_days,
            &symbols,
            5,
        )),
    )]);
    let pairwise_scope =
        return_risk_stats_pairwise_scope_from_symbols(&score_days, &symbols, &symbols);
    let stats_matrices = HashMap::from([(
        5,
        Arc::new(
            build_score_date_return_risk_stats_matrix_with_pairwise_scope(
                &return_history,
                &score_days,
                &symbols,
                5,
                &pairwise_scope,
            ),
        ),
    )]);

    let raw_signals = build_rebalance_factor_signals_with_return_risk_matrices(
        &trading_days,
        &scores_by_date,
        &config,
        &return_history,
        &HashMap::new(),
        &HashMap::new(),
        &raw_matrices,
        |_day, base| base.clone(),
    )
    .expect("raw matrix signals");
    let stats_signals = build_rebalance_factor_signals_with_return_risk_stats_matrices(
        &trading_days,
        &scores_by_date,
        &config,
        &return_history,
        &HashMap::new(),
        &HashMap::new(),
        &stats_matrices,
        |_day, base| base.clone(),
    )
    .expect("stats matrix signals");
    assert_eq!(raw_signals.len(), stats_signals.len());
    for (signal_day, raw_signal) in &raw_signals {
        let stats_signal = stats_signals
            .get(signal_day)
            .expect("stats signal for raw signal day");
        assert_eq!(raw_signal.target_weights, stats_signal.target_weights);
    }

    let raw_output = run_synthetic_oos_backtest(&trading_days, &symbols, &raw_signals);
    let stats_output = run_synthetic_oos_backtest(&trading_days, &symbols, &stats_signals);

    StatsMatrixSignalBuilderOosNoDriftSmoke {
        oos_day_count: trading_days.len(),
        signal_count: raw_signals.len(),
        raw_signal_count: raw_signals.len(),
        stats_signal_count: stats_signals.len(),
        raw_equity_curve: raw_output.equity_curve,
        stats_equity_curve: stats_output.equity_curve,
        raw_metrics: raw_output.metrics,
        stats_metrics: stats_output.metrics,
    }
}

fn run_synthetic_oos_backtest(
    trading_days: &[NaiveDate],
    symbols: &[String],
    signals: &HashMap<NaiveDate, StrategySignal>,
) -> BacktestOutput {
    let mut engine = BacktestEngine::new(BacktestConfig {
        start_date: *trading_days.first().expect("start date"),
        end_date: *trading_days.last().expect("end date"),
        symbols: symbols.to_vec(),
        max_position_pct: Decimal::ONE,
        ..Default::default()
    });
    for (day_idx, day) in trading_days.iter().enumerate() {
        let market = synthetic_oos_market_day(*day, day_idx, symbols);
        engine.process_day(&market, signals.get(day));
    }
    engine.finalize()
}

fn synthetic_oos_market_day(day: NaiveDate, day_idx: usize, symbols: &[String]) -> MarketDay {
    let mut open = HashMap::new();
    let mut close = HashMap::new();
    let mut pre_close = HashMap::new();
    let mut amount = HashMap::new();
    let mut up_limit = HashMap::new();
    let mut down_limit = HashMap::new();

    for (symbol_idx, symbol) in symbols.iter().enumerate() {
        let close_cents =
            1_000 + (symbol_idx as i64 * 75) + (day_idx as i64 * 4) + (day_idx % 3) as i64;
        let pre_close_cents = if day_idx == 0 {
            close_cents
        } else {
            close_cents - 4
        };
        let close_price = Decimal::new(close_cents, 2);
        let pre_close_price = Decimal::new(pre_close_cents.max(1), 2);
        open.insert(symbol.clone(), close_price);
        close.insert(symbol.clone(), close_price);
        pre_close.insert(symbol.clone(), pre_close_price);
        amount.insert(symbol.clone(), Decimal::new(1_000_000_000, 0));
        up_limit.insert(symbol.clone(), pre_close_price * Decimal::new(13, 1));
        down_limit.insert(symbol.clone(), pre_close_price * Decimal::new(7, 1));
    }

    let benchmark_close = Decimal::new(300_000 + day_idx as i64 * 10, 2);
    let benchmark_pre_close = if day_idx == 0 {
        benchmark_close
    } else {
        Decimal::new(300_000 + (day_idx as i64 - 1) * 10, 2)
    };

    MarketDay {
        date: day,
        open,
        close,
        pre_close,
        amount,
        suspended: HashSet::new(),
        up_limit,
        down_limit,
        benchmark_close,
        benchmark_pre_close,
    }
}

#[test]
fn prediction_signals_use_previous_day_ranked_predictions() {
    let trading_days = vec![
        NaiveDate::from_ymd_opt(2025, 1, 9).unwrap(),
        NaiveDate::from_ymd_opt(2025, 1, 10).unwrap(),
        NaiveDate::from_ymd_opt(2025, 1, 13).unwrap(),
    ];
    let score_day = trading_days[1];
    let mut scores_by_date = HashMap::new();
    scores_by_date.insert(
        score_day,
        vec![
            ("000003.SZ".to_string(), 0.8, Some(3)),
            ("000001.SZ".to_string(), 1.0, Some(1)),
            ("000002.SZ".to_string(), 0.9, Some(2)),
            ("000004.SZ".to_string(), 0.7, Some(4)),
            ("000005.SZ".to_string(), 0.6, Some(5)),
        ],
    );
    sort_prediction_scores(&mut scores_by_date, ScoreDirection::Descending);

    let signals = build_rebalance_prediction_signals(
        &trading_days,
        &scores_by_date,
        &PredictionSignalConfig {
            prediction_set_id: "pred-v1".into(),
            top_n: 5,
            rebalance_freq_days: 1,
            entry_delay_days: 0,
            max_position_pct: Decimal::new(20, 2),
            ..Default::default()
        },
        &HashMap::new(),
        &HashMap::new(),
        &HashMap::new(),
    )
    .expect("prediction signals");

    let signal = signals
        .get(&NaiveDate::from_ymd_opt(2025, 1, 13).unwrap())
        .expect("signal on next trading day");
    assert_eq!(signal.target_weights.len(), 5);
    assert!(signal.target_weights.contains_key("000001.SZ"));
    assert!(signal.target_weights.contains_key("000005.SZ"));
    assert_eq!(
        signal.target_weights["000001.SZ"],
        Decimal::from_f64(0.2).unwrap()
    );
}

#[test]
fn prediction_signals_emit_first_available_rebalance_inside_short_oos_window() {
    let trading_days = vec![
        NaiveDate::from_ymd_opt(2025, 1, 9).unwrap(),
        NaiveDate::from_ymd_opt(2025, 1, 10).unwrap(),
        NaiveDate::from_ymd_opt(2025, 1, 13).unwrap(),
    ];
    let score_day = trading_days[0];
    let mut scores_by_date = HashMap::new();
    scores_by_date.insert(
        score_day,
        vec![
            ("000001.SZ".to_string(), 1.0, Some(1)),
            ("000002.SZ".to_string(), 0.9, Some(2)),
            ("000003.SZ".to_string(), 0.8, Some(3)),
            ("000004.SZ".to_string(), 0.7, Some(4)),
            ("000005.SZ".to_string(), 0.6, Some(5)),
        ],
    );
    sort_prediction_scores(&mut scores_by_date, ScoreDirection::Descending);

    let signals = build_rebalance_prediction_signals(
        &trading_days,
        &scores_by_date,
        &PredictionSignalConfig {
            prediction_set_id: "pred-v1".into(),
            top_n: 5,
            rebalance_freq_days: 60,
            entry_delay_days: 0,
            max_position_pct: Decimal::new(20, 2),
            ..Default::default()
        },
        &HashMap::new(),
        &HashMap::new(),
        &HashMap::new(),
    )
    .expect("prediction signals");

    let signal = signals
        .get(&NaiveDate::from_ymd_opt(2025, 1, 10).unwrap())
        .expect("first eligible rebalance should not wait 60 trading days");
    assert_eq!(signal.target_weights.len(), 5);
}

#[test]
fn factor_sparse_score_days_follow_actual_rebalance_schedule() {
    let trading_days = vec![
        NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(),
        NaiveDate::from_ymd_opt(2026, 1, 6).unwrap(),
        NaiveDate::from_ymd_opt(2026, 1, 7).unwrap(),
    ];
    let config = SignalConfig {
        rebalance_freq_days: 60,
        entry_delay_days: 0,
        ..Default::default()
    };

    let score_days = rebalance_score_days(&trading_days, &config, |_day, base| base.clone());

    assert_eq!(
        score_days,
        vec![NaiveDate::from_ymd_opt(2026, 1, 5).unwrap()]
    );
}

#[test]
fn combo_score_dates_query_filters_to_requested_score_days() {
    let sql = combo_score_load_dates_sql(
        ScoreDirection::Descending,
        Some(200),
        TradableUniverseProfile::All,
    );

    assert!(sql.contains("mfv.trade_date = ANY($3)"));
    assert!(sql.contains("ROW_NUMBER() OVER"));
    assert!(sql.contains("score_rank <= $4"));
    assert!(!sql.contains("mfv.trade_date >= $3"));
}

#[test]
fn prediction_query_loads_prior_scores_for_first_signal_day() {
    let start_date = NaiveDate::from_ymd_opt(2025, 1, 21).unwrap();

    let load_start = prediction_load_start_date(start_date, 1);

    assert!(load_start < start_date);
    assert_eq!(load_start, NaiveDate::from_ymd_opt(2024, 12, 19).unwrap());
}

#[test]
fn sort_prediction_scores_honors_ascending_direction() {
    let day = NaiveDate::from_ymd_opt(2025, 1, 10).unwrap();
    let mut scores_by_date = HashMap::from([(
        day,
        vec![
            ("AAA".to_string(), 3.0, Some(1)),
            ("BBB".to_string(), 1.0, Some(3)),
            ("CCC".to_string(), 2.0, Some(2)),
        ],
    )]);

    sort_prediction_scores(&mut scores_by_date, ScoreDirection::Ascending);

    let sorted = scores_by_date.get(&day).unwrap();
    assert_eq!(sorted[0].0, "BBB");
    assert_eq!(sorted[1].0, "CCC");
    assert_eq!(sorted[2].0, "AAA");
}

#[test]
fn prediction_blend_keeps_intersection_and_combines_normalized_scores() {
    let day = NaiveDate::from_ymd_opt(2025, 1, 10).unwrap();
    let mut factor_scores = HashMap::from([(
        day,
        vec![
            ("AAA".to_string(), 10.0),
            ("BBB".to_string(), 20.0),
            ("CCC".to_string(), 30.0),
        ],
    )]);
    let prediction_scores = HashMap::from([(
        day,
        vec![
            ("AAA".to_string(), 0.1, None),
            ("BBB".to_string(), 0.2, None),
        ],
    )]);
    let blend = PredictionBlendConfig {
        prediction_set_id: "pred-quality-growth".to_string(),
        factor_weight: 0.5,
        prediction_weight: 0.5,
        prediction_min_percentile: None,
        prediction_min_score: None,
    };

    blend_factor_prediction_scores(
        &mut factor_scores,
        &prediction_scores,
        &blend,
        ScoreDirection::Descending,
    );

    let blended = factor_scores.get(&day).unwrap();
    assert_eq!(blended.len(), 2);
    assert_eq!(blended[0].0, "AAA");
    assert_eq!(blended[1].0, "BBB");
    assert!(blended[1].1 > blended[0].1);
}

#[test]
fn prediction_blend_aligns_prediction_direction_for_ascending_factor_scores() {
    let day = NaiveDate::from_ymd_opt(2025, 1, 10).unwrap();
    let mut factor_scores = HashMap::from([(
        day,
        vec![
            ("LOW_PRED".to_string(), 10.0),
            ("HIGH_PRED".to_string(), 10.0),
        ],
    )]);
    let prediction_scores = HashMap::from([(
        day,
        vec![
            ("LOW_PRED".to_string(), 0.1, None),
            ("HIGH_PRED".to_string(), 0.9, None),
        ],
    )]);
    let blend = PredictionBlendConfig {
        prediction_set_id: "pred-quality-growth".to_string(),
        factor_weight: 0.0,
        prediction_weight: 1.0,
        prediction_min_percentile: None,
        prediction_min_score: None,
    };

    blend_factor_prediction_scores(
        &mut factor_scores,
        &prediction_scores,
        &blend,
        ScoreDirection::Ascending,
    );
    sort_factor_scores(
        factor_scores.get_mut(&day).unwrap(),
        ScoreDirection::Ascending,
    );

    let blended = factor_scores.get(&day).unwrap();
    assert_eq!(blended[0].0, "HIGH_PRED");
    assert!(blended[0].1 < blended[1].1);
}

#[test]
fn prediction_blend_can_filter_low_prediction_percentiles_without_reweighting() {
    let day = NaiveDate::from_ymd_opt(2025, 1, 10).unwrap();
    let mut factor_scores = HashMap::from([(
        day,
        vec![
            ("LOW_FACTOR_BAD_PRED".to_string(), 1.0),
            ("MID_FACTOR_GOOD_PRED".to_string(), 2.0),
            ("HIGH_FACTOR_GOOD_PRED".to_string(), 3.0),
        ],
    )]);
    let prediction_scores = HashMap::from([(
        day,
        vec![
            ("LOW_FACTOR_BAD_PRED".to_string(), 0.1, None),
            ("MID_FACTOR_GOOD_PRED".to_string(), 0.8, None),
            ("HIGH_FACTOR_GOOD_PRED".to_string(), 0.9, None),
        ],
    )]);
    let blend = PredictionBlendConfig {
        prediction_set_id: "pred-quality-growth".to_string(),
        factor_weight: 1.0,
        prediction_weight: 0.0,
        prediction_min_percentile: Some(0.5),
        prediction_min_score: None,
    };

    blend_factor_prediction_scores(
        &mut factor_scores,
        &prediction_scores,
        &blend,
        ScoreDirection::Ascending,
    );
    sort_factor_scores(
        factor_scores.get_mut(&day).unwrap(),
        ScoreDirection::Ascending,
    );

    let blended = factor_scores.get(&day).unwrap();
    assert_eq!(blended.len(), 2);
    assert_eq!(blended[0].0, "MID_FACTOR_GOOD_PRED");
    assert_eq!(blended[1].0, "HIGH_FACTOR_GOOD_PRED");
}

#[test]
fn prediction_blend_can_filter_negative_raw_prediction_scores() {
    let day = NaiveDate::from_ymd_opt(2025, 1, 10).unwrap();
    let mut factor_scores = HashMap::from([(
        day,
        vec![
            ("NEGATIVE_HIGH_FACTOR".to_string(), 10.0),
            ("POSITIVE_LOW_FACTOR".to_string(), 1.0),
            ("POSITIVE_HIGH_FACTOR".to_string(), 2.0),
        ],
    )]);
    let prediction_scores = HashMap::from([(
        day,
        vec![
            ("NEGATIVE_HIGH_FACTOR".to_string(), -0.01, None),
            ("POSITIVE_LOW_FACTOR".to_string(), 0.00, None),
            ("POSITIVE_HIGH_FACTOR".to_string(), 0.02, None),
        ],
    )]);
    let blend = PredictionBlendConfig {
        prediction_set_id: "pred-quality-growth".to_string(),
        factor_weight: 1.0,
        prediction_weight: 0.0,
        prediction_min_percentile: None,
        prediction_min_score: Some(0.0),
    };

    blend_factor_prediction_scores(
        &mut factor_scores,
        &prediction_scores,
        &blend,
        ScoreDirection::Descending,
    );

    let blended = factor_scores.get(&day).unwrap();
    assert_eq!(
        blended.iter().map(|(symbol, _)| symbol).collect::<Vec<_>>(),
        vec!["POSITIVE_LOW_FACTOR", "POSITIVE_HIGH_FACTOR"]
    );
}

#[test]
fn event_gate_boosts_or_filters_without_replacing_base_ranking() {
    let day = NaiveDate::from_ymd_opt(2025, 1, 10).unwrap();
    let base_scores = HashMap::from([(
        day,
        vec![
            ("BASE_LEADER_NO_EVENT".to_string(), 10.0),
            ("BASE_MID_POSITIVE_EVENT".to_string(), 9.0),
            ("BASE_LOW_NEGATIVE_EVENT".to_string(), 8.0),
        ],
    )]);
    let event_scores = HashMap::from([(
        day,
        vec![
            ("BASE_MID_POSITIVE_EVENT".to_string(), 1.2),
            ("BASE_LOW_NEGATIVE_EVENT".to_string(), -0.7),
        ],
    )]);

    let mut boosted = base_scores.clone();
    apply_event_gate_scores(
        &mut boosted,
        &event_scores,
        &EventGateConfig {
            combo_name: "phase7_event_window_earnings_v1".to_string(),
            version: "1.0.0".to_string(),
            mode: EventGateMode::BoostPositive,
            score_direction: ScoreDirection::Descending,
            min_score: 0.0,
            boost_weight: 0.05,
            active_regimes: vec![],
        },
    );
    let boosted = boosted.get(&day).unwrap();
    assert_eq!(
        boosted.len(),
        3,
        "boost mode must not create sparse deletion"
    );
    assert_eq!(boosted[0].0, "BASE_LEADER_NO_EVENT");
    assert!(boosted[1].1 > base_scores.get(&day).unwrap()[1].1);
    assert_eq!(boosted[2].1, base_scores.get(&day).unwrap()[2].1);

    let mut exclude_negative = base_scores.clone();
    apply_event_gate_scores(
        &mut exclude_negative,
        &event_scores,
        &EventGateConfig {
            combo_name: "phase7_event_window_earnings_v1".to_string(),
            version: "1.0.0".to_string(),
            mode: EventGateMode::ExcludeNegative,
            score_direction: ScoreDirection::Descending,
            min_score: 0.0,
            boost_weight: 0.0,
            active_regimes: vec![],
        },
    );
    let exclude_negative = exclude_negative.get(&day).unwrap();
    assert_eq!(
        exclude_negative
            .iter()
            .map(|(symbol, _)| symbol.as_str())
            .collect::<Vec<_>>(),
        vec!["BASE_LEADER_NO_EVENT", "BASE_MID_POSITIVE_EVENT"]
    );

    let mut require_positive = base_scores.clone();
    apply_event_gate_scores(
        &mut require_positive,
        &event_scores,
        &EventGateConfig {
            combo_name: "phase7_event_window_earnings_v1".to_string(),
            version: "1.0.0".to_string(),
            mode: EventGateMode::RequirePositive,
            score_direction: ScoreDirection::Descending,
            min_score: 0.0,
            boost_weight: 0.0,
            active_regimes: vec![],
        },
    );
    let require_positive = require_positive.get(&day).unwrap();
    assert_eq!(require_positive.len(), 1);
    assert_eq!(require_positive[0].0, "BASE_MID_POSITIVE_EVENT");
}

#[test]
fn event_gate_can_be_limited_to_stress_regimes() {
    let bear_day = NaiveDate::from_ymd_opt(2025, 1, 10).unwrap();
    let bull_day = NaiveDate::from_ymd_opt(2025, 1, 13).unwrap();
    let base_scores = HashMap::from([
        (
            bear_day,
            vec![
                ("BASE_LEADER_NO_EVENT".to_string(), 10.0),
                ("BASE_LOW_NEGATIVE_EVENT".to_string(), 8.0),
            ],
        ),
        (
            bull_day,
            vec![
                ("BASE_LEADER_NO_EVENT".to_string(), 10.0),
                ("BASE_LOW_NEGATIVE_EVENT".to_string(), 8.0),
            ],
        ),
    ]);
    let event_scores = HashMap::from([
        (
            bear_day,
            vec![("BASE_LOW_NEGATIVE_EVENT".to_string(), -0.7)],
        ),
        (
            bull_day,
            vec![("BASE_LOW_NEGATIVE_EVENT".to_string(), -0.7)],
        ),
    ]);

    let mut regime_limited = base_scores.clone();
    apply_event_gate_scores_for_regime(
        &mut regime_limited,
        &event_scores,
        &EventGateConfig {
            combo_name: "phase7_valuation_v1".to_string(),
            version: "1.0.0".to_string(),
            mode: EventGateMode::ExcludeNegative,
            score_direction: ScoreDirection::Descending,
            min_score: 0.0,
            boost_weight: 0.0,
            active_regimes: vec![MarketRegime::Bear, MarketRegime::HighVolatility],
        },
        |date| {
            if date == bear_day {
                MarketRegime::Bear
            } else {
                MarketRegime::Bull
            }
        },
    );

    let bear_symbols = regime_limited
        .get(&bear_day)
        .unwrap()
        .iter()
        .map(|(symbol, _)| symbol.as_str())
        .collect::<Vec<_>>();
    let bull_symbols = regime_limited
        .get(&bull_day)
        .unwrap()
        .iter()
        .map(|(symbol, _)| symbol.as_str())
        .collect::<Vec<_>>();

    assert_eq!(bear_symbols, vec!["BASE_LEADER_NO_EVENT"]);
    assert_eq!(
        bull_symbols,
        vec!["BASE_LEADER_NO_EVENT", "BASE_LOW_NEGATIVE_EVENT"],
        "the valuation guard should stay inactive outside stress regimes"
    );
}

#[test]
fn sort_factor_scores_honors_ascending_direction() {
    let mut scores = vec![
        ("AAA".to_string(), 3.0),
        ("BBB".to_string(), 1.0),
        ("CCC".to_string(), 2.0),
    ];

    sort_factor_scores(&mut scores, ScoreDirection::Ascending);

    assert_eq!(scores[0].0, "BBB");
    assert_eq!(scores[1].0, "CCC");
    assert_eq!(scores[2].0, "AAA");
}

#[test]
fn pit_quality_recovery_scores_use_only_prior_score_days() {
    let d1 = NaiveDate::from_ymd_opt(2026, 1, 2).unwrap();
    let d2 = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
    let future_day = NaiveDate::from_ymd_opt(2026, 1, 6).unwrap();
    let base_scores = HashMap::from([
        (
            d1,
            vec![
                ("RECOVERING".to_string(), 0.80),
                ("STATIC_QUALITY".to_string(), 0.30),
            ],
        ),
        (
            d2,
            vec![
                ("RECOVERING".to_string(), 0.20),
                ("STATIC_QUALITY".to_string(), 0.25),
            ],
        ),
        (
            future_day,
            vec![
                ("RECOVERING".to_string(), 99.0),
                ("STATIC_QUALITY".to_string(), -99.0),
            ],
        ),
    ]);

    let derived = derive_pit_quality_recovery_scores(
        &base_scores,
        &[d1, d2],
        ScoreDirection::Ascending,
        ScoreDirection::Descending,
        0.40,
        0.60,
        None,
    );

    assert!(
        !derived.contains_key(&d1),
        "first score day has no PIT history"
    );
    assert!(!derived.contains_key(&future_day));
    let d2_scores = derived.get(&d2).expect("recovery score day");
    assert_eq!(d2_scores[0].0, "RECOVERING");
    assert_eq!(d2_scores[1].0, "STATIC_QUALITY");
    assert!(d2_scores[0].1 > d2_scores[1].1);
}

#[test]
fn portfolio_construction_filters_highly_correlated_candidates() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let candidates = vec![
        ("AAA".to_string(), 3.0),
        ("BBB".to_string(), 2.0),
        ("CCC".to_string(), 1.0),
    ];
    let return_history = HashMap::from([
        ("AAA".to_string(), dated_returns(&[0.01, 0.02, 0.03, 0.04])),
        (
            "BBB".to_string(),
            dated_returns(&[0.011, 0.021, 0.031, 0.041]),
        ),
        (
            "CCC".to_string(),
            dated_returns(&[0.02, -0.01, 0.01, -0.02]),
        ),
    ]);
    let config = PortfolioConstructionConfig {
        top_n: 3,
        max_position_pct: Decimal::new(50, 2),
        max_pairwise_correlation: Some(0.8),
        ..Default::default()
    };

    let weights = build_portfolio_weights(
        score_day,
        &candidates,
        &return_history,
        &HashMap::new(),
        &HashMap::new(),
        &config,
    );

    assert!(weights.contains_key("AAA"));
    assert!(!weights.contains_key("BBB"));
    assert!(weights.contains_key("CCC"));
}

#[test]
fn portfolio_construction_uses_fractional_kelly_with_caps() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let candidates = vec![("AAA".to_string(), 3.0), ("BBB".to_string(), 2.0)];
    let return_history = HashMap::from([
        ("AAA".to_string(), dated_returns(&[0.03, 0.02, 0.01, 0.02])),
        (
            "BBB".to_string(),
            dated_returns(&[0.03, -0.03, 0.02, -0.019]),
        ),
    ]);
    let config = PortfolioConstructionConfig {
        top_n: 2,
        max_position_pct: Decimal::new(60, 2),
        kelly_fraction: 0.5,
        max_gross_exposure: 1.0,
        ..Default::default()
    };

    let weights = build_portfolio_weights(
        score_day,
        &candidates,
        &return_history,
        &HashMap::new(),
        &HashMap::new(),
        &config,
    );

    assert!(weights["AAA"] > weights["BBB"]);
    let gross: Decimal = weights.values().copied().sum();
    assert!(gross <= Decimal::ONE);
    assert!(weights
        .values()
        .all(|weight| *weight <= Decimal::new(60, 2)));
}

#[test]
fn risk_model_portfolio_config_caps_large_top_n_for_local_search() {
    let config = SignalConfig {
        top_n: 80,
        portfolio_method: PortfolioConstructionMethod::RiskBudget,
        ..Default::default()
    };
    let portfolio_config = PortfolioConstructionConfig::from(&config);

    assert_eq!(portfolio_config.top_n, 50);

    let min_variance_config = SignalConfig {
        top_n: 80,
        portfolio_method: PortfolioConstructionMethod::MinVariance,
        ..Default::default()
    };
    let portfolio_config = PortfolioConstructionConfig::from(&min_variance_config);

    assert_eq!(portfolio_config.top_n, 50);

    let heuristic_config = SignalConfig {
        top_n: 80,
        portfolio_method: PortfolioConstructionMethod::Heuristic,
        ..Default::default()
    };
    let portfolio_config = PortfolioConstructionConfig::from(&heuristic_config);

    assert_eq!(portfolio_config.top_n, 80);
}

#[test]
fn risk_budget_portfolio_penalizes_high_volatility_and_low_capacity() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let candidates = vec![
        ("LOW_RISK".to_string(), 3.0),
        ("HIGH_RISK".to_string(), 2.9),
    ];
    let return_history = HashMap::from([
        (
            "LOW_RISK".to_string(),
            dated_returns(&[0.004, 0.003, 0.005, 0.004, 0.003]),
        ),
        (
            "HIGH_RISK".to_string(),
            dated_returns(&[0.08, -0.07, 0.09, -0.08, 0.07]),
        ),
    ]);
    let average_amounts = HashMap::from([
        ("LOW_RISK".to_string(), 500_000_000.0),
        ("HIGH_RISK".to_string(), 50_000_000.0),
    ]);
    let config = PortfolioConstructionConfig {
        top_n: 2,
        max_position_pct: Decimal::new(80, 2),
        max_gross_exposure: 1.0,
        portfolio_method: PortfolioConstructionMethod::RiskBudget,
        risk_budget_lookback_days: 5,
        capacity_penalty_strength: 1.0,
        ..Default::default()
    };

    let weights = build_portfolio_weights(
        score_day,
        &candidates,
        &return_history,
        &average_amounts,
        &HashMap::new(),
        &config,
    );

    assert!(weights["LOW_RISK"] > weights["HIGH_RISK"]);
    assert!(weights["LOW_RISK"] <= Decimal::new(80, 2));
    let gross: Decimal = weights.values().copied().sum();
    assert!(gross <= Decimal::ONE);
}

#[test]
fn portfolio_construction_caps_target_weight_by_participation_capacity() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let candidates = vec![("LIQUID".to_string(), 3.0), ("THIN".to_string(), 2.9)];
    let average_amounts = HashMap::from([
        ("LIQUID".to_string(), 5_000_000.0),
        ("THIN".to_string(), 1_000_000.0),
    ]);
    let config = PortfolioConstructionConfig {
        top_n: 2,
        max_position_pct: Decimal::new(80, 2),
        max_gross_exposure: 1.0,
        portfolio_notional_cny: Some(1_000_000.0),
        max_participation_rate: Some(0.05),
        ..Default::default()
    };

    let weights = build_portfolio_weights(
        score_day,
        &candidates,
        &HashMap::new(),
        &average_amounts,
        &HashMap::new(),
        &config,
    );

    assert_eq!(weights["LIQUID"], Decimal::new(25, 2));
    assert_eq!(weights["THIN"], Decimal::new(5, 2));
}

#[test]
fn liquidity_candidate_risk_filter_prefers_tradable_low_volatility_names() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let candidates = vec![
        ("THIN_HIGH_SCORE".to_string(), 100.0),
        ("LIQUID_RISKY".to_string(), 99.0),
        ("LIQUID_STABLE".to_string(), 98.0),
        ("MID_LIQUID_STABLE".to_string(), 97.0),
    ];
    let return_history = HashMap::from([
        (
            "THIN_HIGH_SCORE".to_string(),
            dated_returns(&[0.003, 0.004, 0.002, 0.003, 0.004]),
        ),
        (
            "LIQUID_RISKY".to_string(),
            dated_returns(&[0.12, -0.10, 0.11, -0.09, 0.10]),
        ),
        (
            "LIQUID_STABLE".to_string(),
            dated_returns(&[0.004, 0.003, 0.004, 0.003, 0.004]),
        ),
        (
            "MID_LIQUID_STABLE".to_string(),
            dated_returns(&[0.005, 0.004, 0.005, 0.004, 0.005]),
        ),
    ]);
    let average_amounts = HashMap::from([
        ("THIN_HIGH_SCORE".to_string(), 1_000_000.0),
        ("LIQUID_RISKY".to_string(), 120_000_000.0),
        ("LIQUID_STABLE".to_string(), 100_000_000.0),
        ("MID_LIQUID_STABLE".to_string(), 80_000_000.0),
    ]);
    let config = PortfolioConstructionConfig {
        top_n: 1,
        candidate_risk_filter_profile:
            CandidateRiskFilterProfile::SoftLiquidityLowVolatilityLowCorrelationV1,
        risk_budget_lookback_days: 5,
        max_position_pct: Decimal::ONE,
        ..Default::default()
    };

    let weights = build_portfolio_weights(
        score_day,
        &candidates,
        &return_history,
        &average_amounts,
        &HashMap::new(),
        &config,
    );

    assert!(weights.contains_key("LIQUID_STABLE"));
    assert!(!weights.contains_key("THIN_HIGH_SCORE"));
    assert!(!weights.contains_key("LIQUID_RISKY"));
}

#[test]
fn capacity_aware_candidate_ranking_prefers_liquid_near_alpha_candidates() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let candidates = vec![
        ("THIN_ALPHA_1".to_string(), 100.0),
        ("THIN_ALPHA_2".to_string(), 99.0),
        ("LIQUID_NEAR_ALPHA_1".to_string(), 98.0),
        ("LIQUID_NEAR_ALPHA_2".to_string(), 97.0),
    ];
    let average_amounts = HashMap::from([
        ("THIN_ALPHA_1".to_string(), 1_000_000.0),
        ("THIN_ALPHA_2".to_string(), 1_200_000.0),
        ("LIQUID_NEAR_ALPHA_1".to_string(), 1_000_000_000.0),
        ("LIQUID_NEAR_ALPHA_2".to_string(), 800_000_000.0),
    ]);
    let config = PortfolioConstructionConfig {
        top_n: 2,
        max_position_pct: Decimal::new(50, 2),
        candidate_ranking_profile: CandidateRankingProfile::CapacityAwareAlphaLiquidityV1,
        ..Default::default()
    };

    let weights = build_portfolio_weights(
        score_day,
        &candidates,
        &HashMap::new(),
        &average_amounts,
        &HashMap::new(),
        &config,
    );

    assert!(weights.contains_key("LIQUID_NEAR_ALPHA_1"));
    assert!(weights.contains_key("LIQUID_NEAR_ALPHA_2"));
    assert!(!weights.contains_key("THIN_ALPHA_1"));
    assert!(!weights.contains_key("THIN_ALPHA_2"));
}

#[test]
fn alpha_first_low_impact_ranking_preserves_alpha_with_liquidity_bias() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let candidates = vec![
        ("ALPHA_LEADER".to_string(), 100.0),
        ("LIQUID_NEAR_ALPHA".to_string(), 99.0),
        ("LIQUID_BACKUP".to_string(), 98.0),
        ("THIN_BACKUP".to_string(), 97.0),
    ];
    let average_amounts = HashMap::from([
        ("ALPHA_LEADER".to_string(), 80_000_000.0),
        ("LIQUID_NEAR_ALPHA".to_string(), 1_000_000_000.0),
        ("LIQUID_BACKUP".to_string(), 900_000_000.0),
        ("THIN_BACKUP".to_string(), 1_000_000.0),
    ]);
    let config = PortfolioConstructionConfig {
        top_n: 2,
        max_position_pct: Decimal::new(50, 2),
        candidate_ranking_profile: CandidateRankingProfile::AlphaFirstLowImpactV1,
        ..Default::default()
    };

    let weights = build_portfolio_weights(
        score_day,
        &candidates,
        &HashMap::new(),
        &average_amounts,
        &HashMap::new(),
        &config,
    );

    assert!(weights.contains_key("ALPHA_LEADER"));
    assert!(weights.contains_key("LIQUID_NEAR_ALPHA"));
    assert!(!weights.contains_key("LIQUID_BACKUP"));
    assert!(!weights.contains_key("THIN_BACKUP"));
}

#[test]
fn relative_strength_alpha_liquidity_ranking_uses_only_pit_trailing_returns() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let future_day = NaiveDate::from_ymd_opt(2026, 1, 9).unwrap();
    let candidates = vec![
        ("ALPHA_LEADER".to_string(), 100.0),
        ("RELATIVE_STRENGTH".to_string(), 99.0),
        ("FUTURE_SPIKE".to_string(), 98.0),
        ("LIQUID_BACKUP".to_string(), 97.0),
    ];
    let return_history = HashMap::from([
        (
            "ALPHA_LEADER".to_string(),
            dated_returns(&[0.0, 0.0, 0.0, 0.0, 0.0]),
        ),
        (
            "RELATIVE_STRENGTH".to_string(),
            dated_returns(&[0.02, 0.03, 0.01, 0.02, 0.03]),
        ),
        (
            "FUTURE_SPIKE".to_string(),
            vec![
                (NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(), -0.02),
                (NaiveDate::from_ymd_opt(2026, 1, 3).unwrap(), -0.02),
                (NaiveDate::from_ymd_opt(2026, 1, 4).unwrap(), -0.01),
                (future_day, 0.30),
            ],
        ),
        (
            "LIQUID_BACKUP".to_string(),
            dated_returns(&[0.0, 0.0, 0.0, 0.0, 0.0]),
        ),
    ]);
    let average_amounts = HashMap::from([
        ("ALPHA_LEADER".to_string(), 200_000_000.0),
        ("RELATIVE_STRENGTH".to_string(), 1_000_000_000.0),
        ("FUTURE_SPIKE".to_string(), 900_000_000.0),
        ("LIQUID_BACKUP".to_string(), 800_000_000.0),
    ]);
    let config = PortfolioConstructionConfig {
        top_n: 2,
        max_position_pct: Decimal::new(50, 2),
        risk_budget_lookback_days: 5,
        candidate_ranking_profile: CandidateRankingProfile::RelativeStrengthAlphaLiquidityV1,
        ..Default::default()
    };

    let weights = build_portfolio_weights(
        score_day,
        &candidates,
        &return_history,
        &average_amounts,
        &HashMap::new(),
        &config,
    );

    assert!(weights.contains_key("RELATIVE_STRENGTH"));
    assert!(weights.contains_key("ALPHA_LEADER"));
    assert!(!weights.contains_key("FUTURE_SPIKE"));
    assert!(!weights.contains_key("LIQUID_BACKUP"));
}

#[test]
fn nonlinear_regime_alpha_liquidity_ranking_prefers_pit_alpha_with_low_impact() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let future_day = NaiveDate::from_ymd_opt(2026, 1, 9).unwrap();
    let candidates = vec![
        ("ALPHA_LEADER".to_string(), 100.0),
        ("REGIME_ALPHA".to_string(), 99.0),
        ("FUTURE_SPIKE".to_string(), 98.0),
        ("THIN_MOMENTUM".to_string(), 97.0),
        ("LIQUID_BACKUP".to_string(), 96.0),
    ];
    let return_history = HashMap::from([
        (
            "ALPHA_LEADER".to_string(),
            dated_returns(&[0.0, 0.01, 0.0, 0.01, 0.0]),
        ),
        (
            "REGIME_ALPHA".to_string(),
            dated_returns(&[0.02, 0.02, 0.01, 0.02, 0.02]),
        ),
        (
            "FUTURE_SPIKE".to_string(),
            vec![
                (NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(), -0.02),
                (NaiveDate::from_ymd_opt(2026, 1, 3).unwrap(), -0.01),
                (NaiveDate::from_ymd_opt(2026, 1, 4).unwrap(), -0.02),
                (future_day, 0.50),
            ],
        ),
        (
            "THIN_MOMENTUM".to_string(),
            dated_returns(&[0.03, 0.03, 0.02, 0.03, 0.03]),
        ),
        (
            "LIQUID_BACKUP".to_string(),
            dated_returns(&[0.0, 0.0, 0.0, 0.0, 0.0]),
        ),
    ]);
    let average_amounts = HashMap::from([
        ("ALPHA_LEADER".to_string(), 120_000_000.0),
        ("REGIME_ALPHA".to_string(), 900_000_000.0),
        ("FUTURE_SPIKE".to_string(), 1_000_000_000.0),
        ("THIN_MOMENTUM".to_string(), 1_000_000.0),
        ("LIQUID_BACKUP".to_string(), 850_000_000.0),
    ]);
    let config = PortfolioConstructionConfig {
        top_n: 2,
        max_position_pct: Decimal::new(50, 2),
        risk_budget_lookback_days: 5,
        candidate_ranking_profile: CandidateRankingProfile::parse(
            "nonlinear_regime_alpha_liquidity_v1",
        )
        .unwrap(),
        ..Default::default()
    };

    let weights = build_portfolio_weights(
        score_day,
        &candidates,
        &return_history,
        &average_amounts,
        &HashMap::new(),
        &config,
    );

    assert!(weights.contains_key("ALPHA_LEADER"));
    assert!(weights.contains_key("REGIME_ALPHA"));
    assert!(!weights.contains_key("FUTURE_SPIKE"));
    assert!(!weights.contains_key("THIN_MOMENTUM"));
    assert!(!weights.contains_key("LIQUID_BACKUP"));
}

#[test]
fn score_date_return_risk_matrix_matches_raw_trailing_stats_and_ignores_future_rows() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let future_day = NaiveDate::from_ymd_opt(2026, 1, 9).unwrap();
    let symbols = vec!["AAA".to_string(), "BBB".to_string()];
    let return_history = HashMap::from([
        (
            "AAA".to_string(),
            vec![
                (NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(), 0.01),
                (NaiveDate::from_ymd_opt(2026, 1, 3).unwrap(), 0.02),
                (NaiveDate::from_ymd_opt(2026, 1, 4).unwrap(), -0.01),
                (NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(), 0.03),
                (NaiveDate::from_ymd_opt(2026, 1, 6).unwrap(), 0.04),
                (future_day, 0.80),
            ],
        ),
        (
            "BBB".to_string(),
            vec![
                (NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(), -0.01),
                (NaiveDate::from_ymd_opt(2026, 1, 3).unwrap(), 0.01),
                (NaiveDate::from_ymd_opt(2026, 1, 4).unwrap(), 0.02),
                (NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(), -0.02),
                (future_day, 0.70),
            ],
        ),
    ]);

    let matrix =
        build_score_date_return_risk_matrix(&return_history, &[score_day], &symbols, 3);
    let raw_returns = trailing_returns(&return_history, "AAA", score_day, 3);
    let matrix_returns = matrix.returns(score_day, "AAA");

    assert_eq!(matrix_returns, raw_returns.as_slice());
    assert_eq!(matrix_returns, &[-0.01, 0.03, 0.04]);
    assert_eq!(
        matrix.total_return(score_day, "AAA"),
        trailing_total_return(&raw_returns)
    );
    assert_eq!(
        matrix.sample_volatility(score_day, "AAA"),
        sample_volatility(&raw_returns)
    );
    assert_eq!(
        matrix.fractional_kelly_weight(score_day, "AAA", 0.5),
        fractional_kelly_weight(&raw_returns, 0.5)
    );
    assert!(matrix.returns(score_day, "MISSING").is_empty());
}

#[test]
fn score_date_return_risk_matrix_matches_raw_correlation_and_risk_penalty() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let symbols = vec!["AAA".to_string(), "BBB".to_string(), "CCC".to_string()];
    let return_history = HashMap::from([
        (
            "AAA".to_string(),
            dated_returns(&[0.01, -0.02, 0.03, -0.01, 0.02]),
        ),
        (
            "BBB".to_string(),
            dated_returns(&[0.011, -0.021, 0.031, -0.011, 0.021]),
        ),
        (
            "CCC".to_string(),
            dated_returns(&[-0.02, 0.01, -0.03, 0.02, -0.01]),
        ),
    ]);
    let config = PortfolioConstructionConfig {
        risk_budget_lookback_days: 5,
        ..Default::default()
    };

    let matrix =
        build_score_date_return_risk_matrix(&return_history, &[score_day], &symbols, 5);
    let aaa_returns = trailing_returns(&return_history, "AAA", score_day, 5);
    let bbb_returns = trailing_returns(&return_history, "BBB", score_day, 5);

    assert_eq!(
        matrix.pearson_correlation(score_day, "AAA", "BBB"),
        pearson_correlation(&aaa_returns, &bbb_returns)
    );
    assert_eq!(
        matrix.average_abs_correlation_to_reference(score_day, "AAA", &symbols),
        average_abs_correlation_to_reference("AAA", &symbols, &return_history, score_day, 5)
    );
    assert_eq!(
        matrix.covariance_concentration_penalty(score_day, "AAA", &symbols),
        covariance_concentration_penalty("AAA", &symbols, &return_history, score_day, &config)
    );
}

#[test]
fn score_date_return_risk_matrix_matches_raw_portfolio_consumers_across_score_days() {
    let first_score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let second_score_day = NaiveDate::from_ymd_opt(2026, 1, 9).unwrap();
    let future_day = NaiveDate::from_ymd_opt(2026, 1, 10).unwrap();
    let candidates = vec![
        ("AAA".to_string(), 6.0),
        ("BBB".to_string(), 5.0),
        ("CCC".to_string(), 4.0),
        ("DDD".to_string(), 3.0),
        ("EEE".to_string(), 2.0),
    ];
    let symbols = candidates
        .iter()
        .map(|(symbol, _)| symbol.clone())
        .collect::<Vec<_>>();
    let return_history = HashMap::from([
        (
            "AAA".to_string(),
            vec![
                (NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(), 0.010),
                (NaiveDate::from_ymd_opt(2026, 1, 3).unwrap(), -0.010),
                (NaiveDate::from_ymd_opt(2026, 1, 4).unwrap(), 0.015),
                (NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(), -0.005),
                (NaiveDate::from_ymd_opt(2026, 1, 6).unwrap(), 0.020),
                (NaiveDate::from_ymd_opt(2026, 1, 7).unwrap(), 0.010),
                (NaiveDate::from_ymd_opt(2026, 1, 8).unwrap(), -0.020),
                (future_day, 0.500),
            ],
        ),
        (
            "BBB".to_string(),
            vec![
                (NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(), 0.011),
                (NaiveDate::from_ymd_opt(2026, 1, 3).unwrap(), -0.011),
                (NaiveDate::from_ymd_opt(2026, 1, 4).unwrap(), 0.016),
                (NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(), -0.006),
                (NaiveDate::from_ymd_opt(2026, 1, 6).unwrap(), 0.021),
                (NaiveDate::from_ymd_opt(2026, 1, 7).unwrap(), 0.011),
                (NaiveDate::from_ymd_opt(2026, 1, 8).unwrap(), -0.021),
                (future_day, 0.450),
            ],
        ),
        (
            "CCC".to_string(),
            vec![
                (NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(), -0.020),
                (NaiveDate::from_ymd_opt(2026, 1, 3).unwrap(), 0.010),
                (NaiveDate::from_ymd_opt(2026, 1, 4).unwrap(), -0.015),
                (NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(), 0.020),
                (NaiveDate::from_ymd_opt(2026, 1, 6).unwrap(), -0.010),
                (NaiveDate::from_ymd_opt(2026, 1, 7).unwrap(), 0.015),
                (NaiveDate::from_ymd_opt(2026, 1, 8).unwrap(), 0.005),
                (future_day, 0.400),
            ],
        ),
        (
            "DDD".to_string(),
            vec![
                (NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(), 0.050),
                (NaiveDate::from_ymd_opt(2026, 1, 3).unwrap(), -0.045),
                (NaiveDate::from_ymd_opt(2026, 1, 4).unwrap(), 0.040),
                (NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(), -0.035),
                (NaiveDate::from_ymd_opt(2026, 1, 6).unwrap(), 0.030),
                (NaiveDate::from_ymd_opt(2026, 1, 7).unwrap(), -0.025),
                (NaiveDate::from_ymd_opt(2026, 1, 8).unwrap(), 0.020),
                (future_day, 0.350),
            ],
        ),
        (
            "EEE".to_string(),
            vec![
                (NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(), 0.003),
                (NaiveDate::from_ymd_opt(2026, 1, 3).unwrap(), 0.004),
                (NaiveDate::from_ymd_opt(2026, 1, 4).unwrap(), 0.002),
                (NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(), 0.003),
                (NaiveDate::from_ymd_opt(2026, 1, 6).unwrap(), 0.004),
                (NaiveDate::from_ymd_opt(2026, 1, 7).unwrap(), 0.002),
                (NaiveDate::from_ymd_opt(2026, 1, 8).unwrap(), 0.003),
                (future_day, 0.300),
            ],
        ),
    ]);
    let average_amounts = HashMap::from([
        ("AAA".to_string(), 900_000_000.0),
        ("BBB".to_string(), 800_000_000.0),
        ("CCC".to_string(), 500_000_000.0),
        ("DDD".to_string(), 300_000_000.0),
        ("EEE".to_string(), 700_000_000.0),
    ]);
    let config = PortfolioConstructionConfig {
        top_n: 2,
        max_pairwise_correlation: Some(0.80),
        correlation_lookback_days: 4,
        kelly_fraction: 0.35,
        kelly_lookback_days: 3,
        portfolio_method: PortfolioConstructionMethod::RiskBudget,
        risk_budget_lookback_days: 5,
        capacity_penalty_strength: 0.25,
        candidate_risk_filter_profile:
            CandidateRiskFilterProfile::SoftLowVolatilityLowCorrelationV1,
        ..Default::default()
    };
    let score_days = vec![first_score_day, second_score_day];
    let risk_matrix =
        build_score_date_return_risk_matrix(&return_history, &score_days, &symbols, 5);
    let correlation_matrix =
        build_score_date_return_risk_matrix(&return_history, &score_days, &symbols, 4);
    let kelly_matrix =
        build_score_date_return_risk_matrix(&return_history, &score_days, &symbols, 3);

    for score_day in score_days {
        assert_eq!(
            relative_strength_rank_scores(&candidates, &risk_matrix, score_day),
            relative_strength_rank_scores(
                &candidates,
                &ReturnHistoryMatrixView::new(&return_history, 5),
                score_day
            )
        );
        assert_eq!(
            filter_candidate_risk_pool(
                score_day,
                &candidates,
                &risk_matrix,
                &average_amounts,
                &config,
            ),
            filter_candidate_risk_pool(
                score_day,
                &candidates,
                &ReturnHistoryMatrixView::new(&return_history, config.risk_budget_lookback_days),
                &average_amounts,
                &config,
            )
        );
        assert_eq!(
            select_uncorrelated_candidates(
                score_day,
                &candidates,
                &correlation_matrix,
                &config,
                4,
            ),
            select_uncorrelated_candidates(
                score_day,
                &candidates,
                &ReturnHistoryMatrixView::new(&return_history, config.correlation_lookback_days),
                &config,
                4
            )
        );
        assert_eq!(
            build_kelly_raw_weights(score_day, &symbols, &kelly_matrix, &config),
            build_kelly_raw_weights(
                score_day,
                &symbols,
                &ReturnHistoryMatrixView::new(&return_history, config.kelly_lookback_days),
                &config
            )
        );
        assert_eq!(
            build_risk_budget_raw_weights(
                score_day,
                &symbols,
                &risk_matrix,
                &average_amounts,
                &config,
            ),
            build_risk_budget_raw_weights(
                score_day,
                &symbols,
                &ReturnHistoryMatrixView::new(&return_history, config.risk_budget_lookback_days),
                &average_amounts,
                &config,
            )
        );
        assert_eq!(
            build_min_variance_raw_weights(
                score_day,
                &symbols,
                &risk_matrix,
                &average_amounts,
                &config,
            ),
            build_min_variance_raw_weights(
                score_day,
                &symbols,
                &ReturnHistoryMatrixView::new(&return_history, config.risk_budget_lookback_days),
                &average_amounts,
                &config,
            )
        );
        assert_eq!(
            build_risk_parity_raw_weights(
                score_day,
                &symbols,
                &risk_matrix,
                &average_amounts,
                &config,
            ),
            build_risk_parity_raw_weights(
                score_day,
                &symbols,
                &ReturnHistoryMatrixView::new(&return_history, config.risk_budget_lookback_days),
                &average_amounts,
                &config,
            )
        );
        assert_eq!(
            build_max_diversification_raw_weights(
                score_day,
                &symbols,
                &risk_matrix,
                &average_amounts,
                &config,
            ),
            build_max_diversification_raw_weights(
                score_day,
                &symbols,
                &ReturnHistoryMatrixView::new(&return_history, config.risk_budget_lookback_days),
                &average_amounts,
                &config,
            )
        );
    }
}

#[test]
fn score_date_return_risk_stats_matrix_matches_raw_single_symbol_metrics() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let future_day = NaiveDate::from_ymd_opt(2026, 1, 9).unwrap();
    let symbols = vec!["AAA".to_string(), "BBB".to_string()];
    let return_history = HashMap::from([
        (
            "AAA".to_string(),
            vec![
                (NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(), 0.010),
                (NaiveDate::from_ymd_opt(2026, 1, 3).unwrap(), -0.020),
                (NaiveDate::from_ymd_opt(2026, 1, 4).unwrap(), 0.030),
                (NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(), -0.010),
                (NaiveDate::from_ymd_opt(2026, 1, 6).unwrap(), 0.020),
                (NaiveDate::from_ymd_opt(2026, 1, 7).unwrap(), 0.015),
                (future_day, 0.750),
            ],
        ),
        (
            "BBB".to_string(),
            vec![
                (NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(), -0.005),
                (NaiveDate::from_ymd_opt(2026, 1, 3).unwrap(), 0.006),
                (NaiveDate::from_ymd_opt(2026, 1, 4).unwrap(), -0.004),
                (NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(), 0.005),
                (future_day, 0.650),
            ],
        ),
    ]);
    let raw_matrix =
        build_score_date_return_risk_matrix(&return_history, &[score_day], &symbols, 4);
    let stats_matrix =
        build_score_date_return_risk_stats_matrix(&return_history, &[score_day], &symbols, 4);
    let raw_returns = raw_matrix.returns(score_day, "AAA");

    assert_eq!(
        stats_matrix.return_count(score_day, "AAA"),
        raw_returns.len()
    );
    assert_eq!(
        stats_matrix.total_return(score_day, "AAA"),
        trailing_total_return(raw_returns)
    );
    assert_eq!(
        stats_matrix.sample_volatility(score_day, "AAA"),
        sample_volatility(raw_returns)
    );
    assert_eq!(
        stats_matrix.fractional_kelly_weight(score_day, "AAA", 0.25),
        fractional_kelly_weight(raw_returns, 0.25)
    );
    assert_eq!(stats_matrix.return_count(score_day, "MISSING"), 0);
    assert_eq!(
        stats_matrix.total_return(score_day, "AAA"),
        trailing_total_return(&[0.030, -0.010, 0.020, 0.015])
    );
}

#[test]
fn relative_strength_and_kelly_can_use_stats_matrix_without_raw_returns() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let candidates = vec![
        ("AAA".to_string(), 3.0),
        ("BBB".to_string(), 2.0),
        ("CCC".to_string(), 1.0),
    ];
    let symbols = candidates
        .iter()
        .map(|(symbol, _)| symbol.clone())
        .collect::<Vec<_>>();
    let return_history = HashMap::from([
        (
            "AAA".to_string(),
            dated_returns(&[0.010, 0.020, 0.015, 0.018]),
        ),
        (
            "BBB".to_string(),
            dated_returns(&[0.004, 0.006, 0.005, 0.007]),
        ),
        (
            "CCC".to_string(),
            dated_returns(&[-0.010, 0.004, -0.006, 0.003]),
        ),
    ]);
    let config = PortfolioConstructionConfig {
        kelly_fraction: 0.30,
        kelly_lookback_days: 4,
        ..Default::default()
    };
    let stats_matrix =
        build_score_date_return_risk_stats_matrix(&return_history, &[score_day], &symbols, 4);

    assert_eq!(
        relative_strength_rank_scores(&candidates, &stats_matrix, score_day),
        relative_strength_rank_scores(
            &candidates,
            &ReturnHistoryMatrixView::new(&return_history, 4),
            score_day
        )
    );
    assert_eq!(
        build_kelly_raw_weights(score_day, &symbols, &stats_matrix, &config),
        build_kelly_raw_weights(
            score_day,
            &symbols,
            &ReturnHistoryMatrixView::new(&return_history, config.kelly_lookback_days),
            &config
        )
    );
}

#[test]
fn score_date_return_risk_stats_matrix_matches_raw_pairwise_risk_metrics() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let future_day = NaiveDate::from_ymd_opt(2026, 1, 9).unwrap();
    let symbols = vec!["AAA".to_string(), "BBB".to_string(), "CCC".to_string()];
    let return_history = HashMap::from([
        (
            "AAA".to_string(),
            vec![
                (NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(), 0.010),
                (NaiveDate::from_ymd_opt(2026, 1, 3).unwrap(), -0.020),
                (NaiveDate::from_ymd_opt(2026, 1, 4).unwrap(), 0.030),
                (NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(), -0.010),
                (NaiveDate::from_ymd_opt(2026, 1, 6).unwrap(), 0.020),
                (NaiveDate::from_ymd_opt(2026, 1, 7).unwrap(), 0.015),
                (future_day, 0.750),
            ],
        ),
        (
            "BBB".to_string(),
            vec![
                (NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(), 0.012),
                (NaiveDate::from_ymd_opt(2026, 1, 3).unwrap(), -0.018),
                (NaiveDate::from_ymd_opt(2026, 1, 4).unwrap(), 0.028),
                (NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(), -0.009),
                (NaiveDate::from_ymd_opt(2026, 1, 6).unwrap(), 0.022),
                (NaiveDate::from_ymd_opt(2026, 1, 7).unwrap(), 0.014),
                (future_day, -0.700),
            ],
        ),
        (
            "CCC".to_string(),
            vec![
                (NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(), -0.020),
                (NaiveDate::from_ymd_opt(2026, 1, 3).unwrap(), 0.010),
                (NaiveDate::from_ymd_opt(2026, 1, 4).unwrap(), -0.015),
                (NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(), 0.020),
                (NaiveDate::from_ymd_opt(2026, 1, 6).unwrap(), -0.010),
                (NaiveDate::from_ymd_opt(2026, 1, 7).unwrap(), 0.005),
                (future_day, 0.600),
            ],
        ),
    ]);
    let raw_matrix =
        build_score_date_return_risk_matrix(&return_history, &[score_day], &symbols, 5);
    let stats_matrix =
        build_score_date_return_risk_stats_matrix(&return_history, &[score_day], &symbols, 5);

    assert_eq!(
        stats_matrix.pearson_correlation(score_day, "AAA", "BBB"),
        raw_matrix.pearson_correlation(score_day, "AAA", "BBB")
    );
    assert_eq!(
        stats_matrix.pearson_correlation(score_day, "BBB", "AAA"),
        raw_matrix.pearson_correlation(score_day, "BBB", "AAA")
    );
    assert_eq!(
        stats_matrix.average_abs_correlation_to_reference(score_day, "AAA", &symbols),
        raw_matrix.average_abs_correlation_to_reference(score_day, "AAA", &symbols)
    );
    assert_eq!(
        stats_matrix.covariance_concentration_penalty(score_day, "AAA", &symbols),
        raw_matrix.covariance_concentration_penalty(score_day, "AAA", &symbols)
    );
    assert_eq!(
        stats_matrix.pearson_correlation(score_day, "AAA", "AAA"),
        raw_matrix.pearson_correlation(score_day, "AAA", "AAA")
    );
}

#[test]
fn stats_matrix_correlation_consumers_match_raw_portfolio_helpers() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let candidates = vec![
        ("AAA".to_string(), 6.0),
        ("BBB".to_string(), 5.0),
        ("CCC".to_string(), 4.0),
        ("DDD".to_string(), 3.0),
    ];
    let symbols = candidates
        .iter()
        .map(|(symbol, _)| symbol.clone())
        .collect::<Vec<_>>();
    let return_history = HashMap::from([
        (
            "AAA".to_string(),
            dated_returns(&[0.010, -0.020, 0.030, -0.010, 0.020]),
        ),
        (
            "BBB".to_string(),
            dated_returns(&[0.011, -0.019, 0.029, -0.011, 0.021]),
        ),
        (
            "CCC".to_string(),
            dated_returns(&[-0.020, 0.010, -0.015, 0.020, -0.010]),
        ),
        (
            "DDD".to_string(),
            dated_returns(&[0.040, -0.035, 0.030, -0.025, 0.020]),
        ),
    ]);
    let average_amounts = HashMap::from([
        ("AAA".to_string(), 900_000_000.0),
        ("BBB".to_string(), 800_000_000.0),
        ("CCC".to_string(), 600_000_000.0),
        ("DDD".to_string(), 400_000_000.0),
    ]);
    let config = PortfolioConstructionConfig {
        top_n: 2,
        max_pairwise_correlation: Some(0.80),
        correlation_lookback_days: 5,
        risk_budget_lookback_days: 5,
        capacity_penalty_strength: 0.25,
        candidate_risk_filter_profile:
            CandidateRiskFilterProfile::SoftLowVolatilityLowCorrelationV1,
        ..Default::default()
    };
    let stats_matrix =
        build_score_date_return_risk_stats_matrix(&return_history, &[score_day], &symbols, 5);

    assert_eq!(
        select_uncorrelated_candidates(
            score_day,
            &candidates,
            &stats_matrix,
            &config,
            4,
        ),
        select_uncorrelated_candidates(
            score_day,
            &candidates,
            &ReturnHistoryMatrixView::new(&return_history, config.correlation_lookback_days),
            &config,
            4
        )
    );
    assert_eq!(
        filter_candidate_risk_pool(
            score_day,
            &candidates,
            &stats_matrix,
            &average_amounts,
            &config,
        ),
        filter_candidate_risk_pool(
            score_day,
            &candidates,
            &ReturnHistoryMatrixView::new(&return_history, config.risk_budget_lookback_days),
            &average_amounts,
            &config,
        )
    );
    assert_eq!(
        build_risk_budget_raw_weights(
            score_day,
            &symbols,
            &stats_matrix,
            &average_amounts,
            &config,
        ),
        build_risk_budget_raw_weights(
            score_day,
            &symbols,
            &ReturnHistoryMatrixView::new(&return_history, config.risk_budget_lookback_days),
            &average_amounts,
            &config,
        )
    );
    assert_eq!(
        build_min_variance_raw_weights(
            score_day,
            &symbols,
            &stats_matrix,
            &average_amounts,
            &config,
        ),
        build_min_variance_raw_weights(
            score_day,
            &symbols,
            &ReturnHistoryMatrixView::new(&return_history, config.risk_budget_lookback_days),
            &average_amounts,
            &config,
        )
    );
}

#[test]
fn return_risk_stats_matrix_rows_round_trip_single_and_pairwise_stats() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let symbols = vec!["AAA".to_string(), "BBB".to_string(), "CCC".to_string()];
    let return_history = HashMap::from([
        (
            "AAA".to_string(),
            dated_returns(&[0.010, -0.020, 0.030, -0.010, 0.020]),
        ),
        (
            "BBB".to_string(),
            dated_returns(&[0.011, -0.019, 0.029, -0.011, 0.021]),
        ),
        (
            "CCC".to_string(),
            dated_returns(&[-0.020, 0.010, -0.015, 0.020, -0.010]),
        ),
    ]);
    let matrix =
        build_score_date_return_risk_stats_matrix(&return_history, &[score_day], &symbols, 5);
    let (stats_rows, pair_rows) = return_risk_stats_feature_matrix_to_rows(&matrix);

    assert_eq!(stats_rows.len(), symbols.len());
    assert_eq!(pair_rows.len(), 3);

    let restored = return_risk_stats_feature_matrix_from_rows(
        &[score_day],
        &symbols,
        stats_rows,
        pair_rows,
    )
    .expect("restored stats matrix");

    assert_eq!(
        restored.total_return(score_day, "AAA"),
        matrix.total_return(score_day, "AAA")
    );
    assert_eq!(
        restored.sample_volatility(score_day, "AAA"),
        matrix.sample_volatility(score_day, "AAA")
    );
    assert_eq!(
        restored.fractional_kelly_weight(score_day, "AAA", 0.30),
        matrix.fractional_kelly_weight(score_day, "AAA", 0.30)
    );
    assert_eq!(
        restored.pearson_correlation(score_day, "AAA", "BBB"),
        matrix.pearson_correlation(score_day, "AAA", "BBB")
    );
    assert_eq!(
        restored.average_abs_correlation_to_reference(score_day, "AAA", &symbols),
        matrix.average_abs_correlation_to_reference(score_day, "AAA", &symbols)
    );
}

#[test]
fn return_risk_stats_feature_matrix_rows_match_raw_matrix_consumers() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let future_day = NaiveDate::from_ymd_opt(2026, 1, 9).unwrap();
    let candidates = vec![
        ("AAA".to_string(), 6.0),
        ("BBB".to_string(), 5.0),
        ("CCC".to_string(), 4.0),
        ("DDD".to_string(), 3.0),
    ];
    let symbols = candidates
        .iter()
        .map(|(symbol, _)| symbol.clone())
        .collect::<Vec<_>>();
    let return_history = HashMap::from([
        (
            "AAA".to_string(),
            vec![
                (NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(), 0.010),
                (NaiveDate::from_ymd_opt(2026, 1, 3).unwrap(), -0.020),
                (NaiveDate::from_ymd_opt(2026, 1, 4).unwrap(), 0.030),
                (NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(), -0.010),
                (NaiveDate::from_ymd_opt(2026, 1, 6).unwrap(), 0.020),
                (NaiveDate::from_ymd_opt(2026, 1, 7).unwrap(), 0.015),
                (future_day, 0.750),
            ],
        ),
        (
            "BBB".to_string(),
            vec![
                (NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(), 0.012),
                (NaiveDate::from_ymd_opt(2026, 1, 3).unwrap(), -0.018),
                (NaiveDate::from_ymd_opt(2026, 1, 4).unwrap(), 0.028),
                (NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(), -0.009),
                (NaiveDate::from_ymd_opt(2026, 1, 6).unwrap(), 0.022),
                (NaiveDate::from_ymd_opt(2026, 1, 7).unwrap(), 0.014),
                (future_day, -0.700),
            ],
        ),
        (
            "CCC".to_string(),
            vec![
                (NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(), -0.020),
                (NaiveDate::from_ymd_opt(2026, 1, 3).unwrap(), 0.010),
                (NaiveDate::from_ymd_opt(2026, 1, 4).unwrap(), -0.015),
                (NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(), 0.020),
                (NaiveDate::from_ymd_opt(2026, 1, 6).unwrap(), -0.010),
                (NaiveDate::from_ymd_opt(2026, 1, 7).unwrap(), 0.005),
                (future_day, 0.600),
            ],
        ),
        (
            "DDD".to_string(),
            vec![
                (NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(), 0.040),
                (NaiveDate::from_ymd_opt(2026, 1, 3).unwrap(), -0.035),
                (NaiveDate::from_ymd_opt(2026, 1, 4).unwrap(), 0.030),
                (NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(), -0.025),
                (NaiveDate::from_ymd_opt(2026, 1, 6).unwrap(), 0.020),
                (NaiveDate::from_ymd_opt(2026, 1, 7).unwrap(), -0.015),
                (future_day, -0.500),
            ],
        ),
    ]);
    let average_amounts = HashMap::from([
        ("AAA".to_string(), 900_000_000.0),
        ("BBB".to_string(), 800_000_000.0),
        ("CCC".to_string(), 600_000_000.0),
        ("DDD".to_string(), 400_000_000.0),
    ]);
    let config = PortfolioConstructionConfig {
        top_n: 2,
        max_pairwise_correlation: Some(0.80),
        correlation_lookback_days: 5,
        risk_budget_lookback_days: 5,
        kelly_fraction: 0.30,
        kelly_lookback_days: 5,
        capacity_penalty_strength: 0.25,
        candidate_risk_filter_profile:
            CandidateRiskFilterProfile::SoftLowVolatilityLowCorrelationV1,
        ..Default::default()
    };
    let raw_matrix =
        build_score_date_return_risk_matrix(&return_history, &[score_day], &symbols, 5);
    let stats_matrix =
        build_score_date_return_risk_stats_matrix(&return_history, &[score_day], &symbols, 5);
    let (stats_rows, pair_rows) = return_risk_stats_feature_matrix_to_rows(&stats_matrix);
    let db_stats_rows = stats_rows
        .iter()
        .map(|row| {
            (
                row.score_day,
                row.symbol.clone(),
                row.return_count as i64,
                row.total_return,
                row.sample_volatility,
                row.kelly_mean,
                row.kelly_population_variance,
            )
        })
        .collect::<Vec<_>>();
    let db_pair_rows = pair_rows
        .iter()
        .map(|row| {
            (
                row.score_day,
                row.left_symbol.clone(),
                row.right_symbol.clone(),
                row.correlation,
            )
        })
        .collect::<Vec<_>>();
    let restored = persistent_return_risk_stats_feature_matrix_rows_to_matrix(
        &[score_day],
        &symbols,
        stats_rows.len() as i64,
        pair_rows.len() as i64,
        db_stats_rows,
        db_pair_rows,
    )
    .expect("stats matrix should restore from DB-shaped rows");

    assert_eq!(
        relative_strength_rank_scores(&candidates, &restored, score_day),
        relative_strength_rank_scores(&candidates, &raw_matrix, score_day)
    );
    assert_eq!(
        filter_candidate_risk_pool(
            score_day,
            &candidates,
            &restored,
            &average_amounts,
            &config,
        ),
        filter_candidate_risk_pool(
            score_day,
            &candidates,
            &raw_matrix,
            &average_amounts,
            &config,
        )
    );
    assert_eq!(
        select_uncorrelated_candidates(
            score_day,
            &candidates,
            &restored,
            &config,
            5,
        ),
        select_uncorrelated_candidates(
            score_day,
            &candidates,
            &raw_matrix,
            &config,
            5,
        )
    );
    assert_eq!(
        build_kelly_raw_weights(score_day, &symbols, &restored, &config),
        build_kelly_raw_weights(score_day, &symbols, &raw_matrix, &config)
    );
    assert_eq!(
        build_risk_budget_raw_weights(
            score_day,
            &symbols,
            &restored,
            &average_amounts,
            &config,
        ),
        build_risk_budget_raw_weights(
            score_day,
            &symbols,
            &raw_matrix,
            &average_amounts,
            &config,
        )
    );
    assert_eq!(
        build_min_variance_raw_weights(
            score_day,
            &symbols,
            &restored,
            &average_amounts,
            &config,
        ),
        build_min_variance_raw_weights(
            score_day,
            &symbols,
            &raw_matrix,
            &average_amounts,
            &config,
        )
    );
}

#[test]
fn return_risk_stats_pairwise_scope_plan_deduplicates_canonical_pairs() {
    let first_score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let second_score_day = NaiveDate::from_ymd_opt(2026, 1, 9).unwrap();
    let symbols = vec![
        "BBB".to_string(),
        "AAA".to_string(),
        "CCC".to_string(),
        "AAA".to_string(),
    ];
    let candidate_groups = vec![
        vec![
            "BBB".to_string(),
            "AAA".to_string(),
            "AAA".to_string(),
            "MISSING".to_string(),
        ],
        vec!["CCC".to_string(), "BBB".to_string()],
        vec!["AAA".to_string()],
    ];

    let plan = return_risk_stats_pairwise_scope_from_symbol_groups(
        &[second_score_day, first_score_day, first_score_day],
        &symbols,
        &candidate_groups,
    );

    assert_eq!(plan.score_days, vec![first_score_day, second_score_day]);
    assert_eq!(
        plan.symbols,
        vec!["AAA".to_string(), "BBB".to_string(), "CCC".to_string()]
    );
    assert_eq!(
        plan.pair_keys,
        vec![
            pairwise_correlation_key(first_score_day, "AAA", "BBB"),
            pairwise_correlation_key(first_score_day, "BBB", "CCC"),
            pairwise_correlation_key(second_score_day, "AAA", "BBB"),
            pairwise_correlation_key(second_score_day, "BBB", "CCC"),
        ]
    );
    assert_eq!(plan.pair_count(), 4);
    assert!(plan.contains(first_score_day, "BBB", "AAA"));
    assert!(!plan.contains(first_score_day, "AAA", "CCC"));
    assert!(!plan.contains(first_score_day, "AAA", "AAA"));
}

#[test]
fn sparse_return_risk_stats_matrix_matches_dense_for_requested_pairs() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let symbols = vec![
        "AAA".to_string(),
        "BBB".to_string(),
        "CCC".to_string(),
        "DDD".to_string(),
    ];
    let return_history = HashMap::from([
        (
            "AAA".to_string(),
            dated_returns(&[0.010, -0.020, 0.030, -0.010, 0.020, 0.015]),
        ),
        (
            "BBB".to_string(),
            dated_returns(&[0.011, -0.019, 0.029, -0.011, 0.021, 0.014]),
        ),
        (
            "CCC".to_string(),
            dated_returns(&[-0.020, 0.010, -0.015, 0.020, -0.010, 0.005]),
        ),
        (
            "DDD".to_string(),
            dated_returns(&[0.040, -0.035, 0.030, -0.025, 0.020, -0.015]),
        ),
    ]);
    let pairwise_scope = return_risk_stats_pairwise_scope_from_symbol_groups(
        &[score_day],
        &symbols,
        &[
            vec!["AAA".to_string(), "BBB".to_string()],
            vec!["AAA".to_string(), "CCC".to_string()],
        ],
    );

    let dense =
        build_score_date_return_risk_stats_matrix(&return_history, &[score_day], &symbols, 5);
    let sparse = build_score_date_return_risk_stats_matrix_with_pairwise_scope(
        &return_history,
        &[score_day],
        &symbols,
        5,
        &pairwise_scope,
    );

    for symbol in &symbols {
        assert_eq!(
            sparse.total_return(score_day, symbol),
            dense.total_return(score_day, symbol)
        );
        assert_eq!(
            sparse.sample_volatility(score_day, symbol),
            dense.sample_volatility(score_day, symbol)
        );
    }
    assert_eq!(
        sparse.pearson_correlation(score_day, "AAA", "BBB"),
        dense.pearson_correlation(score_day, "AAA", "BBB")
    );
    assert_eq!(
        sparse.pearson_correlation(score_day, "CCC", "AAA"),
        dense.pearson_correlation(score_day, "CCC", "AAA")
    );
    assert_eq!(sparse.pearson_correlation(score_day, "BBB", "CCC"), None);
    assert_eq!(sparse.pearson_correlation(score_day, "AAA", "DDD"), None);
}

#[test]
fn sparse_return_risk_stats_payload_profile_allows_full_universe_stats_with_candidate_pairs() {
    let score_days = vec![
        NaiveDate::from_ymd_opt(2026, 1, 8).unwrap(),
        NaiveDate::from_ymd_opt(2026, 1, 9).unwrap(),
    ];
    let symbols = (0..2_000)
        .map(|idx| format!("S{idx:04}"))
        .collect::<Vec<_>>();
    let candidate_symbols = symbols.iter().take(50).cloned().collect::<Vec<_>>();
    let plan = return_risk_stats_pairwise_scope_from_symbols(
        &score_days,
        &symbols,
        &candidate_symbols,
    );
    let profile = return_risk_stats_feature_matrix_payload_profile(
        &score_days,
        &symbols,
        plan.pair_count(),
        DEFAULT_RETURN_RISK_STATS_PAIRWISE_ROW_LIMIT,
    );

    assert_eq!(profile.stats_rows, 4_000);
    assert_eq!(profile.dense_pair_capacity, Some(3_998_000));
    assert_eq!(profile.pair_rows, 2_450);
    assert_eq!(
        profile.status,
        ReturnRiskStatsFeatureMatrixPayloadStatus::WithinBudget
    );
    assert!(profile.within_budget());
}

#[test]
fn portfolio_construction_matches_raw_with_candidate_scoped_stats_matrix() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let candidates = vec![
        ("AAA".to_string(), 6.0),
        ("BBB".to_string(), 5.0),
        ("CCC".to_string(), 4.0),
        ("DDD".to_string(), 3.0),
        ("EEE".to_string(), 2.0),
    ];
    let candidate_symbols = candidates
        .iter()
        .map(|(symbol, _)| symbol.clone())
        .collect::<Vec<_>>();
    let full_symbol_scope = candidate_symbols
        .iter()
        .cloned()
        .chain(["ZZZ".to_string()])
        .collect::<Vec<_>>();
    let return_history = HashMap::from([
        (
            "AAA".to_string(),
            dated_returns(&[0.010, -0.010, 0.015, -0.005, 0.020, 0.011]),
        ),
        (
            "BBB".to_string(),
            dated_returns(&[0.011, -0.011, 0.016, -0.006, 0.021, 0.010]),
        ),
        (
            "CCC".to_string(),
            dated_returns(&[-0.020, 0.010, -0.015, 0.020, -0.010, 0.004]),
        ),
        (
            "DDD".to_string(),
            dated_returns(&[0.050, -0.045, 0.040, -0.035, 0.030, -0.025]),
        ),
        (
            "EEE".to_string(),
            dated_returns(&[0.004, 0.006, 0.005, 0.007, 0.006, 0.005]),
        ),
        (
            "ZZZ".to_string(),
            dated_returns(&[0.100, -0.090, 0.080, -0.070, 0.060, -0.050]),
        ),
    ]);
    let average_amounts = HashMap::from([
        ("AAA".to_string(), 900_000_000.0),
        ("BBB".to_string(), 800_000_000.0),
        ("CCC".to_string(), 500_000_000.0),
        ("DDD".to_string(), 300_000_000.0),
        ("EEE".to_string(), 700_000_000.0),
        ("ZZZ".to_string(), 50_000_000.0),
    ]);
    let config = PortfolioConstructionConfig {
        top_n: 3,
        max_pairwise_correlation: Some(0.80),
        correlation_lookback_days: 5,
        portfolio_method: PortfolioConstructionMethod::RiskBudget,
        risk_budget_lookback_days: 5,
        capacity_penalty_strength: 0.25,
        candidate_ranking_profile: CandidateRankingProfile::RelativeStrengthAlphaLiquidityV1,
        candidate_risk_filter_profile:
            CandidateRiskFilterProfile::SoftLowVolatilityLowCorrelationV1,
        ..Default::default()
    };
    let pairwise_scope = return_risk_stats_pairwise_scope_for_portfolio_candidate_pool(
        score_day,
        &candidate_symbols,
        &full_symbol_scope,
    );
    let stats_matrix = build_score_date_return_risk_stats_matrix_with_pairwise_scope(
        &return_history,
        &[score_day],
        &full_symbol_scope,
        5,
        &pairwise_scope,
    );

    let raw_weights = build_portfolio_weights(
        score_day,
        &candidates,
        &return_history,
        &average_amounts,
        &HashMap::new(),
        &config,
    );
    let stats_weights = build_portfolio_weights_with_return_risk_stats_matrix(
        score_day,
        &candidates,
        &stats_matrix,
        &average_amounts,
        &HashMap::new(),
        &config,
    );

    assert!(!raw_weights.is_empty());
    assert_eq!(stats_weights, raw_weights);
    assert!(pairwise_scope.contains(score_day, "AAA", "BBB"));
    assert!(!pairwise_scope.contains(score_day, "AAA", "ZZZ"));
    assert!(stats_matrix
        .pearson_correlation(score_day, "AAA", "ZZZ")
        .is_none());
}

#[test]
fn portfolio_construction_matches_raw_with_multi_lookback_stats_matrices() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let candidates = vec![
        ("AAA".to_string(), 6.0),
        ("BBB".to_string(), 5.0),
        ("CCC".to_string(), 4.0),
        ("DDD".to_string(), 3.0),
        ("EEE".to_string(), 2.0),
    ];
    let candidate_symbols = candidates
        .iter()
        .map(|(symbol, _)| symbol.clone())
        .collect::<Vec<_>>();
    let return_history = HashMap::from([
        (
            "AAA".to_string(),
            dated_returns(&[0.010, -0.010, 0.015, -0.005, 0.020, 0.011]),
        ),
        (
            "BBB".to_string(),
            dated_returns(&[0.011, -0.011, 0.016, -0.006, 0.021, 0.010]),
        ),
        (
            "CCC".to_string(),
            dated_returns(&[-0.020, 0.010, -0.015, 0.020, -0.010, 0.004]),
        ),
        (
            "DDD".to_string(),
            dated_returns(&[0.050, -0.045, 0.040, -0.035, 0.030, -0.025]),
        ),
        (
            "EEE".to_string(),
            dated_returns(&[0.004, 0.006, 0.005, 0.007, 0.006, 0.005]),
        ),
    ]);
    let average_amounts = HashMap::from([
        ("AAA".to_string(), 900_000_000.0),
        ("BBB".to_string(), 800_000_000.0),
        ("CCC".to_string(), 500_000_000.0),
        ("DDD".to_string(), 300_000_000.0),
        ("EEE".to_string(), 700_000_000.0),
    ]);
    let config = PortfolioConstructionConfig {
        top_n: 3,
        max_pairwise_correlation: Some(0.80),
        correlation_lookback_days: 3,
        portfolio_method: PortfolioConstructionMethod::RiskBudget,
        risk_budget_lookback_days: 5,
        capacity_penalty_strength: 0.25,
        candidate_ranking_profile: CandidateRankingProfile::RelativeStrengthAlphaLiquidityV1,
        candidate_risk_filter_profile:
            CandidateRiskFilterProfile::SoftLowVolatilityLowCorrelationV1,
        ..Default::default()
    };
    let pairwise_scope = return_risk_stats_pairwise_scope_for_portfolio_candidate_pool(
        score_day,
        &candidate_symbols,
        &candidate_symbols,
    );
    let stats_matrices = HashMap::from([
        (
            3,
            Arc::new(
                build_score_date_return_risk_stats_matrix_with_pairwise_scope(
                    &return_history,
                    &[score_day],
                    &candidate_symbols,
                    3,
                    &pairwise_scope,
                ),
            ),
        ),
        (
            5,
            Arc::new(
                build_score_date_return_risk_stats_matrix_with_pairwise_scope(
                    &return_history,
                    &[score_day],
                    &candidate_symbols,
                    5,
                    &pairwise_scope,
                ),
            ),
        ),
    ]);

    let raw_weights = build_portfolio_weights(
        score_day,
        &candidates,
        &return_history,
        &average_amounts,
        &HashMap::new(),
        &config,
    );
    let stats_weights = build_portfolio_weights_with_return_risk_stats_matrices(
        score_day,
        &candidates,
        &stats_matrices,
        &average_amounts,
        &HashMap::new(),
        &config,
    );

    assert!(!raw_weights.is_empty());
    assert_eq!(stats_weights, raw_weights);
}

#[test]
fn return_risk_stats_payload_profile_flags_dense_full_market_pairwise_cache() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let symbols = (0..2_000)
        .map(|idx| format!("S{idx:04}"))
        .collect::<Vec<_>>();

    let profile = return_risk_stats_feature_matrix_payload_profile(
        &[score_day],
        &symbols,
        1_999_000,
        DEFAULT_RETURN_RISK_STATS_PAIRWISE_ROW_LIMIT,
    );

    assert_eq!(profile.stats_rows, 2_000);
    assert_eq!(profile.dense_pair_capacity, Some(1_999_000));
    assert_eq!(profile.pair_rows, 1_999_000);
    assert_eq!(
        profile.status,
        ReturnRiskStatsFeatureMatrixPayloadStatus::RequiresSparsePairwiseCache
    );
    assert!(!profile.within_budget());
}

#[test]
fn return_risk_stats_payload_profile_allows_candidate_scoped_pairwise_cache() {
    let score_days = vec![
        NaiveDate::from_ymd_opt(2026, 1, 8).unwrap(),
        NaiveDate::from_ymd_opt(2026, 1, 9).unwrap(),
    ];
    let symbols = (0..20).map(|idx| format!("S{idx:04}")).collect::<Vec<_>>();

    let profile = return_risk_stats_feature_matrix_payload_profile(
        &score_days,
        &symbols,
        380,
        DEFAULT_RETURN_RISK_STATS_PAIRWISE_ROW_LIMIT,
    );

    assert_eq!(profile.stats_rows, 40);
    assert_eq!(profile.dense_pair_capacity, Some(380));
    assert_eq!(profile.pair_rows_per_stats_row, Some(9.5));
    assert_eq!(
        profile.status,
        ReturnRiskStatsFeatureMatrixPayloadStatus::WithinBudget
    );
    assert!(profile.within_budget());
}

#[test]
fn return_risk_stats_matrix_rows_reject_incomplete_or_corrupt_payloads() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let symbols = vec!["AAA".to_string(), "BBB".to_string()];
    let return_history = HashMap::from([
        (
            "AAA".to_string(),
            dated_returns(&[0.010, -0.020, 0.030, -0.010, 0.020]),
        ),
        (
            "BBB".to_string(),
            dated_returns(&[0.011, -0.019, 0.029, -0.011, 0.021]),
        ),
    ]);
    let matrix =
        build_score_date_return_risk_stats_matrix(&return_history, &[score_day], &symbols, 5);
    let (stats_rows, pair_rows) = return_risk_stats_feature_matrix_to_rows(&matrix);

    assert!(return_risk_stats_feature_matrix_from_rows(
        &[score_day],
        &symbols,
        stats_rows[..1].to_vec(),
        pair_rows.clone(),
    )
    .is_none());

    let mut duplicate_stats = stats_rows.clone();
    duplicate_stats.push(stats_rows[0].clone());
    assert!(return_risk_stats_feature_matrix_from_rows(
        &[score_day],
        &symbols,
        duplicate_stats,
        pair_rows.clone(),
    )
    .is_none());

    let mut corrupt_stats = stats_rows.clone();
    corrupt_stats[0].sample_volatility = Some(f64::NAN);
    assert!(return_risk_stats_feature_matrix_from_rows(
        &[score_day],
        &symbols,
        corrupt_stats,
        pair_rows.clone(),
    )
    .is_none());

    let mut reversed_pair = pair_rows.clone();
    reversed_pair[0] = ReturnRiskPairwiseCorrelationRow {
        left_symbol: "BBB".to_string(),
        right_symbol: "AAA".to_string(),
        ..reversed_pair[0].clone()
    };
    assert!(return_risk_stats_feature_matrix_from_rows(
        &[score_day],
        &symbols,
        stats_rows.clone(),
        reversed_pair,
    )
    .is_none());

    let mut corrupt_pair = pair_rows;
    corrupt_pair[0].correlation = f64::INFINITY;
    assert!(return_risk_stats_feature_matrix_from_rows(
        &[score_day],
        &symbols,
        stats_rows,
        corrupt_pair,
    )
    .is_none());
}

#[test]
fn build_portfolio_weights_uses_score_date_return_risk_matrix_consumers() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let candidates = vec![
        ("AAA".to_string(), 6.0),
        ("BBB".to_string(), 5.0),
        ("CCC".to_string(), 4.0),
        ("DDD".to_string(), 3.0),
    ];
    let return_history = HashMap::from([
        (
            "AAA".to_string(),
            dated_returns(&[0.010, -0.010, 0.015, -0.005, 0.020]),
        ),
        (
            "BBB".to_string(),
            dated_returns(&[0.011, -0.011, 0.016, -0.006, 0.021]),
        ),
        (
            "CCC".to_string(),
            dated_returns(&[-0.020, 0.010, -0.015, 0.020, -0.010]),
        ),
        (
            "DDD".to_string(),
            dated_returns(&[0.050, -0.045, 0.040, -0.035, 0.030]),
        ),
    ]);
    let average_amounts = HashMap::from([
        ("AAA".to_string(), 900_000_000.0),
        ("BBB".to_string(), 800_000_000.0),
        ("CCC".to_string(), 500_000_000.0),
        ("DDD".to_string(), 300_000_000.0),
    ]);
    let config = PortfolioConstructionConfig {
        top_n: 2,
        max_pairwise_correlation: Some(0.80),
        correlation_lookback_days: 4,
        kelly_fraction: 0.35,
        kelly_lookback_days: 3,
        portfolio_method: PortfolioConstructionMethod::RiskBudget,
        risk_budget_lookback_days: 5,
        capacity_penalty_strength: 0.25,
        candidate_ranking_profile: CandidateRankingProfile::RelativeStrengthAlphaLiquidityV1,
        candidate_risk_filter_profile:
            CandidateRiskFilterProfile::SoftLowVolatilityLowCorrelationV1,
        ..Default::default()
    };

    reset_score_date_return_risk_matrix_read_count();
    let weights = build_portfolio_weights(
        score_day,
        &candidates,
        &return_history,
        &average_amounts,
        &HashMap::new(),
        &config,
    );

    assert!(!weights.is_empty());
    assert!(score_date_return_risk_matrix_read_count() > 0);
}

#[test]
fn style_risk_budget_uses_score_date_return_risk_matrix() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let candidates = vec![
        ("HIGH_VOL".to_string(), 4.0),
        ("LOW_VOL_A".to_string(), 3.0),
        ("LOW_VOL_B".to_string(), 2.0),
        ("LOW_VOL_C".to_string(), 1.0),
    ];
    let return_history = HashMap::from([
        (
            "HIGH_VOL".to_string(),
            dated_returns(&[0.12, -0.11, 0.10, -0.09, 0.08]),
        ),
        (
            "LOW_VOL_A".to_string(),
            dated_returns(&[0.004, 0.003, 0.005, 0.004, 0.003]),
        ),
        (
            "LOW_VOL_B".to_string(),
            dated_returns(&[0.005, 0.004, 0.003, 0.004, 0.005]),
        ),
        (
            "LOW_VOL_C".to_string(),
            dated_returns(&[0.006, 0.005, 0.004, 0.005, 0.006]),
        ),
    ]);
    let average_amounts = HashMap::from([
        ("HIGH_VOL".to_string(), 800_000_000.0),
        ("LOW_VOL_A".to_string(), 700_000_000.0),
        ("LOW_VOL_B".to_string(), 600_000_000.0),
        ("LOW_VOL_C".to_string(), 500_000_000.0),
    ]);
    let config = PortfolioConstructionConfig {
        top_n: 4,
        max_position_pct: Decimal::new(40, 2),
        risk_budget_lookback_days: 5,
        style_risk_budget_profile: StyleRiskBudgetProfile::DefensiveStyleBudgetV1,
        ..Default::default()
    };

    reset_score_date_return_risk_matrix_read_count();
    let weights = build_portfolio_weights(
        score_day,
        &candidates,
        &return_history,
        &average_amounts,
        &HashMap::new(),
        &config,
    );

    assert!(!weights.is_empty());
    assert!(score_date_return_risk_matrix_read_count() > 0);
}

#[test]
fn risk_contribution_control_uses_score_date_return_risk_matrix() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let candidates = vec![
        ("HIGH_RISK".to_string(), 3.0),
        ("LOW_RISK_A".to_string(), 2.0),
        ("LOW_RISK_B".to_string(), 1.0),
    ];
    let return_history = HashMap::from([
        (
            "HIGH_RISK".to_string(),
            dated_returns(&[0.12, -0.11, 0.10, -0.09, 0.08]),
        ),
        (
            "LOW_RISK_A".to_string(),
            dated_returns(&[0.004, 0.003, 0.005, 0.004, 0.003]),
        ),
        (
            "LOW_RISK_B".to_string(),
            dated_returns(&[0.005, 0.004, 0.003, 0.004, 0.005]),
        ),
    ]);
    let average_amounts = candidates
        .iter()
        .map(|(symbol, _)| (symbol.clone(), 500_000_000.0))
        .collect::<HashMap<_, _>>();
    let config = PortfolioConstructionConfig {
        top_n: 3,
        max_position_pct: Decimal::new(80, 2),
        risk_budget_lookback_days: 5,
        risk_contribution_control_profile:
            RiskContributionControlProfile::SoftSingleName20PctV1,
        ..Default::default()
    };

    reset_score_date_return_risk_matrix_read_count();
    let weights = build_portfolio_weights(
        score_day,
        &candidates,
        &return_history,
        &average_amounts,
        &HashMap::new(),
        &config,
    );

    assert!(!weights.is_empty());
    assert!(score_date_return_risk_matrix_read_count() > 0);
}

#[test]
fn cash_utilization_profile_expands_holdings_to_restore_fillable_gross() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let candidates = (0..20)
        .map(|idx| (format!("S{:02}", idx + 1), 100.0 - idx as f64))
        .collect::<Vec<_>>();
    let average_amounts = candidates
        .iter()
        .map(|(symbol, _)| (symbol.clone(), 50_000_000.0))
        .collect::<HashMap<_, _>>();
    let config = PortfolioConstructionConfig {
        top_n: 5,
        max_position_pct: Decimal::new(20, 2),
        portfolio_notional_cny: Some(100_000_000.0),
        max_participation_rate: Some(0.10),
        max_gross_exposure: 1.0,
        cash_utilization_profile: CashUtilizationProfile::FillableGross90V1,
        ..Default::default()
    };

    let weights = build_portfolio_weights(
        score_day,
        &candidates,
        &HashMap::new(),
        &average_amounts,
        &HashMap::new(),
        &config,
    );
    let gross = weights.values().copied().sum::<Decimal>();

    assert!(weights.len() > 5);
    assert!(gross >= Decimal::new(90, 2));
    assert!(weights.values().all(|weight| *weight <= Decimal::new(5, 2)));
}

#[test]
fn stress_fill_cash_utilization_expands_deeper_to_restore_strict_fillable_gross() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let candidates = (0..150)
        .map(|idx| (format!("S{:03}", idx + 1), 100.0 - idx as f64))
        .collect::<Vec<_>>();
    let average_amounts = candidates
        .iter()
        .map(|(symbol, _)| (symbol.clone(), 25_000_000.0))
        .collect::<HashMap<_, _>>();
    let config = PortfolioConstructionConfig {
        top_n: 60,
        max_position_pct: Decimal::new(10, 2),
        portfolio_notional_cny: Some(100_000_000.0),
        max_participation_rate: Some(0.05),
        max_gross_exposure: 1.0,
        cash_utilization_profile: CashUtilizationProfile::StressFillGross98V1,
        ..Default::default()
    };

    let weights = build_portfolio_weights(
        score_day,
        &candidates,
        &HashMap::new(),
        &average_amounts,
        &HashMap::new(),
        &config,
    );
    let gross = weights.values().copied().sum::<Decimal>();

    assert!(weights.len() > 60);
    assert!(gross >= Decimal::new(98, 2));
    assert!(weights
        .values()
        .all(|weight| *weight <= Decimal::new(125, 4)));
}

#[test]
fn capacity_risk_budget_caps_low_capacity_bucket_and_redistributes() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let candidates = vec![
        ("DEEP_A".to_string(), 4.0),
        ("THIN_A".to_string(), 3.0),
        ("DEEP_B".to_string(), 2.0),
        ("THIN_B".to_string(), 1.0),
    ];
    let average_amounts = HashMap::from([
        ("DEEP_A".to_string(), 900_000_000.0),
        ("DEEP_B".to_string(), 800_000_000.0),
        ("THIN_A".to_string(), 20_000_000.0),
        ("THIN_B".to_string(), 10_000_000.0),
    ]);
    let config = PortfolioConstructionConfig {
        top_n: 4,
        max_position_pct: Decimal::new(80, 2),
        max_gross_exposure: 1.0,
        capacity_risk_budget_profile: CapacityRiskBudgetProfile::ParticipationStrictV1,
        ..Default::default()
    };

    let weights = build_portfolio_weights(
        score_day,
        &candidates,
        &HashMap::new(),
        &average_amounts,
        &HashMap::new(),
        &config,
    );

    let low_capacity_weight = weights["THIN_A"] + weights["THIN_B"];
    let deep_capacity_weight = weights["DEEP_A"] + weights["DEEP_B"];

    assert!(low_capacity_weight <= Decimal::new(20, 2));
    assert!(deep_capacity_weight >= Decimal::new(80, 2));
    assert!(weights["DEEP_A"] > Decimal::new(25, 2));
    assert!(weights["DEEP_B"] > Decimal::new(25, 2));
}

#[test]
fn stress_participation_budget_soft_caps_names_under_tight_capacity() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let candidates = vec![
        ("DEEP_A".to_string(), 6.0),
        ("THIN_A".to_string(), 5.0),
        ("DEEP_B".to_string(), 4.0),
        ("THIN_B".to_string(), 3.0),
        ("DEEP_C".to_string(), 2.0),
        ("DEEP_D".to_string(), 1.0),
    ];
    let average_amounts = HashMap::from([
        ("DEEP_A".to_string(), 500_000_000.0),
        ("DEEP_B".to_string(), 500_000_000.0),
        ("DEEP_C".to_string(), 500_000_000.0),
        ("DEEP_D".to_string(), 500_000_000.0),
        ("THIN_A".to_string(), 20_000_000.0),
        ("THIN_B".to_string(), 20_000_000.0),
    ]);
    let config = PortfolioConstructionConfig {
        top_n: 6,
        max_position_pct: Decimal::new(40, 2),
        max_gross_exposure: 1.0,
        portfolio_notional_cny: Some(100_000_000.0),
        max_participation_rate: Some(0.10),
        capacity_risk_budget_profile: CapacityRiskBudgetProfile::StressParticipationSoftCapV1,
        ..Default::default()
    };

    let weights = build_portfolio_weights(
        score_day,
        &candidates,
        &HashMap::new(),
        &average_amounts,
        &HashMap::new(),
        &config,
    );

    assert!(weights["THIN_A"] <= Decimal::new(1, 2));
    assert!(weights["THIN_B"] <= Decimal::new(1, 2));
    assert!(weights["DEEP_A"] > Decimal::new(1660, 4));
    assert!(weights["DEEP_B"] > Decimal::new(1660, 4));
}

#[test]
fn stress_participation_target_scale_reduces_target_gross_to_capacity() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let candidates = (0..24)
        .map(|idx| (format!("STRESS_{idx:02}"), (24 - idx) as f64))
        .collect::<Vec<_>>();
    let average_amounts = candidates
        .iter()
        .map(|(symbol, _)| (symbol.clone(), 25_000_000.0))
        .collect::<HashMap<_, _>>();
    let config = PortfolioConstructionConfig {
        top_n: 24,
        max_position_pct: Decimal::new(8, 2),
        max_gross_exposure: 0.90,
        portfolio_notional_cny: Some(100_000_000.0),
        max_participation_rate: Some(0.05),
        capacity_risk_budget_profile:
            CapacityRiskBudgetProfile::StressParticipationTargetScaleV1,
        ..Default::default()
    };

    let weights = build_portfolio_weights(
        score_day,
        &candidates,
        &HashMap::new(),
        &average_amounts,
        &HashMap::new(),
        &config,
    );
    let gross = weights.values().copied().sum::<Decimal>();

    assert!(gross < Decimal::new(90, 2));
    assert!(gross <= Decimal::new(15, 2));
    assert!(weights.len() >= 12);
    assert!(weights
        .values()
        .all(|weight| *weight <= Decimal::new(625, 5)));
}

#[test]
fn stress_participation_floor_scale_preserves_minimum_investable_gross() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let mut candidates = (0..10)
        .map(|idx| (format!("DEEP_{idx:02}"), (20 - idx) as f64))
        .collect::<Vec<_>>();
    candidates.extend((0..2).map(|idx| (format!("THIN_{idx:02}"), (2 - idx) as f64)));
    let average_amounts = candidates
        .iter()
        .map(|(symbol, _)| {
            let amount = if symbol.starts_with("DEEP_") {
                let suffix = symbol
                    .rsplit_once('_')
                    .and_then(|(_, value)| value.parse::<usize>().ok())
                    .unwrap_or(0);
                60_000_000.0 + suffix as f64 * 5_000_000.0
            } else {
                10_000_000.0
            };
            (symbol.clone(), amount)
        })
        .collect::<HashMap<_, _>>();
    let config = PortfolioConstructionConfig {
        top_n: 12,
        max_position_pct: Decimal::new(8, 2),
        max_gross_exposure: 0.90,
        portfolio_notional_cny: Some(100_000_000.0),
        max_participation_rate: Some(0.05),
        capacity_risk_budget_profile: CapacityRiskBudgetProfile::StressParticipationFloor35V1,
        ..Default::default()
    };

    let weights = build_portfolio_weights(
        score_day,
        &candidates,
        &HashMap::new(),
        &average_amounts,
        &HashMap::new(),
        &config,
    );
    let gross = weights.values().copied().sum::<Decimal>();
    let deep_max = (0..10)
        .filter_map(|idx| weights.get(&format!("DEEP_{idx:02}")).copied())
        .max()
        .unwrap_or(Decimal::ZERO);
    let thin_weight = (0..2)
        .filter_map(|idx| weights.get(&format!("THIN_{idx:02}")).copied())
        .sum::<Decimal>();

    assert!(gross >= Decimal::new(35, 2));
    assert!(gross < Decimal::new(90, 2));
    assert!(deep_max > Decimal::new(2, 2));
    assert!(deep_max <= Decimal::new(6, 2));
    assert!(thin_weight <= Decimal::new(1, 2));
}

#[test]
fn stress_participation_return_recovery_floor_keeps_higher_investable_gross() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let mut candidates = (0..20)
        .map(|idx| (format!("DEEP_{idx:02}"), (40 - idx) as f64))
        .collect::<Vec<_>>();
    candidates.extend((0..4).map(|idx| (format!("THIN_{idx:02}"), (4 - idx) as f64)));
    let average_amounts = candidates
        .iter()
        .map(|(symbol, _)| {
            let amount = if symbol.starts_with("DEEP_") {
                let suffix = symbol
                    .rsplit_once('_')
                    .and_then(|(_, value)| value.parse::<usize>().ok())
                    .unwrap_or(0);
                120_000_000.0 + suffix as f64 * 5_000_000.0
            } else {
                10_000_000.0
            };
            (symbol.clone(), amount)
        })
        .collect::<HashMap<_, _>>();
    let config = PortfolioConstructionConfig {
        top_n: 24,
        max_position_pct: Decimal::new(6, 2),
        max_gross_exposure: 0.90,
        portfolio_notional_cny: Some(100_000_000.0),
        max_participation_rate: Some(0.05),
        capacity_risk_budget_profile: CapacityRiskBudgetProfile::StressParticipationFloor70V1,
        ..Default::default()
    };

    let weights = build_portfolio_weights(
        score_day,
        &candidates,
        &HashMap::new(),
        &average_amounts,
        &HashMap::new(),
        &config,
    );
    let gross = weights.values().copied().sum::<Decimal>();
    let thin_weight = (0..4)
        .filter_map(|idx| weights.get(&format!("THIN_{idx:02}")).copied())
        .sum::<Decimal>();

    assert!(gross >= Decimal::new(70, 2));
    assert!(gross < Decimal::new(90, 2));
    assert!(thin_weight <= Decimal::new(12, 2));
}

#[test]
fn headroom_refill_allocates_more_weight_to_symbols_with_larger_pressure_headroom() {
    let mut weights = HashMap::from([
        ("LOW_HEADROOM".to_string(), Decimal::new(10, 2)),
        ("HIGH_HEADROOM".to_string(), Decimal::new(10, 2)),
    ]);
    let caps = HashMap::from([
        ("LOW_HEADROOM".to_string(), Decimal::new(20, 2)),
        ("HIGH_HEADROOM".to_string(), Decimal::new(80, 2)),
    ]);

    let remaining = redistribute_weight_by_headroom(
        &mut weights,
        &HashSet::new(),
        Decimal::new(20, 2),
        &caps,
        4,
    );

    assert!(remaining <= Decimal::new(1, 8));
    assert!(weights["HIGH_HEADROOM"] > Decimal::new(25, 2));
    assert!(weights["LOW_HEADROOM"] < Decimal::new(15, 2));
}

#[test]
fn alpha_headroom_refill_balances_existing_alpha_weight_and_pressure_headroom() {
    let mut weights = HashMap::from([
        ("HIGH_ALPHA_LOW_HEADROOM".to_string(), Decimal::new(30, 2)),
        ("LOW_ALPHA_HIGH_HEADROOM".to_string(), Decimal::new(10, 2)),
    ]);
    let caps = HashMap::from([
        ("HIGH_ALPHA_LOW_HEADROOM".to_string(), Decimal::new(50, 2)),
        ("LOW_ALPHA_HIGH_HEADROOM".to_string(), Decimal::new(80, 2)),
    ]);

    let remaining = redistribute_weight_by_alpha_headroom(
        &mut weights,
        &HashSet::new(),
        Decimal::new(20, 2),
        &caps,
        4,
    );

    assert!(remaining <= Decimal::new(1, 8));
    assert!(weights["HIGH_ALPHA_LOW_HEADROOM"] > Decimal::new(38, 2));
    assert!(weights["LOW_ALPHA_HIGH_HEADROOM"] > Decimal::new(18, 2));
    assert!(weights["HIGH_ALPHA_LOW_HEADROOM"] < Decimal::new(50, 2));
}

#[test]
fn blended_alpha_headroom_refill_preserves_alpha_without_starving_headroom() {
    let mut weights = HashMap::from([
        ("HIGH_ALPHA_LOW_HEADROOM".to_string(), Decimal::new(30, 2)),
        ("LOW_ALPHA_HIGH_HEADROOM".to_string(), Decimal::new(10, 2)),
    ]);
    let caps = HashMap::from([
        ("HIGH_ALPHA_LOW_HEADROOM".to_string(), Decimal::new(50, 2)),
        ("LOW_ALPHA_HIGH_HEADROOM".to_string(), Decimal::new(80, 2)),
    ]);

    let remaining = redistribute_weight_by_blended_alpha_headroom(
        &mut weights,
        &HashSet::new(),
        Decimal::new(20, 2),
        &caps,
        4,
    );

    assert!(remaining <= Decimal::new(1, 8));
    assert!(weights["HIGH_ALPHA_LOW_HEADROOM"] > Decimal::new(35, 2));
    assert!(weights["LOW_ALPHA_HIGH_HEADROOM"] > Decimal::new(23, 2));
    assert!(weights["HIGH_ALPHA_LOW_HEADROOM"] < Decimal::new(50, 2));
}

#[test]
fn stress_participation_headroom_floor_keeps_target_gross_with_capacity_weighted_refill() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let mut candidates = (0..20)
        .map(|idx| (format!("DEEP_{idx:02}"), (40 - idx) as f64))
        .collect::<Vec<_>>();
    candidates.extend((0..4).map(|idx| (format!("THIN_{idx:02}"), (4 - idx) as f64)));
    let average_amounts = candidates
        .iter()
        .map(|(symbol, _)| {
            let amount = if symbol.starts_with("DEEP_") {
                let suffix = symbol
                    .rsplit_once('_')
                    .and_then(|(_, value)| value.parse::<usize>().ok())
                    .unwrap_or(0);
                120_000_000.0 + suffix as f64 * 5_000_000.0
            } else {
                10_000_000.0
            };
            (symbol.clone(), amount)
        })
        .collect::<HashMap<_, _>>();
    let config = PortfolioConstructionConfig {
        top_n: 24,
        max_position_pct: Decimal::new(6, 2),
        max_gross_exposure: 0.90,
        portfolio_notional_cny: Some(100_000_000.0),
        max_participation_rate: Some(0.05),
        capacity_risk_budget_profile:
            CapacityRiskBudgetProfile::StressParticipationHeadroomFloor70V1,
        ..Default::default()
    };

    let weights = build_portfolio_weights(
        score_day,
        &candidates,
        &HashMap::new(),
        &average_amounts,
        &HashMap::new(),
        &config,
    );
    let gross = weights.values().copied().sum::<Decimal>();
    let thin_weight = (0..4)
        .filter_map(|idx| weights.get(&format!("THIN_{idx:02}")).copied())
        .sum::<Decimal>();

    assert!(gross >= Decimal::new(70, 2));
    assert!(gross < Decimal::new(90, 2));
    assert!(thin_weight <= Decimal::new(12, 2));
}

#[test]
fn stress_participation_alpha_headroom_floor_keeps_target_gross_with_dual_objective_refill() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let mut candidates = (0..20)
        .map(|idx| (format!("DEEP_{idx:02}"), (40 - idx) as f64))
        .collect::<Vec<_>>();
    candidates.extend((0..4).map(|idx| (format!("THIN_{idx:02}"), (4 - idx) as f64)));
    let average_amounts = candidates
        .iter()
        .map(|(symbol, _)| {
            let amount = if symbol.starts_with("DEEP_") {
                let suffix = symbol
                    .rsplit_once('_')
                    .and_then(|(_, value)| value.parse::<usize>().ok())
                    .unwrap_or(0);
                120_000_000.0 + suffix as f64 * 5_000_000.0
            } else {
                10_000_000.0
            };
            (symbol.clone(), amount)
        })
        .collect::<HashMap<_, _>>();
    let config = PortfolioConstructionConfig {
        top_n: 24,
        max_position_pct: Decimal::new(6, 2),
        max_gross_exposure: 0.90,
        portfolio_notional_cny: Some(100_000_000.0),
        max_participation_rate: Some(0.05),
        capacity_risk_budget_profile:
            CapacityRiskBudgetProfile::StressParticipationAlphaHeadroomFloor70V1,
        ..Default::default()
    };

    let weights = build_portfolio_weights(
        score_day,
        &candidates,
        &HashMap::new(),
        &average_amounts,
        &HashMap::new(),
        &config,
    );
    let gross = weights.values().copied().sum::<Decimal>();
    let thin_weight = (0..4)
        .filter_map(|idx| weights.get(&format!("THIN_{idx:02}")).copied())
        .sum::<Decimal>();

    assert!(gross >= Decimal::new(70, 2));
    assert!(gross < Decimal::new(90, 2));
    assert!(thin_weight <= Decimal::new(12, 2));
}

#[test]
fn stress_participation_blended_alpha_headroom_floor_keeps_capacity_floor() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let mut candidates = (0..20)
        .map(|idx| (format!("DEEP_{idx:02}"), (40 - idx) as f64))
        .collect::<Vec<_>>();
    candidates.extend((0..4).map(|idx| (format!("THIN_{idx:02}"), (4 - idx) as f64)));
    let average_amounts = candidates
        .iter()
        .map(|(symbol, _)| {
            let amount = if symbol.starts_with("DEEP_") {
                let suffix = symbol
                    .rsplit_once('_')
                    .and_then(|(_, value)| value.parse::<usize>().ok())
                    .unwrap_or(0);
                120_000_000.0 + suffix as f64 * 5_000_000.0
            } else {
                10_000_000.0
            };
            (symbol.clone(), amount)
        })
        .collect::<HashMap<_, _>>();
    let config = PortfolioConstructionConfig {
        top_n: 24,
        max_position_pct: Decimal::new(6, 2),
        max_gross_exposure: 0.90,
        portfolio_notional_cny: Some(100_000_000.0),
        max_participation_rate: Some(0.05),
        capacity_risk_budget_profile:
            CapacityRiskBudgetProfile::StressParticipationBlendedAlphaHeadroomFloor70V1,
        ..Default::default()
    };

    let weights = build_portfolio_weights(
        score_day,
        &candidates,
        &HashMap::new(),
        &average_amounts,
        &HashMap::new(),
        &config,
    );
    let gross = weights.values().copied().sum::<Decimal>();
    let thin_weight = (0..4)
        .filter_map(|idx| weights.get(&format!("THIN_{idx:02}")).copied())
        .sum::<Decimal>();

    assert!(gross >= Decimal::new(70, 2));
    assert!(gross < Decimal::new(90, 2));
    assert!(thin_weight <= Decimal::new(12, 2));
}

#[test]
fn stress_fill_aware_risk_budget_turns_alpha_capacity_correlation_into_exposure() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let candidates = vec![
        ("HIGH_ALPHA_THIN_RISKY".to_string(), 1.00),
        ("LOWER_ALPHA_DEEP_STABLE".to_string(), 0.82),
        ("DIVERSIFIER".to_string(), 0.76),
        ("DEEP_STABLE_2".to_string(), 0.70),
        ("DEEP_STABLE_3".to_string(), 0.64),
    ];
    let mut return_history = HashMap::new();
    return_history.insert(
        "HIGH_ALPHA_THIN_RISKY".to_string(),
        vec![
            (NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(), 0.080),
            (NaiveDate::from_ymd_opt(2026, 1, 3).unwrap(), -0.070),
            (NaiveDate::from_ymd_opt(2026, 1, 4).unwrap(), 0.065),
            (NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(), -0.060),
            (NaiveDate::from_ymd_opt(2026, 1, 6).unwrap(), 0.055),
        ],
    );
    return_history.insert(
        "LOWER_ALPHA_DEEP_STABLE".to_string(),
        vec![
            (NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(), 0.010),
            (NaiveDate::from_ymd_opt(2026, 1, 3).unwrap(), 0.012),
            (NaiveDate::from_ymd_opt(2026, 1, 4).unwrap(), 0.009),
            (NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(), 0.011),
            (NaiveDate::from_ymd_opt(2026, 1, 6).unwrap(), 0.010),
        ],
    );
    return_history.insert(
        "DIVERSIFIER".to_string(),
        vec![
            (NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(), -0.004),
            (NaiveDate::from_ymd_opt(2026, 1, 3).unwrap(), 0.003),
            (NaiveDate::from_ymd_opt(2026, 1, 4).unwrap(), -0.002),
            (NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(), 0.004),
            (NaiveDate::from_ymd_opt(2026, 1, 6).unwrap(), -0.001),
        ],
    );
    return_history.insert(
        "DEEP_STABLE_2".to_string(),
        vec![
            (NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(), 0.006),
            (NaiveDate::from_ymd_opt(2026, 1, 3).unwrap(), 0.005),
            (NaiveDate::from_ymd_opt(2026, 1, 4).unwrap(), 0.007),
            (NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(), 0.004),
            (NaiveDate::from_ymd_opt(2026, 1, 6).unwrap(), 0.006),
        ],
    );
    return_history.insert(
        "DEEP_STABLE_3".to_string(),
        vec![
            (NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(), 0.003),
            (NaiveDate::from_ymd_opt(2026, 1, 3).unwrap(), 0.004),
            (NaiveDate::from_ymd_opt(2026, 1, 4).unwrap(), 0.002),
            (NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(), 0.004),
            (NaiveDate::from_ymd_opt(2026, 1, 6).unwrap(), 0.003),
        ],
    );
    let average_amounts = HashMap::from([
        ("HIGH_ALPHA_THIN_RISKY".to_string(), 15_000_000.0),
        ("LOWER_ALPHA_DEEP_STABLE".to_string(), 800_000_000.0),
        ("DIVERSIFIER".to_string(), 700_000_000.0),
        ("DEEP_STABLE_2".to_string(), 650_000_000.0),
        ("DEEP_STABLE_3".to_string(), 600_000_000.0),
    ]);
    let config = PortfolioConstructionConfig {
        top_n: 5,
        max_position_pct: Decimal::new(60, 2),
        max_gross_exposure: 0.90,
        portfolio_notional_cny: Some(100_000_000.0),
        max_participation_rate: Some(0.05),
        portfolio_method: PortfolioConstructionMethod::StressFillAwareRiskBudget,
        risk_budget_lookback_days: 5,
        capacity_penalty_strength: 2.0,
        capacity_risk_budget_profile:
            CapacityRiskBudgetProfile::StressParticipationBlendedAlphaHeadroomFloor70V1,
        candidate_ranking_profile: CandidateRankingProfile::NonlinearRegimeAlphaLiquidityV1,
        cash_utilization_profile: CashUtilizationProfile::StressFillGross98V1,
        max_pairwise_correlation: Some(0.95),
        correlation_lookback_days: 5,
        ..Default::default()
    };

    let weights = build_portfolio_weights(
        score_day,
        &candidates,
        &return_history,
        &average_amounts,
        &HashMap::new(),
        &config,
    );
    let gross = weights.values().copied().sum::<Decimal>();
    let high_alpha_thin = weights
        .get("HIGH_ALPHA_THIN_RISKY")
        .copied()
        .unwrap_or(Decimal::ZERO);
    let lower_alpha_deep = weights
        .get("LOWER_ALPHA_DEEP_STABLE")
        .copied()
        .unwrap_or(Decimal::ZERO);

    assert!(gross >= Decimal::new(70, 2));
    assert!(lower_alpha_deep > high_alpha_thin);
    assert!(high_alpha_thin <= Decimal::new(750, 4));
    assert!(
        weights
            .values()
            .filter(|weight| **weight > Decimal::ZERO)
            .count()
            >= 3
    );
}

#[test]
fn stress_fill_confidence_exposure_profile_scales_high_confidence_fillable_names() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let candidates = vec![
        ("HIGH_CONFIDENCE_FILLABLE".to_string(), 1.00),
        ("LOW_CONFIDENCE_FILLABLE".to_string(), 0.12),
        ("MEDIUM_CONFIDENCE_FILLABLE".to_string(), 0.10),
    ];
    let return_history = HashMap::from([
        (
            "HIGH_CONFIDENCE_FILLABLE".to_string(),
            dated_returns(&[0.010, 0.011, 0.009, 0.010, 0.011]),
        ),
        (
            "LOW_CONFIDENCE_FILLABLE".to_string(),
            dated_returns(&[0.010, 0.011, 0.009, 0.010, 0.011]),
        ),
        (
            "MEDIUM_CONFIDENCE_FILLABLE".to_string(),
            dated_returns(&[0.010, 0.011, 0.009, 0.010, 0.011]),
        ),
    ]);
    let average_amounts = HashMap::from([
        ("HIGH_CONFIDENCE_FILLABLE".to_string(), 600_000_000.0),
        ("LOW_CONFIDENCE_FILLABLE".to_string(), 600_000_000.0),
        ("MEDIUM_CONFIDENCE_FILLABLE".to_string(), 600_000_000.0),
    ]);
    let base_config = PortfolioConstructionConfig {
        top_n: 3,
        max_position_pct: Decimal::new(80, 2),
        max_gross_exposure: 1.0,
        portfolio_method: PortfolioConstructionMethod::StressFillAwareRiskBudget,
        risk_budget_lookback_days: 5,
        capacity_penalty_strength: 0.0,
        ..Default::default()
    };
    let confidence_config = PortfolioConstructionConfig {
        stress_fill_confidence_exposure_profile:
            StressFillConfidenceExposureProfile::PredictionConfidenceV1,
        ..base_config.clone()
    };

    let base_weights = build_portfolio_weights(
        score_day,
        &candidates,
        &return_history,
        &average_amounts,
        &HashMap::new(),
        &base_config,
    );
    let confidence_weights = build_portfolio_weights(
        score_day,
        &candidates,
        &return_history,
        &average_amounts,
        &HashMap::new(),
        &confidence_config,
    );

    let high_base = base_weights["HIGH_CONFIDENCE_FILLABLE"];
    let low_base = base_weights["LOW_CONFIDENCE_FILLABLE"];
    let high_confidence = confidence_weights["HIGH_CONFIDENCE_FILLABLE"];
    let low_confidence = confidence_weights["LOW_CONFIDENCE_FILLABLE"];

    assert!(
        high_confidence > high_base,
        "high confidence target should lift from {high_base} to {high_confidence}"
    );
    assert!(
        low_confidence < low_base,
        "low confidence target should shrink from {low_base} to {low_confidence}"
    );
    assert!(high_confidence > low_confidence * Decimal::new(2, 0));
}

#[test]
fn stress_fill_confidence_exposure_profile_can_align_ascending_scores() {
    let candidates = vec![
        ("LOW_SCORE_BEST".to_string(), -1.00),
        ("HIGH_SCORE_WEAK".to_string(), 1.00),
    ];

    let descending_lookup =
        stress_fill_confidence_lookup_for_direction(&candidates, ScoreDirection::Descending);
    let ascending_lookup =
        stress_fill_confidence_lookup_for_direction(&candidates, ScoreDirection::Ascending);

    assert!(
        descending_lookup["LOW_SCORE_BEST"] < descending_lookup["HIGH_SCORE_WEAK"],
        "descending confidence treats higher transformed score as stronger"
    );
    assert!(
        ascending_lookup["LOW_SCORE_BEST"] > ascending_lookup["HIGH_SCORE_WEAK"],
        "ascending confidence must avoid rewarding high raw scores when low is better"
    );
}

#[test]
fn stress_fill_confidence_exposure_profile_gates_confidence_by_capacity_headroom() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let symbols = vec![
        "HIGH_CONFIDENCE_THIN".to_string(),
        "MODERATE_CONFIDENCE_DEEP".to_string(),
        "LOW_CONFIDENCE_DEEP".to_string(),
    ];
    let candidates = vec![
        ("HIGH_CONFIDENCE_THIN".to_string(), -2.0),
        ("MODERATE_CONFIDENCE_DEEP".to_string(), -1.0),
        ("LOW_CONFIDENCE_DEEP".to_string(), 0.0),
    ];
    let return_history = symbols
        .iter()
        .map(|symbol| {
            (
                symbol.clone(),
                dated_returns(&[0.010, 0.011, 0.009, 0.010, 0.011]),
            )
        })
        .collect::<HashMap<_, _>>();
    let average_amounts = HashMap::from([
        ("HIGH_CONFIDENCE_THIN".to_string(), 10_000_000.0),
        ("MODERATE_CONFIDENCE_DEEP".to_string(), 1_000_000_000.0),
        ("LOW_CONFIDENCE_DEEP".to_string(), 1_000_000_000.0),
    ]);
    let config = PortfolioConstructionConfig {
        top_n: 3,
        max_position_pct: Decimal::new(80, 2),
        max_gross_exposure: 1.0,
        portfolio_notional_cny: Some(100_000_000.0),
        max_participation_rate: Some(0.05),
        portfolio_method: PortfolioConstructionMethod::StressFillAwareRiskBudget,
        risk_budget_lookback_days: 5,
        capacity_penalty_strength: 0.0,
        stress_fill_confidence_exposure_profile: StressFillConfidenceExposureProfile::parse(
            "prediction_confidence_ascending_capacity_headroom_v1",
        )
        .unwrap(),
        ..Default::default()
    };

    let view = ReturnHistoryMatrixView::new(&return_history, config.risk_budget_lookback_days);
    let raw_weights = build_stress_fill_aware_risk_budget_raw_weights(
        score_day,
        &symbols,
        &candidates,
        &view,
        &average_amounts,
        &config,
    );

    assert!(
        raw_weights[1] > raw_weights[0],
        "capacity-headroom gated confidence should prefer the fillable moderate-confidence name over the thin high-confidence name: {:?}",
        raw_weights
    );
}

#[test]
fn min_variance_portfolio_penalizes_variance_more_aggressively() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let candidates = vec![
        ("LOW_RISK".to_string(), 3.0),
        ("HIGH_RISK".to_string(), 2.9),
    ];
    let return_history = HashMap::from([
        (
            "LOW_RISK".to_string(),
            dated_returns(&[0.004, 0.003, 0.005, 0.004, 0.003]),
        ),
        (
            "HIGH_RISK".to_string(),
            dated_returns(&[0.08, -0.07, 0.09, -0.08, 0.07]),
        ),
    ]);
    let config = PortfolioConstructionConfig {
        top_n: 2,
        max_position_pct: Decimal::new(95, 2),
        max_gross_exposure: 1.0,
        portfolio_method: PortfolioConstructionMethod::MinVariance,
        risk_budget_lookback_days: 5,
        ..Default::default()
    };

    let weights = build_portfolio_weights(
        score_day,
        &candidates,
        &return_history,
        &HashMap::new(),
        &HashMap::new(),
        &config,
    );

    assert!(weights["LOW_RISK"] > Decimal::new(90, 2));
    assert!(weights["HIGH_RISK"] < Decimal::new(10, 2));
}

#[test]
fn industry_cap_scales_overweight_industry_exposure() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let candidates = vec![
        ("BANK_A".to_string(), 3.0),
        ("BANK_B".to_string(), 2.0),
        ("TECH_A".to_string(), 1.0),
    ];
    let industries = HashMap::from([
        ("BANK_A".to_string(), "bank".to_string()),
        ("BANK_B".to_string(), "bank".to_string()),
        ("TECH_A".to_string(), "tech".to_string()),
    ]);
    let config = PortfolioConstructionConfig {
        top_n: 3,
        max_position_pct: Decimal::ONE,
        max_gross_exposure: 1.0,
        max_industry_weight_pct: Some(0.50),
        ..Default::default()
    };

    let weights = build_portfolio_weights(
        score_day,
        &candidates,
        &HashMap::new(),
        &HashMap::new(),
        &industries,
        &config,
    );

    let bank_weight = weights["BANK_A"] + weights["BANK_B"];

    assert!(bank_weight <= Decimal::new(50, 2));
    assert!(weights["BANK_A"] < weights["TECH_A"]);
    assert!(weights["BANK_B"] < weights["TECH_A"]);
}

#[test]
fn style_risk_budget_caps_high_volatility_and_low_liquidity_exposures() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let candidates = vec![
        ("STABLE_LIQUID".to_string(), 3.0),
        ("HIGH_VOL".to_string(), 2.0),
        ("LOW_LIQUIDITY".to_string(), 1.0),
    ];
    let return_history = HashMap::from([
        (
            "STABLE_LIQUID".to_string(),
            dated_returns(&[0.004, 0.003, 0.005, 0.004, 0.003]),
        ),
        (
            "HIGH_VOL".to_string(),
            dated_returns(&[0.08, -0.07, 0.09, -0.08, 0.07]),
        ),
        (
            "LOW_LIQUIDITY".to_string(),
            dated_returns(&[0.005, 0.004, 0.004, 0.006, 0.005]),
        ),
    ]);
    let average_amounts = HashMap::from([
        ("STABLE_LIQUID".to_string(), 800_000_000.0),
        ("HIGH_VOL".to_string(), 600_000_000.0),
        ("LOW_LIQUIDITY".to_string(), 20_000_000.0),
    ]);
    let config = PortfolioConstructionConfig {
        top_n: 3,
        max_position_pct: Decimal::ONE,
        max_gross_exposure: 1.0,
        style_risk_budget_profile: StyleRiskBudgetProfile::DefensiveStyleBudgetV1,
        ..Default::default()
    };

    let weights = build_portfolio_weights(
        score_day,
        &candidates,
        &return_history,
        &average_amounts,
        &HashMap::new(),
        &config,
    );

    assert!(weights["HIGH_VOL"] <= Decimal::new(30, 2));
    assert!(weights["LOW_LIQUIDITY"] <= Decimal::new(30, 2));
    assert!(weights["STABLE_LIQUID"] > weights["HIGH_VOL"]);
    assert!(weights["STABLE_LIQUID"] > weights["LOW_LIQUIDITY"]);
}

#[test]
fn candidate_risk_filter_removes_high_volatility_candidates_before_selection() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let candidates = vec![
        ("HIGH_VOL_LEADER".to_string(), 4.0),
        ("LOW_VOL_A".to_string(), 3.0),
        ("LOW_VOL_B".to_string(), 2.0),
    ];
    let return_history = HashMap::from([
        (
            "HIGH_VOL_LEADER".to_string(),
            dated_returns(&[0.12, -0.11, 0.10, -0.09, 0.08]),
        ),
        (
            "LOW_VOL_A".to_string(),
            dated_returns(&[0.004, 0.003, 0.005, 0.004, 0.003]),
        ),
        (
            "LOW_VOL_B".to_string(),
            dated_returns(&[0.005, 0.004, 0.003, 0.004, 0.005]),
        ),
    ]);
    let config = PortfolioConstructionConfig {
        top_n: 2,
        max_position_pct: Decimal::new(60, 2),
        risk_budget_lookback_days: 5,
        candidate_risk_filter_profile: CandidateRiskFilterProfile::LowVolatilityV1,
        ..Default::default()
    };

    let weights = build_portfolio_weights(
        score_day,
        &candidates,
        &return_history,
        &HashMap::new(),
        &HashMap::new(),
        &config,
    );

    assert!(!weights.contains_key("HIGH_VOL_LEADER"));
    assert!(weights.contains_key("LOW_VOL_A"));
    assert!(weights.contains_key("LOW_VOL_B"));
}

#[test]
fn build_portfolio_weights_prefers_preloaded_return_risk_matrix() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let candidates = vec![
        ("AAA".to_string(), 3.0),
        ("BBB".to_string(), 2.0),
        ("CCC".to_string(), 1.0),
    ];
    let raw_return_history = HashMap::from([
        (
            "AAA".to_string(),
            dated_returns(&[0.12, -0.11, 0.10, -0.09, 0.08]),
        ),
        (
            "BBB".to_string(),
            dated_returns(&[0.004, 0.003, 0.005, 0.004, 0.003]),
        ),
        (
            "CCC".to_string(),
            dated_returns(&[0.005, 0.004, 0.003, 0.004, 0.005]),
        ),
    ]);
    let preloaded_return_history = HashMap::from([
        (
            "AAA".to_string(),
            dated_returns(&[0.004, 0.003, 0.005, 0.004, 0.003]),
        ),
        (
            "BBB".to_string(),
            dated_returns(&[0.005, 0.004, 0.003, 0.004, 0.005]),
        ),
        (
            "CCC".to_string(),
            dated_returns(&[0.12, -0.11, 0.10, -0.09, 0.08]),
        ),
    ]);
    let symbols = candidates
        .iter()
        .map(|(symbol, _)| symbol.clone())
        .collect::<Vec<_>>();
    let risk_matrix = build_score_date_return_risk_matrix(
        &preloaded_return_history,
        &[score_day],
        &symbols,
        5,
    );
    let preloaded_matrices = HashMap::from([(5, Arc::new(risk_matrix))]);
    let config = PortfolioConstructionConfig {
        top_n: 2,
        max_position_pct: Decimal::new(60, 2),
        risk_budget_lookback_days: 5,
        candidate_risk_filter_profile: CandidateRiskFilterProfile::LowVolatilityV1,
        ..Default::default()
    };

    let weights = build_portfolio_weights_with_return_risk_matrices(
        score_day,
        &candidates,
        &raw_return_history,
        &HashMap::new(),
        &HashMap::new(),
        &config,
        Some(&preloaded_matrices),
    );

    assert!(weights.contains_key("AAA"));
    assert!(weights.contains_key("BBB"));
    assert!(!weights.contains_key("CCC"));
}

#[test]
fn candidate_risk_filter_can_prefer_low_correlation_candidate_over_cluster() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let candidates = vec![
        ("CLUSTER_A".to_string(), 4.0),
        ("CLUSTER_B".to_string(), 3.0),
        ("CLUSTER_C".to_string(), 2.0),
        ("DIVERSIFIER".to_string(), 1.0),
    ];
    let return_history = HashMap::from([
        (
            "CLUSTER_A".to_string(),
            dated_returns(&[0.010, -0.010, 0.010, -0.010, 0.010]),
        ),
        (
            "CLUSTER_B".to_string(),
            dated_returns(&[0.010, -0.010, 0.010, -0.010, 0.010]),
        ),
        (
            "CLUSTER_C".to_string(),
            dated_returns(&[0.010, -0.010, 0.010, -0.010, 0.010]),
        ),
        (
            "DIVERSIFIER".to_string(),
            dated_returns(&[0.010, 0.010, -0.010, -0.010, 0.010]),
        ),
    ]);
    let config = PortfolioConstructionConfig {
        top_n: 1,
        max_position_pct: Decimal::ONE,
        risk_budget_lookback_days: 5,
        candidate_risk_filter_profile:
            CandidateRiskFilterProfile::LowVolatilityLowCorrelationV1,
        ..Default::default()
    };

    let weights = build_portfolio_weights(
        score_day,
        &candidates,
        &return_history,
        &HashMap::new(),
        &HashMap::new(),
        &config,
    );

    assert!(weights.contains_key("DIVERSIFIER"));
    assert_eq!(weights.len(), 1);
}

#[test]
fn soft_candidate_risk_filter_keeps_more_return_candidates() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let candidates = vec![
        ("VERY_HIGH_VOL".to_string(), 5.0),
        ("MID_HIGH_VOL".to_string(), 4.0),
        ("LOW_VOL_A".to_string(), 3.0),
        ("LOW_VOL_B".to_string(), 2.0),
        ("LOW_VOL_C".to_string(), 1.0),
    ];
    let return_history = HashMap::from([
        (
            "VERY_HIGH_VOL".to_string(),
            dated_returns(&[0.15, -0.14, 0.13, -0.12, 0.11]),
        ),
        (
            "MID_HIGH_VOL".to_string(),
            dated_returns(&[0.07, -0.06, 0.06, -0.05, 0.05]),
        ),
        (
            "LOW_VOL_A".to_string(),
            dated_returns(&[0.004, 0.003, 0.005, 0.004, 0.003]),
        ),
        (
            "LOW_VOL_B".to_string(),
            dated_returns(&[0.005, 0.004, 0.003, 0.004, 0.005]),
        ),
        (
            "LOW_VOL_C".to_string(),
            dated_returns(&[0.006, 0.005, 0.004, 0.005, 0.006]),
        ),
    ]);
    let config = PortfolioConstructionConfig {
        top_n: 4,
        max_position_pct: Decimal::new(40, 2),
        risk_budget_lookback_days: 5,
        candidate_risk_filter_profile: CandidateRiskFilterProfile::SoftLowVolatilityV1,
        ..Default::default()
    };

    let weights = build_portfolio_weights(
        score_day,
        &candidates,
        &return_history,
        &HashMap::new(),
        &HashMap::new(),
        &config,
    );

    assert!(!weights.contains_key("VERY_HIGH_VOL"));
    assert!(weights.contains_key("MID_HIGH_VOL"));
    assert!(weights.contains_key("LOW_VOL_A"));
    assert!(weights.len() >= 4);
}

#[test]
fn risk_contribution_control_scales_dominant_risk_name() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let candidates = vec![
        ("HIGH_RISK".to_string(), 3.0),
        ("LOW_RISK_A".to_string(), 2.0),
        ("LOW_RISK_B".to_string(), 1.0),
    ];
    let return_history = HashMap::from([
        (
            "HIGH_RISK".to_string(),
            dated_returns(&[0.12, -0.11, 0.10, -0.09, 0.08]),
        ),
        (
            "LOW_RISK_A".to_string(),
            dated_returns(&[0.004, 0.003, 0.005, 0.004, 0.003]),
        ),
        (
            "LOW_RISK_B".to_string(),
            dated_returns(&[0.005, 0.004, 0.003, 0.004, 0.005]),
        ),
    ]);
    let config = PortfolioConstructionConfig {
        top_n: 3,
        max_position_pct: Decimal::ONE,
        max_gross_exposure: 1.0,
        risk_budget_lookback_days: 5,
        risk_contribution_control_profile:
            RiskContributionControlProfile::SoftSingleName20PctV1,
        ..Default::default()
    };

    let weights = build_portfolio_weights(
        score_day,
        &candidates,
        &return_history,
        &HashMap::new(),
        &HashMap::new(),
        &config,
    );

    assert!(weights["HIGH_RISK"] < weights["LOW_RISK_A"]);
    assert!(weights["HIGH_RISK"] < weights["LOW_RISK_B"]);
}

#[test]
fn signal_data_cache_normalizes_symbol_order_for_portfolio_inputs() {
    let symbols = vec!["BBB".to_string(), "AAA".to_string(), "AAA".to_string()];

    assert_eq!(
        normalized_symbol_key(&symbols),
        vec!["AAA".to_string(), "BBB".to_string()]
    );
}

#[test]
fn market_feature_snapshot_key_is_window_data_and_universe_scoped() {
    let train_start = NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
    let train_end = NaiveDate::from_ymd_opt(2022, 12, 31).unwrap();
    let test_start = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
    let test_end = NaiveDate::from_ymd_opt(2023, 12, 31).unwrap();
    let symbols = vec![
        "000002.SZ".to_string(),
        "000001.SZ".to_string(),
        "000001.SZ".to_string(),
    ];

    let key = MarketFeatureSnapshotKey::new(
        "full-market-2016-v1",
        train_start,
        train_end,
        test_start,
        test_end,
        120,
        &symbols,
    );
    let reordered = MarketFeatureSnapshotKey::new(
        "full-market-2016-v1",
        train_start,
        train_end,
        test_start,
        test_end,
        120,
        &["000001.SZ".to_string(), "000002.SZ".to_string()],
    );
    let different_data = MarketFeatureSnapshotKey::new(
        "full-market-2016-v2",
        train_start,
        train_end,
        test_start,
        test_end,
        120,
        &symbols,
    );

    assert_eq!(key.universe_hash, reordered.universe_hash);
    assert_ne!(key, different_data);
    assert!(key.feature_start() < train_start);
    assert_eq!(key.feature_end(), test_end);
}

#[test]
fn return_history_cache_reuses_overlapping_symbols_incrementally() {
    let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let end = NaiveDate::from_ymd_opt(2026, 1, 31).unwrap();
    let lookback_days = 60;
    let mut cache = SignalDataCache::default();
    let cached_symbols = vec!["AAA".to_string(), "BBB".to_string()];
    let trade_date = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();

    cache.insert_return_history_symbols(
        &cached_symbols,
        start,
        end,
        lookback_days,
        HashMap::from([
            ("AAA".to_string(), vec![(trade_date, 0.01)]),
            ("BBB".to_string(), vec![(trade_date, -0.02)]),
        ]),
    );

    let requested_symbols = vec![
        "BBB".to_string(),
        "CCC".to_string(),
        "AAA".to_string(),
        "AAA".to_string(),
    ];
    let (missing, cached_history) =
        cache.cached_return_history_symbols(&requested_symbols, start, end, lookback_days);

    assert_eq!(missing, vec!["CCC".to_string()]);
    assert_eq!(cached_history["AAA"], vec![(trade_date, 0.01)]);
    assert_eq!(cached_history["BBB"], vec![(trade_date, -0.02)]);
    assert_eq!(cache.stats().return_history_hits, 2);
    assert_eq!(cache.stats().return_history_misses, 1);

    cache.insert_return_history_symbols(&missing, start, end, lookback_days, HashMap::new());
    let (missing_again, cached_again) =
        cache.cached_return_history_symbols(&requested_symbols, start, end, lookback_days);

    assert!(missing_again.is_empty());
    assert_eq!(cached_again["CCC"], Vec::<(NaiveDate, f64)>::new());
    assert_eq!(cache.stats().return_history_hits, 5);
    assert_eq!(cache.stats().return_history_misses, 1);
}

#[test]
fn signal_data_cache_snapshot_forks_prewarmed_market_features_without_copying_stats() {
    let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let end = NaiveDate::from_ymd_opt(2026, 1, 31).unwrap();
    let lookback_days = 60;
    let trade_date = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
    let symbols = vec!["AAA".to_string()];
    let mut base = SignalDataCache::default();
    base.insert_return_history_symbols(
        &symbols,
        start,
        end,
        lookback_days,
        HashMap::from([("AAA".to_string(), vec![(trade_date, 0.01)])]),
    );
    base.insert_average_amount_history_symbols(
        &symbols,
        start,
        end,
        lookback_days,
        HashMap::from([("AAA".to_string(), vec![(trade_date, 1_000.0)])]),
    );

    let snapshot = base.snapshot();
    let mut fork = SignalDataCache::from_snapshot(&snapshot);
    let (missing_returns, returns) =
        fork.cached_return_history_symbols(&symbols, start, end, lookback_days);
    let (missing_amounts, amounts) =
        fork.cached_average_amount_history_symbols(&symbols, start, end, lookback_days);

    assert!(missing_returns.is_empty());
    assert!(missing_amounts.is_empty());
    assert_eq!(returns["AAA"], vec![(trade_date, 0.01)]);
    assert_eq!(amounts["AAA"], vec![(trade_date, 1_000.0)]);
    assert_eq!(base.stats().return_history_hits, 0);
    assert_eq!(fork.stats().return_history_hits, 1);
    assert_eq!(fork.stats().average_amount_hits, 1);
    assert_eq!(fork.stats().average_amount_history_hits, 1);
}

#[test]
fn signal_cache_stats_delta_includes_persistent_market_feature_telemetry() {
    let before = SignalDataCacheStats {
        persistent_return_history_hits: 1,
        persistent_return_history_misses: 2,
        persistent_return_history_writes: 3,
        persistent_average_amount_history_hits: 4,
        persistent_average_amount_history_misses: 5,
        persistent_average_amount_history_writes: 6,
        persistent_pit_average_amount_matrix_hits: 7,
        persistent_pit_average_amount_matrix_misses: 8,
        persistent_pit_average_amount_matrix_writes: 9,
        persistent_return_risk_feature_matrix_hits: 10,
        persistent_return_risk_feature_matrix_misses: 11,
        persistent_return_risk_feature_matrix_writes: 12,
        persistent_return_risk_stats_feature_matrix_hits: 13,
        persistent_return_risk_stats_feature_matrix_misses: 14,
        persistent_return_risk_stats_feature_matrix_writes: 15,
        ..SignalDataCacheStats::default()
    };
    let after = SignalDataCacheStats {
        persistent_return_history_hits: 11,
        persistent_return_history_misses: 22,
        persistent_return_history_writes: 33,
        persistent_average_amount_history_hits: 44,
        persistent_average_amount_history_misses: 55,
        persistent_average_amount_history_writes: 66,
        persistent_pit_average_amount_matrix_hits: 77,
        persistent_pit_average_amount_matrix_misses: 88,
        persistent_pit_average_amount_matrix_writes: 99,
        persistent_return_risk_feature_matrix_hits: 110,
        persistent_return_risk_feature_matrix_misses: 121,
        persistent_return_risk_feature_matrix_writes: 132,
        persistent_return_risk_stats_feature_matrix_hits: 143,
        persistent_return_risk_stats_feature_matrix_misses: 154,
        persistent_return_risk_stats_feature_matrix_writes: 165,
        ..SignalDataCacheStats::default()
    };

    let delta = signal_cache_stats_delta(before, after);

    assert_eq!(delta.persistent_return_history_hits, 10);
    assert_eq!(delta.persistent_return_history_misses, 20);
    assert_eq!(delta.persistent_return_history_writes, 30);
    assert_eq!(delta.persistent_average_amount_history_hits, 40);
    assert_eq!(delta.persistent_average_amount_history_misses, 50);
    assert_eq!(delta.persistent_average_amount_history_writes, 60);
    assert_eq!(delta.persistent_pit_average_amount_matrix_hits, 70);
    assert_eq!(delta.persistent_pit_average_amount_matrix_misses, 80);
    assert_eq!(delta.persistent_pit_average_amount_matrix_writes, 90);
    assert_eq!(delta.persistent_return_risk_feature_matrix_hits, 100);
    assert_eq!(delta.persistent_return_risk_feature_matrix_misses, 110);
    assert_eq!(delta.persistent_return_risk_feature_matrix_writes, 120);
    assert_eq!(delta.persistent_return_risk_stats_feature_matrix_hits, 130);
    assert_eq!(
        delta.persistent_return_risk_stats_feature_matrix_misses,
        140
    );
    assert_eq!(
        delta.persistent_return_risk_stats_feature_matrix_writes,
        150
    );
}

#[test]
fn signal_data_cache_records_persistent_market_feature_telemetry_by_kind() {
    let mut cache = SignalDataCache::default();

    cache.record_persistent_market_feature_hit(PersistentMarketFeatureKind::ReturnHistory);
    cache.record_persistent_market_feature_miss(PersistentMarketFeatureKind::ReturnHistory);
    cache.record_persistent_market_feature_write(PersistentMarketFeatureKind::ReturnHistory);
    cache.record_persistent_market_feature_hit(
        PersistentMarketFeatureKind::AverageAmountHistory,
    );
    cache.record_persistent_market_feature_miss(
        PersistentMarketFeatureKind::AverageAmountHistory,
    );
    cache.record_persistent_market_feature_write(
        PersistentMarketFeatureKind::AverageAmountHistory,
    );
    cache.record_persistent_market_feature_hit(
        PersistentMarketFeatureKind::PitAverageAmountMatrix,
    );
    cache.record_persistent_market_feature_miss(
        PersistentMarketFeatureKind::PitAverageAmountMatrix,
    );
    cache.record_persistent_market_feature_write(
        PersistentMarketFeatureKind::PitAverageAmountMatrix,
    );
    cache.record_persistent_market_feature_hit(
        PersistentMarketFeatureKind::ReturnRiskFeatureMatrix,
    );
    cache.record_persistent_market_feature_miss(
        PersistentMarketFeatureKind::ReturnRiskFeatureMatrix,
    );
    cache.record_persistent_market_feature_write(
        PersistentMarketFeatureKind::ReturnRiskFeatureMatrix,
    );
    cache.record_persistent_market_feature_hit(
        PersistentMarketFeatureKind::ReturnRiskStatsFeatureMatrix,
    );
    cache.record_persistent_market_feature_miss(
        PersistentMarketFeatureKind::ReturnRiskStatsFeatureMatrix,
    );
    cache.record_persistent_market_feature_write(
        PersistentMarketFeatureKind::ReturnRiskStatsFeatureMatrix,
    );

    let stats = cache.stats();
    assert_eq!(stats.persistent_return_history_hits, 1);
    assert_eq!(stats.persistent_return_history_misses, 1);
    assert_eq!(stats.persistent_return_history_writes, 1);
    assert_eq!(stats.persistent_average_amount_history_hits, 1);
    assert_eq!(stats.persistent_average_amount_history_misses, 1);
    assert_eq!(stats.persistent_average_amount_history_writes, 1);
    assert_eq!(stats.persistent_pit_average_amount_matrix_hits, 1);
    assert_eq!(stats.persistent_pit_average_amount_matrix_misses, 1);
    assert_eq!(stats.persistent_pit_average_amount_matrix_writes, 1);
    assert_eq!(stats.persistent_return_risk_feature_matrix_hits, 1);
    assert_eq!(stats.persistent_return_risk_feature_matrix_misses, 1);
    assert_eq!(stats.persistent_return_risk_feature_matrix_writes, 1);
    assert_eq!(stats.persistent_return_risk_stats_feature_matrix_hits, 1);
    assert_eq!(stats.persistent_return_risk_stats_feature_matrix_misses, 1);
    assert_eq!(stats.persistent_return_risk_stats_feature_matrix_writes, 1);
}

#[test]
fn signal_cache_stats_delta_tracks_return_risk_matrix_payload_volume() {
    let before = SignalDataCacheStats {
        persistent_return_risk_feature_matrix_rows_loaded: 10,
        persistent_return_risk_feature_matrix_return_values_loaded: 100,
        persistent_return_risk_feature_matrix_rows_written: 20,
        persistent_return_risk_feature_matrix_return_values_written: 200,
        ..SignalDataCacheStats::default()
    };
    let after = SignalDataCacheStats {
        persistent_return_risk_feature_matrix_rows_loaded: 70,
        persistent_return_risk_feature_matrix_return_values_loaded: 850,
        persistent_return_risk_feature_matrix_rows_written: 95,
        persistent_return_risk_feature_matrix_return_values_written: 1_250,
        ..SignalDataCacheStats::default()
    };

    let delta = signal_cache_stats_delta(before, after);

    assert_eq!(delta.persistent_return_risk_feature_matrix_rows_loaded, 60);
    assert_eq!(
        delta.persistent_return_risk_feature_matrix_return_values_loaded,
        750
    );
    assert_eq!(delta.persistent_return_risk_feature_matrix_rows_written, 75);
    assert_eq!(
        delta.persistent_return_risk_feature_matrix_return_values_written,
        1_050
    );
}

#[test]
fn signal_cache_stats_delta_tracks_return_risk_stats_matrix_payload_volume() {
    let before = SignalDataCacheStats {
        persistent_return_risk_stats_feature_matrix_stats_rows_loaded: 10,
        persistent_return_risk_stats_feature_matrix_pair_rows_loaded: 100,
        persistent_return_risk_stats_feature_matrix_stats_rows_written: 20,
        persistent_return_risk_stats_feature_matrix_pair_rows_written: 200,
        ..SignalDataCacheStats::default()
    };
    let after = SignalDataCacheStats {
        persistent_return_risk_stats_feature_matrix_stats_rows_loaded: 70,
        persistent_return_risk_stats_feature_matrix_pair_rows_loaded: 850,
        persistent_return_risk_stats_feature_matrix_stats_rows_written: 95,
        persistent_return_risk_stats_feature_matrix_pair_rows_written: 1_250,
        ..SignalDataCacheStats::default()
    };

    let delta = signal_cache_stats_delta(before, after);

    assert_eq!(
        delta.persistent_return_risk_stats_feature_matrix_stats_rows_loaded,
        60
    );
    assert_eq!(
        delta.persistent_return_risk_stats_feature_matrix_pair_rows_loaded,
        750
    );
    assert_eq!(
        delta.persistent_return_risk_stats_feature_matrix_stats_rows_written,
        75
    );
    assert_eq!(
        delta.persistent_return_risk_stats_feature_matrix_pair_rows_written,
        1_050
    );
}

#[test]
fn signal_data_cache_records_return_risk_matrix_payload_volume() {
    let mut cache = SignalDataCache::default();

    cache.record_persistent_return_risk_feature_matrix_payload_loaded(3, 180);
    cache.record_persistent_return_risk_feature_matrix_payload_loaded(2, 90);
    cache.record_persistent_return_risk_feature_matrix_payload_written(5, 270);

    let stats = cache.stats();
    assert_eq!(stats.persistent_return_risk_feature_matrix_rows_loaded, 5);
    assert_eq!(
        stats.persistent_return_risk_feature_matrix_return_values_loaded,
        270
    );
    assert_eq!(stats.persistent_return_risk_feature_matrix_rows_written, 5);
    assert_eq!(
        stats.persistent_return_risk_feature_matrix_return_values_written,
        270
    );
}

#[test]
fn signal_data_cache_records_return_risk_stats_matrix_payload_volume() {
    let mut cache = SignalDataCache::default();

    cache.record_persistent_return_risk_stats_feature_matrix_payload_loaded(3, 30);
    cache.record_persistent_return_risk_stats_feature_matrix_payload_loaded(2, 20);
    cache.record_persistent_return_risk_stats_feature_matrix_payload_written(5, 50);

    let stats = cache.stats();
    assert_eq!(
        stats.persistent_return_risk_stats_feature_matrix_stats_rows_loaded,
        5
    );
    assert_eq!(
        stats.persistent_return_risk_stats_feature_matrix_pair_rows_loaded,
        50
    );
    assert_eq!(
        stats.persistent_return_risk_stats_feature_matrix_stats_rows_written,
        5
    );
    assert_eq!(
        stats.persistent_return_risk_stats_feature_matrix_pair_rows_written,
        50
    );
}

#[test]
fn return_risk_cache_economics_prefers_steady_state_stats_payload_savings() {
    let raw = SignalDataCacheStats {
        persistent_return_risk_feature_matrix_hits: 2,
        persistent_return_risk_feature_matrix_rows_loaded: 4_256,
        persistent_return_risk_feature_matrix_return_values_loaded: 500_482,
        ..SignalDataCacheStats::default()
    };
    let stats = SignalDataCacheStats {
        persistent_return_risk_stats_feature_matrix_hits: 4,
        persistent_return_risk_stats_feature_matrix_misses: 0,
        persistent_return_risk_stats_feature_matrix_writes: 0,
        persistent_return_risk_stats_feature_matrix_stats_rows_loaded: 8_512,
        persistent_return_risk_stats_feature_matrix_pair_rows_loaded: 191_580,
        ..SignalDataCacheStats::default()
    };

    let profile = compare_return_risk_cache_economics(raw, stats);

    assert_eq!(
        profile.recommendation,
        ReturnRiskCacheEconomicsRecommendation::PreferStatsMatrix
    );
    assert_eq!(profile.reason, "stats_payload_below_raw_return_values");
    assert!(profile.stats_matrix_steady_state);
    assert_eq!(profile.stats_matrix_payload_rows_loaded, 200_092);
    assert_eq!(
        profile.stats_to_raw_return_value_ratio,
        Some(200_092.0 / 500_482.0)
    );
}

#[test]
fn return_risk_cache_economics_rejects_stats_warmup_as_inconclusive() {
    let raw = SignalDataCacheStats {
        persistent_return_risk_feature_matrix_hits: 2,
        persistent_return_risk_feature_matrix_return_values_loaded: 500_482,
        ..SignalDataCacheStats::default()
    };
    let stats = SignalDataCacheStats {
        persistent_return_risk_stats_feature_matrix_hits: 2,
        persistent_return_risk_stats_feature_matrix_misses: 2,
        persistent_return_risk_stats_feature_matrix_writes: 2,
        persistent_return_risk_stats_feature_matrix_stats_rows_loaded: 4_256,
        persistent_return_risk_stats_feature_matrix_pair_rows_loaded: 95_790,
        ..SignalDataCacheStats::default()
    };

    let profile = compare_return_risk_cache_economics(raw, stats);

    assert_eq!(
        profile.recommendation,
        ReturnRiskCacheEconomicsRecommendation::Inconclusive
    );
    assert_eq!(profile.reason, "stats_matrix_warmup_not_steady_state");
    assert!(!profile.stats_matrix_steady_state);
}

#[test]
fn return_risk_cache_economics_rejects_stats_raw_matrix_fallback_as_inconclusive() {
    let raw = SignalDataCacheStats {
        persistent_return_risk_feature_matrix_hits: 4,
        persistent_return_risk_feature_matrix_return_values_loaded: 8_939_638,
        ..SignalDataCacheStats::default()
    };
    let stats = SignalDataCacheStats {
        persistent_return_risk_feature_matrix_hits: 1,
        persistent_return_risk_feature_matrix_return_values_loaded: 4_155_042,
        persistent_return_risk_stats_feature_matrix_hits: 12,
        persistent_return_risk_stats_feature_matrix_misses: 0,
        persistent_return_risk_stats_feature_matrix_writes: 0,
        persistent_return_risk_stats_feature_matrix_stats_rows_loaded: 218_348,
        persistent_return_risk_stats_feature_matrix_pair_rows_loaded: 3_031_004,
        ..SignalDataCacheStats::default()
    };

    let profile = compare_return_risk_cache_economics(raw, stats);

    assert_eq!(
        profile.recommendation,
        ReturnRiskCacheEconomicsRecommendation::Inconclusive
    );
    assert_eq!(profile.reason, "stats_matrix_raw_fallback_present");
    assert_eq!(profile.stats_matrix_raw_fallback_hits, 1);
    assert_eq!(
        profile.stats_matrix_raw_fallback_return_values_loaded,
        4_155_042
    );
    assert_eq!(profile.stats_matrix_payload_rows_loaded, 3_249_352);
    assert_eq!(profile.stats_matrix_adjusted_payload_rows_loaded, 7_404_394);
    assert_eq!(
        profile.stats_adjusted_to_raw_return_value_ratio,
        Some(7_404_394.0 / 8_939_638.0)
    );
}

#[test]
fn return_risk_cache_economics_prefers_raw_when_stats_payload_is_larger() {
    let raw = SignalDataCacheStats {
        persistent_return_risk_feature_matrix_hits: 2,
        persistent_return_risk_feature_matrix_rows_loaded: 5_000,
        persistent_return_risk_feature_matrix_return_values_loaded: 100_000,
        ..SignalDataCacheStats::default()
    };
    let stats = SignalDataCacheStats {
        persistent_return_risk_stats_feature_matrix_hits: 4,
        persistent_return_risk_stats_feature_matrix_misses: 0,
        persistent_return_risk_stats_feature_matrix_writes: 0,
        persistent_return_risk_stats_feature_matrix_stats_rows_loaded: 5_000,
        persistent_return_risk_stats_feature_matrix_pair_rows_loaded: 120_000,
        ..SignalDataCacheStats::default()
    };

    let profile = compare_return_risk_cache_economics(raw, stats);

    assert_eq!(
        profile.recommendation,
        ReturnRiskCacheEconomicsRecommendation::PreferRawMatrix
    );
    assert_eq!(
        profile.reason,
        "stats_payload_not_smaller_than_raw_return_values"
    );
    assert_eq!(profile.stats_matrix_payload_rows_loaded, 125_000);
    assert_eq!(profile.stats_to_raw_return_value_ratio, Some(1.25));
    assert_eq!(profile.stats_pair_to_stats_row_ratio, Some(24.0));
}

#[test]
fn return_history_cache_reuses_covering_symbol_window() {
    let cached_start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let cached_end = NaiveDate::from_ymd_opt(2026, 3, 31).unwrap();
    let requested_start = NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
    let requested_end = NaiveDate::from_ymd_opt(2026, 2, 28).unwrap();
    let before_request = NaiveDate::from_ymd_opt(2026, 1, 15).unwrap();
    let inside_request = NaiveDate::from_ymd_opt(2026, 2, 10).unwrap();
    let after_request = NaiveDate::from_ymd_opt(2026, 3, 10).unwrap();
    let lookback_days = 60;
    let mut cache = SignalDataCache::default();
    let cached_symbols = vec!["AAA".to_string()];

    cache.insert_return_history_symbols(
        &cached_symbols,
        cached_start,
        cached_end,
        lookback_days,
        HashMap::from([(
            "AAA".to_string(),
            vec![
                (before_request, 0.01),
                (inside_request, 0.02),
                (after_request, 0.03),
            ],
        )]),
    );

    let (missing, cached_history) = cache.cached_return_history_symbols(
        &cached_symbols,
        requested_start,
        requested_end,
        lookback_days,
    );

    assert!(missing.is_empty());
    assert_eq!(
        cached_history["AAA"],
        vec![(before_request, 0.01), (inside_request, 0.02)]
    );
    assert_eq!(cache.stats().return_history_hits, 1);
    assert_eq!(cache.stats().return_history_covering_window_hits, 1);
    assert_eq!(cache.stats().return_history_snapshot_hits, 0);
    assert_eq!(cache.stats().return_history_misses, 0);
}

#[test]
fn average_amount_history_cache_reuses_covering_symbol_window() {
    let cached_start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let cached_end = NaiveDate::from_ymd_opt(2026, 3, 31).unwrap();
    let requested_start = NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
    let requested_end = NaiveDate::from_ymd_opt(2026, 2, 28).unwrap();
    let before_request = NaiveDate::from_ymd_opt(2026, 1, 15).unwrap();
    let inside_request = NaiveDate::from_ymd_opt(2026, 2, 10).unwrap();
    let after_request = NaiveDate::from_ymd_opt(2026, 3, 10).unwrap();
    let lookback_days = 60;
    let mut cache = SignalDataCache::default();
    let cached_symbols = vec!["AAA".to_string()];

    cache.insert_average_amount_history_symbols(
        &cached_symbols,
        cached_start,
        cached_end,
        lookback_days,
        HashMap::from([(
            "AAA".to_string(),
            vec![
                (before_request, 100.0),
                (inside_request, 200.0),
                (after_request, 300.0),
            ],
        )]),
    );

    let (missing, cached_history) = cache.cached_average_amount_history_symbols(
        &cached_symbols,
        requested_start,
        requested_end,
        lookback_days,
    );

    assert!(missing.is_empty());
    assert_eq!(
        cached_history["AAA"],
        vec![(before_request, 100.0), (inside_request, 200.0)]
    );
    assert_eq!(cache.stats().average_amount_hits, 1);
    assert_eq!(cache.stats().average_amount_history_hits, 1);
    assert_eq!(cache.stats().average_amount_history_covering_window_hits, 1);
    assert_eq!(cache.stats().average_amount_history_snapshot_hits, 0);
    assert_eq!(cache.stats().average_amount_history_misses, 0);
    assert_eq!(cache.stats().average_amount_misses, 0);
}

#[test]
fn market_feature_snapshot_reuses_subset_windows_for_return_and_capacity_history() {
    let train_start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let train_end = NaiveDate::from_ymd_opt(2026, 3, 31).unwrap();
    let requested_start = NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
    let requested_end = NaiveDate::from_ymd_opt(2026, 2, 28).unwrap();
    let before_request = NaiveDate::from_ymd_opt(2026, 1, 15).unwrap();
    let inside_request = NaiveDate::from_ymd_opt(2026, 2, 10).unwrap();
    let after_request = NaiveDate::from_ymd_opt(2026, 3, 10).unwrap();
    let symbols = vec!["AAA".to_string(), "BBB".to_string()];
    let requested_symbols = vec!["BBB".to_string()];
    let return_lookback_days = 60;
    let amount_lookback_days = PIT_CAPACITY_AVERAGE_AMOUNT_LOOKBACK_DAYS;
    let mut cache = SignalDataCache::default();
    let key = MarketFeatureSnapshotKey::new(
        "full-market-2016-v1",
        train_start,
        train_end,
        train_start,
        train_end,
        return_lookback_days,
        &symbols,
    );

    cache.insert_market_feature_snapshot(
        key,
        return_lookback_days,
        amount_lookback_days,
        HashMap::from([
            (
                "AAA".to_string(),
                vec![(before_request, 0.01), (inside_request, 0.02)],
            ),
            (
                "BBB".to_string(),
                vec![
                    (before_request, -0.01),
                    (inside_request, -0.02),
                    (after_request, -0.03),
                ],
            ),
        ]),
        HashMap::from([
            (
                "AAA".to_string(),
                vec![(before_request, 100.0), (inside_request, 110.0)],
            ),
            (
                "BBB".to_string(),
                vec![
                    (before_request, 200.0),
                    (inside_request, 210.0),
                    (after_request, 220.0),
                ],
            ),
        ]),
    );

    let (missing_returns, return_history) = cache.cached_return_history_symbols(
        &requested_symbols,
        requested_start,
        requested_end,
        return_lookback_days,
    );
    let (missing_amounts, amount_history) = cache.cached_average_amount_history_symbols(
        &requested_symbols,
        requested_start,
        requested_end,
        amount_lookback_days,
    );

    assert!(missing_returns.is_empty());
    assert!(missing_amounts.is_empty());
    assert_eq!(
        return_history["BBB"],
        vec![(before_request, -0.01), (inside_request, -0.02)]
    );
    assert_eq!(
        amount_history["BBB"],
        vec![(before_request, 200.0), (inside_request, 210.0)]
    );
    assert_eq!(cache.stats().return_history_hits, 1);
    assert_eq!(cache.stats().average_amount_hits, 1);
    assert_eq!(cache.stats().average_amount_history_hits, 1);
    assert_eq!(cache.stats().return_history_covering_window_hits, 0);
    assert_eq!(cache.stats().return_history_snapshot_hits, 1);
    assert_eq!(cache.stats().average_amount_history_covering_window_hits, 0);
    assert_eq!(cache.stats().average_amount_history_snapshot_hits, 1);
    assert_eq!(cache.stats().return_history_misses, 0);
    assert_eq!(cache.stats().average_amount_history_misses, 0);
    assert_eq!(cache.stats().average_amount_misses, 0);
}

#[test]
fn market_feature_snapshot_registers_full_score_universe_from_cached_histories() {
    let train_start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let train_end = NaiveDate::from_ymd_opt(2026, 3, 31).unwrap();
    let requested_start = NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
    let requested_end = NaiveDate::from_ymd_opt(2026, 2, 28).unwrap();
    let before_request = NaiveDate::from_ymd_opt(2026, 1, 15).unwrap();
    let inside_request = NaiveDate::from_ymd_opt(2026, 2, 10).unwrap();
    let after_request = NaiveDate::from_ymd_opt(2026, 3, 10).unwrap();
    let full_universe = vec!["AAA".to_string(), "BBB".to_string(), "CCC".to_string()];
    let requested_symbols = vec!["BBB".to_string()];
    let return_lookback_days = 60;
    let amount_lookback_days = PIT_CAPACITY_AVERAGE_AMOUNT_LOOKBACK_DAYS;
    let mut cache = SignalDataCache::default();
    let key = MarketFeatureSnapshotKey::new(
        "full-market-2016-v1",
        train_start,
        train_end,
        train_start,
        train_end,
        return_lookback_days,
        &full_universe,
    );

    cache.insert_return_history_symbols(
        &full_universe,
        train_start,
        train_end,
        return_lookback_days,
        HashMap::from([
            (
                "AAA".to_string(),
                vec![(before_request, 0.01), (inside_request, 0.02)],
            ),
            (
                "BBB".to_string(),
                vec![
                    (before_request, -0.01),
                    (inside_request, -0.02),
                    (after_request, -0.03),
                ],
            ),
            (
                "CCC".to_string(),
                vec![(inside_request, 0.03), (after_request, 0.04)],
            ),
        ]),
    );
    cache.insert_average_amount_history_symbols(
        &full_universe,
        train_start,
        train_end,
        amount_lookback_days,
        HashMap::from([
            (
                "AAA".to_string(),
                vec![(before_request, 100.0), (inside_request, 110.0)],
            ),
            (
                "BBB".to_string(),
                vec![
                    (before_request, 200.0),
                    (inside_request, 210.0),
                    (after_request, 220.0),
                ],
            ),
            (
                "CCC".to_string(),
                vec![(inside_request, 300.0), (after_request, 310.0)],
            ),
        ]),
    );

    cache.insert_market_feature_snapshot_from_cached_histories(
        key,
        return_lookback_days,
        amount_lookback_days,
        &full_universe,
        train_start,
        train_end,
    );
    let snapshot = cache.snapshot();
    let mut fork = SignalDataCache {
        market_feature_snapshots: snapshot.market_feature_snapshots.clone(),
        ..Default::default()
    };

    let (missing_returns, return_history) = fork.cached_return_history_symbols(
        &requested_symbols,
        requested_start,
        requested_end,
        return_lookback_days,
    );
    let (missing_amounts, amount_history) = fork.cached_average_amount_history_symbols(
        &requested_symbols,
        requested_start,
        requested_end,
        amount_lookback_days,
    );

    assert!(missing_returns.is_empty());
    assert!(missing_amounts.is_empty());
    assert_eq!(
        return_history["BBB"],
        vec![(before_request, -0.01), (inside_request, -0.02)]
    );
    assert_eq!(
        amount_history["BBB"],
        vec![(before_request, 200.0), (inside_request, 210.0)]
    );
    assert_eq!(fork.stats().return_history_snapshot_hits, 1);
    assert_eq!(fork.stats().average_amount_history_snapshot_hits, 1);
}

#[test]
fn factor_signal_prewarm_groups_union_symbols_by_exact_window_and_lookback() {
    let train_start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let train_end = NaiveDate::from_ymd_opt(2026, 3, 31).unwrap();
    let candidates = vec![
        FactorSignalFeaturePrewarmCandidate {
            data_version_id: "full-market-2016-v1".to_string(),
            train_start,
            train_end,
            test_start: train_start,
            test_end: train_end,
            feature_start: train_start,
            feature_end: train_end,
            lookback_days: 60,
            return_risk_feature_cache_mode: ReturnRiskFeatureCacheMode::RawMatrix,
            symbols: vec!["BBB".to_string(), "AAA".to_string()],
            score_days: vec![train_start],
        },
        FactorSignalFeaturePrewarmCandidate {
            data_version_id: "full-market-2016-v1".to_string(),
            train_start,
            train_end,
            test_start: train_start,
            test_end: train_end,
            feature_start: train_start,
            feature_end: train_end,
            lookback_days: 60,
            return_risk_feature_cache_mode: ReturnRiskFeatureCacheMode::RawMatrix,
            symbols: vec!["CCC".to_string(), "AAA".to_string()],
            score_days: vec![train_end, train_start],
        },
        FactorSignalFeaturePrewarmCandidate {
            data_version_id: "full-market-2016-v1".to_string(),
            train_start,
            train_end,
            test_start: train_start,
            test_end: train_end,
            feature_start: train_start,
            feature_end: train_end,
            lookback_days: 120,
            return_risk_feature_cache_mode: ReturnRiskFeatureCacheMode::RawMatrix,
            symbols: vec!["AAA".to_string()],
            score_days: vec![train_end],
        },
    ];

    let groups = merge_factor_signal_feature_prewarm_groups(candidates);

    assert_eq!(groups.len(), 2);
    assert_eq!(groups[0].key.lookback_days, 60);
    assert_eq!(
        groups[0].symbols,
        vec!["AAA".to_string(), "BBB".to_string(), "CCC".to_string()]
    );
    assert_eq!(groups[0].score_days, vec![train_start, train_end]);
    assert_eq!(groups[1].key.lookback_days, 120);
    assert_eq!(groups[1].symbols, vec!["AAA".to_string()]);
    assert_eq!(groups[1].score_days, vec![train_end]);
}

#[test]
fn factor_signal_prewarm_groups_keep_return_risk_cache_modes_separate() {
    let train_start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let train_end = NaiveDate::from_ymd_opt(2026, 3, 31).unwrap();
    let base = FactorSignalFeaturePrewarmCandidate {
        data_version_id: "full-market-2016-v1".to_string(),
        train_start,
        train_end,
        test_start: train_start,
        test_end: train_end,
        feature_start: train_start,
        feature_end: train_end,
        lookback_days: 60,
        return_risk_feature_cache_mode: ReturnRiskFeatureCacheMode::RawMatrix,
        symbols: vec!["AAA".to_string()],
        score_days: vec![train_start],
    };
    let stats = FactorSignalFeaturePrewarmCandidate {
        return_risk_feature_cache_mode: ReturnRiskFeatureCacheMode::StatsMatrixExperimental,
        symbols: vec!["BBB".to_string()],
        ..base.clone()
    };

    let groups = merge_factor_signal_feature_prewarm_groups(vec![base, stats]);

    assert_eq!(groups.len(), 2);
    assert_eq!(
        groups[0].key.return_risk_feature_cache_mode,
        ReturnRiskFeatureCacheMode::RawMatrix
    );
    assert_eq!(groups[0].symbols, vec!["AAA".to_string()]);
    assert_eq!(
        groups[1].key.return_risk_feature_cache_mode,
        ReturnRiskFeatureCacheMode::StatsMatrixExperimental
    );
    assert_eq!(groups[1].symbols, vec!["BBB".to_string()]);
}

#[test]
fn persistent_market_feature_cache_key_is_order_insensitive_and_scope_specific() {
    let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let end = NaiveDate::from_ymd_opt(2026, 3, 31).unwrap();
    let symbols = vec!["BBB".to_string(), "AAA".to_string()];
    let reordered_symbols = vec!["AAA".to_string(), "BBB".to_string()];

    let first = PersistentMarketFeatureCacheKey::new(
        PersistentMarketFeatureKind::ReturnHistory,
        "full-market-2016-v1",
        start,
        end,
        60,
        &symbols,
    );
    let reordered = PersistentMarketFeatureCacheKey::new(
        PersistentMarketFeatureKind::ReturnHistory,
        "full-market-2016-v1",
        start,
        end,
        60,
        &reordered_symbols,
    );
    let different_kind = PersistentMarketFeatureCacheKey::new(
        PersistentMarketFeatureKind::AverageAmountHistory,
        "full-market-2016-v1",
        start,
        end,
        60,
        &symbols,
    );
    let different_lookback = PersistentMarketFeatureCacheKey::new(
        PersistentMarketFeatureKind::ReturnHistory,
        "full-market-2016-v1",
        start,
        end,
        120,
        &symbols,
    );

    assert_eq!(first.cache_key, reordered.cache_key);
    assert_eq!(first.universe_hash, reordered.universe_hash);
    assert_ne!(first.cache_key, different_kind.cache_key);
    assert_ne!(first.cache_key, different_lookback.cache_key);
    assert_eq!(first.symbol_count, 2);
    assert_eq!(first.feature_kind.as_str(), "return_history");
}

#[test]
fn persistent_market_feature_manifest_requires_ready_complete_symbol_scope() {
    let requested = vec!["BBB".to_string(), "AAA".to_string(), "AAA".to_string()];
    let cached_reordered = vec!["AAA".to_string(), "BBB".to_string()];
    let cached_subset = vec!["AAA".to_string()];

    assert!(persistent_market_feature_manifest_is_usable(
        "ready",
        2,
        &cached_reordered,
        &requested
    ));
    assert!(!persistent_market_feature_manifest_is_usable(
        "building",
        2,
        &cached_reordered,
        &requested
    ));
    assert!(!persistent_market_feature_manifest_is_usable(
        "ready",
        1,
        &cached_reordered,
        &requested
    ));
    assert!(!persistent_market_feature_manifest_is_usable(
        "ready",
        2,
        &cached_subset,
        &requested
    ));
}

#[test]
fn persistent_market_feature_grouped_rows_reconstruct_history_with_empty_symbols() {
    let d1 = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
    let d2 = NaiveDate::from_ymd_opt(2026, 1, 6).unwrap();
    let requested = vec!["BBB".to_string(), "AAA".to_string(), "EMPTY".to_string()];

    let history = persistent_market_feature_grouped_rows_to_history(
        &requested,
        3,
        vec![
            ("AAA".to_string(), vec![d1, d2], vec![0.01, -0.02]),
            ("BBB".to_string(), vec![d1], vec![0.03]),
        ],
    )
    .expect("grouped rows should reconstruct");

    assert_eq!(history["AAA"], vec![(d1, 0.01), (d2, -0.02)]);
    assert_eq!(history["BBB"], vec![(d1, 0.03)]);
    assert!(history["EMPTY"].is_empty());
}

#[test]
fn persistent_market_feature_grouped_rows_reject_corrupt_payloads() {
    let d1 = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
    let requested = vec!["AAA".to_string()];

    assert!(persistent_market_feature_grouped_rows_to_history(
        &requested,
        1,
        vec![("AAA".to_string(), vec![d1], vec![])],
    )
    .is_none());
    assert!(persistent_market_feature_grouped_rows_to_history(
        &requested,
        1,
        vec![("UNKNOWN".to_string(), vec![d1], vec![0.01])],
    )
    .is_none());
    assert!(persistent_market_feature_grouped_rows_to_history(
        &requested,
        1,
        vec![("AAA".to_string(), vec![d1], vec![f64::NAN])],
    )
    .is_none());
    assert!(persistent_market_feature_grouped_rows_to_history(
        &requested,
        2,
        vec![("AAA".to_string(), vec![d1], vec![0.01])],
    )
    .is_none());
}

#[test]
fn pit_average_amount_matrix_cache_key_is_universe_and_score_date_scoped() {
    let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let end = NaiveDate::from_ymd_opt(2026, 3, 31).unwrap();
    let dates = vec![
        NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(),
        NaiveDate::from_ymd_opt(2026, 2, 6).unwrap(),
    ];
    let reordered_dates = vec![dates[1], dates[0]];
    let different_dates = vec![
        NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(),
        NaiveDate::from_ymd_opt(2026, 3, 6).unwrap(),
    ];
    let symbols = vec!["BBB".to_string(), "AAA".to_string()];
    let reordered_symbols = vec!["AAA".to_string(), "BBB".to_string()];

    let first = PersistentMarketFeatureCacheKey::new_for_dates(
        PersistentMarketFeatureKind::PitAverageAmountMatrix,
        "full-market-2016-v1",
        start,
        end,
        60,
        &symbols,
        &dates,
    );
    let reordered = PersistentMarketFeatureCacheKey::new_for_dates(
        PersistentMarketFeatureKind::PitAverageAmountMatrix,
        "full-market-2016-v1",
        start,
        end,
        60,
        &reordered_symbols,
        &reordered_dates,
    );
    let different = PersistentMarketFeatureCacheKey::new_for_dates(
        PersistentMarketFeatureKind::PitAverageAmountMatrix,
        "full-market-2016-v1",
        start,
        end,
        60,
        &symbols,
        &different_dates,
    );

    assert_eq!(first.cache_key, reordered.cache_key);
    assert_ne!(first.cache_key, different.cache_key);
    assert!(first.cache_key.contains("pit_average_amount_matrix"));
    assert!(first.cache_key.contains("dates:"));
}

#[test]
fn return_risk_feature_matrix_cache_key_is_universe_and_score_date_scoped() {
    let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let end = NaiveDate::from_ymd_opt(2026, 3, 31).unwrap();
    let dates = vec![
        NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(),
        NaiveDate::from_ymd_opt(2026, 2, 6).unwrap(),
    ];
    let reordered_dates = vec![dates[1], dates[0]];
    let different_dates = vec![
        NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(),
        NaiveDate::from_ymd_opt(2026, 3, 6).unwrap(),
    ];
    let symbols = vec!["BBB".to_string(), "AAA".to_string()];
    let reordered_symbols = vec!["AAA".to_string(), "BBB".to_string()];

    let first = PersistentMarketFeatureCacheKey::new_for_dates(
        PersistentMarketFeatureKind::ReturnRiskFeatureMatrix,
        "full-market-2016-v1",
        start,
        end,
        60,
        &symbols,
        &dates,
    );
    let reordered = PersistentMarketFeatureCacheKey::new_for_dates(
        PersistentMarketFeatureKind::ReturnRiskFeatureMatrix,
        "full-market-2016-v1",
        start,
        end,
        60,
        &reordered_symbols,
        &reordered_dates,
    );
    let different = PersistentMarketFeatureCacheKey::new_for_dates(
        PersistentMarketFeatureKind::ReturnRiskFeatureMatrix,
        "full-market-2016-v1",
        start,
        end,
        60,
        &symbols,
        &different_dates,
    );

    assert_eq!(first.cache_key, reordered.cache_key);
    assert_ne!(first.cache_key, different.cache_key);
    assert!(first.cache_key.contains("return_risk_feature_matrix"));
    assert!(first.cache_key.contains("dates:"));
    assert_eq!(
        PersistentMarketFeatureKind::ReturnRiskFeatureMatrix.as_str(),
        "return_risk_feature_matrix"
    );
}

#[test]
fn return_risk_stats_feature_matrix_cache_key_is_universe_and_score_date_scoped() {
    let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let end = NaiveDate::from_ymd_opt(2026, 3, 31).unwrap();
    let dates = vec![
        NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(),
        NaiveDate::from_ymd_opt(2026, 2, 6).unwrap(),
    ];
    let reordered_dates = vec![dates[1], dates[0]];
    let different_dates = vec![
        NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(),
        NaiveDate::from_ymd_opt(2026, 3, 6).unwrap(),
    ];
    let symbols = vec!["BBB".to_string(), "AAA".to_string()];
    let reordered_symbols = vec!["AAA".to_string(), "BBB".to_string()];

    let first = PersistentMarketFeatureCacheKey::new_for_dates(
        PersistentMarketFeatureKind::ReturnRiskStatsFeatureMatrix,
        "full-market-2016-v1",
        start,
        end,
        60,
        &symbols,
        &dates,
    );
    let reordered = PersistentMarketFeatureCacheKey::new_for_dates(
        PersistentMarketFeatureKind::ReturnRiskStatsFeatureMatrix,
        "full-market-2016-v1",
        start,
        end,
        60,
        &reordered_symbols,
        &reordered_dates,
    );
    let different = PersistentMarketFeatureCacheKey::new_for_dates(
        PersistentMarketFeatureKind::ReturnRiskStatsFeatureMatrix,
        "full-market-2016-v1",
        start,
        end,
        60,
        &symbols,
        &different_dates,
    );

    assert_eq!(first.cache_key, reordered.cache_key);
    assert_ne!(first.cache_key, different.cache_key);
    assert!(first.cache_key.contains("return_risk_stats_feature_matrix"));
    assert!(first.cache_key.contains("dates:"));
    assert_eq!(
        PersistentMarketFeatureKind::ReturnRiskStatsFeatureMatrix.as_str(),
        "return_risk_stats_feature_matrix"
    );
}

#[test]
fn return_risk_stats_feature_matrix_cache_key_includes_pairwise_scope() {
    let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let end = NaiveDate::from_ymd_opt(2026, 3, 31).unwrap();
    let day1 = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
    let day2 = NaiveDate::from_ymd_opt(2026, 2, 6).unwrap();
    let symbols = vec![
        "AAA".to_string(),
        "BBB".to_string(),
        "CCC".to_string(),
        "DDD".to_string(),
    ];
    let scope_ab = ReturnRiskStatsPairwiseScopePlan {
        score_days: vec![day1, day2],
        symbols: symbols.clone(),
        pair_keys: vec![pairwise_correlation_key(day1, "AAA", "BBB")],
    };
    let reordered_scope_ab = ReturnRiskStatsPairwiseScopePlan {
        score_days: vec![day2, day1],
        symbols: symbols.clone(),
        pair_keys: vec![pairwise_correlation_key(day1, "BBB", "AAA")],
    };
    let scope_cd = ReturnRiskStatsPairwiseScopePlan {
        score_days: vec![day1, day2],
        symbols: symbols.clone(),
        pair_keys: vec![pairwise_correlation_key(day2, "CCC", "DDD")],
    };

    let first = persistent_return_risk_stats_feature_matrix_cache_key(
        "full-market-2016-v1",
        start,
        end,
        60,
        &symbols,
        &scope_ab,
    );
    let reordered = persistent_return_risk_stats_feature_matrix_cache_key(
        "full-market-2016-v1",
        start,
        end,
        60,
        &symbols,
        &reordered_scope_ab,
    );
    let different_pair_scope = persistent_return_risk_stats_feature_matrix_cache_key(
        "full-market-2016-v1",
        start,
        end,
        60,
        &symbols,
        &scope_cd,
    );

    assert_eq!(first.cache_key, reordered.cache_key);
    assert_ne!(first.cache_key, different_pair_scope.cache_key);
    assert!(first.cache_key.contains("pairs:"));
}

#[test]
fn persistent_return_risk_stats_feature_matrix_rows_reconstruct_guarded_payload() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let symbols = vec!["BBB".to_string(), "AAA".to_string()];
    let stats_rows = vec![
        (
            score_day,
            "BBB".to_string(),
            3_i64,
            Some(0.01),
            Some(0.03),
            Some(0.004),
            Some(0.0002),
        ),
        (
            score_day,
            "AAA".to_string(),
            3_i64,
            Some(0.06),
            Some(0.02),
            Some(0.02),
            Some(0.0001),
        ),
    ];
    let pair_rows = vec![(score_day, "AAA".to_string(), "BBB".to_string(), 0.25)];

    let restored = persistent_return_risk_stats_feature_matrix_rows_to_matrix(
        &[score_day],
        &symbols,
        2,
        1,
        stats_rows.clone(),
        pair_rows.clone(),
    )
    .expect("stats payload should restore");

    assert_eq!(restored.return_count(score_day, "AAA"), 3);
    assert_eq!(restored.total_return(score_day, "AAA"), Some(0.06));
    assert_eq!(restored.sample_volatility(score_day, "BBB"), Some(0.03));
    assert_eq!(
        restored.pearson_correlation(score_day, "BBB", "AAA"),
        Some(0.25)
    );
    assert_eq!(
        restored.covariance_concentration_penalty(score_day, "AAA", &symbols),
        1.25
    );

    assert!(persistent_return_risk_stats_feature_matrix_rows_to_matrix(
        &[score_day],
        &symbols,
        1,
        1,
        stats_rows.clone(),
        pair_rows.clone(),
    )
    .is_none());
    assert!(persistent_return_risk_stats_feature_matrix_rows_to_matrix(
        &[score_day],
        &symbols,
        2,
        0,
        stats_rows.clone(),
        pair_rows.clone(),
    )
    .is_none());

    let mut negative_count = stats_rows;
    negative_count[0].2 = -1;
    assert!(persistent_return_risk_stats_feature_matrix_rows_to_matrix(
        &[score_day],
        &symbols,
        2,
        1,
        negative_count,
        pair_rows,
    )
    .is_none());
}

#[test]
fn return_risk_feature_matrix_rows_round_trip_without_future_rows() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let next_score_day = NaiveDate::from_ymd_opt(2026, 1, 9).unwrap();
    let future_day = NaiveDate::from_ymd_opt(2026, 1, 10).unwrap();
    let score_days = vec![next_score_day, score_day];
    let symbols = vec!["BBB".to_string(), "AAA".to_string()];
    let return_history = HashMap::from([
        (
            "AAA".to_string(),
            vec![
                (NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(), 0.01),
                (NaiveDate::from_ymd_opt(2026, 1, 3).unwrap(), -0.02),
                (NaiveDate::from_ymd_opt(2026, 1, 4).unwrap(), 0.03),
                (NaiveDate::from_ymd_opt(2026, 1, 8).unwrap(), 0.04),
                (future_day, 0.90),
            ],
        ),
        (
            "BBB".to_string(),
            vec![
                (NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(), -0.01),
                (NaiveDate::from_ymd_opt(2026, 1, 3).unwrap(), 0.02),
                (NaiveDate::from_ymd_opt(2026, 1, 4).unwrap(), -0.03),
                (NaiveDate::from_ymd_opt(2026, 1, 8).unwrap(), -0.04),
                (future_day, 0.80),
            ],
        ),
    ]);
    let matrix = build_score_date_return_risk_matrix(&return_history, &score_days, &symbols, 3);

    let rows = return_risk_feature_matrix_to_rows(&matrix);
    let restored =
        return_risk_feature_matrix_from_rows(&score_days, &symbols, rows).expect("restored");

    assert_eq!(restored.returns(score_day, "AAA"), &[0.01, -0.02, 0.03]);
    assert_eq!(
        restored.returns(next_score_day, "AAA"),
        &[-0.02, 0.03, 0.04]
    );
    for score_day in normalized_dates(&score_days) {
        for symbol in normalized_symbol_key(&symbols) {
            assert_eq!(
                restored.returns(score_day, &symbol),
                matrix.returns(score_day, &symbol)
            );
            assert_eq!(
                restored.total_return(score_day, &symbol),
                matrix.total_return(score_day, &symbol)
            );
            assert_eq!(
                restored.sample_volatility(score_day, &symbol),
                matrix.sample_volatility(score_day, &symbol)
            );
        }
    }
    assert!(return_risk_feature_matrix_from_rows(
        &score_days,
        &symbols,
        vec![ReturnRiskFeatureMatrixRow {
            score_day,
            symbol: "AAA".to_string(),
            returns: vec![f64::NAN],
        }],
    )
    .is_none());
}

#[test]
fn persistent_return_risk_feature_matrix_rows_require_complete_scope_and_row_count() {
    let score_day = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();
    let next_score_day = NaiveDate::from_ymd_opt(2026, 1, 9).unwrap();
    let score_days = vec![score_day, next_score_day];
    let symbols = vec!["AAA".to_string(), "BBB".to_string()];
    let rows = vec![
        (score_day, "AAA".to_string(), vec![0.01]),
        (score_day, "BBB".to_string(), vec![0.02]),
        (next_score_day, "AAA".to_string(), vec![0.03]),
        (next_score_day, "BBB".to_string(), vec![0.04]),
    ];

    let matrix = persistent_return_risk_feature_matrix_rows_to_matrix(
        &score_days,
        &symbols,
        4,
        rows.clone(),
    )
    .expect("complete matrix");

    assert_eq!(matrix.returns(score_day, "AAA"), &[0.01]);
    assert!(persistent_return_risk_feature_matrix_rows_to_matrix(
        &score_days,
        &symbols,
        3,
        rows.clone(),
    )
    .is_none());
    assert!(persistent_return_risk_feature_matrix_rows_to_matrix(
        &score_days,
        &symbols,
        4,
        rows[..3].to_vec(),
    )
    .is_none());
    let mut duplicate_rows = rows.clone();
    duplicate_rows.push((score_day, "AAA".to_string(), vec![0.05]));
    assert!(persistent_return_risk_feature_matrix_rows_to_matrix(
        &score_days,
        &symbols,
        5,
        duplicate_rows,
    )
    .is_none());
}

#[test]
fn pit_average_amount_matrix_round_trips_through_symbol_history_shape() {
    let d1 = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
    let d2 = NaiveDate::from_ymd_opt(2026, 2, 6).unwrap();
    let d3 = NaiveDate::from_ymd_opt(2026, 3, 6).unwrap();
    let matrix = HashMap::from([
        (
            d1,
            HashMap::from([("AAA".to_string(), 10.0), ("BBB".to_string(), 20.0)]),
        ),
        (d2, HashMap::from([("AAA".to_string(), 30.0)])),
    ]);

    let history = pit_average_amount_matrix_to_symbol_history(&matrix);
    let restored = average_amount_symbol_history_to_matrix(&history, &[d2, d1, d3]);

    assert_eq!(restored[&d1]["AAA"], 10.0);
    assert_eq!(restored[&d1]["BBB"], 20.0);
    assert_eq!(restored[&d2]["AAA"], 30.0);
    assert!(restored[&d2].get("BBB").is_none());
    assert!(restored[&d3].is_empty());
}

#[test]
fn pit_average_amount_matrix_cache_reuses_exact_score_date_scope() {
    let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let end = NaiveDate::from_ymd_opt(2026, 3, 31).unwrap();
    let d1 = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
    let d2 = NaiveDate::from_ymd_opt(2026, 2, 6).unwrap();
    let d3 = NaiveDate::from_ymd_opt(2026, 3, 6).unwrap();
    let symbols = vec!["BBB".to_string(), "AAA".to_string()];
    let key = PitAverageAmountMatrixCacheKey::new(start, end, 60, &symbols, &[d1, d2]);
    let reordered = PitAverageAmountMatrixCacheKey::new(
        start,
        end,
        60,
        &["AAA".to_string(), "BBB".to_string()],
        &[d2, d1],
    );
    let uncovered_date = PitAverageAmountMatrixCacheKey::new(start, end, 60, &symbols, &[d3]);
    let matrix = HashMap::from([(d1, HashMap::from([("AAA".to_string(), 10.0)]))]);
    let mut cache = SignalDataCache::default();

    cache.insert_pit_average_amount_matrix(key, matrix);

    assert_eq!(
        cache
            .cached_pit_average_amount_matrix(&reordered)
            .unwrap()
            .get(&d1)
            .unwrap()["AAA"],
        10.0
    );
    assert!(cache
        .cached_pit_average_amount_matrix(&uncovered_date)
        .is_none());
}

#[test]
fn pit_average_amount_matrix_cache_reuses_covering_symbol_and_date_scope() {
    let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let end = NaiveDate::from_ymd_opt(2026, 3, 31).unwrap();
    let d1 = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
    let d2 = NaiveDate::from_ymd_opt(2026, 2, 6).unwrap();
    let covering_symbols = vec!["AAA".to_string(), "BBB".to_string(), "CCC".to_string()];
    let covering_key =
        PitAverageAmountMatrixCacheKey::new(start, end, 60, &covering_symbols, &[d1, d2]);
    let subset_key = PitAverageAmountMatrixCacheKey::new(
        start,
        end,
        60,
        &["CCC".to_string(), "AAA".to_string()],
        &[d2],
    );
    let matrix = HashMap::from([
        (
            d1,
            HashMap::from([
                ("AAA".to_string(), 10.0),
                ("BBB".to_string(), 20.0),
                ("CCC".to_string(), 30.0),
            ]),
        ),
        (
            d2,
            HashMap::from([
                ("AAA".to_string(), 40.0),
                ("BBB".to_string(), 50.0),
                ("CCC".to_string(), 60.0),
            ]),
        ),
    ]);
    let mut cache = SignalDataCache::default();

    cache.insert_pit_average_amount_matrix(covering_key, matrix);

    let subset = cache
        .cached_pit_average_amount_matrix(&subset_key)
        .expect("covering matrix should satisfy subset request");
    assert_eq!(subset.len(), 1);
    assert_eq!(subset[&d2]["AAA"], 40.0);
    assert_eq!(subset[&d2]["CCC"], 60.0);
    assert!(subset[&d2].get("BBB").is_none());
    assert!(subset.get(&d1).is_none());
}

#[test]
fn score_days_for_signal_dates_uses_actual_signal_days_and_entry_delay() {
    let trading_days = vec![
        NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(),
        NaiveDate::from_ymd_opt(2026, 1, 6).unwrap(),
        NaiveDate::from_ymd_opt(2026, 1, 7).unwrap(),
        NaiveDate::from_ymd_opt(2026, 1, 8).unwrap(),
        NaiveDate::from_ymd_opt(2026, 1, 9).unwrap(),
    ];
    let signal_dates = vec![trading_days[4], trading_days[2], trading_days[4]];

    let score_days = score_days_for_signal_dates(&trading_days, &signal_dates, 1);

    assert_eq!(score_days, vec![trading_days[0], trading_days[2]]);
}

#[test]
fn return_risk_feature_matrix_cache_reuses_exact_score_date_scope() {
    let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let end = NaiveDate::from_ymd_opt(2026, 3, 31).unwrap();
    let d1 = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
    let d2 = NaiveDate::from_ymd_opt(2026, 2, 6).unwrap();
    let d3 = NaiveDate::from_ymd_opt(2026, 3, 6).unwrap();
    let symbols = vec!["BBB".to_string(), "AAA".to_string()];
    let key = ReturnRiskFeatureMatrixCacheKey::new(start, end, 60, &symbols, &[d1, d2]);
    let reordered = ReturnRiskFeatureMatrixCacheKey::new(
        start,
        end,
        60,
        &["AAA".to_string(), "BBB".to_string()],
        &[d2, d1],
    );
    let different_dates = ReturnRiskFeatureMatrixCacheKey::new(start, end, 60, &symbols, &[d3]);
    let matrix = ScoreDateReturnRiskMatrix {
        returns_by_score_symbol: HashMap::from([((d1, "AAA".to_string()), vec![0.01, 0.02])]),
    };
    let mut cache = SignalDataCache::default();

    cache.insert_return_risk_feature_matrix(key, matrix);

    assert_eq!(
        cache
            .cached_return_risk_feature_matrix(&reordered)
            .unwrap()
            .returns(d1, "AAA"),
        &[0.01, 0.02]
    );
    assert!(cache
        .cached_return_risk_feature_matrix(&different_dates)
        .is_none());
}

#[test]
fn return_risk_feature_matrix_cache_reuses_covering_symbol_and_score_date_scope() {
    let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let end = NaiveDate::from_ymd_opt(2026, 3, 31).unwrap();
    let d1 = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
    let d2 = NaiveDate::from_ymd_opt(2026, 2, 6).unwrap();
    let covering_key = ReturnRiskFeatureMatrixCacheKey::new(
        start,
        end,
        60,
        &["AAA".to_string(), "BBB".to_string(), "CCC".to_string()],
        &[d1, d2],
    );
    let subset_key =
        ReturnRiskFeatureMatrixCacheKey::new(start, end, 60, &["CCC".to_string()], &[d2]);
    let matrix = ScoreDateReturnRiskMatrix {
        returns_by_score_symbol: HashMap::from([
            ((d1, "AAA".to_string()), vec![0.01]),
            ((d2, "AAA".to_string()), vec![0.02]),
            ((d2, "CCC".to_string()), vec![0.03, -0.01]),
        ]),
    };
    let mut cache = SignalDataCache::default();

    cache.insert_return_risk_feature_matrix(covering_key, matrix);

    let subset = cache
        .cached_return_risk_feature_matrix(&subset_key)
        .expect("covering return/risk matrix should satisfy subset request");
    assert_eq!(subset.row_count(), 1);
    assert_eq!(subset.returns(d2, "CCC"), &[0.03, -0.01]);
    assert!(subset.returns(d1, "AAA").is_empty());
    assert!(subset.returns(d2, "AAA").is_empty());
}

#[test]
fn return_risk_matrices_cover_required_lookbacks_for_lazy_return_history_loading() {
    let matrix = Arc::new(ScoreDateReturnRiskMatrix::default());
    let config = PortfolioConstructionConfig {
        candidate_ranking_profile: CandidateRankingProfile::RelativeStrengthAlphaLiquidityV1,
        risk_budget_lookback_days: 60,
        max_pairwise_correlation: Some(0.65),
        correlation_lookback_days: 120,
        ..PortfolioConstructionConfig::default()
    };

    assert!(!return_risk_matrices_cover_required_lookbacks(
        &HashMap::from([(60, Arc::clone(&matrix))]),
        &config,
    ));
    assert!(return_risk_matrices_cover_required_lookbacks(
        &HashMap::from([(60, Arc::clone(&matrix)), (120, matrix)]),
        &config,
    ));
    assert!(return_risk_matrices_cover_required_lookbacks(
        &HashMap::new(),
        &PortfolioConstructionConfig::default(),
    ));
}

#[test]
fn average_amount_cache_reuses_overlapping_symbols_incrementally() {
    let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let end = NaiveDate::from_ymd_opt(2026, 1, 31).unwrap();
    let mut cache = SignalDataCache::default();
    let cached_symbols = vec!["AAA".to_string(), "BBB".to_string()];

    cache.insert_average_amount_symbols(
        &cached_symbols,
        start,
        end,
        HashMap::from([("AAA".to_string(), 5_000.0), ("BBB".to_string(), 3_000.0)]),
    );

    let requested_symbols = vec![
        "BBB".to_string(),
        "CCC".to_string(),
        "AAA".to_string(),
        "AAA".to_string(),
    ];
    let (missing, cached_amounts) =
        cache.cached_average_amount_symbols(&requested_symbols, start, end);

    assert_eq!(missing, vec!["CCC".to_string()]);
    assert_eq!(cached_amounts["AAA"], 5_000.0);
    assert_eq!(cached_amounts["BBB"], 3_000.0);
    assert_eq!(cache.stats().average_amount_hits, 2);
    assert_eq!(cache.stats().average_amount_symbol_hits, 2);
    assert_eq!(cache.stats().average_amount_history_hits, 0);
    assert_eq!(cache.stats().average_amount_misses, 1);
    assert_eq!(cache.stats().average_amount_symbol_misses, 1);
    assert_eq!(cache.stats().average_amount_history_misses, 0);

    cache.insert_average_amount_symbols(&missing, start, end, HashMap::new());
    let (missing_again, cached_again) =
        cache.cached_average_amount_symbols(&requested_symbols, start, end);

    assert!(missing_again.is_empty());
    assert!(!cached_again.contains_key("CCC"));
    assert_eq!(cache.stats().average_amount_hits, 5);
    assert_eq!(cache.stats().average_amount_symbol_hits, 5);
    assert_eq!(cache.stats().average_amount_misses, 1);
    assert_eq!(cache.stats().average_amount_symbol_misses, 1);
}

#[test]
fn average_amount_cache_does_not_reuse_covering_aggregate_window() {
    let cached_start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let cached_end = NaiveDate::from_ymd_opt(2026, 3, 31).unwrap();
    let requested_start = NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
    let requested_end = NaiveDate::from_ymd_opt(2026, 2, 28).unwrap();
    let mut cache = SignalDataCache::default();
    let cached_symbols = vec!["AAA".to_string()];

    cache.insert_average_amount_symbols(
        &cached_symbols,
        cached_start,
        cached_end,
        HashMap::from([("AAA".to_string(), 5_000.0)]),
    );

    let (missing, cached_amounts) =
        cache.cached_average_amount_symbols(&cached_symbols, requested_start, requested_end);

    assert_eq!(missing, vec!["AAA".to_string()]);
    assert!(cached_amounts.is_empty());
    assert_eq!(cache.stats().average_amount_hits, 0);
    assert_eq!(cache.stats().average_amount_misses, 1);
}

#[test]
fn combo_score_cache_key_is_direction_aware_only_when_candidate_pool_is_pruned() {
    let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let end = NaiveDate::from_ymd_opt(2026, 1, 31).unwrap();

    let full_desc = SignalDataCacheKey::combo_scores(
        "phase7_price_volume_expanded_v1",
        "1.0.0",
        start,
        end,
        ScoreDirection::Descending,
        None,
        TradableUniverseProfile::All,
    );
    let full_asc = SignalDataCacheKey::combo_scores(
        "phase7_price_volume_expanded_v1",
        "1.0.0",
        start,
        end,
        ScoreDirection::Ascending,
        None,
        TradableUniverseProfile::All,
    );
    let pruned_desc = SignalDataCacheKey::combo_scores(
        "phase7_price_volume_expanded_v1",
        "1.0.0",
        start,
        end,
        ScoreDirection::Descending,
        Some(200),
        TradableUniverseProfile::All,
    );
    let pruned_asc = SignalDataCacheKey::combo_scores(
        "phase7_price_volume_expanded_v1",
        "1.0.0",
        start,
        end,
        ScoreDirection::Ascending,
        Some(200),
        TradableUniverseProfile::All,
    );

    assert_eq!(full_desc, full_asc);
    assert_ne!(pruned_desc, pruned_asc);
}

#[test]
fn combo_score_pruned_query_uses_directional_daily_window_rank() {
    let descending = combo_score_load_sql(
        ScoreDirection::Descending,
        Some(200),
        TradableUniverseProfile::All,
    );
    let ascending = combo_score_load_sql(
        ScoreDirection::Ascending,
        Some(200),
        TradableUniverseProfile::All,
    );

    assert!(descending.contains("ROW_NUMBER() OVER"));
    assert!(descending.contains("PARTITION BY mfv.trade_date"));
    assert!(descending.contains("COALESCE(mfv.raw_score, 0.0) DESC"));
    assert!(descending.contains("score_rank <= $5"));
    assert!(ascending.contains("COALESCE(mfv.raw_score, 0.0) ASC"));
}

#[test]
fn combo_score_query_filters_tradable_universe_before_daily_ranking() {
    let sql = combo_score_load_sql(
        ScoreDirection::Descending,
        Some(200),
        TradableUniverseProfile::ListedNonSt,
    );

    assert!(sql.contains("JOIN market_stock ms ON ms.symbol = mfv.symbol"));
    assert!(sql.contains("ms.list_status = 'L'"));
    assert!(sql.contains("COALESCE(ms.is_st, false) = false"));
    assert!(sql.contains("ROW_NUMBER() OVER"));
    assert!(sql.contains("score_rank <= $5"));
}

#[test]
fn combo_score_query_filters_main_board_universe_to_main_board_stocks() {
    let sql = combo_score_load_sql(
        ScoreDirection::Descending,
        Some(200),
        TradableUniverseProfile::MainBoardNonSt,
    );

    assert!(sql.contains("JOIN market_stock ms ON ms.symbol = mfv.symbol"));
    assert!(sql.contains("ms.list_status = 'L'"));
    assert!(sql.contains("COALESCE(ms.is_st, false) = false"));
    assert!(sql.contains("ms.exchange IN ('SSE', 'SZSE')"));
    assert!(sql.contains("ms.market = '主板'"));
}

#[test]
fn combo_score_query_filters_main_chinext_universe_and_excludes_star_market() {
    let sql = combo_score_load_sql(
        ScoreDirection::Descending,
        Some(200),
        TradableUniverseProfile::MainChinextNonSt,
    );

    assert!(sql.contains("JOIN market_stock ms ON ms.symbol = mfv.symbol"));
    assert!(sql.contains("ms.list_status = 'L'"));
    assert!(sql.contains("COALESCE(ms.is_st, false) = false"));
    assert!(sql.contains("ms.exchange IN ('SSE', 'SZSE')"));
    assert!(sql.contains("ms.market IN ('主板', '创业板')"));
    assert!(sql.contains("ms.symbol NOT LIKE '688%SH'"));
}

#[test]
fn tradable_universe_profile_parses_main_chinext_non_st_aliases() {
    assert_eq!(
        TradableUniverseProfile::parse("main_chinext_non_st").unwrap(),
        TradableUniverseProfile::MainChinextNonSt
    );
    assert_eq!(
        TradableUniverseProfile::parse("main-chinext-non-st").unwrap(),
        TradableUniverseProfile::MainChinextNonSt
    );
}

#[test]
fn combo_score_cache_key_separates_universe_profiles() {
    let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let end = NaiveDate::from_ymd_opt(2026, 1, 31).unwrap();

    let all = SignalDataCacheKey::combo_scores(
        "phase7_financial_quality_v1",
        "1.0.0",
        start,
        end,
        ScoreDirection::Descending,
        Some(500),
        TradableUniverseProfile::All,
    );
    let listed_non_st = SignalDataCacheKey::combo_scores(
        "phase7_financial_quality_v1",
        "1.0.0",
        start,
        end,
        ScoreDirection::Descending,
        Some(500),
        TradableUniverseProfile::ListedNonSt,
    );

    assert_ne!(all, listed_non_st);
}

#[test]
fn signal_data_cache_tracks_hits_for_reused_combo_scores() {
    let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let end = NaiveDate::from_ymd_opt(2026, 1, 31).unwrap();
    let key = SignalDataCacheKey::combo_scores(
        "phase7_financial_quality_v1",
        "1.0.0",
        start,
        end,
        ScoreDirection::Descending,
        None,
        TradableUniverseProfile::All,
    );
    let mut cache = SignalDataCache::default();
    let mut scores = HashMap::new();
    scores.insert(
        start,
        vec![("AAA".to_string(), 0.1), ("BBB".to_string(), 0.2)],
    );

    cache.store_combo_scores_for_test(key.clone(), scores);
    let first = cache.cached_combo_scores(&key).expect("first hit");
    let second = cache.cached_combo_scores(&key).expect("second hit");

    assert_eq!(first.len(), second.len());
    assert_eq!(cache.stats().combo_score_hits, 2);
    assert_eq!(cache.stats().combo_score_misses, 0);
}

#[test]
fn signal_data_cache_reuses_larger_combo_candidate_pool_for_smaller_request() {
    let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let end = NaiveDate::from_ymd_opt(2026, 1, 31).unwrap();
    let cached_key = SignalDataCacheKey::combo_scores(
        "phase7_financial_quality_v1",
        "1.0.0",
        start,
        end,
        ScoreDirection::Descending,
        Some(3),
        TradableUniverseProfile::All,
    );
    let requested_key = SignalDataCacheKey::combo_scores(
        "phase7_financial_quality_v1",
        "1.0.0",
        start,
        end,
        ScoreDirection::Descending,
        Some(2),
        TradableUniverseProfile::All,
    );
    let mut scores = HashMap::new();
    scores.insert(
        start,
        vec![
            ("AAA".to_string(), 0.9),
            ("BBB".to_string(), 0.8),
            ("CCC".to_string(), 0.7),
        ],
    );
    let mut cache = SignalDataCache::default();
    cache.store_combo_scores_for_test(cached_key, scores);

    let reused = cache
        .cached_combo_scores(&requested_key)
        .expect("larger candidate pool should satisfy smaller request");

    assert_eq!(
        reused.get(&start).unwrap(),
        &vec![("AAA".to_string(), 0.9), ("BBB".to_string(), 0.8)]
    );
    assert_eq!(cache.stats().combo_score_hits, 1);
    assert_eq!(cache.stats().combo_score_misses, 0);
}

#[test]
fn signal_data_cache_reuses_covering_combo_score_window() {
    let wide_start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let wide_end = NaiveDate::from_ymd_opt(2026, 3, 31).unwrap();
    let narrow_start = NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
    let narrow_day = NaiveDate::from_ymd_opt(2026, 2, 10).unwrap();
    let narrow_end = NaiveDate::from_ymd_opt(2026, 2, 28).unwrap();
    let outside_day = NaiveDate::from_ymd_opt(2026, 3, 10).unwrap();
    let cached_key = SignalDataCacheKey::combo_scores(
        "phase7_financial_quality_v1",
        "1.0.0",
        wide_start,
        wide_end,
        ScoreDirection::Descending,
        Some(3),
        TradableUniverseProfile::ListedNonSt,
    );
    let requested_key = SignalDataCacheKey::combo_scores(
        "phase7_financial_quality_v1",
        "1.0.0",
        narrow_start,
        narrow_end,
        ScoreDirection::Descending,
        Some(2),
        TradableUniverseProfile::ListedNonSt,
    );
    let scores = HashMap::from([
        (
            narrow_day,
            vec![
                ("AAA".to_string(), 0.9),
                ("BBB".to_string(), 0.8),
                ("CCC".to_string(), 0.7),
            ],
        ),
        (outside_day, vec![("DDD".to_string(), 0.6)]),
    ]);
    let mut cache = SignalDataCache::default();
    cache.store_combo_scores_for_test(cached_key, scores);

    let reused = cache
        .cached_combo_scores(&requested_key)
        .expect("wider combo score window should satisfy narrower request");

    assert_eq!(reused.len(), 1);
    assert_eq!(
        reused.get(&narrow_day).unwrap(),
        &vec![("AAA".to_string(), 0.9), ("BBB".to_string(), 0.8)]
    );
    assert!(!reused.contains_key(&outside_day));
    assert_eq!(cache.stats().combo_score_hits, 1);
    assert_eq!(cache.stats().combo_score_misses, 0);
}

#[test]
fn signal_data_cache_ranks_unbounded_covering_combo_window_for_directional_request() {
    let wide_start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let wide_end = NaiveDate::from_ymd_opt(2026, 3, 31).unwrap();
    let narrow_start = NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
    let narrow_day = NaiveDate::from_ymd_opt(2026, 2, 10).unwrap();
    let narrow_end = NaiveDate::from_ymd_opt(2026, 2, 28).unwrap();
    let cached_key = SignalDataCacheKey::combo_scores(
        "phase7_financial_quality_v1",
        "1.0.0",
        wide_start,
        wide_end,
        ScoreDirection::Descending,
        None,
        TradableUniverseProfile::ListedNonSt,
    );
    let requested_key = SignalDataCacheKey::combo_scores(
        "phase7_financial_quality_v1",
        "1.0.0",
        narrow_start,
        narrow_end,
        ScoreDirection::Ascending,
        Some(2),
        TradableUniverseProfile::ListedNonSt,
    );
    let scores = HashMap::from([(
        narrow_day,
        vec![
            ("AAA".to_string(), 0.9),
            ("BBB".to_string(), 0.7),
            ("CCC".to_string(), 0.8),
        ],
    )]);
    let mut cache = SignalDataCache::default();
    cache.store_combo_scores_for_test(cached_key, scores);

    let reused = cache
        .cached_combo_scores(&requested_key)
        .expect("unbounded covering window should be ranked for requested direction");

    assert_eq!(
        reused.get(&narrow_day).unwrap(),
        &vec![("BBB".to_string(), 0.7), ("CCC".to_string(), 0.8)]
    );
    assert_eq!(cache.stats().combo_score_hits, 1);
    assert_eq!(cache.stats().combo_score_misses, 0);
}

#[test]
fn signal_data_cache_reuses_unbounded_combo_scores_for_directional_pool_request() {
    let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let end = NaiveDate::from_ymd_opt(2026, 1, 31).unwrap();
    let cached_key = SignalDataCacheKey::combo_scores(
        "phase7_financial_quality_v1",
        "1.0.0",
        start,
        end,
        ScoreDirection::Descending,
        None,
        TradableUniverseProfile::ListedNonSt,
    );
    let requested_key = SignalDataCacheKey::combo_scores(
        "phase7_financial_quality_v1",
        "1.0.0",
        start,
        end,
        ScoreDirection::Descending,
        Some(2),
        TradableUniverseProfile::ListedNonSt,
    );
    let mut scores = HashMap::new();
    scores.insert(
        start,
        vec![
            ("CCC".to_string(), 0.7),
            ("AAA".to_string(), 0.9),
            ("BBB".to_string(), 0.8),
        ],
    );
    let mut cache = SignalDataCache::default();
    cache.store_combo_scores_for_test(cached_key, scores);

    let reused = cache
        .cached_combo_scores(&requested_key)
        .expect("unbounded cache should satisfy directional pool request");

    assert_eq!(
        reused.get(&start).unwrap(),
        &vec![("AAA".to_string(), 0.9), ("BBB".to_string(), 0.8)]
    );
    assert_eq!(cache.stats().combo_score_hits, 1);
    assert_eq!(cache.stats().combo_score_misses, 0);
}

#[test]
fn signal_data_cache_does_not_reuse_combo_candidate_pool_across_direction_or_universe() {
    let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let end = NaiveDate::from_ymd_opt(2026, 1, 31).unwrap();
    let cached_key = SignalDataCacheKey::combo_scores(
        "phase7_financial_quality_v1",
        "1.0.0",
        start,
        end,
        ScoreDirection::Descending,
        Some(500),
        TradableUniverseProfile::All,
    );
    let ascending_request = SignalDataCacheKey::combo_scores(
        "phase7_financial_quality_v1",
        "1.0.0",
        start,
        end,
        ScoreDirection::Ascending,
        Some(400),
        TradableUniverseProfile::All,
    );
    let universe_request = SignalDataCacheKey::combo_scores(
        "phase7_financial_quality_v1",
        "1.0.0",
        start,
        end,
        ScoreDirection::Descending,
        Some(400),
        TradableUniverseProfile::ListedNonSt,
    );
    let mut cache = SignalDataCache::default();
    cache.store_combo_scores_for_test(cached_key, HashMap::from([(start, Vec::new())]));

    assert!(cache.cached_combo_scores(&ascending_request).is_none());
    assert!(cache.cached_combo_scores(&universe_request).is_none());
    assert_eq!(cache.stats().combo_score_hits, 0);
    assert_eq!(cache.stats().combo_score_misses, 2);
}

#[test]
fn signal_data_cache_tracks_hits_for_reused_prediction_scores() {
    let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let end = NaiveDate::from_ymd_opt(2026, 1, 31).unwrap();
    let key = SignalDataCacheKey::prediction_scores("pred-phase7-wide", start, end);
    let mut cache = SignalDataCache::default();
    let scores = HashMap::from([(
        start,
        vec![
            ("AAA".to_string(), 0.1, Some(1)),
            ("BBB".to_string(), 0.2, Some(2)),
        ],
    )]);

    cache.store_prediction_scores_for_test(key.clone(), scores);
    let first = cache.cached_prediction_scores(&key).expect("first hit");
    let second = cache.cached_prediction_scores(&key).expect("second hit");

    assert_eq!(first.len(), second.len());
    assert_eq!(cache.stats().prediction_score_hits, 2);
    assert_eq!(cache.stats().prediction_score_misses, 0);
}

#[test]
fn liquidity_filter_uses_cached_average_amount_inputs() {
    let trade_date = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
    let mut scores_by_date = HashMap::from([(
        trade_date,
        vec![
            ("LIQUID".to_string(), 0.9),
            ("THIN".to_string(), 0.8),
            ("MISSING".to_string(), 0.7),
        ],
    )]);
    let average_amounts = HashMap::from([
        ("LIQUID".to_string(), 5_000.0),
        ("THIN".to_string(), 1_000.0),
    ]);

    let stats = retain_scores_with_min_average_amount(
        &mut scores_by_date,
        3_000_000.0,
        &average_amounts,
    );

    assert_eq!(stats.before, 3);
    assert_eq!(stats.after, 1);
    assert_eq!(stats.liquid_symbols, 1);
    assert_eq!(
        scores_by_date[&trade_date],
        vec![("LIQUID".to_string(), 0.9)]
    );
}

#[test]
fn market_regime_policy_classifies_and_applies_bear_defensive_rule() {
    let base = SignalConfig {
        combo_name: "phase7_financial_quality_v1".to_string(),
        version: "1.0.0".to_string(),
        top_n: 80,
        rebalance_freq_days: 20,
        max_gross_exposure: 1.0,
        score_direction: ScoreDirection::Descending,
        ..Default::default()
    };
    let policy = MarketRegimePolicy::professional_default("000300.SH");
    let returns = vec![-0.015, -0.02, 0.004, -0.012, -0.008, 0.003, -0.01];

    let regime = classify_market_regime(&returns, &policy);
    let active = policy.apply(&base, regime);

    assert_eq!(regime, MarketRegime::Bear);
    assert_eq!(active.top_n, 30);
    assert_eq!(active.rebalance_freq_days, 60);
    assert_eq!(active.max_gross_exposure, 0.50);
    assert_eq!(active.score_direction, ScoreDirection::Ascending);
}

#[test]
fn market_regime_rule_can_route_to_regime_specific_alpha_combo() {
    let base = SignalConfig {
        combo_name: "phase7_financial_quality_v1".to_string(),
        version: "1.0.0".to_string(),
        top_n: 20,
        rebalance_freq_days: 60,
        max_gross_exposure: 1.0,
        max_position_pct: Decimal::new(15, 2),
        skip_top_pct: 0.10,
        score_direction: ScoreDirection::Ascending,
        ..Default::default()
    };
    let policy = MarketRegimePolicy::quality_regime_alpha_switch_v1("000300.SH");

    let bull = policy.apply(&base, MarketRegime::Bull);
    let bear = policy.apply(&base, MarketRegime::Bear);
    let high_volatility = policy.apply(&base, MarketRegime::HighVolatility);

    assert_eq!(bull.combo_name, "phase7_financial_quality_v1");
    assert_eq!(bull.score_direction, ScoreDirection::Ascending);
    assert_eq!(bear.combo_name, "phase7_industry_residual_quality_v1");
    assert_eq!(bear.version, "1.0.0");
    assert_eq!(bear.score_direction, ScoreDirection::Descending);
    assert_eq!(bear.max_gross_exposure, 0.78);
    assert_eq!(
        high_volatility.combo_name,
        "phase7_industry_residual_quality_v1"
    );
    assert_eq!(high_volatility.score_direction, ScoreDirection::Descending);
    assert_eq!(high_volatility.max_gross_exposure, 0.66);
}

#[test]
fn market_regime_alpha_switch_variants_route_to_distinct_stress_sleeves() {
    let base = SignalConfig {
        combo_name: "phase7_financial_quality_v1".to_string(),
        version: "1.0.0".to_string(),
        top_n: 20,
        rebalance_freq_days: 60,
        max_gross_exposure: 1.0,
        max_position_pct: Decimal::new(15, 2),
        skip_top_pct: 0.10,
        score_direction: ScoreDirection::Ascending,
        ..Default::default()
    };

    let value = MarketRegimePolicy::quality_regime_alpha_switch_value_v1("000300.SH")
        .apply(&base, MarketRegime::Bear);
    let recovery = MarketRegimePolicy::quality_regime_alpha_switch_recovery_v1("000300.SH")
        .apply(&base, MarketRegime::Bear);
    let blend = MarketRegimePolicy::quality_regime_alpha_switch_blend_v1("000300.SH")
        .apply(&base, MarketRegime::HighVolatility);

    assert_eq!(value.combo_name, "phase7_valuation_v1");
    assert_eq!(value.score_direction, ScoreDirection::Descending);
    assert_eq!(recovery.combo_name, "phase7_growth_recovery_v1");
    assert_eq!(recovery.score_direction, ScoreDirection::Descending);
    assert_eq!(blend.combo_name, "phase7_quality_value_recovery_confirm_v1");
    assert_eq!(blend.score_direction, ScoreDirection::Descending);
}

#[test]
fn drawdown_control_policy_deleverages_bear_and_high_volatility() {
    let base = SignalConfig {
        top_n: 80,
        rebalance_freq_days: 20,
        max_gross_exposure: 1.0,
        score_direction: ScoreDirection::Descending,
        ..Default::default()
    };
    let policy = MarketRegimePolicy::drawdown_control_v1("000300.SH");

    let bear = policy.apply(&base, MarketRegime::Bear);
    let high_volatility = policy.apply(&base, MarketRegime::HighVolatility);

    assert_eq!(policy.bear_drawdown_threshold, 0.12);
    assert_eq!(policy.high_volatility_threshold, 0.24);
    assert_eq!(bear.top_n, 20);
    assert_eq!(bear.rebalance_freq_days, 60);
    assert_eq!(bear.max_gross_exposure, 0.35);
    assert_eq!(bear.score_direction, ScoreDirection::Ascending);
    assert_eq!(bear.skip_top_pct, 0.10);
    assert_eq!(high_volatility.max_gross_exposure, 0.25);
}

#[test]
fn drawdown_control_v2_is_stricter_than_v1() {
    let base = SignalConfig {
        top_n: 80,
        rebalance_freq_days: 20,
        max_gross_exposure: 1.0,
        max_position_pct: Decimal::new(10, 2),
        score_direction: ScoreDirection::Descending,
        ..Default::default()
    };
    let v1 = MarketRegimePolicy::drawdown_control_v1("000300.SH");
    let v2 = MarketRegimePolicy::drawdown_control_v2("000300.SH");

    let bear_v1 = v1.apply(&base, MarketRegime::Bear);
    let bear_v2 = v2.apply(&base, MarketRegime::Bear);
    let high_volatility_v1 = v1.apply(&base, MarketRegime::HighVolatility);
    let high_volatility_v2 = v2.apply(&base, MarketRegime::HighVolatility);
    let sideways_v2 = v2.apply(&base, MarketRegime::Sideways);

    assert!(v2.bear_drawdown_threshold < v1.bear_drawdown_threshold);
    assert!(v2.high_volatility_threshold < v1.high_volatility_threshold);
    assert_eq!(v2.min_observations, 10);
    assert_eq!(bear_v2.top_n, 15);
    assert_eq!(bear_v2.max_gross_exposure, 0.25);
    assert_eq!(bear_v2.max_position_pct, Decimal::new(4, 2));
    assert_eq!(bear_v2.skip_top_pct, 0.15);
    assert_eq!(bear_v2.score_direction, ScoreDirection::Ascending);
    assert!(bear_v2.max_gross_exposure < bear_v1.max_gross_exposure);
    assert_eq!(high_volatility_v2.top_n, 15);
    assert_eq!(high_volatility_v2.max_gross_exposure, 0.18);
    assert_eq!(high_volatility_v2.max_position_pct, Decimal::new(4, 2));
    assert!(high_volatility_v2.max_gross_exposure < high_volatility_v1.max_gross_exposure);
    assert_eq!(sideways_v2.max_gross_exposure, 0.55);
    assert_eq!(sideways_v2.max_position_pct, Decimal::new(6, 2));
}

#[test]
fn quality_risk_off_keeps_alpha_direction_while_reducing_exposure() {
    let base = SignalConfig {
        top_n: 20,
        rebalance_freq_days: 60,
        max_gross_exposure: 1.0,
        max_position_pct: Decimal::new(15, 2),
        score_direction: ScoreDirection::Ascending,
        ..Default::default()
    };
    let policy = MarketRegimePolicy::quality_risk_off_v1("000300.SH");

    let bull = policy.apply(&base, MarketRegime::Bull);
    let bear = policy.apply(&base, MarketRegime::Bear);
    let high_volatility = policy.apply(&base, MarketRegime::HighVolatility);

    assert_eq!(bull.score_direction, ScoreDirection::Ascending);
    assert_eq!(bull.top_n, 20);
    assert_eq!(bull.rebalance_freq_days, 60);
    assert_eq!(bear.score_direction, ScoreDirection::Ascending);
    assert_eq!(bear.top_n, 20);
    assert_eq!(bear.max_gross_exposure, 0.80);
    assert_eq!(bear.max_position_pct, Decimal::new(15, 2));
    assert_eq!(high_volatility.score_direction, ScoreDirection::Ascending);
    assert_eq!(high_volatility.max_gross_exposure, 0.65);
    assert_eq!(high_volatility.max_position_pct, Decimal::new(12, 2));
}

#[test]
fn quality_crash_guard_preserves_alpha_shape_and_only_scales_tail_regimes() {
    let base = SignalConfig {
        top_n: 20,
        rebalance_freq_days: 60,
        max_gross_exposure: 1.0,
        max_position_pct: Decimal::new(15, 2),
        skip_top_pct: 0.10,
        score_direction: ScoreDirection::Ascending,
        ..Default::default()
    };
    let policy = MarketRegimePolicy::quality_crash_guard_v1("000300.SH");

    let bull = policy.apply(&base, MarketRegime::Bull);
    let sideways = policy.apply(&base, MarketRegime::Sideways);
    let bear = policy.apply(&base, MarketRegime::Bear);
    let high_volatility = policy.apply(&base, MarketRegime::HighVolatility);

    assert_eq!(policy.bear_drawdown_threshold, 0.25);
    assert_eq!(policy.high_volatility_threshold, 0.50);
    assert_eq!(bull.score_direction, ScoreDirection::Ascending);
    assert_eq!(bull.top_n, 20);
    assert_eq!(bull.rebalance_freq_days, 60);
    assert_eq!(bull.skip_top_pct, 0.10);
    assert_eq!(sideways.max_gross_exposure, 1.0);
    assert_eq!(bear.score_direction, ScoreDirection::Ascending);
    assert_eq!(bear.top_n, 20);
    assert_eq!(bear.rebalance_freq_days, 60);
    assert_eq!(bear.skip_top_pct, 0.10);
    assert_eq!(bear.max_gross_exposure, 0.85);
    assert_eq!(bear.max_position_pct, Decimal::new(12, 2));
    assert_eq!(high_volatility.max_gross_exposure, 0.75);
    assert_eq!(high_volatility.max_position_pct, Decimal::new(10, 2));
}

#[test]
fn quality_crash_guard_v2_preserves_alpha_shape_with_stronger_tail_scaling() {
    let base = SignalConfig {
        top_n: 20,
        rebalance_freq_days: 60,
        max_gross_exposure: 1.0,
        max_position_pct: Decimal::new(15, 2),
        skip_top_pct: 0.10,
        score_direction: ScoreDirection::Ascending,
        ..Default::default()
    };
    let policy = MarketRegimePolicy::quality_crash_guard_v2("000300.SH");

    let bull = policy.apply(&base, MarketRegime::Bull);
    let sideways = policy.apply(&base, MarketRegime::Sideways);
    let bear = policy.apply(&base, MarketRegime::Bear);
    let high_volatility = policy.apply(&base, MarketRegime::HighVolatility);

    assert_eq!(policy.bear_drawdown_threshold, 0.22);
    assert_eq!(policy.high_volatility_threshold, 0.45);
    assert_eq!(bull.score_direction, ScoreDirection::Ascending);
    assert_eq!(bull.top_n, 20);
    assert_eq!(bull.skip_top_pct, 0.10);
    assert_eq!(sideways.max_gross_exposure, 1.0);
    assert_eq!(bear.score_direction, ScoreDirection::Ascending);
    assert_eq!(bear.top_n, 20);
    assert_eq!(bear.rebalance_freq_days, 60);
    assert_eq!(bear.skip_top_pct, 0.10);
    assert_eq!(bear.max_gross_exposure, 0.75);
    assert_eq!(bear.max_position_pct, Decimal::new(10, 2));
    assert_eq!(high_volatility.max_gross_exposure, 0.60);
    assert_eq!(high_volatility.max_position_pct, Decimal::new(8, 2));
}

#[test]
fn quality_crash_guard_v3_keeps_late_trigger_with_mid_tail_scaling() {
    let base = SignalConfig {
        top_n: 20,
        rebalance_freq_days: 60,
        max_gross_exposure: 1.0,
        max_position_pct: Decimal::new(15, 2),
        skip_top_pct: 0.10,
        score_direction: ScoreDirection::Ascending,
        ..Default::default()
    };
    let policy = MarketRegimePolicy::quality_crash_guard_v3("000300.SH");

    let bull = policy.apply(&base, MarketRegime::Bull);
    let bear = policy.apply(&base, MarketRegime::Bear);
    let high_volatility = policy.apply(&base, MarketRegime::HighVolatility);

    assert_eq!(policy.bear_drawdown_threshold, 0.25);
    assert_eq!(policy.high_volatility_threshold, 0.50);
    assert_eq!(bull.score_direction, ScoreDirection::Ascending);
    assert_eq!(bull.top_n, 20);
    assert_eq!(bear.score_direction, ScoreDirection::Ascending);
    assert_eq!(bear.top_n, 20);
    assert_eq!(bear.skip_top_pct, 0.10);
    assert_eq!(bear.max_gross_exposure, 0.80);
    assert_eq!(bear.max_position_pct, Decimal::new(11, 2));
    assert_eq!(high_volatility.max_gross_exposure, 0.68);
    assert_eq!(high_volatility.max_position_pct, Decimal::new(9, 2));
}

#[test]
fn quality_bear_window_guard_triggers_earlier_without_flipping_quality_alpha() {
    let base = SignalConfig {
        top_n: 20,
        rebalance_freq_days: 60,
        max_gross_exposure: 1.0,
        max_position_pct: Decimal::new(15, 2),
        skip_top_pct: 0.10,
        score_direction: ScoreDirection::Ascending,
        ..Default::default()
    };
    let policy = MarketRegimePolicy::quality_bear_window_guard_v1("000300.SH");

    let bull = policy.apply(&base, MarketRegime::Bull);
    let bear = policy.apply(&base, MarketRegime::Bear);
    let high_volatility = policy.apply(&base, MarketRegime::HighVolatility);

    assert_eq!(policy.lookback_days, 126);
    assert_eq!(policy.bear_drawdown_threshold, 0.16);
    assert_eq!(policy.high_volatility_threshold, 0.32);
    assert_eq!(bull.max_gross_exposure, 1.0);
    assert_eq!(bear.score_direction, ScoreDirection::Ascending);
    assert_eq!(bear.top_n, 20);
    assert_eq!(bear.rebalance_freq_days, 60);
    assert_eq!(bear.skip_top_pct, 0.10);
    assert_eq!(bear.max_gross_exposure, 0.78);
    assert_eq!(bear.max_position_pct, Decimal::new(11, 2));
    assert_eq!(high_volatility.score_direction, ScoreDirection::Ascending);
    assert_eq!(high_volatility.max_gross_exposure, 0.66);
    assert_eq!(high_volatility.max_position_pct, Decimal::new(9, 2));
}

#[test]
fn quality_bear_position_guard_changes_only_position_risk_shape_in_stress_regimes() {
    let base = SignalConfig {
        top_n: 20,
        rebalance_freq_days: 60,
        max_gross_exposure: 1.0,
        max_position_pct: Decimal::new(15, 2),
        skip_top_pct: 0.10,
        score_direction: ScoreDirection::Ascending,
        ..Default::default()
    };
    let policy = MarketRegimePolicy::quality_bear_position_guard_v1("000300.SH");

    let bull = policy.apply(&base, MarketRegime::Bull);
    let bear = policy.apply(&base, MarketRegime::Bear);
    let high_volatility = policy.apply(&base, MarketRegime::HighVolatility);

    assert_eq!(policy.lookback_days, 126);
    assert_eq!(policy.bear_drawdown_threshold, 0.14);
    assert_eq!(bull.score_direction, ScoreDirection::Ascending);
    assert_eq!(bull.top_n, 20);
    assert_eq!(bull.max_gross_exposure, 1.0);
    assert_eq!(bear.score_direction, ScoreDirection::Ascending);
    assert_eq!(bear.top_n, 25);
    assert_eq!(bear.rebalance_freq_days, 80);
    assert_eq!(bear.skip_top_pct, 0.05);
    assert_eq!(bear.max_gross_exposure, 0.74);
    assert_eq!(bear.max_position_pct, Decimal::new(9, 2));
    assert_eq!(high_volatility.top_n, 25);
    assert_eq!(high_volatility.rebalance_freq_days, 40);
    assert_eq!(high_volatility.max_gross_exposure, 0.58);
    assert_eq!(high_volatility.max_position_pct, Decimal::new(75, 3));
}

#[test]
fn quality_bear_position_guard_v3_is_milder_for_return_preservation() {
    let base = SignalConfig {
        top_n: 20,
        rebalance_freq_days: 60,
        max_gross_exposure: 1.0,
        max_position_pct: Decimal::new(15, 2),
        skip_top_pct: 0.10,
        score_direction: ScoreDirection::Ascending,
        ..Default::default()
    };
    let policy = MarketRegimePolicy::quality_bear_position_guard_v3("000300.SH");

    let bull = policy.apply(&base, MarketRegime::Bull);
    let bear = policy.apply(&base, MarketRegime::Bear);
    let high_volatility = policy.apply(&base, MarketRegime::HighVolatility);

    assert_eq!(policy.lookback_days, 126);
    assert_eq!(policy.bear_drawdown_threshold, 0.14);
    assert_eq!(bull.score_direction, ScoreDirection::Ascending);
    assert_eq!(bull.top_n, 20);
    assert_eq!(bull.max_gross_exposure, 1.0);
    assert_eq!(bear.score_direction, ScoreDirection::Ascending);
    assert_eq!(bear.top_n, 25);
    assert_eq!(bear.rebalance_freq_days, 80);
    assert_eq!(bear.skip_top_pct, 0.05);
    assert_eq!(bear.max_gross_exposure, 0.82);
    assert_eq!(bear.max_position_pct, Decimal::new(11, 2));
    assert_eq!(high_volatility.top_n, 25);
    assert_eq!(high_volatility.rebalance_freq_days, 40);
    assert_eq!(high_volatility.max_gross_exposure, 0.66);
    assert_eq!(high_volatility.max_position_pct, Decimal::new(9, 2));
}

#[test]
fn quality_event_window_position_guard_keeps_event_sleeve_and_position_shape() {
    let base = SignalConfig {
        combo_name: "phase7_financial_quality_v1".to_string(),
        version: "1.0.0".to_string(),
        top_n: 20,
        rebalance_freq_days: 60,
        max_gross_exposure: 1.0,
        max_position_pct: Decimal::new(15, 2),
        skip_top_pct: 0.10,
        score_direction: ScoreDirection::Ascending,
        ..Default::default()
    };
    let policy = MarketRegimePolicy::quality_event_window_position_guard_v3("000300.SH");

    let bull = policy.apply(&base, MarketRegime::Bull);
    let bear = policy.apply(&base, MarketRegime::Bear);
    let high_volatility = policy.apply(&base, MarketRegime::HighVolatility);

    assert!(bull.portfolio_sleeve.is_none());
    assert_eq!(bear.top_n, 25);
    assert_eq!(bear.rebalance_freq_days, 80);
    assert_eq!(bear.max_gross_exposure, 0.82);
    assert_eq!(bear.max_position_pct, Decimal::new(11, 2));
    let bear_sleeve = bear.portfolio_sleeve.expect("bear event sleeve");
    assert_eq!(bear_sleeve.combo_name, "phase7_event_window_earnings_v1");
    assert_eq!(bear_sleeve.version, "1.0.0");
    assert_eq!(bear_sleeve.score_direction, ScoreDirection::Descending);
    assert!((bear_sleeve.weight - 0.15).abs() < 1e-9);
    assert_eq!(high_volatility.top_n, 25);
    assert_eq!(high_volatility.max_gross_exposure, 0.66);
    assert!(high_volatility.portfolio_sleeve.is_some());
}

#[test]
fn quality_event_window_return_sharpe_router_tightens_correlation_in_stress_only() {
    let base = SignalConfig {
        combo_name: "phase7_financial_quality_v1".to_string(),
        version: "1.0.0".to_string(),
        top_n: 20,
        rebalance_freq_days: 60,
        max_gross_exposure: 1.0,
        max_position_pct: Decimal::new(15, 2),
        max_pairwise_correlation: Some(0.75),
        skip_top_pct: 0.10,
        score_direction: ScoreDirection::Ascending,
        ..Default::default()
    };
    let policy = MarketRegimePolicy::quality_event_window_return_sharpe_router_v1("000300.SH");

    let bull = policy.apply(&base, MarketRegime::Bull);
    let bear = policy.apply(&base, MarketRegime::Bear);
    let high_volatility = policy.apply(&base, MarketRegime::HighVolatility);

    assert_eq!(bull.max_pairwise_correlation, Some(0.75));
    assert!(bull.portfolio_sleeve.is_none());
    assert_eq!(bear.max_pairwise_correlation, Some(0.65));
    assert_eq!(bear.top_n, 20);
    assert_eq!(bear.max_gross_exposure, 0.72);
    assert_eq!(bear.max_position_pct, Decimal::new(10, 2));
    let bear_sleeve = bear.portfolio_sleeve.expect("bear event sleeve");
    assert_eq!(bear_sleeve.combo_name, "phase7_event_window_earnings_v1");
    assert!((bear_sleeve.weight - 0.15).abs() < 1e-9);
    assert_eq!(high_volatility.max_pairwise_correlation, Some(0.65));
    assert_eq!(high_volatility.max_gross_exposure, 0.58);
    assert!(high_volatility.portfolio_sleeve.is_some());
}

#[test]
fn quality_event_window_return_sharpe_router_frontier_interpolates_exposure() {
    let base = SignalConfig {
        combo_name: "phase7_financial_quality_v1".to_string(),
        version: "1.0.0".to_string(),
        top_n: 20,
        rebalance_freq_days: 60,
        max_gross_exposure: 1.0,
        max_position_pct: Decimal::new(15, 2),
        max_pairwise_correlation: Some(0.75),
        skip_top_pct: 0.10,
        score_direction: ScoreDirection::Ascending,
        ..Default::default()
    };
    let balanced =
        MarketRegimePolicy::quality_event_window_return_sharpe_router_v3("000300.SH");
    let tighter = MarketRegimePolicy::quality_event_window_return_sharpe_router_v4("000300.SH");

    let balanced_bear = balanced.apply(&base, MarketRegime::Bear);
    let tighter_bear = tighter.apply(&base, MarketRegime::Bear);
    let balanced_high_vol = balanced.apply(&base, MarketRegime::HighVolatility);
    let tighter_high_vol = tighter.apply(&base, MarketRegime::HighVolatility);

    assert_eq!(balanced_bear.max_gross_exposure, 0.75);
    assert_eq!(balanced_bear.max_position_pct, Decimal::new(10, 2));
    assert_eq!(balanced_high_vol.max_gross_exposure, 0.62);
    assert_eq!(balanced_high_vol.max_position_pct, Decimal::new(9, 2));
    assert_eq!(tighter_bear.max_gross_exposure, 0.68);
    assert_eq!(tighter_bear.max_position_pct, Decimal::new(9, 2));
    assert_eq!(tighter_high_vol.max_gross_exposure, 0.54);
    assert_eq!(tighter_high_vol.max_position_pct, Decimal::new(7, 2));
    assert!(balanced_bear.portfolio_sleeve.is_some());
    assert!(tighter_bear.portfolio_sleeve.is_some());
}

#[test]
fn quality_state_alpha_selector_routes_distinct_sleeves_by_regime() {
    let base = SignalConfig {
        combo_name: "phase7_financial_quality_v1".to_string(),
        version: "1.0.0".to_string(),
        top_n: 20,
        rebalance_freq_days: 60,
        max_gross_exposure: 1.0,
        max_position_pct: Decimal::new(15, 2),
        max_pairwise_correlation: Some(0.75),
        skip_top_pct: 0.10,
        score_direction: ScoreDirection::Ascending,
        ..Default::default()
    };
    let policy = MarketRegimePolicy::quality_state_alpha_selector_v1("000300.SH");

    let bull = policy.apply(&base, MarketRegime::Bull);
    let bear = policy.apply(&base, MarketRegime::Bear);
    let sideways = policy.apply(&base, MarketRegime::Sideways);
    let mixed = policy.apply(&base, MarketRegime::Mixed);

    let bull_sleeve = bull.portfolio_sleeve.expect("bull quality/value sleeve");
    assert_eq!(
        bull_sleeve.combo_name,
        "phase7_quality_value_recovery_confirm_v1"
    );
    assert_eq!(bull_sleeve.score_direction, ScoreDirection::Descending);
    assert!((bull_sleeve.weight - 0.05).abs() < 1e-9);
    let bear_sleeve = bear.portfolio_sleeve.expect("bear event sleeve");
    assert_eq!(bear.max_pairwise_correlation, Some(0.65));
    assert_eq!(bear.max_gross_exposure, 0.68);
    assert_eq!(bear_sleeve.combo_name, "phase7_event_window_earnings_v1");
    assert!((bear_sleeve.weight - 0.15).abs() < 1e-9);
    let sideways_sleeve = sideways.portfolio_sleeve.expect("sideways low-risk sleeve");
    assert_eq!(
        sideways_sleeve.combo_name,
        "phase7_price_volume_expanded_v1"
    );
    assert_eq!(sideways_sleeve.score_direction, ScoreDirection::Ascending);
    assert!((sideways_sleeve.weight - 0.10).abs() < 1e-9);
    let mixed_sleeve = mixed.portfolio_sleeve.expect("mixed value sleeve");
    assert_eq!(mixed_sleeve.combo_name, "phase7_valuation_v1");
    assert_eq!(mixed_sleeve.score_direction, ScoreDirection::Descending);
    assert!((mixed_sleeve.weight - 0.10).abs() < 1e-9);
}

#[test]
fn quality_state_alpha_overlay_selector_adds_small_orthogonal_overlay() {
    let base = SignalConfig {
        combo_name: "phase7_financial_quality_v1".to_string(),
        version: "1.0.0".to_string(),
        top_n: 20,
        rebalance_freq_days: 60,
        max_gross_exposure: 1.0,
        max_position_pct: Decimal::new(15, 2),
        max_pairwise_correlation: Some(0.75),
        skip_top_pct: 0.10,
        score_direction: ScoreDirection::Ascending,
        ..Default::default()
    };
    let policy = MarketRegimePolicy::quality_state_alpha_overlay_selector_v1("000300.SH");

    let bear = policy.apply(&base, MarketRegime::Bear);
    let high_volatility = policy.apply(&base, MarketRegime::HighVolatility);

    let bear_sleeve = bear.portfolio_sleeve.expect("bear event sleeve");
    let bear_overlay = bear.score_overlay.expect("bear valuation overlay");
    assert_eq!(bear_sleeve.combo_name, "phase7_event_window_earnings_v1");
    assert_eq!(bear_overlay.combo_name, "phase7_valuation_v1");
    assert_eq!(bear_overlay.score_direction, ScoreDirection::Descending);
    assert!((bear_overlay.weight - 0.05).abs() < 1e-9);
    let high_volatility_sleeve = high_volatility
        .portfolio_sleeve
        .expect("high-volatility low-risk sleeve");
    let high_volatility_overlay = high_volatility
        .score_overlay
        .expect("high-volatility low-risk overlay");
    assert_eq!(
        high_volatility_sleeve.combo_name,
        "phase7_price_volume_expanded_v1"
    );
    assert_eq!(
        high_volatility_overlay.combo_name,
        "phase7_price_volume_expanded_v1"
    );
    assert_eq!(
        high_volatility_overlay.score_direction,
        ScoreDirection::Ascending
    );
    assert!((high_volatility_overlay.weight - 0.05).abs() < 1e-9);
}

#[test]
fn quality_mixed_event_state_selector_routes_event_flow_in_mixed_state_only() {
    let base = SignalConfig {
        combo_name: "phase7_financial_quality_v1".to_string(),
        version: "1.0.0".to_string(),
        top_n: 20,
        rebalance_freq_days: 60,
        max_gross_exposure: 1.0,
        max_position_pct: Decimal::new(15, 2),
        max_pairwise_correlation: Some(0.75),
        skip_top_pct: 0.10,
        score_direction: ScoreDirection::Ascending,
        ..Default::default()
    };
    let policy = MarketRegimePolicy::quality_mixed_event_state_overlay_selector_v1("000300.SH");

    let mixed = policy.apply(&base, MarketRegime::Mixed);
    let sideways = policy.apply(&base, MarketRegime::Sideways);
    let mixed_sleeve = mixed.portfolio_sleeve.expect("mixed event-window sleeve");
    let mixed_overlay = mixed.score_overlay.expect("mixed valuation overlay");
    let sideways_sleeve = sideways
        .portfolio_sleeve
        .expect("sideways valuation sleeve");

    assert_eq!(mixed_sleeve.combo_name, "phase7_event_window_earnings_v1");
    assert_eq!(mixed_sleeve.score_direction, ScoreDirection::Descending);
    assert!((mixed_sleeve.weight - 0.10).abs() < 1e-9);
    assert_eq!(mixed_overlay.combo_name, "phase7_valuation_v1");
    assert!((mixed_overlay.weight - 0.05).abs() < 1e-9);
    assert_eq!(sideways_sleeve.combo_name, "phase7_valuation_v1");
    assert!(sideways.score_overlay.is_none());
}

#[test]
fn quality_mixed_state_risk_memory_router_only_tightens_mixed_state() {
    let base = SignalConfig {
        combo_name: "phase7_financial_quality_v1".to_string(),
        version: "1.0.0".to_string(),
        top_n: 20,
        rebalance_freq_days: 60,
        max_gross_exposure: 1.0,
        max_position_pct: Decimal::new(15, 2),
        max_pairwise_correlation: Some(0.75),
        skip_top_pct: 0.10,
        score_direction: ScoreDirection::Ascending,
        ..Default::default()
    };
    let policy = MarketRegimePolicy::quality_mixed_state_risk_memory_router_v4("000300.SH");

    let mixed = policy.apply(&base, MarketRegime::Mixed);
    let bull = policy.apply(&base, MarketRegime::Bull);
    let mixed_sleeve = mixed.portfolio_sleeve.expect("mixed event-window sleeve");

    assert_eq!(mixed_sleeve.combo_name, "phase7_event_window_earnings_v1");
    assert_eq!(mixed.max_gross_exposure, 0.90);
    assert_eq!(mixed.max_position_pct, Decimal::new(13, 2));
    assert_eq!(mixed.max_pairwise_correlation, Some(0.70));
    assert_eq!(bull.max_gross_exposure, 1.0);
    assert_eq!(bull.max_position_pct, Decimal::new(15, 2));
    assert_eq!(bull.max_pairwise_correlation, Some(0.75));
}

#[test]
fn quality_mixed_state_risk_memory_router_frontier_relaxes_mixed_risk_only() {
    let base = SignalConfig {
        combo_name: "phase7_financial_quality_v1".to_string(),
        version: "1.0.0".to_string(),
        top_n: 20,
        rebalance_freq_days: 60,
        max_gross_exposure: 1.0,
        max_position_pct: Decimal::new(15, 2),
        max_pairwise_correlation: Some(0.75),
        skip_top_pct: 0.10,
        score_direction: ScoreDirection::Ascending,
        ..Default::default()
    };
    let strict = MarketRegimePolicy::quality_mixed_state_risk_memory_router_v14("000300.SH");
    let relaxed = MarketRegimePolicy::quality_mixed_state_risk_memory_router_v16("000300.SH");

    let strict_mixed = strict.apply(&base, MarketRegime::Mixed);
    let strict_bear = strict.apply(&base, MarketRegime::Bear);
    let strict_bull = strict.apply(&base, MarketRegime::Bull);
    let relaxed_mixed = relaxed.apply(&base, MarketRegime::Mixed);
    let relaxed_bear = relaxed.apply(&base, MarketRegime::Bear);
    let relaxed_bull = relaxed.apply(&base, MarketRegime::Bull);

    assert_eq!(strict_mixed.max_gross_exposure, 1.0);
    assert_eq!(strict_mixed.max_position_pct, Decimal::new(13, 2));
    assert_eq!(strict_mixed.max_pairwise_correlation, Some(0.70));
    assert_eq!(relaxed_mixed.max_gross_exposure, 1.0);
    assert_eq!(relaxed_mixed.max_position_pct, Decimal::new(15, 2));
    assert_eq!(relaxed_mixed.max_pairwise_correlation, Some(0.75));
    assert_eq!(relaxed_bear.max_position_pct, strict_bear.max_position_pct);
    assert_eq!(
        relaxed_bear.max_pairwise_correlation,
        strict_bear.max_pairwise_correlation
    );
    assert_eq!(relaxed_bull.max_position_pct, strict_bull.max_position_pct);
    assert_eq!(
        relaxed_bull.max_pairwise_correlation,
        strict_bull.max_pairwise_correlation
    );
}

#[test]
fn quality_mixed_orthogonal_alpha_routes_residual_confirmation_in_mixed_state() {
    let base = SignalConfig {
        combo_name: "phase7_financial_quality_v1".to_string(),
        version: "1.0.0".to_string(),
        top_n: 20,
        rebalance_freq_days: 60,
        max_gross_exposure: 1.0,
        max_position_pct: Decimal::new(15, 2),
        max_pairwise_correlation: Some(0.75),
        skip_top_pct: 0.10,
        score_direction: ScoreDirection::Ascending,
        ..Default::default()
    };
    let policy = MarketRegimePolicy::quality_mixed_orthogonal_alpha_selector_v1("000300.SH");

    let mixed = policy.apply(&base, MarketRegime::Mixed);
    let bear = policy.apply(&base, MarketRegime::Bear);
    let mixed_sleeve = mixed
        .portfolio_sleeve
        .expect("mixed residual confirmation sleeve");
    let mixed_overlay = mixed
        .score_overlay
        .expect("mixed valuation confirmation overlay");
    let bear_sleeve = bear.portfolio_sleeve.expect("bear event-window sleeve");

    assert_eq!(
        mixed_sleeve.combo_name,
        "phase7_quality_residual_confirm_10pct_v1"
    );
    assert_eq!(mixed_sleeve.score_direction, ScoreDirection::Ascending);
    assert!((mixed_sleeve.weight - 0.10).abs() < 1e-9);
    assert_eq!(mixed_overlay.combo_name, "phase7_valuation_v1");
    assert_eq!(mixed_overlay.score_direction, ScoreDirection::Descending);
    assert_eq!(bear_sleeve.combo_name, "phase7_event_window_earnings_v1");
}

#[test]
fn quality_mixed_orthogonal_risk_memory_reuses_cc_risk_shell() {
    let base = SignalConfig {
        combo_name: "phase7_financial_quality_v1".to_string(),
        version: "1.0.0".to_string(),
        top_n: 20,
        rebalance_freq_days: 60,
        max_gross_exposure: 1.0,
        max_position_pct: Decimal::new(15, 2),
        max_pairwise_correlation: Some(0.75),
        skip_top_pct: 0.10,
        score_direction: ScoreDirection::Ascending,
        ..Default::default()
    };
    let policy =
        MarketRegimePolicy::quality_mixed_orthogonal_risk_memory_router_v2("000300.SH");

    let mixed = policy.apply(&base, MarketRegime::Mixed);
    let bull = policy.apply(&base, MarketRegime::Bull);
    let mixed_sleeve = mixed
        .portfolio_sleeve
        .expect("mixed value/recovery/event sleeve");
    let mixed_overlay = mixed
        .score_overlay
        .expect("mixed residual confirmation overlay");

    assert_eq!(
        mixed_sleeve.combo_name,
        "phase7_quality_value_recovery_event_confirm_v1"
    );
    assert_eq!(
        mixed_overlay.combo_name,
        "phase7_quality_residual_confirm_10pct_v1"
    );
    assert_eq!(mixed.max_gross_exposure, 0.90);
    assert_eq!(mixed.max_position_pct, Decimal::new(13, 2));
    assert_eq!(mixed.max_pairwise_correlation, Some(0.70));
    assert_eq!(bull.max_gross_exposure, 1.0);
    assert_eq!(bull.max_pairwise_correlation, Some(0.75));
}

#[test]
fn regime_rules_never_relax_search_level_exposure_caps() {
    let base = SignalConfig {
        combo_name: "phase7_financial_quality_v1".to_string(),
        version: "1.0.0".to_string(),
        top_n: 20,
        rebalance_freq_days: 60,
        max_gross_exposure: 0.35,
        max_position_pct: Decimal::new(8, 2),
        max_pairwise_correlation: Some(0.65),
        skip_top_pct: 0.10,
        score_direction: ScoreDirection::Ascending,
        ..Default::default()
    };
    let policy =
        MarketRegimePolicy::quality_mixed_orthogonal_risk_memory_router_v3("000300.SH");

    let mixed = policy.apply(&base, MarketRegime::Mixed);
    let bull = policy.apply(&base, MarketRegime::Bull);

    assert_eq!(mixed.max_gross_exposure, 0.35);
    assert_eq!(mixed.max_position_pct, Decimal::new(8, 2));
    assert_eq!(mixed.max_pairwise_correlation, Some(0.65));
    assert_eq!(bull.max_gross_exposure, 0.35);
}

#[test]
fn quality_state_sharpe_bridge_router_preserves_bx_return_sleeves_with_tighter_stress_risk() {
    let base = SignalConfig {
        combo_name: "phase7_financial_quality_v1".to_string(),
        version: "1.0.0".to_string(),
        top_n: 20,
        rebalance_freq_days: 60,
        max_gross_exposure: 1.0,
        max_position_pct: Decimal::new(15, 2),
        max_pairwise_correlation: Some(0.75),
        skip_top_pct: 0.10,
        score_direction: ScoreDirection::Ascending,
        ..Default::default()
    };
    let policy = MarketRegimePolicy::quality_state_sharpe_bridge_router_v1("000300.SH");

    let bull = policy.apply(&base, MarketRegime::Bull);
    let bear = policy.apply(&base, MarketRegime::Bear);
    let high_volatility = policy.apply(&base, MarketRegime::HighVolatility);
    let mixed = policy.apply(&base, MarketRegime::Mixed);
    let bear_sleeve = bear.portfolio_sleeve.expect("bear event-window sleeve");
    let high_volatility_sleeve = high_volatility
        .portfolio_sleeve
        .expect("high-volatility low-risk sleeve");
    let mixed_sleeve = mixed.portfolio_sleeve.expect("mixed value/recovery sleeve");

    assert_eq!(bull.max_gross_exposure, 1.0);
    assert_eq!(bear_sleeve.combo_name, "phase7_event_window_earnings_v1");
    assert_eq!(
        high_volatility_sleeve.combo_name,
        "phase7_price_volume_expanded_v1"
    );
    assert_eq!(
        mixed_sleeve.combo_name,
        "phase7_quality_value_recovery_confirm_v1"
    );
    assert_eq!(bear.max_gross_exposure, 0.70);
    assert_eq!(high_volatility.max_gross_exposure, 0.56);
    assert_eq!(mixed.max_gross_exposure, 0.96);
    assert_eq!(mixed.max_position_pct, Decimal::new(13, 2));
    assert_eq!(mixed.max_pairwise_correlation, Some(0.70));
}

#[test]
fn quality_nonlinear_alpha_router_uses_distinct_state_alpha_without_handpicked_dates() {
    let base = SignalConfig {
        combo_name: "phase7_financial_quality_v1".to_string(),
        version: "1.0.0".to_string(),
        top_n: 20,
        rebalance_freq_days: 60,
        max_gross_exposure: 1.0,
        max_position_pct: Decimal::new(15, 2),
        max_pairwise_correlation: Some(0.75),
        skip_top_pct: 0.10,
        score_direction: ScoreDirection::Ascending,
        ..Default::default()
    };
    let policy = MarketRegimePolicy::quality_nonlinear_alpha_router_v1("000300.SH");

    let bull = policy.apply(&base, MarketRegime::Bull);
    let bear = policy.apply(&base, MarketRegime::Bear);
    let mixed = policy.apply(&base, MarketRegime::Mixed);
    let high_volatility = policy.apply(&base, MarketRegime::HighVolatility);

    let bull_sleeve = bull
        .portfolio_sleeve
        .expect("bull valuation/recovery sleeve");
    let bear_sleeve = bear.portfolio_sleeve.expect("bear event sleeve");
    let bear_overlay = bear.score_overlay.expect("bear valuation overlay");
    let mixed_sleeve = mixed
        .portfolio_sleeve
        .expect("mixed residual confirmation sleeve");
    let mixed_overlay = mixed.score_overlay.expect("mixed quality/recovery overlay");
    let high_vol_sleeve = high_volatility
        .portfolio_sleeve
        .expect("high-volatility low-risk sleeve");

    assert_eq!(
        bull_sleeve.combo_name,
        "phase7_quality_value_recovery_event_confirm_v1"
    );
    assert_eq!(bull_sleeve.score_direction, ScoreDirection::Descending);
    assert_eq!(bear_sleeve.combo_name, "phase7_event_window_earnings_v1");
    assert_eq!(bear_overlay.combo_name, "phase7_valuation_v1");
    assert_eq!(
        mixed_sleeve.combo_name,
        "phase7_quality_residual_confirm_10pct_v1"
    );
    assert_eq!(
        mixed_overlay.combo_name,
        "phase7_quality_value_recovery_confirm_v1"
    );
    assert_eq!(mixed.max_gross_exposure, 0.98);
    assert_eq!(mixed.max_position_pct, Decimal::new(14, 2));
    assert_eq!(mixed.max_pairwise_correlation, Some(0.72));
    assert_eq!(
        high_vol_sleeve.combo_name,
        "phase7_price_volume_expanded_v1"
    );
    assert_eq!(high_volatility.max_gross_exposure, 0.60);
}

#[test]
fn quality_nonlinear_alpha_risk_memory_relaxed_routers_bridge_return_without_date_rules() {
    let base = SignalConfig {
        combo_name: "phase7_financial_quality_v1".to_string(),
        version: "1.0.0".to_string(),
        top_n: 20,
        rebalance_freq_days: 60,
        max_gross_exposure: 1.0,
        max_position_pct: Decimal::new(15, 2),
        max_pairwise_correlation: Some(0.75),
        skip_top_pct: 0.10,
        score_direction: ScoreDirection::Ascending,
        ..Default::default()
    };
    let strict = MarketRegimePolicy::quality_nonlinear_alpha_risk_memory_router_v1("000300.SH");
    let relaxed =
        MarketRegimePolicy::quality_nonlinear_alpha_risk_memory_router_v2("000300.SH");
    let overlay_relaxed =
        MarketRegimePolicy::quality_nonlinear_alpha_risk_memory_router_v3("000300.SH");

    let strict_mixed = strict.apply(&base, MarketRegime::Mixed);
    let relaxed_mixed = relaxed.apply(&base, MarketRegime::Mixed);
    let overlay_mixed = overlay_relaxed.apply(&base, MarketRegime::Mixed);

    assert_eq!(strict_mixed.max_gross_exposure, 0.94);
    assert_eq!(strict_mixed.max_position_pct, Decimal::new(13, 2));
    assert_eq!(strict_mixed.max_pairwise_correlation, Some(0.70));
    assert_eq!(relaxed_mixed.max_gross_exposure, 0.97);
    assert_eq!(relaxed_mixed.max_position_pct, Decimal::new(14, 2));
    assert_eq!(relaxed_mixed.max_pairwise_correlation, Some(0.72));
    assert_eq!(overlay_mixed.max_gross_exposure, 0.98);
    assert_eq!(overlay_mixed.max_pairwise_correlation, Some(0.75));
    assert!(relaxed_mixed.portfolio_sleeve.is_some());
    assert!(overlay_mixed.score_overlay.is_some());
}

#[test]
fn quality_frontier_regime_bridge_keeps_return_sleeves_and_v14_mixed_risk_memory() {
    let base = SignalConfig {
        combo_name: "phase7_financial_quality_v1".to_string(),
        version: "1.0.0".to_string(),
        top_n: 20,
        rebalance_freq_days: 60,
        max_gross_exposure: 1.0,
        max_position_pct: Decimal::new(15, 2),
        max_pairwise_correlation: Some(0.75),
        skip_top_pct: 0.10,
        score_direction: ScoreDirection::Ascending,
        ..Default::default()
    };
    let policy = MarketRegimePolicy::quality_frontier_regime_bridge_router_v1("000300.SH");

    let bull = policy.apply(&base, MarketRegime::Bull);
    let bear = policy.apply(&base, MarketRegime::Bear);
    let high_volatility = policy.apply(&base, MarketRegime::HighVolatility);
    let mixed = policy.apply(&base, MarketRegime::Mixed);
    let sideways = policy.apply(&base, MarketRegime::Sideways);
    let bear_sleeve = bear.portfolio_sleeve.expect("bear event sleeve");
    let high_vol_sleeve = high_volatility
        .portfolio_sleeve
        .expect("high-volatility price/volume sleeve");
    let mixed_sleeve = mixed.portfolio_sleeve.expect("mixed value/recovery sleeve");
    let sideways_sleeve = sideways
        .portfolio_sleeve
        .expect("sideways valuation sleeve");

    assert_eq!(bull.max_gross_exposure, 1.0);
    assert_eq!(bear_sleeve.combo_name, "phase7_event_window_earnings_v1");
    assert_eq!(
        high_vol_sleeve.combo_name,
        "phase7_price_volume_expanded_v1"
    );
    assert_eq!(
        mixed_sleeve.combo_name,
        "phase7_quality_value_recovery_confirm_v1"
    );
    assert_eq!(sideways_sleeve.combo_name, "phase7_valuation_v1");
    assert_eq!(bear.max_gross_exposure, 0.74);
    assert_eq!(high_volatility.max_gross_exposure, 0.60);
    assert_eq!(mixed.max_gross_exposure, 1.0);
    assert_eq!(mixed.max_position_pct, Decimal::new(13, 2));
    assert_eq!(mixed.max_pairwise_correlation, Some(0.70));
}

#[test]
fn quality_frontier_regime_bridge_decomposition_splits_stress_and_mixed_risk_axes() {
    let base = SignalConfig {
        combo_name: "phase7_financial_quality_v1".to_string(),
        version: "1.0.0".to_string(),
        top_n: 20,
        rebalance_freq_days: 60,
        max_gross_exposure: 1.0,
        max_position_pct: Decimal::new(15, 2),
        max_pairwise_correlation: Some(0.75),
        skip_top_pct: 0.10,
        score_direction: ScoreDirection::Ascending,
        ..Default::default()
    };

    let stress_lift_strict_mixed =
        MarketRegimePolicy::quality_frontier_regime_bridge_router_v4("000300.SH");
    let base_stress_relaxed_mixed =
        MarketRegimePolicy::quality_frontier_regime_bridge_router_v5("000300.SH");
    let stress_lift_position_only =
        MarketRegimePolicy::quality_frontier_regime_bridge_router_v6("000300.SH");
    let stress_lift_correlation_only =
        MarketRegimePolicy::quality_frontier_regime_bridge_router_v7("000300.SH");

    let v4_bear = stress_lift_strict_mixed.apply(&base, MarketRegime::Bear);
    let v4_mixed = stress_lift_strict_mixed.apply(&base, MarketRegime::Mixed);
    let v5_bear = base_stress_relaxed_mixed.apply(&base, MarketRegime::Bear);
    let v5_mixed = base_stress_relaxed_mixed.apply(&base, MarketRegime::Mixed);
    let v6_mixed = stress_lift_position_only.apply(&base, MarketRegime::Mixed);
    let v7_mixed = stress_lift_correlation_only.apply(&base, MarketRegime::Mixed);

    assert_eq!(v4_bear.max_gross_exposure, 0.76);
    assert_eq!(v4_mixed.max_position_pct, Decimal::new(13, 2));
    assert_eq!(v4_mixed.max_pairwise_correlation, Some(0.70));
    assert_eq!(v5_bear.max_gross_exposure, 0.74);
    assert_eq!(v5_mixed.max_position_pct, Decimal::new(14, 2));
    assert_eq!(v5_mixed.max_pairwise_correlation, Some(0.72));
    assert_eq!(v6_mixed.max_position_pct, Decimal::new(14, 2));
    assert_eq!(v6_mixed.max_pairwise_correlation, Some(0.70));
    assert_eq!(v7_mixed.max_position_pct, Decimal::new(13, 2));
    assert_eq!(v7_mixed.max_pairwise_correlation, Some(0.72));
    assert!(v4_mixed.portfolio_sleeve.is_some());
    assert!(v5_mixed.portfolio_sleeve.is_some());
}

#[test]
fn rebalance_smoothing_keeps_small_changes_and_partially_moves_large_ones() {
    let previous = HashMap::from([
        ("AAA".to_string(), Decimal::new(10, 2)),
        ("BBB".to_string(), Decimal::new(10, 2)),
    ]);
    let mut next = HashMap::from([
        ("AAA".to_string(), Decimal::new(14, 2)),
        ("BBB".to_string(), Decimal::new(6, 2)),
    ]);

    apply_rebalance_path_smoothing(&mut next, Some(&previous), 0.01, 0.50);

    assert_eq!(next.get("AAA"), Some(&Decimal::new(12, 2)));
    assert_eq!(next.get("BBB"), Some(&Decimal::new(8, 2)));
}

#[test]
fn rebalance_smoothing_leaves_small_deltas_unchanged() {
    let previous = HashMap::from([
        ("AAA".to_string(), Decimal::new(10, 2)),
        ("BBB".to_string(), Decimal::new(10, 2)),
    ]);
    let mut next = HashMap::from([
        ("AAA".to_string(), Decimal::new(105, 3)),
        ("BBB".to_string(), Decimal::new(95, 3)),
    ]);

    apply_rebalance_path_smoothing(&mut next, Some(&previous), 0.01, 0.50);

    assert_eq!(next.get("AAA"), Some(&Decimal::new(10, 2)));
    assert_eq!(next.get("BBB"), Some(&Decimal::new(10, 2)));
}

#[test]
fn impact_risk_budget_limits_aggregate_rebalance_turnover() {
    let previous = HashMap::from([
        ("OLD_A".to_string(), Decimal::new(50, 2)),
        ("OLD_B".to_string(), Decimal::new(50, 2)),
    ]);
    let mut next = HashMap::from([
        ("NEW_A".to_string(), Decimal::new(50, 2)),
        ("NEW_B".to_string(), Decimal::new(50, 2)),
    ]);

    apply_execution_impact_budget(
        &mut next,
        Some(&previous),
        ExecutionImpactBudgetProfile::Turnover20PctV1,
    );

    let mut symbols = previous
        .keys()
        .chain(next.keys())
        .cloned()
        .collect::<Vec<_>>();
    symbols.sort();
    symbols.dedup();
    let turnover = symbols.iter().fold(Decimal::ZERO, |acc, symbol| {
        let prev = previous.get(symbol).copied().unwrap_or_default();
        let target = next.get(symbol).copied().unwrap_or_default();
        acc + (target - prev).abs()
    });

    assert!(turnover <= Decimal::new(20, 2));
    assert!(next.get("OLD_A").copied().unwrap_or_default() > Decimal::new(40, 2));
    assert!(next.get("OLD_B").copied().unwrap_or_default() > Decimal::new(40, 2));
    assert!(next.get("NEW_A").copied().unwrap_or_default() <= Decimal::new(4, 2));
    assert!(next.get("NEW_B").copied().unwrap_or_default() <= Decimal::new(4, 2));
}

#[test]
fn regime_aware_factor_signals_use_dynamic_direction_and_exposure() {
    let d1 = NaiveDate::from_ymd_opt(2026, 1, 2).unwrap();
    let d2 = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
    let d3 = NaiveDate::from_ymd_opt(2026, 1, 6).unwrap();
    let trading_days = vec![d1, d2, d3];
    let mut scores_by_date = HashMap::new();
    scores_by_date.insert(d1, vec![("AAA".to_string(), 3.0), ("BBB".to_string(), 1.0)]);
    scores_by_date.insert(d2, vec![("AAA".to_string(), 3.0), ("BBB".to_string(), 1.0)]);
    let base = SignalConfig {
        top_n: 1,
        rebalance_freq_days: 1,
        max_position_pct: Decimal::ONE,
        max_gross_exposure: 1.0,
        score_direction: ScoreDirection::Descending,
        ..Default::default()
    };

    let signals = build_rebalance_factor_signals(
        &trading_days,
        &scores_by_date,
        &base,
        &HashMap::new(),
        &HashMap::new(),
        &HashMap::new(),
        |day, base| {
            if day == d3 {
                let mut defensive = base.clone();
                defensive.max_gross_exposure = 0.50;
                defensive.score_direction = ScoreDirection::Ascending;
                defensive
            } else {
                base.clone()
            }
        },
    )
    .expect("regime-aware signals");

    let bull_signal = signals.get(&d2).expect("bull signal");
    assert_eq!(bull_signal.target_weights.get("AAA"), Some(&Decimal::ONE));
    let bear_signal = signals.get(&d3).expect("bear signal");
    assert_eq!(
        bear_signal.target_weights.get("BBB"),
        Some(&Decimal::from_f64(0.50).unwrap())
    );
    assert!(!bear_signal.target_weights.contains_key("AAA"));
}

#[test]
fn regime_aware_factor_signals_can_switch_score_source_by_active_combo() {
    let d1 = NaiveDate::from_ymd_opt(2026, 1, 2).unwrap();
    let d2 = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
    let d3 = NaiveDate::from_ymd_opt(2026, 1, 6).unwrap();
    let trading_days = vec![d1, d2, d3];
    let quality_scores = HashMap::from([
        (
            d1,
            vec![
                ("QUALITY_WINNER".to_string(), 3.0),
                ("RESIDUAL_WINNER".to_string(), 1.0),
            ],
        ),
        (
            d2,
            vec![
                ("QUALITY_WINNER".to_string(), 3.0),
                ("RESIDUAL_WINNER".to_string(), 1.0),
            ],
        ),
    ]);
    let residual_scores = HashMap::from([
        (
            d1,
            vec![
                ("QUALITY_WINNER".to_string(), 1.0),
                ("RESIDUAL_WINNER".to_string(), 3.0),
            ],
        ),
        (
            d2,
            vec![
                ("QUALITY_WINNER".to_string(), 1.0),
                ("RESIDUAL_WINNER".to_string(), 3.0),
            ],
        ),
    ]);
    let base = SignalConfig {
        combo_name: "phase7_financial_quality_v1".to_string(),
        top_n: 1,
        rebalance_freq_days: 1,
        max_position_pct: Decimal::ONE,
        max_gross_exposure: 1.0,
        score_direction: ScoreDirection::Descending,
        ..Default::default()
    };

    let signals = build_rebalance_factor_signals_with_score_selector(
        &trading_days,
        &base,
        &HashMap::new(),
        &HashMap::new(),
        &HashMap::new(),
        |score_day, active_config| match active_config.combo_name.as_str() {
            "phase7_financial_quality_v1" => quality_scores.get(&score_day).cloned(),
            "phase7_industry_residual_quality_v1" => residual_scores.get(&score_day).cloned(),
            _ => None,
        },
        |day, base| {
            if day == d3 {
                let mut residual = base.clone();
                residual.combo_name = "phase7_industry_residual_quality_v1".to_string();
                residual
            } else {
                base.clone()
            }
        },
    )
    .expect("regime-aware alpha-routed signals");

    let quality_signal = signals.get(&d2).expect("quality signal");
    assert_eq!(
        quality_signal.target_weights.get("QUALITY_WINNER"),
        Some(&Decimal::ONE)
    );
    let residual_signal = signals.get(&d3).expect("residual signal");
    assert_eq!(
        residual_signal.target_weights.get("RESIDUAL_WINNER"),
        Some(&Decimal::ONE)
    );
    assert!(!residual_signal
        .target_weights
        .contains_key("QUALITY_WINNER"));
}

#[test]
fn quality_regime_alpha_overlay_keeps_quality_anchor_and_adds_small_stress_sleeve() {
    let base = SignalConfig {
        combo_name: "phase7_financial_quality_v1".to_string(),
        version: "1.0.0".to_string(),
        score_direction: ScoreDirection::Ascending,
        max_gross_exposure: 1.0,
        ..Default::default()
    };
    let policy = MarketRegimePolicy::quality_regime_alpha_overlay_value_10pct_v1("000300.SH");

    let bull = policy.apply(&base, MarketRegime::Bull);
    let bear = policy.apply(&base, MarketRegime::Bear);

    assert_eq!(bull.combo_name, "phase7_financial_quality_v1");
    assert!(bull.score_overlay.is_none());
    assert_eq!(bear.combo_name, "phase7_financial_quality_v1");
    assert_eq!(bear.score_direction, ScoreDirection::Ascending);
    assert_eq!(bear.max_gross_exposure, 0.72);
    let overlay = bear.score_overlay.expect("stress overlay");
    assert_eq!(overlay.combo_name, "phase7_valuation_v1");
    assert_eq!(overlay.version, "1.0.0");
    assert_eq!(overlay.score_direction, ScoreDirection::Descending);
    assert!((overlay.weight - 0.10).abs() < 1e-9);
}

#[test]
fn regime_score_selector_blends_overlay_scores_without_replacing_base_source() {
    let d1 = NaiveDate::from_ymd_opt(2026, 1, 2).unwrap();
    let mut base = SignalConfig {
        combo_name: "phase7_financial_quality_v1".to_string(),
        version: "1.0.0".to_string(),
        score_direction: ScoreDirection::Ascending,
        ..Default::default()
    };
    base.score_overlay = Some(FactorScoreOverlayConfig {
        combo_name: "phase7_valuation_v1".to_string(),
        version: "1.0.0".to_string(),
        weight: 0.25,
        score_direction: ScoreDirection::Descending,
    });

    let mut score_sources = HashMap::new();
    let mut quality_scores = HashMap::new();
    quality_scores.insert(
        d1,
        vec![
            ("QUALITY_BEST".to_string(), 1.0),
            ("VALUATION_BEST".to_string(), 2.0),
        ],
    );
    let mut valuation_scores = HashMap::new();
    valuation_scores.insert(
        d1,
        vec![
            ("QUALITY_BEST".to_string(), 1.0),
            ("VALUATION_BEST".to_string(), 5.0),
        ],
    );
    score_sources.insert(FactorScoreSourceKey::from_config(&base), quality_scores);
    let overlay_config = score_source_config_for_overlay(
        &base,
        base.score_overlay.as_ref().expect("overlay config"),
    );
    score_sources.insert(
        FactorScoreSourceKey::from_config(&overlay_config),
        valuation_scores,
    );

    let rows = score_rows_for_active_config(&score_sources, d1, &base).expect("blended rows");
    let mut sorted = rows.clone();
    sort_factor_scores(&mut sorted, base.score_direction);

    assert_eq!(sorted[0].0, "QUALITY_BEST");
    assert_eq!(sorted.len(), 2);
    assert_ne!(rows[0].1, 1.0);
}

#[test]
fn quality_regime_alpha_portfolio_sleeve_keeps_anchor_and_allocates_stress_sleeve() {
    let base = SignalConfig {
        combo_name: "phase7_financial_quality_v1".to_string(),
        version: "1.0.0".to_string(),
        score_direction: ScoreDirection::Ascending,
        max_gross_exposure: 1.0,
        ..Default::default()
    };
    let policy =
        MarketRegimePolicy::quality_regime_alpha_portfolio_sleeve_value_15pct_v1("000300.SH");

    let bull = policy.apply(&base, MarketRegime::Bull);
    let bear = policy.apply(&base, MarketRegime::Bear);

    assert_eq!(bull.combo_name, "phase7_financial_quality_v1");
    assert!(bull.portfolio_sleeve.is_none());
    assert_eq!(bear.combo_name, "phase7_financial_quality_v1");
    assert_eq!(bear.score_direction, ScoreDirection::Ascending);
    let sleeve = bear.portfolio_sleeve.expect("stress portfolio sleeve");
    assert_eq!(sleeve.combo_name, "phase7_valuation_v1");
    assert_eq!(sleeve.version, "1.0.0");
    assert_eq!(sleeve.score_direction, ScoreDirection::Descending);
    assert!((sleeve.weight - 0.15).abs() < 1e-9);
}

#[test]
fn quality_regime_alpha_portfolio_sleeve_can_allocate_low_risk_sleeve() {
    let base = SignalConfig {
        combo_name: "phase7_financial_quality_v1".to_string(),
        version: "1.0.0".to_string(),
        score_direction: ScoreDirection::Ascending,
        max_gross_exposure: 1.0,
        ..Default::default()
    };
    let policy =
        MarketRegimePolicy::quality_regime_alpha_portfolio_sleeve_lowrisk_15pct_v1("000300.SH");

    let bull = policy.apply(&base, MarketRegime::Bull);
    let bear = policy.apply(&base, MarketRegime::Bear);

    assert!(bull.portfolio_sleeve.is_none());
    let sleeve = bear.portfolio_sleeve.expect("low-risk portfolio sleeve");
    assert_eq!(sleeve.combo_name, "phase7_price_volume_expanded_v1");
    assert_eq!(sleeve.version, "1.0.0");
    assert_eq!(sleeve.score_direction, ScoreDirection::Ascending);
    assert!((sleeve.weight - 0.15).abs() < 1e-9);
}

#[test]
fn quality_regime_alpha_portfolio_sleeve_can_allocate_event_sleeve() {
    let base = SignalConfig {
        combo_name: "phase7_financial_quality_v1".to_string(),
        version: "1.0.0".to_string(),
        score_direction: ScoreDirection::Ascending,
        max_gross_exposure: 1.0,
        ..Default::default()
    };
    let policy =
        MarketRegimePolicy::quality_regime_alpha_portfolio_sleeve_event_window_10pct_v1(
            "000300.SH",
        );

    let bull = policy.apply(&base, MarketRegime::Bull);
    let bear = policy.apply(&base, MarketRegime::Bear);
    let high_volatility = policy.apply(&base, MarketRegime::HighVolatility);

    assert!(bull.portfolio_sleeve.is_none());
    let bear_sleeve = bear.portfolio_sleeve.expect("event portfolio sleeve");
    assert_eq!(bear_sleeve.combo_name, "phase7_event_window_earnings_v1");
    assert_eq!(bear_sleeve.version, "1.0.0");
    assert_eq!(bear_sleeve.score_direction, ScoreDirection::Descending);
    assert!((bear_sleeve.weight - 0.10).abs() < 1e-9);
    assert_eq!(
        high_volatility
            .portfolio_sleeve
            .expect("high-vol event sleeve")
            .combo_name,
        "phase7_event_window_earnings_v1"
    );
}

#[test]
fn quality_regime_alpha_portfolio_sleeve_can_allocate_fractional_event_window_sleeve() {
    let base = SignalConfig {
        combo_name: "phase7_financial_quality_v1".to_string(),
        version: "1.0.0".to_string(),
        score_direction: ScoreDirection::Ascending,
        max_gross_exposure: 1.0,
        ..Default::default()
    };
    let policy =
        MarketRegimePolicy::quality_regime_alpha_portfolio_sleeve_event_window_125pct_v1(
            "000300.SH",
        );

    let bear = policy.apply(&base, MarketRegime::Bear);

    let sleeve = bear.portfolio_sleeve.expect("event window sleeve");
    assert_eq!(sleeve.combo_name, "phase7_event_window_earnings_v1");
    assert_eq!(sleeve.score_direction, ScoreDirection::Descending);
    assert!((sleeve.weight - 0.125).abs() < 1e-9);
}

#[test]
fn quality_regime_alpha_portfolio_sleeve_can_allocate_upper_bound_event_window_sleeve() {
    let base = SignalConfig {
        combo_name: "phase7_financial_quality_v1".to_string(),
        version: "1.0.0".to_string(),
        score_direction: ScoreDirection::Ascending,
        max_gross_exposure: 1.0,
        ..Default::default()
    };
    let policy =
        MarketRegimePolicy::quality_regime_alpha_portfolio_sleeve_event_window_15pct_v1(
            "000300.SH",
        );

    let bear = policy.apply(&base, MarketRegime::Bear);

    let sleeve = bear.portfolio_sleeve.expect("event window sleeve");
    assert_eq!(sleeve.combo_name, "phase7_event_window_earnings_v1");
    assert_eq!(sleeve.score_direction, ScoreDirection::Descending);
    assert!((sleeve.weight - 0.15).abs() < 1e-9);
}

#[test]
fn quality_all_regime_event_window_sleeve_allocates_event_flow_in_every_state() {
    let base = SignalConfig {
        combo_name: "phase7_financial_quality_v1".to_string(),
        version: "1.0.0".to_string(),
        score_direction: ScoreDirection::Ascending,
        max_gross_exposure: 1.0,
        ..Default::default()
    };
    let policy =
        MarketRegimePolicy::quality_all_regime_event_window_sleeve_10pct_v1("000300.SH");

    for regime in [
        MarketRegime::Bull,
        MarketRegime::Bear,
        MarketRegime::HighVolatility,
        MarketRegime::Sideways,
        MarketRegime::Mixed,
    ] {
        let active = policy.apply(&base, regime);
        let sleeve = active.portfolio_sleeve.expect("all-regime event sleeve");
        assert_eq!(sleeve.combo_name, "phase7_event_window_earnings_v1");
        assert_eq!(sleeve.version, "1.0.0");
        assert_eq!(sleeve.score_direction, ScoreDirection::Descending);
        assert!((sleeve.weight - 0.10).abs() < 1e-9);
    }
}

#[test]
fn quality_regime_alpha_portfolio_sleeve_can_route_event_window_by_regime() {
    let bear_only =
        MarketRegimePolicy::quality_regime_alpha_portfolio_sleeve_event_window_15pct_bear_only_v1(
            "000300.SH",
        );
    let highvol_only =
        MarketRegimePolicy::quality_regime_alpha_portfolio_sleeve_event_window_15pct_highvol_only_v1(
            "000300.SH",
        );

    assert!(bear_only
        .rules
        .get(&MarketRegime::Bear)
        .and_then(|rule| rule.portfolio_sleeve.as_ref())
        .is_some());
    assert!(bear_only
        .rules
        .get(&MarketRegime::HighVolatility)
        .and_then(|rule| rule.portfolio_sleeve.as_ref())
        .is_none());
    assert!(highvol_only
        .rules
        .get(&MarketRegime::Bear)
        .and_then(|rule| rule.portfolio_sleeve.as_ref())
        .is_none());
    assert!(highvol_only
        .rules
        .get(&MarketRegime::HighVolatility)
        .and_then(|rule| rule.portfolio_sleeve.as_ref())
        .is_some());
}

#[test]
fn quality_regime_alpha_portfolio_sleeve_can_select_event_window_decay_variant() {
    let short_decay =
        MarketRegimePolicy::quality_regime_alpha_portfolio_sleeve_event_window_15pct_10d_v1(
            "000300.SH",
        );
    let long_decay =
        MarketRegimePolicy::quality_regime_alpha_portfolio_sleeve_event_window_15pct_40d_v1(
            "000300.SH",
        );

    let short_sleeve = short_decay
        .rules
        .get(&MarketRegime::Bear)
        .and_then(|rule| rule.portfolio_sleeve.as_ref())
        .expect("short-decay event sleeve");
    let long_sleeve = long_decay
        .rules
        .get(&MarketRegime::Bear)
        .and_then(|rule| rule.portfolio_sleeve.as_ref())
        .expect("long-decay event sleeve");

    assert_eq!(
        short_sleeve.combo_name,
        "phase7_event_window_earnings_10d_v1"
    );
    assert_eq!(short_sleeve.score_direction, ScoreDirection::Descending);
    assert!((short_sleeve.weight - 0.15).abs() < 1e-9);
    assert_eq!(
        long_sleeve.combo_name,
        "phase7_event_window_earnings_40d_v1"
    );
    assert_eq!(long_sleeve.score_direction, ScoreDirection::Descending);
    assert!((long_sleeve.weight - 0.15).abs() < 1e-9);
}

#[test]
fn quality_regime_alpha_portfolio_sleeve_can_select_event_quality_segment() {
    let surprise =
        MarketRegimePolicy::quality_regime_alpha_portfolio_sleeve_event_surprise_15pct_v1(
            "000300.SH",
        );
    let confirm =
        MarketRegimePolicy::quality_regime_alpha_portfolio_sleeve_event_confirm_15pct_v1(
            "000300.SH",
        );

    let surprise_sleeve = surprise
        .rules
        .get(&MarketRegime::Bear)
        .and_then(|rule| rule.portfolio_sleeve.as_ref())
        .expect("event-surprise sleeve");
    let confirm_sleeve = confirm
        .rules
        .get(&MarketRegime::Bear)
        .and_then(|rule| rule.portfolio_sleeve.as_ref())
        .expect("event-confirm sleeve");

    assert_eq!(surprise_sleeve.combo_name, "phase7_event_surprise_v1");
    assert_eq!(surprise_sleeve.score_direction, ScoreDirection::Descending);
    assert!((surprise_sleeve.weight - 0.15).abs() < 1e-9);
    assert_eq!(confirm_sleeve.combo_name, "phase7_event_earnings_v1");
    assert_eq!(confirm_sleeve.score_direction, ScoreDirection::Descending);
    assert!((confirm_sleeve.weight - 0.15).abs() < 1e-9);
}

#[test]
fn symbol_return_history_query_uses_daily_bar_pct_change_without_adjustment_view() {
    assert!(SYMBOL_RETURN_HISTORY_SQL.contains("pct_change"));
    assert!(SYMBOL_RETURN_HISTORY_SQL.contains("market_stock_daily_bar"));
    assert!(!SYMBOL_RETURN_HISTORY_SQL.contains("market_stock_daily_bar_adj"));
    assert!(!SYMBOL_RETURN_HISTORY_SQL.contains("market_adjustment_factor"));
}

#[test]
fn pct_change_decimal_is_already_fractional_daily_return() {
    let positive = daily_return_from_pct_change(Decimal::new(2352, 6))
        .expect("positive fractional return");
    let negative = daily_return_from_pct_change(Decimal::new(-11612, 6))
        .expect("negative fractional return");

    assert!((positive - 0.002352).abs() < 1e-12);
    assert!((negative + 0.011612).abs() < 1e-12);
}

#[test]
fn regime_aware_factor_signals_can_allocate_portfolio_sleeve_weights() {
    let d1 = NaiveDate::from_ymd_opt(2026, 1, 2).unwrap();
    let d2 = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
    let d3 = NaiveDate::from_ymd_opt(2026, 1, 6).unwrap();
    let trading_days = vec![d1, d2, d3];
    let quality_scores = HashMap::from([
        (
            d1,
            vec![
                ("QUALITY_WINNER".to_string(), 1.0),
                ("SLEEVE_WINNER".to_string(), 3.0),
            ],
        ),
        (
            d2,
            vec![
                ("QUALITY_WINNER".to_string(), 1.0),
                ("SLEEVE_WINNER".to_string(), 3.0),
            ],
        ),
    ]);
    let sleeve_scores = HashMap::from([
        (
            d1,
            vec![
                ("QUALITY_WINNER".to_string(), 1.0),
                ("SLEEVE_WINNER".to_string(), 3.0),
            ],
        ),
        (
            d2,
            vec![
                ("QUALITY_WINNER".to_string(), 1.0),
                ("SLEEVE_WINNER".to_string(), 3.0),
            ],
        ),
    ]);
    let base = SignalConfig {
        combo_name: "phase7_financial_quality_v1".to_string(),
        version: "1.0.0".to_string(),
        top_n: 1,
        rebalance_freq_days: 1,
        max_position_pct: Decimal::ONE,
        max_gross_exposure: 1.0,
        score_direction: ScoreDirection::Ascending,
        ..Default::default()
    };

    let signals = build_rebalance_factor_signals_with_score_selector(
        &trading_days,
        &base,
        &HashMap::new(),
        &HashMap::new(),
        &HashMap::new(),
        |score_day, active_config| match active_config.combo_name.as_str() {
            "phase7_financial_quality_v1" => quality_scores.get(&score_day).cloned(),
            "phase7_valuation_v1" => sleeve_scores.get(&score_day).cloned(),
            _ => None,
        },
        |day, base| {
            if day == d3 {
                let mut stressed = base.clone();
                stressed.portfolio_sleeve = Some(FactorPortfolioSleeveConfig {
                    combo_name: "phase7_valuation_v1".to_string(),
                    version: "1.0.0".to_string(),
                    weight: 0.25,
                    score_direction: ScoreDirection::Descending,
                });
                stressed
            } else {
                base.clone()
            }
        },
    )
    .expect("portfolio-sleeve signals");

    let normal_signal = signals.get(&d2).expect("normal signal");
    assert_eq!(
        normal_signal.target_weights.get("QUALITY_WINNER"),
        Some(&Decimal::ONE)
    );
    let sleeve_signal = signals.get(&d3).expect("sleeve signal");
    assert_eq!(
        sleeve_signal.target_weights.get("QUALITY_WINNER"),
        Some(&Decimal::new(75, 2))
    );
    assert_eq!(
        sleeve_signal.target_weights.get("SLEEVE_WINNER"),
        Some(&Decimal::new(25, 2))
    );
}

fn dated_returns(values: &[f64]) -> Vec<(NaiveDate, f64)> {
    let start = NaiveDate::from_ymd_opt(2026, 1, 2).unwrap();
    values
        .iter()
        .enumerate()
        .map(|(idx, value)| (start + chrono::Duration::days(idx as i64), *value))
        .collect()
}
