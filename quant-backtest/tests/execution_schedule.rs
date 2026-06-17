use chrono::NaiveDate;
use quant_backtest::engine::{BacktestConfig, ExecutionPrice, ExecutionTiming};

#[test]
fn default_backtest_config_uses_next_open_execution() {
    let config = BacktestConfig::default();

    assert_eq!(config.execution_timing, ExecutionTiming::NextOpen);
    assert_eq!(config.execution_price, ExecutionPrice::Open);
    assert_eq!(config.strategy_version_id, "debug-strategy");
    assert_eq!(config.data_version_id, "debug-data");
    assert_eq!(config.symbols, Vec::<String>::new());
    assert_eq!(
        config.start_date,
        NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()
    );
}

use quant_backtest::engine::StrategySignal;
use quant_backtest::runner::schedule_signals_for_execution;
use rust_decimal::Decimal;
use std::collections::HashMap;

#[test]
fn next_open_executes_signal_on_next_trading_day() {
    let days = vec![
        NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
        NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
        NaiveDate::from_ymd_opt(2024, 1, 4).unwrap(),
    ];
    let signal_day = days[0];
    let mut weights = HashMap::new();
    weights.insert("000001.SZ".to_string(), Decimal::new(1, 1));
    let signal = StrategySignal {
        date: signal_day,
        target_weights: weights,
    };
    let mut signals = HashMap::new();
    signals.insert(signal_day, signal);

    let scheduled = schedule_signals_for_execution(&days, &signals);

    assert!(scheduled.get(&signal_day).is_none());
    assert_eq!(scheduled.get(&days[1]).unwrap().date, signal_day);
}

#[test]
fn signal_on_last_trading_day_is_not_executed_without_next_day() {
    let days = vec![
        NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
        NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
    ];
    let signal_day = days[1];
    let mut weights = HashMap::new();
    weights.insert("000001.SZ".to_string(), Decimal::new(1, 1));
    let signal = StrategySignal {
        date: signal_day,
        target_weights: weights,
    };
    let mut signals = HashMap::new();
    signals.insert(signal_day, signal);

    let scheduled = schedule_signals_for_execution(&days, &signals);

    assert!(scheduled.is_empty());
}

use quant_backtest::runner::{backtest_task_parameters, BacktestTaskInsert};

#[test]
fn task_insert_uses_config_context_not_placeholders() {
    let config = BacktestConfig {
        strategy_version_id: "strategy-v1".into(),
        data_version_id: "data-v1".into(),
        symbols: vec!["000001.SZ".into(), "600000.SH".into()],
        rebalance_frequency: "monthly".into(),
        ..BacktestConfig::default()
    };

    let insert = BacktestTaskInsert::from_config("bt-1", &config);

    assert_eq!(insert.strategy_version_id, "strategy-v1");
    assert_eq!(insert.data_version_id, "data-v1");
    assert_eq!(insert.symbols, vec!["000001.SZ", "600000.SH"]);
    assert_eq!(insert.rebalance_frequency, "monthly");
}

#[test]
fn task_parameters_preserve_request_snapshot_and_fill_core_ids() {
    let mut config = BacktestConfig {
        research_dataset_id: Some("research-v1".into()),
        feature_set_version_id: Some("features-v1".into()),
        prediction_set_id: Some("prediction-v1".into()),
        portfolio_policy_id: Some("policy-v1".into()),
        ..BacktestConfig::default()
    };
    config.parameters = serde_json::json!({
        "request_type": "factor_backtest",
        "combo_name": "full_pit_icir_37f",
        "prediction_set_id": "request-prediction-v1",
        "score_direction": "ascending"
    });

    let parameters = backtest_task_parameters(&config);

    assert_eq!(parameters["request_type"], "factor_backtest");
    assert_eq!(parameters["combo_name"], "full_pit_icir_37f");
    assert_eq!(parameters["score_direction"], "ascending");
    assert_eq!(parameters["prediction_set_id"], "request-prediction-v1");
    assert_eq!(parameters["research_dataset_id"], "research-v1");
    assert_eq!(parameters["feature_set_version_id"], "features-v1");
    assert_eq!(parameters["portfolio_policy_id"], "policy-v1");
}

#[test]
fn task_parameters_replace_non_object_snapshot_with_core_ids() {
    let mut config = BacktestConfig {
        prediction_set_id: Some("prediction-v1".into()),
        ..BacktestConfig::default()
    };
    config.parameters = serde_json::json!("invalid-snapshot");

    let parameters = backtest_task_parameters(&config);

    assert!(parameters.is_object());
    assert_eq!(parameters["prediction_set_id"], "prediction-v1");
}

use quant_backtest::engine::{BacktestEngine, MarketDay};
use std::collections::HashSet;

#[test]
fn default_execution_uses_execution_day_open_price() {
    let mut config = BacktestConfig::default();
    config.max_position_pct = Decimal::new(101, 2);
    let mut engine = BacktestEngine::new(config);
    let execution_day = NaiveDate::from_ymd_opt(2024, 1, 3).unwrap();
    let mut weights = HashMap::new();
    weights.insert("000001.SZ".to_string(), Decimal::new(95, 2));
    let signal = StrategySignal {
        date: NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
        target_weights: weights,
    };
    let market = MarketDay {
        date: execution_day,
        open: HashMap::from([("000001.SZ".to_string(), Decimal::new(9, 0))]),
        close: HashMap::from([("000001.SZ".to_string(), Decimal::new(10, 0))]),
        pre_close: HashMap::from([("000001.SZ".to_string(), Decimal::new(10, 0))]),
        amount: HashMap::from([("000001.SZ".to_string(), Decimal::new(100000000, 0))]),
        suspended: HashSet::new(),
        up_limit: HashMap::from([("000001.SZ".to_string(), Decimal::new(11, 0))]),
        down_limit: HashMap::from([("000001.SZ".to_string(), Decimal::new(9, 0))]),
        benchmark_close: Decimal::ONE,
        benchmark_pre_close: Decimal::ONE,
    };

    engine.process_day(&market, Some(&signal));
    let output = engine.finalize();

    assert!(!output.trades.is_empty());
    assert_eq!(output.trades[0].price, Decimal::new(9, 0));
    assert!(!output.trades[0].quantity.is_zero());
}
