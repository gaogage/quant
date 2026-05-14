use quant_backtest::perf_baseline::{
    parse_phase2_perf_args, run_phase2_smoke_baseline, Phase2PerfBaselineConfig,
};

#[test]
fn phase2_smoke_baseline_reports_fixed_workload_dimensions() {
    let report = run_phase2_smoke_baseline(Phase2PerfBaselineConfig {
        trading_days: 40,
        symbols: 25,
        rebalance_every_n_days: 10,
        basket_size: 8,
        ..Phase2PerfBaselineConfig::default()
    });

    assert_eq!(report.trading_days, 40);
    assert_eq!(report.symbols, 25);
    assert_eq!(report.rebalance_every_n_days, 10);
    assert_eq!(report.basket_size, 8);
    assert_eq!(report.equity_points, 40);
    assert!(report.signal_count > 0);
    assert!(report.trade_count > 0);
    assert!(report.total_return.to_string().parse::<rust_decimal::Decimal>().is_ok());
}

#[test]
fn phase2_perf_args_override_default_workload_dimensions() {
    let config = parse_phase2_perf_args([
        "phase2_perf_baseline",
        "--days",
        "120",
        "--symbols",
        "80",
        "--rebalance-days",
        "15",
        "--basket-size",
        "20",
    ])
    .expect("valid arguments should parse");

    assert_eq!(config.trading_days, 120);
    assert_eq!(config.symbols, 80);
    assert_eq!(config.rebalance_every_n_days, 15);
    assert_eq!(config.basket_size, 20);
}
