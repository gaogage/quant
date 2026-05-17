use chrono::NaiveDate;
use quant_backtest::db_perf_baseline::{parse_db_perf_args, DbPerfBaselineConfig};

#[test]
fn db_perf_args_override_default_database_workload_dimensions() {
    let config = parse_db_perf_args([
        "db_perf_baseline",
        "--database-url",
        "postgres://gaocheng@localhost/quant",
        "--days",
        "120",
        "--symbols",
        "80",
        "--rebalance-days",
        "15",
        "--basket-size",
        "20",
        "--start-date",
        "20240102",
        "--end-date",
        "20240630",
        "--task-prefix",
        "perf-test",
    ])
    .expect("valid arguments should parse");

    assert_eq!(config.database_url, "postgres://gaocheng@localhost/quant");
    assert_eq!(config.trading_days, 120);
    assert_eq!(config.symbols, 80);
    assert_eq!(config.rebalance_every_n_days, 15);
    assert_eq!(config.basket_size, 20);
    assert_eq!(
        config.start_date,
        NaiveDate::from_ymd_opt(2024, 1, 2).unwrap()
    );
    assert_eq!(
        config.end_date,
        NaiveDate::from_ymd_opt(2024, 6, 30).unwrap()
    );
    assert_eq!(config.task_prefix, "perf-test");
}

#[test]
fn default_db_perf_task_prefix_is_clearly_isolated() {
    let config = DbPerfBaselineConfig::default();

    assert!(config.task_prefix.starts_with("perf-db-"));
    assert_eq!(config.benchmark, "000300.SH");
    assert!(config.basket_size <= config.symbols);
}
