use quant_backtest::db_perf_baseline::{parse_db_perf_args, run_db_perf_baseline};

#[tokio::main]
async fn main() {
    let config = match parse_db_perf_args(std::env::args()) {
        Ok(config) => config,
        Err(message) => {
            eprintln!("{}", message);
            std::process::exit(2);
        }
    };

    match run_db_perf_baseline(config).await {
        Ok(report) => println!("{}", serde_json::to_string_pretty(&report).unwrap()),
        Err(error) => {
            eprintln!("{}", error);
            std::process::exit(1);
        }
    }
}
