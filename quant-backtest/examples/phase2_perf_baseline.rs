use quant_backtest::perf_baseline::{parse_phase2_perf_args, run_phase2_smoke_baseline};

fn main() {
    let config = match parse_phase2_perf_args(std::env::args()) {
        Ok(config) => config,
        Err(message) => {
            eprintln!("{}", message);
            std::process::exit(2);
        }
    };
    let report = run_phase2_smoke_baseline(config);
    println!("{}", serde_json::to_string_pretty(&report).unwrap());
}
