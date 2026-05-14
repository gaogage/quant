use quant_api::query_perf_baseline::{parse_query_perf_args, run_query_perf_baseline};

#[tokio::main]
async fn main() {
    let config = match parse_query_perf_args(std::env::args()) {
        Ok(config) => config,
        Err(message) => {
            eprintln!("{}", message);
            std::process::exit(2);
        }
    };

    match run_query_perf_baseline(config).await {
        Ok(report) => println!("{}", serde_json::to_string_pretty(&report).unwrap()),
        Err(error) => {
            eprintln!("{}", error);
            std::process::exit(1);
        }
    }
}
