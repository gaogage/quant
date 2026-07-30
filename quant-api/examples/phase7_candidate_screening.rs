use quant_api::discovery::strategy_discovery::{screen_optimization_results, CandidateTargets};
use serde_json::{json, Value};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let input_path = std::env::args().nth(1).ok_or(
        "usage: cargo run -p quant-common --example phase7_candidate_screening -- <results.json>",
    )?;
    let raw = std::fs::read_to_string(input_path)?;
    let results: Vec<Value> = serde_json::from_str(&raw)?;
    let rows = screen_optimization_results(&results, &CandidateTargets::default());
    let professional_count = rows
        .iter()
        .filter(|row| {
            matches!(
                row.candidate_type,
                quant_api::discovery::strategy_discovery::CandidateType::Professional
            )
        })
        .count();
    let output = json!({
        "targets": CandidateTargets::default(),
        "candidate_count": rows.len(),
        "professional_count": professional_count,
        "rows": rows,
    });
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}
