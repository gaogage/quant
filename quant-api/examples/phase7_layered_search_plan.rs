use quant_api::discovery::phase7::{build_layered_search_plan, LayeredSearchConfig, LocalResourcePlan};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut resource_plan = LocalResourcePlan::local_mac();
    if let Some(max_trials) = std::env::args().nth(1) {
        resource_plan.max_trials = max_trials.parse::<usize>()?;
    }

    let config = LayeredSearchConfig::local_professional_default();
    let plan = build_layered_search_plan(&config, &resource_plan);
    println!("{}", serde_json::to_string_pretty(&plan)?);
    Ok(())
}
