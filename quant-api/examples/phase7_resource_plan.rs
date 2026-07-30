use quant_api::discovery::strategy_discovery::LocalResourcePlan;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let plan = LocalResourcePlan::local_mac();
    println!("{}", serde_json::to_string_pretty(&plan)?);
    Ok(())
}
