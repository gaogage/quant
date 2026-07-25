use quant_api::discovery::phase7::LocalResourcePlan;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let plan = LocalResourcePlan::local_mac();
    println!("{}", serde_json::to_string_pretty(&plan)?);
    Ok(())
}
