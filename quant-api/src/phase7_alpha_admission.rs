use serde_json::{json, Map, Value};

pub(crate) const INDUSTRY_PROSPERITY_SOURCE: &str = "industry_prosperity_proxy";
pub(crate) const INDUSTRY_MEMBERSHIP_MARKET_SCOPE_GATE_ID: &str =
    "phase7_industry_membership_market_scope_gate_v1";
pub(crate) const INDUSTRY_PROSPERITY_REQUIRED_UNIVERSE_PROFILE: &str = "main_chinext_non_st";
pub(crate) const INDUSTRY_MEMBERSHIP_COVERAGE_THRESHOLD: f64 = 0.995;
pub(crate) const FUTURES_PRICE_CHAIN_SOURCE: &str = "futures_price_chain";
pub(crate) const FUTURES_PRICE_CHAIN_COVERAGE_GATE_ID: &str =
    "futures_price_chain_coverage_ready_v1";
pub(crate) const EQUITY_PLEDGE_PRESSURE_SOURCE: &str = "equity_pledge_pressure";
pub(crate) const EQUITY_PLEDGE_COVERAGE_GATE_ID: &str = "equity_pledge_coverage_ready_v1";
pub(crate) const SHAREHOLDER_STRUCTURE_SOURCE: &str = "shareholder_structure";
pub(crate) const SHAREHOLDER_STRUCTURE_LOW_FANOUT_STRICT_GATE_ID: &str =
    "shareholder_structure_low_fanout_strict_pit_gate_v1";
pub(crate) const MARGIN_DETAIL_SOURCE: &str = "margin_detail_leverage_crowding";
pub(crate) const MARGIN_DETAIL_COVERAGE_GATE_ID: &str = "margin_detail_coverage_ready_v1";
pub(crate) const ANALYST_REVISION_SOURCE: &str = "multi_vendor_analyst_revision";
pub(crate) const ANALYST_REVISION_COVERAGE_GATE_ID: &str = "analyst_revision_coverage_ready_v1";

pub(crate) fn industry_prosperity_alpha_admission_policy(
    eligible_markets: Vec<String>,
    excluded_markets: Vec<String>,
) -> Value {
    json!({
        "gate_id": INDUSTRY_MEMBERSHIP_MARKET_SCOPE_GATE_ID,
        "source": INDUSTRY_PROSPERITY_SOURCE,
        "status": "market_scope_gated",
        "coverage_threshold": INDUSTRY_MEMBERSHIP_COVERAGE_THRESHOLD,
        "eligible_markets": eligible_markets,
        "excluded_markets": excluded_markets,
        "required_universe_profile": INDUSTRY_PROSPERITY_REQUIRED_UNIVERSE_PROFILE,
        "gate_rule": "Only evaluate industry prosperity proxy inside markets with coverage_ratio >= 0.995 and multi_membership_symbol_days = 0; excluded markets must not be statically backfilled.",
        "entrypoint_policy": {
            "factor_builder": "must bind this gate and required_universe_profile before constructing an industry prosperity factor",
            "p310_diagnostics": "must use the same gate and universe_profile; full-market diagnostics are blocked while any market is excluded",
            "bounded_wfa": "must inherit this gate from diagnostics; test windows are evaluation-only",
            "v19_train_selection": "must reject industry prosperity candidates unless the same gate and universe_profile are present"
        },
        "forbidden_inputs": [
            "market_stock.industry static snapshot",
            "stock_basic.industry current snapshot",
            "concept_detail concept labels as direct industry replacement",
            "SW2021 current memberships before source-version availability"
        ],
    })
}

pub(crate) fn industry_prosperity_alpha_admission_policy_static() -> Value {
    industry_prosperity_alpha_admission_policy(
        vec!["主板".to_string(), "创业板".to_string()],
        vec!["科创板".to_string()],
    )
}

pub(crate) fn identifier_requires_industry_prosperity_gate(value: &str) -> bool {
    let normalized = value.trim().to_ascii_lowercase();
    normalized == INDUSTRY_PROSPERITY_SOURCE
        || normalized.contains("industry_prosperity")
        || normalized.contains("industry-prosperity")
}

pub(crate) fn identifier_requires_futures_price_chain_gate(value: &str) -> bool {
    let normalized = value.trim().to_ascii_lowercase();
    normalized == FUTURES_PRICE_CHAIN_SOURCE
        || normalized.contains("futures_price_chain")
        || normalized.contains("futures-price-chain")
        || normalized.starts_with("fpc_")
}

pub(crate) fn identifier_requires_equity_pledge_gate(value: &str) -> bool {
    let normalized = value.trim().to_ascii_lowercase();
    normalized == EQUITY_PLEDGE_PRESSURE_SOURCE
        || normalized.contains("equity_pledge")
        || normalized.contains("equity-pledge")
        || normalized.contains("pledge_pressure")
        || normalized.contains("pledge-pressure")
}

pub(crate) fn identifier_requires_shareholder_structure_gate(value: &str) -> bool {
    let normalized = value.trim().to_ascii_lowercase();
    normalized == SHAREHOLDER_STRUCTURE_SOURCE
        || normalized.contains("shareholder_structure")
        || normalized.contains("shareholder-structure")
        || normalized.contains("holder_number")
        || normalized.contains("holder-number")
        || normalized.contains("holder_trade")
        || normalized.contains("holder-trade")
}

pub(crate) fn identifier_requires_margin_detail_gate(value: &str) -> bool {
    let normalized = value.trim().to_ascii_lowercase();
    normalized == MARGIN_DETAIL_SOURCE
        || normalized == "margin_detail"
        || normalized.contains("margin_detail")
        || normalized.contains("margin-detail")
        || normalized.contains("leverage_crowding")
        || normalized.contains("leverage-crowding")
}

pub(crate) fn identifier_requires_analyst_revision_gate(value: &str) -> bool {
    let normalized = value.trim().to_ascii_lowercase();
    normalized == ANALYST_REVISION_SOURCE
        || normalized == "analyst_revision"
        || normalized.contains("analyst_revision")
        || normalized.contains("analyst-revision")
        || normalized.contains("stock_rank_forecast_cninfo")
        || normalized.contains("cninfo_revision")
        || normalized.contains("forecast_cninfo")
}

fn optional_string_from_maps<'a>(
    params: &'a Map<String, Value>,
    template: &'a Map<String, Value>,
    name: &str,
) -> Option<&'a str> {
    params
        .get(name)
        .or_else(|| template.get(name))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

pub(crate) fn validate_industry_prosperity_trial_admission(
    params: &Map<String, Value>,
    template: &Map<String, Value>,
) -> Result<(), String> {
    let requires_gate = ["combo_name", "source", "candidate_source", "factor_code"]
        .iter()
        .filter_map(|name| optional_string_from_maps(params, template, name))
        .any(identifier_requires_industry_prosperity_gate);

    if !requires_gate {
        return Ok(());
    }

    let gate_id = optional_string_from_maps(params, template, "alpha_admission_gate_id");
    let universe_profile = optional_string_from_maps(params, template, "universe_profile");
    validate_industry_prosperity_entrypoint_admission(
        INDUSTRY_PROSPERITY_SOURCE,
        gate_id,
        universe_profile,
        "optimization trial",
    )
}

pub(crate) fn validate_equity_pledge_trial_admission(
    params: &Map<String, Value>,
    template: &Map<String, Value>,
) -> Result<(), String> {
    let requires_gate = ["combo_name", "source", "candidate_source", "factor_code"]
        .iter()
        .filter_map(|name| optional_string_from_maps(params, template, name))
        .any(identifier_requires_equity_pledge_gate);

    if !requires_gate {
        return Ok(());
    }

    let gate_id = optional_string_from_maps(params, template, "alpha_admission_gate_id");
    let universe_profile = optional_string_from_maps(params, template, "universe_profile");
    validate_equity_pledge_entrypoint_admission(
        EQUITY_PLEDGE_PRESSURE_SOURCE,
        gate_id,
        universe_profile,
        "optimization trial",
    )
}

pub(crate) fn validate_shareholder_structure_trial_admission(
    params: &Map<String, Value>,
    template: &Map<String, Value>,
) -> Result<(), String> {
    let requires_gate = ["combo_name", "source", "candidate_source", "factor_code"]
        .iter()
        .filter_map(|name| optional_string_from_maps(params, template, name))
        .any(identifier_requires_shareholder_structure_gate);

    if !requires_gate {
        return Ok(());
    }

    let gate_id = optional_string_from_maps(params, template, "alpha_admission_gate_id");
    let universe_profile = optional_string_from_maps(params, template, "universe_profile");
    validate_shareholder_structure_entrypoint_admission(
        SHAREHOLDER_STRUCTURE_SOURCE,
        gate_id,
        universe_profile,
        "optimization trial",
    )
}

pub(crate) fn validate_margin_detail_trial_admission(
    params: &Map<String, Value>,
    template: &Map<String, Value>,
) -> Result<(), String> {
    let requires_gate = ["combo_name", "source", "candidate_source", "factor_code"]
        .iter()
        .filter_map(|name| optional_string_from_maps(params, template, name))
        .any(identifier_requires_margin_detail_gate);

    if !requires_gate {
        return Ok(());
    }

    let gate_id = optional_string_from_maps(params, template, "alpha_admission_gate_id");
    let universe_profile = optional_string_from_maps(params, template, "universe_profile");
    validate_margin_detail_entrypoint_admission(
        MARGIN_DETAIL_SOURCE,
        gate_id,
        universe_profile,
        "optimization trial",
    )
}

pub(crate) fn validate_analyst_revision_trial_admission(
    params: &Map<String, Value>,
    template: &Map<String, Value>,
) -> Result<(), String> {
    let requires_gate = ["combo_name", "source", "candidate_source", "factor_code"]
        .iter()
        .filter_map(|name| optional_string_from_maps(params, template, name))
        .any(identifier_requires_analyst_revision_gate);

    if !requires_gate {
        return Ok(());
    }

    let gate_id = optional_string_from_maps(params, template, "alpha_admission_gate_id");
    let universe_profile = optional_string_from_maps(params, template, "universe_profile");
    validate_analyst_revision_entrypoint_admission(
        ANALYST_REVISION_SOURCE,
        gate_id,
        universe_profile,
        "optimization trial",
    )
}

pub(crate) fn validate_industry_prosperity_entrypoint_admission(
    identifier: &str,
    gate_id: Option<&str>,
    universe_profile: Option<&str>,
    entrypoint: &str,
) -> Result<(), String> {
    if !identifier_requires_industry_prosperity_gate(identifier) {
        return Ok(());
    }

    if gate_id == Some(INDUSTRY_MEMBERSHIP_MARKET_SCOPE_GATE_ID)
        && universe_profile == Some(INDUSTRY_PROSPERITY_REQUIRED_UNIVERSE_PROFILE)
    {
        return Ok(());
    }

    Err(format!(
        "{} requires alpha_admission_gate_id={} and universe_profile={} at {} so 科创板等 coverage 不达标市场被统一排除 before factor builder, P3.10 diagnostics, bounded WFA, or v19 train selection",
        identifier.trim(),
        INDUSTRY_MEMBERSHIP_MARKET_SCOPE_GATE_ID,
        INDUSTRY_PROSPERITY_REQUIRED_UNIVERSE_PROFILE,
        entrypoint
    ))
}

pub(crate) fn validate_futures_price_chain_entrypoint_admission(
    identifier: &str,
    gate_id: Option<&str>,
    universe_profile: Option<&str>,
    entrypoint: &str,
) -> Result<(), String> {
    if !identifier_requires_futures_price_chain_gate(identifier) {
        return Ok(());
    }

    if gate_id == Some(FUTURES_PRICE_CHAIN_COVERAGE_GATE_ID)
        && universe_profile == Some(INDUSTRY_PROSPERITY_REQUIRED_UNIVERSE_PROFILE)
    {
        return Ok(());
    }

    Err(format!(
        "{} requires alpha_admission_gate_id={} and universe_profile={} at {} so futures raw/mapping coverage and 科创板等 excluded-market scope cannot be bypassed before factor builder, P3.10 diagnostics, bounded WFA, or v19 train selection",
        identifier.trim(),
        FUTURES_PRICE_CHAIN_COVERAGE_GATE_ID,
        INDUSTRY_PROSPERITY_REQUIRED_UNIVERSE_PROFILE,
        entrypoint
    ))
}

pub(crate) fn validate_equity_pledge_entrypoint_admission(
    identifier: &str,
    gate_id: Option<&str>,
    universe_profile: Option<&str>,
    entrypoint: &str,
) -> Result<(), String> {
    if !identifier_requires_equity_pledge_gate(identifier) {
        return Ok(());
    }

    if gate_id == Some(EQUITY_PLEDGE_COVERAGE_GATE_ID)
        && universe_profile == Some(INDUSTRY_PROSPERITY_REQUIRED_UNIVERSE_PROFILE)
    {
        return Ok(());
    }

    Err(format!(
        "{} requires alpha_admission_gate_id={} and universe_profile={} at {} so equity pledge raw coverage, PIT availability and 科创板等 excluded-market scope cannot be bypassed before factor builder, P3.10 diagnostics, bounded WFA, or v19 train selection",
        identifier.trim(),
        EQUITY_PLEDGE_COVERAGE_GATE_ID,
        INDUSTRY_PROSPERITY_REQUIRED_UNIVERSE_PROFILE,
        entrypoint
    ))
}

pub(crate) fn validate_shareholder_structure_entrypoint_admission(
    identifier: &str,
    gate_id: Option<&str>,
    universe_profile: Option<&str>,
    entrypoint: &str,
) -> Result<(), String> {
    if !identifier_requires_shareholder_structure_gate(identifier) {
        return Ok(());
    }

    if gate_id == Some(SHAREHOLDER_STRUCTURE_LOW_FANOUT_STRICT_GATE_ID)
        && universe_profile == Some(INDUSTRY_PROSPERITY_REQUIRED_UNIVERSE_PROFILE)
    {
        return Ok(());
    }

    Err(format!(
        "{} requires alpha_admission_gate_id={} and universe_profile={} at {} so shareholder_structure uses only strict low-fanout PIT rows: holder_number available_at >= end_date with positive holder_num, holder_trade ratio/interval anomalies excluded, and raw top10 data cannot be bypassed before separate coverage admission",
        identifier.trim(),
        SHAREHOLDER_STRUCTURE_LOW_FANOUT_STRICT_GATE_ID,
        INDUSTRY_PROSPERITY_REQUIRED_UNIVERSE_PROFILE,
        entrypoint
    ))
}

pub(crate) fn validate_margin_detail_entrypoint_admission(
    identifier: &str,
    gate_id: Option<&str>,
    universe_profile: Option<&str>,
    entrypoint: &str,
) -> Result<(), String> {
    if !identifier_requires_margin_detail_gate(identifier) {
        return Ok(());
    }

    if gate_id == Some(MARGIN_DETAIL_COVERAGE_GATE_ID)
        && universe_profile == Some(INDUSTRY_PROSPERITY_REQUIRED_UNIVERSE_PROFILE)
    {
        return Ok(());
    }

    Err(format!(
        "{} requires alpha_admission_gate_id={} and universe_profile={} at {} so margin_detail uses only full-history PIT rows with next-session availability, source_published_at present, low same-family correlation, and main/ChiNext non-ST scope before factor builder, P3.10 diagnostics, bounded WFA, or v19 train selection",
        identifier.trim(),
        MARGIN_DETAIL_COVERAGE_GATE_ID,
        INDUSTRY_PROSPERITY_REQUIRED_UNIVERSE_PROFILE,
        entrypoint
    ))
}

pub(crate) fn validate_analyst_revision_entrypoint_admission(
    identifier: &str,
    gate_id: Option<&str>,
    universe_profile: Option<&str>,
    entrypoint: &str,
) -> Result<(), String> {
    if !identifier_requires_analyst_revision_gate(identifier) {
        return Ok(());
    }

    if gate_id == Some(ANALYST_REVISION_COVERAGE_GATE_ID)
        && universe_profile == Some(INDUSTRY_PROSPERITY_REQUIRED_UNIVERSE_PROFILE)
    {
        return Ok(());
    }

    Err(format!(
        "{} requires alpha_admission_gate_id={} and universe_profile={} at {} so AkShare/multi-vendor analyst revision uses only full-history PIT rows with source_published_at present, revision semantics complete, duplicate-hash audit clean, low same-family correlation, and main/ChiNext non-ST scope before factor builder, P3.10 diagnostics, bounded WFA, or v19 train selection",
        identifier.trim(),
        ANALYST_REVISION_COVERAGE_GATE_ID,
        INDUSTRY_PROSPERITY_REQUIRED_UNIVERSE_PROFILE,
        entrypoint
    ))
}

pub(crate) fn validate_industry_prosperity_factor_builder_admission(
    identifier: &str,
) -> Result<(), String> {
    if !identifier_requires_industry_prosperity_gate(identifier) {
        return Ok(());
    }

    Err(format!(
        "{} must use a dedicated market-scope factor builder bound to {} and universe_profile={}; generic alpha blend cannot enforce excluded markets such as 科创板",
        identifier.trim(),
        INDUSTRY_MEMBERSHIP_MARKET_SCOPE_GATE_ID,
        INDUSTRY_PROSPERITY_REQUIRED_UNIVERSE_PROFILE
    ))
}
