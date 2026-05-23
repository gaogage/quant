-- Phase 7 professional discovery metadata seed.
--
-- This file is intentionally idempotent. It restores the conventional
-- strategy/data version IDs used by Phase 7 OOS/WFA discovery requests after a
-- local PostgreSQL rebuild or upgrade, without relaxing any robustness gate.

INSERT INTO public.strategy_definition
    (strategy_code, name, strategy_type, description, status)
VALUES
    (
        'PHASE7_PROFESSIONAL',
        'Phase 7 professional automated discovery',
        'phase7_professional_discovery',
        'Automated PIT/OOS/WFA Phase 7 strategy discovery with professional robustness gates',
        'active'
    )
ON CONFLICT (strategy_code) DO UPDATE SET
    name = EXCLUDED.name,
    strategy_type = EXCLUDED.strategy_type,
    description = EXCLUDED.description,
    status = EXCLUDED.status,
    updated_at = now();

INSERT INTO public.strategy_version
    (
        strategy_version_id,
        strategy_code,
        version,
        parameter_schema,
        default_parameters,
        required_factors,
        required_models,
        risk_constraints,
        status
    )
VALUES
    (
        'phase7-professional-v1',
        'PHASE7_PROFESSIONAL',
        '1.0.0',
        '{
            "type": "object",
            "properties": {
                "search_profile": {"type": "string"},
                "gate_policy": {"type": "string"},
                "train_selection_gate_policy": {"type": "string"},
                "final_promotion_gate_policy": {"type": "string"},
                "oos_top_n": {"type": "integer"},
                "trial_concurrency": {"type": "integer"},
                "train_cache_mode": {"type": "string"}
            },
            "additionalProperties": true
        }'::jsonb,
        '{
            "search_profile": "professional_breakthrough",
            "gate_policy": "professional",
            "train_selection_gate_policy": "oos_train_selection",
            "final_promotion_gate_policy": "professional_oos_promotion",
            "oos_top_n": 20,
            "trial_concurrency": 1,
            "train_cache_mode": "shared_window",
            "pit_required": true,
            "walk_forward_required": true,
            "bootstrap_required": true,
            "market_scenario_required": true,
            "cost_capacity_required": true
        }'::jsonb,
        '[
            "phase7_financial_quality_v1",
            "phase7_valuation_v1",
            "phase7_growth_recovery_v1",
            "phase7_relative_strength_v1",
            "phase7_moneyflow_v1",
            "phase7_event_window_earnings_v1",
            "phase7_quality_value_recovery_event_confirm_v1",
            "phase7_quality_event_window_overlay_v1",
            "phase7_quality_value_recovery_confirm_v1",
            "phase7_quality_relative_strength_v1"
        ]'::jsonb,
        '[]'::jsonb,
        '{
            "min_annual_return": 0.15,
            "min_sharpe": 1.0,
            "elite_min_sharpe": 1.5,
            "elite_min_sortino": 1.8,
            "elite_min_calmar": 2.0,
            "max_drawdown": 0.35,
            "stitched_oos_min_calmar": 1.2,
            "requires_no_train_test_overlap": true
        }'::jsonb,
        'active'
    )
ON CONFLICT (strategy_version_id) DO UPDATE SET
    strategy_code = EXCLUDED.strategy_code,
    version = EXCLUDED.version,
    parameter_schema = EXCLUDED.parameter_schema,
    default_parameters = EXCLUDED.default_parameters,
    required_factors = EXCLUDED.required_factors,
    required_models = EXCLUDED.required_models,
    risk_constraints = EXCLUDED.risk_constraints,
    status = EXCLUDED.status;

WITH source_version AS (
    SELECT
        source,
        start_date,
        end_date,
        tables,
        quality_check_id,
        snapshot_hash,
        metadata
    FROM public.data_version
    WHERE data_version_id = 'research-full-2016-2026-20260515'
),
version_payload AS (
    SELECT
        'full-market-2016-v1'::varchar AS data_version_id,
        'Full market 2016+ research dataset alias'::varchar AS name,
        source,
        start_date,
        end_date,
        tables,
        quality_check_id,
        'full-market-2016-v1-alias-research-20260515'::varchar AS snapshot_hash,
        jsonb_build_object(
            'alias_of', 'research-full-2016-2026-20260515',
            'purpose', 'phase7_professional_oos_wfa_discovery',
            'pit_required', true,
            'source_snapshot_hash', snapshot_hash,
            'source_metadata', metadata
        ) AS metadata
    FROM source_version

    UNION ALL

    SELECT
        'full-market-2016-v1'::varchar AS data_version_id,
        'Full market 2016+ research dataset alias'::varchar AS name,
        'local_quant'::varchar AS source,
        DATE '2016-02-01' AS start_date,
        DATE '2026-05-15' AS end_date,
        ARRAY[
            'market_trade_calendar',
            'market_stock',
            'market_stock_daily_bar',
            'market_index_daily_bar',
            'market_adjustment_factor',
            'market_stock_daily_basic',
            'market_stock_moneyflow',
            'market_financial_statement',
            'market_financial_indicator',
            'market_stock_forecast',
            'market_stock_express',
            'market_stock_disclosure_date',
            'factor_value',
            'multi_factor_value'
        ]::text[] AS tables,
        NULL::varchar AS quality_check_id,
        'full-market-2016-v1-manual-20260515'::varchar AS snapshot_hash,
        jsonb_build_object(
            'alias_of', null,
            'purpose', 'phase7_professional_oos_wfa_discovery',
            'pit_required', true,
            'note', 'Fallback metadata inserted because research-full-2016-2026-20260515 was not present'
        ) AS metadata
    WHERE NOT EXISTS (SELECT 1 FROM source_version)
)
INSERT INTO public.data_version
    (
        data_version_id,
        name,
        source,
        start_date,
        end_date,
        tables,
        quality_check_id,
        snapshot_hash,
        metadata
    )
SELECT
    data_version_id,
    name,
    source,
    start_date,
    end_date,
    tables,
    quality_check_id,
    snapshot_hash,
    metadata
FROM version_payload
ON CONFLICT (data_version_id) DO UPDATE SET
    name = EXCLUDED.name,
    source = EXCLUDED.source,
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    tables = EXCLUDED.tables,
    quality_check_id = EXCLUDED.quality_check_id,
    snapshot_hash = EXCLUDED.snapshot_hash,
    metadata = EXCLUDED.metadata;
