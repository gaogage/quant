#!/usr/bin/env python3
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
checks = [
    (
        "backtest_task insert must not use phase2-v1 placeholders",
        ROOT / "quant-backtest/src/runner.rs",
        ["phase2-v1", "Vec::<String>::new())  // symbols"],
    ),
    (
        "benchmark data must be loaded from market_index_daily_bar",
        ROOT / "quant-backtest/src/runner.rs",
        ["FROM market_stock_daily_bar\n             WHERE symbol = $1"],
    ),
    (
        "index daily sync must not write into stock daily table",
        ROOT / "quant-data/src/sync.rs",
        ["upsert_daily_bars_batch(pool, &bars, dv_id, \"tushare\").await?"],
    ),
    (
        "factor_value inserts must persist available_at",
        ROOT / "quant-api/src/routes/factors.rs",
        [
            "INSERT INTO factor_value (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value)",
            "VALUES ($1,$2,$3,$4,$5,$6)",
        ],
    ),
]

failed = []
for label, path, forbidden in checks:
    text = path.read_text(encoding="utf-8")
    hits = [item for item in forbidden if item in text]
    if hits:
        failed.append((label, str(path.relative_to(ROOT)), hits))

prototype_sql = ROOT / "sql/phase3_factor_value.sql"
if prototype_sql.exists():
    first_lines = "\n".join(prototype_sql.read_text(encoding="utf-8").splitlines()[:2])
    if "PROTOTYPE ONLY" not in first_lines:
        failed.append(("phase3_factor_value.sql must be marked as prototype", str(prototype_sql.relative_to(ROOT)), ["missing PROTOTYPE ONLY marker"]))

financial_prototype_sql = ROOT / "sql/phase3_financial.sql"
if financial_prototype_sql.exists():
    first_lines = "\n".join(financial_prototype_sql.read_text(encoding="utf-8").splitlines()[:2])
    if "PROTOTYPE ONLY" not in first_lines:
        failed.append(("phase3_financial.sql must be marked as prototype", str(financial_prototype_sql.relative_to(ROOT)), ["missing PROTOTYPE ONLY marker"]))

event_alpha_sql = ROOT / "sql/phase7_event_alpha.sql"
if event_alpha_sql.exists():
    event_alpha_text = event_alpha_sql.read_text(encoding="utf-8")
    for required in [
        "CREATE TABLE IF NOT EXISTS public.market_stock_forecast",
        "CREATE TABLE IF NOT EXISTS public.market_stock_express",
        "CREATE TABLE IF NOT EXISTS public.market_stock_disclosure_date",
        "PRIMARY KEY (symbol, ann_date, end_date, forecast_type, first_ann_date, available_at)",
        "PRIMARY KEY (symbol, ann_date, end_date, available_at)",
        "PRIMARY KEY (symbol, end_date, available_at)",
    ]:
        if required not in event_alpha_text:
            failed.append(("phase7_event_alpha.sql must mirror official event schema", str(event_alpha_sql.relative_to(ROOT)), [required]))
else:
    failed.append(("phase7_event_alpha.sql must exist for local incremental DDL", "sql/phase7_event_alpha.sql", ["missing file"]))

optional_sources_sql = ROOT / "sql/phase7_optional_financial_sources.sql"
if optional_sources_sql.exists():
    optional_sources_text = optional_sources_sql.read_text(encoding="utf-8")
    for required in [
        "CREATE TABLE IF NOT EXISTS public.market_stock_cashflow",
        "f_ann_date DATE NULL",
        "available_at DATE NOT NULL",
        "PRIMARY KEY (symbol, end_date, ann_date, available_at)",
        "idx_market_stock_cashflow_symbol_date",
        "fk_market_stock_cashflow_data_version",
        "CREATE TABLE IF NOT EXISTS public.market_stock_dividend",
        "PRIMARY KEY (symbol, end_date, ann_date, div_proc, available_at)",
        "idx_market_stock_dividend_symbol_date",
        "fk_market_stock_dividend_data_version",
        "CREATE TABLE IF NOT EXISTS public.market_stock_repurchase",
        "PRIMARY KEY (symbol, ann_date, end_date, proc, available_at)",
        "idx_market_stock_repurchase_symbol_date",
        "fk_market_stock_repurchase_data_version",
    ]:
        if required not in optional_sources_text:
            failed.append(("phase7_optional_financial_sources.sql must define PIT cashflow schema", str(optional_sources_sql.relative_to(ROOT)), [required]))
else:
    failed.append(("Phase 7 optional financial source SQL must exist", "sql/phase7_optional_financial_sources.sql", ["missing file"]))

phase7_metadata_sql = ROOT / "sql/phase7_professional_metadata.sql"
if phase7_metadata_sql.exists():
    phase7_metadata_text = phase7_metadata_sql.read_text(encoding="utf-8")
    for required in [
        "PHASE7_PROFESSIONAL",
        "phase7-professional-v1",
        "full-market-2016-v1",
        "research-full-2016-2026-20260515",
        "ON CONFLICT (strategy_code) DO UPDATE",
        "ON CONFLICT (strategy_version_id) DO UPDATE",
        "ON CONFLICT (data_version_id) DO UPDATE",
        "phase7_quality_value_recovery_event_confirm_v1",
        "phase7_quality_event_window_overlay_v1",
        "phase7_quality_value_recovery_confirm_v1",
        "phase7_quality_relative_strength_v1",
        "pit_required",
        "walk_forward_required",
        "bootstrap_required",
        "market_scenario_required",
        "cost_capacity_required",
    ]:
        if required not in phase7_metadata_text:
            failed.append(("Phase 7 metadata seed must restore professional FK aliases", str(phase7_metadata_sql.relative_to(ROOT)), [required]))
else:
    failed.append(("Phase 7 metadata seed must exist for local DB rebuilds", "sql/phase7_professional_metadata.sql", ["missing file"]))

phase7_perf_indexes_sql = ROOT / "sql/phase7_discovery_perf_indexes.sql"
if phase7_perf_indexes_sql.exists():
    phase7_perf_indexes_text = phase7_perf_indexes_sql.read_text(encoding="utf-8")
    for required in [
        "idx_multi_factor_value_combo_date_score_symbol",
        "ON public.multi_factor_value (combo_name, version, trade_date DESC, raw_score, symbol)",
        "idx_market_stock_daily_date_symbol_cover",
        "ON public.market_stock_daily_bar (trade_date, symbol)",
        "INCLUDE (open, close, pre_close, amount)",
    ]:
        if required not in phase7_perf_indexes_text:
            failed.append(("Phase 7 discovery perf indexes must cover OOS/WFA hot queries", str(phase7_perf_indexes_sql.relative_to(ROOT)), [required]))
else:
    failed.append(("Phase 7 discovery perf indexes must exist for strict OOS/WFA scaling", "sql/phase7_discovery_perf_indexes.sql", ["missing file"]))

phase7_market_feature_cache_sql = ROOT / "sql/phase7_market_feature_cache.sql"
if phase7_market_feature_cache_sql.exists():
    phase7_market_feature_cache_text = phase7_market_feature_cache_sql.read_text(encoding="utf-8")
    for required in [
        "return_risk_feature_matrix",
        "return_risk_stats_feature_matrix",
        "CREATE TABLE IF NOT EXISTS public.market_feature_cache_return_risk_matrix_row",
        "score_day DATE NOT NULL",
        "returns DOUBLE PRECISION[] NOT NULL",
        "array_position(returns, NULL) IS NULL",
        "PRIMARY KEY (cache_key, score_day, symbol)",
        "idx_market_feature_cache_return_risk_matrix_symbol_day",
        "CREATE TABLE IF NOT EXISTS public.market_feature_cache_return_risk_stats_row",
        "return_count BIGINT NOT NULL CHECK (return_count >= 0)",
        "kelly_population_variance DOUBLE PRECISION NULL",
        "CREATE TABLE IF NOT EXISTS public.market_feature_cache_return_risk_pairwise_row",
        "left_symbol TEXT NOT NULL",
        "right_symbol TEXT NOT NULL",
        "CHECK (left_symbol < right_symbol)",
        "CHECK (correlation BETWEEN -1.000000000001 AND 1.000000000001)",
        "idx_market_feature_cache_return_risk_pairwise_left_day",
    ]:
        if required not in phase7_market_feature_cache_text:
            failed.append(("Phase 7 market feature cache must support return/risk matrix rows", str(phase7_market_feature_cache_sql.relative_to(ROOT)), [required]))
else:
    failed.append(("Phase 7 market feature cache SQL must exist", "sql/phase7_market_feature_cache.sql", ["missing file"]))

schema_sql = ROOT.parent / "docs/projects/quant/tasks/quant/sql/001_initial_schema.sql"
if schema_sql.exists():
    schema_text = schema_sql.read_text(encoding="utf-8")
    factor_value_block = schema_text.split("CREATE TABLE IF NOT EXISTS public.factor_value", 1)[-1]
    factor_value_block = factor_value_block.split("CREATE INDEX IF NOT EXISTS idx_factor_value_factor_date", 1)[0]
    if "available_at DATE NULL" not in factor_value_block:
        failed.append(("factor_value schema must include available_at", str(schema_sql.relative_to(ROOT.parent)), ["missing available_at DATE NULL"]))
    for required in [
        "CREATE TABLE IF NOT EXISTS public.market_financial_statement",
        "CREATE TABLE IF NOT EXISTS public.market_financial_indicator",
        "CREATE TABLE IF NOT EXISTS public.market_stock_daily_basic",
        "CREATE TABLE IF NOT EXISTS public.market_stock_moneyflow",
        "CREATE TABLE IF NOT EXISTS public.market_stock_cashflow",
        "CREATE TABLE IF NOT EXISTS public.market_stock_dividend",
        "CREATE TABLE IF NOT EXISTS public.market_stock_repurchase",
        "CREATE TABLE IF NOT EXISTS public.market_stock_forecast",
        "CREATE TABLE IF NOT EXISTS public.market_stock_express",
        "CREATE TABLE IF NOT EXISTS public.market_stock_disclosure_date",
        "idx_market_stock_daily_basic_symbol_date",
        "idx_market_stock_moneyflow_symbol_date",
        "idx_market_stock_cashflow_symbol_date",
        "idx_market_stock_dividend_symbol_date",
        "idx_market_stock_repurchase_symbol_date",
        "idx_market_stock_forecast_symbol_date",
        "idx_market_stock_express_symbol_date",
        "idx_market_stock_disclosure_date_symbol_date",
        "fk_market_stock_daily_basic_data_version",
        "fk_market_stock_moneyflow_data_version",
        "fk_market_stock_cashflow_data_version",
        "fk_market_stock_dividend_data_version",
        "fk_market_stock_repurchase_data_version",
        "fk_market_stock_forecast_data_version",
        "fk_market_stock_express_data_version",
        "fk_market_stock_disclosure_date_data_version",
        "ann_date DATE NOT NULL",
        "idx_fin_ind_ann_date",
        "PRIMARY KEY (symbol, ann_date, end_date, forecast_type, first_ann_date, available_at)",
        "PRIMARY KEY (symbol, ann_date, end_date, available_at)",
        "PRIMARY KEY (symbol, end_date, available_at)",
        "CREATE TABLE IF NOT EXISTS public.factor_evaluation",
        "CREATE TABLE IF NOT EXISTS public.multi_factor_weight",
        "CREATE TABLE IF NOT EXISTS public.multi_factor_value",
        "available_at DATE NULL",
        "idx_multi_factor_value_combo_date",
        "idx_multi_factor_value_combo_date_score_symbol",
        "idx_market_stock_daily_date_symbol_cover",
        "CREATE TABLE IF NOT EXISTS public.strategy_parameter_candidate",
        "UNIQUE (optimization_task_id, trial_id, target_strategy_version)",
        "fk_strategy_parameter_candidate_trial",
        "CREATE TABLE IF NOT EXISTS public.robustness_gate_result",
        "CREATE TABLE IF NOT EXISTS public.experiment_run",
        "approved_candidate",
        "CREATE TABLE IF NOT EXISTS public.training_dataset",
        "CREATE TABLE IF NOT EXISTS public.prediction_set",
        "CREATE TABLE IF NOT EXISTS public.model_prediction",
        "training_dataset_id VARCHAR(64) NULL",
        "SELECT create_hypertable(",
        "'public.model_prediction'",
    ]:
        if required not in schema_text:
            failed.append(("financial PIT schema must be in official init SQL", str(schema_sql.relative_to(ROOT.parent)), [required]))

factor_routes = ROOT / "quant-api/src/routes/factors.rs"
if factor_routes.exists():
    factor_routes_text = factor_routes.read_text(encoding="utf-8")
    if factor_routes_text.count("upsert_factor_definition(") < 4:
        failed.append(("factor sync must register factor_definition", str(factor_routes.relative_to(ROOT)), ["expected helper plus three call sites"]))
    for required in [
        "register_factor_definition(",
        "list_factor_definitions(",
        "get_factor_definition(",
        "upsert_factor_definition_input(",
        "sync_financial_factor_values(",
        "parse_financial_factor(",
        "FinancialIndicatorRow",
        "Phase7RelativeStrengthBackfillRequest",
        "Phase7QualityRelativeStrengthBackfillRequest",
        "Phase7GrowthRecoveryBackfillRequest",
        "Phase7ValuationBackfillRequest",
        "Phase7MoneyflowBackfillRequest",
        "Phase7EventAlphaBackfillRequest",
        "Phase7EventWindowAlphaBackfillRequest",
        "Phase7AlphaBlendBackfillRequest",
        "Phase7AlphaBlendProfilesBackfillRequest",
        "phase7_relative_strength_backfill_specs",
        "phase7_quality_relative_strength_combo_specs",
        "phase7_growth_recovery_backfill_specs",
        "phase7_valuation_backfill_specs",
        "phase7_moneyflow_backfill_specs",
        "phase7_event_alpha_backfill_specs",
        "phase7_event_window_alpha_backfill_specs",
        "phase7_event_surprise_backfill_specs",
        "phase7_alpha_blend_backfill_sql",
        "phase7_optional_overlay_blend_backfill_sql",
        "phase7_relative_momentum_backfill_sql",
        "phase7_financial_annual_change_backfill_sql",
        "phase7_daily_basic_latest_backfill_sql",
        "phase7_moneyflow_backfill_sql",
        "phase7_event_latest_backfill_sql",
        "phase7_event_window_backfill_sql",
        "phase7_combo_required_factor_count",
        "weighted_event_earnings",
        "weighted_event_window_earnings",
        "weighted_event_surprise",
        "weighted_combo_optional_overlay",
        "phase7_event_surprise_v1",
        "phase7_event_window_earnings_v1",
        "phase7_quality_event_surprise_confirm_v1",
        "phase7_quality_event_window_overlay_v1",
        "HAVING COUNT(DISTINCT fv.factor_code) >= $6",
        "/ NULLIF(SUM(weights.weight), 0.0) AS raw_score",
        "set_local_combo_backfill_planner",
        "SET LOCAL enable_bitmapscan = off",
        "backfill_phase7_relative_strength_background",
        "backfill_phase7_quality_relative_strength_background",
        "backfill_phase7_growth_recovery_background",
        "backfill_phase7_valuation_background",
        "backfill_phase7_moneyflow_background",
        "backfill_phase7_event_alpha_background",
        "backfill_phase7_event_surprise_background",
        "backfill_phase7_event_window_alpha_background",
        "backfill_phase7_alpha_blend_background",
        "backfill_phase7_alpha_blend_profiles_background",
    ]:
        if required not in factor_routes_text:
            failed.append(("factor_definition API handlers must exist", str(factor_routes.relative_to(ROOT)), [required]))
    for required in [
        '"pit_date": "ann_date"',
        "factor_value.trade_date = ann_date",
    ]:
        if required == "factor_value.trade_date = ann_date":
            if ".bind(fv.date)" not in factor_routes_text or ".bind(fv.available_at)" not in factor_routes_text:
                failed.append(("financial factors must persist PIT ann_date into factor_value", str(factor_routes.relative_to(ROOT)), ["missing fv.date/fv.available_at bind"]))
        elif required not in factor_routes_text:
            failed.append(("financial factors must document PIT ann_date in metadata", str(factor_routes.relative_to(ROOT)), [required]))

main_routes = ROOT / "quant-api/src/main.rs"
if main_routes.exists():
    main_routes_text = main_routes.read_text(encoding="utf-8")
    for required in [
        '"/api/v1/quant/backtests/run-prediction"',
        '"/api/v1/quant/factor-definitions"',
        '"/api/v1/quant/factor-definitions/{factor_code}/{version}"',
        '"/api/v1/quant/factors"',
        '"/api/v1/quant/factors/sync-financial"',
        '"/api/v1/quant/factors/phase7-relative-strength-backfill/background"',
        '"/api/v1/quant/factors/phase7-quality-relative-strength-backfill/background"',
        '"/api/v1/quant/factors/phase7-growth-recovery-backfill/background"',
        '"/api/v1/quant/factors/phase7-valuation-backfill/background"',
        '"/api/v1/quant/factors/phase7-moneyflow-backfill/background"',
        '"/api/v1/quant/factors/phase7-event-alpha-backfill/background"',
        '"/api/v1/quant/factors/phase7-event-surprise-backfill/background"',
        '"/api/v1/quant/factors/phase7-event-window-alpha-backfill/background"',
        '"/api/v1/quant/factors/phase7-alpha-blend-backfill/background"',
        '"/api/v1/quant/factors/phase7-alpha-blend-profiles-backfill/background"',
        '"/api/v1/quant/optimizations"',
        '"/api/v1/quant/optimizations/{optimization_task_id}"',
        '"/api/v1/quant/optimizations/{optimization_task_id}/trials"',
        '"/api/v1/quant/optimizations/{optimization_task_id}/run"',
        '"/api/v1/quant/optimizations/{optimization_task_id}/promote"',
        '"/api/v1/quant/optimizations/{optimization_task_id}/robustness-gates"',
        '"/api/v1/quant/ml/prediction-sets/linear-smoke"',
        '"/api/v1/quant/ml/training-tasks/linear"',
        '"/api/v1/quant/ml/prediction-sets/walk-forward-linear"',
        '"/api/v1/quant/ml/prediction-sets/evaluate"',
        '"/api/v1/quant/data/phase7-optional-source-coverage-sync"',
        '"/api/v1/quant/data/phase7-optional-source-coverage-batches"',
    ]:
        if required not in main_routes_text:
            failed.append(("factor_definition API routes must be mounted", str(main_routes.relative_to(ROOT)), [required]))

optimization_routes = ROOT / "quant-api/src/routes/optimization.rs"
if optimization_routes.exists():
    optimization_text = optimization_routes.read_text(encoding="utf-8")
    for required in [
        "create_optimization(",
        "get_optimization(",
        "list_optimization_trials(",
        "run_optimization_trials(",
        "promote_optimization_trial(",
        "evaluate_optimization_robustness(",
        "evaluate_robustness_gates(",
        "execute_pending_trials(",
        "build_factor_trial_request(",
        "build_prediction_trial_request(",
        "build_optimization_trial_request(",
        "execute_prediction_backtest(",
        "signal_source",
        "model_prediction",
        "max_pairwise_correlation",
        "correlation_lookback_days",
        "kelly_fraction",
        "kelly_lookback_days",
        "max_gross_exposure",
        "score_direction",
        "score_trial(",
        "trial_reuse_key(",
        "find_reusable_trial(",
        "normalize_performance_gate(",
        "evaluate_optimization_performance_gates(",
        "persist_optimization_experiment_run(",
        "build_market_scenario_analysis(",
        "build_walk_forward_analysis(",
        "build_bootstrap_analysis(",
        "evaluate_robustness_gates_with_analysis(",
        "load_robustness_timeseries_analysis(",
        "walk_forward_min_window_count",
        "bootstrap_positive_return_probability",
        "market_scenario_coverage",
        "optimization_trial_batch_release_gate",
        "strategy_parameter_candidate",
        "INSERT INTO audit_event",
        "INSERT INTO robustness_gate_result",
        "INSERT INTO experiment_run",
        "generate_trial_parameters(",
        "DeterministicRng",
        "INSERT INTO optimization_task",
        "INSERT INTO optimization_trial",
        "backtest_template",
        "UPDATE optimization_trial",
    ]:
        if required not in optimization_text:
            failed.append(("optimization task/trial API must be present", str(optimization_routes.relative_to(ROOT)), [required]))

phase7_common = ROOT / "quant-common/src/phase7.rs"
if phase7_common.exists():
    phase7_common_text = phase7_common.read_text(encoding="utf-8")
    for required in [
        "phase7_event_earnings_v1",
        "phase7_event_surprise_v1",
        "phase7_event_window_earnings_v1",
        "phase7_quality_event_window_overlay_v1",
        "phase7_quality_event_surprise_confirm_v1",
        "phase7_quality_event_confirm_v1",
        "phase7_quality_value_recovery_event_confirm_v1",
        "phase7_quality_value_recovery_confirm_v1",
        "phase7_quality_relative_strength_v1",
        "quality_event_confirm_5pct",
        "quality_event_surprise_confirm_5pct",
        "quality_value_recovery_event_confirm_5pct",
    ]:
        if required not in phase7_common_text:
            failed.append(("Phase 7 search space must include event alpha confirmation blends", str(phase7_common.relative_to(ROOT)), [required]))

if schema_sql.exists():
    schema_text = schema_sql.read_text(encoding="utf-8")
    optimization_task_block = schema_text.split("CREATE TABLE IF NOT EXISTS public.optimization_task", 1)[-1]
    optimization_task_block = optimization_task_block.split("CREATE INDEX IF NOT EXISTS idx_optimization_task_status_created_at", 1)[0]
    if "backtest_template JSONB NULL" not in optimization_task_block:
        failed.append(("optimization_task schema must include backtest_template", str(schema_sql.relative_to(ROOT.parent)), ["missing backtest_template JSONB NULL"]))
    model_training_task_block = schema_text.split("CREATE TABLE IF NOT EXISTS public.model_training_task", 1)[-1]
    model_training_task_block = model_training_task_block.split("CREATE INDEX IF NOT EXISTS idx_model_training_task_status_created_at", 1)[0]
    if "training_dataset_id VARCHAR(64) NULL" not in model_training_task_block:
        failed.append(("model_training_task schema must include training_dataset_id", str(schema_sql.relative_to(ROOT.parent)), ["missing training_dataset_id VARCHAR(64) NULL"]))
    if "fk_model_training_task_training_dataset" not in schema_text:
        failed.append(("model_training_task schema must FK training_dataset", str(schema_sql.relative_to(ROOT.parent)), ["missing fk_model_training_task_training_dataset"]))
    experiment_run_block = schema_text.split("CREATE TABLE IF NOT EXISTS public.experiment_run", 1)[-1]
    experiment_run_block = experiment_run_block.split("CREATE INDEX IF NOT EXISTS idx_experiment_run_type_created_at", 1)[0]
    for required in [
        "experiment_type VARCHAR(64) NOT NULL",
        "related_entity_type VARCHAR(64) NULL",
        "related_entity_id VARCHAR(64) NULL",
        "metrics JSONB NULL",
    ]:
        if required not in experiment_run_block:
            failed.append(("experiment_run schema must support ML experiment summaries", str(schema_sql.relative_to(ROOT.parent)), [required]))
    backtest_task_block = schema_text.split("CREATE TABLE IF NOT EXISTS public.backtest_task", 1)[-1]
    backtest_task_block = backtest_task_block.split("CREATE INDEX IF NOT EXISTS idx_backtest_task_status_created_at", 1)[0]
    if "prediction_set_id VARCHAR(64) NULL" not in backtest_task_block:
        failed.append(("backtest_task schema must include prediction_set_id", str(schema_sql.relative_to(ROOT.parent)), ["missing prediction_set_id VARCHAR(64) NULL"]))
    if "fk_backtest_task_prediction_set" not in schema_text:
        failed.append(("backtest_task schema must FK prediction_set", str(schema_sql.relative_to(ROOT.parent)), ["missing fk_backtest_task_prediction_set"]))
else:
    failed.append(("optimization routes module must exist", "quant-api/src/routes/optimization.rs", ["missing file"]))

sync_rs = ROOT / "quant-data/src/sync.rs"
if sync_rs.exists():
    sync_text = sync_rs.read_text(encoding="utf-8")
    for required in [
        "MarketStockDailyBasic",
        "daily_basic_row_from_map",
        "sync_daily_basic",
        "market_stock_daily_basic",
        ".daily_basic(",
    ]:
        if required not in sync_text:
            failed.append(("daily_basic sync path must exist", str(sync_rs.relative_to(ROOT)), [required]))
    for required in [
        "MarketStockMoneyflow",
        "moneyflow_row_from_map",
        "sync_moneyflow",
        "market_stock_moneyflow",
        ".moneyflow(",
    ]:
        if required not in sync_text:
            failed.append(("moneyflow sync path must exist", str(sync_rs.relative_to(ROOT)), [required]))
    for required in [
        "MarketStockForecast",
        "forecast_row_from_map",
        "sync_forecast",
        "market_stock_forecast",
        ".forecast(",
        "MarketStockExpress",
        "express_row_from_map",
        "sync_express",
        "market_stock_express",
        ".express(",
        "MarketStockDisclosureDate",
        "disclosure_date_row_from_map",
        "sync_disclosure_date",
        "market_stock_disclosure_date",
        ".disclosure_date(",
    ]:
        if required not in sync_text:
            failed.append(("event sync path must exist", str(sync_rs.relative_to(ROOT)), [required]))
    for required in [
        "MarketStockCashflow",
        "cashflow_row_from_map",
        "sync_cashflow",
        "market_stock_cashflow",
        ".cashflow(",
        "MarketStockDividend",
        "dividend_row_from_map",
        "sync_dividend",
        "market_stock_dividend",
        ".dividend(",
        "MarketStockRepurchase",
        "repurchase_row_from_map",
        "sync_repurchase",
        "market_stock_repurchase",
        ".repurchase(",
    ]:
        if required not in sync_text:
            failed.append(("optional financial/event sync path must exist", str(sync_rs.relative_to(ROOT)), [required]))

tushare_client = ROOT / "quant-data/src/tushare/client.rs"
if tushare_client.exists():
    client_text = tushare_client.read_text(encoding="utf-8")
    for required in [
        "pub async fn daily_basic(",
        "\"daily_basic\"",
        "\"pe_ttm\"",
        "\"ps_ttm\"",
        "\"dv_ttm\"",
    ]:
        if required not in client_text:
            failed.append(("Tushare daily_basic client must expose valuation fields", str(tushare_client.relative_to(ROOT)), [required]))
    for required in [
        "pub async fn moneyflow(",
        "\"moneyflow\"",
        "\"buy_elg_amount\"",
        "\"sell_elg_amount\"",
        "\"net_mf_amount\"",
    ]:
        if required not in client_text:
            failed.append(("Tushare moneyflow client must expose fund-flow fields", str(tushare_client.relative_to(ROOT)), [required]))
    for required in [
        "pub async fn forecast(",
        "\"forecast\"",
        "\"first_ann_date\"",
        "pub async fn express(",
        "\"express\"",
        "\"diluted_roe\"",
        "pub async fn disclosure_date(",
        "\"disclosure_date\"",
        "\"modify_date\"",
    ]:
        if required not in client_text:
            failed.append(("Tushare event client must expose corporate-event fields", str(tushare_client.relative_to(ROOT)), [required]))
    for required in [
        "pub async fn cashflow(",
        "\"cashflow\"",
        "\"f_ann_date\"",
        "\"n_cashflow_act\"",
        "\"c_cash_equ_end_period\"",
        "pub async fn dividend(",
        "\"dividend\"",
        "\"cash_div_tax\"",
        "\"imp_ann_date\"",
        "pub async fn repurchase(",
        "\"repurchase\"",
        "\"high_limit\"",
        "\"low_limit\"",
    ]:
        if required not in client_text:
            failed.append(("Tushare optional source client must expose PIT fields", str(tushare_client.relative_to(ROOT)), [required]))

repository_rs = ROOT / "quant-data/src/repository.rs"
if repository_rs.exists():
    repository_text = repository_rs.read_text(encoding="utf-8")
    for required in [
        "upsert_daily_basic",
        "upsert_daily_basic_batch",
        "INSERT INTO market_stock_daily_basic",
    ]:
        if required not in repository_text:
            failed.append(("daily_basic repository upsert must exist", str(repository_rs.relative_to(ROOT)), [required]))
    for required in [
        "upsert_moneyflow",
        "upsert_moneyflow_batch",
        "INSERT INTO market_stock_moneyflow",
    ]:
        if required not in repository_text:
            failed.append(("moneyflow repository upsert must exist", str(repository_rs.relative_to(ROOT)), [required]))
    for required in [
        "upsert_forecast",
        "upsert_forecast_batch",
        "INSERT INTO market_stock_forecast",
        "ON CONFLICT (symbol, ann_date, end_date, forecast_type, first_ann_date, available_at)",
        "upsert_express",
        "upsert_express_batch",
        "INSERT INTO market_stock_express",
        "ON CONFLICT (symbol, ann_date, end_date, available_at)",
        "upsert_disclosure_date",
        "upsert_disclosure_date_batch",
        "INSERT INTO market_stock_disclosure_date",
        "ON CONFLICT (symbol, end_date, available_at)",
        "list_listed_stock_symbols",
    ]:
        if required not in repository_text:
            failed.append(("event repository upserts must exist", str(repository_rs.relative_to(ROOT)), [required]))
    for required in [
        "upsert_cashflow_batch",
        "INSERT INTO market_stock_cashflow",
        "ON CONFLICT (symbol, end_date, ann_date, available_at)",
        "upsert_dividend_batch",
        "INSERT INTO market_stock_dividend",
        "ON CONFLICT (symbol, end_date, ann_date, div_proc, available_at)",
        "upsert_repurchase_batch",
        "INSERT INTO market_stock_repurchase",
        "ON CONFLICT (symbol, ann_date, end_date, proc, available_at)",
    ]:
        if required not in repository_text:
            failed.append(("optional financial/event repository upsert must exist", str(repository_rs.relative_to(ROOT)), [required]))

sync_routes = ROOT / "quant-api/src/routes/sync.rs"
if sync_routes.exists():
    sync_routes_text = sync_routes.read_text(encoding="utf-8")
    for required in [
        '"daily_basic" | "stock_daily_basic"',
        "sync_daily_basic",
    ]:
        if required not in sync_routes_text:
            failed.append(("daily_basic dataset must be routed", str(sync_routes.relative_to(ROOT)), [required]))
    for required in [
        '"moneyflow" | "stock_moneyflow"',
        "sync_moneyflow",
    ]:
        if required not in sync_routes_text:
            failed.append(("moneyflow dataset must be routed", str(sync_routes.relative_to(ROOT)), [required]))
    for required in [
        '"forecast" | "stock_forecast"',
        "sync_forecast",
        '"express" | "stock_express"',
        "sync_express",
        '"disclosure_date" | "stock_disclosure_date"',
        "sync_disclosure_date",
    ]:
        if required not in sync_routes_text:
            failed.append(("event dataset must be routed", str(sync_routes.relative_to(ROOT)), [required]))
    for required in [
        '"cashflow" | "stock_cashflow"',
        "sync_cashflow",
        '"dividend" | "stock_dividend"',
        "sync_dividend",
        '"repurchase" | "stock_repurchase"',
        "sync_repurchase",
        'mode=full_market',
        "phase7_optional_source_readiness",
        "phase7_optional_source_coverage_sync",
        "phase7_optional_source_coverage_batches",
        "phase7_optional_source_sync_limit",
        "phase7_optional_source_batch_size",
        "bounded_symbols",
        "sample_only_do_not_train",
        "cashflow_quality_pit_features",
        "dividend_stability_quality_pit_features",
        "repurchase_event_capital_return_pit_features",
    ]:
        if required not in sync_routes_text:
            failed.append(("optional financial/event dataset must be routed with explicit full-market guard and coverage audit", str(sync_routes.relative_to(ROOT)), [required]))

combine_rs = ROOT / "quant-factor/src/combine.rs"
if combine_rs.exists():
    combine_text = combine_rs.read_text(encoding="utf-8")
    for required in [
        "INSERT INTO multi_factor_value",
        "available_at",
        "VALUES ($1, $2, $3, $4, $5, $5, $4)",
    ]:
        if required not in combine_text:
            failed.append(("multi_factor_value must persist alpha score available_at", str(combine_rs.relative_to(ROOT)), [required]))

signal_generator = ROOT / "quant-backtest/src/signal_generator.rs"
if signal_generator.exists():
    signal_text = signal_generator.read_text(encoding="utf-8")
    for required in [
        "score_day_for_signal(",
        "generate_prediction_signals(",
        "model_prediction",
        "prediction_load_start_date(",
        "available_at <= trade_date",
        "prediction_score_day_for_signal(",
        "build_portfolio_weights(",
        "select_uncorrelated_candidates(",
        "max_pairwise_correlation",
        "fractional_kelly_weight(",
        "kelly_fraction",
        "max_gross_exposure",
        "load_symbol_return_history(",
        "ScoreDirection",
        "sort_factor_scores(",
        "drawdown_control_v1(",
    ]:
        if required not in signal_text:
            failed.append(("factor signal generation must preserve PIT score timing", str(signal_generator.relative_to(ROOT)), [required]))
    if (
        "available_at IS NULL OR available_at <= trade_date" not in signal_text
        and "mfv.available_at IS NULL OR mfv.available_at <= mfv.trade_date" not in signal_text
    ):
        failed.append((
            "factor signal generation must preserve PIT score timing",
            str(signal_generator.relative_to(ROOT)),
            ["available_at IS NULL OR available_at <= trade_date"],
        ))

backtest_routes = ROOT / "quant-api/src/routes/backtest.rs"
if backtest_routes.exists():
    backtest_text = backtest_routes.read_text(encoding="utf-8")
    for required in [
        "RunPredictionBacktestReq",
        "run_prediction_backtest(",
        "execute_prediction_backtest(",
        "generate_prediction_signals(",
        "prediction_set_id: Some(prediction_set_id)",
        "max_pairwise_correlation",
        "correlation_lookback_days",
        "kelly_fraction",
        "kelly_lookback_days",
        "max_gross_exposure",
        "score_direction",
        "parse_score_direction(",
    ]:
        if required not in backtest_text:
            failed.append(("prediction-set backtest API must bind model predictions into backtest", str(backtest_routes.relative_to(ROOT)), [required]))

paper_routes = ROOT / "quant-api/src/routes/paper.rs"
if paper_routes.exists():
    paper_text = paper_routes.read_text(encoding="utf-8")
    for required in [
        "create_paper_account(",
        "submit_paper_order(",
        "fill_paper_order(",
        "paper_account_summary(",
        "paper_health(",
        "INSERT INTO paper_account",
        "INSERT INTO paper_order",
        "INSERT INTO paper_fill",
        "UPDATE paper_account SET cash",
        "INSERT INTO audit_event",
        "paper_order.risk_reject",
        "evaluate_order_risk(",
    ]:
        if required not in paper_text:
            failed.append(("paper trading API must close account/order/fill/audit loop", str(paper_routes.relative_to(ROOT)), [required]))
    if main_routes.exists():
        main_routes_text = main_routes.read_text(encoding="utf-8")
        for required in [
            '"/api/v1/quant/paper/accounts"',
            '"/api/v1/quant/paper/accounts/{account_id}"',
            '"/api/v1/quant/paper/orders"',
            '"/api/v1/quant/paper/orders/{order_id}/fills"',
            '"/api/v1/quant/paper/health"',
        ]:
            if required not in main_routes_text:
                failed.append(("paper trading API routes must be mounted", str(main_routes.relative_to(ROOT)), [required]))
else:
    failed.append(("paper trading routes module must exist", "quant-api/src/routes/paper.rs", ["missing file"]))

ml_routes = ROOT / "quant-api/src/routes/ml.rs"
if ml_routes.exists():
    ml_text = ml_routes.read_text(encoding="utf-8")
    for required in [
        "create_linear_prediction_set(",
        "train_linear_model(",
        "create_walk_forward_linear_prediction_set(",
        "WalkForwardLinearPredictionSetRequest",
        "walk_forward_covariance_linear_v1",
        "ml_walk_forward_linear",
        "INSERT INTO model_training_task",
        "INSERT INTO training_dataset",
        "INSERT INTO model_registry",
        "INSERT INTO prediction_set",
        "INSERT INTO model_prediction",
        "available_at",
        "available_at IS NULL OR available_at <= trade_date",
        "future_return_label(",
        "fit_linear_weights(",
        "INSERT INTO experiment_run",
        "ml_training_linear",
        "linear_training_experiment_config(",
        "linear_training_experiment_metrics(",
        "evaluate_prediction_set(",
        "ml_prediction_backtest_gate",
        "evaluate_prediction_gates(",
    ]:
        if required not in ml_text:
            failed.append(("ML prediction-set smoke route must persist PIT model predictions", str(ml_routes.relative_to(ROOT)), [required]))
else:
    failed.append(("ML routes module must exist", "quant-api/src/routes/ml.rs", ["missing file"]))

if failed:
    for label, path, hits in failed:
        print(f"FAIL: {label} in {path}: {', '.join(hits)}")
    sys.exit(1)

print("schema alignment checks passed")
