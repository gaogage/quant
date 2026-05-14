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
        "ann_date DATE NOT NULL",
        "idx_fin_ind_ann_date",
        "CREATE TABLE IF NOT EXISTS public.factor_evaluation",
        "CREATE TABLE IF NOT EXISTS public.multi_factor_weight",
        "CREATE TABLE IF NOT EXISTS public.multi_factor_value",
        "available_at DATE NULL",
        "idx_multi_factor_value_combo_date",
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
        '"/api/v1/quant/optimizations"',
        '"/api/v1/quant/optimizations/{optimization_task_id}"',
        '"/api/v1/quant/optimizations/{optimization_task_id}/trials"',
        '"/api/v1/quant/optimizations/{optimization_task_id}/run"',
        '"/api/v1/quant/optimizations/{optimization_task_id}/promote"',
        '"/api/v1/quant/optimizations/{optimization_task_id}/robustness-gates"',
        '"/api/v1/quant/ml/prediction-sets/linear-smoke"',
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
        "score_trial(",
        "trial_reuse_key(",
        "find_reusable_trial(",
        "strategy_parameter_candidate",
        "INSERT INTO audit_event",
        "INSERT INTO robustness_gate_result",
        "generate_trial_parameters(",
        "DeterministicRng",
        "INSERT INTO optimization_task",
        "INSERT INTO optimization_trial",
        "backtest_template",
        "UPDATE optimization_trial",
    ]:
        if required not in optimization_text:
            failed.append(("optimization task/trial API must be present", str(optimization_routes.relative_to(ROOT)), [required]))

if schema_sql.exists():
    schema_text = schema_sql.read_text(encoding="utf-8")
    optimization_task_block = schema_text.split("CREATE TABLE IF NOT EXISTS public.optimization_task", 1)[-1]
    optimization_task_block = optimization_task_block.split("CREATE INDEX IF NOT EXISTS idx_optimization_task_status_created_at", 1)[0]
    if "backtest_template JSONB NULL" not in optimization_task_block:
        failed.append(("optimization_task schema must include backtest_template", str(schema_sql.relative_to(ROOT.parent)), ["missing backtest_template JSONB NULL"]))
    backtest_task_block = schema_text.split("CREATE TABLE IF NOT EXISTS public.backtest_task", 1)[-1]
    backtest_task_block = backtest_task_block.split("CREATE INDEX IF NOT EXISTS idx_backtest_task_status_created_at", 1)[0]
    if "prediction_set_id VARCHAR(64) NULL" not in backtest_task_block:
        failed.append(("backtest_task schema must include prediction_set_id", str(schema_sql.relative_to(ROOT.parent)), ["missing prediction_set_id VARCHAR(64) NULL"]))
    if "fk_backtest_task_prediction_set" not in schema_text:
        failed.append(("backtest_task schema must FK prediction_set", str(schema_sql.relative_to(ROOT.parent)), ["missing fk_backtest_task_prediction_set"]))
else:
    failed.append(("optimization routes module must exist", "quant-api/src/routes/optimization.rs", ["missing file"]))

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
        "available_at IS NULL OR available_at <= trade_date",
        "generate_prediction_signals(",
        "model_prediction",
        "available_at <= trade_date",
        "prediction_score_day_for_signal(",
    ]:
        if required not in signal_text:
            failed.append(("factor signal generation must preserve PIT score timing", str(signal_generator.relative_to(ROOT)), [required]))

backtest_routes = ROOT / "quant-api/src/routes/backtest.rs"
if backtest_routes.exists():
    backtest_text = backtest_routes.read_text(encoding="utf-8")
    for required in [
        "RunPredictionBacktestReq",
        "run_prediction_backtest(",
        "execute_prediction_backtest(",
        "generate_prediction_signals(",
        "prediction_set_id: Some(prediction_set_id)",
    ]:
        if required not in backtest_text:
            failed.append(("prediction-set backtest API must bind model predictions into backtest", str(backtest_routes.relative_to(ROOT)), [required]))

ml_routes = ROOT / "quant-api/src/routes/ml.rs"
if ml_routes.exists():
    ml_text = ml_routes.read_text(encoding="utf-8")
    for required in [
        "create_linear_prediction_set(",
        "INSERT INTO training_dataset",
        "INSERT INTO model_registry",
        "INSERT INTO prediction_set",
        "INSERT INTO model_prediction",
        "available_at",
        "available_at IS NULL OR available_at <= trade_date",
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
