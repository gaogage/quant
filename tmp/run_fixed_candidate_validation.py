#!/usr/bin/env python3
import json
from pathlib import Path

import run_rolling_optimization as base

OUT = Path(__file__).with_name("fixed_candidate_validation_results.json")

CANDIDATES = [
    {
        "candidate_id": "eq_asc_top30_nokelly_corr09",
        "combo_name": "full_eq_16f",
        "version": "1.0.0",
        "top_n": 30,
        "rebalance": "20",
        "skip_top_pct": 0.0,
        "kelly_fraction": 0.0,
        "score_direction": "ascending",
        "max_pairwise_correlation": 0.9,
    },
    {
        "candidate_id": "icir_asc_top30_nokelly_corr09",
        "combo_name": "full_icir_16f",
        "version": "1.0.0",
        "top_n": 30,
        "rebalance": "20",
        "skip_top_pct": 0.0,
        "kelly_fraction": 0.0,
        "score_direction": "ascending",
        "max_pairwise_correlation": 0.9,
    },
    {
        "candidate_id": "icir_v2_asc_top30_nokelly_corr09",
        "combo_name": "full_icir_16f_v2",
        "version": "20260511",
        "top_n": 30,
        "rebalance": "20",
        "skip_top_pct": 0.0,
        "kelly_fraction": 0.0,
        "score_direction": "ascending",
        "max_pairwise_correlation": 0.9,
    },
    {
        "candidate_id": "icir_v3_desc_top30_kelly025_corr075",
        "combo_name": "full_icir_16f_v3",
        "version": "1.0.0",
        "top_n": 30,
        "rebalance": "20",
        "skip_top_pct": 0.05,
        "kelly_fraction": 0.25,
        "score_direction": "descending",
        "max_pairwise_correlation": 0.75,
    },
    {
        "candidate_id": "icir_v3_desc_top80_kelly025_corr075",
        "combo_name": "full_icir_16f_v3",
        "version": "1.0.0",
        "top_n": 80,
        "rebalance": "20",
        "skip_top_pct": 0.05,
        "kelly_fraction": 0.25,
        "score_direction": "descending",
        "max_pairwise_correlation": 0.75,
    },
]

FIXED_DEFAULTS = {
    "max_position_pct": 0.08,
    "min_amount": 0.0,
    "max_gross_exposure": 1.0,
    "kelly_lookback_days": 60,
    "correlation_lookback_days": 60,
}

ROBUSTNESS_POLICY = {
    "min_trade_count": 20,
    "max_drawdown": 0.60,
    "min_score_gap": 0.0,
    "walk_forward_window_days": 252,
    "walk_forward_step_days": 63,
    "bootstrap_trials": 128,
    "bootstrap_seed": 20260514,
    "min_walk_forward_windows": 3,
    "min_positive_excess_window_ratio": 0.35,
    "min_bootstrap_positive_return_probability": 0.35,
    "min_market_scenarios": 1,
}


def fixed_search_space(candidate):
    params = {**FIXED_DEFAULTS, **candidate}
    params.pop("candidate_id")
    params.pop("combo_name")
    params.pop("version")
    return {name: {"type": "choice", "values": [value]} for name, value in params.items()}


def run_candidate_window(idx, start_date, end_date, candidate):
    candidate_id = candidate["candidate_id"]
    template = {
        "combo_name": candidate["combo_name"],
        "version": candidate["version"],
        "benchmark": "000300.SH",
        "start_date": start_date,
        "end_date": end_date,
        "data_version_id": "research-full-2016-2026-20260514",
        "research_dataset_id": "research-full-2016-2026-20260514",
        "initial_capital": 1_000_000.0,
        "mode": "standard",
    }
    create = base.post(
        "/api/v1/quant/optimizations",
        {
            "strategy_version_id": "long-term-correlation-kelly-v1",
            "data_version_id": "research-full-2016-2026-20260514",
            "search_method": "random_search",
            "search_space": fixed_search_space(candidate),
            "objective": base.OBJECTIVE,
            "constraints": {
                "max_drawdown": 0.60,
                "max_turnover": 120.0,
                "min_trade_count": 20,
            },
            "backtest_template": template,
            "random_seed": 20260700 + idx,
            "max_trials": 1,
        },
    )
    if create.get("code") != 0:
        raise RuntimeError(f"create failed: {create}")
    task_id = create["data"]["optimization_task_id"]
    print(f"[{candidate_id} W{idx}] created {task_id} {start_date}-{end_date}", flush=True)

    run = base.post(
        f"/api/v1/quant/optimizations/{task_id}/run",
        {
            "trial_limit": 1,
            "performance_gate": {
                "min_completed_trials": 1,
                "max_failed_trials": 0,
                "max_elapsed_ms": 900_000,
            },
        },
        timeout=1200,
    )
    if run.get("code") != 0:
        raise RuntimeError(f"run failed for {task_id}: {run}")
    gate = base.post(
        f"/api/v1/quant/optimizations/{task_id}/robustness-gates",
        {"gate_policy": ROBUSTNESS_POLICY},
        timeout=1200,
    )
    print(f"[{candidate_id} W{idx}] gate {gate.get('data', {}).get('status')}", flush=True)
    return {
        "candidate_id": candidate_id,
        "window_index": idx,
        "start_date": start_date,
        "end_date": end_date,
        "optimization_task_id": task_id,
        "create": create,
        "run": run,
        "robustness": gate,
        "best_trial": base.best_trial(task_id),
    }


def main():
    results = []
    if OUT.exists():
        results = json.loads(OUT.read_text())
    done = {(item["candidate_id"], item["window_index"]) for item in results}
    for candidate in CANDIDATES:
        for idx, (start_date, end_date) in enumerate(base.WINDOWS, start=1):
            key = (candidate["candidate_id"], idx)
            if key in done:
                continue
            results.append(run_candidate_window(idx, start_date, end_date, candidate))
            OUT.write_text(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
