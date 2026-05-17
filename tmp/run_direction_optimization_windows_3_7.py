#!/usr/bin/env python3
import json
from pathlib import Path

import run_rolling_optimization as base

base.OUT = Path(__file__).with_name("direction_optimization_windows_3_7_results.json")
TARGET_WINDOW_INDEXES = {3, 7}
COMBO_VERSIONS = [
    ("full_eq_16f", "1.0.0"),
    ("full_icir_16f", "1.0.0"),
    ("full_icir_16f_v2", "20260511"),
    ("full_icir_16f_v3", "1.0.0"),
]

base.SEARCH_SPACE = {
    "top_n": {"type": "choice", "values": [30, 50, 80]},
    "rebalance": {"type": "choice", "values": ["20"]},
    "max_position_pct": {"type": "choice", "values": [0.08]},
    "skip_top_pct": {"type": "choice", "values": [0.0, 0.05]},
    "min_amount": {"type": "choice", "values": [0.0]},
    "max_pairwise_correlation": {"type": "choice", "values": [0.75, 0.90]},
    "correlation_lookback_days": {"type": "choice", "values": [60]},
    "kelly_fraction": {"type": "choice", "values": [0.0, 0.25]},
    "kelly_lookback_days": {"type": "choice", "values": [60]},
    "max_gross_exposure": {"type": "choice", "values": [1.0]},
    "score_direction": {"type": "choice", "values": ["descending", "ascending"]},
}
base.CONSTRAINTS = {
    "max_drawdown": 0.60,
    "max_turnover": 120.0,
    "min_trade_count": 20,
}
base.ROBUSTNESS_POLICY = {
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


def run_combo_window(idx, start_date, end_date, combo_name, version, combo_idx):
    template = {
        "combo_name": combo_name,
        "version": version,
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
            "search_space": base.SEARCH_SPACE,
            "objective": base.OBJECTIVE,
            "constraints": base.CONSTRAINTS,
            "backtest_template": template,
            "random_seed": 20260600 + idx * 100 + combo_idx,
            "max_trials": 8,
        },
    )
    if create.get("code") != 0:
        raise RuntimeError(f"create failed: {create}")
    task_id = create["data"]["optimization_task_id"]
    print(f"[{idx}/{len(base.WINDOWS)}] created {task_id} {start_date}-{end_date} {combo_name}@{version}", flush=True)

    run = base.post(
        f"/api/v1/quant/optimizations/{task_id}/run",
        {
            "trial_limit": 8,
            "performance_gate": {
                "min_completed_trials": 8,
                "max_failed_trials": 0,
                "max_elapsed_ms": 2_400_000,
            },
        },
        timeout=3000,
    )
    if run.get("code") != 0:
        raise RuntimeError(f"run failed for {task_id}: {run}")
    print(
        f"[{idx}/{len(base.WINDOWS)}] ran {task_id}: completed={run['data'].get('completed')} elapsed_ms={run['data'].get('elapsed_ms')}",
        flush=True,
    )

    gate = base.post(
        f"/api/v1/quant/optimizations/{task_id}/robustness-gates",
        {"gate_policy": base.ROBUSTNESS_POLICY},
        timeout=1200,
    )
    print(f"[{idx}/{len(base.WINDOWS)}] gate {gate.get('data', {}).get('status')}", flush=True)

    return {
        "window_index": idx,
        "start_date": start_date,
        "end_date": end_date,
        "combo_name": combo_name,
        "version": version,
        "optimization_task_id": task_id,
        "create": create,
        "run": run,
        "robustness": gate,
        "best_trial": base.best_trial(task_id),
    }


def main():
    results = []
    if base.OUT.exists():
        results = json.loads(base.OUT.read_text())
    done = {
        (
            item["window_index"],
            item.get("combo_name", item.get("best_trial", {}).get("parameters", {}).get("combo_name")),
            item.get("version", item.get("best_trial", {}).get("parameters", {}).get("version", "1.0.0")),
        )
        for item in results
    }
    for idx, (start_date, end_date) in enumerate(base.WINDOWS, start=1):
        if idx not in TARGET_WINDOW_INDEXES:
            continue
        for combo_idx, (combo_name, version) in enumerate(COMBO_VERSIONS, start=1):
            if (idx, combo_name, version) in done:
                continue
            results.append(run_combo_window(idx, start_date, end_date, combo_name, version, combo_idx))
            base.OUT.write_text(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
