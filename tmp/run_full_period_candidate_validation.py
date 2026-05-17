#!/usr/bin/env python3
import json
from pathlib import Path

import run_fixed_candidate_validation as fixed
import run_rolling_optimization as base

OUT = Path(__file__).with_name("full_period_candidate_validation_results.json")
START_DATE = "20160405"
END_DATE = "20260511"


def run_candidate(candidate):
    candidate_id = candidate["candidate_id"]
    template = {
        "combo_name": candidate["combo_name"],
        "version": candidate["version"],
        "benchmark": "000300.SH",
        "start_date": START_DATE,
        "end_date": END_DATE,
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
            "search_space": fixed.fixed_search_space(candidate),
            "objective": base.OBJECTIVE,
            "constraints": {
                "max_drawdown": 0.60,
                "max_turnover": 120.0,
                "min_trade_count": 20,
            },
            "backtest_template": template,
            "random_seed": 20260801,
            "max_trials": 1,
        },
    )
    if create.get("code") != 0:
        raise RuntimeError(f"create failed: {create}")
    task_id = create["data"]["optimization_task_id"]
    print(f"[{candidate_id}] created {task_id} {START_DATE}-{END_DATE}", flush=True)

    run = base.post(
        f"/api/v1/quant/optimizations/{task_id}/run",
        {
            "trial_limit": 1,
            "performance_gate": {
                "min_completed_trials": 1,
                "max_failed_trials": 0,
                "max_elapsed_ms": 2_400_000,
            },
        },
        timeout=3000,
    )
    if run.get("code") != 0:
        raise RuntimeError(f"run failed for {task_id}: {run}")
    gate = base.post(
        f"/api/v1/quant/optimizations/{task_id}/robustness-gates",
        {"gate_policy": fixed.ROBUSTNESS_POLICY},
        timeout=1800,
    )
    print(f"[{candidate_id}] gate {gate.get('data', {}).get('status')}", flush=True)
    return {
        "candidate_id": candidate_id,
        "start_date": START_DATE,
        "end_date": END_DATE,
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
    done = {item["candidate_id"] for item in results}
    for candidate in fixed.CANDIDATES:
        if candidate["candidate_id"] in done:
            continue
        results.append(run_candidate(candidate))
        OUT.write_text(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
