#!/usr/bin/env python3
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

API = "http://127.0.0.1:18080"
OUT = Path(__file__).with_name("rolling_optimization_results.json")

WINDOWS = [
    ("20160405", "20190404"),
    ("20170405", "20200403"),
    ("20180405", "20210402"),
    ("20190405", "20220401"),
    ("20200406", "20230405"),
    ("20210406", "20240405"),
    ("20220406", "20250405"),
    ("20230512", "20260511"),
]

SEARCH_SPACE = {
    "top_n": {"type": "choice", "values": [30, 50, 80]},
    "rebalance": {"type": "choice", "values": ["5", "20"]},
    "max_position_pct": {"type": "choice", "values": [0.05, 0.08]},
    "skip_top_pct": {"type": "choice", "values": [0.0, 0.05]},
    "min_amount": {"type": "choice", "values": [0.0, 50_000_000.0]},
    "max_pairwise_correlation": {"type": "choice", "values": [0.75, 0.90]},
    "correlation_lookback_days": {"type": "choice", "values": [60]},
    "kelly_fraction": {"type": "choice", "values": [0.0, 0.25]},
    "kelly_lookback_days": {"type": "choice", "values": [60]},
    "max_gross_exposure": {"type": "choice", "values": [0.90, 1.0]},
}

OBJECTIVE = {"type": "risk_adjusted", "maximize": True}
CONSTRAINTS = {
    "max_drawdown": 0.60,
    "max_turnover": 80.0,
    "min_trade_count": 20,
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


def post(path, payload, timeout=1200):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        f"{API}{path}",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} {path}: {body}") from exc


def psql_json(sql):
    cmd = ["psql", "postgres://gaocheng@localhost/quant", "-Atc", sql]
    raw = subprocess.check_output(cmd, text=True)
    return [json.loads(line) for line in raw.splitlines() if line.strip()]


def best_trial(task_id):
    rows = psql_json(
        f"""
        SELECT json_build_object(
            'optimization_task_id', optimization_task_id,
            'trial_id', trial_id,
            'backtest_task_id', backtest_task_id,
            'score', score,
            'parameters', parameters,
            'metrics', metrics,
            'constraint_violations', constraint_violations,
            'status', status
        )
        FROM optimization_trial
        WHERE optimization_task_id = '{task_id}'
          AND status = 'completed'
        ORDER BY score DESC NULLS LAST
        LIMIT 1;
        """
    )
    return rows[0] if rows else None


def run_window(idx, start_date, end_date):
    template = {
        "combo_name": "full_icir_16f",
        "version": "1.0.0",
        "benchmark": "000300.SH",
        "start_date": start_date,
        "end_date": end_date,
        "data_version_id": "research-full-2016-2026-20260514",
        "research_dataset_id": "research-full-2016-2026-20260514",
        "initial_capital": 1_000_000.0,
        "mode": "standard",
    }
    create = post(
        "/api/v1/quant/optimizations",
        {
            "strategy_version_id": "long-term-correlation-kelly-v1",
            "data_version_id": "research-full-2016-2026-20260514",
            "search_method": "random_search",
            "search_space": SEARCH_SPACE,
            "objective": OBJECTIVE,
            "constraints": CONSTRAINTS,
            "backtest_template": template,
            "random_seed": 20260514 + idx,
            "max_trials": 4,
        },
    )
    if create.get("code") != 0:
        raise RuntimeError(f"create failed: {create}")
    task_id = create["data"]["optimization_task_id"]
    print(f"[{idx}/{len(WINDOWS)}] created {task_id} {start_date}-{end_date}", flush=True)

    run = post(
        f"/api/v1/quant/optimizations/{task_id}/run",
        {
            "trial_limit": 4,
            "performance_gate": {
                "min_completed_trials": 4,
                "max_failed_trials": 0,
                "max_elapsed_ms": 1_800_000,
            },
        },
        timeout=2400,
    )
    if run.get("code") != 0:
        raise RuntimeError(f"run failed for {task_id}: {run}")
    print(
        f"[{idx}/{len(WINDOWS)}] ran {task_id}: completed={run['data'].get('completed')} elapsed_ms={run['data'].get('elapsed_ms')}",
        flush=True,
    )

    gate = post(
        f"/api/v1/quant/optimizations/{task_id}/robustness-gates",
        {"gate_policy": ROBUSTNESS_POLICY},
        timeout=1200,
    )
    if gate.get("code") != 0:
        print(f"[{idx}/{len(WINDOWS)}] robustness failed: {gate}", flush=True)
    else:
        print(
            f"[{idx}/{len(WINDOWS)}] gate {gate['data'].get('status')}",
            flush=True,
        )

    return {
        "window_index": idx,
        "start_date": start_date,
        "end_date": end_date,
        "optimization_task_id": task_id,
        "create": create,
        "run": run,
        "robustness": gate,
        "best_trial": best_trial(task_id),
    }


def main():
    results = []
    if OUT.exists():
        results = json.loads(OUT.read_text())
    done = {item["window_index"] for item in results}

    started = time.time()
    for idx, (start_date, end_date) in enumerate(WINDOWS, start=1):
        if idx in done:
            continue
        result = run_window(idx, start_date, end_date)
        results.append(result)
        OUT.write_text(json.dumps(results, ensure_ascii=False, indent=2))
    print(f"completed {len(results)} windows in {time.time() - started:.1f}s")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
