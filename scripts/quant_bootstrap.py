#!/usr/bin/env python3
"""
Bootstrap Sharpe Ratio Confidence Interval Analysis

用法:
  python3 scripts/quant_bootstrap.py --task-id <task_id> [--account-id <account_id>]
                                     [--n-bootstrap 2000] [--confidence 95]
                                     [--strategy-start DATE] [--strategy-end DATE]

从数据库读取日收益率序列，执行 bootstrap 重采样计算 Sharpe 比率的置信区间。
输出 JSON 格式结果到标准输出，便于其他脚本或报告程序解析。
"""

import argparse
import json
import math
import random
import sys
from pathlib import Path

# 确保可以 import 同目录的 quant_common
_THIS_DIR = Path(__file__).resolve().parent
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))

from quant_common import (
    compute_sharpe,
    compute_annualized_return,
    compute_maxdd,
    db_connect,
)


def fetch_returns_by_task(task_id: str, db_url: str = None) -> list:
    """从 backtest_equity_curve 获取日收益率序列（从 portfolio_value 计算）"""
    conn = db_connect(db_url)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT trade_date, portfolio_value
                FROM backtest_equity_curve
                WHERE task_id = %s
                  AND portfolio_value IS NOT NULL
                ORDER BY trade_date
                """,
                (task_id,),
            )
            rows = cur.fetchall()
        # 从 portfolio_value 计算日收益率
        if len(rows) < 2:
            return []
        results = []
        prev_nav = float(rows[0][1])
        for i in range(1, len(rows)):
            nav = float(rows[i][1])
            if prev_nav > 0:
                ret = nav / prev_nav - 1.0
                results.append((rows[i][0], ret))
            prev_nav = nav
        return results
    finally:
        conn.close()


def fetch_returns_by_account(account_id: str, db_url: str = None) -> list:
    """从 paper_nav_snapshot 获取日收益率序列（优先 daily_return，回退从 nav 计算）"""
    conn = db_connect(db_url)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT snapshot_date, daily_return, nav
                FROM paper_nav_snapshot
                WHERE paper_account_id = %s
                ORDER BY snapshot_date
                """,
                (account_id,),
            )
            rows = cur.fetchall()
        # 优先使用 daily_return，为 NULL 时从 nav 计算
        if len(rows) < 2:
            return []
        results = []
        prev_nav = float(rows[0][2]) if rows[0][2] is not None else None
        for i in range(1, len(rows)):
            date = rows[i][0]
            daily_ret = float(rows[i][1]) if rows[i][1] is not None else None
            nav = float(rows[i][2]) if rows[i][2] is not None else None
            if daily_ret is not None:
                results.append((date, daily_ret))
            elif nav is not None and prev_nav is not None and prev_nav > 0:
                results.append((date, nav / prev_nav - 1.0))
            if nav is not None:
                prev_nav = nav
        return results
    finally:
        conn.close()


def bootstrap_analysis(
    returns: list,
    n_bootstrap: int = 2000,
    seed: int = 20260713,
    confidence: int = 95,
    annualize: int = 252,
) -> dict:
    """
    对日收益率序列执行 bootstrap 分析。

    Returns:
        dict 包含：
        - sample_size: 样本数
        - raw_sharpe: 原始 Sharpe
        - raw_annual_return: 原始年化收益率
        - bootstrap: {n, confidence, ci_lower, ci_upper, median, mean, std}
        - judgment: 关于蓝图目标的判断
    """
    import numpy as np

    arr = np.array(returns, dtype=float)
    n = len(arr)

    raw_sharpe = compute_sharpe(arr, annualize)
    raw_annual = compute_annualized_return(
        np.cumprod(1 + arr) if n > 0 else np.array([1.0]), annualize
    )

    rng = np.random.default_rng(seed)
    boot_sharpes = np.empty(n_bootstrap, dtype=float)
    boot_annuals = np.empty(n_bootstrap, dtype=float)

    for i in range(n_bootstrap):
        sample = rng.choice(arr, size=n, replace=True)
        boot_sharpes[i] = compute_sharpe(sample, annualize)
        # 年化收益用累积方式
        cum = np.cumprod(1 + sample)
        boot_annuals[i] = cum[-1] ** (annualize / n) - 1

    alpha = (100 - confidence) / 2
    ci_lower = float(np.percentile(boot_sharpes, alpha))
    ci_upper = float(np.percentile(boot_sharpes, 100 - alpha))

    ann_ci_lower = float(np.percentile(boot_annuals, alpha))
    ann_ci_upper = float(np.percentile(boot_annuals, 100 - alpha))

    return {
        "sample_size": n,
        "raw_sharpe": round(raw_sharpe, 4),
        "raw_annual_return": round(raw_annual, 4),
        "bootstrap": {
            "n": n_bootstrap,
            "confidence": confidence,
            "sharpe_ci": [round(ci_lower, 4), round(ci_upper, 4)],
            "annual_return_ci": [round(ann_ci_lower, 4), round(ann_ci_upper, 4)],
            "sharpe_median": round(float(np.median(boot_sharpes)), 4),
            "sharpe_mean": round(float(np.mean(boot_sharpes)), 4),
            "sharpe_std": round(float(np.std(boot_sharpes, ddof=1)), 4),
        },
        "judgment": {
            "sharpe_robust_above_1": ci_lower > 1.0,
            "positive_significant": ci_lower > 0,
            "ci_width": round(ci_upper - ci_lower, 4),
            "note": (
                "CI 下界 > 1.0 → 统计显著达标"
                if ci_lower > 1.0
                else (
                    "CI 下界 > 0 → 正收益显著但 Sharpe 未稳健达标"
                    if ci_lower > 0
                    else "CI 下界 <= 0 → 收益统计上不显著"
                )
            ),
        },
    }


def main():
    parser = argparse.ArgumentParser(description="Bootstrap Sharpe 置信区间分析")
    parser.add_argument("--task-id", help="backtest task_id")
    parser.add_argument("--account-id", help="paper account id")
    parser.add_argument(
        "--csv", help="直接从 CSV 文件读取（跳过数据库）", default=None
    )
    parser.add_argument(
        "--n-bootstrap", type=int, default=2000, help="重采样次数 (默认 2000)"
    )
    parser.add_argument("--confidence", type=int, default=95, help="置信度 %% (默认 95)")
    parser.add_argument("--seed", type=int, default=20260713, help="随机种子")
    parser.add_argument("--annualize", type=int, default=252, help="年化交易日数")
    parser.add_argument("--db-url", help="PostgreSQL 连接 URL")
    parser.add_argument("--output", "-o", help="输出 JSON 文件路径（默认 stdout）")

    args = parser.parse_args()

    if args.csv:
        # 从 CSV 读取
        from quant_common import read_csv_returns

        _, rets = read_csv_returns(args.csv)
        returns_list = rets.tolist()
    elif args.task_id:
        data = fetch_returns_by_task(args.task_id, args.db_url)
        returns_list = [r[1] for r in data]
    elif args.account_id:
        data = fetch_returns_by_account(args.account_id, args.db_url)
        returns_list = [float(r[1]) for r in data]
    else:
        parser.error("必须指定 --task-id, --account-id 或 --csv")

    if len(returns_list) < 30:
        print(
            f"警告：样本量过小 ({len(returns_list)} 天)，结果可能不可靠",
            file=sys.stderr,
        )

    result = bootstrap_analysis(
        returns_list,
        n_bootstrap=args.n_bootstrap,
        seed=args.seed,
        confidence=args.confidence,
        annualize=args.annualize,
    )

    result_json = json.dumps(result, ensure_ascii=False, indent=2)

    if args.output:
        Path(args.output).write_text(result_json, encoding="utf-8")
        print(f"结果已写入: {args.output}", file=sys.stderr)
    else:
        print(result_json)


if __name__ == "__main__":
    main()
