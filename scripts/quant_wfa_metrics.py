#!/usr/bin/env python3
"""
Walk-Forward Analysis (WFA) Metrics

用法:
  python3 scripts/quant_wfa_metrics.py --task-id <task_id>
                                       [--train-end DATE]
                                       [--test-start DATE]
                                       [--test-end DATE]
                                       [--step 63]
                                       [--window 756]
                                       [--min-windows 4]
                                       [--min-sharpe 0.30]

执行滚动窗口走前验证（Walk-Forward Analysis），输出每段绩效及汇总。
"""

import argparse
import json
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional

# 确保可以 import 同目录的 quant_common
_THIS_DIR = Path(__file__).resolve().parent
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))

from quant_common import (
    db_connect,
    compute_sharpe,
    compute_annualized_return,
    compute_maxdd,
    compute_calmar,
    compute_annualized_vol,
)
import numpy as np


def fetch_equity_curve(task_id: str, db_url: str = None) -> List[Dict[str, Any]]:
    """从 backtest_equity_curve 获取完整权益曲线（从 portfolio_value 计算 strategy_return）"""
    conn = db_connect(db_url)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT trade_date, portfolio_value, benchmark_value,
                       strategy_return, benchmark_return, drawdown
                FROM backtest_equity_curve
                WHERE task_id = %s
                ORDER BY trade_date
                """,
                (task_id,),
            )
            rows = cur.fetchall()
        result = []
        prev_nav = None
        for r in rows:
            date = str(r[0])
            nav = float(r[1]) if r[1] is not None else None
            bench_nav = float(r[2]) if r[2] is not None else None
            ret = float(r[3]) if r[3] is not None else None
            bench_ret = float(r[4]) if r[4] is not None else None
            dd = float(r[5]) if r[5] is not None else None

            # 如果 ret 为 None 但 nav 有效，从 nav 计算
            if ret is None and nav is not None and prev_nav is not None and prev_nav > 0:
                ret = nav / prev_nav - 1.0

            if nav is not None:
                prev_nav = nav

            result.append({
                "date": date,
                "portfolio_value": nav,
                "benchmark_value": bench_nav,
                "strategy_return": ret,
                "benchmark_return": bench_ret,
                "drawdown": dd,
            })
        return result
    finally:
        conn.close()


def compute_segment_metrics(dates: List[str], returns: List[float], annualize: int = 252) -> Dict[str, Any]:
    """计算单段绩效指标"""
    import numpy as np

    ret_arr = np.array(returns, dtype=float)
    # 构建净值序列（起点为1）
    nav = np.cumprod(1.0 + ret_arr)

    ann_ret = compute_annualized_return(nav, annualize)
    ann_vol = compute_annualized_vol(ret_arr, annualize)
    sharpe = compute_sharpe(ret_arr, annualize)
    maxdd = compute_maxdd(nav)
    calmar = compute_calmar(ann_ret, maxdd)

    # 正收益月占比
    monthly_returns = []
    for i in range(0, len(ret_arr), 21):  # ~每月21个交易日
        month_ret = ret_arr[i:i+21]
        if len(month_ret) > 0:
            monthly_returns.append(float(np.prod(1 + month_ret) - 1))

    pos_month_pct = sum(1 for r in monthly_returns if r > 0) / len(monthly_returns) if monthly_returns else 0.0

    return {
        "start_date": dates[0],
        "end_date": dates[-1],
        "n_days": len(returns),
        "ann_return": round(ann_ret, 4),
        "ann_vol": round(ann_vol, 4),
        "sharpe": round(sharpe, 4),
        "maxdd": round(maxdd, 4),
        "calmar": round(calmar, 4),
        "pos_month_pct": round(pos_month_pct, 4),
    }


def run_wfa(
    task_id: str,
    train_start: Optional[str] = None,
    train_end: Optional[str] = None,
    test_start: Optional[str] = None,
    test_end: Optional[str] = None,
    step: int = 63,
    window: int = 756,
    min_windows: int = 4,
    min_sharpe: float = 0.30,
    db_url: str = None,
    annualize: int = 252,
) -> Dict[str, Any]:
    """
    执行 Walk-Forward Analysis

    参数:
        task_id: 回测任务ID
        train_start: 训练集起始日（默认数据最早日）
        train_end: 训练集结束日（若未指定则按 window 自动切分）
        test_start: 测试集起始日
        test_end: 测试集结束日（默认数据最末日）
        step: 滚动步进（交易日数，默认 63 ≈ 3个月）
        window: 训练窗口（交易日数，默认 756 ≈ 3年）
        min_windows: 最少窗口数
        min_sharpe: 最低中位 Sharpe 阈值
    """
    data = fetch_equity_curve(task_id, db_url)

    if len(data) < 2:
        return {"error": f"task_id={task_id} 数据不足，仅 {len(data)} 行"}

    dates = [d["date"] for d in data]
    returns = [d["strategy_return"] for d in data if d["strategy_return"] is not None]
    navs = [d["portfolio_value"] for d in data if d["portfolio_value"] is not None]

    # 对齐 dates 和 returns
    valid = [(d, r, n) for d, r, n in zip(dates, returns, navs) if r is not None and n is not None]
    dates = [v[0] for v in valid]
    returns = [v[1] for v in valid]
    navs = [v[2] for v in valid]

    n_total = len(returns)

    # 确定分段
    if train_start and train_end and test_start and test_end:
        # 手动指定两段
        segments = []
        for seg_name, seg_start, seg_end in [("train", train_start, train_end), ("test", test_start, test_end)]:
            seg_idx = [(i, d, r, n) for i, (d, r, n) in enumerate(zip(dates, returns, navs))
                       if seg_start <= d <= seg_end]
            if seg_idx:
                seg_dates = [x[1] for x in seg_idx]
                seg_rets = [x[2] for x in seg_idx]
                seg_navs = [x[3] for x in seg_idx]
                metrics = compute_segment_metrics(seg_dates, seg_rets, annualize)
                metrics["segment"] = seg_name
                segments.append(metrics)
    else:
        # 自动滚动窗口 WFA
        segments = []
        start_idx = 0
        window_idx = 0

        while start_idx + window + step <= n_total:
            train_idx_start = start_idx
            train_idx_end = start_idx + window
            test_idx_start = train_idx_end
            test_idx_end = min(test_idx_start + step, n_total)

            if test_idx_end <= test_idx_start:
                break

            # 训练段
            train_dates = dates[train_idx_start:train_idx_end]
            train_rets = returns[train_idx_start:train_idx_end]
            train_metrics = compute_segment_metrics(train_dates, train_rets, annualize)
            train_metrics["segment"] = f"train_w{window_idx}"

            # 测试段
            test_dates = dates[test_idx_start:test_idx_end]
            test_rets = returns[test_idx_start:test_idx_end]
            test_metrics = compute_segment_metrics(test_dates, test_rets, annualize)
            test_metrics["segment"] = f"test_w{window_idx}"

            # 衰减
            sharpe_decay = (
                (train_metrics["sharpe"] - test_metrics["sharpe"]) / abs(train_metrics["sharpe"])
                if train_metrics["sharpe"] != 0 else 0.0
            )

            segments.append({
                "window": window_idx,
                "train": train_metrics,
                "test": test_metrics,
                "sharpe_decay": round(sharpe_decay, 4),
            })

            start_idx += step
            window_idx += 1

    # 汇总
    if train_start and train_end:
        # 手动两段模式
        result = {
            "task_id": task_id,
            "mode": "manual_split",
            "segments": segments,
            "total_days": n_total,
            "date_range": [dates[0], dates[-1]],
        }
    else:
        # 滚动窗口模式
        test_sharpes = [s["test"]["sharpe"] for s in segments if "test" in s]
        test_returns = [s["test"]["ann_return"] for s in segments if "test" in s]
        decay_values = [s["sharpe_decay"] for s in segments]

        median_test_sharpe = float(np.median(test_sharpes)) if test_sharpes else 0.0
        median_test_return = float(np.median(test_returns)) if test_returns else 0.0
        median_decay = float(np.median(decay_values)) if decay_values else 0.0

        passed = (
            len(segments) >= min_windows
            and median_test_sharpe >= min_sharpe
        )

        result = {
            "task_id": task_id,
            "mode": "rolling_wfa",
            "n_windows": len(segments),
            "window_size": window,
            "step": step,
            "min_windows": min_windows,
            "min_sharpe": min_sharpe,
            "summary": {
                "median_test_sharpe": round(median_test_sharpe, 4),
                "median_test_ann_return": round(median_test_return, 4),
                "median_sharpe_decay": round(median_decay, 4),
                "windows_pass_min_sharpe": sum(1 for s in test_sharpes if s >= min_sharpe),
                "total_windows": len(segments),
            },
            "segments": segments,
            "total_days": n_total,
            "date_range": [dates[0], dates[-1]],
            "passed": passed,
        }

    return result


def format_wfa_report(result: dict) -> str:
    """将 WFA 结果格式化为 Markdown 报告"""
    lines = []

    if result.get("mode") == "manual_split":
        lines.append(f"# WFA 分段验证 — {result['task_id']}")
        lines.append("")
        lines.append(f"> 数据范围: {result['date_range'][0]} ~ {result['date_range'][1]} ({result['total_days']} 天)")
        lines.append("")

        for seg in result["segments"]:
            name = seg.get("segment", "unknown")
            lines.append(f"## {name} 段 ({seg['start_date']} ~ {seg['end_date']})")
            lines.append("")
            lines.append(format_metrics_table(seg, title=""))
            lines.append("")
    else:
        lines.append(f"# Walk-Forward Analysis — {result['task_id']}")
        lines.append("")
        lines.append(f"> 数据范围: {result['date_range'][0]} ~ {result['date_range'][1]}")
        lines.append(f"> 窗口: {result['window_size']} 天, 步进: {result['step']} 天, 共 {result['n_windows']} 个窗口")
        lines.append("")

        summary = result["summary"]
        lines.append("## 汇总")
        lines.append("")
        lines.append("| 指标 | 值 |")
        lines.append("|------|-----|")
        lines.append(f"| 中位测试 Sharpe | {summary['median_test_sharpe']:.4f} |")
        lines.append(f"| 中位测试年化收益 | {summary['median_test_ann_return']*100:.2f}% |")
        lines.append(f"| 中位 Sharpe 衰减 | {summary['median_sharpe_decay']*100:.2f}% |")
        lines.append(f"| 通过 Sharpe>{result['min_sharpe']} 窗口数 | {summary['windows_pass_min_sharpe']}/{summary['total_windows']} |")
        lines.append(f"| 整体判定 | {'✅ 通过' if result['passed'] else '❌ 未通过'} |")
        lines.append("")

        lines.append("## 逐窗口结果")
        lines.append("")
        lines.append("| 窗口 | 训练 Sharpe | 测试 Sharpe | 衰减 | 测试年化 | 测试 MaxDD |")
        lines.append("|------|------------|------------|------|---------|-----------|")
        for seg in result["segments"]:
            lines.append(
                f"| W{seg['window']} | {seg['train']['sharpe']:.4f} | "
                f"{seg['test']['sharpe']:.4f} | {seg['sharpe_decay']*100:.1f}% | "
                f"{seg['test']['ann_return']*100:.2f}% | {seg['test']['maxdd']*100:.2f}% |"
            )
        lines.append("")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="WFA 训练/测试分段绩效对比")
    parser.add_argument("--task-id", required=True, help="回测 task_id")
    parser.add_argument("--train-start", help="训练段起始日 YYYY-MM-DD")
    parser.add_argument("--train-end", help="训练段结束日 YYYY-MM-DD")
    parser.add_argument("--test-start", help="测试段起始日 YYYY-MM-DD")
    parser.add_argument("--test-end", help="测试段结束日 YYYY-MM-DD")
    parser.add_argument("--step", type=int, default=63, help="滚动步进（交易日，默认 63）")
    parser.add_argument("--window", type=int, default=756, help="训练窗口（交易日，默认 756）")
    parser.add_argument("--min-windows", type=int, default=4, help="最少窗口数")
    parser.add_argument("--min-sharpe", type=float, default=0.30, help="最低中位 Sharpe 阈值")
    parser.add_argument("--db-url", help="PostgreSQL URL")
    parser.add_argument("--annualize", type=int, default=252, help="年化交易日数")
    parser.add_argument("--output", "-o", help="输出 JSON 文件路径")
    parser.add_argument("--report", help="输出 Markdown 报告路径")

    args = parser.parse_args()

    result = run_wfa(
        task_id=args.task_id,
        train_start=args.train_start,
        train_end=args.train_end,
        test_start=args.test_start,
        test_end=args.test_end,
        step=args.step,
        window=args.window,
        min_windows=args.min_windows,
        min_sharpe=args.min_sharpe,
        db_url=args.db_url,
        annualize=args.annualize,
    )

    result_json = json.dumps(result, ensure_ascii=False, indent=2)

    if args.output:
        Path(args.output).write_text(result_json, encoding="utf-8")
        print(f"结果已写入: {args.output}", file=sys.stderr)
    else:
        print(result_json)

    if args.report:
        report_md = format_wfa_report(result)
        Path(args.report).write_text(report_md, encoding="utf-8")
        print(f"报告已写入: {args.report}", file=sys.stderr)


if __name__ == "__main__":
    main()
