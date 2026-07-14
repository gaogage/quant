#!/usr/bin/env python3
"""
年度收益分解分析

用法:
  python3 scripts/quant_annual_breakdown.py --task-id <task_id>
                                            [--account-id <account_id>]
                                            [--benchmark-task-id <benchmark_task_id>]
                                            [--benchmark-account-id <benchmark_account_id>]

按年度计算年化收益率、波动率、Sharpe、MaxDD 等指标。
可选对比基准（如 h1 vs h20 年度收益对比）。
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
    compute_annualized_vol,
    compute_calmar,
)
import numpy as np


def fetch_daily_data(task_id: str = None, account_id: str = None, db_url: str = None) -> List[Dict]:
    """从数据库获取日收益数据（自动选择表）"""
    conn = db_connect(db_url)
    try:
        with conn.cursor() as cur:
            if task_id:
                cur.execute(
                    """
                    SELECT trade_date, strategy_return, portfolio_value,
                           benchmark_return, drawdown
                    FROM backtest_equity_curve
                    WHERE task_id = %s
                    ORDER BY trade_date
                    """,
                    (task_id,),
                )
            elif account_id:
                cur.execute(
                    """
                    SELECT snapshot_date AS trade_date, daily_return AS strategy_return,
                           nav AS portfolio_value, benchmark_return, max_drawdown AS drawdown
                    FROM paper_nav_snapshot
                    WHERE paper_account_id = %s
                    ORDER BY snapshot_date
                    """,
                    (account_id,),
                )
            else:
                raise ValueError("必须指定 --task-id 或 --account-id")
            rows = cur.fetchall()

        result = []
        for r in rows:
            date = str(r[0])
            ret = float(r[1]) if r[1] is not None else None
            nav = float(r[2]) if r[2] is not None else None
            bench = float(r[3]) if r[3] is not None else None
            dd = float(r[4]) if len(r) > 4 and r[4] is not None else None

            # 如果 ret 为 None 但 nav 有效，从 nav 计算日收益率
            if ret is None and nav is not None and len(result) > 0:
                prev_nav = result[-1]["_nav"]
                if prev_nav and prev_nav > 0:
                    ret = nav / prev_nav - 1.0

            result.append({
                "date": date,
                "strategy_return": ret,
                "portfolio_value": nav,
                "benchmark_return": bench,
                "drawdown": dd,
                "_nav": nav,  # 内部使用，用于计算日收益率
            })
        return result
    finally:
        conn.close()


def compute_annual_breakdown(data: List[Dict], annualize: int = 252) -> Dict[str, Any]:
    """
    按年度计算绩效指标

    Returns:
        dict 包含：
        - overall: 全周期指标
        - annual: {year: metrics} 各年度指标
        - years: [year_list] 年份列表
    """
    if not data:
        return {"error": "无数据"}

    # 过滤有效数据
    valid = [(d, r, n) for d, r, n in
             zip([x["date"] for x in data],
                 [x["strategy_return"] for x in data],
                 [x["portfolio_value"] for x in data])
             if r is not None and n is not None]

    if not valid:
        return {"error": "无有效数据"}

    dates = [v[0] for v in valid]
    returns = [v[1] for v in valid]
    navs = [v[2] for v in valid]

    # 全周期指标
    all_ret_arr = np.array(returns, dtype=float)
    all_nav_arr = np.array(navs, dtype=float)

    overall = {
        "start_date": dates[0],
        "end_date": dates[-1],
        "n_days": len(returns),
        "ann_return": round(compute_annualized_return(all_nav_arr, annualize), 4),
        "ann_vol": round(compute_annualized_vol(all_ret_arr, annualize), 4),
        "sharpe": round(compute_sharpe(all_ret_arr, annualize), 4),
        "maxdd": round(compute_maxdd(all_nav_arr), 4),
    }
    overall["calmar"] = round(compute_calmar(overall["ann_return"], overall["maxdd"]), 4)

    # 按年分组
    from collections import defaultdict
    yearly = defaultdict(lambda: {"dates": [], "returns": [], "navs": []})

    for d, r, n in zip(dates, returns, navs):
        year = int(d[:4])
        yearly[year]["dates"].append(d)
        yearly[year]["returns"].append(r)
        yearly[year]["navs"].append(n)

    annual = {}
    for year in sorted(yearly.keys()):
        yd = yearly[year]
        ret_arr = np.array(yd["returns"], dtype=float)
        nav_arr = np.array(yd["navs"], dtype=float)

        # 年内净值归一化（起点=1）
        nav_norm = nav_arr / nav_arr[0]

        ann = {
            "start_date": yd["dates"][0],
            "end_date": yd["dates"][-1],
            "n_days": len(yd["returns"]),
            "ann_return": round(compute_annualized_return(nav_norm, annualize), 4),
            "ann_vol": round(compute_annualized_vol(ret_arr, annualize), 4),
            "sharpe": round(compute_sharpe(ret_arr, annualize), 4),
            "maxdd": round(compute_maxdd(nav_norm), 4),
        }
        ann["calmar"] = round(compute_calmar(ann["ann_return"], ann["maxdd"]), 4)

        # 正收益月占比
        monthly_rets = []
        for i in range(0, len(ret_arr), 21):
            m = ret_arr[i:i+21]
            if len(m) > 0:
                monthly_rets.append(float(np.prod(1 + m) - 1))
        ann["pos_month_pct"] = round(
            sum(1 for r in monthly_rets if r > 0) / len(monthly_rets) if monthly_rets else 0.0, 4
        )

        annual[year] = ann

    return {
        "overall": overall,
        "annual": annual,
        "years": sorted(annual.keys()),
    }


def compare_annual_breakdown(
    strategy_data: List[Dict],
    benchmark_data: List[Dict],
    annualize: int = 252,
) -> Dict[str, Any]:
    """
    对比两个策略的年度收益

    Returns:
        dict 包含：
        - strategy: 策略的年度分解
        - benchmark: 基准的年度分解
        - comparison: [{year: {strategy, benchmark, diff}}] 年度对比
    """
    strategy = compute_annual_breakdown(strategy_data, annualize)
    benchmark = compute_annual_breakdown(benchmark_data, annualize)

    if "error" in strategy or "error" in benchmark:
        return {"error": "数据不足，无法对比"}

    # 取两者共有的年份
    common_years = sorted(set(strategy["years"]) & set(benchmark["years"]))

    comparison = []
    for year in common_years:
        s = strategy["annual"][year]
        b = benchmark["annual"][year]
        comparison.append({
            "year": year,
            "strategy_ann_return": s["ann_return"],
            "benchmark_ann_return": b["ann_return"],
            "diff": round(s["ann_return"] - b["ann_return"], 4),
            "strategy_sharpe": s["sharpe"],
            "benchmark_sharpe": b["sharpe"],
            "strategy_maxdd": s["maxdd"],
            "benchmark_maxdd": b["maxdd"],
        })

    return {
        "strategy": strategy,
        "benchmark": benchmark,
        "comparison": comparison,
    }


def format_annual_report(result: dict, title: str = "年度收益分解") -> str:
    """格式化为 Markdown 报告"""
    lines = []
    lines.append(f"# {title}")
    lines.append("")

    if "error" in result:
        lines.append(f"❌ {result['error']}")
        return "\n".join(lines)

    overall = result.get("overall")
    if overall:
        lines.append(f"> 数据范围: {overall['start_date']} ~ {overall['end_date']} ({overall['n_days']} 天)")
        lines.append("")
        lines.append("## 全周期指标")
        lines.append("")
        lines.append("| 指标 | 值 |")
        lines.append("|------|-----|")
        lines.append(f"| 年化收益率 | {overall['ann_return']*100:.2f}% |")
        lines.append(f"| 年化波动率 | {overall['ann_vol']*100:.2f}% |")
        lines.append(f"| Sharpe | {overall['sharpe']:.4f} |")
        lines.append(f"| MaxDD | {overall['maxdd']*100:.2f}% |")
        lines.append(f"| Calmar | {overall['calmar']:.4f} |")
        lines.append("")

    annual = result.get("annual", {})
    if annual:
        lines.append("## 年度明细")
        lines.append("")
        lines.append("| 年份 | 天数 | 年化收益 | 波动率 | Sharpe | MaxDD | Calmar | 正月占比 |")
        lines.append("|------|------|---------|--------|--------|-------|--------|---------|")
        for year in result["years"]:
            a = annual[year]
            lines.append(
                f"| {year} | {a['n_days']} | {a['ann_return']*100:.2f}% | "
                f"{a['ann_vol']*100:.2f}% | {a['sharpe']:.4f} | {a['maxdd']*100:.2f}% | "
                f"{a['calmar']:.4f} | {a['pos_month_pct']*100:.1f}% |"
            )
        lines.append("")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="年度收益分解分析")
    parser.add_argument("--task-id", help="回测 task_id")
    parser.add_argument("--account-id", help="模拟账户 account_id")
    parser.add_argument("--benchmark-task-id", help="基准回测 task_id（用于对比）")
    parser.add_argument("--benchmark-account-id", help="基准模拟账户 account_id")
    parser.add_argument("--db-url", help="PostgreSQL URL")
    parser.add_argument("--annualize", type=int, default=252, help="年化交易日数")
    parser.add_argument("--output", "-o", help="输出 JSON 文件路径")
    parser.add_argument("--report", help="输出 Markdown 报告路径")
    parser.add_argument("--title", default="年度收益分解", help="报告标题")

    args = parser.parse_args()

    if not args.task_id and not args.account_id:
        parser.error("必须指定 --task-id 或 --account-id")

    strategy_data = fetch_daily_data(
        task_id=args.task_id,
        account_id=args.account_id,
        db_url=args.db_url,
    )

    if args.benchmark_task_id or args.benchmark_account_id:
        benchmark_data = fetch_daily_data(
            task_id=args.benchmark_task_id,
            account_id=args.benchmark_account_id,
            db_url=args.db_url,
        )
        result = compare_annual_breakdown(strategy_data, benchmark_data, args.annualize)
    else:
        result = compute_annual_breakdown(strategy_data, args.annualize)

    result_json = json.dumps(result, ensure_ascii=False, indent=2)

    if args.output:
        Path(args.output).write_text(result_json, encoding="utf-8")
        print(f"结果已写入: {args.output}", file=sys.stderr)
    else:
        print(result_json)

    if args.report:
        report_md = format_annual_report(result, args.title)
        Path(args.report).write_text(report_md, encoding="utf-8")
        print(f"报告已写入: {args.report}", file=sys.stderr)


if __name__ == "__main__":
    main()
