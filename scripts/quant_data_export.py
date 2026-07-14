#!/usr/bin/env python3
"""
quant_data_export — 从 PostgreSQL 导出日收益/权益曲线到 CSV

用法：
  # 导出回测权益曲线（按 task_id）
  python3 scripts/quant_data_export.py \
    --source equity_curve \
    --task-id fbt-844d906f-9a17-4c4b-b108-8b01aa072162 \
    --output /tmp/h20_daily_ret.csv

  # 导出模拟盘 NAV（按 paper_account_id）
  python3 scripts/quant_data_export.py \
    --source nav_snapshot \
    --account-id pa-v21-fix25 \
    --output /tmp/pa_fix25_daily_ret.csv

  # 列出可用的 task_id（最近 20 个）
  python3 scripts/quant_data_export.py --list-tasks

  # 列出可用的 account_id（最近 20 个）
  python3 scripts/quant_data_export.py --list-accounts
"""

import argparse
import csv
import sys
from pathlib import Path

# 确保可以 import 同目录的 quant_common
_THIS_DIR = Path(__file__).resolve().parent
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))

from quant_common import db_connect


# ---------------------------------------------------------------------------
# 查询模板
# ---------------------------------------------------------------------------

EQUITY_CURVE_QUERY = """
SELECT
    trade_date,
    strategy_return,
    portfolio_value,
    benchmark_value,
    benchmark_return,
    excess_return,
    drawdown
FROM backtest_equity_curve
WHERE task_id = %s
{date_filter}
ORDER BY trade_date
"""

NAV_SNAPSHOT_QUERY = """
SELECT
    snapshot_date AS trade_date,
    daily_return AS strategy_return,
    nav AS portfolio_value,
    benchmark_return,
    excess_return,
    max_drawdown AS drawdown
FROM paper_nav_snapshot
WHERE paper_account_id = %s
{date_filter}
ORDER BY snapshot_date
"""


def list_recent_tasks(limit: int = 20, db_url: str = None):
    """列出最近的 task_id"""
    conn = db_connect(db_url or __import__("quant_common").DEFAULT_DB_URL)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT t.task_id, t.strategy_version_id, t.start_date, t.end_date,
                       COUNT(e.trade_date) AS n_days
                FROM backtest_task t
                LEFT JOIN backtest_equity_curve e ON t.task_id = e.task_id
                GROUP BY t.task_id, t.strategy_version_id, t.start_date, t.end_date
                ORDER BY t.start_date DESC
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
        print(f"\n最近 {limit} 个回测任务：")
        print(f"{'task_id':<45} {'strategy':<30} {'range':<25} {'days':>5}")
        print("-" * 110)
        for row in rows:
            print(f"{row[0]:<45} {row[1]:<30} {str(row[2])+' ~ '+str(row[3]):<25} {row[4]:>5}")
    finally:
        conn.close()


def list_recent_accounts(limit: int = 20, db_url: str = None):
    """列出最近的 paper_account_id"""
    conn = db_connect(db_url or __import__("quant_common").DEFAULT_DB_URL)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT paper_account_id,
                       MIN(snapshot_date) AS start_date,
                       MAX(snapshot_date) AS end_date,
                       COUNT(*) AS n_days,
                       MAX(nav) AS latest_nav
                FROM paper_nav_snapshot
                GROUP BY paper_account_id
                ORDER BY MAX(snapshot_date) DESC
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
        print(f"\n最近 {limit} 个模拟账户：")
        print(f"{'paper_account_id':<45} {'range':<25} {'days':>5} {'latest_nav':>12}")
        print("-" * 95)
        for row in rows:
            print(f"{row[0]:<45} {str(row[1])+' ~ '+str(row[2]):<25} {row[3]:>5} {row[4]:>12.4f}")
    finally:
        conn.close()


def export_equity_curve(task_id: str, output: str, start_date: str = None, end_date: str = None, db_url: str = None):
    """导出 backtest_equity_curve 数据到 CSV"""
    date_filter = ""
    params = [task_id]
    if start_date:
        date_filter += " AND trade_date >= %s"
        params.append(start_date)
    if end_date:
        date_filter += " AND trade_date <= %s"
        params.append(end_date)

    sql = EQUITY_CURVE_QUERY.format(date_filter=date_filter)
    conn = db_connect(db_url or __import__("quant_common").DEFAULT_DB_URL)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        print(f"⚠️  警告：task_id={task_id} 无数据", file=sys.stderr)
        # 仍然输出空 CSV（带表头）
        with open(output, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(columns)
        return 0

    with open(output, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)

    print(f"✅ 导出完成：{output}")
    print(f"   task_id: {task_id}")
    print(f"   行数: {len(rows)}")
    print(f"   日期范围: {rows[0][0]} ~ {rows[-1][0]}")
    return len(rows)


def export_nav_snapshot(account_id: str, output: str, start_date: str = None, end_date: str = None, db_url: str = None):
    """导出 paper_nav_snapshot 数据到 CSV"""
    date_filter = ""
    params = [account_id]
    if start_date:
        date_filter += " AND snapshot_date >= %s"
        params.append(start_date)
    if end_date:
        date_filter += " AND snapshot_date <= %s"
        params.append(end_date)

    sql = NAV_SNAPSHOT_QUERY.format(date_filter=date_filter)
    conn = db_connect(db_url or __import__("quant_common").DEFAULT_DB_URL)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        print(f"⚠️  警告：account_id={account_id} 无数据", file=sys.stderr)
        with open(output, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(columns)
        return 0

    with open(output, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)

    print(f"✅ 导出完成：{output}")
    print(f"   account_id: {account_id}")
    print(f"   行数: {len(rows)}")
    print(f"   日期范围: {rows[0][0]} ~ {rows[-1][0]}")
    return len(rows)


# ---------------------------------------------------------------------------
# 主入口
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="从 PostgreSQL 导出量化数据到 CSV",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--source",
        choices=["equity_curve", "nav_snapshot"],
        help="数据源：equity_curve（回测权益曲线）或 nav_snapshot（模拟盘 NAV）",
    )
    parser.add_argument("--task-id", help="回测任务 ID（当 source=equity_curve）")
    parser.add_argument("--account-id", help="模拟账户 ID（当 source=nav_snapshot）")
    parser.add_argument("--output", "-o", help="输出 CSV 路径")
    parser.add_argument("--start-date", help="起始日期（YYYY-MM-DD）")
    parser.add_argument("--end-date", help="结束日期（YYYY-MM-DD）")
    parser.add_argument("--db-url", help="PostgreSQL 连接 URL（默认从环境变量或本地）")
    parser.add_argument("--list-tasks", action="store_true", help="列出最近的 task_id")
    parser.add_argument("--list-accounts", action="store_true", help="列出最近的 account_id")
    parser.add_argument("--limit", type=int, default=20, help="--list-* 的条目数（默认 20）")

    args = parser.parse_args()

    if args.list_tasks:
        list_recent_tasks(limit=args.limit, db_url=args.db_url)
        return

    if args.list_accounts:
        list_recent_accounts(limit=args.limit, db_url=args.db_url)
        return

    if not args.source:
        parser.error("必须指定 --source（equity_curve 或 nav_snapshot）")
    if not args.output:
        parser.error("必须指定 --output 输出路径")

    if args.source == "equity_curve":
        if not args.task_id:
            parser.error("source=equity_curve 时必须指定 --task-id")
        n = export_equity_curve(
            task_id=args.task_id,
            output=args.output,
            start_date=args.start_date,
            end_date=args.end_date,
            db_url=args.db_url,
        )
    elif args.source == "nav_snapshot":
        if not args.account_id:
            parser.error("source=nav_snapshot 时必须指定 --account-id")
        n = export_nav_snapshot(
            account_id=args.account_id,
            output=args.output,
            start_date=args.start_date,
            end_date=args.end_date,
            db_url=args.db_url,
        )

    if n == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
