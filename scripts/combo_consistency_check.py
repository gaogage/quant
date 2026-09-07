#!/usr/bin/env python3
"""combo 一致性抽样校验（2026-09-05 建，专业量化系统守护组件）。

用法：.venv/bin/python3 scripts/combo_consistency_check.py [--combo NAME] [--date YYYY-MM-DD]

逻辑：取审计日志（combo_materialization_log）记录的该日实际因子清单与参数，
用 factor_value + factor_evaluation 复刻 ICIR 加权 + 行业中性化打分，
与 multi_factor_value 库存分做截面相关。相关 < 0.99 判 FAIL（口径漂移告警）。

设计约束：PIT——评估用 end_date <= 该日最新一条（与物化一致的滚动快照）。
"""
import argparse
import os
from collections import defaultdict

import numpy as np
import psycopg2
import psycopg2.extras


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--combo", default="full_pit_icir_indneutral_val_v1")
    ap.add_argument("--date", default=None, help="默认取库存最新一日")
    ap.add_argument("--threshold", type=float, default=0.99)
    args = ap.parse_args()

    conn = psycopg2.connect(os.environ.get("DATABASE_URL", "postgres://gaocheng@localhost/quant"))
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    d = args.date
    if not d:
        cur.execute("SELECT MAX(trade_date) FROM multi_factor_value WHERE combo_name=%s", (args.combo,))
        d = str(cur.fetchone()[0])

    # 审计：该日所属季度的物化记录（实际因子清单 + horizon）
    cur.execute("""SELECT as_of, horizon, actual_factor_list FROM combo_materialization_log
      WHERE combo_name=%s AND as_of <= %s::date ORDER BY as_of DESC LIMIT 1""", (args.combo, d))
    row = cur.fetchone()
    if not row:
        print(f"FAIL[audit]: {d} 无物化审计记录（combo={args.combo}）——先跑一次物化")
        return
    as_of, horizon, factors = row["as_of"], row["horizon"], row["actual_factor_list"]
    if not factors:
        print(f"FAIL[audit]: 审计记录 actual_factor_list 为空")
        return

    # 库存分
    cur.execute("""SELECT symbol, raw_score::float8 FROM multi_factor_value
      WHERE combo_name=%s AND trade_date=%s""", (args.combo, d))
    prod = dict(cur.fetchall())
    if not prod:
        print(f"FAIL[data]: {d} 无库存分")
        return

    # 复刻打分：该日因子值 + PIT ICIR 权重 + 行业中性化
    cur.execute("""SELECT DISTINCT ON (fe.factor_code) fe.factor_code, fe.mean_ic::float8, fe.ic_ir::float8
      FROM factor_evaluation fe
      WHERE fe.horizon=%s AND fe.end_date <= %s AND fe.mean_ic IS NOT NULL AND fe.ic_ir IS NOT NULL
        AND fe.factor_code = ANY(%s)
      ORDER BY fe.factor_code, fe.end_date DESC""", (horizon, as_of, factors))
    wmap = {r["factor_code"]: (r["mean_ic"], r["ic_ir"]) for r in cur.fetchall()}
    tot = sum(abs(v[1]) for v in wmap.values()) or 1.0

    cur.execute("""SELECT fv.symbol, fv.factor_code,
        (fv.normalized_value::float8 - COALESCE(ia.ind_avg, 0)) AS z
      FROM factor_value fv
      LEFT JOIN LATERAL (
        SELECT AVG(fv2.normalized_value) AS ind_avg
        FROM factor_value fv2 JOIN market_stock ms2 ON ms2.symbol=fv2.symbol AND ms2.industry IS NOT NULL
        WHERE fv2.factor_code=fv.factor_code AND fv2.trade_date=fv.trade_date AND fv2.factor_version=fv.factor_version
          AND fv2.normalized_value IS NOT NULL
          AND ms2.industry = (SELECT industry FROM market_stock ms WHERE ms.symbol=fv.symbol)
      ) ia ON true
      WHERE fv.trade_date=%s AND fv.factor_version='1.0.0'
        AND fv.normalized_value IS NOT NULL AND fv.factor_code = ANY(%s)""", (d, factors))
    acc = defaultdict(float)
    for r in cur.fetchall():
        if r["symbol"] in prod and r["factor_code"] in wmap and r["z"] is not None:
            ic, ir = wmap[r["factor_code"]]
            sign = -1.0 if ic < 0 else 1.0
            acc[r["symbol"]] += r["z"] * (abs(ir) / tot) * sign

    keys = [k for k in prod if k in acc]
    if len(keys) < 100:
        print(f"FAIL[sample]: 复刻交集样本不足 ({len(keys)})")
        return
    x = np.array([acc[k] for k in keys])
    y = np.array([prod[k] for k in keys])
    corr = float(np.corrcoef(x, y)[0, 1])
    status = "PASS" if corr >= args.threshold else "FAIL"
    print(f"{status} [{args.combo}] {d}: 复刻vs库存 相关 = {corr:.4f} (阈值 {args.threshold}, "
          f"因子 {len(wmap)}/{len(factors)}, 样本 {len(keys)})")
    if corr < args.threshold:
        print("  → 口径漂移嫌疑：检查审计参数与库存分数的物化时点是否一致")


if __name__ == "__main__":
    main()
