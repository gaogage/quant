#!/usr/bin/env python3
"""P3: 目标函数替换验证 — Sortino-max / CVaR-min vs Sharpe-max

核心假设: MVO最大化Sharpe ratio (对称惩罚波动)过度限制了上行空间。
改用Sortino-max (仅惩罚下行波动)或CVaR-min (关注尾部风险)可能
产生更高收益的配置。

验证方法:
1. Sharpe-max MVO (baseline)
2. Sortino-max MVO (仅最小化下行半方差)
3. Sortino-max + regime weights (组合P2最优方案)
4. CVaR-min MVO (最小化95% VaR)
"""

import psycopg2
import numpy as np
from collections import defaultdict
from datetime import date

conn = psycopg2.connect('postgres://gaocheng@localhost/quant')
cur = conn.cursor()

START, END = "2014-01-01", "2026-05-31"
L, MS, G, RF = 36, 0.75, 0.10, 0.02

# ============================================================
# 1. Data Loading (same as p2)
# ============================================================

eq, nav = {}, 1.0
tasks = [
    (2014,'fbt-8efaf724'),(2015,'fbt-44695112'),
    (2016,'fbt-5ad5e2c2'),(2016,'fbt-ebd238f7'),(2016,'fbt-ac1fa73b'),
    (2016,'fbt-3fda847e'),(2016,'fbt-abe09d59'),(2016,'fbt-ab87b659'),
    (2017,'fbt-8d1e5b6f'),(2017,'fbt-c2089e4c'),(2017,'fbt-6d7a5dda'),
    (2017,'fbt-c59c15f6'),(2017,'fbt-8fd1e78c'),(2017,'fbt-7dac36db'),
    (2018,'fbt-4fdf169c'),(2018,'fbt-9c3a8341'),(2018,'fbt-3ba15a87'),
    (2018,'fbt-e9f45664'),(2018,'fbt-e5e8c4b0'),
    (2019,'fbt-71dc9def'),(2019,'fbt-96415548'),(2019,'fbt-390d993d'),
    (2019,'fbt-d7839658'),(2019,'fbt-1111529e'),(2019,'fbt-012ea701'),
    (2020,'fbt-970c21bd'),(2021,'fbt-1a2a1fc8'),(2022,'fbt-1979f133'),
    (2023,'fbt-676fdf29'),(2024,'fbt-122ba726'),(2025,'fbt-0f6fded0'),
]
for yr, tid in tasks:
    cur.execute("SELECT trade_date, portfolio_value FROM backtest_equity_curve WHERE task_id LIKE %s ORDER BY trade_date", (tid+'%',))
    rows = cur.fetchall()
    if rows:
        fv = float(rows[0][1])
        for td, pv in rows:
            if td not in eq:
                eq[td] = nav * (float(pv) / fv)
        nav = max(eq.values())

cur.execute("SELECT trade_date, portfolio_value FROM backtest_equity_curve WHERE task_id='bt-ee3f3b65-6cc4-4cc3-bf6f-a28acacff461' AND trade_date>='2026-01-01' ORDER BY trade_date")
rows = cur.fetchall()
if rows:
    fv = float(rows[0][1])
    for td, pv in rows:
        if td not in eq:
            eq[td] = nav * (float(pv) / fv)
    nav = max(eq.values())

cur.execute("SELECT trade_date, close FROM market_index_daily_bar WHERE symbol='000300.SH' AND trade_date>=%s AND trade_date<=%s ORDER BY trade_date",
            ('2012-01-01', END))
csi_data = [(r[0], float(r[1])) for r in cur.fetchall()]

ALL_ETF = ['518880.SH','511010.SH','513500.SH','513100.SH','159980.SZ','159985.SZ']
etf_prices = {}
for etf in ALL_ETF:
    cur.execute('SELECT trade_date, close FROM market_stock_daily_bar_adj WHERE symbol=%s AND trade_date>=%s AND trade_date<=%s ORDER BY trade_date',
                (etf, START, END))
    etf_prices[etf] = {td: float(c) for td, c in cur.fetchall()}

cur.execute('SELECT trade_date FROM market_trade_calendar WHERE trade_date>=%s AND trade_date<=%s AND is_open=true ORDER BY trade_date',
            (START, END))
tdates = [r[0] for r in cur.fetchall()]

# ============================================================
# 2. Regime Detection
# ============================================================

def get_csi_closes_upto(d):
    return np.array([c for td, c in csi_data if td <= d])

def detect_regime(d):
    closes = get_csi_closes_upto(d)
    if len(closes) < 250:
        return 'neutral'
    t12 = closes[-1]/closes[-250] - 1
    ma60, ma250 = np.mean(closes[-60:]), np.mean(closes[-250:])
    if t12 < -0.15: return 'bear'
    elif t12 > 0.10 and ma60 > ma250: return 'bull'
    return 'neutral'

# ============================================================
# 3. Build Returns
# ============================================================

def build_dr(etf_list):
    dr = []
    for di, d in enumerate(tdates):
        if di == 0: continue
        prev_d = tdates[di-1]
        ap, ac = eq.get(prev_d), eq.get(d)
        if not ap or not ac or ap <= 0: continue
        ar_ = ac/ap - 1
        if abs(ar_) > 0.5: continue
        row = [ar_]
        for etf in etf_list:
            p = etf_prices[etf]
            pp, cp = p.get(prev_d, 0), p.get(d, 0)
            r = cp/pp - 1 if pp > 0 and cp > 0 and abs(cp/pp-1) < 0.5 else 0.0
            row.append(r)
        dr.append((d, row))
    return dr

def build_mo(dr):
    mo = []; cm, cc = None, None
    for d, rets in dr:
        mk = f'{d.year}-{d.month:02d}'
        if cm == mk:
            for j in range(len(cc)):
                cc[j] = (1+cc[j])*(1+rets[j])-1
        else:
            if cm: mo.append((cm, d, cc))
            cm, cc = mk, rets.copy()
    if cm: mo.append((cm, d, cc))
    return mo

# ============================================================
# 4. MVO with Multiple Objectives
# ============================================================

def lw_shrink(rets):
    S = np.cov(rets, rowvar=False); var = np.diag(S)
    std = np.sqrt(np.maximum(var, 1e-10))
    n_a = S.shape[0]
    if n_a <= 1: return S
    cs = sum(S[i,j]/(std[i]*std[j]) for i in range(n_a) for j in range(i+1,n_a) if std[i]>0 and std[j]>0)
    ac = cs/max(n_a*(n_a-1)/2, 1)
    F = np.outer(std, std)*ac; np.fill_diagonal(F, var)
    pi2 = np.sum((S-F)**2)
    sh = max(0, min(1, pi2/max(pi2,1e-10)/max(len(rets),1)))
    return (1-sh)*S + sh*F

def grid_search_n(train, n_a, min_stock, objective='sharpe', target_return=None):
    """Generic N-asset recursive grid search with selectable objective.

    Objectives:
      'sharpe'  — maximize (mu - RF) / sigma
      'sortino' — maximize (mu - target) / downside_sigma
      'cvar'    — minimize CVaR_95 (maximize negative CVaR for risk-seeking)
      'return'  — maximize expected return (unconstrained by risk)
    """
    if len(train) < 12 or n_a < 2:
        return None

    X = np.array(train)
    mu = np.mean(X, axis=0) * 12
    cov = lw_shrink(train) * 12
    tgt = target_return if target_return is not None else RF

    sv = [i * G for i in range(int(1.0 / G) + 1)]
    best_score, best_w = -np.inf, np.ones(n_a) / n_a

    def calc_score(w):
        pm = np.dot(mu, w)
        pv = np.sqrt(max(np.dot(w, np.dot(cov, w)), 1e-10))
        if objective == 'sharpe':
            return (pm - RF) / pv if pv > 0 else -np.inf
        elif objective == 'sortino':
            port_rets = np.dot(X, w)
            neg = port_rets[port_rets < 0]
            downs = np.sqrt(np.mean(neg**2)) * np.sqrt(12) if len(neg) > 0 else pv
            return (pm - tgt) / max(downs, 1e-10)
        elif objective == 'cvar':
            port_rets = np.dot(X, w)
            # CVaR_95: mean of worst 5% returns, multiplied by sqrt(12) for annual
            cutoff = np.percentile(port_rets, 5)
            tail = port_rets[port_rets <= cutoff]
            cvar_95 = np.mean(tail) * np.sqrt(12) if len(tail) > 0 else 0
            # We want to MINIMIZE CVaR (less negative = better)
            # Convert to maximization: higher score = less tail risk
            return -abs(cvar_95)  # prefer smaller magnitude tail loss
        elif objective == 'return':
            return pm  # maximize expected return (still constrained by min_stock/MS)
        return -np.inf

    def search(idx, remaining, cur):
        nonlocal best_score, best_w
        if idx == n_a - 1:
            cur[idx] = remaining
            w = np.array(cur)
            if np.sum(w) <= 0: return
            w = w / np.sum(w)
            w[w < 0] = 0
            if np.sum(w) <= 0: return
            w = w / np.sum(w)
            # For CVaR objective, also require Sharpe > 0.5 as sanity check
            if objective == 'cvar':
                pm = np.dot(mu, w)
                pv = np.sqrt(max(np.dot(w, np.dot(cov, w)), 1e-10))
                if pv > 0 and (pm - RF) / pv < 0.3:
                    return  # filter out crazy portfolios
            if objective == 'return':
                pm = np.dot(mu, w)
                pv = np.sqrt(max(np.dot(w, np.dot(cov, w)), 1e-10))
                if pv > 0 and (pm - RF) / pv < 0.5:
                    return  # require minimum efficiency
            s = calc_score(w)
            if s > best_score:
                best_score, best_w = s, w.copy()
            return

        for wi in sv:
            if wi > remaining + 0.001 or wi > MS:
                continue
            cur[idx] = wi
            search(idx + 1, remaining - wi, cur)

    search(0, 1.0, np.zeros(n_a))
    return best_w if best_score > -np.inf else None

# ============================================================
# 5. Performance Calculation
# ============================================================

def calc(rets):
    rets = np.array(rets)
    years = len(rets)/12.0
    cum = np.cumprod(1+rets); total = cum[-1]-1
    geo = (1+total)**(1/years)-1 if years > 0 else 0
    vol = np.std(rets)*np.sqrt(12)
    sharpe = (geo-RF)/max(vol,0.001)
    peak = np.maximum.accumulate(cum)
    max_dd = np.max((peak-cum)/peak) if len(peak)>0 else 0
    neg = rets[rets<0]
    d_std = np.std(neg)*np.sqrt(12) if len(neg)>0 else 0.01
    sortino = (geo-RF)/max(d_std,0.001)
    wr = np.sum(rets>0)/len(rets)
    return {'geo':geo,'total':total,'vol':vol,'sharpe':sharpe,'max_dd':max_dd,'sortino':sortino,'wr':wr}

# ============================================================
# 6. Run Experiments
# ============================================================

print("="*70)
print("  P3: 目标函数替换验证 (Sortino-max / CVaR-min vs Sharpe-max)")
print("="*70)

dr = build_dr(ALL_ETF[:6])
mo = build_mo(dr)
n_a = 7
rebal = {i for i,(mk,_,_) in enumerate(mo) if int(mk.split('-')[1]) in (1,4,7,10)}

def run_backtest(objective_name, objective_fn, use_regime_data=False, use_trend_filter=False):
    """Run a full backtest with given MVO objective and optional enhancements."""
    wh, cw = {}, np.ones(n_a)/n_a

    for i in range(L, len(mo)):
        if i not in rebal:
            continue

        mk_date = mo[i][1]
        regime = detect_regime(mk_date)

        # Select training data
        if use_regime_data:
            bull_t, neutral_t, bear_t = [], [], []
            for j in range(max(0, i-L), i):
                r = detect_regime(mo[j][1])
                d = mo[j][2]
                if r == 'bull': bull_t.append(d)
                elif r == 'bear': bear_t.append(d)
                else: neutral_t.append(d)
            if regime == 'bull': train = bull_t if len(bull_t) >= 12 else neutral_t+bull_t
            elif regime == 'bear': train = bear_t if len(bear_t) >= 12 else neutral_t+bear_t
            else: train = neutral_t if len(neutral_t) >= 12 else neutral_t+bull_t+bear_t
        else:
            train = [mo[j][2] for j in range(max(0, i-L), i)]

        # Set min_stock per regime
        if regime == 'bull': ms = 0.35
        elif regime == 'bear': ms = 0.00
        else: ms = 0.25

        if len(train) < 12:
            continue

        w = grid_search_n(train, n_a, ms, objective=objective_fn)
        if w is None:
            continue

        # Trend filter
        if use_trend_filter:
            w_arr = np.array(w)
            for ei, etf in enumerate(ALL_ETF[:6]):
                closes = [etf_prices[etf][td] for td in sorted(etf_prices[etf].keys()) if td <= mk_date]
                if len(closes) >= 200:
                    if closes[-1] < np.mean(closes[-200:]):
                        w_arr[ei+1] = 0
            if w_arr.sum() > 0:
                w = (w_arr / w_arr.sum()).tolist()

        wh[i] = w

    mvo_m = []
    for i, (mk, _, rets) in enumerate(mo):
        if i in wh: cw = wh[i]
        mvo_m.append(np.dot(cw, rets))
    return calc(mvo_m)

# ---- 6a. Baseline: Sharpe-max + regime min_stock ----
r_baseline = run_backtest('Sharpe-max (Baseline)', 'sharpe')
print(f"\n  Baseline (Sharpe-max):        AR={r_baseline['geo']*100:.1f}% DD={r_baseline['max_dd']*100:.1f}% "
      f"SR={r_baseline['sharpe']:.2f} SO={r_baseline['sortino']:.2f}")

# ---- 6b. Sortino-max ----
r_sortino = run_backtest('Sortino-max', 'sortino')
print(f"  Sortino-max:                  AR={r_sortino['geo']*100:.1f}% DD={r_sortino['max_dd']*100:.1f}% "
      f"SR={r_sortino['sharpe']:.2f} SO={r_sortino['sortino']:.2f} "
      f"Δ={r_sortino['geo']-r_baseline['geo']:+.1%}")

# ---- 6c. CVaR-min ----
r_cvar = run_backtest('CVaR-min', 'cvar')
print(f"  CVaR-min:                     AR={r_cvar['geo']*100:.1f}% DD={r_cvar['max_dd']*100:.1f}% "
      f"SR={r_cvar['sharpe']:.2f} SO={r_cvar['sortino']:.2f} "
      f"Δ={r_cvar['geo']-r_baseline['geo']:+.1%}")

# ---- 6d. Return-max (max expected return, constrained) ----
r_return = run_backtest('Return-max', 'return')
print(f"  Return-max (constrained):     AR={r_return['geo']*100:.1f}% DD={r_return['max_dd']*100:.1f}% "
      f"SR={r_return['sharpe']:.2f} SO={r_return['sortino']:.2f} "
      f"Δ={r_return['geo']-r_baseline['geo']:+.1%}")

# ---- 6e. Sortino-max + regime data ----
r_sortino_r = run_backtest('Sortino-max + Regime', 'sortino', use_regime_data=True)
print(f"  Sortino-max + 体制分化:         AR={r_sortino_r['geo']*100:.1f}% DD={r_sortino_r['max_dd']*100:.1f}% "
      f"SR={r_sortino_r['sharpe']:.2f} SO={r_sortino_r['sortino']:.2f} "
      f"Δ={r_sortino_r['geo']-r_baseline['geo']:+.1%}")

# ---- 6f. Sortino-max + regime + trend (full combo) ----
r_fcombo = run_backtest('Full Combo', 'sortino', use_regime_data=True, use_trend_filter=True)
print(f"  Sortino-max + 体制分化 + ETF趋势: AR={r_fcombo['geo']*100:.1f}% DD={r_fcombo['max_dd']*100:.1f}% "
      f"SR={r_fcombo['sharpe']:.2f} SO={r_fcombo['sortino']:.2f} "
      f"Δ={r_fcombo['geo']-r_baseline['geo']:+.1%}")

# ---- 6g. Check: dynamic target for Sortino ----
print(f"\n  --- 动态收益目标 (Sortino-max) ---")
for tgt, label in [(0.02, 'target=2%'), (0.04, 'target=4%'), (0.06, 'target=6%'), (0.08, 'target=8%')]:
    wh, cw = {}, np.ones(n_a)/n_a
    for i in range(L, len(mo)):
        if i not in rebal: continue
        train = [mo[j][2] for j in range(max(0, i-L), i)]
        regime = detect_regime(mo[i][1])
        ms = 0.35 if regime == 'bull' else (0.0 if regime == 'bear' else 0.25)
        if len(train) >= 12:
            w = grid_search_n(train, n_a, ms, objective='sortino', target_return=tgt)
            if w is not None: wh[i] = w
    mvo_m = []
    for i, (mk, _, rets) in enumerate(mo):
        if i in wh: cw = wh[i]
        mvo_m.append(np.dot(cw, rets))
    c = calc(mvo_m)
    print(f"    Sortino-max ({label:>9s}): AR={c['geo']*100:.1f}% DD={c['max_dd']*100:.1f}% "
          f"SR={c['sharpe']:.2f} SO={c['sortino']:.2f} Δ={c['geo']-r_baseline['geo']:+.1%}")

# ============================================================
# 7. Summary
# ============================================================

print(f"\n{'='*70}")
print(f"  汇总对比")
print(f"{'='*70}")

results = [
    ('Baseline (Sharpe-max)', r_baseline),
    ('Sortino-max', r_sortino),
    ('CVaR-min', r_cvar),
    ('Return-max (constrained)', r_return),
    ('Sortino + Regime', r_sortino_r),
    ('Sortino + Regime + Trend', r_fcombo),
]

print(f"  {'策略':35s} {'AR':>7s} {'MaxDD':>7s} {'Sharpe':>7s} {'Sortino':>7s} {'Vol':>7s} {'ΔAR':>7s}")
print(f"  {'':-<35s} {'':->7s} {'':->7s} {'':->7s} {'':->7s} {'':->7s} {'':->7s}")
for name, c in results:
    delta = c['geo'] - results[0][1]['geo']
    print(f"  {name:35s} {c['geo']*100:>6.1f}% {c['max_dd']*100:>6.1f}% "
          f"{c['sharpe']:>6.2f}  {c['sortino']:>6.2f}  {c['vol']*100:>6.1f}% {delta*100:>+6.1f}%")

best = max(results, key=lambda x: x[1]['geo'])
print(f"\n  🏆 最佳: {best[0]} (AR={best[1]['geo']*100:.1f}%)")

# Cross-comparison: P2 best (Sharpe + Regime + Trend) vs P3 best
print(f"\n  📊 跨模块对比:")
print(f"     P2最优 (Sharpe + 体制分化 + ETF趋势)")
print(f"     P3最优 ({best[0]})")

cur.close()
conn.close()
print()
