#!/usr/bin/env python3
"""P0: A股选股增强敏感性分析 — 验证ML升级对组合AR的边际贡献

目标: 量化 A 股选股质量提升对整体组合 AR 的影响。
A 股占组合 25-35%, 其选股 AR 直接影响整体 1.8pp 缺口。

方法:
1. 用当前 A 股 backtest equity curve 建立基准
2. 模拟 A 股选股提升 10%/20%/30%/50% (通过缩放超额收益)
3. 结合 P2+P3 最优方案 (Sortino-max target=6% + ETF趋势过滤)
4. 输出 A股选股AR → 组合AR 的弹性曲线
"""

import psycopg2, numpy as np
from collections import defaultdict

conn = psycopg2.connect('postgres://gaocheng@localhost/quant')
cur = conn.cursor()

START, END = "2014-01-01", "2026-05-31"
L, MS, G, RF = 36, 0.75, 0.10, 0.02
ALL_ETF = ['518880.SH', '511010.SH', '513500.SH', '513100.SH', '159980.SZ', '159985.SZ']

# ============================================================
# 1. Data Loading
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
for td, pv in cur.fetchall():
    if td not in eq:
        eq[td] = nav * (float(pv) / fv)

# CSI300
cur.execute("SELECT trade_date, close FROM market_index_daily_bar WHERE symbol='000300.SH' AND trade_date>=%s AND trade_date<=%s ORDER BY trade_date",
            ('2012-01-01', END))
csi_data = [(r[0], float(r[1])) for r in cur.fetchall()]

# ETF prices
etf_prices = {}
for etf in ALL_ETF:
    cur.execute('SELECT trade_date, close FROM market_stock_daily_bar_adj WHERE symbol=%s AND trade_date>=%s AND trade_date<=%s ORDER BY trade_date',
                (etf, START, END))
    etf_prices[etf] = {td: float(c) for td, c in cur.fetchall()}

cur.execute('SELECT trade_date FROM market_trade_calendar WHERE trade_date>=%s AND trade_date<=%s AND is_open=true ORDER BY trade_date',
            (START, END))
tdates = [r[0] for r in cur.fetchall()]

# ============================================================
# 2. Functions
# ============================================================

def get_csi_closes_upto(d):
    return np.array([c for td, c in csi_data if td <= d])

def detect_regime(d):
    closes = get_csi_closes_upto(d)
    if len(closes) < 250: return 'neutral'
    t12 = closes[-1]/closes[-250] - 1
    ma60, ma250 = np.mean(closes[-60:]), np.mean(closes[-250:])
    if t12 < -0.15: return 'bear'
    elif t12 > 0.10 and ma60 > ma250: return 'bull'
    return 'neutral'

def build_dr(ashare_boost=0.0):
    """
    Build daily returns.
    ashare_boost: relative boost to A-share returns (0.1 = +10% relative improvement)
    E.g., if original return is 0.5%, boosted return = 0.5% * 1.1 = 0.55%
    """
    dr = []
    for di, d in enumerate(tdates):
        if di == 0: continue
        prev_d = tdates[di-1]
        ap, ac = eq.get(prev_d), eq.get(d)
        if not ap or not ac or ap <= 0: continue
        ar_ = (ac/ap - 1) * (1.0 + ashare_boost)  # boost A-share return
        if abs(ar_) > 0.5: continue
        row = [ar_]
        for etf in ALL_ETF:
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
            for j in range(len(cc)): cc[j] = (1+cc[j])*(1+rets[j])-1
        else:
            if cm: mo.append((cm, d, cc))
            cm, cc = mk, rets.copy()
    if cm: mo.append((cm, d, cc))
    return mo

def lw_shrink(rets):
    S = np.cov(rets, rowvar=False); var = np.diag(S)
    std = np.sqrt(np.maximum(var, 1e-10)); n_a = S.shape[0]
    if n_a <= 1: return S
    cs = sum(S[i,j]/(std[i]*std[j]) for i in range(n_a) for j in range(i+1,n_a) if std[i]>0 and std[j]>0)
    ac = cs/max(n_a*(n_a-1)/2, 1)
    F = np.outer(std, std)*ac; np.fill_diagonal(F, var)
    pi2 = np.sum((S-F)**2)
    sh = max(0, min(1, pi2/max(pi2,1e-10)/max(len(rets),1)))
    return (1-sh)*S + sh*F

def grid_search_n(train, n_a, min_stock, objective='sharpe', target_return=None):
    if len(train) < 12 or n_a < 2: return None
    X = np.array(train)
    mu = np.mean(X, axis=0) * 12
    cov = lw_shrink(train) * 12
    tgt = target_return if target_return is not None else RF
    sv = [i * G for i in range(int(1.0 / G) + 1)]
    best_score, best_w = -np.inf, np.ones(n_a) / n_a

    def calc_score(w):
        pm = np.dot(mu, w); pv = np.sqrt(max(np.dot(w, np.dot(cov, w)), 1e-10))
        if objective == 'sharpe': return (pm - RF) / pv if pv > 0 else -np.inf
        elif objective == 'sortino':
            port_rets = np.dot(X, w); neg = port_rets[port_rets < 0]
            downs = np.sqrt(np.mean(neg**2)) * np.sqrt(12) if len(neg) > 0 else pv
            return (pm - tgt) / max(downs, 1e-10)
        return -np.inf

    def search(idx, remaining, cur):
        nonlocal best_score, best_w
        if idx == n_a - 1:
            cur[idx] = remaining
            w = np.array(cur)
            if np.sum(w) <= 0: return
            w = w / np.sum(w); w[w < 0] = 0
            if np.sum(w) <= 0: return
            w = w / np.sum(w)
            s = calc_score(w)
            if s > best_score: best_score, best_w = s, w.copy()
            return
        for wi in sv:
            if wi > remaining + 0.001 or wi > MS: continue
            cur[idx] = wi; search(idx + 1, remaining - wi, cur)
    search(0, 1.0, np.zeros(n_a))
    return best_w if best_score > -np.inf else None

def calc(rets):
    rets = np.array(rets); years = len(rets)/12.0
    cum = np.cumprod(1+rets); total = cum[-1]-1
    geo = (1+total)**(1/years)-1 if years > 0 else 0
    vol = np.std(rets)*np.sqrt(12); sharpe = (geo-RF)/max(vol,0.001)
    peak = np.maximum.accumulate(cum)
    max_dd = np.max((peak-cum)/peak) if len(peak)>0 else 0
    neg = rets[rets<0]
    d_std = np.std(neg)*np.sqrt(12) if len(neg)>0 else 0.01
    sortino = (geo-RF)/max(d_std,0.001)
    return {'geo':geo,'total':total,'vol':vol,'sharpe':sharpe,'max_dd':max_dd,'sortino':sortino}

def run_simulation(ashare_boost, mvo_objective='sortino', mvo_target=0.06, use_trend=True):
    """Run a full backtest with boosted A-share returns."""
    dr = build_dr(ashare_boost); mo = build_mo(dr); n_a = 7
    rebal = {i for i,(mk,_,_) in enumerate(mo) if int(mk.split('-')[1]) in (1,4,7,10)}
    wh, cw = {}, np.ones(n_a)/n_a

    for i in range(L, len(mo)):
        if i not in rebal: continue
        train = [mo[j][2] for j in range(max(0, i-L), i)]
        regime = detect_regime(mo[i][1])
        ms = 0.35 if regime == 'bull' else (0.0 if regime == 'bear' else 0.25)
        if len(train) < 12: continue
        w = grid_search_n(train, n_a, ms, objective=mvo_objective, target_return=mvo_target)
        if w is None: continue

        if use_trend:
            w_arr = np.array(w); mk_date = mo[i][1]
            for ei, etf in enumerate(ALL_ETF):
                closes = [etf_prices[etf][td] for td in sorted(etf_prices[etf].keys()) if td <= mk_date]
                if len(closes) >= 200 and closes[-1] < np.mean(closes[-200:]):
                    w_arr[ei+1] = 0
            if w_arr.sum() > 0: w = (w_arr / w_arr.sum()).tolist()
        wh[i] = w

    mvo_m = []
    for i, (mk, _, rets) in enumerate(mo):
        if i in wh: cw = wh[i]
        mvo_m.append(np.dot(cw, rets))
    return calc(mvo_m)

# ============================================================
# 3. Run Sensitivity Analysis
# ============================================================

print("="*70)
print("  P0: A股选股增强敏感性分析")
print("="*70)
print()
print("  问题: A股选股AR每提升10%, 组合总AR提升多少?")
print("  方法: Sortino-max(target=6%) + ETF趋势过滤, 缩放A股日收益")
print()

boosts = [0.0, 0.05, 0.10, 0.15, 0.20, 0.30, 0.50]
boost_labels = ['0% (当前)', '+5%', '+10%', '+15%', '+20%', '+30%', '+50%']

results = []
for boost, label in zip(boosts, boost_labels):
    c = run_simulation(boost, 'sortino', 0.06, True)
    results.append((label, boost, c))
    print(f"  A股选股增强 {label:>12s}: "
          f"组合AR={c['geo']*100:5.1f}% DD={c['max_dd']*100:5.1f}% "
          f"SR={c['sharpe']:.2f} SO={c['sortino']:.2f}")

# ============================================================
# 4. Elasticity Analysis
# ============================================================

print(f"\n{'='*70}")
print(f"  弹性分析")
print(f"{'='*70}")

base_ar = results[0][2]['geo']
base_total = results[0][2]['total']
print(f"\n  基准组合AR: {base_ar*100:.1f}%")
print(f"\n  {'A股增强':>12s} {'组合AR':>8s} {'组合ΔAR':>8s} {'弹性系数':>8s} {'达标?':>6s}")
print(f"  {'':->12s} {'':->8s} {'':->8s} {'':->8s} {'':->6s}")

for label, boost, c in results:
    delta_ar = c['geo'] - base_ar
    # Elasticity: % change in portfolio AR / % change in A-share return
    if boost > 0:
        elasticity = (c['geo'] / base_ar - 1) / boost
    else:
        elasticity = 0
    meets = '✅' if c['geo'] >= 0.20 else '⚠'
    print(f"  {label:>12s} {c['geo']*100:>7.1f}% {delta_ar*100:>+7.1f}% {elasticity:>7.2f}x {meets:>6s}")

# ============================================================
# 5. Required ML Improvement
# ============================================================

print(f"\n{'='*70}")
print(f"  达标所需ML提升估算")
print(f"{'='*70}")

target_ar = 0.20
for label, boost, c in results:
    if c['geo'] >= target_ar:
        print(f"\n  🎯 {label} A股增强即可使组合AR达标(>20%)")
        print(f"     组合AR={c['geo']*100:.1f}%, ΔAR={c['geo']-base_ar:+.1%}")
        # Find exact boost needed
        # Linear interpolation between this and previous result
        prev = results[results.index((label, boost, c)) - 1]
        prev_boost, prev_ar = prev[1], prev[2]['geo']
        # Interpolate
        if boost > prev_boost:
            frac = (target_ar - prev_ar) / (c['geo'] - prev_ar)
            exact_boost = prev_boost + frac * (boost - prev_boost)
            print(f"     精确所需: A股选股增强约 {exact_boost*100:.1f}%")
        break

# ============================================================
# 6. ML Improvement Pathways Estimate
# ============================================================

print(f"\n{'='*70}")
print(f"  ML改进路径的预期贡献")
print(f"{'='*70}")

pathways = [
    ("训练窗口优化 (756→optimal)", 0.03, 0.08, "搜索504/756/1008/1260天窗口"),
    ("Label horizon优化 (20d→optimal)", 0.03, 0.08, "搜索5/10/20/40/60天标签"),
    ("行业+市值双中性化", 0.02, 0.06, "减少风格暴露,提升纯Alpha"),
    ("Top-N与Kelly联动优化", 0.02, 0.05, "top_n=20/30/40 + Kelly fraction搜索"),
    ("XGBoost/LightGBM ensemble", 0.05, 0.15, "替代单一NLQR模型"),
]

total_min, total_max = 0, 0
print(f"  {'改进路径':35s} {'最低预期':>10s} {'最高预期':>10s} {'备注'}")
print(f"  {'':-<35s} {'':->10s} {'':->10s} {'':->30s}")
for name, lo, hi, note in pathways:
    total_min += lo; total_max += hi
    print(f"  {name:35s} {lo*100:>+8.1f}% {hi*100:>+8.1f}% {note}")

print(f"  {'':-<35s} {'':->10s} {'':->10s} {'':->30s}")
print(f"  {'合计':35s} {total_min*100:>+8.1f}% {total_max*100:>+8.1f}%")
print(f"\n  注: 合并效果考虑相关性折扣, 实际预期约 {total_min*0.7*100:.0f}%-{total_max*0.7*100:.0f}% A股增强")

cur.close()
conn.close()
print()
