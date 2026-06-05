#!/usr/bin/env python3
"""P2: 体制差异化ETF权重可行性验证 — 无杠杆 + PIT合规

核心假设: 不同市场体制下，各ETF的最优相对权重不同。
- Bull: 应多配A股+纳指，少配黄金+国债
- Bear: 应多配黄金+国债，少配A股+商品
- Neutral: 均衡配置

验证方法:
1. 每种体制独立运行MVO（仅用该体制的历史月度数据）
2. 在每个调仓日，根据当前体制选择对应的MVO权重
3. 对比Baseline（统一MVO + 仅改min_stock）
"""

import psycopg2
import numpy as np
from collections import defaultdict
from datetime import date, timedelta

conn = psycopg2.connect('postgres://gaocheng@localhost/quant')
cur = conn.cursor()

START, END = "2014-01-01", "2026-05-31"
L, MS, G, RF = 36, 0.75, 0.10, 0.02  # lookback, max_single, grid_step, risk-free

# ============================================================
# 1. Data Loading
# ============================================================

# Load equity curve
eq, nav = {}, 1.0
tasks = [
    (2014, 'fbt-8efaf724'), (2015, 'fbt-44695112'),
    (2016, 'fbt-5ad5e2c2'), (2016, 'fbt-ebd238f7'), (2016, 'fbt-ac1fa73b'),
    (2016, 'fbt-3fda847e'), (2016, 'fbt-abe09d59'), (2016, 'fbt-ab87b659'),
    (2017, 'fbt-8d1e5b6f'), (2017, 'fbt-c2089e4c'), (2017, 'fbt-6d7a5dda'),
    (2017, 'fbt-c59c15f6'), (2017, 'fbt-8fd1e78c'), (2017, 'fbt-7dac36db'),
    (2018, 'fbt-4fdf169c'), (2018, 'fbt-9c3a8341'), (2018, 'fbt-3ba15a87'),
    (2018, 'fbt-e9f45664'), (2018, 'fbt-e5e8c4b0'),
    (2019, 'fbt-71dc9def'), (2019, 'fbt-96415548'), (2019, 'fbt-390d993d'),
    (2019, 'fbt-d7839658'), (2019, 'fbt-1111529e'), (2019, 'fbt-012ea701'),
    (2020, 'fbt-970c21bd'), (2021, 'fbt-1a2a1fc8'), (2022, 'fbt-1979f133'),
    (2023, 'fbt-676fdf29'), (2024, 'fbt-122ba726'), (2025, 'fbt-0f6fded0'),
]
for yr, tid in tasks:
    cur.execute("SELECT trade_date, portfolio_value FROM backtest_equity_curve WHERE task_id LIKE %s ORDER BY trade_date", (tid + '%',))
    rows = cur.fetchall()
    if rows:
        fv = float(rows[0][1])
        for td, pv in rows:
            if td not in eq:
                eq[td] = nav * (float(pv) / fv)
        nav = max(eq.values())

# 2026 extension
cur.execute("SELECT trade_date, portfolio_value FROM backtest_equity_curve WHERE task_id='bt-ee3f3b65-6cc4-4cc3-bf6f-a28acacff461' AND trade_date>='2026-01-01' ORDER BY trade_date")
rows = cur.fetchall()
if rows:
    fv = float(rows[0][1])
    for td, pv in rows:
        if td not in eq:
            eq[td] = nav * (float(pv) / fv)
    nav = max(eq.values())

# Load CSI300
cur.execute("SELECT trade_date, close FROM market_index_daily_bar WHERE symbol='000300.SH' AND trade_date>=%s AND trade_date<=%s ORDER BY trade_date",
            ('2012-01-01', END))
csi_data = [(r[0], float(r[1])) for r in cur.fetchall()]

# Load ETF prices
ALL_ETF = ['518880.SH', '511010.SH', '513500.SH', '513100.SH', '159980.SZ', '159985.SZ']
ETF_NAMES = ['黄金', '国债', 'SP500', '纳指', '有色', '豆粕']
etf_prices = {}
for etf in ALL_ETF:
    cur.execute('SELECT trade_date, close FROM market_stock_daily_bar_adj WHERE symbol=%s AND trade_date>=%s AND trade_date<=%s ORDER BY trade_date',
                (etf, START, END))
    etf_prices[etf] = {td: float(c) for td, c in cur.fetchall()}

# Trade calendar
cur.execute('SELECT trade_date FROM market_trade_calendar WHERE trade_date>=%s AND trade_date<=%s AND is_open=true ORDER BY trade_date',
            (START, END))
tdates = [r[0] for r in cur.fetchall()]

# ============================================================
# 2. Regime Detection (PIT-compliant: only data up to date)
# ============================================================

def get_csi_closes_upto(d):
    """Get CSI300 closes up to date d (PIT)"""
    return np.array([c for td, c in csi_data if td <= d])

def detect_regime(d):
    """PIT regime: bull/neutral/bear based on CSI300 up to date d"""
    closes = get_csi_closes_upto(d)
    if len(closes) < 250:
        return 'neutral'
    t12 = closes[-1] / closes[-250] - 1
    ma60 = np.mean(closes[-60:])
    ma250 = np.mean(closes[-250:])

    if t12 < -0.15:
        return 'bear'
    elif t12 > 0.10 and ma60 > ma250:
        return 'bull'
    else:
        return 'neutral'

# ============================================================
# 3. Build Daily & Monthly Returns
# ============================================================

def build_dr(etf_list):
    """Build daily returns for A-share + ETFs"""
    dr = []
    n = len(etf_list) + 1
    for di, d in enumerate(tdates):
        if di == 0:
            continue
        prev_d = tdates[di - 1]
        ap, ac = eq.get(prev_d), eq.get(d)
        if not ap or not ac or ap <= 0:
            continue
        ar_ = ac / ap - 1
        if abs(ar_) > 0.5:
            continue
        row = [ar_]
        for etf in etf_list:
            p = etf_prices[etf]
            pp, cp = p.get(prev_d, 0), p.get(d, 0)
            r = cp / pp - 1 if pp > 0 and cp > 0 and abs(cp / pp - 1) < 0.5 else 0.0
            row.append(r)
        dr.append((d, row))
    return dr

def build_mo(dr):
    """Aggregate daily returns to monthly"""
    mo = []
    cm, cc = None, None
    n = len(dr[0][1])
    for d, rets in dr:
        mk = f'{d.year}-{d.month:02d}'
        if cm == mk:
            for j in range(n):
                cc[j] = (1 + cc[j]) * (1 + rets[j]) - 1
        else:
            if cm:
                mo.append((cm, d, cc))  # (month_key, last_trade_date, returns)
            cm, cc = mk, rets.copy()
    if cm:
        mo.append((cm, d, cc))
    return mo

# ============================================================
# 4. MVO with Ledoit-Wolf Shrinkage
# ============================================================

def lw_shrink(rets):
    """Ledoit-Wolf shrinkage for covariance matrix"""
    S = np.cov(rets, rowvar=False)
    var = np.diag(S)
    std = np.sqrt(np.maximum(var, 1e-10))
    n_a = S.shape[0]
    if n_a <= 1:
        return S
    cs = sum(S[i, j] / (std[i] * std[j])
             for i in range(n_a) for j in range(i + 1, n_a)
             if std[i] > 0 and std[j] > 0)
    ac = cs / max(n_a * (n_a - 1) / 2, 1)
    F = np.outer(std, std) * ac
    np.fill_diagonal(F, var)
    pi2 = np.sum((S - F) ** 2)
    sh = max(0, min(1, pi2 / max(pi2, 1e-10) / max(len(rets), 1)))
    return (1 - sh) * S + sh * F

def grid_search_n(train, n_a, min_stock, objective='sharpe', target_return=None):
    """Generic N-asset recursive grid search.

    objective: 'sharpe' | 'sortino'
    target_return: for sortino objective, the minimum acceptable return (default: RF)
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
            neg_rets = port_rets[port_rets < 0]
            downs = np.sqrt(np.mean(neg_rets**2)) * np.sqrt(12) if len(neg_rets) > 0 else pv
            return (pm - tgt) / max(downs, 1e-10)
        return -np.inf

    def search(idx, remaining, cur):
        nonlocal best_score, best_w
        if idx == n_a - 1:
            cur[idx] = remaining
            w = np.array(cur)
            w_sum = np.sum(w)
            if w_sum <= 0:
                return
            w = w / w_sum
            w[w < 0] = 0
            if np.sum(w) <= 0:
                return
            w = w / np.sum(w)
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
    """Calculate performance metrics"""
    years = len(rets) / 12.0
    rets = np.array(rets)
    cum = np.cumprod(1 + rets)
    total = cum[-1] - 1
    geo = (1 + total) ** (1 / years) - 1 if years > 0 else 0
    vol = np.std(rets) * np.sqrt(12)
    sharpe = (geo - RF) / max(vol, 0.001)
    peak = np.maximum.accumulate(cum)
    max_dd = np.max((peak - cum) / peak) if len(peak) > 0 else 0
    neg = rets[rets < 0]
    d_std = np.std(neg) * np.sqrt(12) if len(neg) > 0 else 0.01
    sortino = (geo - RF) / max(d_std, 0.001)
    wr = np.sum(rets > 0) / len(rets) if len(rets) > 0 else 0
    return {'geo': geo, 'total': total, 'vol': vol, 'sharpe': sharpe,
            'max_dd': max_dd, 'sortino': sortino, 'wr': wr}

# ============================================================
# 6. Run Baselines & Experiments
# ============================================================

print("=" * 70)
print("  P2: 体制差异化 ETF 权重验证")
print("=" * 70)

dr = build_dr(ALL_ETF[:6])
mo = build_mo(dr)
n_a = 7  # A股 + 6 ETFs
rebal_months = {i for i, (mk, _, _) in enumerate(mo) if int(mk.split('-')[1]) in (1, 4, 7, 10)}

# ---- 6a. Baseline: v17 统一MVO + 动态min_stock ----
print("\n[1/4] Baseline: v17统一MVO (regime → min_stock only)...")
wh_baseline, cw_baseline = {}, np.ones(n_a) / n_a
for i in range(L, len(mo)):
    if i in rebal_months:
        train = [mo[j][2] for j in range(max(0, i - L), i)]
        mk_date = mo[i][1]  # last trade date of this month
        regime = detect_regime(mk_date)
        if regime == 'bull':
            ms = 0.35
        elif regime == 'bear':
            ms = 0.00
        else:
            ms = 0.25
        w = grid_search_n(train, n_a, ms)
        if w is not None:
            wh_baseline[i] = w

baseline_mo = []
for i, (mk, _, rets) in enumerate(mo):
    if i in wh_baseline:
        cw_baseline = wh_baseline[i]
    baseline_mo.append(np.dot(cw_baseline, rets))

c_base = calc(baseline_mo)
print(f"  Baseline: AR={c_base['geo']*100:.1f}% DD={c_base['max_dd']*100:.1f}% "
      f"SR={c_base['sharpe']:.2f} SO={c_base['sortino']:.2f} Vol={c_base['vol']*100:.1f}%")

# ---- 6b. Experiment 1: 按体制分别训练MVO ----
print("\n[2/4] Experiment 1: 体制分化MVO (每体制独立训练)...")

# Build regime-labeled training sets (PIT: only months on or before the rebalance date)
def get_regime_labeled_data(mo, idx):
    """Get all months up to idx, labeled by regime at each month's date."""
    bull_data, neutral_data, bear_data = [], [], []
    for j in range(max(0, idx - L), idx):
        mk_date = mo[j][1]
        r = detect_regime(mk_date)
        if r == 'bull':
            bull_data.append(mo[j][2])
        elif r == 'bear':
            bear_data.append(mo[j][2])
        else:
            neutral_data.append(mo[j][2])
    return bull_data, neutral_data, bear_data

wh_regime, cw_regime = {}, np.ones(n_a) / n_a
for i in range(L, len(mo)):
    if i in rebal_months:
        bull_train, neutral_train, bear_train = get_regime_labeled_data(mo, i)
        mk_date = mo[i][1]
        current_regime = detect_regime(mk_date)

        # Select training data based on current regime
        if current_regime == 'bull':
            train = bull_train if len(bull_train) >= 12 else neutral_train + bull_train
        elif current_regime == 'bear':
            train = bear_train if len(bear_train) >= 12 else neutral_train + bear_train
        else:
            train = neutral_train if len(neutral_train) >= 12 else neutral_train + bull_train + bear_train

        # Use appropriate min_stock
        if current_regime == 'bull':
            ms = 0.35
        elif current_regime == 'bear':
            ms = 0.00
        else:
            ms = 0.25

        if len(train) >= 12:
            w = grid_search_n(train, n_a, ms)
            if w is not None:
                wh_regime[i] = w

regime_mo = []
for i, (mk, _, rets) in enumerate(mo):
    if i in wh_regime:
        cw_regime = wh_regime[i]
    regime_mo.append(np.dot(cw_regime, rets))

c_regime = calc(regime_mo)
print(f"  Exp1: AR={c_regime['geo']*100:.1f}% DD={c_regime['max_dd']*100:.1f}% "
      f"SR={c_regime['sharpe']:.2f} SO={c_regime['sortino']:.2f} Vol={c_regime['vol']*100:.1f}%")
print(f"  Δ vs Baseline: AR={c_regime['geo']-c_base['geo']:+.1%}")

# ---- 6c. Experiment 2: 5-Regime (Strong Bull/Bull/Neutral/Bear/Strong Bear) ----
print("\n[3/4] Experiment 2: 5体制扩展...")

def detect_regime_5(d):
    """5-regime detection: strong_bull, bull, neutral, bear, strong_bear"""
    closes = get_csi_closes_upto(d)
    if len(closes) < 250:
        return 'neutral'
    t12 = closes[-1] / closes[-250] - 1
    t3 = closes[-1] / closes[-60] - 1
    ma60 = np.mean(closes[-60:])
    ma250 = np.mean(closes[-250:])

    if t12 > 0.20 and ma60 > ma250 and t3 > 0.05:
        return 'strong_bull'
    elif t12 > 0.10 and ma60 > ma250:
        return 'bull'
    elif t12 < -0.25:
        return 'strong_bear'
    elif t12 < -0.15:
        return 'bear'
    else:
        return 'neutral'

def get_regime_labeled_data_5(mo, idx):
    data = defaultdict(list)
    for j in range(max(0, idx - L), idx):
        mk_date = mo[j][1]
        r = detect_regime_5(mk_date)
        data[r].append(mo[j][2])
    return data

# Regime-specific min_stock
REGIME_MS_5 = {
    'strong_bull': 0.40,
    'bull': 0.30,
    'neutral': 0.20,
    'bear': 0.05,
    'strong_bear': 0.00,
}

wh_5r, cw_5r = {}, np.ones(n_a) / n_a
for i in range(L, len(mo)):
    if i in rebal_months:
        labeled = get_regime_labeled_data_5(mo, i)
        mk_date = mo[i][1]
        current_regime = detect_regime_5(mk_date)

        # Select training data: prioritize current regime, fallback to neutral+current, then all
        current_data = labeled.get(current_regime, [])
        neutral_data = labeled.get('neutral', [])
        if len(current_data) >= 12:
            train = current_data
        elif len(current_data) + len(neutral_data) >= 12:
            train = current_data + neutral_data
        else:
            train = []
            for r, d in labeled.items():
                train.extend(d)

        ms = REGIME_MS_5.get(current_regime, 0.20)
        if len(train) >= 12:
            w = grid_search_n(train, n_a, ms)
            if w is not None:
                wh_5r[i] = w

r5_mo = []
for i, (mk, _, rets) in enumerate(mo):
    if i in wh_5r:
        cw_5r = wh_5r[i]
    r5_mo.append(np.dot(cw_5r, rets))

c_5r = calc(r5_mo)
print(f"  Exp2: AR={c_5r['geo']*100:.1f}% DD={c_5r['max_dd']*100:.1f}% "
      f"SR={c_5r['sharpe']:.2f} SO={c_5r['sortino']:.2f} Vol={c_5r['vol']*100:.1f}%")
print(f"  Δ vs Baseline: AR={c_5r['geo']-c_base['geo']:+.1%}")

# ---- 6d. Experiment 3: Regime-specific MVO + Trend Filter (MA200) ----
print("\n[4/4] Experiment 3: 体制分化MVO + ETF MA200趋势过滤...")

wh_trend, cw_trend = {}, np.ones(n_a) / n_a
for i in range(L, len(mo)):
    if i in rebal_months:
        bull_train, neutral_train, bear_train = get_regime_labeled_data(mo, i)
        mk_date = mo[i][1]
        current_regime = detect_regime(mk_date)

        if current_regime == 'bull':
            train = bull_train if len(bull_train) >= 12 else neutral_train + bull_train
        elif current_regime == 'bear':
            train = bear_train if len(bear_train) >= 12 else neutral_train + bear_train
        else:
            train = neutral_train if len(neutral_train) >= 12 else neutral_train + bull_train + bear_train

        if current_regime == 'bull':
            ms = 0.35
        elif current_regime == 'bear':
            ms = 0.00
        else:
            ms = 0.25

        if len(train) >= 12:
            w = grid_search_n(train, n_a, ms)
            if w is not None:
                # Trend filter: check each ETF's MA200
                filtered_w = np.array(w)
                for ei, etf in enumerate(ALL_ETF[:6]):
                    closes = [etf_prices[etf][td] for td in sorted(etf_prices[etf].keys()) if td <= mk_date]
                    if len(closes) >= 200:
                        ma200 = np.mean(closes[-200:])
                        current_price = closes[-1]
                        if current_price < ma200:
                            # ETF below MA200 → redistribute weight to others
                            filtered_w[ei + 1] = 0
                # Renormalize
                if filtered_w.sum() > 0:
                    filtered_w = filtered_w / filtered_w.sum()
                wh_trend[i] = filtered_w.tolist()

trend_mo = []
for i, (mk, _, rets) in enumerate(mo):
    if i in wh_trend:
        cw_trend = wh_trend[i]
    trend_mo.append(np.dot(cw_trend, rets))

c_trend = calc(trend_mo)
print(f"  Exp3: AR={c_trend['geo']*100:.1f}% DD={c_trend['max_dd']*100:.1f}% "
      f"SR={c_trend['sharpe']:.2f} SO={c_trend['sortino']:.2f} Vol={c_trend['vol']*100:.1f}%")
print(f"  Δ vs Baseline: AR={c_trend['geo']-c_base['geo']:+.1%}")

# ============================================================
# 7. Summary
# ============================================================

print("\n" + "=" * 70)
print("  汇总对比")
print("=" * 70)

results = [
    ('Baseline v17', c_base),
    ('Exp1: 体制分化MVO', c_regime),
    ('Exp2: 5体制MVO', c_5r),
    ('Exp3: 体制分化+ETF趋势', c_trend),
]

print(f"  {'策略':30s} {'AR':>7s} {'MaxDD':>7s} {'Sharpe':>7s} {'Sortino':>7s} {'Vol':>7s} {'ΔAR':>7s}")
print(f"  {'':-<30s} {'':->7s} {'':->7s} {'':->7s} {'':->7s} {'':->7s} {'':->7s}")
for name, c in results:
    delta = c['geo'] - results[0][1]['geo']
    print(f"  {name:30s} {c['geo']*100:>6.1f}% {c['max_dd']*100:>6.1f}% "
          f"{c['sharpe']:>6.2f}  {c['sortino']:>6.2f}  {c['vol']*100:>6.1f}% {delta*100:>+6.1f}%")

# Best performing
best = max(results, key=lambda x: x[1]['geo'])
print(f"\n  🏆 最佳: {best[0]} (AR={best[1]['geo']*100:.1f}%)")

# Check if any meets blueprint target
print(f"\n  📊 蓝图达标检查 (AR≥20%, DD≤35%, SR≥1.5, SO≥1.8):")
for name, c in results:
    checks = []
    checks.append('✅' if c['geo'] >= 0.20 else f"AR={c['geo']*100:.1f}%")
    checks.append('✅' if c['max_dd'] <= 0.35 else f"DD={c['max_dd']*100:.1f}%")
    checks.append('✅' if c['sharpe'] >= 1.5 else f"SR={c['sharpe']:.2f}")
    checks.append('✅' if c['sortino'] >= 1.8 else f"SO={c['sortino']:.2f}")
    all_ok = all('✅' in ck for ck in checks)
    status = '✅ 达标' if all_ok else '⚠'
    print(f"  {status} {name:28s} {' | '.join(checks)}")

# Regime distribution stats
regime_counts = defaultdict(int)
for i in range(L, len(mo)):
    if i in rebal_months:
        r = detect_regime(mo[i][1])
        regime_counts[r] += 1
total = sum(regime_counts.values())
print(f"\n  📈 体制分布 (季度调仓日):")
for r in ['bull', 'neutral', 'bear']:
    print(f"     {r:10s}: {regime_counts.get(r, 0):3d} ({regime_counts.get(r,0)/max(total,1)*100:.0f}%)")

cur.close()
conn.close()
print()
