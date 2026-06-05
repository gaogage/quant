#!/usr/bin/env python3
"""v17 vs v16 最终对比验证 — 修正NAV拼接 + PIT合规 + 无杠杆

修正:
- NAV拼接使用前一任务的LAST值(而非peak), 避免向上偏差
- 使用正确的Sortino-max MVO + ETF MA200趋势过滤
- 输出年度+累计指标+基准对比(CSI300, SP500, Gold)
"""

import psycopg2, numpy as np
from datetime import date

conn = psycopg2.connect('postgres://gaocheng@localhost/quant')
cur = conn.cursor()
START, END = "2014-01-01", "2026-05-31"
L, MS, G, RF = 36, 0.75, 0.10, 0.02
ALL_ETF = ['518880.SH','511010.SH','513500.SH','513100.SH','159980.SZ','159985.SZ']

# ---- Load equity curves (CORRECTED stitching: chain by last value) ----
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

# CORRECTED stitching: chain by last NAV of previous task
eq = {}; prev_last_nav = 1.0; prev_last_date = None
for yr, tid in tasks:
    cur.execute("SELECT trade_date, portfolio_value FROM backtest_equity_curve WHERE task_id LIKE %s ORDER BY trade_date", (tid+'%',))
    rows = cur.fetchall()
    if not rows: continue
    fv = float(rows[0][1])  # first portfolio value of this task
    for td, pv in rows:
        if prev_last_date and td <= prev_last_date: continue
        eq[td] = prev_last_nav * (float(pv) / fv)
    prev_last_nav = prev_last_nav * (float(rows[-1][1]) / fv)
    prev_last_date = rows[-1][0]

# 2026 extension
cur.execute("SELECT trade_date, portfolio_value FROM backtest_equity_curve WHERE task_id='fbt-8487d9ca-ded2-40bc-9181-ac750f9d0ac5' AND trade_date>='2026-01-01' ORDER BY trade_date")
rows = cur.fetchall()
if rows:
    fv = float(rows[0][1])
    for td, pv in rows:
        if prev_last_date and td <= prev_last_date: continue
        eq[td] = prev_last_nav * (float(pv) / fv)

# ---- CSI300, ETF prices, trade calendar ----
cur.execute("SELECT trade_date, close FROM market_index_daily_bar WHERE symbol='000300.SH' AND trade_date>=%s AND trade_date<=%s ORDER BY trade_date", ('2012-01-01', END))
csi_data = [(r[0], float(r[1])) for r in cur.fetchall()]

# Benchmark data: SP500 ETF, Gold ETF
benchmarks = {'CSI300': {}, 'SP500': {}, 'Gold': {}}
cur.execute("SELECT trade_date, close FROM market_index_daily_bar WHERE symbol='000300.SH' AND trade_date>=%s AND trade_date<=%s ORDER BY trade_date", (START, END))
for td, c in cur.fetchall(): benchmarks['CSI300'][td] = float(c)
cur.execute("SELECT trade_date, close FROM market_stock_daily_bar_adj WHERE symbol='513500.SH' AND trade_date>=%s AND trade_date<=%s ORDER BY trade_date", (START, END))
for td, c in cur.fetchall(): benchmarks['SP500'][td] = float(c)
cur.execute("SELECT trade_date, close FROM market_stock_daily_bar_adj WHERE symbol='518880.SH' AND trade_date>=%s AND trade_date<=%s ORDER BY trade_date", (START, END))
for td, c in cur.fetchall(): benchmarks['Gold'][td] = float(c)

etf_prices = {}
for etf in ALL_ETF:
    cur.execute('SELECT trade_date, close FROM market_stock_daily_bar_adj WHERE symbol=%s AND trade_date>=%s AND trade_date<=%s ORDER BY trade_date', (etf, START, END))
    etf_prices[etf] = {td: float(c) for td, c in cur.fetchall()}

cur.execute('SELECT trade_date FROM market_trade_calendar WHERE trade_date>=%s AND trade_date<=%s AND is_open=true ORDER BY trade_date', (START, END))
tdates = [r[0] for r in cur.fetchall()]

# ---- Regime Detection ----
def get_csi_closes_upto(d):
    return np.array([c for td, c in csi_data if td <= d])
def detect_regime(d):
    closes = get_csi_closes_upto(d)
    if len(closes) < 250: return 'neutral', 0.25
    t12 = closes[-1]/closes[-250] - 1
    ma60, ma250 = np.mean(closes[-60:]), np.mean(closes[-250:])
    if t12 < -0.15: return 'bear', 0.00
    elif t12 > 0.10 and ma60 > ma250: return 'bull', 0.35
    return 'neutral', 0.25

# ---- Build Returns ----
def build_dr():
    dr = []
    for di, d in enumerate(tdates):
        if di == 0: continue
        prev_d = tdates[di-1]
        ap, ac = eq.get(prev_d), eq.get(d)
        if not ap or not ac or ap <= 0: continue
        ar_ = ac/ap - 1
        if abs(ar_) > 0.5: continue
        row = [ar_]
        for etf in ALL_ETF:
            pp, cp = etf_prices[etf].get(prev_d, 0), etf_prices[etf].get(d, 0)
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

# ---- MVO ----
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

def downside_semi_cov(rets):
    """Annualized downside semi-covariance (12x)"""
    X = np.array(rets); n_a = X.shape[1]; n_p = X.shape[0]
    semi = np.zeros((n_a, n_a))
    means = X.mean(axis=0)
    for t in range(n_p):
        for i in range(n_a):
            for j in range(n_a):
                ri, rj = X[t,i]-means[i], X[t,j]-means[j]
                if ri < 0 and rj < 0: semi[i,j] += ri*rj
    return semi / max(n_p-1, 1) * 12

def grid_search_sortino(train, n_a, min_stock, target=0.06):
    if len(train) < 12 or n_a < 2: return None
    X = np.array(train)
    mu = X.mean(axis=0)*12; cov = lw_shrink(train)*12
    semi = downside_semi_cov(train)
    sv = [i*G for i in range(int(1.0/G)+1)]
    best_score, best_w = -np.inf, np.ones(n_a)/n_a

    def search(idx, remaining, cur):
        nonlocal best_score, best_w
        if idx == n_a-1:
            cur[idx] = remaining
            w = np.array(cur)
            if np.sum(w) <= 0: return
            w = w/np.sum(w); w[w<0]=0
            if np.sum(w) <= 0: return
            w = w/np.sum(w)
            pm = np.dot(mu, w)
            pv = np.sqrt(max(np.dot(w, np.dot(cov, w)), 1e-10))
            ds = np.sqrt(max(np.dot(w, np.dot(semi, w)), 1e-10))
            port_ret = np.dot(X, w); neg = port_ret[port_ret<0]
            if len(neg) >= 3:
                dn = np.sqrt(np.mean(neg**2))*np.sqrt(12)
            else: dn = ds
            s = (pm - target)/max(dn, 1e-10)
            if s > best_score: best_score, best_w = s, w.copy()
            return
        for wi in sv:
            if wi > remaining+0.001 or wi > MS: continue
            cur[idx] = wi; search(idx+1, remaining-wi, cur)
    search(0, 1.0, np.zeros(n_a))
    return best_w if best_score > -np.inf else None

def grid_search_sharpe(train, n_a, min_stock):
    if len(train) < 12 or n_a < 2: return None
    X = np.array(train)
    mu = X.mean(axis=0)*12; cov = lw_shrink(train)*12
    sv = [i*G for i in range(int(1.0/G)+1)]
    best_score, best_w = -np.inf, np.ones(n_a)/n_a
    def search(idx, remaining, cur):
        nonlocal best_score, best_w
        if idx == n_a-1:
            cur[idx] = remaining
            w = np.array(cur)
            if np.sum(w) <= 0: return
            w = w/np.sum(w); w[w<0]=0
            if np.sum(w) <= 0: return
            w = w/np.sum(w)
            pm = np.dot(mu, w)
            pv = np.sqrt(max(np.dot(w, np.dot(cov, w)), 1e-10))
            s = (pm-RF)/pv if pv>0 else -np.inf
            if s > best_score: best_score, best_w = s, w.copy()
            return
        for wi in sv:
            if wi > remaining+0.001 or wi > MS: continue
            cur[idx] = wi; search(idx+1, remaining-wi, cur)
    search(0, 1.0, np.zeros(n_a))
    return best_w if best_score > -np.inf else None

def calc(rets):
    rets = np.array(rets); years = len(rets)/12.0
    cum = np.cumprod(1+rets); total = cum[-1]-1
    geo = (1+total)**(1/years)-1 if years > 0 else 0
    vol = np.std(rets)*np.sqrt(12)
    sharpe = (geo-RF)/max(vol,0.001)
    peak = np.maximum.accumulate(cum)
    max_dd = np.max((peak-cum)/peak) if len(peak)>0 else 0
    neg = rets[rets<0]
    d_std = np.std(neg)*np.sqrt(12) if len(neg)>0 else 0.01
    sortino = (geo-RF)/max(d_std,0.001)
    wr = np.sum(rets>0)/len(rets) if len(rets)>0 else 0
    return {'geo':geo,'total':total,'vol':vol,'sharpe':sharpe,'max_dd':max_dd,'sortino':sortino,'wr':wr}

def calc_daily_metrics(daily_rets):
    """Compute annualized metrics from DAILY returns"""
    rets = np.array(daily_rets)
    n = len(rets); years = n/252.0
    if years < 0.5 or n < 10: return {'geo':0,'total':0,'vol':0,'sharpe':0,'max_dd':0,'sortino':0,'wr':0}
    cum = np.cumprod(1+rets); total = cum[-1]-1
    geo = (1+total)**(1/years)-1 if years>0 else 0
    vol = np.std(rets)*np.sqrt(252)
    sharpe = (geo-RF)/max(vol,0.001)
    peak = np.maximum.accumulate(cum)
    max_dd = np.max((peak-cum)/peak) if len(peak)>0 else 0
    neg = rets[rets<0]
    d_std = np.std(neg)*np.sqrt(252) if len(neg)>0 else 0.01
    sortino = (geo-RF)/max(d_std,0.001)
    wr = np.sum(rets>0)/n if n>0 else 0
    return {'geo':geo,'total':total,'vol':vol,'sharpe':sharpe,'max_dd':max_dd,'sortino':sortino,'wr':wr}

def benchmark_yearly(rets, dates):
    """Compute yearly returns from daily returns"""
    yearly = {}
    yr_start_nav = 1.0; yr_nav = 1.0; prev_yr = None
    for i, (r, d) in enumerate(zip(rets, dates)):
        yr = str(d.year)
        if prev_yr and yr != prev_yr:
            yearly[prev_yr] = yr_nav/yr_start_nav - 1
            yr_start_nav = yr_nav
        yr_nav *= (1+r); prev_yr = yr
    if prev_yr: yearly[prev_yr] = yr_nav/yr_start_nav - 1
    return yearly

# ---- Run Simulations ----
dr = build_dr(); mo = build_mo(dr); n_a = 7
rebal = {i for i,(mk,_,_) in enumerate(mo) if int(mk.split('-')[1]) in (1,4,7,10)}

def run_sim(label, use_sortino, use_trend):
    wh, cw = {}, np.ones(n_a)/n_a
    for i in range(L, len(mo)):
        if i not in rebal: continue
        train = [mo[j][2] for j in range(max(0, i-L), i)]
        mk_date = mo[i][1]; regime, ms = detect_regime(mk_date)
        if len(train) < 12: continue

        if use_sortino:
            w = grid_search_sortino(train, n_a, ms)
        else:
            w = grid_search_sharpe(train, n_a, ms)
        if w is None: continue

        if use_trend:
            w_arr = np.array(w)
            for ei, etf in enumerate(ALL_ETF):
                closes = [etf_prices[etf][td] for td in sorted(etf_prices[etf].keys()) if td <= mk_date]
                if len(closes) >= 200 and closes[-1] < np.mean(closes[-200:]):
                    w_arr[ei+1] = 0
            if w_arr.sum() > 0: w = (w_arr / w_arr.sum()).tolist()
        wh[i] = w

    mvo_m, mvo_dates = [], []
    for i, (mk, _, rets) in enumerate(mo):
        if i in wh: cw = wh[i]
        mvo_m.append(np.dot(cw, rets))
        mvo_dates.append(date(int(mk[:4]), int(mk[5:7]), 1))
    c = calc(mvo_m)
    yr = benchmark_yearly(mvo_m, mvo_dates)
    return c, yr

# Baseline (v16: Sharpe-max)
c_baseline, yr_baseline = run_sim('v16 Baseline (Sharpe-max)', False, False)
# v17 (Sortino-max + Trend)
c_v17, yr_v17 = run_sim('v17 (Sortino-max + Trend)', True, True)

# ---- Benchmarks ----
bm_cols = {}
for bm_name, bm_prices in benchmarks.items():
    bm_daily = []
    for di in range(1, len(tdates)):
        d, prev_d = tdates[di], tdates[di-1]
        pp, cp = bm_prices.get(prev_d, 0), bm_prices.get(d, 0)
        if pp > 0 and cp > 0: bm_daily.append((cp/pp-1, d))
    if bm_daily:
        bm_rets = [r for r,_ in bm_daily]
        bm_dates = [d for _,d in bm_daily]
        bm_cols[bm_name] = {
            'metrics': calc_daily_metrics(bm_rets),
            'yearly': benchmark_yearly(bm_rets, bm_dates)
        }

# ---- Output ----
print("="*80)
print("  v16 vs v17 最终对比 (修正NAV拼接, PIT合规, 无杠杆, 2014-2026.5)")
print("="*80)
print()

print(f"  {'指标':25s} {'v16 Sharpe':>10s} {'v17 Sortino+Trend':>18s} {'CSI300':>10s} {'SP500':>10s} {'Gold':>10s}")
print(f"  {'':-<25s} {'':->10s} {'':->18s} {'':->10s} {'':->10s} {'':->10s}")
for metric, fmt in [('geo','AR%'),('max_dd','DD%'),('sharpe','SR'),('sortino','SO'),('vol','Vol%')]:
    vals = [c_baseline, c_v17] + [bm_cols[b]['metrics'] for b in ['CSI300','SP500','Gold']]
    label = {'geo':'年化收益','max_dd':'最大回撤','sharpe':'Sharpe','sortino':'Sortino','vol':'年化波动'}[metric]
    is_pct = metric in ('geo','max_dd','vol')
    fmt_str = f'  {label:25s} ' + ' '.join([f'{v[metric]*100:>9.1f}%' if is_pct else f'{v[metric]:>9.2f}' for v in vals])
    print(fmt_str)

print(f"\n  蓝图达标:")
for label, c in [('v16 Sharpe', c_baseline), ('v17 Sortino+Trend', c_v17)]:
    checks = [(c['geo']>=0.20, 'AR', c['geo']), (c['max_dd']<=0.35, 'DD', c['max_dd']),
              (c['sharpe']>=1.5, 'SR', c['sharpe']), (c['sortino']>=1.8, 'SO', c['sortino'])]
    all_ok = all(ck[0] for ck in checks)
    status = '✅' if all_ok else '⚠'
    parts = []
    for ok, n, v in checks:
        if n in ('AR','DD'): parts.append(f'{"✅" if ok else "⚠"} {n}={v*100:.1f}%')
        else: parts.append(f'{"✅" if ok else "⚠"} {n}={v:.2f}')
    print(f'  {status} {label:28s} ' + ' | '.join(parts))

print(f"\n  {'年份':>6s} {'v16 Sharpe':>10s} {'v17 Sortino':>10s} {'CSI300':>10s} {'SP500':>10s} {'Gold':>10s}")
print(f"  {'':->6s} {'':->10s} {'':->10s} {'':->10s} {'':->10s} {'':->10s}")
all_years = sorted(set(list(yr_baseline.keys()) + list(yr_v17.keys())))
for yr in all_years:
    v16_v = yr_baseline.get(yr, 0)*100
    v17_v = yr_v17.get(yr, 0)*100
    csi_v = bm_cols['CSI300']['yearly'].get(yr, 0)*100
    sp500_v = bm_cols['SP500']['yearly'].get(yr, 0)*100
    gold_v = bm_cols['Gold']['yearly'].get(yr, 0)*100
    print(f"  {yr:>6s} {v16_v:>+9.1f}% {v17_v:>+9.1f}% {csi_v:>+9.1f}% {sp500_v:>+9.1f}% {gold_v:>+9.1f}%")

# Improvement summary
delta_ar = c_v17['geo'] - c_baseline['geo']
print(f"\n  v16→v17 ΔAR: {delta_ar*100:+.1f}%")
print(f"  v17 vs CSI300 超额: {(c_v17['geo'] - bm_cols['CSI300']['metrics']['geo'])*100:+.1f}%")

cur.close(); conn.close()
print()
