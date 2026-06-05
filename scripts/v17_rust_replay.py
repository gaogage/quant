#!/usr/bin/env python3
"""创建干净的全周期回测任务, 调用Rust mvo-simulate端点, 统计结果"""

import psycopg2, numpy as np, json, urllib.request, sys

conn = psycopg2.connect('postgres://gaocheng@localhost/quant')
cur = conn.cursor()
START, END = "2014-01-01", "2026-05-31"

# ---- 1. 构建干净的全周期A股权益曲线 (修正NAV拼接) ----
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

eq = {}; prev_nav = 1.0; prev_date = None
for yr, tid in tasks:
    cur.execute("SELECT trade_date, portfolio_value FROM backtest_equity_curve WHERE task_id LIKE %s ORDER BY trade_date", (tid+'%',))
    rows = cur.fetchall()
    if not rows: continue
    fv = float(rows[0][1])
    for td, pv in rows:
        if prev_date and td <= prev_date: continue
        ret = float(pv)/fv - 1
        if abs(ret) > 0.5: continue  # filter outliers
        eq[td] = prev_nav * (1 + ret)
    prev_nav = prev_nav * (1 + (float(rows[-1][1])/fv - 1))
    prev_date = rows[-1][0]

# 2026 extension
cur.execute("SELECT trade_date, portfolio_value FROM backtest_equity_curve WHERE task_id='fbt-8487d9ca-ded2-40bc-9181-ac750f9d0ac5' AND trade_date>='2026-01-01' ORDER BY trade_date")
rows = cur.fetchall()
if rows:
    fv = float(rows[0][1])
    for td, pv in rows:
        if prev_date and td <= prev_date: continue
        ret = float(pv)/fv - 1
        if abs(ret) <= 0.5:
            eq[td] = prev_nav * (1 + ret)

# ---- 2. 创建临时回测任务并插入权益曲线 ----
import uuid
new_task_id = f"v17replay-{uuid.uuid4().hex[:12]}"
new_result_id = f"v17result-{uuid.uuid4().hex[:12]}"

cur.execute("""
    INSERT INTO backtest_task (task_id, strategy_version_id, data_version_id, benchmark_symbol,
        symbols, start_date, end_date, initial_capital, rebalance_frequency,
        cost_model, slippage_model, execution_rules, parameters, status, mode)
    VALUES (%s, 'phase7-professional-v1', 'dv-20260510-044521', '000300.SH',
        ARRAY['000300.SH'], '2014-01-01', '2026-05-31', 1000000, 'daily',
        '{}'::jsonb, '{}'::jsonb, '{}'::jsonb,
        '{"start_date":"2014-01-01","end_date":"2026-05-31"}'::jsonb, 'completed', 'standard')
""", (new_task_id,))

for td in sorted(eq.keys()):
    v = eq[td]
    cur.execute("""
        INSERT INTO backtest_equity_curve (task_id, trade_date, portfolio_value, cash)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (task_id, trade_date) DO UPDATE SET portfolio_value = EXCLUDED.portfolio_value
    """, (new_task_id, td, str(v), str(v)))

# Insert result
cur.execute("""
    INSERT INTO backtest_result (result_id, task_id, reproducibility_hash)
    VALUES (%s, %s, 'v17-replay')
    ON CONFLICT DO NOTHING
""", (new_result_id, new_task_id))

conn.commit()

# Check
cur.execute("SELECT COUNT(*), MIN(trade_date), MAX(trade_date) FROM backtest_equity_curve WHERE task_id=%s", (new_task_id,))
count, start, end = cur.fetchone()
first_v = eq[min(eq.keys())]
last_v = eq[max(eq.keys())]
total_ret = (last_v/first_v - 1)*100
years = (end - start).days/365.25
cagr = ((last_v/first_v)**(1/years) - 1)*100
print(f"✓ 创建全周期回测任务: {new_task_id}")
print(f"  日期: {start} ~ {end}, {count} 个交易日")
print(f"  A股累计收益: {total_ret:.1f}%, CAGR: {cagr:.1f}%")
print(f"  跨度: {years:.1f} 年")
print()

# ---- 3. 调用Rust mvo-simulate端点 ----
API = "http://127.0.0.1:8080"
req_body = json.dumps({
    "mvo_lookback_months": 36,
    "grid_step": 0.10,
    "commission_pct": 0.0003,
    "benchmark": "000300.SH",
    "leverage_mode": "none",
    "min_stock": 0.25,
    "etf_symbols": ["518880.SH","511010.SH","513500.SH","513100.SH","159980.SZ","159985.SZ"]
}).encode()

print("调用 Rust mvo-simulate 端点...")
req = urllib.request.Request(
    f"{API}/api/v1/quant/backtests/{new_task_id}/mvo-simulate",
    data=req_body,
    headers={"Content-Type": "application/json"},
    method="POST"
)
try:
    with urllib.request.urlopen(req, timeout=600) as resp:
        data = json.loads(resp.read())
    d = data['data']
    blend = d['mvo_blended']

    # ---- 4. 加载基准数据 ----
    bm_data = {}
    for bm_name, bm_sym in [('CSI300','000300.SH'),('SP500','513500.SH'),('Gold','518880.SH')]:
        table = 'market_index_daily_bar' if bm_sym == '000300.SH' else 'market_stock_daily_bar_adj'
        cur.execute(f"SELECT trade_date, close FROM {table} WHERE symbol=%s AND trade_date>=%s AND trade_date<=%s ORDER BY trade_date", (bm_sym, START, END))
        prices = {td: float(c) for td, c in cur.fetchall()}
        cur.execute("SELECT trade_date FROM market_trade_calendar WHERE trade_date>=%s AND trade_date<=%s AND is_open=true ORDER BY trade_date", (START, END))
        tdates = [r[0] for r in cur.fetchall()]
        rets = []
        for di in range(1, len(tdates)):
            pp, cp = prices.get(tdates[di-1], 0), prices.get(tdates[di], 0)
            if pp > 0 and cp > 0: rets.append(cp/pp - 1)
        if rets:
            rets = np.array(rets); n = len(rets); yrs = n/252
            cum = np.cumprod(1+rets); total = cum[-1]-1
            geo = (1+total)**(1/yrs)-1
            vol = np.std(rets)*np.sqrt(252)
            sharpe = (geo-0.02)/max(vol,0.001)
            peak = np.maximum.accumulate(cum)
            max_dd = np.max((peak-cum)/peak)
            neg = rets[rets<0]
            d_std = np.std(neg)*np.sqrt(252) if len(neg)>0 else 0.01
            sortino = (geo-0.02)/max(d_std,0.001)
            # yearly
            yearly = {}; yn = 1.0; ys = 1.0; py = None
            for i, (r, td) in enumerate(zip(rets, tdates[1:])):
                yr = str(td.year)
                if py and yr != py:
                    yearly[py] = yn/ys - 1; ys = yn
                yn *= (1+r); py = yr
            if py: yearly[py] = yn/ys - 1
            bm_data[bm_name] = {'geo':geo,'max_dd':max_dd,'sharpe':sharpe,'sortino':sortino,'vol':vol,'yearly':yearly}

    # ---- 5. 输出结果 ----
    print()
    print("="*80)
    print("  v17 Rust Sortino-max MVO 完整模拟回放 (2014-2026.5, 无杠杆, PIT合规)")
    print("="*80)
    print(f"  任务ID: {new_task_id}")
    print(f"  MVO起始: {d['date_range']['mvo_start']}  结束: {d['date_range']['full_end']}")
    print(f"  ETF: {', '.join(d['config']['etf_symbols'])}")
    print(f"  资产数: {d['config']['n_assets']}")
    print()
    print(f"  {'指标':20s} {'v17 MVO':>10s} {'CSI300':>10s} {'SP500':>10s} {'Gold':>10s} {'蓝图目标':>10s}")
    print(f"  {'':-<20s} {'':->10s} {'':->10s} {'':->10s} {'':->10s} {'':->10s}")

    metrics = [
        ('年化收益', 'geo', True, 20.0),
        ('最大回撤', 'max_dd', True, 35.0),
        ('Sharpe', 'sharpe', False, 1.5),
        ('Sortino', 'sortino', False, 1.8),
        ('年化波动', 'vol', True, None),
    ]
    for name, key, is_pct, target in metrics:
        v17_v = blend['annual_return_pct'] if key == 'geo' else \
                blend['max_drawdown_pct'] if key == 'max_dd' else \
                blend['sharpe_ratio'] if key == 'sharpe' else \
                blend['sortino_ratio'] if key == 'sortino' else \
                blend['volatility_pct']
        vals = [v17_v] + [bm_data[b][key]*100 if is_pct else bm_data[b][key] for b in ['CSI300','SP500','Gold']]
        target_str = f'>={target}%' if target and is_pct else f'>={target}' if target else ''
        fmt_str = f'  {name:20s} '
        for v in vals:
            if is_pct: fmt_str += f'{v:>9.1f}% '
            else: fmt_str += f'{v:>9.2f} '
        fmt_str += f'{target_str:>10s}'
        print(fmt_str)

    print()
    print(f"  {'年份':>6s} {'v17 MVO':>10s} {'CSI300':>10s} {'SP500':>10s} {'Gold':>10s}")
    print(f"  {'':->6s} {'':->10s} {'':->10s} {'':->10s} {'':->10s}")
    all_years = sorted(set(
        [yr['year'] for yr in d['yearly_returns']] +
        list(bm_data['CSI300']['yearly'].keys())
    ))
    for yr in all_years:
        v17_yr = next((yr_data['return_pct'] for yr_data in d['yearly_returns'] if yr_data['year'] == yr), None)
        csi_yr = bm_data['CSI300']['yearly'].get(yr, 0)*100
        sp500_yr = bm_data['SP500']['yearly'].get(yr, 0)*100
        gold_yr = bm_data['Gold']['yearly'].get(yr, 0)*100
        v17_str = f'{v17_yr:>+9.1f}%' if v17_yr is not None else f'{"N/A":>10s}'
        print(f'  {yr:>6s} {v17_str} {csi_yr:>+9.1f}% {sp500_yr:>+9.1f}% {gold_yr:>+9.1f}%')

    # 蓝图达标检查
    print()
    ar, dd, sr, so = blend['annual_return_pct'], blend['max_drawdown_pct'], blend['sharpe_ratio'], blend['sortino_ratio']
    checks = [
        ('AR>=20%', ar>=20, f'{ar:.1f}%'),
        ('MaxDD<=35%', dd<=35, f'{dd:.1f}%'),
        ('Sharpe>=1.5', sr>=1.5, f'{sr:.2f}'),
        ('Sortino>=1.8', so>=1.8, f'{so:.2f}'),
    ]
    all_ok = all(c[1] for c in checks)
    print(f"  蓝图达标: {'🎉 全部达标!' if all_ok else '⚠ 部分未达标'}")
    for name, ok, val in checks:
        print(f"    {'✅' if ok else '⚠'} {name:15s} ({val})")

    # Cleanup
    cur.execute("DELETE FROM backtest_equity_curve WHERE task_id=%s", (new_task_id,))
    cur.execute("DELETE FROM backtest_result WHERE task_id=%s", (new_task_id,))
    cur.execute("DELETE FROM backtest_task WHERE task_id=%s", (new_task_id,))
    conn.commit()
    print(f"\n  (已清理临时回测数据)")

except Exception as e:
    print(f"Error: {e}")
    # Cleanup on error
    cur.execute("DELETE FROM backtest_equity_curve WHERE task_id=%s", (new_task_id,))
    cur.execute("DELETE FROM backtest_result WHERE task_id=%s", (new_task_id,))
    cur.execute("DELETE FROM backtest_task WHERE task_id=%s", (new_task_id,))
    conn.commit()

cur.close(); conn.close()
print()
