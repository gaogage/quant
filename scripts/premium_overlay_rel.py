# -*- coding: utf-8 -*-
"""相对异常度溢价 overlay 验证: 绝对下限 × 自身滚动分位, 分年表现对比"""
import numpy as np
import pandas as pd
import sys
sys.path.insert(0, 'scripts')
from premium_overlay_backtest import load, run_with_turnover, metrics, ETFS

close, openp, premium = load()
close = close.dropna()
openp = openp.reindex(close.index)
premium = premium.reindex(close.index)

# 相对异常度信号: r = premium - 自身60日90分位(>0 表示突破自身常态分布)
roll_q90 = premium.rolling(60, min_periods=40).quantile(0.90)
rel = premium - roll_q90

schemes = [
    ('不处理', 'baseline', {}),
    ('清仓@10%(绝对)', 'clear', {'entry': 0.10, 'exit_': 0.05}),
    ('清仓@15%(绝对)', 'clear', {'entry': 0.15, 'exit_': 0.075}),
    ('相对: abs8%+突破', 'rel', {'entry': 0.08, 'exit_': 0.02, '_roll': rel}),
    ('相对: abs10%+突破', 'rel', {'entry': 0.10, 'exit_': 0.03, '_roll': rel}),
    ('相对: abs12%+突破', 'rel', {'entry': 0.12, 'exit_': 0.04, '_roll': rel}),
]
navs = {}
summary = []
for label, mode, kw in schemes:
    nav, to, trig = run_with_turnover(close, openp, premium, mode, **kw)
    navs[label] = nav
    cagr, vol, sharpe, dd, calmar = metrics(nav)
    yrs = (close.index[-1] - close.index[0]).days / 365.25
    summary.append({'方案': label, '年化%': round(cagr*100, 2), '波动%': round(vol*100, 1),
                    'Sharpe': round(sharpe, 3), 'MaxDD%': round(dd*100, 1),
                    'Calmar': round(calmar, 3), '年换手x': round(to/yrs, 1), '触发日': trig})
print(pd.DataFrame(summary).to_string(index=False))

print('\n== 分年收益(%) ==')
rows = []
for y in sorted(set(close.index.year)):
    row = {'年': y}
    for label, _, _ in schemes:
        nv = navs[label][navs[label].index.year == y]
        row[label] = round((nv.iloc[-1]/nv.iloc[0]-1)*100, 1) if len(nv) > 1 else None
    rows.append(row)
print(pd.DataFrame(rows).to_string(index=False))

print('\n== 相对方案的分年差值(vs 不处理, pp) ==')
base = navs['不处理']
for label, _, _ in schemes[1:]:
    nv = navs[label]
    d = [(y, round(((nv[nv.index.year==y].iloc[-1]/nv[nv.index.year==y].iloc[0]) -
                    (base[base.index.year==y].iloc[-1]/base[base.index.year==y].iloc[0]))*100, 1))
         for y in sorted(set(close.index.year)) if len(nv[nv.index.year==y]) > 1]
    print('%-14s' % label, d)
