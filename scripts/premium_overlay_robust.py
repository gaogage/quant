# -*- coding: utf-8 -*-
"""溢价 overlay 分年稳健性检验 + 触发时间分布"""
import numpy as np
import pandas as pd
import sys
sys.path.insert(0, 'scripts')
from premium_overlay_backtest import load, run_with_turnover, ETFS

close, openp, premium = load()
close = close.dropna()
openp = openp.reindex(close.index)
premium = premium.reindex(close.index)

schemes = [
    ('不处理', 'baseline', {}),
    ('清仓@5%', 'clear', {'entry': 0.05, 'exit_': 0.02}),
    ('减半@5%', 'half', {'entry': 0.05, 'exit_': 0.02}),
    ('清仓@10%', 'clear', {'entry': 0.10, 'exit_': 0.05}),
]
navs = {}
for label, mode, kw in schemes:
    nav, to, trig = run_with_turnover(close, openp, premium, mode, **kw)
    navs[label] = nav

print('== 分年收益(%) ==')
years = sorted(set(close.index.year))
rows = []
for y in years:
    row = {'年': y}
    for label, _, _ in schemes:
        nv = navs[label]
        seg = nv[nv.index.year == y]
        row[label] = round((seg.iloc[-1] / seg.iloc[0] - 1) * 100, 1) if len(seg) > 1 else None
    rows.append(row)
print(pd.DataFrame(rows).to_string(index=False))

print('\n== 触发时间分布(溢价>5%, 按年×标的) ==')
m = premium > 0.05
trig = m.sum()
print('按年:')
print(m.groupby(m.index.year).sum().to_string())
print('\n2020-07 之后仍触发的标的×年:')
m_post = m[m.index > '2020-07-01']
print(m_post.groupby(m_post.index.year).sum().to_string())

print('\n== 各方案分年相对 baseline 的差值(累计, pp) ==')
base = navs['不处理']
for label, _, _ in schemes[1:]:
    nv = navs[label]
    diffs = []
    for y in years:
        b = base[base.index.year == y]
        s = nv[nv.index.year == y]
        if len(b) > 1:
            diffs.append((y, round(((s.iloc[-1]/s.iloc[0]) - (b.iloc[-1]/b.iloc[0])) * 100, 1)))
    print(label, diffs)
