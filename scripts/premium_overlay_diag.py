# -*- coding: utf-8 -*-
"""溢价 overlay 回测的归因深挖: MaxDD 来源 / 事件段逐标的贡献 / 尾部日分布"""
import numpy as np
import pandas as pd
import sys
sys.path.insert(0, 'scripts')
from premium_overlay_backtest import load, run_with_turnover, ETFS

close, openp, premium = load()
close = close.dropna()
openp = openp.reindex(close.index)
premium = premium.reindex(close.index)

# 1. baseline MaxDD 区间与持仓归因
nav_b, _, _ = run_with_turnover(close, openp, premium, 'baseline')
dd_series = nav_b / nav_b.cummax() - 1
trough = dd_series.idxmin()
peak = nav_b[:trough].idxmax()
print('== baseline MaxDD 归因 ==')
print('回撤区间: %s ~ %s, 深度 %.1f%%' % (peak.date(), trough.date(), dd_series.min() * 100))
seg = close[(close.index >= peak) & (close.index <= trough)]
for s in ETFS:
    r = seg[s].iloc[-1] / seg[s].iloc[0] - 1
    print('  %s 回撤段收益 %+.1f%%' % (s, r * 100))

# 2. 2026-01~02 事件段: 逐标的贡献与 overlay 行为
print('\n== 2026-01-15 ~ 02-10 逐标的 ==')
seg26 = close[(close.index >= '2026-01-15') & (close.index <= '2026-02-10')]
for s in ETFS:
    r = seg26[s].iloc[-1] / seg26[s].iloc[0] - 1
    p_max = premium.loc[seg26.index, s].max()
    print('  %s 涨跌 %+.1f%%  PIT溢价峰值 %+.1f%%' % (s, r * 100, p_max * 100))

# 3. 单日尾部: baseline vs clear@10% 的最差 10 日对比
nav_c, _, _ = run_with_turnover(close, openp, premium, 'clear', entry=0.10, exit_=0.05)
rb = nav_b.pct_change().dropna()
rc = nav_c.pct_change().dropna()
print('\n== 最差 10 个单日(baseline) 及 overlay 同日 ==')
worst = rb.nsmallest(10)
for d, v in worst.items():
    print('  %s  baseline %+.2f%%  clear@10%% %+.2f%%' % (d.date(), v * 100, rc.get(d, np.nan) * 100))
print('baseline 日收益 5%% 分位: %.2f%%  clear@10%%: %.2f%%' % (
    rb.quantile(0.05) * 100, rc.quantile(0.05) * 100))
print('baseline 年化波动 %.1f%%  clear@10%% %.1f%%' % (
    rb.std() * np.sqrt(244) * 100, rc.std() * np.sqrt(244) * 100))

# 4. 501018 溢价>10% 期间的次日收益分布(溢价回归的杀伤力)
print('\n== 501018 PIT溢价>10% 的持有次日收益(全历史) ==')
m = premium['501018.SH'] > 0.10
nxt = close['501018.SH'].shift(-1) / close['501018.SH'] - 1
sel = nxt[m].dropna()
print('样本 %d 天, 次日均值 %+.2f%%, 中位 %+.2f%%, 最差 %.1f%%, 最好 %+.1f%%' % (
    len(sel), sel.mean() * 100, sel.median() * 100, sel.min() * 100, sel.max() * 100))
m5 = premium['501018.SH'] > 0.20
sel5 = nxt[m5].dropna()
print('溢价>20%%: 样本 %d 天, 次日均值 %+.2f%%, 中位 %+.2f%%' % (
    len(sel5), sel5.mean() * 100, sel5.median() * 100))
