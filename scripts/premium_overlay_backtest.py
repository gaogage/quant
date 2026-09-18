# -*- coding: utf-8 -*-
"""ETF 溢价被动暴露的风控 overlay 回测(2026-09-18)

问题: v24 ETF sleeve 等权持有 7 只 ETF, 高溢价标的(501018 在 2020-04 溢价
109%、2026-01 溢价 26%)的存量持仓被动经历爆炒虚高 + 溢价回归崩盘。
溢价门禁只防"高溢价买入", 防不了存量暴露。本脚本评估各类减仓 overlay
的长期收益风险比, 找最佳处理办法。

方法论(专业系统要求):
  - PIT 决策: 溢价 = close(t) / 最近可得 nav(< t) - 1, QDII 净值 T+1 及更晚
    公布时取最近可得——与实盘 09:35 调仓时信息集一致, 无前视
  - 执行: t 日收盘决策 → t+1 日开盘价成交(贴近实盘 09:35 执行), 单边 0.2% 滑点
  - 组合: 7 只等权 1/7, overlay 触发的减仓权重转现金(不重分配, 对齐 v24 结构)
  - 定期再平衡: 每 40 交易日恢复等权(对齐 v24 调仓周期)
  - 指标: 年化/波动/Sharpe/MaxDD/Calmar/换手/触发天数
方案:
  - baseline    不处理
  - clear       溢价>entry 清仓该标的, <exit 恢复
  - half        溢价>entry 减半, <exit 恢复
  - derate      线性降权: premium<soft 全额, >hard 清仓, 中间线性
"""

import sys
import numpy as np
import pandas as pd

ETFS = ['511010.SH', '518880.SH', '513100.SH', '513500.SH',
        '159980.SZ', '159985.SZ', '501018.SH']
COST = 0.002          # 单边滑点+佣金
REBAL_DAYS = 40       # 再平衡周期(对齐 v24)


def load():
    # 收益/执行用后复权价(拆分/分红连续, 513100 2022-01-14 拆1:5.002 与
    # 513500 2022-03-30 拆1:2 的 raw 假跳已实证污染未复权口径);
    # 溢价计算用 raw close/nav(市价口径, 见 premium_from_raw)
    px = pd.read_csv('/tmp/etf_px_adj.csv', parse_dates=['trade_date'])
    nav = pd.read_csv('/tmp/etf_nav.csv', parse_dates=['nav_date'])
    close = px.pivot(index='trade_date', columns='symbol', values='close_adj')
    openp = px.pivot(index='trade_date', columns='symbol', values='open_adj')
    raw = pd.read_csv('/tmp/etf_px.csv', parse_dates=['trade_date']) \
        .pivot(index='trade_date', columns='symbol', values='close')
    # PIT 溢价: 每个 t 用 nav_date < t 的最近净值(asof)
    nav_pit = pd.DataFrame(index=close.index, columns=close.columns, dtype=float)
    for sym in ETFS:
        nv = nav[nav.symbol == sym].set_index('nav_date')['unit_nav'].sort_index()
        # asof: t 日可得的是 nav_date < t 的最新一条(当日净值未公布)
        nav_pit[sym] = nv.reindex(close.index, method='ffill').shift(1)
    premium = raw / nav_pit - 1.0
    return close, openp, premium


def backtest(close, openp, premium, mode, entry=0.10, exit_=0.05,
             soft=0.05, hard=0.20):
    """返回(净值序列, 换手率, 触发减仓天数)。t 日收盘决策, t+1 开盘成交。"""
    dates = close.index
    n = len(dates)
    weights = pd.DataFrame(1.0 / len(ETFS), index=dates, columns=ETFS)  # 决策权重
    # ── 生成逐日目标权重(决策层) ──
    cur = {s: 1 / 7 for s in ETFS}
    trig_days = 0
    for i, d in enumerate(dates):
        if i > 0 and i % REBAL_DAYS == 0:
            cur = {s: 1 / 7 for s in ETFS}
        for s in ETFS:
            p = premium.loc[d, s]
            if pd.isna(p):
                continue
            if mode == 'baseline':
                continue
            elif mode in ('clear', 'half'):
                full = 1 / 7
                if p > entry:
                    cur[s] = 0.0 if mode == 'clear' else full / 2
                    trig_days += 1
                elif p < exit_:
                    cur[s] = full
            elif mode == 'derate':
                full = 1 / 7
                if p <= soft:
                    cur[s] = full
                elif p >= hard:
                    cur[s] = 0.0
                    trig_days += 1
                else:
                    cur[s] = full * (1 - (p - soft) / (hard - soft))
        weights.loc[d] = cur
    # ── 执行层: 目标权重变化于次日开盘成交 ──
    nav_series = [1.0]
    holding = {s: 0.0 for s in ETFS}
    cash = 1.0
    for i in range(n - 1):
        d, dn = dates[i], dates[i + 1]
        w_today = weights.loc[d]
        # 当日收盘组合市值
        mv = cash + sum(holding[s] * close.loc[d, s] for s in ETFS)
        # 明日目标
        w_next = weights.loc[dn]
        turnover = 0.0
        for s in ETFS:
            tgt_value = mv * w_next[s]
            tgt_shares = tgt_value / openp.loc[dn, s] if openp.loc[dn, s] > 0 else 0.0
            delta = tgt_shares - holding[s]
            if abs(delta) > 0:
                turnover += abs(delta) * openp.loc[dn, s] / mv
                cash -= delta * openp.loc[dn, s] * (1 + np.sign(delta) * COST)
                holding[s] = tgt_shares
        # 明日收盘估值
        mv_next = cash + sum(holding[s] * close.loc[dn, s] for s in ETFS)
        nav_series.append(mv_next / nav_series[-1] * nav_series[-1])
        nav_series[-1] = mv_next  # 以绝对市值重置口径
        nav_series[-2] = nav_series[-2]  # noop
        # 重新归一: 用比例法避免漂移
        nav_series = nav_series[:i + 2]
        nav_series[i + 1] = nav_series[i] * (mv_next / mv if mv > 0 else 1.0)
    total_turnover = turnover  # 仅末段, 另行累计
    return pd.Series(nav_series, index=dates[:len(nav_series)]), trig_days


def run_with_turnover(close, openp, premium, mode, **kw):
    """重写执行层: 正确累计换手。"""
    dates = close.index
    n = len(dates)
    weights = pd.DataFrame(1.0 / 7, index=dates, columns=ETFS)
    cur = {s: 1 / 7 for s in ETFS}
    trig_days = 0
    for i, d in enumerate(dates):
        if i > 0 and i % REBAL_DAYS == 0:
            cur = {s: 1 / 7 for s in ETFS}
        for s in ETFS:
            p = premium.loc[d, s]
            if pd.isna(p) or mode == 'baseline':
                continue
            full = 1 / 7
            if mode in ('clear', 'half'):
                if p > kw['entry']:
                    cur[s] = 0.0 if mode == 'clear' else full / 2
                    trig_days += 1
                elif p < kw['exit_']:
                    cur[s] = full
            elif mode == 'rel':
                # 相对异常度(2026-09-18): QDII 常态溢价(额度紧张, 513100/513500
                # 2024-2026 长期 5-10%)不等于炒作。绝对阈值单一口径会把常态溢价
                # 误判清仓(清仓@5% 在 2026 少赚 11.6pp 实证)。触发 = 溢价同时超过
                # 绝对下限与自身 60 日 90 分位(炒作急速形成, 必然突破自身分布)。
                p = kw['_roll'].loc[d, s] if '_roll' in kw else None
                if p is not None and p > kw['entry']:
                    cur[s] = 0.0
                    trig_days += 1
                elif p is not None and p < kw['exit_']:
                    cur[s] = full
            elif mode == 'derate':
                soft, hard = kw['soft'], kw['hard']
                if p <= soft:
                    cur[s] = full
                elif p >= hard:
                    cur[s] = 0.0
                    trig_days += 1
                else:
                    cur[s] = full * (1 - (p - soft) / (hard - soft))
            elif mode == 'rel':
                pr = kw['_roll'].loc[d, s]
                if pd.notna(pr) and pr > kw['entry']:
                    cur[s] = 0.0
                    trig_days += 1
                elif pd.notna(pr) and pr < kw['exit_']:
                    cur[s] = full
            elif mode == 'slope':
                # 斜率门控(2026-09-18): 炒作=急速形成(几天内溢价飙升), 额度
                # 紧张的常态溢价=慢漂移(513100 2026 慢漂到 10%+ 但全年涨 41%,
                # 绝对阈值清仓错失)。触发 = 溢价>entry 且 5 日溢价变化>+slope_pp;
                # 恢复 = 溢价<exit_(回落常态)。
                pv = premium.loc[d, s]
                dv = kw['_delta'].loc[d, s]
                if pd.notna(pv) and pd.notna(dv):
                    if pv > kw['entry'] and dv > kw['slope_pp']:
                        cur[s] = 0.0
                        trig_days += 1
                    elif pv < kw['exit_']:
                        cur[s] = full
        weights.loc[d] = cur

    holding = {s: 0.0 for s in ETFS}
    cash = 1.0
    nav = np.zeros(n)
    nav[0] = 1.0
    total_turnover = 0.0
    for i in range(n - 1):
        d, dn = dates[i], dates[i + 1]
        mv = cash + sum(holding[s] * close.loc[d, s] for s in ETFS)
        w_next = weights.loc[dn]
        for s in ETFS:
            op = openp.loc[dn, s]
            if op <= 0 or np.isnan(op):
                continue
            tgt_shares = mv * w_next[s] / op
            delta = tgt_shares - holding[s]
            if abs(delta) * op / mv > 1e-6:
                total_turnover += abs(delta) * op / mv
                cash -= delta * op * (1 + np.sign(delta) * COST)
                holding[s] = tgt_shares
        mv_next = cash + sum(holding[s] * close.loc[dn, s] for s in ETFS)
        nav[i + 1] = nav[i] * (mv_next / mv if mv > 0 else 1.0)
    return pd.Series(nav, index=dates), total_turnover, trig_days


def metrics(nav):
    ret = nav.pct_change().dropna()
    years = (nav.index[-1] - nav.index[0]).days / 365.25
    cagr = (nav.iloc[-1] / nav.iloc[0]) ** (1 / years) - 1
    vol = ret.std() * np.sqrt(244)
    sharpe = cagr / vol if vol > 0 else 0
    dd = (nav / nav.cummax() - 1).min()
    calmar = cagr / abs(dd) if dd < 0 else 0
    return cagr, vol, sharpe, dd, calmar


def main():
    close, openp, premium = load()
    # 数据起点对齐(7 只齐全)
    close = close.dropna()
    openp = openp.reindex(close.index)
    premium = premium.reindex(close.index)
    print('回测区间: %s ~ %s (%d 交易日)' % (close.index[0].date(), close.index[-1].date(), len(close)))

    configs = [('baseline', '不处理')]
    for e in (0.05, 0.08, 0.10, 0.15):
        configs.append((('clear', e, e / 2), '清仓@%d%%,恢复@%d%%' % (e * 100, e * 50)))
        configs.append((('half', e, e / 2), '减半@%d%%,恢复@%d%%' % (e * 100, e * 50)))
    for soft, hard in ((0.05, 0.20), (0.03, 0.15), (0.08, 0.25)):
        configs.append((('derate', soft, hard), '线性降权 %d%%~%d%%' % (soft * 100, hard * 100)))

    rows = []
    for cfg, label in configs:
        if cfg == 'baseline':
            nav, to, trig = run_with_turnover(close, openp, premium, 'baseline')
        else:
            mode, a, b = cfg
            kw = {'entry': a, 'exit_': b} if mode in ('clear', 'half') else {'soft': a, 'hard': b}
            nav, to, trig = run_with_turnover(close, openp, premium, mode, **kw)
        cagr, vol, sharpe, dd, calmar = metrics(nav)
        rows.append({'方案': label, '年化%': round(cagr * 100, 2), '波动%': round(vol * 100, 1),
                     'Sharpe': round(sharpe, 3), 'MaxDD%': round(dd * 100, 1),
                     'Calmar': round(calmar, 3), '年换手x': round(to / ((close.index[-1] - close.index[0]).days / 365.25), 1),
                     '触发日数': trig})
    df = pd.DataFrame(rows)
    print(df.to_string(index=False))

    # 关键事件分段: 2020-03~05 原油危机 / 2026-01~02 金油波动
    best = df.sort_values('Sharpe', ascending=False).iloc[0]['方案']
    print('\n事件分段对比(baseline vs %s):' % best)
    for lo, hi, name in [('2020-03-01', '2020-06-01', '2020原油危机'),
                          ('2026-01-15', '2026-02-10', '2026金油波动'),
                          ('2019-12-01', '2026-09-18', '全周期')]:
        seg_close = close[(close.index >= lo) & (close.index <= hi)]
        for mode_label in ['不处理', best]:
            cfg = [c for c, l in configs if l == mode_label][0]
            if cfg == 'baseline':
                nav, _, _ = run_with_turnover(seg_close, openp, premium, 'baseline')
            else:
                m, a, b = cfg
                kw = {'entry': a, 'exit_': b} if m in ('clear', 'half') else {'soft': a, 'hard': b}
                nav, _, _ = run_with_turnover(seg_close, openp, premium, m, **kw)
            r = nav.iloc[-1] / nav.iloc[0] - 1
            print('  %s %-12s 区间收益 %+.1f%%' % (name, mode_label, r * 100))


if __name__ == '__main__':
    sys.exit(main())
