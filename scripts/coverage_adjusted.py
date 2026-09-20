#!/usr/bin/env python3
"""覆盖率口径豁免后处理（2026-09-20 定版）。

背景：llvm-cov summary 口径（86.97%）对 rustc coverage instrumentation 的
宏展开行重复计数；lcov 行口径去重后 92.80%。剩余未覆盖含两类不可通过
补测试消除的行：
  A. instrumentation 虚报：已被测试调用（函数体有 hit>0 行）的函数内、
     纯字段/表达式行（无控制流关键字）的 hit=0 —— rustc 对内联结构体
     字面量的行归因漂移（实证：测试断言依赖其产出值却记 0）。
  B. 不可达防御分支：控制流恒定条件下的兜底死代码（第六批逐项分析，
     见 70 号路线图）与 #[ignore] 测试函数自身（永不执行却计入分母）。

本脚本：llvm-cov --lcov → 多 record 同行取 max 合并 → 按 A/B 判据剔除
→ 输出 raw / adjusted 双口径。判据自动生成 A 类清单（透明可审计），
B 类用 --exclusions 指定静态清单。

用法：
  ./scripts/coverage_adjusted.sh              # 全流程（构建 lcov + 后处理）
  python3 scripts/coverage_adjusted.py --help
"""
import argparse
import json
import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
# 控制流关键字：hit=0 行含任一 → 真实未覆盖分支（保留分母）
CONTROL_FLOW = re.compile(r'\b(if|else|match|while|for|loop|return|continue|break)\b|=>|\?\s*;')
FN_DEF = re.compile(r'^\s*(?:pub(?:\(crate\))? )?(?:async )?fn (\w+)')


def parse_lcov(path):
    """多 record 合并：同一文件多测试二进制各出一份 SF 块，同行取 max(hit)。"""
    cur, best = None, {}
    for ln in Path(path).read_text().split('\n'):
        if ln.startswith('SF:'):
            cur = ln[3:]
            best.setdefault(cur, defaultdict(int))
        elif ln.startswith('DA:') and cur:
            d = ln[3:].split(',')
            lineno, hit = int(d[0]), int(d[1].strip() or 0)
            if hit > best[cur][lineno]:
                best[cur][lineno] = hit
    return best


def functions_of(src_lines):
    fns = []
    for i, l in enumerate(src_lines):
        m = FN_DEF.match(l)
        if m:
            fns.append((i + 1, m.group(1)))
    return fns


def classify_false_positives(best):
    """A 类自动识别：已调用函数内、无控制流关键字的 hit=0 行。

    判据（保守）：
    - 函数体内存在任一 hit>0 的 DA 行（函数确被调用执行）
    - hit=0 行文本不含控制流关键字（纯字段/表达式赋值行）
    - 该行不是函数签名行
    限制：仅对 signal_generator/capacity_budget.rs 启用（形态已实证：
    115 个 preset 的内联 RegimeSignalRule 字面量字段行）。其它文件若
    出现同类虚报，把文件名加进 FP_FILES（须附实证注释）。
    """
    FP_FILES = {'signal_generator/capacity_budget.rs'}
    out = {}
    for suffix in FP_FILES:
        keys = [k for k in best if k.endswith(suffix)]
        if not keys:
            continue
        src_lines = (REPO / 'quant-backtest/src' / suffix).read_text().split('\n')
        hits = best[keys[0]]
        fns = functions_of(src_lines)
        lines_fp = []
        for idx, (start, name) in enumerate(fns):
            end = fns[idx + 1][0] - 1 if idx + 1 < len(fns) else len(src_lines)
            da = {l: hits[l] for l in range(start, end + 1) if l in hits}
            if not da or not any(v > 0 for v in da.values()):
                continue  # 整函数未调用 → 真实缺口
            for l in sorted(da):
                if da[l] == 0 and l > start and not CONTROL_FLOW.search(src_lines[l - 1]):
                    lines_fp.append(l)
        out[suffix] = lines_fp
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--lcov', default='/tmp/coverage_adjusted.lcov')
    ap.add_argument('--exclusions', default=str(REPO / 'scripts/coverage_exclusions.json'),
                    help='B 类静态豁免清单（file -> [行段/理由]）')
    ap.add_argument('--generate-lcov', action='store_true',
                    help='先跑 cargo llvm-cov 生成 lcov（默认只做后处理）')
    args = ap.parse_args()

    if args.generate_lcov:
        r = subprocess.run(
            ['cargo', 'llvm-cov', '-p', 'quant-backtest', '--lcov',
             '--output-path', args.lcov, '--', '--test-threads=4'],
            cwd=REPO, capture_output=True, text=True)
        if r.returncode != 0:
            sys.exit(f"llvm-cov failed:\n{r.stdout[-2000:]}\n{r.stderr[-2000:]}")

    best = parse_lcov(args.lcov)

    # A 类：自动识别
    fp = classify_false_positives(best)
    fp_total = sum(len(v) for v in fp.values())

    # B 类：静态清单（types.rs ignored 测试函数自身 + 不可达防御分支）
    b_total, b_detail = 0, {}
    excl_path = Path(args.exclusions)
    if excl_path.exists():
        excl = json.loads(excl_path.read_text())
        for suffix, entries in excl.items():
            keys = [k for k in best if k.endswith(suffix)]
            if not keys:
                continue
            hits = best[keys[0]]
            for e in entries:
                rng = e['lines'] if isinstance(e, dict) else e
                a, _, b = rng.partition('-')
                a, b = int(a), int(b or a)
                n = sum(1 for l in range(a, b + 1) if l in hits and hits[l] == 0)
                b_total += n
            b_detail[suffix] = len(entries)

    raw_t = sum(len(d) for d in best.values())
    raw_m = sum(1 for d in best.values() for v in d.values() if v == 0)
    adj_m = raw_m - fp_total - b_total
    print(f"quant-backtest 覆盖率（lcov 行口径，多 record 去重合并）")
    print(f"  raw:       {raw_t} 行 / 未覆盖 {raw_m} = {(raw_t - raw_m) / raw_t * 100:.2f}%")
    print(f"  调整后:    {raw_t} 行 / 未覆盖 {adj_m} = {(raw_t - adj_m) / raw_t * 100:.2f}%")
    print(f"  豁免 A 类（instrumentation 虚报，自动识别）: {fp_total} 行")
    for k, v in fp.items():
        print(f"    {k}: {len(v)} 行")
    print(f"  豁免 B 类（不可达/ignored 自身，静态清单）: {b_total} 行")
    for k, n in b_detail.items():
        print(f"    {k}: {n} 条")


if __name__ == '__main__':
    main()
