#!/usr/bin/env bash
# quant 质量门禁（路线图 70 号 P1 固化，2026-09-19）
#
# 用法:
#   ./scripts/quality_gate.sh          # 全量四件套(fmt + clippy ratchet + 全量测试 + audit hash),~15min
#   ./scripts/quality_gate.sh --fast   # 快门禁(fmt + clippy ratchet),~3min,适合 pre-push/高频自查
#
# clippy ratchet: 各 crate 告警数不得超过 scripts/clippy_ratchet.txt 基线(只许降不许升);
#                 基线归零后该 crate 进入 strict(-D warnings) 轨道。
set -euo pipefail
cd "$(dirname "$0")/.."

FAST="${1:-}"
RED=$'\033[0;31m'; GREEN=$'\033[0;32m'; YELLOW=$'\033[0;33m'; NC=$'\033[0m'
fail=0

pass() { echo -e "${GREEN}PASS${NC} $1"; }
faild() { echo -e "${RED}FAIL${NC} $1"; fail=1; }

# ---------- [1] fmt ----------
echo "=== [1/4] cargo fmt --check ==="
if cargo fmt --all -- --check; then pass "fmt"; else faild "fmt (cargo fmt --all 修复后重跑)"; fi

# ---------- [2] clippy ratchet ----------
echo "=== [2/4] clippy ratchet (基线 scripts/clippy_ratchet.txt) ==="
declare -A RATCHET
if [ -f scripts/clippy_ratchet.txt ]; then
  while read -r crate count; do
    case "$crate" in ''|'#'*) continue ;; esac
    RATCHET[$crate]=$count
  done < scripts/clippy_ratchet.txt
else
  echo -e "${YELLOW}WARN${NC} 基线文件缺失,clippy ratchet 跳过"
fi

declare -A ACTUAL
while IFS= read -r line; do
  crate=${line%%:*}; crate=${crate%%/*}
  [ -n "$crate" ] && ACTUAL[$crate]=$(( ${ACTUAL[$crate]:-0} + 1 ))
done < <(cargo clippy --workspace --all-targets --message-format short 2>&1 \
         | grep 'warning:' | grep -v 'generated')

viol=0
for crate in "${!ACTUAL[@]}"; do
  cap=${RATCHET[$crate]:-0}
  n=${ACTUAL[$crate]}
  if [ "$n" -gt "$cap" ]; then
    echo -e "${RED}FAIL${NC} $crate: $n > 基线 $cap (修复新增告警或更新基线)"
    viol=1
  elif [ "$n" -lt "$cap" ]; then
    echo -e "${YELLOW}改善${NC} $crate: $n < 基线 $cap (可收紧基线: sed 更新 clippy_ratchet.txt)"
  else
    echo "  持平 $crate: $n"
  fi
done
[ "$viol" -eq 0 ] && pass "clippy ratchet" || { faild "clippy ratchet"; }

[ "$FAST" = "--fast" ] && { echo; [ "$fail" -eq 0 ] && pass "快门禁全绿" || faild "快门禁有红灯"; exit "$fail"; }

# ---------- [3] 全量测试 ----------
echo "=== [3/4] cargo test --workspace ==="
if cargo test --workspace --no-fail-fast 2>&1 | tee /tmp/quant_gate_test.log | grep -q 'FAILED\|error\['; then
  faild "全量测试 (详见 /tmp/quant_gate_test.log)"
else
  pass "全量测试"
fi

# ---------- [4] audit hash ----------
echo "=== [4/4] audit hash (需本机 PG) ==="
if cargo test -p quant-backtest --test audit_hash_baseline -- --ignored 2>&1 | grep -q 'test result: ok. 2 passed'; then
  pass "audit hash 2/2 (equity/signal/config/data_version 与金标准一致)"
else
  faild "audit hash (回测确定性被破坏!变更需说明原因并更新 scripts/audit_hash_baseline.json)"
fi

echo
if [ "$fail" -eq 0 ]; then
  pass "质量门禁全绿 ✅"
else
  faild "质量门禁有红灯 ❌"
fi
exit "$fail"
