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

# ---------- [2] clippy strict ----------
# 2026-09-19 P1 收官：五 crate 告警全部清零，ratchet 基线归零退役（历史见
# scripts/clippy_ratchet.txt），门禁升级为全 workspace strict(-D warnings)。
# 建模债两类（too_many_arguments/type_complexity）经 quant-api Cargo.toml
# [lints.clippy] 显式豁免并登记 DDD Step 2，不影响 strict 判定。
echo "=== [2/4] clippy strict (-D warnings, workspace) ==="
if cargo clippy --workspace --all-targets -- -D warnings > /tmp/quant_gate_clippy.log 2>&1; then
  pass "clippy strict"
else
  faild "clippy strict (详见 /tmp/quant_gate_clippy.log)"
fi

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
