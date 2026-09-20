#!/usr/bin/env bash
# 覆盖率双口径（raw / 豁免调整后）——口径豁免定版 2026-09-20。
# 详见 scripts/coverage_adjusted.py 文档头与 70 号路线图覆盖率条目。
set -euo pipefail
cd "$(dirname "$0")/.."
python3 scripts/coverage_adjusted.py --generate-lcov "$@"
