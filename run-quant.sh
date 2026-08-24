#!/usr/bin/env bash
# quant-api 部署统一入口：本机容器
#
# 用法:
#   run-quant.sh [--build] [--target local]
#     --build           先执行 build-push.sh 构建并推送多架构镜像到 ACR
#     --target <env>    部署目标，默认 local
#       local  本机 docker run 直接起容器（端口 8080->8080，复用本机 PostgreSQL）
#
# 多架构说明：quant-api:latest 这一个 tag 要同时服务本机 Mac（arm64）和可能的 amd64 节点，
# 靠 build-push.sh 一次构建 amd64+arm64 两份推到同一个 ACR tag（manifest list）。
# 若只构建单架构就推送会导致另一架构 exec format error。
#
# 镜像策略（与 aip / mcp-rsmysql-http / mcp-gitlab-http 一致）：
# 个人制作或改动的镜像一律推 ACR 并用 ACR 镜像运行（不走本地 quant-api:latest）。
# 本地临时调试可用 IMAGE=quant-api:latest 覆盖为本地构建镜像（跳过 pull）。
set -euo pipefail
# 解析软链接真实路径（~/.local/bin/run-quant -> quant/run-quant.sh），确保 ./build-push.sh 相对路径生效
SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
cd "$SCRIPT_DIR"

BUILD=0
TARGET="local"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --build) BUILD=1; shift ;;
    --target) TARGET="${2:?}"; shift 2 ;;
    -h|--help) echo "用法: run-quant.sh [--build] [--target local]"; exit 0 ;;
    *) echo "未知参数: $1" >&2; exit 1 ;;
  esac
done
case "$TARGET" in local) ;; *) echo "非法 --target: $TARGET（仅支持 local）" >&2; exit 1 ;; esac

ACR_REGISTRY="crpi-9l8t2h4ro3skt9yg.cn-hangzhou.personal.cr.aliyuncs.com"
ACR_IMAGE="$ACR_REGISTRY/gaogage/quant-api:latest"

if [[ "$BUILD" == "1" ]]; then
  ./build-push.sh
else
  echo "[run-quant] 未传 --build，跳过构建/推送，直接使用现有 ACR 镜像: $ACR_IMAGE"
fi

case "$TARGET" in
  local)
    # 读取 .env.quant（若存在），提供 JWT_SECRET / TUSHARE_TOKEN 等敏感配置
    if [ -f .env.quant ]; then set -a; . ./.env.quant; set +a; fi

    # 可用 IMAGE=quant-api:latest 覆盖为本地构建镜像（本地调试）
    RUN_IMAGE="${IMAGE:-$ACR_IMAGE}"

    # buildx --push 只更新远端 ACR manifest，不会刷新本机 docker daemon 的本地镜像缓存；
    # 若不显式 pull，docker run 会直接复用本地同名旧 tag，看起来部署成功但跑的是旧代码
    # （aip 2026-07-24 踩过：--build 推送成功但容器仍是前一天的旧镜像）。
    # 仅当使用默认 ACR 镜像时才 pull，IMAGE= 覆盖为纯本地 tag 调试时不应尝试拉取远端。
    if [[ "$RUN_IMAGE" == "$ACR_IMAGE" ]]; then
      echo "[run-quant] 拉取最新 ACR 镜像: $RUN_IMAGE"
      docker pull "$RUN_IMAGE"
    fi

    # 旧容器清理
    docker rm -f quant >/dev/null 2>&1 || true

    # 本机部署：127.0.0.1:8080->8080（仅回环，不暴露局域网/Tailscale；HTTPS 经 gateway:443 转发），
    # 复用本机 PG（host.docker.internal:5432/quant）
    # 敏感配置（JWT/TUSHARE/钉钉）通过 -e 从 .env.quant 注入，绝不打印。
    docker run -d \
      --name quant \
      --restart always \
      -p 127.0.0.1:8080:8080 \
      ${DATABASE_URL:+-e "DATABASE_URL=$DATABASE_URL"} \
      ${PORT:+-e "PORT=$PORT"} \
      -e "TUSHARE_TOKEN=$TUSHARE_TOKEN" \
      -e "TUSHARE_API_URL=$TUSHARE_API_URL" \
      -e "JWT_SECRET=$JWT_SECRET" \
      ${DINGTALK_CLIENT_ID:+-e "DINGTALK_CLIENT_ID=$DINGTALK_CLIENT_ID"} \
      ${RUST_LOG:+-e "RUST_LOG=$RUST_LOG"} \
      -e "TZ=Asia/Shanghai" \
      "$RUN_IMAGE"

    echo "[run-quant] 本机已启动: http://localhost:8080 (镜像: $RUN_IMAGE)"
    ;;
esac
