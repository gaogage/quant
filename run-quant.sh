#!/usr/bin/env bash
# quant-api 容器部署（不使用 docker-compose）
# 端口: 8080 -> 8080  数据库: 本机 PostgreSQL host.docker.internal:5432/quant
#
# 镜像策略（与 aip / mcp-rsmysql-http / mcp-gitlab-http 一致）：
# 个人制作或改动的镜像一律推 ACR 并用 ACR 镜像运行。
# 默认 IMAGE 指向 ACR，docker run 用 ACR 镜像名（非本地 quant-api:latest）。
# 本地临时调试可用 IMAGE=quant-api:latest 跳过构建/推送。
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

# 读取 .env.quant（若存在），提供 JWT_SECRET / TUSHARE_TOKEN 等敏感配置
if [ -f .env.quant ]; then set -a; . ./.env.quant; set +a; fi

# ── 镜像配置 ──────────────────────────────────────────────────────────────
# 默认 ACR 镜像；可用 IMAGE=quant-api:latest 覆盖为本地构建镜像（跳过 ACR 推送）
ACR_REGISTRY="crpi-9l8t2h4ro3skt9yg.cn-hangzhou.personal.cr.aliyuncs.com"
ACR_IMAGE="$ACR_REGISTRY/gaogage/quant-api:latest"
IMAGE="${IMAGE:-$ACR_IMAGE}"

# ── 构建 + ACR 推送（仅当用 ACR 镜像时）──────────────────────────────────
# profile.release: fat LTO + codegen-units=1 + panic=abort + opt-level=3
if [ "$IMAGE" = "$ACR_IMAGE" ]; then
  echo "[run-quant] 构建生产镜像 quant-api:latest ..."
  docker build -t quant-api:latest .

  echo "[run-quant] 推送到 ACR: $ACR_IMAGE"
  docker tag quant-api:latest "$ACR_IMAGE"
  docker push "$ACR_IMAGE"
  # docker run 用 ACR 镜像名，确保本机跑的就是 ACR 里的版本
  RUN_IMAGE="$ACR_IMAGE"
else
  echo "[run-quant] 使用本地镜像跳过 ACR 推送: $IMAGE"
  RUN_IMAGE="$IMAGE"
fi

# ── 旧容器清理 ────────────────────────────────────────────────────────────
docker rm -f quant >/dev/null 2>&1 || true

# ── 本机部署：8080->8080，复用本机 PG ───────────────────────────────────
# DATABASE_URL 默认走 Dockerfile 内的 host.docker.internal:5432/quant，
# .env.quant 若显式覆盖 DATABASE_URL 则以它为准。
# 敏感配置（JWT/TUSHARE/钉钉）通过 -e 从 .env.quant 注入，绝不打印。
docker run -d \
  --name quant \
  --restart always \
  -p 8080:8080 \
  ${DATABASE_URL:+-e "DATABASE_URL=$DATABASE_URL"} \
  ${PORT:+-e "PORT=$PORT"} \
  -e "TUSHARE_TOKEN=$TUSHARE_TOKEN" \
  -e "TUSHARE_API_URL=$TUSHARE_API_URL" \
  -e "JWT_SECRET=$JWT_SECRET" \
  ${DINGTALK_CLIENT_ID:+-e "DINGTALK_CLIENT_ID=$DINGTALK_CLIENT_ID"} \
  ${RUST_LOG:+-e "RUST_LOG=$RUST_LOG"} \
  -e "TZ=Asia/Shanghai" \
  "$RUN_IMAGE"

echo "quant-api 已启动: http://localhost:8080 (镜像: $RUN_IMAGE)"
if [ "$IMAGE" = "$ACR_IMAGE" ]; then
  echo "ACR 镜像: $ACR_IMAGE"
fi
