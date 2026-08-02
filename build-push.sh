#!/usr/bin/env bash
# quant-api 多架构构建 + 推送 ACR
#
# Dockerfile 是多阶段构建：后端 Rust release + 前端 Dioxus/Trunk WASM -> 精简运行镜像。
# 一次构建 linux/amd64 + linux/arm64 两份推到同一个 ACR tag（manifest list），
# 客户端按自己架构自动拉取匹配那份。
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

ACR_REGISTRY="crpi-9l8t2h4ro3skt9yg.cn-hangzhou.personal.cr.aliyuncs.com"
ACR_IMAGE="$ACR_REGISTRY/gaogage/quant-api:latest"
BUILDX_BUILDER="${BUILDX_BUILDER:-default-builder}"

echo "[build-push] 多架构构建 (linux/amd64 + linux/arm64) 并推送: $ACR_IMAGE"
docker buildx build --builder "$BUILDX_BUILDER" \
  --platform linux/amd64,linux/arm64 \
  -t "$ACR_IMAGE" \
  --push .

echo "[build-push] 完成: $ACR_IMAGE"
