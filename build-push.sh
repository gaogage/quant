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
# 2026-09-26 版本管理定版（用户裁决）：镜像 tag 从 1.0.0 起与代码语义版本同步——
# 每次构建双 tag（latest + vX.Y.Z），X.Y.Z 读 workspace Cargo.toml。
# a=大版本(框架/大功能) b=小版本(小功能) c=bug修复
VERSION=$(grep -m1 '^version' Cargo.toml | cut -d" -f2)
ACR_VERSIONED_IMAGE="$ACR_REGISTRY/gaogage/quant-api:v$VERSION"
BUILDX_BUILDER="${BUILDX_BUILDER:-default-builder}"

echo "[build-push] 多架构构建 (linux/amd64 + linux/arm64) 并推送: $ACR_IMAGE"
docker buildx build --builder "$BUILDX_BUILDER" \
  --platform linux/amd64,linux/arm64 \
  -t "$ACR_IMAGE" \
  -t "$ACR_VERSIONED_IMAGE" \
  --push .

echo "[build-push] 完成: $ACR_IMAGE + $ACR_VERSIONED_IMAGE"
