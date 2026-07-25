# 多阶段构建：后端 Rust release + 前端 Dioxus/Trunk WASM -> 精简运行镜像
# 参考 aip/Dockerfile 的构建模式。

# ── Stage 1: 编译后端 quant-api ────────────────────────────────────────────
FROM rust:1-slim-bookworm AS build-backend

RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 生产级优化由 Cargo.toml profile.release 控制（fat LTO + codegen-units=1 + panic=abort + opt-level=3）

# 先复制 workspace 清单做依赖缓存层
COPY Cargo.toml Cargo.lock* ./
COPY quant-common/Cargo.toml quant-common/Cargo.toml
COPY quant-data/Cargo.toml quant-data/Cargo.toml
COPY quant-backtest/Cargo.toml quant-backtest/Cargo.toml
COPY quant-factor/Cargo.toml quant-factor/Cargo.toml
COPY quant-api/Cargo.toml quant-api/Cargo.toml
RUN mkdir -p quant-common/src quant-data/src quant-backtest/src quant-factor/src quant-api/src \
    && echo "fn main() {}" > quant-api/src/main.rs \
    && echo "// placeholder" > quant-common/src/lib.rs \
    && echo "// placeholder" > quant-data/src/lib.rs \
    && echo "// placeholder" > quant-backtest/src/lib.rs \
    && echo "// placeholder" > quant-factor/src/lib.rs \
    && cargo build -p quant-api --release \
    && rm -rf quant-common/src quant-data/src quant-backtest/src quant-factor/src quant-api/src \
              target/release/deps/quant_api* target/release/deps/libquant_*

# 复制真实源码并编译
COPY quant-common/ quant-common/
COPY quant-data/ quant-data/
COPY quant-backtest/ quant-backtest/
COPY quant-factor/ quant-factor/
COPY quant-api/ quant-api/
# quant-api 里若干 include_str! 引用 ../../../sql/*.sql（DDL 定义），构建期需要该目录存在
COPY sql/ sql/
RUN cargo build -p quant-api --release

# ── Stage 2: 编译前端 quant-ui (Dioxus + Trunk WASM) ───────────────────────
FROM rust:1-slim-bookworm AS build-frontend

RUN apt-get update && apt-get install -y pkg-config libssl-dev curl && rm -rf /var/lib/apt/lists/*
RUN rustup target add wasm32-unknown-unknown
# cargo install trunk：首次约 2-3 分钟，Docker layer 缓存后续构建不受影响
RUN cargo install trunk --locked

WORKDIR /app/quant-ui
COPY quant-ui/ ./
RUN trunk build --release

# ── Stage 3: 运行镜像 ───────────────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates libssl3 curl tzdata python3 python3-pip \
    && rm -rf /var/lib/apt/lists/* \
    && pip3 install --no-cache-dir --break-system-packages akshare \
    && ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime \
    && echo "Asia/Shanghai" > /etc/timezone

ENV TZ=Asia/Shanghai

WORKDIR /app

COPY --from=build-backend /app/target/release/quant-api /app/quant-api
COPY --from=build-frontend /app/quant-ui/dist /app/ui-dist

ENV PORT=8080 \
    QUANT_UI_DIST=/app/ui-dist \
    AKSHARE_PYTHON=/usr/bin/python3 \
    DATABASE_URL=postgres://gaocheng@host.docker.internal:5432/quant \
    RUST_LOG=quant=info,quant_api=info \
    RUST_BACKTRACE=0

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8080/api/v1/health || exit 1

CMD ["/app/quant-api"]
