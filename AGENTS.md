# Quant 量化系统

## 项目结构

```
quant/
├── quant-common/    # 共享类型、错误、任务状态
├── quant-data/      # Tushare 数据接入、标准化、持久化
├── quant-backtest/  # 回测引擎（多标的、A股规则、基准跟踪）
└── quant-api/       # Axum HTTP API 服务器
```

## 设计文档

所有设计文档位于 `docs/projects/quant/`，遵循以下硬规则：
- `05-表结构设计.md` 是数据库唯一权威文档
- 任何表结构变更必须先更新文档并评审
- 模块边界见 `07-模块细化设计路线图.md`

## 当前阶段

Phase 0.6：评估整改与实施门禁固化。

## 开发命令

```bash
# 编译检查
cargo check

# 运行
cargo run -p quant-api

# 测试
cargo test

# 格式化
cargo fmt

# Lint
cargo clippy
```

## 模块边界

| 模块 | 职责 | 禁止 |
|------|------|------|
| quant-data | 数据接入、标准化、质量检查、落库、数据版本 | 不生成策略信号、不计算回测指标 |
| quant-backtest | 回测执行、组合管理、指标计算 | 不读"当前最新数据"、不改策略参数 |
| quant-api | HTTP API、路由、请求响应 | 不直接访问数据库 |

## 环境

- Rust 2021 edition
- PostgreSQL + TimescaleDB
- Tushare Pro API
- Axum 0.8
