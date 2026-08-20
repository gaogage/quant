# Quant 量化系统

## 项目概述

A 股量化研究平台：数据采集 → 因子计算 → 策略回测 → 绩效验证 → 模拟交易。

## 项目结构

```
quant/
├── quant-common/    # 共享类型、错误、任务状态
├── quant-data/      # Tushare 数据接入、标准化、持久化
├── quant-factor/    # 因子计算引擎
├── quant-backtest/  # 回测引擎（多标的、A股规则、基准跟踪）
├── quant-api/       # Axum HTTP API 服务器（默认端口 8081）
├── quant-ui/        # Web UI（嵌入 API）
├── scripts/         # Python 分析脚本（见下方）
├── sql/             # 数据库迁移脚本
└── .venv/           # Python 虚拟环境（uv 管理，Python 3.14）
```

## 技术栈

| 组件 | 技术 | 版本/说明 |
|------|------|----------|
| 后端 | Rust | 2021 edition |
| Web | Axum | 0.8 |
| 数据库 | PostgreSQL + TimescaleDB | `postgres://gaocheng@localhost/quant` |
| 数据源 | Tushare Pro | Valentina 专线代理 |
| 分析 | Python 3.14 | numpy + psycopg2-binary |
| 包管理 | uv | 0.7.2 |

## 开发命令

```bash
# Rust
cargo check                    # 编译检查
cargo run -p quant-api         # 启动 API（端口 8081）
cargo test                     # 测试
cargo fmt                      # 格式化
cargo clippy                   # Lint

# Python 分析脚本
.venv/bin/python3 scripts/quant_bootstrap.py --help
.venv/bin/python3 scripts/quant_data_export.py --help
.venv/bin/python3 scripts/quant_wfa_metrics.py --help
.venv/bin/python3 scripts/quant_annual_breakdown.py --help
```

## Python 分析脚本

位于 `scripts/` 目录，公共模块 `quant_common.py`：

| 脚本 | 用途 |
|------|------|
| `quant_data_export.py` | 从 PostgreSQL 导出日收益/权益曲线到 CSV |
| `quant_bootstrap.py` | Bootstrap Sharpe 置信区间分析 |
| `quant_wfa_metrics.py` | WFA 训练/测试分段绩效对比 |
| `quant_annual_breakdown.py` | 年度收益分解 |
| `quant_common.py` | 公共函数（DB连接、指标计算、格式化） |

### 运行时零 Python 依赖（铁律）

- **正式量化系统（quant-api 运行镜像）不引入任何 Python 运行时依赖**。主程序是 Rust，生产链路（数据同步/调仓/盯市/日报）全部用 Rust 实现，禁止在正式代码中桥接 python 进程。
- Python 只用于**验证、预研、离线分析**（`scripts/` + `.venv/`），属于量化系统的独立辅助工具，不参与生产数据链路。
- 数据源当日缺失时走"次日 T+1 补齐 + 补发"机制，**不引入第二数据源做运行时兜底**（曾试过 akshare 东财源运行时兜底，因限流不稳且有 python 依赖，2026-08-20 移除）。
- 存量例外：`sync/analyst_revision.rs` 的 akshare python 桥接为历史遗留，待迁移 Rust 实现或下线，新功能不得模仿。

## 数据库核心表

| 表 | 用途 |
|----|------|
| `backtest_equity_curve` | 回测净值曲线（TimescaleDB hypertable） |
| `backtest_task` | 回测任务定义 |
| `backtest_result` | 回测指标汇总 |
| `paper_nav_snapshot` | 模拟盘每日净值 |
| `paper_account` | 模拟账户 |
| `optimization_trial` | 优化参数试验 |
| `robustness_gate_result` | 鲁棒性门禁结果 |

> `05-表结构设计.md` 是数据库权威文档，任何表结构变更必须先更新。

## 模块边界

| 模块 | 职责 | 禁止 |
|------|------|------|
| quant-data | 数据接入、标准化、质量检查、落库 | 不生成策略信号、不计算回测指标 |
| quant-factor | 因子计算、PIT 对齐 | 不直接访问外部 API |
| quant-backtest | 回测执行、组合管理、指标计算 | 不读"当前最新数据"、不改策略参数 |
| quant-api | HTTP API、路由、请求响应 | 不直接访问数据库（通过 service 层） |
| quant-common | 共享类型、错误定义 | 不依赖其他模块 |

## 设计文档

所有设计文档位于 `docs/projects/quant/`：

| 文档 | 说明 |
|------|------|
| `tasks/quant/19-专业量化系统目标蓝图.md` | 蓝图目标（专业级/精英级） |
| `knowledge/system/策略方向问题指引.md` | 策略绩效分析与优化方向 |
| `tasks/quant/05-表结构设计.md` | 数据库权威文档 |

## 当前阶段

Phase 7-DB：专业级数据底座 + 回测引擎 + 策略发现全链路打通。
- 日线数据 2014-2026，5130+ 只股票
- 多因子组合方案（h1/h20 等 combo）
- MVO 组合优化 + 杠杆配置
- WFA 鲁棒性验证管线
- 模拟交易闭环
