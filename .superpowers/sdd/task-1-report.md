# Task 1 报告: equity_curve_sync.rs 核心同步模块

## 状态

**DONE_WITH_CONCERNS** — 代码完成,待主代理编译验证。子代理未运行 cargo(按约束)。

## 改动文件

| 文件 | 操作 | 说明 |
|------|------|------|
| `quant-api/src/routes/equity_curve_sync.rs` | 新建 | 463 行,含全部 pub 函数 + 3 个集成测试 |
| `quant-api/src/routes/mod.rs` | 修改 | 加 `pub mod equity_curve_sync;`(按字母序插在 dingtalk 之后) |
| `quant-api/src/routes/scheduler.rs` | 修改 | `get_latest_data_version` 从私有改为 `pub(crate)`(仅可见性,1 行) |

## Commit

- `973634c` — `feat(equity-sync): 新建 equity_curve_sync 模块(单策略同步+活跃策略遍历+readiness审计+ETF发行日)`

## pub 函数最终签名

```rust
pub async fn is_etf_listed_on(db: &PgPool, symbol: &str, date: NaiveDate) -> bool
pub async fn collect_active_strategies(db: &PgPool) -> Vec<String>
pub async fn detect_combo_sharing(db: &PgPool, strategy_id: &str) -> Vec<String>
pub async fn sync_strategy_equity_curve(db: &PgPool, strategy_id: &str, start: NaiveDate, end: NaiveDate, background: bool) -> Result<SyncResult, String>
pub async fn sync_active_strategies_equity_curves(db: &PgPool) -> Vec<SyncResult>
pub async fn audit_equity_curve_readiness(db: &PgPool, strategy_id: &str) -> Result<ReadinessReport, String>
```

## pub 结构体

```rust
pub struct SyncResult { strategy_id: String, task_id: Option<String>, updated_strategy_ids: Vec<String>, status: String, error: Option<String> }
pub struct ReadinessReport { strategy_id: String, equity_curve_task_id: Option<String>, equity_curve_coverage: EquityCurveCoverage, etf_price_coverage: EtfPriceCoverage, missing_items: Vec<String>, ready: bool }
pub struct EquityCurveCoverage { trade_day_count: i64, first_date: Option<NaiveDate>, last_date: Option<NaiveDate> }
pub struct EtfPriceCoverage { total_etfs: usize, listed_etfs: usize, not_yet_listed: Vec<String> }
```

## 与 brief 的差异

### 1. scheduler.rs 可见性改动(brief 明确要求)

brief 提示 `get_latest_data_version` 是私有 `async fn`,跨模块调会编译失败。已将 scheduler.rs:25 的 `async fn get_latest_data_version` 改为 `pub(crate) async fn get_latest_data_version`,只改可见性,不动实现。

### 2. UPDATE strategy_config 双写(brief 漏洞修复)

**问题**: brief 的 `UPDATE strategy_config SET equity_curve_task_id = $1 WHERE strategy_id = $2` 只更新 composite 行。但 DB 中 `equity_curve_task_id` 同时冗余存储在:
- composite 行(`strategy_id = 'v19'`,无 parent)
- a_share 子行(`strategy_id = 'v19-a_share'`, `parent_strategy_id = 'v19'`)

两者值相同(已通过 psql 验证: v19 composite 和 v19-a_share 都 = `fbt-6dccbf98-...`)。

`load_strategy_config`(scheduler.rs:697) 读 composite 行的 `equity_curve_task_id`;`load_resolved_strategy`(strategy.rs:159) 通过 a_share 子行的 `equity_curve_task_id` 加载。若只更新 composite 行,`load_resolved_strategy` 会读到旧值。

**修复**: `sync_strategy_equity_curve` 中对 `sharing` 列表每个策略 ID 执行两条 UPDATE:
- `WHERE strategy_id = $2`(composite 行)
- `WHERE parent_strategy_id = $2 AND asset_class = 'a_share'`(a_share 子行)

保持两处一致。此改动不影响 brief 的接口签名和测试。

### 3. 代码格式化

brief 的代码片段部分行宽超 120 字符,实现时按 4 空格缩进 + 行宽 120 格式化,逻辑不变。

## 自审发现

1. **DB 验证已通过**(非编译验证):
   - `strategy_config` 表 v19/v21/v21_lev 的 composite + a_share 子行均存在,`equity_curve_task_id` 冗余一致。
   - `market_stock.list_date` 518880.SH=2013-07-29, 159980.SZ=2019-12-24(Task 0 回填正确)。
   - `backtest_equity_curve` 表存在。
   - `paper_account` 当前 active 账号挂 v21/v21_lev(测试期望含 v19,但 v19 当前无 active 账号 — 测试是 `#[ignore]` 集成测试,需主代理按需准备数据或调整断言)。

2. **测试 `test_collect_active_strategies_dedup` 可能失败**: 测试断言含 v19,但 DB 当前只有 v21/v21_lev 活跃。这是 brief 的测试数据假设,非代码缺陷。主代理运行测试前需确认 DB 状态,或调整该测试断言。

3. **`load_strategy_config` panic 风险**: 若 `strategy_id` 不在 `strategy_config` 表中,`load_strategy_config` 会 panic(非返回 Result)。`sync_strategy_equity_curve` 调用方应确保传入有效 strategy_id。这与 brief 一致(brief 直接 `.await` 无错误处理)。

4. **`collect_active_strategies` 和 `detect_combo_sharing` 用 `unwrap_or_default`**: DB 查询失败时返回空 Vec 而非 panic,符合容错设计。

## 待主代理编译命令

```bash
# 编译验证(预期 dead_code 警告,Task 2/3 才引用)
cargo build --manifest-path /Users/gaocheng/workspace/quant/quant-api/Cargo.toml --bin quant-api 2>&1 | tail -30

# 集成测试(需 DB,注意 test_collect_active_strategies_dedup 可能因 v19 无 active 账号失败)
cargo test --manifest-path /Users/gaocheng/workspace/quant/quant-api/Cargo.toml --bin quant-api routes::equity_curve_sync -- --ignored 2>&1 | tail -20
```

## 测试代码位置

`quant-api/src/routes/equity_curve_sync.rs` 的 `#[cfg(test)] mod tests`,3 个 `#[ignore]` 集成测试:
- `test_collect_active_strategies_dedup` — 活跃策略收集 + 去重
- `test_detect_combo_sharing_v21_v21_lev` — combo 共用检测
- `test_is_etf_listed_on` — ETF 发行日双保险
