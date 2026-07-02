# Task 1 报告: 回放入口通用化重命名 paper_v19_replay → historical_replay

## 状态

DONE (待主代理编译验证)

## 改动文件清单

### quant-api (commit: effe1a3)

| 文件 | 改动 |
|:-----|:-----|
| `src/routes/paper_v19_replay.rs` → `src/routes/historical_replay.rs` | git mv 重命名(保留历史,91% 相似度) |
| `src/routes/historical_replay.rs` | 模块文档去 v19、V19ReplayRequest→HistoricalReplayRequest、historical_replay_v19→historical_replay、run_v19_replay→run_historical_replay、删 lev_enabled→"v21_lev" override + unwrap_or("v19") fallback(改用账号 strategy_version_id)、"paper_replay_v19"→"paper_replay"、"v19 模拟无有效交易日"→"回放无有效交易日" |
| `src/routes/mod.rs` | `pub mod paper_v19_replay;`→`pub mod historical_replay;`,按字母序移到 factors 后 ml 前 |
| `src/main.rs` | 路由 `/api/v1/quant/paper/historical-replay-v19` → `/api/v1/quant/paper/historical-replay`,handler 改 `routes::historical_replay::historical_replay` |
| `src/routes/accounts.rs` | 注释 `paper_v19_replay::compute_yearly` → `historical_replay::compute_yearly` |
| `src/routes/portfolio.rs` | 注释 `paper_v19_replay::compute_yearly_from_navs` → `historical_replay::compute_yearly_from_navs` |

### quant-ui (commit: c9c1649)

| 文件 | 改动 |
|:-----|:-----|
| `src/api.rs:357` | 路径 `historical-replay-v19` → `historical-replay` |

## git mv 历史保留

成功。git 检测到 91% 相似度,正确识别为 rename 而非 delete+add。

## 自审发现

1. **路由冲突处理**:发现 `main.rs` 原有一个旧路由 `/api/v1/quant/paper/historical-replay` 指向 `routes::paper::historical_replay`(paper.rs 中一个独立的旧回放 handler)。按 brief 要求将 v19 路由重命名到同路径会导致 Axum 运行时 panic(重复路由)。处理:删除旧路由注册(paper.rs 中的旧 handler 函数保留为 dead code,不影响编译)。旧 handler 前端已不再调用(grep 确认前端只调 historical-replay-v19)。

2. **仓库结构**:quant-api 和 quant-ui 实际是同一个 git 仓库(`/Users/gaocheng/workspace/quant`),非两个独立仓库。两次 commit 均在同一仓库,各自捕获正确文件。

3. **lev_enabled/lev_mult/lev_mode**:删除 override 后这三个变量仍被 `run_daily_simulation` 调用使用(行 98-101),不会有 unused 警告。

4. **strategy_id 变量**:原代码 `strategy_id` 从 SQL 查出后用于 override 逻辑。现改为 `let sid = strategy_id.as_deref().ok_or(...)?`,strategy_id 仍被使用,无 unused 警告。

## Commit Hash

- quant-api: `effe1a3` (refactor(replay): 回放入口通用化重命名)
- quant-ui: `c9c1649` (refactor(api): 回放路径)
- 注:两者在同一 git 仓库 `/Users/gaocheng/workspace/quant`

## 测试状态

未编译(主代理统一执行)。
