# Task 1 Report: P4.0c — resolved_to_legacy_sc fallback 改报错

## 状态

DONE (待主代理编译验证)

## 改动文件清单

### 1. `quant-api/src/routes/scheduler.rs`

**函数签名变更 (行 790):**
- 旧: `pub(crate) fn resolved_to_legacy_sc(rs: &ResolvedStrategy) -> StrategyConfig`
- 新: `pub(crate) fn resolved_to_legacy_sc(rs: &ResolvedStrategy) -> Result<StrategyConfig, String>`

**函数体变更 (行 790-824):**
- `let mvo = rs.mvo.as_ref();` → `let mvo = rs.mvo.as_ref().ok_or_else(|| format!("策略 {} 缺 mvo 配置", rs.strategy_id))?;`
- `let a_share = rs.assets.iter().find(|a| a.asset_class == AssetClass::AShare);` → `.ok_or_else(|| format!("策略 {} 缺 a_share asset", rs.strategy_id))?;`
- mvo 从 `Option<&MvoParams>` 变为 `&MvoParams`,a_share 从 `Option<&AssetStrategy>` 变为 `&AssetStrategy`
- 所有 `mvo.map(|m| m.xxx).unwrap_or(default)` → `mvo.xxx`
- 所有 `a_share.map(|a| a.security.xxx).unwrap_or(default)` → `a_share.security.xxx`
- `a_share.and_then(|a| a.security.equity_curve_task_id.clone()).unwrap_or_default()` → `a_share.security.equity_curve_task_id.clone().unwrap_or_default()` (equity_curve_task_id 是 Option<String>)
- `a_share.and_then(|a| a.security.prediction_set_id.clone())` → `a_share.security.prediction_set_id.clone()`
- 末尾 `StrategyConfig { ... }` → `Ok(StrategyConfig { ... })`

**调用点 1 — 测试 (行 612):**
- `let sc = resolved_to_legacy_sc(&rs);` → `let sc = resolved_to_legacy_sc(&rs).unwrap();`

**调用点 2 — generate_paper_signals_for_all (行 2571):**
- `let sc = resolved_to_legacy_sc(&rs);` → `let sc = resolved_to_legacy_sc(&rs)?;`
- (该函数返回 `Result<(), String>`,直接 `?` 传播)

**新测试 (行 646-784):**
- `test_resolved_to_legacy_sc_missing_mvo_errors`:构造 `rs.mvo=None`,断言 `resolved_to_legacy_sc(&rs).is_err()` 且错误消息包含 "缺 mvo 配置"
- `test_resolved_to_legacy_sc_missing_a_share_errors`:构造 `rs.assets` 无 AShare,断言 `resolved_to_legacy_sc(&rs).is_err()` 且错误消息包含 "缺 a_share asset"

### 2. `quant-api/src/routes/rebalance.rs`

**调用点 (行 107):**
- `let sc = resolved_to_legacy_sc(rs);` → `let sc = resolved_to_legacy_sc(rs)?;`
- (`rebalance_account` 返回 `Result<usize, String>`,直接 `?` 传播)

## 自审发现

1. **语义对齐**:原 fallback 值 (0.12, 0.75, 0.80, 0.5, 600, 250, 0.2, 2.5, "fixed", 0.5, 30, 0.30, 0.12, "descending", "research_baseline") 全部移除。这些值此前掩盖了配置缺失,现在改为显式报错,与 `load_strategy_config` 的 panic 初衷一致。

2. **equity_curve_task_id 兼容**:`equity_curve_task_id` 字段本身是 `Option<String>`,`.unwrap_or_default()` 保留在 Option 层面而非 a_share 层面,语义正确 — a_share 存在但 equity_curve_task_id 为 None 时仍返回空字符串(与原行为一致,不算 fallback)。

3. **prediction_set_id 兼容**:同理,`prediction_set_id` 是 `Option<String>`,a_share 存在时直接 clone,可为 None。

4. **调用点 generate_paper_signals_for_all (行 2571)**:该函数后续 (行 2573) 仍用 `let a_share = rs.assets.iter().find(...)` 做信号源展示,这个 a_share 是独立的局部变量,与 resolved_to_legacy_sc 内部的 a_share 无关,不冲突。如果 rs 缺 a_share,resolved_to_legacy_sc 会先报错返回,不会走到后续的 a_share 查找,逻辑正确。

5. **错误传播链**:rebalance.rs:107 `?` 传播到 `rebalance_account` 的 `Result<usize, String>`;scheduler.rs:2571 `?` 传播到 `generate_paper_signals_for_all` 的 `Result<(), String>`。两个调用函数都返回 `Result<_, String>`,类型匹配。

## 待主代理编译命令

```bash
# 编译验证
cargo build --manifest-path /Users/gaocheng/workspace/quant/quant-api/Cargo.toml --bin quant-api 2>&1 | tail -30

# 测试验证
cargo test --manifest-path /Users/gaocheng/workspace/quant/quant-api/Cargo.toml --bin quant-api routes::scheduler 2>&1 | tail -15
```

预期:
- BUILD SUCCESS
- test_resolved_to_legacy_sc_mapping PASS
- test_resolved_to_legacy_sc_missing_mvo_errors PASS
- test_resolved_to_legacy_sc_missing_a_share_errors PASS

## 测试代码位置

- `quant-api/src/routes/scheduler.rs` 行 646-784:
  - `test_resolved_to_legacy_sc_missing_mvo_errors` (行 646-705)
  - `test_resolved_to_legacy_sc_missing_a_share_errors` (行 707-784)

## 潜在风险点 (需人类开发者确认)

1. **运行时行为变更**:此前如果数据库中某个策略确实缺 mvo 或 a_share 配置,`resolved_to_legacy_sc` 会静默用 fallback 值继续运行。现在改为返回 Err,会导致 `rebalance_account` 和 `generate_paper_signals_for_all` 跳过该账号/策略。需确认生产环境无此类数据(策略配置必须完整)。

2. **generate_paper_signals_for_all 错误处理**:该函数对每个账号循环调用,行 2571 的 `?` 会直接返回错误终止整个函数。如果希望单个账号失败时跳过而非终止,需改为 `match` 或 `if let Ok(sc) = ...`。但根据 brief 的要求,直接 `?` 传播是预期行为(与原 panic 初衷一致 — 配置缺失是严重错误)。

## Commit

```
refactor(strategy): resolved_to_legacy_sc fallback 改报错(P4.0c),缺失 mvo/a_share 必报错

Co-Authored-By: Claude <noreply@anthropic.com>
```
