-- 2026-09-27 MVO 权重引擎残余硬编码全部 DB 配置化（用户裁决：审计清单全批）
-- 背景：mvo_weights.rs compute_lw_mvo_weights 内 Kelly 缩放、动量窗口、
-- bull fallback 共 7 个写死值，违反配置化治理（任务80/81 精神）。
-- 默认值保持现行为，存量策略零行为变化。

ALTER TABLE strategy_config
    -- Kelly 缩放：scale = (kelly_scale_base + annual_ir).clamp(kelly_scale_floor, kelly_scale_cap)
    ADD COLUMN IF NOT EXISTS kelly_scale_base double precision DEFAULT 0.5,
    ADD COLUMN IF NOT EXISTS kelly_scale_floor double precision DEFAULT 0.3,
    ADD COLUMN IF NOT EXISTS kelly_scale_cap double precision DEFAULT 1.5,
    -- 动量检测：月度收益 lookback 月数 + 短/长动量窗口月数
    -- bear: 短窗累计 < regime_bear_momentum_threshold；bull: 长窗累计 > bull 门槛
    ADD COLUMN IF NOT EXISTS mvo_lookback_months integer DEFAULT 36,
    ADD COLUMN IF NOT EXISTS regime_momentum_window_short integer DEFAULT 3,
    ADD COLUMN IF NOT EXISTS regime_momentum_window_long integer DEFAULT 6,
    -- bull 分支 fallback：regime_bull_min_stock <= 0 时的兜底牛市仓位
    ADD COLUMN IF NOT EXISTS regime_bull_min_stock_fallback double precision DEFAULT 0.20;
