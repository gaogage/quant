-- Phase 11 策略发现 profile 分派配置表（R8 批次3b）。
--
-- 目的：把 phase7_search_config 的 162 分支硬编码 match 的"profile 名→default 方法"映射
-- 搬到 DB 驱动。DB 空时回退硬编码 match（三层降级，过渡期保留）。
--
-- 设计：本表只存"profile 名→default 方法名"映射 + 别名，不存 config jsonb。
-- default 方法体含继承链+字段覆盖+generator 调用（151/162 调 generator 函数），
-- 无法安全序列化为 JSON。新增 profile 仍需写 *_default() 方法，但别名/优先级/启停可 DB 驱动。
--
-- 参考：factor_backfill_route 表的三层降级模式（scheduler.rs:trigger_v24_backfill_routes）。

CREATE TABLE IF NOT EXISTS public.search_profile_config (
    profile_name    TEXT PRIMARY KEY,          -- 规范 profile 名（如 professional_risk_breakthrough）
    aliases         JSONB NOT NULL DEFAULT '[]'::jsonb,  -- 别名数组（如 ["phase7_t", "sharpe_stabilization"]）
    default_method  TEXT NOT NULL,              -- 对应 LayeredSearchConfig::*_default() 方法名（如 professional_sharpe_stabilization_default）
    enabled         BOOLEAN NOT NULL DEFAULT true,
    priority        INT NOT NULL DEFAULT 0,
    description     TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE public.search_profile_config IS '策略发现 profile 分派配置（R8 批次3b）。DB 空时回退硬编码 match。';
COMMENT ON COLUMN public.search_profile_config.profile_name IS '规范 profile 名，与 LayeredSearchConfig::*_default() 的 *_default 前缀一致';
COMMENT ON COLUMN public.search_profile_config.aliases IS '别名 JSON 数组，包含 phase7_* 历史代号、缩写等（search_profile 参数匹配此字段）';
COMMENT ON COLUMN public.search_profile_config.default_method IS '对应 Rust 方法名（如 professional_risk_breakthrough_default），代码侧 match 此字段调 *_default()';
COMMENT ON COLUMN public.search_profile_config.enabled IS 'false 则该 profile 不参与分派';
COMMENT ON COLUMN public.search_profile_config.priority IS '同别名冲突时优先级（小者优先）';

-- 别名查询索引（aliases jsonb 包含查询用 GIN）
CREATE INDEX IF NOT EXISTS idx_search_profile_config_aliases ON public.search_profile_config USING gin (aliases jsonb_path_ops);
CREATE INDEX IF NOT EXISTS idx_search_profile_config_enabled_priority ON public.search_profile_config (enabled, priority);
