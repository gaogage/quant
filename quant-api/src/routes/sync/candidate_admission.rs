//! phase7 候选源/准入装配：new_alpha 候选源构造、with_* 市场状态变体、
//! 行业成员快照 readiness 与 optional_source JSON 汇总。
use super::*;

pub(crate) fn phase7_new_alpha_candidate_sources() -> Vec<Value> {
    vec![
        json!({
            "source": "market_margin_regime",
            "current_tables": ["market_margin"],
            "admission_scope": "regime_or_risk_budget_only",
            "readiness": "market_level_ready_not_cross_sectional_alpha",
            "pit_boundary": "trade_date is same-day market-level data; use only after the trade date is closed or as next-session regime input",
            "why_not_trainable_now": "融资融券汇总是交易所级时间序列，不能直接形成股票横截面排序 alpha",
            "next_step": "evaluate_as_regime_or_risk_budget_feature"
        }),
        json!({
            "source": "market_moneyflow_hsgt_regime",
            "current_tables": ["market_moneyflow_hsgt"],
            "admission_scope": "regime_or_risk_budget_only",
            "readiness": "market_level_ready_not_cross_sectional_alpha",
            "pit_boundary": "trade_date is same-day market-level data; use only after the trade date is closed or as next-session regime input",
            "why_not_trainable_now": "北向资金汇总是市场级资金流，适合 regime/risk budget，不适合作为单股横截面 alpha",
            "next_step": "evaluate_as_regime_or_risk_budget_feature"
        }),
        json!({
            "source": "industry_prosperity_proxy",
            "current_tables": ["market_stock", "market_financial_indicator", "market_stock_daily_bar", "market_stock_moneyflow"],
            "candidate_raw_sources": ["tushare:index_classify", "tushare:index_member"],
            "admission_scope": "broad_base_proxy_candidate",
            "alpha_admission_gate": industry_prosperity_alpha_admission_policy_static(),
            "readiness": "industry_membership_permission_probe_required",
            "pit_boundary": "current market_stock.industry is static and cannot be treated as historical PIT industry classification",
            "why_not_trainable_now": "行业 PIT 原始源接入后仍需通过全历史 membership snapshot/coverage 审计和 P3.10 诊断，不能直接进入训练",
            "next_step": "run_industry_membership_permission_smoke_then_schema_available_at_audit"
        }),
        json!({
            "source": "block_trade_supply_demand",
            "current_tables": ["market_stock_block_trade"],
            "admission_scope": "stopped_same_family_after_p310",
            "readiness": "stopped_after_p310_economics_weak",
            "pit_boundary": "must persist announcement/trade publication date as available_at before any event-window feature",
            "why_not_trainable_now": "大宗交易供需源数据/PIT 可用，但 P3.10 显示覆盖偏窄、alpha economics 弱；不得继续同族扩参或直接 WFA",
            "next_step": "do_not_expand_same_family_shift_to_p320_new_source_admission"
        }),
        json!({
            "source": "equity_pledge_pressure",
            "current_tables": [],
            "candidate_raw_sources": ["tushare:pledge_stat", "tushare:pledge_detail"],
            "admission_scope": "new_p320_schema_contract_candidate",
            "readiness": "permission_smoke_passed_schema_contract_ready",
            "pit_boundary": "pledge_detail.ann_date is native available_at candidate; pledge_stat.end_date is a measurement date and cannot be used alone as availability",
            "why_not_trainable_now": "新候选源已完成生产 permission-smoke 和 schema contract，但尚未人工审查/应用 schema、全历史 bounded sync、coverage/readiness 或 P3.10",
            "next_step": "review_apply_equity_pledge_schema_then_bounded_sync_plan"
        }),
        json!({
            "source": "equity_incentive_execution_quality",
            "current_tables": [],
            "admission_scope": "raw_source_onboarding_required",
            "readiness": "schema_and_client_missing",
            "pit_boundary": "must persist disclosure/announcement date as available_at and execution periods as event attributes",
            "why_not_trainable_now": "当前没有股权激励原始表、Tushare client、同步账本或 PIT 可得日审计",
            "next_step": "add_permission_smoke_then_schema_and_bounded_sync"
        }),
    ]
}

pub(crate) fn phase7_exchange_announcement_order_capacity_candidate() -> Value {
    json!({
        "source_id": "exchange_announcement_order_capacity_text",
        "source_family": "regulatory_exchange_announcement_real_operations_text",
        "economic_hypothesis": "订单、合同、产能、投产、价格调整和重大供货协议等公告文本比常规财务/价量/资金流更接近真实经营边际变化；若能证明公告可得时间、文本证据和 broad-base 覆盖，可能提供低相关 PIT alpha source。",
        "candidate_raw_sources": [
            "akshare:stock_zh_a_disclosure_report_cninfo",
            "cninfo:announcement_detail_page",
            "licensed_vendor:broad_base_announcement_text_feed"
        ],
        "source_discovery_evidence": [
            {
                "candidate": "akshare:stock_zh_a_disclosure_report_cninfo",
                "status": "read_only_sample_smoke_available_source_discovery_only",
                "upstream": "cninfo",
                "observed_smoke": {
                    "as_of": "2026-06-25",
                    "akshare_version": "1.18.64",
                    "symbol": "000001",
                    "market": "沪深京",
                    "category": "日常经营",
                    "date_range": "20230101..20231231",
                    "row_count": 31,
                    "fields": ["代码", "简称", "公告标题", "公告时间", "公告链接"]
                },
                "native_available_at_candidate": "公告时间",
                "risk": "current smoke is one symbol/category only; category parser reliability, announcement detail text fetch and source_published_at timestamp are not audited",
                "decision": "permission_history_category_smoke_required_before_schema_or_sync"
            },
            {
                "candidate": "cninfo:announcement_detail_page",
                "status": "source_discovery_required",
                "required_from_link": ["announcementId", "orgId", "stockCode", "announcementTime"],
                "required_audit": ["text_fetch_success_rate", "source_published_at_timestamp", "text_hash_stability", "manual_evidence_span_precision_sample"],
                "decision": "do_not_sync_until_text_and_timestamp_audit_is_proven"
            },
            {
                "candidate": "licensed_vendor:broad_base_announcement_text_feed",
                "status": "source_discovery_required",
                "reason": "if public CNInfo/AkShare feed cannot provide reliable timestamps, full history, text and category stability, a licensed timestamped announcement feed is required",
                "decision": "restart_from_permission_schema_available_at_admission_if_vendor_is_available"
            }
        ],
        "current_tables": [],
        "schema_status": "schema_contract_defined_source_discovery_required",
        "client_status": "read_only_manual_smoke_only_no_rust_client",
        "sync_status": "not_started",
        "coverage_status": "not_started",
        "p310_status": "blocked",
        "factor_builder": "blocked",
        "bounded_wfa": "blocked",
        "v19_train_selection": "blocked",
        "pit_required": true,
        "available_at_policy": "use source_published_at from official announcement detail/feed when available; if only announcement date exists, map to next open session and forbid same-session intraday use",
        "schema_contract_endpoint": "GET /api/v1/quant/data/exchange-announcement-order-capacity/schema-contract",
        "admission_decision": "source_discovery_required_before_schema_or_sync",
        "blocked_reason": "sample feed proves candidate existence only; no full-history category replay, source_published_at timestamp audit, text fetch audit, evidence span precision audit, coverage audit, or correlation audit yet",
        "next_step": "implement_read_only_permission_history_category_smoke_for_akshare_cninfo_disclosure_then_cninfo_detail_text_fetch_audit"
    })
}

pub(crate) fn phase7_p319_candidate_admission_sources(
    futures_price_chain_readiness: Option<&Value>,
) -> Value {
    let mut admission = json!({
        "stage": "P3.24",
        "objective": "discover lower-correlation broad-base PIT alpha sources before any factor build, ML training, WFA admission, or v19 train selection",
        "hard_gate": "permission_schema_available_at_first",
        "global_policy": {
            "pit_required": true,
            "no_oos_reverse_tuning": true,
            "no_same_family_parameter_expansion": true,
            "model_algorithm_policy": {
                "algorithm_is_secondary_to_source_economics": true,
                "allowed_after": "candidate source passes data/PIT coverage and P3.10A-D economics gates",
                "forbidden_use": "do not use a new ML algorithm, full-period sign flip, label mining, or OOS feedback to rescue a source that failed RankIC, group return, decay, turnover/capacity, or bounded train robustness",
                "allowed_use": "after source admission, compare linear, tree/boosting and calibrated ensemble models only inside rolling train windows with net-of-cost objectives and unchanged test-window evaluation"
            },
            "required_sequence": [
                "permission_smoke",
                "schema_and_available_at_audit",
                "bounded_history_sync",
                "coverage_readiness_audit",
                "p310_rankic_group_decay_turnover_capacity",
                "bounded_wfa_only_after_diagnostics_pass"
            ],
            "promotion_rule": "only candidates passing data/PIT, RankIC, group return, decay, turnover/capacity and regime exposure gates may enter bounded WFA"
        },
        "stopped_same_family_sources": [
            "industry_prosperity_proxy",
            "market_residual_risk",
            "liquidity_regime",
            "event_surprise",
            "event_post_return_overlay",
            "moneyflow_congestion",
            "repurchase",
            "supply_float",
            "unlock_pressure",
            "block_trade_supply_demand",
            "main_business_fina_mainbz",
            "broad_analyst_revision_current_raw_bundle",
            "futures_price_chain_current_version",
            "equity_pledge_pressure_current_low_ratio_atom",
            "shareholder_structure_current_low_fanout_sleeve",
            "multi_vendor_analyst_revision_current_akshare_cninfo_revision"
        ],
        "candidates": [
            {
                "source_id": "p322_source_inventory",
                "source_family": "new_low_correlation_pit_broad_base_source_discovery",
                "economic_hypothesis": "蓝图达标需要新的信息增量，而不是继续压榨已证伪的公开低频同族源；优先寻找更接近经营兑现、订单、产能、价格链、真实预期修正或股权激励执行质量的 PIT broad-base 数据。",
                "candidate_raw_sources": [
                    "licensed_broad_base_analyst_revision_or_consensus_estimate_feed",
                    "regulatory_or_exchange_equity_incentive_employee_stock_plan_execution_feed",
                    "regulated_disclosure_or_exchange_feed_for_orders_capacity_price_chain",
                    "akshare:stock_rank_forecast_cninfo_stopped_p323f",
                    "akshare:stock_research_report_em_low_fanout_evidence",
                    "tushare:report_rc_blocked_40101"
                ],
                "ranked_candidates": [
                    {
                        "rank": 1,
                        "source_id": "licensed_broad_base_consensus_revision",
                        "status": "source_discovery_required",
                        "reason": "best aligned with true expectation-revision economics, but no licensed callable source is configured; current AkShare CNInfo rating-change expression failed P3.10 economics"
                    },
                    {
                        "rank": 2,
                        "source_id": "exchange_announcement_order_capacity_text",
                        "status": "source_discovery_required",
                        "reason": "closest to real operations/order/capacity information, but requires reliable announcement feed, parsing schema and available_at audit"
                    },
                    {
                        "rank": 3,
                        "source_id": "multi_vendor_analyst_revision",
                        "status": "stopped_after_p310_economics_failed",
                        "reason": "AkShare stock_rank_forecast_cninfo passed raw/PIT/correlation and factor/combo construction, but daily symbol cliff and 0/4 P3.10 horizons block WFA/v19; do not rescue current expression"
                    },
                    {
                        "rank": 4,
                        "source_id": "margin_detail_leverage_crowding",
                        "status": "stopped_after_p310_economics_failed",
                        "reason": "daily security-level margin financing/short data passed raw coverage/PIT but failed P3.10 economics, so do not rescue current version"
                    }
                ],
                "schema_status": "source_discovery_required",
                "client_status": "candidate_source_discovery_required",
                "sync_status": "not_started",
                "coverage_status": "not_started",
                "p310_status": "not_started",
                "pit_required": true,
                "available_at_policy": "native announcement/report/publication timestamp required; conservative next-session availability is allowed only when source publication time cannot be audited",
                "admission_decision": "multi_vendor_analyst_revision_stopped_after_p310_shift_to_next_low_correlation_source",
                "blocked_reason": "akshare_stock_rank_forecast_cninfo_current_expression_passed_data_pit_but_failed_daily_breadth_and_p310_economics",
                "next_step": "search_licensed_consensus_revision_or_exchange_announcement_order_capacity_source"
            },
            {
                "source_id": "margin_detail_leverage_crowding",
                "source_family": "security_level_leverage_crowding_and_short_pressure",
                "economic_hypothesis": "个股融资买入、偿还、融资余额、融券余量和融券卖出可刻画杠杆资金拥挤和去杠杆压力；当前版本已通过 raw/PIT 覆盖但 P3.10 经济性为负，不应继续救该同族表达。",
                "candidate_raw_sources": [
                    "tushare:margin_detail"
                ],
                "source_discovery_evidence": [
                    {
                        "candidate": "tushare:margin_detail",
                        "status": "stopped_after_p310_economics_failed",
                        "official_doc": "https://tushare.pro/document/2?doc_id=59",
                        "official_semantics": "security_level_margin_trading_detail_updated_next_day_around_0830",
                        "observed_fields_from_doc": ["trade_date", "ts_code", "rzye", "rqye", "rzmre", "rqyl", "rzche", "rqchl", "rqmcl", "rzrqye"],
                        "native_available_at_candidate": "next_session_after_exchange_publication",
                        "full_history_summary": {
                            "rows": 4604635,
                            "date_range": "2014-02-07..2026-06-23",
                            "pit_violations": 0
                        },
                        "diagnostics_summary": {
                            "latest_report_id": "exp-3812df52-2786-4a9a-b0a3-b5fc475e8ca4",
                            "mean_rankic_20_45_60_120": [-0.0332, -0.0320, -0.0288, -0.0256],
                            "passed_horizon_count": 0,
                            "decision": "do_not_enter_bounded_wfa_or_v19_train_selection"
                        },
                        "decision": "stop_margin_detail_current_version_after_negative_p310_economics"
                    }
                ],
                "current_tables": ["market_stock_margin_detail"],
                "schema_status": "completed",
                "client_status": "read_only_and_sync_client_completed",
                "sync_status": "full_history_raw_sync_completed",
                "coverage_status": "coverage_pit_green",
                "correlation_status": "completed_but_economics_failed",
                "p310_status": "completed_failed_economics",
                "pit_required": true,
                "available_at_policy": "official source says prior-day data is updated around next trading day 08:30; intraday decisions must use only records whose source_published_at or conservative next-session available_at is <= decision time",
                "admission_decision": "stopped_after_p310_economics_failed",
                "blocked_reason": "full_history_raw_coverage_pit_passed_but_rankic_negative_across_horizons; no_sign_flip_no_same_family_rescue_no_oos_reverse_tuning",
                "next_step": "do_not_rescue_margin_detail_current_version_shift_to_multi_vendor_analyst_revision_source_admission"
            },
            {
                "source_id": "futures_price_chain",
                "source_family": "real_operations_and_order_price_chain",
                "economic_hypothesis": "真实经营、订单、产能、价格链变化比价格成交同族特征更接近基本面边际变化，若可 PIT 化且覆盖 broad-base，可能提供低相关横截面信息。",
                "candidate_raw_sources": [
                    "tushare:fina_mainbz",
                    "tushare:fut_daily",
                    "tushare:fut_wsr",
                    "tushare:fut_holding",
                    "source_discovery_required_for_order_price_chain"
                ],
                "source_discovery_evidence": [
                    {
                        "candidate": "tushare:fina_mainbz",
                        "status": "stopped_after_p310_economics_weak",
                        "official_semantics": "main_business_composition_by_product_region_or_industry",
                        "observed_fields": ["ts_code", "end_date", "bz_item", "bz_code", "bz_sales", "bz_profit", "bz_cost", "curr_type", "update_flag"],
                        "missing_pit_fields": ["ann_date", "f_ann_date", "disclosure_date"],
                        "available_at_join_candidates": [
                            "market_financial_statement.ann_date_by_ts_code_end_date",
                            "market_stock_disclosure_date.actual_date_by_ts_code_period"
                        ],
                        "audit_endpoint": "POST /api/v1/quant/data/main-business/available-at-audit",
                        "readiness_endpoint": "GET /api/v1/quant/data/main-business/readiness-audit",
                        "diagnostics_endpoint": "POST /api/v1/quant/alpha-sources/main-business/diagnostics/report",
                        "latest_diagnostics_report_id": "exp-f6374904-1d5d-4bfc-afaf-64f95ce24040",
                        "diagnostics_summary": {
                            "data_pit_coverage": "green",
                            "effective_start_date": "2014-08-29",
                            "raw_rows": 500223,
                            "pit_violation_rows": 0,
                            "period_universe_mismatch_rows": 0,
                            "rankic_verdict": "weak_or_negative_across_most_profiles_horizons",
                            "best_profile": "segment_concentration_inverse",
                            "best_profile_mean_rankic_range": "0.0022..0.0038",
                            "best_profile_spread_range": "-0.12%..0.17%",
                            "decision": "do_not_enter_bounded_wfa_or_v19_train_selection"
                        },
                        "decision": "stop_fina_mainbz_main_business_source_after_p310_economics_failed"
                    },
                    {
                        "candidate": "tushare:futures_price_chain",
                        "status": "stopped_after_p310_component_economics_failed",
                        "smoke_source": "futures_price_chain",
                        "smoke_endpoint": "POST /api/v1/quant/data/tushare/permission-smoke",
                        "schema_contract_endpoint": "GET /api/v1/quant/data/futures-price-chain/schema-contract",
                        "readiness_endpoint": "GET /api/v1/quant/data/futures-price-chain/readiness-audit",
                        "diagnostics_endpoint": "POST /api/v1/quant/alpha-sources/diagnostics/report",
                        "latest_diagnostics_report_id": "exp-4d38dadc-8a09-4158-8b0c-9894004e3714",
                        "official_docs": [
                            "https://tushare.pro/wctapi/documents/138.md",
                            "https://tushare.pro/wctapi/documents/139.md",
                            "https://tushare.pro/wctapi/documents/140.md"
                        ],
                        "raw_endpoints": [
                            {
                                "api": "fut_daily",
                                "semantics": "daily futures OHLC/settlement/volume/open-interest",
                                "native_available_at_candidate": "trade_date_after_market_close",
                                "minimum_points": 2000
                            },
                            {
                                "api": "fut_wsr",
                                "semantics": "warehouse receipt daily inventory changes",
                                "native_available_at_candidate": "trade_date_after_market_close",
                                "minimum_points": 2000
                            },
                            {
                                "api": "fut_holding",
                                "semantics": "daily broker volume/long/short holding ranking",
                                "native_available_at_candidate": "trade_date_after_market_close",
                                "minimum_points": 2000
                            }
                        ],
                        "pit_policy": "trade_date may be used only after the futures market publication point; intraday stock decisions must use previous available futures trade_date unless a source_published_at audit proves earlier availability",
                        "mapping_gate": "must design product-to-industry/stock exposure mapping before factor backfill; no static hindsight mapping may revise prior samples",
                        "production_smoke": {
                            "as_of": "2026-06-21",
                            "trade_date": "20181113",
                            "fut_daily_rows": 5,
                            "fut_wsr_rows": 5,
                            "fut_holding_rows": 5,
                            "status": "available"
                        },
                        "full_history_summary": {
                            "raw_rows": 28375979,
                            "mapped_products": 84,
                            "excluded_products": 10,
                            "missing_products": 0,
                            "factor_rows": 2770856,
                            "factor_start": "2014-04-03",
                            "factor_end": "2026-06-18",
                            "raw_mapping_exclusion_pit_violations": 0
                        },
                        "diagnostics_summary": {
                            "combo_report_id": "exp-e0e525c6-612d-4b12-b11e-91088dbc8150",
                            "component_report_id": "exp-4d38dadc-8a09-4158-8b0c-9894004e3714",
                            "coverage_pit_market_scope": "green",
                            "research_economic_admission": "blocked_by_p310_economics",
                            "component_passed_horizon_count": 0,
                            "decision": "do_not_enter_bounded_wfa_or_v19_train_selection"
                        },
                        "decision": "stop_futures_price_chain_after_p310_component_economics_failed"
                    }
                ],
                "current_tables": ["market_stock_main_business", "market_futures_daily", "market_futures_warehouse_receipt", "market_futures_holding_rank", "market_futures_product_exposure_mapping_pit"],
                "schema_status": "completed",
                "client_status": "read_only_and_sync_client_completed",
                "sync_status": "full_history_raw_sync_completed",
                "coverage_status": "coverage_pit_mapping_exclusion_green",
                "p310_status": "completed_failed_economics",
                "pit_required": true,
                "available_at_policy": "source_publication_or_disclosure_date_required_before_effective_period; futures trade_date is usable only after source publication/market close",
                "admission_decision": "stopped_after_p310_component_economics_failed",
                "blocked_reason": "data_pit_coverage_mapping_green_but_combo_and_component_p310_economics_failed; no_full_period_sign_flip_no_same_family_weight_rescue_no_oos_reverse_tuning",
                "next_step": "do_not_expand_same_family_shift_to_p320_new_source_admission"
            },
            {
                "source_id": "equity_pledge_pressure",
                "source_family": "shareholder_financing_pressure_and_governance_risk",
                "economic_hypothesis": "股权质押压力可能刻画控股股东融资约束、治理风险和潜在被动减持压力；若能用公告日 PIT 化并覆盖足够广的股票池，可能提供与价格成交、事件后收益、期货价格链较低相关的横截面信息。",
                "candidate_raw_sources": [
                    "tushare:pledge_stat",
                    "tushare:pledge_detail"
                ],
                "source_discovery_evidence": [
                    {
                        "candidate": "tushare:pledge_stat",
                        "status": "production_permission_smoke_passed",
                        "official_doc": "https://tushare.pro/wctapi/documents/110.md",
                        "official_semantics": "stock_equity_pledge_stat_snapshot",
                        "observed_fields_from_doc": ["ts_code", "end_date", "pledge_count", "unrest_pledge", "rest_pledge", "total_share", "pledge_ratio"],
                        "production_smoke": {
                            "as_of": "2026-06-23",
                            "scope": "symbol_probe",
                            "sample_symbol_count": 3,
                            "status": "available"
                        },
                        "native_available_at_candidate": "not_native_end_date_is_measurement_date",
                        "decision": "do_not_use_pledge_stat_alone_until_available_at_policy_is_joined_or_conservatively_derived"
                    },
                    {
                        "candidate": "tushare:pledge_detail",
                        "status": "production_permission_smoke_passed",
                        "official_doc": "https://tushare.pro/wctapi/documents/111.md",
                        "official_semantics": "stock_equity_pledge_detail_events",
                        "observed_fields_from_doc": ["ts_code", "ann_date", "holder_name", "pledge_amount", "start_date", "end_date", "is_release", "release_date", "pledgor", "holding_amount", "pledged_amount", "p_total_ratio", "h_total_ratio", "is_buyback"],
                        "production_smoke": {
                            "as_of": "2026-06-23",
                            "scope": "announcement_date_range_probe",
                            "status": "available"
                        },
                        "native_available_at_candidate": "ann_date",
                        "decision": "schema_contract_ready_but_full_history_coverage_and_pit_audit_required_before_factor_design"
                    }
                ],
                "current_tables": ["market_stock_pledge_stat", "market_stock_pledge_detail"],
                "schema_status": "completed",
                "client_status": "read_only_and_sync_client_completed",
                "sync_status": "full_history_raw_sync_completed",
                "coverage_status": "coverage_pit_green",
                "p310_status": "completed_failed_economics",
                "pit_required": true,
                "available_at_policy": "pledge_detail.ann_date is the native available_at candidate; pledge_stat.end_date is only a snapshot measurement date and must be joined to detail announcements or shifted conservatively before any feature use",
                "diagnostics_summary": {
                    "data_pit_coverage": "green_after_2015_coverage_cliff_check",
                    "latest_report_id": "exp-0dc8dfd6-c43b-4c01-a42b-99339fc5ef25",
                    "mean_rankic_20_45_60_120": [0.00353, 0.00505, 0.00580, 0.00895],
                    "high_minus_low_spread": [-0.00127, -0.00340, -0.00394, -0.00296],
                    "passed_horizon_count": 0,
                    "decision": "do_not_enter_bounded_wfa_or_v19_train_selection"
                },
                "admission_decision": "stopped_after_p310_economics_failed",
                "blocked_reason": "coverage_and_pit_passed_but_low_pledge_ratio_atom_failed_rankic_group_spread_and_monotonicity; no_same_family_event_or_sign_flip_rescue",
                "next_step": "do_not_expand_same_family_shift_to_p322_new_source_inventory"
            },
            {
                "source_id": "shareholder_structure",
                "source_family": "ownership_structure_and_governance_breadth",
                "economic_hypothesis": "股东户数和交易事件可刻画筹码扩散、低 fanout 持有人变化与治理压力，但当前 low-fanout 表达在 v19 control 上未能转化为可交易净收益。",
                "candidate_raw_sources": [
                    "tushare:stk_holdernumber",
                    "tushare:stk_holdertrade",
                    "tushare:top10_holders",
                    "tushare:top10_floatholders"
                ],
                "current_tables": [
                    "market_stock_holder_number",
                    "market_stock_holder_trade",
                    "market_stock_top10_holders",
                    "market_stock_top10_float_holders"
                ],
                "schema_status": "completed",
                "client_status": "read_only_and_sync_client_completed",
                "sync_status": "full_history_low_fanout_raw_sync_completed",
                "coverage_status": "strict_low_fanout_coverage_pit_green_top10_high_fanout_not_admitted",
                "p310_status": "completed_passed_single_factor_diagnostics",
                "wfa_status": "completed_skipped_all_windows_by_train_robustness_and_cost_capacity_gate",
                "pit_required": true,
                "available_at_policy": "holder_number requires available_at >= end_date and holder_num > 0; holder_trade requires ann_date/native available_at and ratio/interval quality gates; top10 raw cannot bypass separate high-fanout coverage admission",
                "admission_decision": "stopped_after_bounded_wfa_train_robustness_failed",
                "diagnostics_summary": {
                    "p310_rankic_5_10_20d": [0.0187, 0.0285, 0.0361],
                    "p310_monotonicity": 0.75,
                    "wfa_experiment_id": "exp-39d6d820-0073-4a22-889f-e9eb67962785",
                    "wfa_windows": 13,
                    "skipped_windows": 13,
                    "stitched_oos": false,
                    "control_train_annual_return": -0.0677,
                    "sleeve_train_annual_return_5_10_15pct": [-0.0764, -0.0691, -0.0668],
                    "decision": "do_not_enter_v19_train_selection_or_expand_same_family_sleeve"
                },
                "blocked_reason": "raw_pit_and_p310_single_factor_passed_but_bounded_sleeve_wfa_failed_train_robustness_and_cost_capacity_gate",
                "next_step": "do_not_expand_same_family_shift_to_p322_new_source_inventory"
            },
            {
                "source_id": "equity_incentive_execution_quality",
                "source_family": "equity_incentive_and_employee_stock_plan_execution",
                "economic_hypothesis": "股权激励、员工持股与执行进度可能代表治理层对未来经营兑现的约束和信号，但必须使用公告可得日与执行窗口，不能用事后完成状态回填。",
                "candidate_raw_sources": [
                    "tushare:stk_rewards_rejected_semantic_mismatch",
                    "tushare:stk_reward_rejected_invalid_endpoint",
                    "source_discovery_required"
                ],
                "source_discovery_evidence": [
                    {
                        "candidate": "tushare:stk_rewards",
                        "status": "rejected_semantic_mismatch",
                        "official_semantics": "management_compensation_and_shareholding",
                        "observed_fields": ["ts_code", "ann_date", "end_date", "name", "title", "reward", "hold_vol"],
                        "missing_required_execution_fields": [
                            "plan_id",
                            "grant_date",
                            "grant_price_or_exercise_price",
                            "vesting_or_unlock_schedule",
                            "participant_scope",
                            "execution_progress",
                            "cancellation_or_adjustment_events"
                        ],
                        "decision": "do_not_build_schema_or_factor_from_stk_rewards_for_equity_incentive_execution_quality"
                    },
                    {
                        "candidate": "tushare:stk_reward",
                        "status": "rejected_invalid_endpoint",
                        "decision": "do_not_retry_without_official_endpoint_evidence"
                    }
                ],
                "current_tables": [],
                "schema_status": "blocked_until_valid_source_identified",
                "client_status": "do_not_add_stk_rewards_client_for_equity_incentive",
                "sync_status": "missing",
                "coverage_status": "not_started",
                "p310_status": "not_started",
                "pit_required": true,
                "available_at_policy": "announcement_or_disclosure_date_required_before_event_effective_date",
                "admission_decision": "blocked_no_valid_equity_incentive_source",
                "blocked_reason": "stk_rewards_is_management_compensation_shareholding_not_equity_incentive_execution_and_stk_reward_is_invalid",
                "next_step": "search_regulatory_disclosure_or_licensed_vendor_source_for_equity_incentive_employee_stock_plan_execution"
            },
            {
                "source_id": "broad_analyst_revision",
                "source_family": "broad_base_analyst_expectation_revision",
                "economic_hypothesis": "更 broad-base 的业绩预告/快报/披露日历修正可以刻画一致预期边际变化，但必须避免退化为稀疏公告后收益曲线 overlay。",
                "candidate_raw_sources": [
                    "tushare:forecast_stopped_sparse_revision_bundle",
                    "tushare:express_stopped_sparse_revision_bundle",
                    "tushare:disclosure_date_stopped_sparse_revision_bundle",
                    "tushare:report_rc"
                ],
                "source_discovery_evidence": [
                    {
                        "candidate": "tushare:forecast/express/disclosure_date",
                        "status": "stopped_after_full_history_audit_sparse_revision_semantics",
                        "audit_endpoint": "GET /api/v1/quant/data/broad-analyst-revision/audit",
                        "available_at_policy": {
                            "forecast": "available_at equals ann_date; available_at can be before end_date because forecasts may be published before period end",
                            "express": "available_at equals ann_date",
                            "disclosure_date": "available_at equals max(ann_date, actual_date, modify_date); pre_date is not true availability"
                        },
                        "audit_summary": {
                            "union_symbols": 4074,
                            "union_symbol_coverage_ratio": 0.5650,
                            "forecast_symbols": 1682,
                            "forecast_symbol_coverage_ratio": 0.2333,
                            "forecast_symbol_periods": 27681,
                            "multi_announcement_symbol_periods": 1825,
                            "multi_announcement_symbol_period_ratio": 0.0659,
                            "revision_event_symbols": 832,
                            "revision_event_symbol_coverage_ratio": 0.1154,
                            "available_at_rule_violations": 0,
                            "decision": "do_not_enter_p310_wfa_or_v19_train_selection"
                        },
                        "decision": "stop_current_forecast_express_disclosure_bundle_search_replacement_revision_source"
                    },
                    {
                        "candidate": "tushare:report_rc",
                        "status": "blocked_current_api_unknown_source_after_production_smoke",
                        "official_doc": "https://tushare.pro/wctapi/documents/292.md",
                        "official_semantics": "sell_side_research_report_earnings_forecast_daily_since_2010",
                        "observed_fields_from_doc": ["ts_code", "report_date", "report_title", "report_type", "classify", "org_name", "author_name", "quarter", "op_rt", "op_pr", "tp", "np", "eps", "pe", "rd", "roe", "ev_ebitda", "rating", "max_price", "min_price", "imp_dg", "create_time"],
                        "native_available_at_candidate": "report_date",
                        "permission_note": "120 points can trial 10 requests/day; formal permission requires 8000 points according to official doc",
                        "smoke_source": "report_rc",
                        "smoke_endpoint": "POST /api/v1/quant/data/tushare/permission-smoke",
                        "production_smoke": {
                            "as_of": "2026-06-21",
                            "request": {"sources": ["report_rc"], "start_date": "20260401", "end_date": "20260621", "limit": 5},
                            "status": "error",
                            "error_code": "40101",
                            "error": "未知的数据源"
                        },
                        "decision": "do_not_build_schema_sync_factor_or_p310_from_report_rc_until_the_callable_api_name_or_permission_path_is_verified"
                    }
                ],
                "current_tables": [
                    "market_stock_forecast",
                    "market_stock_express",
                    "market_stock_disclosure_date"
                ],
                "schema_status": "current_event_bundle_present_report_rc_blocked_current_api_unknown_source",
                "client_status": "report_rc_read_only_smoke_available_but_current_api_returns_unknown_source",
                "sync_status": "current_event_bundle_bounded_sync_available_report_rc_blocked_not_synced",
                "coverage_status": "current_event_bundle_full_history_audit_completed_report_rc_blocked_current_api_unknown_source",
                "p310_status": "not_started",
                "pit_required": true,
                "available_at_policy": "ann_date_or_latest_required_disclosure_date_as_available_at",
                "admission_decision": "blocked_report_rc_current_api_unknown_source_after_permission_smoke",
                "blocked_reason": "current_forecast_express_disclosure_bundle_stopped_and_report_rc_production_smoke_returned_tushare_40101_unknown_data_source",
                "guardrail": "must_be_broad_base_revision_not_sparse_event_post_return_overlay",
                "next_step": "search_licensed_or_alternative_broad_pit_expectation_revision_source; do_not_shift_back_to_stopped_futures_price_chain_proxy"
            }
        ]
    });
    if let Some(candidates) = admission
        .get_mut("candidates")
        .and_then(|candidates| candidates.as_array_mut())
    {
        candidates.push(phase7_exchange_announcement_order_capacity_candidate());
        candidates.push(phase7_multi_vendor_analyst_revision_candidate());
    }
    if let Some(readiness) = futures_price_chain_readiness {
        apply_p319_futures_price_chain_readiness(&mut admission, readiness);
    }
    admission
}

pub(crate) fn apply_p320_equity_pledge_readiness(admission: &mut Value, readiness: &Value) {
    let Some(candidates) = admission
        .get_mut("candidates")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    let decision = readiness
        .get("decision")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let admission_decision = decision
        .get("admission_decision")
        .and_then(Value::as_str)
        .unwrap_or("schema_available_at_contract_ready_for_review");
    let sync_status = decision
        .get("sync_status")
        .and_then(Value::as_str)
        .unwrap_or("missing");
    let schema_status = decision
        .get("schema_status")
        .and_then(Value::as_str)
        .unwrap_or("schema_contract_ready_review_required");
    let next_step = decision
        .get("next_step")
        .and_then(Value::as_str)
        .unwrap_or("review_apply_equity_pledge_schema_then_bounded_sync_plan");
    let p310_status = decision
        .get("p310_status")
        .and_then(Value::as_str)
        .unwrap_or("blocked_until_coverage_readiness_passes");

    for source in candidates.iter_mut() {
        let Some("equity_pledge_pressure") =
            source.get("source_id").and_then(|value| value.as_str())
        else {
            continue;
        };
        if let Value::Object(object) = source {
            let source_is_stopped_by_research = object
                .get("admission_decision")
                .and_then(Value::as_str)
                .map(|decision| decision.starts_with("stopped_"))
                .unwrap_or(false)
                || object
                    .get("p310_status")
                    .and_then(Value::as_str)
                    .map(|status| status == "completed_failed_economics")
                    .unwrap_or(false);

            object.insert("equity_pledge_readiness".to_string(), readiness.clone());
            if source_is_stopped_by_research {
                object.insert("latest_raw_readiness".to_string(), readiness.clone());
                continue;
            }

            object.insert(
                "current_tables".to_string(),
                json!(["market_stock_pledge_stat", "market_stock_pledge_detail"]),
            );
            object.insert("schema_status".to_string(), json!(schema_status));
            object.insert("sync_status".to_string(), json!(sync_status));
            object.insert(
                "coverage_status".to_string(),
                json!(match admission_decision {
                    "coverage_readiness_audit_required_before_p310" =>
                        "raw_pit_ready_coverage_audit_not_started",
                    "raw_pit_failed" => "blocked_by_raw_pit_violations",
                    "bounded_sync_required_before_coverage_audit" => "not_started",
                    "apply_schema_before_sync" => "blocked_until_schema_applied",
                    _ => "not_started",
                }),
            );
            object.insert("p310_status".to_string(), json!(p310_status));
            object.insert("admission_decision".to_string(), json!(admission_decision));
            object.insert(
                "blocked_reason".to_string(),
                json!(match admission_decision {
                    "coverage_readiness_audit_required_before_p310" =>
                        "equity_pledge_raw_pit_passed_but_year_symbol_ann_date_coverage_duplicate_and_sync_attempt_audit_not_done",
                    "raw_pit_failed" =>
                        "equity_pledge_raw_rows_have_available_at_future_leak_or_schema_pit_violation",
                    "bounded_sync_required_before_coverage_audit" =>
                        "equity_pledge_schema_exists_but_no_raw_rows_synced",
                    "apply_schema_before_sync" =>
                        "equity_pledge_schema_contract_ready_but_database_schema_missing_or_invalid",
                    _ =>
                        "equity_pledge_candidate_waiting_for_schema_available_at_bounded_sync_and_p310",
                }),
            );
            object.insert("next_step".to_string(), json!(next_step));
        }
    }
}

pub(crate) fn phase7_new_alpha_candidate_sources_with_market_status(
    market_stats: &BTreeMap<String, Phase7MarketLevelSourceAudit>,
    market_sync_tasks: &BTreeMap<String, Phase7MarketLevelSyncAudit>,
    today: NaiveDate,
) -> Vec<Value> {
    let mut sources = phase7_new_alpha_candidate_sources();
    for source in sources.iter_mut() {
        let Some(source_name) = source.get("source").and_then(|value| value.as_str()) else {
            continue;
        };
        let Some(stats) = market_stats.get(source_name) else {
            continue;
        };
        let readiness = phase7_market_level_source_readiness(stats);
        let sync_start = phase7_p315_sync_start_date(stats, today);
        let sync_dataset = phase7_market_level_sync_dataset(source_name);
        let last_sync = market_sync_tasks.get(source_name);
        let zero_row_sync_covers_gap =
            phase7_market_level_zero_row_sync_covers_gap(last_sync, sync_start, today);
        let effective_readiness =
            if readiness == "market_level_stale_needs_sync" && zero_row_sync_covers_gap {
                "market_level_upstream_zero_rows_unavailable"
            } else {
                readiness
            };
        let freshness_gate = if effective_readiness == "market_level_ready_for_regime_feature" {
            "passed"
        } else {
            "failed"
        };

        if let Value::Object(object) = source {
            object.insert("readiness".to_string(), json!(effective_readiness));
            object.insert(
                "market_data_status".to_string(),
                json!({
                    "data_rows": stats.data_rows,
                    "min_trade_date": phase7_date_json(stats.min_trade_date),
                    "latest_trade_date": phase7_date_json(stats.latest_trade_date),
                    "open_day_lag": stats.open_day_lag,
                    "freshness_max_open_day_lag": 2,
                    "freshness_gate": freshness_gate,
                }),
            );
            if let Some(last_sync) = last_sync {
                object.insert(
                    "last_sync_task".to_string(),
                    json!({
                        "task_id": last_sync.task_id,
                        "task_type": last_sync.task_type,
                        "start_date": phase7_date_json(last_sync.start_date),
                        "end_date": phase7_date_json(last_sync.end_date),
                        "status": last_sync.status,
                        "total_count": last_sync.total_count,
                        "success_count": last_sync.success_count,
                        "failed_count": last_sync.failed_count,
                        "error_message": last_sync.error_message,
                        "completed_at": phase7_datetime_json(last_sync.completed_at),
                    }),
                );
            }
            if effective_readiness == "market_level_upstream_zero_rows_unavailable" {
                object.insert(
                    "sync_remediation".to_string(),
                    json!({
                        "status": "not_retriable_until_upstream_resolved",
                        "reason": "latest stale-gap sync completed successfully but returned zero rows; treat this source as live-unavailable until upstream endpoint, permission, fields, or replacement source is fixed",
                    }),
                );
            }
            if let Some(dataset) = sync_dataset {
                object.insert(
                    "sync_task_payload".to_string(),
                    json!({
                        "dataset": dataset,
                        "source": "tushare",
                        "start_date": sync_start.format("%Y%m%d").to_string(),
                        "end_date": today.format("%Y%m%d").to_string(),
                        "background": true,
                        "reason": "p315_market_level_regime_source_freshness"
                    }),
                );
            }
        }
    }
    sources
}

pub(crate) fn phase7_new_alpha_candidate_sources_with_block_trade_status(
    mut sources: Vec<Value>,
    stats: &Phase7BlockTradeSourceAudit,
) -> Vec<Value> {
    for source in sources.iter_mut() {
        let Some("block_trade_supply_demand") =
            source.get("source").and_then(|value| value.as_str())
        else {
            continue;
        };
        let readiness = phase7_block_trade_readiness(stats);
        if let Value::Object(object) = source {
            let final_readiness = if readiness == "raw_source_pit_failed" {
                readiness
            } else {
                "stopped_after_p310_economics_weak"
            };
            object.insert("readiness".to_string(), json!(final_readiness));
            object.insert("raw_source_readiness".to_string(), json!(readiness));
            object.insert(
                "raw_source_status".to_string(),
                json!({
                    "data_rows": stats.data_rows,
                    "symbols": stats.symbols,
                    "covered_trade_days": stats.covered_trade_days,
                    "open_days_in_range": stats.open_days_in_range,
                    "open_day_coverage_ratio": phase7_ratio(stats.covered_trade_days, stats.open_days_in_range),
                    "min_trade_date": phase7_date_json(stats.min_trade_date),
                    "latest_trade_date": phase7_date_json(stats.latest_trade_date),
                    "min_available_at": phase7_date_json(stats.min_available_at),
                    "latest_available_at": phase7_date_json(stats.latest_available_at),
                    "pit_violation_rows": stats.pit_violation_rows,
                }),
            );
            object.insert(
                "next_step".to_string(),
                json!(match final_readiness {
                    "stopped_after_p310_economics_weak" => {
                        "do_not_expand_same_family_shift_to_p320_new_source_admission"
                    }
                    "raw_source_pit_failed" => "repair_available_at_before_any_diagnostics",
                    _ => "do_not_expand_same_family_shift_to_p320_new_source_admission",
                }),
            );
        }
    }
    sources
}

pub(crate) fn phase7_new_alpha_candidate_sources_with_equity_pledge_status(
    mut sources: Vec<Value>,
    readiness: Option<&Value>,
) -> Vec<Value> {
    let Some(readiness) = readiness else {
        return sources;
    };
    let decision = readiness
        .get("decision")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let admission_decision = decision
        .get("admission_decision")
        .and_then(Value::as_str)
        .unwrap_or("schema_available_at_contract_ready_for_review");
    let readiness_label = equity_pledge_readiness_label(admission_decision);
    let next_step = decision
        .get("next_step")
        .and_then(Value::as_str)
        .unwrap_or("review_apply_equity_pledge_schema_then_bounded_sync_plan");

    for source in sources.iter_mut() {
        let Some("equity_pledge_pressure") = source.get("source").and_then(|value| value.as_str())
        else {
            continue;
        };
        if let Value::Object(object) = source {
            object.insert("readiness".to_string(), json!(readiness_label));
            object.insert("next_step".to_string(), json!(next_step));
            object.insert(
                "current_tables".to_string(),
                json!(["market_stock_pledge_stat", "market_stock_pledge_detail"]),
            );
            object.insert("raw_source_status".to_string(), readiness.clone());
            object.insert(
                "why_not_trainable_now".to_string(),
                json!("股权质押源仍停在 schema/raw/coverage 准入层；只有全历史 coverage/readiness/PIT 与 P3.10A-D 通过后才允许进入 WFA/v19"),
            );
        }
    }
    sources
}

pub(crate) fn phase7_new_alpha_candidate_sources_with_industry_membership_status(
    mut sources: Vec<Value>,
    stats: &Phase7IndustryMembershipSourceAudit,
) -> Vec<Value> {
    for source in sources.iter_mut() {
        let Some("industry_prosperity_proxy") =
            source.get("source").and_then(|value| value.as_str())
        else {
            continue;
        };
        let readiness = phase7_industry_membership_readiness(stats);
        if let Value::Object(object) = source {
            object.insert("readiness".to_string(), json!(readiness));
            object.insert(
                "raw_source_status".to_string(),
                json!({
                    "table": "market_stock_industry_membership_pit",
                    "data_rows": stats.data_rows,
                    "symbols": stats.symbols,
                    "index_codes": stats.index_codes,
                    "current_active_stock_symbols": stats.current_active_stock_symbols,
                    "current_covered_stock_symbols": stats.current_covered_stock_symbols,
                    "current_missing_stock_symbols": stats.current_active_stock_symbols.saturating_sub(stats.current_covered_stock_symbols),
                    "current_stock_coverage_ratio": phase7_ratio(stats.current_covered_stock_symbols, stats.current_active_stock_symbols),
                    "min_in_date": phase7_date_json(stats.min_in_date),
                    "latest_in_date": phase7_date_json(stats.latest_in_date),
                    "min_out_date": phase7_date_json(stats.min_out_date),
                    "latest_out_date": phase7_date_json(stats.latest_out_date),
                    "min_available_at": phase7_date_json(stats.min_available_at),
                    "latest_available_at": phase7_date_json(stats.latest_available_at),
                    "pit_violation_rows": stats.pit_violation_rows,
                    "invalid_interval_rows": stats.invalid_interval_rows,
                    "duplicate_key_rows": stats.duplicate_key_rows,
                }),
            );
            object.insert(
                "next_step".to_string(),
                json!(match readiness {
                    "industry_membership_raw_source_ready_for_coverage_audit" => {
                        "run_full_history_coverage_and_pit_membership_snapshot_audit"
                    }
                    "industry_membership_raw_source_pit_failed" => {
                        "repair_industry_membership_intervals_before_factor_design"
                    }
                    "industry_membership_current_coverage_undercovered" => {
                        "run_full_l1_membership_sync_or_repair_missing_symbols"
                    }
                    _ => "run_bounded_industry_membership_sync",
                }),
            );
        }
    }
    sources
}

pub(crate) fn phase7_industry_membership_snapshot_readiness(
    expected_symbol_days: i64,
    covered_symbol_days: i64,
    _missing_symbol_days: i64,
    multi_membership_symbol_days: i64,
    pit_violation_rows: i64,
    invalid_interval_rows: i64,
    duplicate_key_rows: i64,
    missing_exit_available_at_rows: i64,
) -> &'static str {
    if expected_symbol_days <= 0 {
        return "snapshot_no_universe_symbol_days";
    }
    if pit_violation_rows > 0
        || invalid_interval_rows > 0
        || duplicate_key_rows > 0
        || missing_exit_available_at_rows > 0
    {
        return "snapshot_raw_source_pit_failed";
    }
    if multi_membership_symbol_days > 0 {
        return "snapshot_multi_membership_blocked";
    }
    if phase7_ratio(covered_symbol_days, expected_symbol_days).unwrap_or(0.0) < 0.995 {
        return "snapshot_coverage_gaps_need_review";
    }
    "snapshot_ready_for_p310_diagnostics"
}

pub(crate) fn phase7_industry_membership_audit_limit(limit: Option<i64>) -> i64 {
    limit.unwrap_or(20).clamp(1, 100)
}

pub(crate) fn phase7_industry_membership_market_scope_eligible(
    coverage_ratio: Option<f64>,
    multi_membership_symbol_days: i64,
) -> bool {
    multi_membership_symbol_days == 0 && coverage_ratio.unwrap_or(0.0) >= 0.995
}

pub(crate) fn phase7_industry_membership_snapshot_summary_sql() -> &'static str {
    r#"
    WITH params AS (
        SELECT $1::date AS start_date, $2::date AS end_date
    ),
    days AS (
        SELECT cal.trade_date
        FROM market_trade_calendar cal
        JOIN params ON cal.trade_date BETWEEN params.start_date AND params.end_date
        WHERE cal.exchange = 'SSE'
          AND cal.is_open
    ),
    universe AS (
        SELECT days.trade_date, stock.symbol
        FROM days
        JOIN market_stock stock
          ON COALESCE(stock.market, '') <> ''
         AND (stock.list_date IS NULL OR stock.list_date <= days.trade_date)
         AND (stock.delist_date IS NULL OR stock.delist_date > days.trade_date)
    ),
    membership_counts AS (
        SELECT days.trade_date,
               membership.symbol,
               COUNT(DISTINCT membership.index_code)::bigint AS active_index_count
        FROM days
        JOIN market_stock_industry_membership_pit membership
          ON membership.classification_source = CASE
                 WHEN days.trade_date < DATE '2021-12-13' THEN 'SW2014'
                 ELSE 'SW2021'
             END
         AND membership.industry_level = 'L1'
         AND membership.available_at <= days.trade_date
         AND membership.in_date <= days.trade_date
         AND (
             membership.exit_available_at IS NULL
             OR membership.exit_available_at > days.trade_date
         )
        GROUP BY days.trade_date, membership.symbol
    ),
    joined AS (
        SELECT universe.trade_date,
               universe.symbol,
               COALESCE(membership_counts.active_index_count, 0) AS active_index_count
        FROM universe
        LEFT JOIN membership_counts
          ON membership_counts.trade_date = universe.trade_date
         AND membership_counts.symbol = universe.symbol
    ),
    raw_stats AS (
        SELECT COUNT(*) FILTER (WHERE available_at < in_date)::bigint AS pit_violation_rows,
               COUNT(*) FILTER (WHERE out_date IS NOT NULL AND out_date < in_date)::bigint
                   AS invalid_interval_rows,
               COUNT(*) FILTER (WHERE out_date IS NOT NULL AND exit_available_at IS NULL)::bigint
                   AS missing_exit_available_at_rows
        FROM market_stock_industry_membership_pit
        WHERE classification_source IN ('SW2014', 'SW2021')
          AND industry_level = 'L1'
    ),
    duplicate_keys AS (
        SELECT COALESCE(SUM(row_count - 1), 0)::bigint AS duplicate_key_rows
        FROM (
            SELECT classification_source, index_code, symbol, in_date, COUNT(*)::bigint AS row_count
            FROM market_stock_industry_membership_pit
            WHERE classification_source IN ('SW2014', 'SW2021')
              AND industry_level = 'L1'
            GROUP BY classification_source, index_code, symbol, in_date
            HAVING COUNT(*) > 1
        ) duplicate_groups
    )
    SELECT COUNT(DISTINCT joined.trade_date)::bigint AS trade_days,
           COUNT(*)::bigint AS expected_symbol_days,
           COUNT(*) FILTER (WHERE joined.active_index_count = 1)::bigint AS covered_symbol_days,
           COUNT(*) FILTER (WHERE joined.active_index_count = 0)::bigint AS missing_symbol_days,
           COUNT(*) FILTER (WHERE joined.active_index_count > 1)::bigint
               AS multi_membership_symbol_days,
           MIN(joined.trade_date) AS min_trade_date,
           MAX(joined.trade_date) AS max_trade_date,
           raw_stats.pit_violation_rows,
           raw_stats.invalid_interval_rows,
           duplicate_keys.duplicate_key_rows,
           raw_stats.missing_exit_available_at_rows
    FROM joined, raw_stats, duplicate_keys
    GROUP BY raw_stats.pit_violation_rows,
             raw_stats.invalid_interval_rows,
             duplicate_keys.duplicate_key_rows,
             raw_stats.missing_exit_available_at_rows
    "#
}

pub(crate) fn phase7_industry_membership_year_breakdown_sql() -> &'static str {
    r#"
    WITH params AS (
        SELECT $1::date AS start_date, $2::date AS end_date
    ),
    days AS (
        SELECT cal.trade_date
        FROM market_trade_calendar cal
        JOIN params ON cal.trade_date BETWEEN params.start_date AND params.end_date
        WHERE cal.exchange = 'SSE'
          AND cal.is_open
    ),
    universe AS (
        SELECT days.trade_date, stock.symbol
        FROM days
        JOIN market_stock stock
          ON COALESCE(stock.market, '') <> ''
         AND (stock.list_date IS NULL OR stock.list_date <= days.trade_date)
         AND (stock.delist_date IS NULL OR stock.delist_date > days.trade_date)
    ),
    membership_counts AS (
        SELECT days.trade_date,
               membership.symbol,
               COUNT(DISTINCT membership.index_code)::bigint AS active_index_count
        FROM days
        JOIN market_stock_industry_membership_pit membership
          ON membership.classification_source = CASE
                 WHEN days.trade_date < DATE '2021-12-13' THEN 'SW2014'
                 ELSE 'SW2021'
             END
         AND membership.industry_level = 'L1'
         AND membership.available_at <= days.trade_date
         AND membership.in_date <= days.trade_date
         AND (
             membership.exit_available_at IS NULL
             OR membership.exit_available_at > days.trade_date
         )
        GROUP BY days.trade_date, membership.symbol
    ),
    joined AS (
        SELECT universe.trade_date,
               universe.symbol,
               COALESCE(membership_counts.active_index_count, 0) AS active_index_count
        FROM universe
        LEFT JOIN membership_counts
          ON membership_counts.trade_date = universe.trade_date
         AND membership_counts.symbol = universe.symbol
    )
    SELECT DATE_TRUNC('year', joined.trade_date)::date AS period_start,
           COUNT(*)::bigint AS expected_symbol_days,
           COUNT(*) FILTER (WHERE joined.active_index_count = 1)::bigint AS covered_symbol_days,
           COUNT(*) FILTER (WHERE joined.active_index_count = 0)::bigint AS missing_symbol_days,
           COUNT(*) FILTER (WHERE joined.active_index_count > 1)::bigint
               AS multi_membership_symbol_days,
           (COUNT(*) FILTER (WHERE joined.active_index_count = 1))::double precision
               / NULLIF(COUNT(*), 0)::double precision AS coverage_ratio
    FROM joined
    GROUP BY DATE_TRUNC('year', joined.trade_date)::date
    ORDER BY period_start
    "#
}

pub(crate) fn phase7_industry_membership_market_breakdown_sql() -> &'static str {
    r#"
    WITH params AS (
        SELECT $1::date AS start_date, $2::date AS end_date
    ),
    days AS (
        SELECT cal.trade_date
        FROM market_trade_calendar cal
        JOIN params ON cal.trade_date BETWEEN params.start_date AND params.end_date
        WHERE cal.exchange = 'SSE'
          AND cal.is_open
    ),
    universe AS (
        SELECT days.trade_date,
               stock.symbol,
               COALESCE(stock.market, '') AS market
        FROM days
        JOIN market_stock stock
          ON COALESCE(stock.market, '') <> ''
         AND (stock.list_date IS NULL OR stock.list_date <= days.trade_date)
         AND (stock.delist_date IS NULL OR stock.delist_date > days.trade_date)
    ),
    membership_counts AS (
        SELECT days.trade_date,
               membership.symbol,
               COUNT(DISTINCT membership.index_code)::bigint AS active_index_count
        FROM days
        JOIN market_stock_industry_membership_pit membership
          ON membership.classification_source = CASE
                 WHEN days.trade_date < DATE '2021-12-13' THEN 'SW2014'
                 ELSE 'SW2021'
             END
         AND membership.industry_level = 'L1'
         AND membership.available_at <= days.trade_date
         AND membership.in_date <= days.trade_date
         AND (
             membership.exit_available_at IS NULL
             OR membership.exit_available_at > days.trade_date
         )
        GROUP BY days.trade_date, membership.symbol
    ),
    joined AS (
        SELECT universe.trade_date,
               universe.symbol,
               universe.market,
               COALESCE(membership_counts.active_index_count, 0) AS active_index_count
        FROM universe
        LEFT JOIN membership_counts
          ON membership_counts.trade_date = universe.trade_date
         AND membership_counts.symbol = universe.symbol
    )
    SELECT joined.market,
           COUNT(*)::bigint AS expected_symbol_days,
           COUNT(*) FILTER (WHERE joined.active_index_count = 1)::bigint AS covered_symbol_days,
           COUNT(*) FILTER (WHERE joined.active_index_count = 0)::bigint AS missing_symbol_days,
           COUNT(*) FILTER (WHERE joined.active_index_count > 1)::bigint
               AS multi_membership_symbol_days,
           (COUNT(*) FILTER (WHERE joined.active_index_count = 1))::double precision
               / NULLIF(COUNT(*), 0)::double precision AS coverage_ratio
    FROM joined
    GROUP BY joined.market
    ORDER BY missing_symbol_days DESC, joined.market
    "#
}

pub(crate) fn phase7_optional_source_json(
    source: &str,
    table: &str,
    table_exists: bool,
    stats: Option<&(i64, Option<NaiveDate>, Option<NaiveDate>, i64)>,
    attempted_symbols: i64,
    attempted_zero_row_symbols: i64,
    reference_symbols: i64,
    next_feature: &str,
) -> Value {
    let (rows, min_date, max_date, symbols) = stats.copied().unwrap_or((0, None, None, 0));
    let available_or_attempted_symbols = symbols.max(attempted_symbols);
    let readiness = phase7_optional_source_readiness(
        table_exists,
        rows,
        available_or_attempted_symbols,
        reference_symbols,
    );
    let mut value = phase7_coverage_json(
        source.to_string(),
        rows,
        min_date,
        max_date,
        available_or_attempted_symbols,
        reference_symbols,
    );
    if let Value::Object(ref mut object) = value {
        object.insert("source".to_string(), json!(source));
        object.insert("table".to_string(), json!(table));
        object.insert("table_exists".to_string(), json!(table_exists));
        object.insert("data_row_symbols".to_string(), json!(symbols));
        object.insert("attempted_symbols".to_string(), json!(attempted_symbols));
        object.insert(
            "attempted_zero_row_symbols".to_string(),
            json!(attempted_zero_row_symbols),
        );
        object.insert(
            "coverage_basis".to_string(),
            json!("data_rows_or_successful_attempts"),
        );
        object.insert("feature_readiness".to_string(), json!(readiness));
        object.insert("next_feature".to_string(), json!(next_feature));
        object.insert(
            "next_step".to_string(),
            json!(phase7_optional_source_next_step(readiness)),
        );
    }
    value
}
