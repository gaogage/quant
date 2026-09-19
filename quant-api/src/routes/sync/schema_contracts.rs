//! phase7 数据源 schema 契约与 readiness/审计决策：期货链/股权质押/融资融细则/
//! 股东结构/公告订单容量/分析师修正等源的 PIT 契约声明与准入判定。
use super::*;
use serde_json::{json, Value};

pub(crate) fn phase7_futures_price_chain_schema_contract() -> Value {
    json!({
        "source_id": "futures_price_chain",
        "stage": "P3.19J",
        "mode": "read_only_schema_mapping_pit_contract",
        "source_status": "permission_smoke_available_schema_contract_defined",
        "admission_decision": "schema_mapping_available_at_audit_required_before_sync",
        "raw_sources": [
            {
                "api": "fut_daily",
                "doc": "https://tushare.pro/wctapi/documents/138.md",
                "semantics": "daily futures OHLC settlement volume and open-interest",
                "native_time_key": "trade_date"
            },
            {
                "api": "fut_wsr",
                "doc": "https://tushare.pro/wctapi/documents/140.md",
                "semantics": "warehouse receipt inventory and daily inventory change",
                "native_time_key": "trade_date"
            },
            {
                "api": "fut_holding",
                "doc": "https://tushare.pro/wctapi/documents/139.md",
                "semantics": "broker-level daily volume long and short holding ranking",
                "native_time_key": "trade_date"
            }
        ],
        "raw_tables": [
            {
                "table": "market_futures_daily",
                "natural_key": ["ts_code", "trade_date"],
                "required_time_fields": ["trade_date", "available_at", "source_published_at"],
                "required_value_fields": ["close", "settle", "vol", "amount", "oi", "oi_chg"],
                "pit_rule": "available_at must be >= trade_date and downstream features must filter available_at <= stock_trade_date"
            },
            {
                "table": "market_futures_warehouse_receipt",
                "natural_key": ["trade_date", "symbol", "exchange", "warehouse"],
                "required_time_fields": ["trade_date", "available_at", "source_published_at"],
                "required_value_fields": ["pre_vol", "vol", "vol_chg", "unit"],
                "pit_rule": "warehouse inventory changes are usable only after source publication"
            },
            {
                "table": "market_futures_holding_rank",
                "natural_key": ["trade_date", "symbol", "exchange", "broker"],
                "required_time_fields": ["trade_date", "available_at", "source_published_at"],
                "required_value_fields": ["vol", "vol_chg", "long_hld", "long_chg", "short_hld", "short_chg"],
                "pit_rule": "broker position changes are usable only after source publication"
            }
        ],
        "mapping_tables": [
            {
                "table": "market_futures_product_exposure_mapping_pit",
                "natural_key": ["product_symbol", "exposure_type", "exposure_code", "valid_from", "mapping_version"],
                "required_fields": ["product_symbol", "exposure_type", "exposure_code", "direction", "weight", "valid_from", "valid_to", "available_at", "source", "mapping_version"],
                "allowed_exposure_types": ["sw_industry", "stock_symbol"],
                "pit_rule": "mapping.available_at <= stock_trade_date; mapping rows must be versioned and must not be derived from future stock returns or future factor performance",
                "preferred_first_pass": "product-to-sw-industry mapping joined to market_stock_industry_membership_pit; direct stock_symbol mapping requires stronger evidence"
            },
            {
                "table": "market_futures_product_exclusion_gate_pit",
                "natural_key": ["product_symbol", "gate_scope", "valid_from", "gate_version"],
                "required_fields": ["product_symbol", "gate_scope", "reason_code", "valid_from", "valid_to", "available_at", "source", "gate_version", "evidence"],
                "allowed_reason_codes": ["financial_index_future", "interest_rate_future", "non_industry_derivative", "ambiguous_product_symbol", "insufficient_industry_evidence"],
                "pit_rule": "exclusion gates are admission controls: excluded products must not be forced into product-to-industry mappings or downstream factors",
                "preferred_first_pass": "pre-register non-industry derivatives such as equity index and treasury bond futures as excluded before product-to-SW-industry review"
            }
        ],
        "pit_policy": {
            "native_available_at_candidate": "trade_date_after_market_close",
            "source_published_at_required": true,
            "intraday_stock_decision_rule": "use_previous_available_futures_trade_date_until_source_published_at_is_audited",
            "prohibited": [
                "using same-day futures close or warehouse data in an intraday stock rebalance before publication",
                "using static hindsight product-to-stock mapping without available_at",
                "backfilling exposure weights from later performance or later industry reclassification"
            ]
        },
        "coverage_audit_required": {
            "full_history_range": "2014-01-01_to_latest_complete_trade_date",
            "required_breakdowns": ["year", "endpoint", "product_symbol", "exchange", "mapped_industry", "stock_market_scope"],
            "minimum_before_p310": "coverage/readiness green or explicitly gated market/date scope"
        },
        "promotion_gate": {
            "schema_status": "not_created",
            "sync_status": "not_started",
            "coverage_status": "not_started",
            "p310_status": "not_started",
            "wfa_status": "blocked_until_p310_passes",
            "v19_train_selection": "blocked"
        },
        "ddl_path": "sql/phase7_futures_price_chain_source.sql",
        "next_step": "create_schema_then_run_bounded_full_history_sync_and_coverage_readiness_audit"
    })
}

pub(crate) fn phase7_equity_pledge_schema_contract() -> Value {
    json!({
        "audit_version": "p3.20b-equity-pledge-pressure-schema-contract-v1",
        "source_id": "equity_pledge_pressure",
        "stage": "P3.20B",
        "status": "permission_smoke_passed_schema_review_required",
        "mode": "read_only_schema_available_at_contract",
        "ddl_path": "sql/phase7_equity_pledge_source.sql",
        "raw_sources": [
            {
                "api": "pledge_stat",
                "official_doc": "https://tushare.pro/wctapi/documents/110.md",
                "semantics": "stock_equity_pledge_stat_snapshot",
                "native_available_at_candidate": "not_native_end_date_is_measurement_date",
                "required_fields": ["ts_code", "end_date", "pledge_count", "unrest_pledge", "rest_pledge", "total_share", "pledge_ratio"],
                "minimum_points": 2000
            },
            {
                "api": "pledge_detail",
                "official_doc": "https://tushare.pro/wctapi/documents/111.md",
                "semantics": "stock_equity_pledge_detail_events",
                "native_available_at_candidate": "ann_date",
                "required_fields": ["ts_code", "ann_date", "holder_name", "pledge_amount", "start_date", "end_date", "is_release", "release_date", "pledgor", "holding_amount", "pledged_amount", "p_total_ratio", "h_total_ratio", "is_buyback"],
                "minimum_points": 2000
            }
        ],
        "tables": [
            {
                "table": "market_stock_pledge_stat",
                "natural_key": ["symbol", "end_date"],
                "required_fields": ["symbol", "end_date", "pledge_count", "unrest_pledge", "rest_pledge", "total_share", "pledge_ratio", "available_at", "source_published_at", "raw_payload", "source", "data_version_id"],
                "pit_rule": "available_at must be >= end_date. Default sync may only use conservative end_date+1day unless a source_published_at audit proves earlier availability.",
                "training_gate": "stat snapshots cannot enter factor construction until joined to detail announcements or audited with a conservative availability lag."
            },
            {
                "table": "market_stock_pledge_detail",
                "natural_key": ["symbol", "ann_date", "source_row_hash"],
                "required_fields": ["symbol", "ann_date", "holder_name", "pledge_amount", "pledge_start_date", "pledge_end_date", "is_release", "release_date", "pledgor", "holding_amount", "pledged_amount", "p_total_ratio", "h_total_ratio", "is_buyback", "available_at", "raw_payload", "source", "data_version_id", "source_row_hash"],
                "pit_rule": "available_at equals ann_date. Downstream features must use available_at <= stock_trade_date and must preserve multiple pledge rows on the same announcement date.",
                "nullable_source_fields": ["pledge_start_date", "pledge_end_date", "release_date", "holding_amount", "pledged_amount", "p_total_ratio", "h_total_ratio"],
                "duplicate_policy": "preserve source rows by source_row_hash; do not collapse same-day multiple pledges before audit. Nullable source fields must remain nullable and must not be promoted into the primary key."
            }
        ],
        "available_at_policy": {
            "pledge_detail": "ann_date is native available_at candidate and must be persisted as available_at.",
            "pledge_stat": "end_date is a measurement date, not disclosure availability; use only after conservative lag or detail-derived audit.",
            "intraday_trading": "without verified source publication timestamps, same-day pledge updates are not available to intraday rebalancing."
        },
        "coverage_audit_required": [
            "year_symbol_ann_date_breakdown",
            "detail_available_at_null_or_future_leak_count",
            "detail_release_before_start_count",
            "detail_ratio_out_of_range_count",
            "stat_pledge_ratio_out_of_range_count",
            "stat_end_date_coverage_and_lag_policy",
            "symbol_breadth_vs_main_chinext_non_st",
            "duplicate_source_row_hash_count",
            "sync_attempt_success_failure_breakdown"
        ],
        "promotion_gate": {
            "schema_apply": "blocked_until_manual_review",
            "bounded_sync": "blocked_until_schema_applied",
            "factor_builder": "blocked_until_full_history_coverage_pit_passes",
            "p310_status": "blocked_until_factor_builder_and_readiness_pass",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        }
    })
}

pub(crate) fn phase7_margin_detail_schema_contract() -> Value {
    json!({
        "audit_version": "p3.22c-margin-detail-schema-contract-v1",
        "source_id": "margin_detail_leverage_crowding",
        "stage": "P3.22C",
        "status": "permission_smoke_passed_schema_created_sync_smoke_passed",
        "mode": "read_only_schema_available_at_quality_contract",
        "ddl_path": "sql/phase7_margin_detail_source.sql",
        "raw_sources": [
            {
                "api": "margin_detail",
                "official_doc": "https://tushare.pro/document/2?doc_id=59",
                "semantics": "security_level_margin_financing_and_short_selling_detail",
                "native_time_key": "trade_date",
                "official_publication_hint": "previous trading day data updates around next trading day 08:30",
                "required_fields": ["trade_date", "ts_code", "name", "rzye", "rqye", "rzmre", "rqyl", "rzche", "rqchl", "rqmcl", "rzrqye"]
            }
        ],
        "tables": [
            {
                "table": "market_stock_margin_detail",
                "natural_key": ["symbol", "trade_date"],
                "required_time_fields": ["trade_date", "available_at", "source_published_at"],
                "required_value_fields": ["rzye", "rqye", "rzmre", "rqyl", "rzche", "rqchl", "rqmcl", "rzrqye"],
                "pit_rule": "available_at must be the next open trading date after trade_date; downstream features and intraday trading must additionally require source_published_at <= decision timestamp",
                "quality_rule": "rzche and rqchl may be negative vendor adjustment fields and must be preserved; balances/core activity fields must be nonnegative",
                "raw_landing_policy": "raw sync preserves vendor rows and reports anomalies; admission gates decide factor eligibility"
            }
        ],
        "available_at_policy": {
            "default": "conservative next-session availability",
            "source_published_at": "next open trading day 08:30 China time when native timestamp is unavailable",
            "intraday_trading": "same-day margin_detail must not be used for intraday rebalance; only rows with source_published_at <= decision timestamp are usable"
        },
        "coverage_audit_required": [
            "year_market_symbol_trade_date_breakdown",
            "open_trade_day_coverage_ratio",
            "available_at_pit_violation_rows",
            "missing_source_published_at_rows",
            "rzche_rqchl_negative_adjustment_breakdown",
            "core_nonnegative_field_violation_rows",
            "sync_attempt_success_failure_breakdown",
            "correlation_vs_moneyflow_liquidity_price_volume"
        ],
        "promotion_gate": {
            "schema_status": "created_or_review_required",
            "bounded_sync": "allowed_only_as_raw_admission_sync",
            "factor_builder": "blocked_until_full_history_coverage_pit_quality_and_correlation_pass",
            "p310_status": "blocked_until_coverage_pit_quality_and_correlation_readiness_pass",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        },
        "next_step": "run_full_history_bounded_sync_by_year_or_quarter_then_rerun_coverage_pit_quality_correlation_audit"
    })
}

pub(crate) fn phase7_shareholder_structure_schema_contract() -> Value {
    json!({
        "audit_version": "p3.21b-shareholder-structure-schema-contract-v1",
        "source_id": "shareholder_structure",
        "stage": "P3.21B",
        "status": "permission_smoke_passed_schema_review_required",
        "mode": "read_only_schema_available_at_contract",
        "ddl_path": "sql/phase7_shareholder_structure_source.sql",
        "raw_sources": [
            {
                "api": "stk_holdernumber",
                "semantics": "stock_shareholder_count_snapshot",
                "native_available_at_candidate": "ann_date",
                "measurement_date": "end_date",
                "required_fields": ["ts_code", "ann_date", "end_date", "holder_num"],
                "minimum_points": 2000
            },
            {
                "api": "top10_holders",
                "semantics": "top10_shareholder_concentration_snapshot",
                "native_available_at_candidate": "ann_date",
                "measurement_date": "end_date",
                "required_fields": ["ts_code", "ann_date", "end_date", "holder_name", "hold_amount", "hold_ratio", "hold_change", "holder_type"],
                "minimum_points": 2000
            },
            {
                "api": "top10_floatholders",
                "semantics": "top10_float_shareholder_concentration_snapshot",
                "native_available_at_candidate": "ann_date",
                "measurement_date": "end_date",
                "required_fields": ["ts_code", "ann_date", "end_date", "holder_name", "hold_amount", "hold_ratio", "hold_float_ratio", "hold_change", "holder_type"],
                "minimum_points": 2000
            },
            {
                "api": "stk_holdertrade",
                "semantics": "major_holder_or_insider_increase_decrease_event",
                "native_available_at_candidate": "ann_date",
                "required_fields": ["ts_code", "ann_date", "holder_name", "holder_type", "in_de", "change_vol", "change_ratio", "after_share", "after_ratio", "avg_price", "begin_date", "close_date"],
                "minimum_points": 2000
            }
        ],
        "tables": [
            {
                "table": "market_stock_holder_number",
                "natural_key": ["symbol", "ann_date", "end_date"],
                "required_fields": ["symbol", "ann_date", "end_date", "holder_num", "available_at", "raw_payload", "source", "data_version_id", "source_row_hash"],
                "pit_rule": "available_at equals ann_date; ann_date before end_date is retained as a raw source anomaly and blocks admission until repaired, excluded, or gated."
            },
            {
                "table": "market_stock_top10_holders",
                "natural_key": ["symbol", "ann_date", "end_date", "source_row_hash"],
                "required_fields": ["symbol", "ann_date", "end_date", "holder_name", "hold_amount", "hold_ratio", "hold_float_ratio", "hold_change", "holder_type", "available_at", "raw_payload", "source", "data_version_id", "source_row_hash"],
                "pit_rule": "available_at equals ann_date; downstream features must filter available_at <= stock_trade_date and exclude or gate ann_date before end_date anomalies."
            },
            {
                "table": "market_stock_top10_float_holders",
                "natural_key": ["symbol", "ann_date", "end_date", "source_row_hash"],
                "required_fields": ["symbol", "ann_date", "end_date", "holder_name", "hold_amount", "hold_ratio", "hold_float_ratio", "hold_change", "holder_type", "available_at", "raw_payload", "source", "data_version_id", "source_row_hash"],
                "pit_rule": "available_at equals ann_date; do not infer float-holder concentration before disclosure, and treat ann_date before end_date as a blocking raw anomaly."
            },
            {
                "table": "market_stock_holder_trade",
                "natural_key": ["symbol", "ann_date", "source_row_hash"],
                "required_fields": ["symbol", "ann_date", "holder_name", "holder_type", "in_de", "change_vol", "change_ratio", "after_share", "after_ratio", "avg_price", "total_share", "begin_date", "close_date", "available_at", "raw_payload", "source", "data_version_id", "source_row_hash"],
                "pit_rule": "available_at equals ann_date; begin_date/close_date are event attributes and must not move availability earlier."
            }
        ],
        "available_at_policy": {
            "default": "available_at equals native ann_date for all four shareholder_structure raw tables; end_date is a measurement period only.",
            "period_snapshot_rule": "holdernumber/top10/top10_float rows with ann_date before end_date land as raw source anomalies, but block factor/P3.10/WFA until repaired, excluded, or gated.",
            "raw_landing": "raw tables accept native source rows for auditability; admission gates, not insert constraints, decide whether rows can feed factor work.",
            "intraday_trading": "without verified source publication timestamps, same-day shareholder disclosures are not available to intraday rebalancing."
        },
        "coverage_audit_required": [
            "year_source_symbol_ann_date_breakdown",
            "symbol_breadth_vs_main_chinext_non_st",
            "ann_date_before_end_date_count",
            "available_at_before_ann_date_count",
            "holder_ratio_out_of_range_count",
            "holder_trade_interval_violation_count",
            "duplicate_source_row_hash_count",
            "sync_attempt_success_failure_breakdown"
        ],
        "promotion_gate": {
            "schema_apply": "blocked_until_manual_review",
            "bounded_sync": "blocked_until_schema_applied",
            "factor_builder": "blocked_until_full_history_coverage_pit_passes",
            "p310_status": "blocked_until_factor_builder_and_readiness_pass",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        }
    })
}

pub(crate) fn phase7_exchange_announcement_order_capacity_schema_contract() -> Value {
    json!({
        "audit_version": "p3.24a-exchange-announcement-order-capacity-source-contract-v1",
        "source_id": "exchange_announcement_order_capacity_text",
        "stage": "P3.24A",
        "status": "schema_contract_defined_pdf_detail_admission_required_before_manual_review",
        "mode": "read_only_source_admission_contract_no_sync",
        "ddl_path": "sql/phase7_exchange_announcement_order_capacity_source.sql",
        "raw_sources": [
            {
                "vendor": "akshare",
                "upstream": "cninfo",
                "vendor_endpoint": "stock_zh_a_disclosure_report_cninfo",
                "source_semantics": "CNInfo-listed company disclosure reports by symbol, market, category and date range",
                "request_key": "symbol+market+category+start_date+end_date",
                "native_available_at_candidate": "公告时间",
                "source_published_at_candidate": "announcement detail page publish timestamp if available; otherwise date-only announcement time",
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
                "known_risks": [
                    "symbol fanout can be expensive; bounded history sync must be batched by symbol and date range",
                    "some categories may return empty or parser errors and need category-level reliability audit before full sync",
                    "date-only announcement time is insufficient for same-day intraday decisions"
                ],
                "admission_gate": "permission_history_category_smoke_and_available_at_text_parse_audit_required_before_schema_apply"
            },
            {
                "vendor": "cninfo",
                "vendor_endpoint": "announcement_detail_page",
                "source_semantics": "official disclosure detail page referenced by announcement link",
                "request_key": "announcement_id+org_id+stock_code",
                "native_available_at_candidate": "detail page disclosure timestamp when present",
                "required_from_link": ["announcementId", "orgId", "stockCode", "announcementTime"],
                "admission_gate": "official_page_fetch_text_hash_and_publication_timestamp_audit_required"
            },
            {
                "vendor": "licensed_vendor",
                "vendor_endpoint": "broad_base_announcement_text_feed",
                "source_semantics": "licensed exchange/CNInfo disclosure feed with timestamped announcement text",
                "native_available_at_candidate": "vendor source publication timestamp",
                "admission_gate": "permission_and_schema_contract_required_if_public_feed_is_not_reliable_enough"
            }
        ],
        "event_taxonomy": [
            {
                "event_type": "order_or_contract_signed",
                "positive_evidence": ["中标", "签订合同", "重大合同", "订单", "框架协议"],
                "required_evidence": ["counterparty", "contract_amount_or_capacity", "time_window_or_delivery_schedule"]
            },
            {
                "event_type": "capacity_expansion_or_commissioning",
                "positive_evidence": ["扩产", "产能", "投产", "试生产", "达产"],
                "required_evidence": ["project_name", "capacity_or_capex", "expected_start_or_completion_date"]
            },
            {
                "event_type": "product_price_adjustment",
                "positive_evidence": ["价格调整", "上调", "下调", "产品价格"],
                "required_evidence": ["product", "price_direction", "effective_date"]
            },
            {
                "event_type": "major_supply_or_customer_agreement",
                "positive_evidence": ["供货协议", "采购协议", "长期协议", "战略合作"],
                "required_evidence": ["customer_or_supplier", "covered_product", "duration_or_amount"]
            }
        ],
        "tables": [
            {
                "table": "market_exchange_announcement_text_raw",
                "natural_key": ["vendor", "vendor_endpoint", "announcement_id", "symbol"],
                "required_fields": [
                    "vendor",
                    "vendor_endpoint",
                    "request_key",
                    "symbol",
                    "symbol_name",
                    "announcement_id",
                    "org_id",
                    "announcement_category",
                    "announcement_title",
                    "announcement_time",
                    "source_published_at",
                    "source_published_at_quality",
                    "available_at",
                    "announcement_url",
                    "pdf_final_url",
                    "text_content",
                    "text_hash",
                    "text_hash_algorithm",
                    "timestamp_candidates",
                    "pdf_metadata_keys",
                    "raw_payload",
                    "raw_payload_hash",
                    "parser_used",
                    "parser_version",
                    "event_type",
                    "evidence_spans",
                    "ingested_at",
                    "data_version_id"
                ],
                "pit_rule": "available_at must be no earlier than source_published_at/date-only announcement_time. If source_published_at_quality is date_only_next_session, downstream trading must promote availability to the next open session. Intraday trading must additionally require a trusted timestamp with source_published_at <= decision timestamp.",
                "text_evidence_rule": "event_type is invalid without evidence_spans that quote the exact announcement text supporting order, capacity, contract, price-adjustment or commissioning semantics. PDF-only rows without evidence_spans remain blocked.",
                "raw_landing_policy": "preserve full raw payload, source URL, pdf_final_url, text hash and parser identity; category/parser errors and scanned_pdf_ocr_required cases must be audited, not silently dropped."
            }
        ],
        "available_at_policy": {
            "preferred": "use official source_published_at timestamp from the announcement detail/feed when available",
            "date_only_policy": "if only announcement date is available, set available_at to next open session for trading decisions until source_published_at timestamp is audited",
            "intraday_trading": "same-day announcement events are forbidden for intraday rebalance unless source_published_at <= decision timestamp is proven",
            "weekend_or_holiday_publications": "bounded sync must scan calendar days and map date-only announcements to the next open session rather than dropping non-trading-day disclosures"
        },
        "pdf_admission": {
            "runtime_default_python": "~/.local/share/quant-pdf-audit/venv/bin/python",
            "pdf_parser_readiness_endpoint": "GET /api/v1/quant/data/exchange-announcement-order-capacity/pdf-parser-readiness",
            "pdf_detail_audit_endpoint": "POST /api/v1/quant/data/exchange-announcement-order-capacity/pdf-detail-audit",
            "required_audit_version": "p3.24e-exchange-announcement-order-capacity-pdf-detail-audit-v1",
            "required_detail_fields": [
                "pdf_final_url",
                "parser_used",
                "text_hash",
                "text_hash_algorithm",
                "timestamp_candidates",
                "source_published_at",
                "source_published_at_quality",
                "evidence_spans",
                "pdf_metadata_keys"
            ],
            "next_session_policy": "date_only_next_session is admissible only for next-open-session daily PIT usage; same-session and intraday usage remain blocked",
            "ocr_policy": "scanned_pdf_ocr_required stays blocked until a separate OCR runtime and audit path are reviewed"
        },
        "manual_schema_review": {
            "endpoint": "GET /api/v1/quant/data/exchange-announcement-order-capacity/manual-schema-review",
            "audit_version": "p3.24f-exchange-announcement-order-capacity-manual-schema-review-v1",
            "mode": "read_only_schema_contract_ddl_review_no_apply",
            "decision_scope": "ddl_contract_review_only_bounded_sync_design_allowed_next",
            "requires": [
                "pdf_detail_audit_passed_manual_schema_review_allowed_next",
                "date_only_next_session_policy_encoded",
                "raw_failure_samples_preserved",
                "evidence_span_jsonb_preserved"
            ]
        },
        "sync_plan": {
            "endpoint": "GET /api/v1/quant/data/exchange-announcement-order-capacity/sync-plan",
            "audit_version": "p3.24g-exchange-announcement-order-capacity-bounded-sync-plan-v1",
            "mode": "read_only_plan_only_calendar_day_symbol_category_sync_design",
            "sync_endpoint_status": "disabled_plan_only_design_until_operator_schema_apply_and_small_batch_review"
        },
        "coverage_audit_required": [
            "year_market_symbol_category_breakdown",
            "calendar_day_and_open_day_publication_coverage",
            "announcement_id_duplicate_or_missing_count",
            "source_published_at_null_or_date_only_count",
            "text_fetch_success_rate",
            "text_hash_duplicate_count",
            "category_parser_error_breakdown",
            "event_taxonomy_precision_manual_sample",
            "evidence_span_presence_rate",
            "correlation_vs_existing_event_moneyflow_liquidity_price_volume_quality_sources"
        ],
        "promotion_gate": {
            "permission_smoke": "required",
            "history_category_replay": "required_before_schema_apply",
            "schema_apply": "blocked_until_pdf_detail_audit_passes_and_manual_review",
            "bounded_sync": "blocked",
            "factor_builder": "blocked",
            "p310_status": "blocked",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        },
        "guardrails": [
            "do not build generic post-announcement return overlay from this source",
            "do not use full-period keyword sign or horizon mining",
            "do not merge announcement categories before category-level coverage and parser quality pass",
            "do not use date-only same-day announcements for intraday or same-session decisions",
            "do not enter P3.10 until coverage/PIT/text-evidence/correlation audits pass"
        ],
        "next_step": "use permission_smoke_detail_audit_pdf_parser_readiness_and_pdf_detail_audit_evidence_to_finish_manual_schema_review_before_any_bounded_sync_design"
    })
}

pub(crate) fn phase7_exchange_announcement_order_capacity_next_source_admission_plan() -> Value {
    let blocked_promotion_gate = json!({
        "schema_apply": "blocked",
        "bounded_sync": "blocked",
        "factor_builder": "blocked",
        "p310_status": "blocked",
        "bounded_wfa": "blocked",
        "v19_train_selection": "blocked"
    });

    json!({
        "audit_version": "p3.25a-exchange-announcement-order-capacity-next-source-admission-plan-v1",
        "source_id": "exchange_announcement_order_capacity_text",
        "stage": "P3.25A",
        "mode": "read_only_next_source_admission_plan_no_sync_no_factor_no_p310",
        "current_pilot_decision": "stopped_current_4_symbol_daily_operation_pilot_after_clean_target_recall_failed",
        "current_pilot_evidence": {
            "scope": "002459.SZ,600519.SH,300750.SZ,000001.SZ / 日常经营 / 2023-10-01..2025-03-31",
            "raw_rows": 249,
            "target_event_rows_before_title_risk_gate": 119,
            "taxonomy_blocked_target_event_rows": 113,
            "admissible_target_event_rows": 6,
            "manual_review_min_clean_target_samples": 50,
            "manual_review_sample_shortfall": 44,
            "pit_violation_rows": 0,
            "duplicate_key_rows": 0,
            "blocking_failed_attempts": 0,
            "raw_sync_quality": "passed_after_high_density_landing_cap_fix",
            "interpretation": "engineering raw landing and PIT controls are healthy; blocker is insufficient clean target-event recall and noisy vendor category semantics"
        },
        "stop_rules": [
            "do_not_continue_month_or_quarter_raw_sync_for_current_4_symbol_daily_operation_pilot",
            "do_not_relax_title_taxonomy_or_manual_precision_gate_to_create_false_ready_status",
            "do_not_enter_factor_builder_p310_bounded_wfa_or_v19_from_current_pilot",
            "do_not_use_oos_feedback_full_period_sign_flip_or_horizon_mining_to_rescue_this_source"
        ],
        "candidate_routes": [
            {
                "priority": 1,
                "route_id": "structured_order_capacity_contract_price_chain_source",
                "source_family": "licensed_or_structured_real_operations_feed",
                "economic_hypothesis": "structured order, capacity, contract, price-adjustment or commissioning data may provide cleaner operational information than noisy category-level disclosure text",
                "candidate_sources": [
                    "licensed_exchange_or_cninfo_timestamped_announcement_feed",
                    "licensed_structured_order_contract_capacity_event_feed",
                    "authorized_industry_price_capacity_order_chain_feed"
                ],
                "universe_policy": "broad_base_main_chinext_non_st_or_pre_registered_market_scope_gate; any excluded market/date/symbol scope must be declared before diagnostics",
                "required_gates": [
                    "vendor_permission_and_legal_usage_audit",
                    "raw_schema_contract_with_stable_natural_key",
                    "available_at_source_published_at_audit",
                    "full_history_bounded_sync_plan",
                    "coverage_readiness_pit_duplicate_hash_audit",
                    "manual_precision_ge_0_80_with_min_50_clean_target_samples",
                    "correlation_vs_existing_moneyflow_liquidity_price_volume_quality_event_sources"
                ],
                "stop_rule": "stop_before_factor_builder_if_coverage_pit_precision_or_correlation_gate_fails",
                "promotion_gate": blocked_promotion_gate.clone(),
                "next_step": "source_discovery_permission_schema_available_at_contract_before_any_raw_sync"
            },
            {
                "priority": 2,
                "route_id": "announcement_text_broader_universe",
                "source_family": "public_or_licensed_announcement_text_with_pre_registered_broader_universe",
                "economic_hypothesis": "if announcement text remains the source, recall must be improved by pre-registering a broader universe and category/taxonomy scope rather than extending the stopped 4-symbol pilot",
                "candidate_sources": [
                    "akshare_cninfo_disclosure_feed_with_broader_symbol_universe",
                    "official_cninfo_or_exchange_feed_with_timestamped_detail_pages",
                    "licensed_timestamped_announcement_text_feed"
                ],
                "universe_policy": "pre_register_symbols_markets_categories_and_date_range; no post-hoc symbol/category selection based on return performance",
                "required_gates": [
                    "permission_history_category_smoke",
                    "detail_text_pdf_ocr_timestamp_hash_audit",
                    "available_at_source_published_at_audit",
                    "bounded_calendar_day_symbol_category_sync_plan",
                    "coverage_readiness_pit_failed_attempt_duplicate_hash_audit",
                    "manual_precision_ge_0_80_with_min_50_clean_target_samples",
                    "taxonomy_precision_false_positive_review",
                    "correlation_vs_existing_event_moneyflow_liquidity_price_volume_quality_sources"
                ],
                "stop_rule": "stop_if_broader_pre_registered_scope_still_cannot_produce_50_clean_target_review_samples_or_precision_below_0_80",
                "promotion_gate": blocked_promotion_gate.clone(),
                "next_step": "write_pre_registered_broader_universe_plan_then_run_permission_and_available_at_smoke_only"
            }
        ],
        "promotion_gate": {
            "schema_apply": "blocked_until_new_route_permission_schema_available_at_contract_passes",
            "bounded_sync": "blocked_until_new_route_manual_schema_review_and_plan_pass",
            "factor_builder": "blocked",
            "p310_status": "blocked",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        },
        "next_required_steps": [
            "choose_route_1_structured_source_if_usable_vendor_exists_otherwise_route_2_broader_universe",
            "create_read_only_permission_schema_available_at_admission_for_selected_route",
            "keep_current_4_symbol_daily_operation_pilot_stopped"
        ]
    })
}

pub(crate) fn phase7_structured_order_capacity_price_chain_source_contract() -> Value {
    json!({
        "audit_version": "p3.25b-structured-order-capacity-contract-price-chain-source-contract-v1",
        "source_id": "structured_order_capacity_contract_price_chain_source",
        "stage": "P3.25B",
        "mode": "read_only_structured_source_admission_contract_no_sync",
        "admission_decision": "blocked_vendor_permission_and_available_at_contract_required",
        "source_priority": 1,
        "why_now": "current public CNInfo/AkShare 4-symbol daily-operation pilot is stopped for clean target-event recall and taxonomy precision; next source must prefer structured, timestamped, legally usable real-operation events",
        "candidate_source_families": [
            {
                "family": "licensed_structured_order_contract_capacity_event_feed",
                "examples": [
                    "vendor-normalized listed-company contract/order/capacity/commissioning events",
                    "licensed exchange or CNInfo feed with event tags and publication timestamps"
                ],
                "minimum_admission_state": "vendor_permission_and_schema_sample_required"
            },
            {
                "family": "authorized_industry_price_capacity_order_chain_feed",
                "examples": [
                    "industry product price adjustment feed",
                    "capacity commissioning or production schedule feed",
                    "order backlog or contract award feed with listed-company identifiers"
                ],
                "minimum_admission_state": "legal_usage_and_symbol_mapping_required"
            }
        ],
        "legal_and_vendor_gate": {
            "status": "required_before_schema_or_sync",
            "required_evidence": [
                "license_or_terms_allow_research_and_internal_trading_use",
                "redistribution_and_storage_rights_reviewed",
                "historical_access_range_confirmed",
                "api_rate_limit_and_cost_estimated",
                "vendor_field_dictionary_or_sample_payload_collected"
            ],
            "blocked_if": [
                "no_storage_rights",
                "no_historical_access",
                "no_source_publication_timestamp_or_conservative_availability_rule",
                "event_labels_are_derived_from_future_returns"
            ]
        },
        "required_time_fields": [
            "event_date",
            "source_published_at",
            "available_at",
            "ingested_at"
        ],
        "raw_schema_contract": {
            "table_candidate": "market_structured_operation_event_raw",
            "natural_key": [
                "vendor",
                "vendor_endpoint",
                "vendor_event_id",
                "symbol",
                "event_type",
                "source_published_at",
                "raw_payload_hash"
            ],
            "required_identity_fields": [
                "vendor",
                "vendor_endpoint",
                "request_key",
                "vendor_event_id",
                "symbol",
                "symbol_name",
                "event_type"
            ],
            "required_evidence_fields": [
                "source_url",
                "source_title",
                "source_document_id",
                "source_excerpt_or_structured_payload",
                "evidence_hash",
                "raw_payload",
                "raw_payload_hash",
                "parser_or_vendor_model_version"
            ],
            "pit_rule": "downstream features must filter available_at <= trade_date; intraday decisions must additionally require source_published_at <= decision_timestamp"
        },
        "event_schema": [
            {
                "event_type": "order_or_contract_signed",
                "required_fields": [
                    "counterparty",
                    "contract_amount",
                    "covered_product_or_service",
                    "delivery_or_execution_window",
                    "contract_status"
                ],
                "quality_checks": [
                    "contract_amount_nonnegative_or_null_with_reason",
                    "counterparty_not_empty",
                    "execution_window_not_before_source_published_at"
                ]
            },
            {
                "event_type": "capacity_expansion_or_commissioning",
                "required_fields": [
                    "project_name",
                    "capacity_or_capex",
                    "product_or_line",
                    "expected_start_or_completion_date",
                    "project_location"
                ],
                "quality_checks": [
                    "capacity_or_capex_nonnegative_or_null_with_reason",
                    "project_timeline_not_backfilled_from_later_reports",
                    "location_or_product_scope_reviewed"
                ]
            },
            {
                "event_type": "product_price_adjustment",
                "required_fields": [
                    "product",
                    "price_direction",
                    "effective_date",
                    "price_change_magnitude_or_bucket",
                    "scope"
                ],
                "quality_checks": [
                    "price_direction_increase_decrease_or_mixed",
                    "effective_date_available_only_after_source_published_at",
                    "scope_not_market_return_derived"
                ]
            },
            {
                "event_type": "supply_customer_agreement_or_order_backlog",
                "required_fields": [
                    "customer_or_supplier",
                    "covered_product",
                    "duration_or_amount",
                    "agreement_type",
                    "execution_status"
                ],
                "quality_checks": [
                    "customer_supplier_not_empty",
                    "duration_or_amount_not_future_filled",
                    "agreement_type_from_vendor_or_source_text_not_return_label"
                ]
            }
        ],
        "available_at_policy": {
            "daily_rule": "if source has only date-level publication, available_at must map to the next open trading session",
            "intraday_rule": "decision_timestamp must be >= source_published_at; date-only events are daily next-session only",
            "weekend_holiday_rule": "weekend or holiday publications map to the next open trading session",
            "forbidden": [
                "using event effective_date as available_at",
                "using vendor ingestion time as source publication time",
                "same-session trading from date-only publications",
                "labels or event directions inferred from future stock returns"
            ]
        },
        "coverage_audit_required": [
            "vendor_endpoint_year_month_breakdown",
            "market_scope_symbol_coverage_vs_main_chinext_non_st",
            "event_type_distribution_and_target_yield",
            "source_published_at_null_or_date_only_count",
            "available_at_pit_violation_rows",
            "duplicate_vendor_event_or_payload_hash_rows",
            "manual_precision_ge_0_80_with_min_50_clean_target_samples",
            "field_null_rate_and_range_checks_by_event_type",
            "correlation_vs_existing_moneyflow_liquidity_price_volume_quality_event_sources",
            "cost_rate_limit_and_refresh_latency_budget"
        ],
        "promotion_gate": {
            "permission_smoke": "blocked_until_vendor_candidate_selected",
            "schema_apply": "blocked",
            "bounded_sync": "blocked",
            "factor_builder": "blocked",
            "p310_status": "blocked",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        },
        "guardrails": [
            "do not proceed without legal/vendor storage and usage review",
            "do not map event_type from post-event returns or OOS performance",
            "do not merge public noisy text pilot rows into structured-source positives",
            "do not enter P3.10 before full coverage/PIT/precision/correlation gates pass"
        ],
        "next_step": "identify_vendor_or_authorized_structured_source_then_run_permission_and_sample_payload_smoke"
    })
}

pub(crate) fn phase7_structured_order_capacity_price_chain_vendor_admission_plan() -> Value {
    let blocked_promotion_gate = json!({
        "permission_smoke": "blocked_until_candidate_vendor_and_endpoint_selected",
        "schema_apply": "blocked",
        "bounded_sync": "blocked",
        "factor_builder": "blocked",
        "p310_status": "blocked",
        "bounded_wfa": "blocked",
        "v19_train_selection": "blocked"
    });

    let sample_payload_evidence_required = json!([
        "sample_request_and_response_captured_read_only",
        "vendor_field_dictionary_or_payload_schema",
        "source_published_at_or_conservative_available_at",
        "event_date_and_event_type_semantics",
        "stable_vendor_event_id_or_deterministic_natural_key",
        "raw_payload_hash",
        "symbol_mapping_evidence",
        "license_storage_and_internal_use_note"
    ]);

    json!({
        "audit_version": "p3.25c-structured-order-capacity-price-chain-vendor-admission-plan-v1",
        "source_id": "structured_order_capacity_contract_price_chain_source",
        "stage": "P3.25C",
        "mode": "read_only_vendor_source_candidate_discovery_plan_no_schema_no_sync",
        "admission_decision": "blocked_until_vendor_permission_history_available_at_and_sample_payload_smoke_pass",
        "why_now": "P3.25B defined the structured source contract; P3.25C must discover a legally usable vendor or source and prove payload/PIT semantics before any schema or sync work",
        "candidate_sources": [
            {
                "priority": 1,
                "vendor": "licensed_structured_financial_data_vendor",
                "source_name": "listed_company_order_contract_capacity_event_feed",
                "source_family": "licensed_structured_order_contract_capacity_event_feed",
                "legal_storage_use_status": "unknown_requires_terms_or_contract_review",
                "endpoint_payload_availability": "unknown_requires_permission_and_sample_payload_smoke",
                "historical_coverage_range": "unknown_requires_history_date_probe_covering_2014_to_present_or_declared_start_date",
                "source_published_at_semantics": "must_provide_publication_timestamp_or_auditable_date_level_publication",
                "symbol_mapping_requirement": "must_map_vendor_company_identifier_to_ts_code_or_exchange_symbol_with_effective_date_scope",
                "sample_payload_evidence_required": sample_payload_evidence_required.clone(),
                "stop_rule": "stop_if_event_labels_are_return_derived_or_publication_time_is_missing_without_conservative_available_at"
            },
            {
                "priority": 2,
                "vendor": "licensed_exchange_or_cninfo_metadata_vendor",
                "source_name": "timestamped_announcement_metadata_with_structured_event_tags",
                "source_family": "licensed_timestamped_disclosure_metadata_feed",
                "legal_storage_use_status": "unknown_requires_license_storage_redistribution_and_internal_trading_use_review",
                "endpoint_payload_availability": "unknown_requires_endpoint_smoke_for_event_tags_detail_url_and_payload_hash",
                "historical_coverage_range": "unknown_requires_replay_probe_by_publication_date_and_category",
                "source_published_at_semantics": "must_distinguish_source_publication_time_from_vendor_ingestion_time",
                "symbol_mapping_requirement": "must_preserve exchange_symbol and normalized ts_code mapping without current_snapshot_backfill",
                "sample_payload_evidence_required": sample_payload_evidence_required.clone(),
                "stop_rule": "stop_if_tags_reproduce_the_noisy_current_daily_operation_category_without_clean_target_precision"
            },
            {
                "priority": 3,
                "vendor": "authorized_industry_chain_data_vendor",
                "source_name": "industry_price_capacity_order_chain_feed",
                "source_family": "authorized_industry_price_capacity_order_chain_feed",
                "legal_storage_use_status": "unknown_requires_data_use_storage_and_symbol_mapping_terms",
                "endpoint_payload_availability": "unknown_requires_sample_for_product_price_capacity_order_records",
                "historical_coverage_range": "unknown_requires_product_or_company_history_probe_and_market_scope_declaration",
                "source_published_at_semantics": "must_provide_observation_publication_or_release_time_not_future_revised_series_only",
                "symbol_mapping_requirement": "must map product/industry/company exposure using pre_registered PIT mapping or exclusion gate",
                "sample_payload_evidence_required": sample_payload_evidence_required.clone(),
                "stop_rule": "stop_if_mapping_to_listed_company_or_sw_industry_requires_future_performance_or_subjective_post_hoc_weights"
            }
        ],
        "required_sample_payload_fields": [
            "vendor",
            "vendor_endpoint",
            "request_key",
            "vendor_event_id",
            "symbol_or_company_identifier",
            "event_type",
            "event_date",
            "source_published_at",
            "available_at_rule",
            "source_url_or_document_id",
            "source_title_or_structured_payload_excerpt",
            "raw_payload",
            "raw_payload_hash",
            "vendor_schema_or_model_version"
        ],
        "permission_and_history_smoke_plan": {
            "write_enabled": false,
            "db_write_enabled": false,
            "minimum_smoke": [
                "terms_or_license_storage_use_review",
                "single_endpoint_permission_probe",
                "history_date_probe_for_old_mid_recent_periods",
                "sample_payload_capture_without_persistence",
                "source_published_at_available_at_semantics_review",
                "symbol_mapping_and_market_scope_review"
            ],
            "representative_history_dates": [
                "2014-01-02",
                "2017-01-03",
                "2020-07-01",
                "2024-01-02",
                "latest_completed_trading_or_publication_date"
            ],
            "blocked_outputs": [
                "ddl_generation",
                "schema_apply",
                "bounded_raw_sync",
                "factor_backfill",
                "p310_diagnostics",
                "bounded_wfa",
                "v19_train_selection"
            ]
        },
        "stop_rules": [
            "stop_if_vendor_terms_do_not_allow_storage_research_and_internal_trading_use",
            "stop_if_vendor_cannot_provide_historical_payload_samples_with_source_published_at_or_auditable_availability",
            "stop_if_only_current_snapshot_or_forward_revised_series_is_available",
            "stop_if_event_type_or_direction_is_derived_from_future_returns",
            "stop_if_symbol_mapping_requires_post_hoc_performance_weights",
            "stop_if_sample_payload_cannot_preserve_stable_natural_key_and_raw_payload_hash",
            "stop_if_sample_precision_or_event_semantics_are_equivalent_to_the_stopped_noisy_cninfo_daily_operation_pilot"
        ],
        "promotion_gate": blocked_promotion_gate,
        "fallback_if_no_usable_vendor": "switch_to_announcement_text_broader_universe_pre_registered_plan_without_rescuing_current_4_symbol_pilot",
        "next_step": "collect_candidate_vendor_terms_endpoint_dictionary_and_read_only_sample_payload_evidence_before_any_schema_or_sync_design"
    })
}

pub(crate) fn phase7_structured_order_capacity_price_chain_source_evidence_inventory() -> Value {
    let blocked_promotion_gate = json!({
        "schema_apply": "blocked",
        "bounded_sync": "blocked",
        "factor_builder": "blocked",
        "p310_status": "blocked",
        "bounded_wfa": "blocked",
        "v19_train_selection": "blocked"
    });
    let missing_core_evidence = json!([
        "license_or_terms_allow_storage_research_and_internal_trading_use",
        "read_only_sample_payload_with_source_published_at",
        "historical_access_range_covering_2014_to_present_or_declared_start",
        "stable_vendor_event_id_or_natural_key",
        "raw_payload_hash_and_field_dictionary",
        "symbol_mapping_effective_date_scope",
        "rate_limit_cost_and_refresh_latency_budget"
    ]);

    json!({
        "audit_version": "p3.25d-structured-order-capacity-price-chain-source-evidence-inventory-v1",
        "source_id": "structured_order_capacity_contract_price_chain_source",
        "stage": "P3.25D",
        "mode": "read_only_source_evidence_inventory_no_permission_probe_no_schema_no_sync",
        "admission_decision": "blocked_no_candidate_has_complete_vendor_terms_history_payload_and_available_at_evidence",
        "permission_smoke": "blocked_until_candidate_access_configured",
        "candidate_evidence": [
            {
                "candidate_id": "cninfo_data_service",
                "vendor": "CNINFO Data Service / Shenzhen Securities Information",
                "source_url": "https://webapi.cninfo.com.cn/",
                "source_family": "licensed_timestamped_disclosure_metadata_feed",
                "observed_relevance": "official data service site advertises listed-company announcements, thematic statistics, data browser, quantitative data service, and industry-chain entry points",
                "candidate_strength": "official_channel_for_cninfo_disclosure_and_data_service",
                "admission_status": "candidate_permission_sample_smoke_required",
                "pit_risk": "public site confirms product family but not sample payload timestamp semantics or storage rights",
                "missing_required_evidence": missing_core_evidence.clone(),
                "allowed_next_action": "contact_or_configure_authorized_access_then_read_only_endpoint_dictionary_and_sample_payload_smoke"
            },
            {
                "candidate_id": "eastmoney_major_contracts_public_page",
                "vendor": "Eastmoney Data Center",
                "source_url": "https://data.eastmoney.com/zdht/",
                "source_family": "public_major_contracts_web_page",
                "observed_relevance": "public page lists major-contract fields such as stock code, contract type, contract name, contract amount, sign date and announcement date",
                "candidate_strength": "confirms_major_contract_event_taxonomy_exists_publicly",
                "admission_status": "blocked_public_web_page_not_licensed_api",
                "pit_risk": "public web page is not evidence of licensed API use, storage rights, stable payload contract, or source publication timestamp",
                "missing_required_evidence": missing_core_evidence.clone(),
                "allowed_next_action": "use_only_as_taxonomy_hint_until_choice_or_other_licensed_api_terms_and_sample_payload_are_available"
            },
            {
                "candidate_id": "cnopendata_major_contracts_dataset",
                "vendor": "CnOpenData",
                "source_url": "https://m.cnopendata.com/pages/data?module=listedcompany-basic&dataKey=listedco-zdht",
                "source_family": "licensed_or_paid_major_contracts_dataset",
                "observed_relevance": "dataset description advertises A-share listed-company major-contract fields including announcement date, sign date, contract name, contract type, amount, content and impact",
                "candidate_strength": "field_semantics_close_to_order_contract_source",
                "admission_status": "candidate_permission_sample_smoke_required",
                "pit_risk": "mobile catalog snippet does not prove API access, historical coverage, source_published_at, storage rights or raw payload stability",
                "missing_required_evidence": missing_core_evidence.clone(),
                "allowed_next_action": "request_terms_field_dictionary_history_range_and_read_only_sample_payload_before_schema_design"
            },
            {
                "candidate_id": "wind_client_api_platform",
                "vendor": "Wind",
                "source_url": "https://www.wind.com.cn/mobile/ClientApi/zh.html",
                "source_family": "licensed_financial_terminal_or_client_api",
                "observed_relevance": "official ClientApi page advertises secure and consistent access to Wind data for internal or third-party applications",
                "candidate_strength": "mature_licensed_data_platform_candidate",
                "admission_status": "candidate_catalog_and_entitlement_review_required",
                "pit_risk": "platform availability alone does not prove the needed structured operation-event endpoint, entitlement, source timestamp, or storage rights",
                "missing_required_evidence": missing_core_evidence.clone(),
                "allowed_next_action": "review_contract_entitlement_and_catalog_for_contract_capacity_price_chain_events_then_sample_payload_smoke"
            },
            {
                "candidate_id": "choice_dataservice_platform",
                "vendor": "Eastmoney Choice",
                "source_url": "https://choice.eastmoney.com/dataservice",
                "source_family": "licensed_financial_dataservice_platform",
                "observed_relevance": "Choice data-service page advertises data interface delivery across assets and macro/industry datasets into enterprise data warehouses",
                "candidate_strength": "possible_licensed_path_for_eastmoney_major_contracts_or_related_event_data",
                "admission_status": "candidate_catalog_and_entitlement_review_required",
                "pit_risk": "data-service page does not prove major-contract endpoint, payload schema, source_published_at, or allowed research/trading storage",
                "missing_required_evidence": missing_core_evidence.clone(),
                "allowed_next_action": "verify_whether_choice_entitlement_exposes_major_contracts_or_announcement_event_dataset_with_payload_samples"
            },
            {
                "candidate_id": "juyuan_gildata_platform",
                "vendor": "Gildata / Hundsun Juyuan",
                "source_url": "https://www.gildata.com/",
                "source_family": "licensed_financial_data_platform",
                "observed_relevance": "public site describes broad financial market data, applied databases and information terminal products",
                "candidate_strength": "possible_licensed_structured_event_or_announcement_dataset_provider",
                "admission_status": "candidate_catalog_and_entitlement_review_required",
                "pit_risk": "public homepage does not prove specific order/capacity/contract/price-chain endpoint or PIT timestamp semantics",
                "missing_required_evidence": missing_core_evidence.clone(),
                "allowed_next_action": "request_product_catalog_and_sample_payload_for_listed_company_operation_event_or_announcement_structuring_dataset"
            }
        ],
        "hard_stop_if_missing": [
            "vendor_terms_allowing_storage_research_internal_trading_use",
            "source_published_at_or_conservative_available_at_semantics",
            "historical_payload_samples",
            "stable_natural_key_and_raw_payload_hash",
            "symbol_mapping_with_effective_date_or_pre_registered_scope",
            "event_labels_not_derived_from_future_returns"
        ],
        "promotion_gate": blocked_promotion_gate,
        "fallback_rule": "if_no_candidate_can_supply_terms_history_payload_and_available_at_evidence_then_prepare_announcement_text_broader_universe_pre_registration_instead",
        "next_step": "select_one_candidate_with_legal_access_then_run_read_only_permission_and_sample_payload_smoke"
    })
}

pub(crate) fn phase7_structured_order_capacity_price_chain_cninfo_access_smoke_contract() -> Value {
    let blocked_promotion_gate = json!({
        "schema_apply": "blocked",
        "bounded_sync": "blocked",
        "factor_builder": "blocked",
        "p310_status": "blocked",
        "bounded_wfa": "blocked",
        "v19_train_selection": "blocked"
    });

    json!({
        "audit_version": "p3.25e-cninfo-data-service-access-smoke-contract-v1",
        "source_id": "structured_order_capacity_contract_price_chain_source",
        "stage": "P3.25E",
        "candidate_id": "cninfo_data_service",
        "mode": "read_only_cninfo_access_and_sample_payload_contract_no_network_no_schema_no_sync",
        "access_status": "not_configured_or_not_reviewed",
        "admission_decision": "blocked_cninfo_terms_credentials_endpoint_dictionary_and_sample_payload_required",
        "why_selected_first": "CNINFO Data Service is the official disclosure/data-service candidate and is closer to source publication semantics than public scraped pages",
        "selected_candidate": {
            "vendor": "CNINFO Data Service / Shenzhen Securities Information",
            "source_url": "https://webapi.cninfo.com.cn/",
            "source_family": "licensed_timestamped_disclosure_metadata_feed",
            "target_dataset_candidates": [
                "listed_company_announcements",
                "announcement_customization",
                "thematic_statistics_for_major_contracts_or_operation_events",
                "industry_chain_or_quantitative_data_service_if_contract_capacity_price_chain_fields_exist"
            ],
            "explicit_non_goals": [
                "do_not_use_public_cninfo_or_eastmoney_web_scraping_as_licensed_source",
                "do_not_import_current_4_symbol_daily_operation_pilot_rows",
                "do_not_accept_html_page_timestamp_as_source_published_at_without_payload_proof"
            ]
        },
        "required_local_evidence": [
            "CNINFO authorized account or API token configured outside source control",
            "license or terms file reviewed and stored outside repository secrets",
            "terms explicitly allow local storage for research and internal trading use",
            "endpoint dictionary identifies operation-event, major-contract, announcement metadata, or industry-chain dataset",
            "history access range covers 2014-present or declares audited start date",
            "sample request plan uses read-only calls only and persists no raw rows",
            "operator records rate limit, cost, and refresh latency budget"
        ],
        "required_sample_payload_fields": [
            "vendor",
            "vendor_endpoint",
            "request_key",
            "vendor_event_id_or_document_id",
            "symbol_or_company_identifier",
            "event_type_or_announcement_category",
            "event_date_or_announcement_date",
            "source_published_at",
            "available_at_rule",
            "source_url_or_document_url",
            "source_title_or_payload_excerpt",
            "raw_payload",
            "raw_payload_hash",
            "vendor_schema_or_model_version"
        ],
        "permission_smoke": {
            "status": "blocked_no_authorized_access_or_endpoint_dictionary",
            "network_enabled": false,
            "db_write_enabled": false,
            "allowed_after_evidence": [
                "single_endpoint_read_only_permission_probe",
                "representative_history_date_probe_2014_2017_2020_2024_latest",
                "sample_payload_hash_and_timestamp_audit",
                "symbol_mapping_effective_date_scope_review"
            ],
            "forbidden_outputs": [
                "schema_apply",
                "raw_sync",
                "factor_backfill",
                "p310_diagnostics",
                "wfa",
                "v19_train_selection"
            ]
        },
        "available_at_contract": {
            "daily_rule": "date-only CNINFO announcements or metadata must map to next open trading session until timestamp precision is proven",
            "intraday_rule": "intraday use requires source_published_at timestamp and decision_timestamp >= source_published_at",
            "forbidden": [
                "using vendor ingestion time as source_published_at",
                "using announcement effective date as available_at",
                "same-session trading from date-only publication",
                "event labels inferred from future returns"
            ]
        },
        "stop_rules": [
            "stop_if_cninfo_terms_do_not_allow_local_storage_research_and_internal_trading_use",
            "stop_if_no_endpoint_dictionary_for_operation_event_major_contract_or_announcement_metadata",
            "stop_if_history_access_cannot_cover_2014_to_present_or_declared_start_date",
            "stop_if_sample_payload_lacks_source_published_at_or_auditable_date_only_publication",
            "stop_if_payload_lacks_stable_document_or_event_id_and_raw_payload_hash",
            "stop_if_event_tags_are_equivalent_to_noisy_public_daily_operation_category_without_precision_evidence"
        ],
        "promotion_gate": blocked_promotion_gate,
        "next_step": "obtain_cninfo_terms_endpoint_dictionary_and_authorized_read_only_sample_payload_before_any_network_probe"
    })
}

pub(crate) fn phase7_structured_order_capacity_price_chain_cninfo_operator_evidence_contract(
) -> Value {
    let blocked_promotion_gate = json!({
        "permission_smoke": "blocked_until_manifest_exists_and_manual_review_passes",
        "schema_apply": "blocked",
        "bounded_sync": "blocked",
        "factor_builder": "blocked",
        "p310_status": "blocked",
        "bounded_wfa": "blocked",
        "v19_train_selection": "blocked"
    });

    json!({
        "audit_version": "p3.25f-cninfo-operator-evidence-manifest-contract-v1",
        "source_id": "structured_order_capacity_contract_price_chain_source",
        "stage": "P3.25F",
        "candidate_id": "cninfo_data_service",
        "mode": "read_only_cninfo_operator_evidence_manifest_contract_no_network_no_secret_read_no_schema_no_sync",
        "admission_decision": "blocked_until_redacted_operator_evidence_manifest_is_reviewed",
        "why_now": "P3.25E selected CNINFO as the first candidate but correctly blocks network probes until legal, endpoint, history, PIT and sample-payload evidence exists; P3.25F formalizes the external evidence manifest without storing secrets or vendor raw data in the repository",
        "runtime_actions": {
            "network_enabled": false,
            "credential_read_enabled": false,
            "db_write_enabled": false,
            "schema_apply_enabled": false,
            "raw_payload_persistence_enabled": false
        },
        "evidence_manifest_contract": {
            "manifest_env_var": "QUANT_CNINFO_EVIDENCE_MANIFEST_PATH",
            "manifest_default_location": "operator_controlled_path_outside_git_repository",
            "repository_storage_policy": "forbid_secrets_raw_payloads_and_vendor_documents_in_repo",
            "accepted_evidence_categories": [
                "terms_review_attestation",
                "credential_presence_attestation_without_secret_value",
                "endpoint_dictionary_reference",
                "history_range_attestation",
                "sample_payload_redacted_hash_evidence",
                "available_at_source_published_at_semantics_note",
                "symbol_mapping_scope_note",
                "rate_limit_cost_refresh_latency_budget"
            ],
            "required_manifest_fields": [
                "artifact_id",
                "artifact_type",
                "owner",
                "review_status",
                "reviewed_at",
                "storage_location_type",
                "content_hash",
                "redaction_status",
                "source_effective_start_date",
                "source_effective_end_date",
                "pit_relevance",
                "notes"
            ],
            "review_status_allowed_values": [
                "missing",
                "pending_review",
                "reviewed_pass",
                "reviewed_blocked"
            ],
            "minimum_pass_conditions": [
                "terms_review_attestation_reviewed_pass",
                "credential_presence_attestation_reviewed_pass_without_secret_value",
                "endpoint_dictionary_reference_reviewed_pass",
                "history_range_attestation_covers_2014_to_present_or_declared_start",
                "sample_payload_redacted_hash_evidence_contains_stable_id_source_published_at_available_at_rule_and_raw_hash",
                "symbol_mapping_scope_note_reviewed_pass",
                "rate_limit_cost_refresh_latency_budget_reviewed_pass"
            ]
        },
        "forbidden_manifest_contents": [
            "api_token_or_password",
            "raw_vendor_payload_or_full_vendor_document",
            "unredacted_license_contract",
            "cookie_session_or_authorization_header",
            "private_endpoint_secret",
            "material_non_public_information",
            "post_event_return_label_or_oos_performance_based_event_direction"
        ],
        "manual_review_required": [
            "legal_or_operator_attestation_terms_allow_local_storage_research_and_internal_trading_use",
            "endpoint_dictionary_contains_operation_event_major_contract_announcement_metadata_or_industry_chain_dataset",
            "history_range_covers_2014_to_present_or_has_pre_registered_audited_start_date",
            "sample_payload_has_source_published_at_or_auditable_date_only_publication_rule",
            "sample_payload_has_stable_document_or_event_id_and_raw_payload_hash",
            "available_at_rule_is_next_open_session_for_date_only_publications",
            "intraday_use_requires_minute_level_source_published_at",
            "symbol_mapping_has_effective_date_scope_or_pre_registered_market_scope_exclusion",
            "event_tags_are_not_equivalent_to_noisy_daily_operation_category_without_precision_evidence"
        ],
        "allowed_after_manual_review_passes": [
            "design_read_only_permission_sample_smoke_without_persisting_raw_vendor_rows",
            "run_single_endpoint_permission_probe_with_operator_supplied_credentials_outside_source_control",
            "probe_representative_history_dates_2014_2017_2020_2024_latest",
            "audit_sample_payload_hash_timestamp_available_at_and_symbol_mapping",
            "decide_schema_contract_only_after_smoke_payload_semantics_pass"
        ],
        "stop_rules": [
            "stop_if_manifest_missing_or_not_operator_reviewed",
            "stop_if_manifest_contains_secret_or_raw_vendor_payload",
            "stop_if_terms_do_not_allow_storage_research_and_internal_trading_use",
            "stop_if_endpoint_dictionary_lacks_target_operation_event_or_metadata_dataset",
            "stop_if_sample_payload_cannot_prove_source_published_at_or_conservative_available_at",
            "stop_if_history_access_is_current_snapshot_only_or_forward_revised",
            "stop_if_symbol_mapping_requires_post_hoc_performance_weights",
            "stop_if_event_tags_are_return_derived_or_oos_tuned"
        ],
        "promotion_gate": blocked_promotion_gate,
        "fallback_if_evidence_cannot_be_supplied": "return_to_p3.25d_other_candidates_or_prepare_announcement_text_broader_universe_pre_registration_without_rescuing_current_4_symbol_pilot",
        "next_step": "prepare_redacted_external_cninfo_evidence_manifest_then_manual_review_before_any_read_only_network_probe"
    })
}

pub(crate) fn phase7_structured_order_capacity_price_chain_cninfo_operator_evidence_audit_from_manifest(
    manifest_path: Option<&str>,
    manifest: Option<&Value>,
    read_error: Option<&str>,
) -> Value {
    let base = |manifest_status: &str, admission_decision: &str, permission_smoke: &str| {
        json!({
            "audit_version": "p3.25g-cninfo-operator-evidence-manifest-structure-audit-v1",
            "source_id": "structured_order_capacity_contract_price_chain_source",
            "stage": "P3.25G",
            "candidate_id": "cninfo_data_service",
            "mode": "read_only_cninfo_operator_evidence_manifest_structure_audit_no_network_no_secret_read_no_schema_no_sync",
            "manifest_env_var": "QUANT_CNINFO_EVIDENCE_MANIFEST_PATH",
            "manifest_path_configured": manifest_path
                .map(|path| !path.trim().is_empty())
                .unwrap_or(false),
            "manifest_status": manifest_status,
            "admission_decision": admission_decision,
            "runtime_actions": phase7_cninfo_operator_evidence_runtime_actions(),
            "privacy_guards": phase7_cninfo_operator_evidence_privacy_guards(),
            "promotion_gate": phase7_cninfo_operator_evidence_promotion_gate(permission_smoke)
        })
    };

    let configured_path = manifest_path.map(str::trim).filter(|path| !path.is_empty());
    if configured_path.is_none() {
        let mut response = base(
            "missing_env_var",
            "blocked_manifest_env_var_not_configured",
            "blocked_until_manifest_audit_passes",
        );
        response["audit_summary"] = json!({
            "artifact_count": 0,
            "reviewed_pass_count": 0,
            "missing_required_category_count": cninfo_operator_evidence_required_categories().len(),
            "forbidden_manifest_key_count": 0,
            "missing_required_field_count": 0,
            "redaction_failure_count": 0
        });
        response["missing_required_categories"] =
            json!(cninfo_operator_evidence_required_categories());
        response["missing_required_fields"] = json!([]);
        response["forbidden_manifest_keys"] = json!([]);
        response["next_step"] =
            json!("configure_quant_cninfo_evidence_manifest_path_outside_git_repository");
        return response;
    }

    if read_error.is_some() {
        let mut response = base(
            "manifest_file_unreadable_or_invalid_json",
            "blocked_manifest_file_unreadable_or_invalid_json",
            "blocked_until_manifest_audit_passes",
        );
        response["read_error_redacted"] = json!(true);
        response["audit_summary"] = json!({
            "artifact_count": 0,
            "reviewed_pass_count": 0,
            "missing_required_category_count": cninfo_operator_evidence_required_categories().len(),
            "forbidden_manifest_key_count": 0,
            "missing_required_field_count": 0,
            "redaction_failure_count": 0
        });
        response["missing_required_categories"] =
            json!(cninfo_operator_evidence_required_categories());
        response["missing_required_fields"] = json!([]);
        response["forbidden_manifest_keys"] = json!([]);
        response["next_step"] =
            json!("fix_external_manifest_readability_or_json_structure_without_committing_secrets");
        return response;
    }

    let Some(manifest) = manifest else {
        let mut response = base(
            "manifest_json_missing",
            "blocked_manifest_json_missing",
            "blocked_until_manifest_audit_passes",
        );
        response["audit_summary"] = json!({
            "artifact_count": 0,
            "reviewed_pass_count": 0,
            "missing_required_category_count": cninfo_operator_evidence_required_categories().len(),
            "forbidden_manifest_key_count": 0,
            "missing_required_field_count": 0,
            "redaction_failure_count": 0
        });
        response["missing_required_categories"] =
            json!(cninfo_operator_evidence_required_categories());
        response["missing_required_fields"] = json!([]);
        response["forbidden_manifest_keys"] = json!([]);
        response["next_step"] = json!("provide_external_redacted_json_manifest");
        return response;
    };

    let artifacts = manifest
        .get("artifacts")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let artifact_count = artifacts.len();
    let mut present_categories = BTreeSet::new();
    let mut missing_required_fields = BTreeSet::new();
    let mut reviewed_pass_count = 0usize;
    let mut redaction_failure_count = 0usize;

    for artifact in &artifacts {
        for field in cninfo_operator_evidence_required_fields() {
            if !cninfo_operator_evidence_string_present(artifact.get(field)) {
                missing_required_fields.insert(field.to_string());
            }
        }
        if let Some(artifact_type) = artifact.get("artifact_type").and_then(Value::as_str) {
            present_categories.insert(artifact_type.to_string());
        }
        if artifact
            .get("review_status")
            .and_then(Value::as_str)
            .map(|status| status == "reviewed_pass")
            .unwrap_or(false)
        {
            reviewed_pass_count += 1;
        }
        let redacted = artifact
            .get("redaction_status")
            .and_then(Value::as_str)
            .map(|status| status.contains("redacted") && !status.contains("unredacted"))
            .unwrap_or(false);
        if !redacted {
            redaction_failure_count += 1;
        }
    }

    let missing_required_categories: Vec<String> = cninfo_operator_evidence_required_categories()
        .into_iter()
        .filter(|category| !present_categories.contains(*category))
        .map(ToString::to_string)
        .collect();
    let mut forbidden_manifest_keys = BTreeSet::new();
    collect_cninfo_operator_forbidden_manifest_keys(manifest, &mut forbidden_manifest_keys);
    let forbidden_manifest_keys: Vec<String> = forbidden_manifest_keys.into_iter().collect();
    let missing_required_fields: Vec<String> = missing_required_fields.into_iter().collect();

    let top_level_valid = manifest
        .get("source_id")
        .and_then(Value::as_str)
        .map(|source_id| source_id == "structured_order_capacity_contract_price_chain_source")
        .unwrap_or(false)
        && manifest
            .get("candidate_id")
            .and_then(Value::as_str)
            .map(|candidate_id| candidate_id == "cninfo_data_service")
            .unwrap_or(false);
    let structure_passed = top_level_valid
        && artifact_count > 0
        && reviewed_pass_count == artifact_count
        && redaction_failure_count == 0
        && missing_required_categories.is_empty()
        && missing_required_fields.is_empty()
        && forbidden_manifest_keys.is_empty();

    let (manifest_status, admission_decision, permission_smoke) =
        if !forbidden_manifest_keys.is_empty() {
            (
                "forbidden_content_detected",
                "blocked_manifest_contains_forbidden_secret_or_raw_payload_fields",
                "blocked_until_manifest_audit_passes",
            )
        } else if structure_passed {
            (
                "structure_passed",
                "passed_for_read_only_permission_sample_smoke_design_only",
                "allowed_read_only_sample_smoke_design_only",
            )
        } else {
            (
                "structure_incomplete_or_not_reviewed",
                "blocked_manifest_structure_or_review_incomplete",
                "blocked_until_manifest_audit_passes",
            )
        };

    let mut response = base(manifest_status, admission_decision, permission_smoke);
    response["audit_summary"] = json!({
        "artifact_count": artifact_count,
        "reviewed_pass_count": reviewed_pass_count,
        "missing_required_category_count": missing_required_categories.len(),
        "forbidden_manifest_key_count": forbidden_manifest_keys.len(),
        "missing_required_field_count": missing_required_fields.len(),
        "redaction_failure_count": redaction_failure_count,
        "top_level_source_candidate_valid": top_level_valid
    });
    response["missing_required_categories"] = json!(missing_required_categories);
    response["missing_required_fields"] = json!(missing_required_fields);
    response["forbidden_manifest_keys"] = json!(forbidden_manifest_keys);
    response["allowed_after_pass"] = json!([
        "design_read_only_permission_sample_smoke_without_persisting_raw_vendor_rows",
        "run_single_endpoint_permission_probe_only_with_operator_supplied_credentials",
        "probe_representative_history_dates_and_sample_payload_hash_timestamp_semantics"
    ]);
    response["still_forbidden_after_pass"] = json!([
        "schema_apply",
        "bounded_sync",
        "factor_builder",
        "p310_diagnostics",
        "bounded_wfa",
        "v19_train_selection"
    ]);
    response["next_step"] = if structure_passed {
        json!("design_cninfo_read_only_permission_sample_smoke_without_raw_payload_persistence")
    } else {
        json!("fix_external_redacted_manifest_then_repeat_structure_audit_before_any_network_probe")
    };
    response
}

pub(crate) fn phase7_structured_order_capacity_price_chain_cninfo_permission_sample_smoke_plan(
) -> Value {
    let promotion_gate = json!({
        "permission_smoke": "blocked_until_p3_25g_manifest_audit_passes",
        "schema_apply": "blocked",
        "bounded_sync": "blocked",
        "factor_builder": "blocked",
        "p310_status": "blocked",
        "bounded_wfa": "blocked",
        "v19_train_selection": "blocked"
    });

    json!({
        "audit_version": "p3.25h-cninfo-permission-sample-smoke-plan-v1",
        "source_id": "structured_order_capacity_contract_price_chain_source",
        "stage": "P3.25H",
        "candidate_id": "cninfo_data_service",
        "mode": "read_only_cninfo_permission_sample_smoke_plan_no_network_no_secret_read_no_db_write_no_schema_no_sync",
        "admission_decision": "blocked_until_p3_25g_manifest_audit_passes",
        "why_now": "P3.25G can verify that external redacted CNINFO evidence is structurally reviewed; P3.25H defines the next read-only single-endpoint smoke plan but still performs no network call, credential read, DB write, schema apply, raw sync, factor build or training",
        "preconditions": {
            "required_previous_gate": "P3.25G",
            "required_previous_gate_endpoint": "GET /api/v1/quant/data/structured-order-capacity-price-chain/cninfo-operator-evidence-audit",
            "required_previous_gate_decision": "passed_for_read_only_permission_sample_smoke_design_only",
            "manifest_env_var": "QUANT_CNINFO_EVIDENCE_MANIFEST_PATH",
            "credential_source": "operator_supplied_outside_source_control_only_after_manifest_audit_passes",
            "blocked_current_production_reason": "manifest_audit_is_not_passed_or_not_configured"
        },
        "runtime_actions": {
            "network_enabled": false,
            "credential_read_enabled": false,
            "db_write_enabled": false,
            "schema_apply_enabled": false,
            "raw_payload_persistence_enabled": false
        },
        "smoke_plan": {
            "endpoint_scope": "single_endpoint_only",
            "max_endpoints_per_run": 1,
            "max_sample_rows_per_probe": 20,
            "persist_raw_rows": false,
            "persist_credentials": false,
            "capture_raw_payload_in_repo": false,
            "sample_payload_handling": "hash_and_redacted_shape_only_no_raw_payload_return",
            "representative_history_dates": [
                "2014-01-02",
                "2017-01-03",
                "2020-07-01",
                "2024-01-02",
                "latest_completed_publication_or_trading_date"
            ],
            "target_endpoint_candidates_from_manifest_only": [
                "operation_event_or_major_contract_endpoint",
                "timestamped_announcement_metadata_endpoint",
                "industry_chain_or_price_capacity_order_dataset_if_manifest_reviewed"
            ],
            "expected_probe_outputs": [
                "permission_status",
                "endpoint_name_or_id_hash",
                "history_date_status_by_probe_date",
                "sample_row_count_by_probe_date",
                "schema_field_presence_summary",
                "source_published_at_quality_summary",
                "available_at_rule_summary",
                "stable_id_and_raw_hash_presence_summary",
                "symbol_mapping_presence_summary"
            ]
        },
        "required_sample_payload_fields": [
            "vendor",
            "vendor_endpoint",
            "request_key",
            "vendor_event_id_or_document_id",
            "symbol_or_company_identifier",
            "event_type_or_announcement_category",
            "event_date_or_announcement_date",
            "source_published_at",
            "available_at_rule",
            "source_url_or_document_url",
            "source_title_or_payload_excerpt",
            "raw_payload_hash",
            "vendor_schema_or_model_version"
        ],
        "pit_and_available_at_rules": {
            "date_only_publication": "available_at must be next open trading session",
            "intraday_publication": "decision_timestamp must be >= source_published_at",
            "weekend_or_holiday_publication": "available_at maps to next open trading session",
            "forbidden": [
                "using vendor ingestion time as source_published_at",
                "using effective date as available_at",
                "same_session_trading_from_date_only_publication",
                "event_direction_from_future_returns_or_oos_performance"
            ]
        },
        "forbidden_outputs": [
            "raw_vendor_payload_persistence",
            "credential_or_token_echo",
            "schema_apply",
            "bounded_sync",
            "factor_backfill",
            "p310_diagnostics",
            "bounded_wfa",
            "v19_train_selection",
            "full_history_pull",
            "multi_endpoint_probe"
        ],
        "stop_rules": [
            "stop_if_p3_25g_manifest_audit_not_passed",
            "stop_if_endpoint_selected_outside_reviewed_manifest",
            "stop_if_probe_would_persist_raw_vendor_payload",
            "stop_if_probe_requires_more_than_one_endpoint",
            "stop_if_sample_payload_lacks_source_published_at_or_conservative_available_at_rule",
            "stop_if_history_probe_cannot_cover_2014_2017_2020_2024_latest_or_declared_start",
            "stop_if_symbol_mapping_is_current_snapshot_only_or_post_hoc",
            "stop_if_event_tags_are_equivalent_to_noisy_daily_operation_category_without_precision_evidence"
        ],
        "promotion_gate": promotion_gate,
        "allowed_next_implementation_after_precondition_passes": "implement_cninfo_single_endpoint_read_only_permission_sample_smoke_executor_no_raw_persistence",
        "next_step": "wait_for_p3_25g_manifest_audit_pass_then_implement_single_endpoint_read_only_permission_sample_smoke_executor"
    })
}

pub(crate) fn phase7_structured_order_capacity_price_chain_cninfo_operator_evidence_manifest_template(
) -> Value {
    let artifacts: Vec<Value> = cninfo_operator_evidence_required_categories()
        .into_iter()
        .enumerate()
        .map(|(idx, artifact_type)| {
            json!({
                "artifact_id": format!("replace_me_cninfo_evidence_{:02}", idx + 1),
                "artifact_type": artifact_type,
                "owner": "replace_me_operator_or_reviewer",
                "review_status": "missing",
                "reviewed_at": "replace_me_iso8601_after_manual_review",
                "storage_location_type": "external_operator_controlled_redacted_reference",
                "content_hash": "replace_me_sha256_of_redacted_evidence_metadata_or_document_reference",
                "redaction_status": "missing",
                "source_effective_start_date": "replace_me_yyyy_mm_dd_or_declared_start",
                "source_effective_end_date": "replace_me_yyyy_mm_dd_or_present",
                "pit_relevance": "replace_me_why_this_artifact_supports_cninfo_pit_source_admission",
                "notes": "replace_me_redacted_summary_no_secret_no_raw_payload_no_vendor_document"
            })
        })
        .collect();

    json!({
        "audit_version": "p3.25i-cninfo-operator-evidence-manifest-template-v1",
        "source_id": "structured_order_capacity_contract_price_chain_source",
        "stage": "P3.25I",
        "candidate_id": "cninfo_data_service",
        "mode": "read_only_cninfo_operator_evidence_manifest_template_no_network_no_secret_no_db_write",
        "admission_decision": "template_only_not_evidence_blocked_until_operator_review_replaces_placeholders",
        "why_now": "P3.25G/H correctly block real CNINFO probes until an external redacted evidence manifest exists; P3.25I provides a machine-auditable template so operators can prepare evidence without committing secrets, raw payloads or vendor documents",
        "runtime_actions": {
            "network_enabled": false,
            "credential_read_enabled": false,
            "db_write_enabled": false,
            "schema_apply_enabled": false,
            "raw_payload_persistence_enabled": false
        },
        "template_policy": {
            "template_can_pass_p3_25g_without_operator_review": false,
            "must_be_stored_outside_git_repository": true,
            "must_replace_all_placeholders": true,
            "must_set_review_status_to_reviewed_pass_only_after_manual_review": true,
            "must_not_include_secret_values_raw_vendor_payloads_or_full_vendor_documents": true
        },
        "manifest_env_var": "QUANT_CNINFO_EVIDENCE_MANIFEST_PATH",
        "suggested_external_path": "operator_controlled_path_outside_git_repository/cninfo_manifest.redacted.json",
        "manifest_template": {
            "manifest_version": "p3.25i-cninfo-operator-evidence-manifest-template-v1",
            "source_id": "structured_order_capacity_contract_price_chain_source",
            "candidate_id": "cninfo_data_service",
            "artifacts": artifacts
        },
        "operator_fill_instructions": [
            "copy_manifest_template_to_operator_controlled_path_outside_git_repository",
            "replace_every_replace_me_placeholder_with_redacted_metadata_or_hash_only",
            "keep_credential_values_contract_text_raw_payloads_and_vendor_documents_out_of_manifest",
            "set_review_status_reviewed_pass_only_after_legal_operator_manual_review",
            "configure_quant_cninfo_evidence_manifest_path_to_the_external_manifest",
            "rerun_p3_25g_operator_evidence_audit"
        ],
        "forbidden_manifest_contents": [
            "api_token",
            "password",
            "cookie",
            "authorization",
            "authorization_header",
            "raw_payload",
            "raw_vendor_payload",
            "full_vendor_document",
            "unredacted_license_contract",
            "private_endpoint_secret",
            "material_non_public_information",
            "oos_performance_label"
        ],
        "promotion_gate": {
            "manifest_audit": "blocked_until_operator_replaces_template_and_manual_review_passes",
            "permission_smoke": "blocked_until_p3_25g_manifest_audit_passes",
            "schema_apply": "blocked",
            "bounded_sync": "blocked",
            "factor_builder": "blocked",
            "p310_status": "blocked",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        },
        "next_step": "operator_prepares_external_redacted_manifest_then_reruns_p3_25g_audit"
    })
}

pub(crate) fn ddl_contains_all(ddl: &str, required: &[&str]) -> (Vec<String>, Vec<String>) {
    let mut present = Vec::new();
    let mut missing = Vec::new();
    for item in required {
        if ddl.contains(item) {
            present.push((*item).to_string());
        } else {
            missing.push((*item).to_string());
        }
    }
    (present, missing)
}

pub(crate) fn phase7_exchange_announcement_order_capacity_manual_schema_review() -> Value {
    const DDL_PATH: &str = "sql/phase7_exchange_announcement_order_capacity_source.sql";
    let ddl =
        include_str!("../../../../sql/phase7_exchange_announcement_order_capacity_source.sql");

    let required_fields = [
        "vendor",
        "vendor_endpoint",
        "request_key",
        "symbol",
        "announcement_id",
        "announcement_time",
        "source_published_at",
        "source_published_at_ts",
        "source_published_date",
        "source_published_at_quality",
        "available_at",
        "announcement_url",
        "pdf_final_url",
        "text_content",
        "text_hash",
        "text_hash_algorithm",
        "timestamp_candidates",
        "pdf_metadata_keys",
        "raw_payload",
        "raw_payload_hash",
        "parser_used",
        "parser_version",
        "parser_errors",
        "pdf_parse_status",
        "event_type",
        "evidence_spans",
        "ingested_at",
        "data_version_id",
    ];
    let required_constraints = [
        "PRIMARY KEY (vendor, vendor_endpoint, announcement_id, symbol, raw_payload_hash)",
        "CHECK (available_at >= announcement_time)",
        "source_published_at_quality IN ('timestamp', 'date_only_next_session', 'missing')",
        "text_hash_algorithm IN ('sha256')",
        "source_published_at_quality = 'timestamp' AND source_published_at_ts IS NOT NULL",
        "source_published_at_quality = 'date_only_next_session' AND source_published_date IS NOT NULL",
        "order_or_contract_signed",
        "capacity_expansion_or_commissioning",
        "product_price_adjustment",
        "major_supply_or_customer_agreement",
        "scanned_pdf_ocr_required",
    ];
    let required_indexes = [
        "idx_market_exchange_announcement_available_at",
        "idx_market_exchange_announcement_symbol_available_at",
        "idx_market_exchange_announcement_quality",
        "idx_market_exchange_announcement_hash",
        "idx_market_exchange_announcement_parser_status",
    ];

    let (present_fields, missing_fields) = ddl_contains_all(ddl, &required_fields);
    let (present_constraints, missing_constraints) = ddl_contains_all(ddl, &required_constraints);
    let (present_indexes, missing_indexes) = ddl_contains_all(ddl, &required_indexes);

    let passed = missing_fields.is_empty()
        && missing_constraints.is_empty()
        && missing_indexes.is_empty()
        && !ddl.contains("CREATE TABLE IF NOT EXISTS factor_")
        && !ddl.contains("model_prediction")
        && !ddl.contains("experiment_run");

    json!({
        "audit_version": "p3.24f-exchange-announcement-order-capacity-manual-schema-review-v1",
        "source_id": "exchange_announcement_order_capacity_text",
        "stage": "P3.24F",
        "mode": "read_only_schema_contract_ddl_review_no_apply",
        "write_enabled": false,
        "ddl_path": DDL_PATH,
        "schema_review_decision": if passed {
            "passed_for_bounded_sync_design_only"
        } else {
            "blocked_schema_contract_or_pit_ddl_gap"
        },
        "pit_review": {
            "timestamp_policy": "timestamp requires source_published_at_ts; intraday remains blocked unless decision-time ordering is proven",
            "date_only_next_session_policy": if ddl.contains("date_only_next_session") && ddl.contains("source_published_date") {
                "encoded"
            } else {
                "missing"
            },
            "available_at_policy": "available_at >= announcement_time is necessary but not sufficient; bounded sync must map date_only_next_session to the next open session",
            "future_data_policy": "features must still require available_at <= trade_date; this review does not admit factor generation"
        },
        "ddl_checks": {
            "required_fields": present_fields,
            "missing_required_fields": missing_fields,
            "missing_required_field_count": missing_fields.len(),
            "required_constraints": present_constraints,
            "missing_required_constraints": missing_constraints,
            "missing_required_constraint_count": missing_constraints.len(),
            "required_indexes": present_indexes,
            "missing_required_indexes": missing_indexes,
            "missing_required_index_count": missing_indexes.len(),
            "raw_failure_sample_preservation": if ddl.contains("parser_errors") && ddl.contains("pdf_parse_status") {
                "encoded"
            } else {
                "missing"
            },
            "text_evidence_preservation": if ddl.contains("evidence_spans JSONB") && ddl.contains("text_content TEXT") {
                "encoded"
            } else {
                "missing"
            }
        },
        "promotion_gate": {
            "schema_apply": if passed {
                "manual_review_passed_apply_still_requires_explicit_operator_action"
            } else {
                "blocked_until_schema_review_passes"
            },
            "bounded_sync": if passed {
                "blocked_until_plan_only_bounded_sync_review"
            } else {
                "blocked"
            },
            "factor_builder": "blocked",
            "p310_status": "blocked",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        },
        "guardrails": [
            "this endpoint does not execute DDL or write raw tables",
            "manual schema review passing is not data coverage readiness",
            "date-only announcementTime may only be mapped to next open session",
            "P3.10, WFA and v19 remain blocked until bounded sync, coverage, PIT, evidence precision and correlation audits pass"
        ],
        "next_required_steps": if passed {
            json!([
                "build_plan_only_calendar_day_symbol_category_bounded_sync_design",
                "review_calendar_day_to_open_session_mapping",
                "review_idempotent_raw_landing_and_failure_sample_policy",
                "run_one_small_batch_after_operator_schema_apply"
            ])
        } else {
            json!([
                "fix_schema_contract_or_ddl_gaps",
                "rerun_manual_schema_review_before_bounded_sync_design"
            ])
        }
    })
}

pub(crate) fn phase7_exchange_announcement_order_capacity_coverage_quality_audit_contract() -> Value
{
    json!({
        "audit_version": "p3.24h-exchange-announcement-order-capacity-coverage-quality-audit-contract-v1",
        "source_id": "exchange_announcement_order_capacity_text",
        "stage": "P3.24H",
        "status": "coverage_pit_quality_contract_defined_small_batch_audit_required_next",
        "mode": "read_only_coverage_pit_quality_contract_no_raw_sync",
        "write_enabled": false,
        "endpoint": "GET /api/v1/quant/data/exchange-announcement-order-capacity/coverage-quality-audit-contract",
        "depends_on": [
            "p3.24f-exchange-announcement-order-capacity-manual-schema-review-v1",
            "p3.24g-exchange-announcement-order-capacity-bounded-sync-plan-v1"
        ],
        "small_batch_preconditions": [
            "operator_applies_sql_phase7_exchange_announcement_order_capacity_source_explicitly",
            "run_one_small_calendar_day_symbol_category_raw_sync_after_schema_apply",
            "retain ok_empty, parser_error, scanned_pdf_ocr_required and incomplete_metadata rows as auditable outcomes",
            "do not start full-history raw sync before this contract is implemented as a real audit endpoint"
        ],
        "coverage_breakdowns": [
            "year_market_category_symbol",
            "calendar_day_publication_and_next_open_session",
            "announcement_category_by_year_market",
            "event_type_by_year_market_category",
            "source_published_at_quality_by_year_category",
            "pdf_parse_status_by_year_category",
            "ok_empty_and_parser_error_by_request_key"
        ],
        "required_small_batch_audit_fields": [
            "raw_row_count",
            "request_key_count",
            "covered_calendar_day_count",
            "covered_open_session_count",
            "missing_calendar_day_count",
            "missing_next_open_session_count",
            "year_market_category_symbol_breakdown",
            "source_published_at_quality_distribution",
            "announcement_time_to_available_at_mapping_sample",
            "duplicate_announcement_id_rows",
            "duplicate_raw_payload_hash_groups",
            "missing_announcement_link_metadata_rows",
            "missing_pdf_final_url_rows",
            "parser_error_rows",
            "scanned_pdf_ocr_required_rows",
            "ocr_taxonomy_excluded_rows",
            "trainable_scanned_pdf_blocking_rows",
            "ok_empty_request_count",
            "missing_text_hash_rows",
            "missing_evidence_span_rows",
            "event_taxonomy_manual_precision_sample",
            "correlation_vs_existing_sources"
        ],
        "pit_audit": {
            "announcement_time_source": "CNInfo link announcementTime plus PDF/detail timestamp candidates when available",
            "date_only_next_session_mapping": "required_for_every_date_only_or_weekend_holiday_publication",
            "timestamp_mapping": "trusted source_published_at_ts may use same daily session only when source_published_at_ts <= decision timestamp is proven; intraday remains blocked otherwise",
            "open_session_calendar": "market_trade_calendar distinct open_date mapping; non-trading-day announcements map to the next open trading session",
            "no_future_data_rule": "every feature row must require available_at <= feature_trade_date and raw ingested_at must never be used as source publication time",
            "required_violation_checks": [
                "available_at_before_source_published_at",
                "available_at_after_feature_trade_date",
                "date_only_mapped_to_previous_open_session",
                "weekend_or_holiday_publication_dropped_or_backfilled_to_previous_session",
                "source_published_at_quality_missing_used_for_training"
            ]
        },
        "quality_thresholds": {
            "pit_violation_rows": 0,
            "missing_available_at_rows": 0,
            "missing_source_published_at_quality_rows": 0,
            "duplicate_announcement_id_rows": 0,
            "duplicate_raw_payload_hash_groups": 0,
            "missing_text_hash_rows_for_parsed_pdf": 0,
            "missing_evidence_span_rows_for_event_rows": 0,
            "missing_link_metadata_rows": 0,
            "parser_error_rows_policy": "allowed_only_as_retained_raw_failures_not_as_trainable_rows",
            "scanned_pdf_ocr_required_policy": "blocked_until_separate_ocr_runtime_and_audit_or_narrow_taxonomy_exclusion",
            "ocr_taxonomy_exclusion_policy": "only pre-registered non-target special reports such as related-party funds, finance-company transaction reports, audit reports, legal opinions and financial-advisor reports may be excluded from trainable OCR blockers",
            "trainable_scanned_pdf_blocking_rows": 0,
            "ok_empty_policy": "allowed_as_request_coverage_outcome_but_not_as_positive_event_evidence",
            "evidence_span_precision_manual_sample_min": "0.80",
            "event_taxonomy_precision_manual_sample_min": "0.80",
            "event_taxonomy_precision_sample_size_min": 50,
            "max_abs_correlation_vs_existing_source_daily_score": "0.30"
        },
        "correlation_audit": {
            "required_before_p310": true,
            "compare_against": [
                "moneyflow_congestion",
                "liquidity",
                "price_volume",
                "financial_quality_change",
                "earnings_recovery_persistence",
                "event_post_return_overlay",
                "multi_vendor_analyst_revision"
            ],
            "alignment_rule": "use conservative available_at aligned daily feature dates only; never align by announcement title date alone",
            "decision_rule": "high correlation does not imply failure alone, but requires orthogonalization or source stop before P3.10"
        },
        "diagnostics_after_contract_and_small_batch_pass": [
            "implement real coverage-quality-audit endpoint against market_exchange_announcement_text_raw",
            "expand bounded raw sync by month or quarter only after small-batch audit passes",
            "run full-history coverage/PIT/evidence/correlation audit before P3.10",
            "run P3.10A-D diagnostics only after raw source audit passes"
        ],
        "promotion_gate": {
            "schema_apply": "manual_review_passed_apply_still_requires_explicit_operator_action",
            "bounded_sync": "blocked_until_operator_schema_apply_and_one_small_batch_raw_sync_review",
            "coverage_quality_audit": "required_after_each_small_batch_and_before_any_full_history_sync",
            "full_history_sync": "blocked_until_small_batch_coverage_quality_audit_passes",
            "factor_builder": "blocked",
            "p310_status": "blocked",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        },
        "guardrails": [
            "this endpoint is a read-only contract and never reads or writes raw data",
            "do not promote raw source readiness to trainable readiness",
            "do not mine keywords, event signs, horizons, or gates on the full OOS period",
            "do not use same-session or intraday date-only announcements",
            "do not enter P3.10, WFA or v19 until bounded sync, coverage, PIT, evidence precision and low-correlation audits pass"
        ],
        "next_step": "operator_schema_apply_then_one_small_batch_raw_sync_then_implement_real_coverage_quality_audit_endpoint"
    })
}

pub(crate) fn build_exchange_announcement_order_capacity_sync_plan(
    req: ExchangeAnnouncementOrderCapacitySyncPlanReq,
) -> Result<Value, String> {
    let today = Utc::now().date_naive();
    let start_date = req.start_date.unwrap_or_else(|| "20140101".to_string());
    let end_date = req
        .end_date
        .unwrap_or_else(|| today.format("%Y%m%d").to_string());
    let start = parse_optional_date(Some(start_date.as_str()))?
        .ok_or_else(|| "start_date is required".to_string())?;
    let end = parse_optional_date(Some(end_date.as_str()))?
        .ok_or_else(|| "end_date is required".to_string())?;
    if start > end {
        return Err("exchange announcement sync-plan start_date cannot be after end_date".into());
    }

    let requested_symbols = exchange_announcement_order_capacity_csv_values(req.symbols);
    let symbols = exchange_announcement_order_capacity_symbols(&requested_symbols);
    if symbols.is_empty() {
        return Err(
            "exchange announcement sync-plan requires at least one comma-separated symbol".into(),
        );
    }
    let requested_categories = exchange_announcement_order_capacity_csv_values(req.categories);
    let categories = exchange_announcement_order_capacity_categories(&requested_categories);
    if categories.is_empty() {
        return Err("exchange announcement sync-plan requires at least one category".into());
    }
    let market = req
        .market
        .filter(|market| !market.trim().is_empty())
        .unwrap_or_else(|| "沪深京".to_string());
    let batch_mode = req
        .batch
        .unwrap_or_else(|| "quarter".to_string())
        .trim()
        .to_ascii_lowercase();
    let batches = exchange_announcement_order_capacity_sync_plan_batches(start, end, &batch_mode)?;

    Ok(exchange_announcement_order_capacity_sync_plan_response(
        start,
        end,
        &batch_mode,
        market,
        symbols,
        categories,
        batches,
    ))
}

pub(crate) fn phase7_akshare_analyst_revision_schema_contract() -> Value {
    json!({
        "audit_version": "p3.23c-akshare-analyst-revision-schema-contract-v1",
        "source_id": "multi_vendor_analyst_revision",
        "stage": "P3.23C",
        "status": "history_replay_passed_schema_review_allowed",
        "mode": "read_only_vendor_schema_available_at_contract",
        "ddl_path": "sql/phase7_akshare_analyst_revision_source.sql",
        "raw_sources": [
            {
                "vendor": "akshare",
                "vendor_endpoint": "stock_rank_forecast_cninfo",
                "source_semantics": "cninfo analyst stock rating and target-price forecast by publication date",
                "request_key": "date",
                "native_available_at_candidate": "发布日期",
                "source_published_at_candidate": "发布日期",
                "observed_smoke": {
                    "akshare_version": "1.18.64",
                    "date": "20260623",
                    "row_count": 26,
                    "fields": ["证券代码", "发布日期", "研究机构简称", "研究员名称", "投资评级", "是否首次评级", "评级变化", "前一次投资评级", "目标价格-上限", "目标价格-下限"]
                },
                "admission_gate": "permission_history_date_available_at_and_full_history_coverage_audit_required_before_raw_sync"
            },
            {
                "vendor": "akshare",
                "vendor_endpoint": "stock_research_report_em",
                "source_semantics": "eastmoney single-stock research report list with rating, institution, earnings forecast, report date and pdf link",
                "request_key": "symbol",
                "native_available_at_candidate": "日期",
                "source_published_at_candidate": "日期",
                "observed_smoke": {
                    "akshare_version": "1.18.64",
                    "symbol": "000001",
                    "row_count": 225
                },
                "admission_gate": "low_fanout_evidence_layer_only_until_full_symbol_fanout_coverage_cost_and_history_stability_pass"
            },
            {
                "vendor": "akshare",
                "vendor_endpoint": "stock_profit_forecast_em",
                "source_semantics": "current consensus profit forecast snapshot",
                "request_key": "snapshot",
                "native_available_at_candidate": "none_observed",
                "admission_gate": "blocked_snapshot_not_pit_ready_without_vendor_snapshot_archive"
            },
            {
                "vendor": "akshare",
                "vendor_endpoint": "stock_institute_recommend_or_ths_family",
                "source_semantics": "institution recommendation/profit forecast pages observed as parser-unstable in smoke",
                "request_key": "varies",
                "native_available_at_candidate": "not_admitted",
                "admission_gate": "blocked_parse_unreliable_do_not_sync"
            },
            {
                "vendor": "tushare",
                "vendor_endpoint": "report_rc",
                "source_semantics": "sell-side research report earnings forecast daily since 2010",
                "request_key": "report_date_range",
                "native_available_at_candidate": "report_date",
                "admission_gate": "blocked_current_api_unknown_source_after_40101_permission_smoke"
            }
        ],
        "tables": [
            {
                "table": "market_vendor_analyst_revision_raw",
                "natural_key": ["vendor", "vendor_endpoint", "request_key", "symbol", "source_published_at", "raw_payload_hash"],
                "required_fields": [
                    "vendor",
                    "vendor_source",
                    "vendor_endpoint",
                    "request_key",
                    "symbol",
                    "publication_date",
                    "source_published_at",
                    "available_at",
                    "ingested_at",
                    "institution_name",
                    "analyst_name",
                    "rating_current",
                    "rating_previous",
                    "rating_change",
                    "is_first_rating",
                    "target_price_min",
                    "target_price_max",
                    "report_title",
                    "report_url",
                    "raw_payload",
                    "raw_payload_hash",
                    "data_version_id"
                ],
                "pit_rule": "available_at must be no earlier than the native source publication date; without audited intraday timestamp, intraday trading must use next-session availability only.",
                "raw_landing_policy": "preserve vendor rows and raw payload hash; source admission gates decide whether rows can feed diagnostics."
            }
        ],
        "available_at_policy": {
            "stock_rank_forecast_cninfo": "发布日期 is a date-level available_at/source_published_at candidate; source must pass historical date replay and same-day publication timing audit before intraday use.",
            "stock_research_report_em": "日期 is a candidate only for evidence-layer reports; full-market fanout and pagination/history stability must pass before factor use.",
            "stock_profit_forecast_em": "current snapshot has no historical snapshot date, so it is blocked for 2014-2026 PIT revision unless a vendor snapshot archive is built prospectively.",
            "blocked_parse_unreliable": "parser-unstable endpoints must not create raw tables until smoke becomes repeatable."
        },
        "coverage_audit_required": [
            "history_date_replay_by_year",
            "trading_day_and_calendar_day_breakdown",
            "vendor_endpoint_symbol_breadth",
            "publication_date_null_or_future_leak_count",
            "source_published_at_null_count",
            "revision_semantics_rating_change_and_previous_rating_coverage",
            "duplicate_raw_payload_hash_count",
            "cross_vendor_overlap_vs_tushare_report_rc_if_available"
        ],
        "promotion_gate": {
            "schema_apply": "schema_review_allowed_after_history_replay_passed",
            "bounded_sync": "blocked_until_schema_review_and_manual_apply",
            "factor_builder": "blocked",
            "p310_status": "blocked",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        },
        "sync_plan_endpoint": "GET /api/v1/quant/data/akshare/analyst-revision/sync-plan",
        "readiness_audit_endpoint": "GET /api/v1/quant/data/akshare/analyst-revision/readiness-audit",
        "next_step": "manual_schema_review_apply_then_bounded_calendar_day_sync"
    })
}

pub(crate) fn phase7_akshare_analyst_revision_available_at_contract() -> Value {
    json!({
        "audit_version": "p3.23a-akshare-analyst-revision-available-at-contract-v1",
        "source_id": "multi_vendor_analyst_revision",
        "mode": "read_only_vendor_available_at_source_published_at_semantics_audit",
        "endpoint_semantics": [
            {
                "vendor": "akshare",
                "vendor_endpoint": "stock_rank_forecast_cninfo",
                "available_at_candidate": "发布日期",
                "source_published_at_candidate": "发布日期",
                "revision_fields": ["评级变化", "前一次投资评级", "投资评级", "是否首次评级", "目标价格-上限", "目标价格-下限"],
                "verdict": "history_replay_required_before_raw_sync",
                "blocked_until": ["permission_smoke_passed", "multiple_history_dates_return_replayable_rows", "publication_date_parse_rate_audited", "source_published_at_lag_policy_registered"]
            },
            {
                "vendor": "akshare",
                "vendor_endpoint": "stock_research_report_em",
                "available_at_candidate": "日期",
                "source_published_at_candidate": "日期",
                "revision_fields": ["评级", "机构", "盈利预测", "报告名称", "PDF链接"],
                "verdict": "low_fanout_evidence_layer_only_until_full_symbol_fanout_coverage_passes",
                "blocked_until": ["symbol_fanout_cost_audited", "pagination_history_stability_audited", "report_date_parse_rate_audited"]
            },
            {
                "vendor": "akshare",
                "vendor_endpoint": "stock_profit_forecast_em",
                "available_at_candidate": "none_observed",
                "source_published_at_candidate": "none_observed",
                "revision_fields": [],
                "verdict": "blocked_snapshot_not_pit_ready",
                "blocked_reason": "current snapshot without historical snapshot date cannot reconstruct 2014-2026 PIT revision history"
            },
            {
                "vendor": "akshare",
                "vendor_endpoint": "stock_institute_recommend_or_ths_family",
                "available_at_candidate": "not_admitted",
                "source_published_at_candidate": "not_admitted",
                "revision_fields": [],
                "verdict": "blocked_parse_unreliable",
                "blocked_reason": "prior smoke observed parser/XML failures; do not create schema or sync until repeatability is proven"
            },
            {
                "vendor": "tushare",
                "vendor_endpoint": "report_rc",
                "available_at_candidate": "report_date",
                "source_published_at_candidate": "report_date_or_create_time",
                "revision_fields": ["rating", "eps", "max_price", "min_price", "quarter", "org_name", "author_name"],
                "verdict": "blocked_permission_unknown_source",
                "blocked_reason": "production smoke returned Tushare 40101 unknown data source"
            }
        ],
        "pit_policy": {
            "daily_research": "date-level publication fields are allowed only as end-of-day/next-session availability until intraday source timestamps are audited.",
            "downstream_rule": "factor and diagnostics must filter source.available_at <= equity_trade_date; intraday simulation must additionally require source_published_at <= decision_timestamp.",
            "prohibited": [
                "using current snapshot fields to reconstruct past consensus",
                "using ingestion time as historical source publication time",
                "using full-period coverage or OOS results to choose endpoint sign or endpoint inclusion"
            ]
        },
        "promotion_gate": {
            "schema_apply": "blocked_until_permission_history_available_at_review",
            "bounded_sync": "blocked_until_schema_review_and_history_smoke_pass",
            "coverage_status": "blocked_until_raw_sync_exists",
            "p310_status": "blocked",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        },
        "next_step": "permission_history_date_smoke_for_stock_rank_forecast_cninfo_across_multiple_years"
    })
}

pub(crate) fn phase7_multi_vendor_analyst_revision_candidate() -> Value {
    json!({
        "source_id": "multi_vendor_analyst_revision",
        "source_family": "broad_base_analyst_expectation_revision",
        "economic_hypothesis": "真正的评级/目标价/盈利预期修正如果能做到 broad-base、PIT、低相关，可能比已证伪的公开低频经营代理更接近可交易的信息增量；供应商替换不能绕过 source admission、coverage、P3.10 或 bounded WFA。",
        "candidate_raw_sources": [
            "akshare:stock_rank_forecast_cninfo",
            "akshare:stock_research_report_em",
            "akshare:stock_profit_forecast_em_blocked_snapshot",
            "akshare:stock_institute_recommend_blocked_parse_unreliable",
            "tushare:report_rc_blocked_40101"
        ],
        "source_discovery_evidence": [
            {
                "candidate": "akshare:stock_rank_forecast_cninfo",
                "status": "stopped_after_p310_economics_failed",
                "akshare_version": "1.18.64",
                "observed_smoke": {
                    "date": "20260623",
                    "row_count": 26,
                    "fields": ["证券代码", "发布日期", "研究机构简称", "研究员名称", "投资评级", "是否首次评级", "评级变化", "前一次投资评级", "目标价格-上限", "目标价格-下限"]
                },
                "history_date_smoke": "passed",
                "native_available_at_candidate": "发布日期",
                "source_published_at_candidate": "发布日期",
                "full_history_summary": {
                    "raw_rows": 1222450,
                    "raw_date_range": "2014-01-01..2026-06-24",
                    "factor_task_id": "fs-20260624-155815497-05f24316",
                    "combo_rows": 1025751,
                    "combo_date_range": "2014-01-03..2026-06-23",
                    "future_leak_rows": 0,
                    "null_available_at_rows": 0,
                    "null_score_rows": 0
                },
                "diagnostics_summary": {
                    "latest_report_id": "exp-0930e5fa-f125-4f22-b9d2-5041da2c44c3",
                    "level": "red",
                    "passed": false,
                    "mean_rankic_20_45_60_120": [-0.00216, -0.00190, -0.00298, 0.00588],
                    "high_minus_low_spread_20_45_60_120": [0.00372, 0.00269, 0.00616, 0.00824],
                    "passed_horizon_count": 0,
                    "daily_weak_day_count": 1316,
                    "daily_weak_day_ratio": 0.4345,
                    "decision": "do_not_enter_bounded_wfa_or_v19_train_selection"
                },
                "decision": "stop_current_akshare_cninfo_revision_expression_after_p310_economics_failed"
            },
            {
                "candidate": "akshare:stock_research_report_em",
                "status": "low_fanout_evidence_layer_candidate",
                "akshare_version": "1.18.64",
                "observed_smoke": {
                    "symbol": "000001",
                    "row_count": 225
                },
                "native_available_at_candidate": "日期",
                "decision": "do_not_treat_as_broad_base_until_full_symbol_fanout_coverage_and_pagination_history_stability_pass"
            },
            {
                "candidate": "akshare:stock_profit_forecast_em",
                "status": "blocked_snapshot_not_pit_ready",
                "blocked_reason": "current snapshot has no historical snapshot date and cannot reconstruct 2014-2026 PIT consensus revisions"
            },
            {
                "candidate": "akshare:stock_institute_recommend_or_ths_family",
                "status": "blocked_parse_unreliable",
                "blocked_reason": "prior smoke observed parser/XML failures"
            },
            {
                "candidate": "tushare:report_rc",
                "status": "blocked_current_api_unknown_source_after_production_smoke",
                "production_smoke": {
                    "as_of": "2026-06-21",
                    "error_code": "40101",
                    "error": "未知的数据源"
                }
            }
        ],
        "current_tables": ["market_vendor_analyst_revision_raw", "factor_value", "multi_factor_value"],
        "schema_status": "completed",
        "client_status": "read_only_and_sync_client_completed",
        "sync_status": "full_history_raw_sync_completed",
        "coverage_status": "coverage_pit_quality_correlation_green",
        "p310_status": "completed_failed_economics",
        "diagnostics_summary": {
            "latest_report_id": "exp-0930e5fa-f125-4f22-b9d2-5041da2c44c3",
            "level": "red",
            "passed": false,
            "mean_rankic_20_45_60_120": [-0.00216, -0.00190, -0.00298, 0.00588],
            "high_minus_low_spread_20_45_60_120": [0.00372, 0.00269, 0.00616, 0.00824],
            "passed_horizon_count": 0,
            "daily_weak_day_count": 1316,
            "daily_weak_day_ratio": 0.4345,
            "decision": "do_not_enter_bounded_wfa_or_v19_train_selection"
        },
        "bounded_wfa": "blocked",
        "v19_train_selection": "blocked",
        "pit_required": true,
        "available_at_policy": "vendor publication date or report date must be persisted as source_published_at/available_at; without intraday timestamp, same-day use is blocked for intraday simulation and only next-session availability is allowed",
        "schema_contract_endpoint": "GET /api/v1/quant/data/akshare/analyst-revision/schema-contract",
        "permission_smoke_endpoint": "POST /api/v1/quant/data/akshare/analyst-revision/permission-smoke",
        "available_at_audit_endpoint": "GET /api/v1/quant/data/akshare/analyst-revision/available-at-audit",
        "coverage_audit_endpoint": "GET /api/v1/quant/data/akshare/analyst-revision/coverage-audit",
        "admission_decision": "stopped_after_p310_economics_failed",
        "blocked_reason": "raw_pit_coverage_and_factor_combo_completed_but_daily_symbol_cliff_and_p310_economics_failed; no_same_family_event_window_or_horizon_rescue",
        "next_step": "search_licensed_broad_base_consensus_revision_or_exchange_announcement_order_capacity_source"
    })
}

pub(crate) fn shareholder_structure_expected_schema() -> Vec<(&'static str, Vec<&'static str>)> {
    vec![
        (
            "market_stock_holder_number",
            vec![
                "market_stock_holder_number_pkey",
                "market_stock_holder_number_available_at_check",
            ],
        ),
        (
            "market_stock_top10_holders",
            vec![
                "market_stock_top10_holders_pkey",
                "market_stock_top10_holders_available_at_check",
            ],
        ),
        (
            "market_stock_top10_float_holders",
            vec![
                "market_stock_top10_float_holders_pkey",
                "market_stock_top10_float_holders_available_at_check",
            ],
        ),
        (
            "market_stock_holder_trade",
            vec![
                "market_stock_holder_trade_pkey",
                "market_stock_holder_trade_available_at_check",
                "market_stock_holder_trade_interval_check",
            ],
        ),
    ]
}

pub(crate) async fn table_exists(db: &sqlx::PgPool, table: &str) -> Result<bool, String> {
    let regclass_name = format!("public.{table}");
    sqlx::query_scalar("SELECT to_regclass($1)::text IS NOT NULL")
        .bind(&regclass_name)
        .fetch_one(db)
        .await
        .map_err(|error| format!("Failed to inspect table {table}: {error}"))
}

pub(crate) fn decide_equity_pledge_readiness(
    schema_passed: bool,
    stat_rows: i64,
    detail_rows: i64,
    pit_violation_rows: i64,
) -> Value {
    let raw_rows = stat_rows + detail_rows;
    let (schema_status, sync_status, admission_decision, next_step) = if !schema_passed {
        (
            "missing_or_invalid",
            "blocked",
            "apply_schema_before_sync",
            "apply_sql_phase7_equity_pledge_source_then_rerun_readiness_audit",
        )
    } else if raw_rows == 0 {
        (
            "created",
            "not_started",
            "bounded_sync_required_before_coverage_audit",
            "run_bounded_equity_pledge_pressure_sync_then_readiness_audit",
        )
    } else if pit_violation_rows > 0 {
        (
            "created",
            "raw_synced_pit_failed",
            "raw_pit_failed",
            "fix_or_delete_bad_equity_pledge_rows_then_rerun_sync_and_audit",
        )
    } else {
        (
            "created",
            "raw_synced",
            "coverage_readiness_audit_required_before_p310",
            "run_year_symbol_ann_date_coverage_and_duplicate_audit_before_factor_builder",
        )
    };

    json!({
        "schema_status": schema_status,
        "sync_status": sync_status,
        "admission_decision": admission_decision,
        "p310_status": "blocked_until_coverage_readiness_passes",
        "wfa_status": "blocked",
        "v19_train_selection": "blocked",
        "stat_rows": stat_rows,
        "detail_rows": detail_rows,
        "raw_rows": raw_rows,
        "pit_violation_rows": pit_violation_rows,
        "next_step": next_step,
    })
}

pub(crate) fn decide_margin_detail_readiness(
    schema_passed: bool,
    raw_rows: i64,
    pit_violation_rows: i64,
    missing_source_published_at_rows: i64,
    core_negative_rows: i64,
) -> Value {
    let (schema_status, sync_status, admission_decision, next_step) = if !schema_passed {
        (
            "missing_or_invalid",
            "blocked",
            "apply_schema_before_sync",
            "apply_sql_phase7_margin_detail_source_then_rerun_readiness_audit",
        )
    } else if raw_rows <= 0 {
        (
            "created",
            "not_started",
            "bounded_sync_required_before_coverage_audit",
            "run_bounded_margin_detail_sync_then_readiness_audit",
        )
    } else if pit_violation_rows > 0 {
        (
            "created",
            "raw_synced_pit_failed",
            "raw_pit_failed",
            "repair_available_at_or_delete_bad_margin_detail_rows_then_rerun_sync_and_audit",
        )
    } else if missing_source_published_at_rows > 0 {
        (
            "created",
            "raw_synced_publication_time_incomplete",
            "raw_publication_time_failed",
            "repair_margin_detail_source_published_at_before_intraday_or_p310_use",
        )
    } else if core_negative_rows > 0 {
        (
            "created",
            "raw_synced_quality_failed",
            "raw_core_nonnegative_failed",
            "inspect_core_negative_margin_detail_rows_then_repair_exclude_or_gate",
        )
    } else {
        (
            "created",
            "raw_synced",
            "coverage_correlation_readiness_audit_required_before_p310",
            "run_year_market_symbol_negative_adjustment_and_correlation_audit_before_p310",
        )
    };

    json!({
        "schema_status": schema_status,
        "sync_status": sync_status,
        "admission_decision": admission_decision,
        "p310_status": "blocked_until_coverage_pit_quality_and_correlation_readiness_pass",
        "wfa_status": "blocked",
        "v19_train_selection": "blocked",
        "raw_rows": raw_rows,
        "pit_violation_rows": pit_violation_rows,
        "missing_source_published_at_rows": missing_source_published_at_rows,
        "core_negative_rows": core_negative_rows,
        "negative_adjustment_policy": {
            "rzche": "allowed_as_raw_vendor_adjustment_and_must_be_reported",
            "rqchl": "allowed_as_raw_vendor_adjustment_and_must_be_reported",
            "core_nonnegative_fields": ["rzye", "rqye", "rzmre", "rqyl", "rqmcl", "rzrqye"]
        },
        "next_step": next_step,
    })
}

pub(crate) fn margin_detail_correlation_decision(max_abs_correlation: Option<f64>) -> &'static str {
    match max_abs_correlation {
        Some(value) if value >= 0.70 => "blocked_same_family_high_correlation",
        Some(value) if value >= 0.50 => {
            "caution_same_family_medium_correlation_requires_manual_review"
        }
        Some(_) => "passed_low_linear_correlation_screen",
        None => "blocked_until_correlation_sample_available",
    }
}

pub(crate) fn decide_margin_detail_coverage_audit(
    schema_passed: bool,
    raw_rows: i64,
    covered_trade_days: i64,
    open_trade_days: i64,
    covered_trade_day_ratio: f64,
    symbol_coverage_ratio: f64,
    pit_violation_rows: i64,
    missing_source_published_at_rows: i64,
    core_negative_rows: i64,
    missing_year_count: i64,
    correlation_decision: &str,
) -> Value {
    const MIN_MARGINABLE_SYMBOL_COVERAGE: f64 = 0.20;
    const MIN_RAW_ROWS_FOR_P310: i64 = 1_000_000;
    let missing_open_trade_day_count = (open_trade_days - covered_trade_days).max(0);

    let (coverage_status, admission_decision, next_step) = if !schema_passed {
        (
            "schema_missing_or_invalid",
            "apply_schema_before_sync",
            "apply_sql_phase7_margin_detail_source_then_rerun_coverage_audit",
        )
    } else if raw_rows <= 0 {
        (
            "raw_missing",
            "bounded_sync_required_before_coverage_audit",
            "run_bounded_margin_detail_sync_then_coverage_audit",
        )
    } else if pit_violation_rows > 0 {
        (
            "raw_pit_failed",
            "raw_pit_failed",
            "repair_available_at_or_delete_bad_margin_detail_rows_then_rerun_audit",
        )
    } else if missing_source_published_at_rows > 0 {
        (
            "source_publication_time_failed",
            "source_publication_time_failed",
            "backfill_conservative_source_published_at_before_intraday_or_p310_use",
        )
    } else if core_negative_rows > 0 {
        (
            "raw_core_quality_failed",
            "raw_core_quality_failed",
            "inspect_core_negative_margin_detail_rows_then_repair_exclude_or_gate",
        )
    } else if open_trade_days <= 0 || missing_year_count > 0 || missing_open_trade_day_count > 0 {
        (
            "full_history_coverage_failed",
            "full_history_coverage_failed",
            "run_missing_year_quarter_or_trade_day_margin_detail_sync_then_rerun_coverage_audit",
        )
    } else if symbol_coverage_ratio < MIN_MARGINABLE_SYMBOL_COVERAGE
        || raw_rows < MIN_RAW_ROWS_FOR_P310
    {
        (
            "bounded_sample_or_undercovered",
            "bounded_sample_passed_needs_full_history_sync",
            "run_full_history_bounded_margin_detail_sync_then_rerun_coverage_audit",
        )
    } else if correlation_decision != "passed_low_linear_correlation_screen" {
        (
            "correlation_gate_failed_or_requires_review",
            "correlation_readiness_not_passed",
            "complete_moneyflow_liquidity_price_volume_correlation_review_before_p310",
        )
    } else {
        (
            "coverage_correlation_readiness_ready_for_p310_diagnostics",
            "coverage_correlation_readiness_ready_for_p310_diagnostics",
            "run_p310_rankic_group_decay_turnover_capacity_diagnostics",
        )
    };

    json!({
        "coverage_status": coverage_status,
        "admission_decision": admission_decision,
        "p310_status": if admission_decision == "coverage_correlation_readiness_ready_for_p310_diagnostics" {
            "ready_for_p310_diagnostics_only"
        } else {
            "blocked_until_coverage_pit_quality_and_correlation_readiness_pass"
        },
        "wfa_status": "blocked",
        "v19_train_selection": "blocked",
        "raw_rows": raw_rows,
        "covered_trade_days": covered_trade_days,
        "open_trade_days": open_trade_days,
        "missing_open_trade_day_count": missing_open_trade_day_count,
        "covered_trade_day_ratio": covered_trade_day_ratio,
        "symbol_coverage_ratio": symbol_coverage_ratio,
        "pit_violation_rows": pit_violation_rows,
        "missing_source_published_at_rows": missing_source_published_at_rows,
        "core_negative_rows": core_negative_rows,
        "missing_year_count": missing_year_count,
        "correlation_decision": correlation_decision,
        "next_step": next_step,
    })
}

pub(crate) fn decide_shareholder_structure_coverage_audit(
    schema_passed: bool,
    raw_rows: i64,
    symbol_coverage_ratio: f64,
    pit_violation_rows: i64,
    duplicate_source_row_hash_count: i64,
    data_quality_violation_rows: i64,
    missing_year_count: i64,
) -> Value {
    const MIN_BROAD_BASE_SYMBOL_COVERAGE: f64 = 0.70;
    const MIN_RAW_ROWS_FOR_P310: i64 = 250_000;

    let (coverage_status, admission_decision, next_step) = if !schema_passed {
        (
            "schema_missing_or_invalid",
            "apply_schema_before_sync",
            "apply_sql_phase7_shareholder_structure_source_then_rerun_coverage_audit",
        )
    } else if raw_rows <= 0 {
        (
            "raw_missing",
            "bounded_sync_required_before_coverage_audit",
            "run_bounded_shareholder_structure_sync_then_coverage_audit",
        )
    } else if pit_violation_rows > 0 {
        (
            "raw_pit_failed",
            "raw_pit_failed",
            "review_period_snapshot_anomalies_then_repair_exclude_or_gate_before_factor_builder",
        )
    } else if data_quality_violation_rows > 0 {
        (
            "raw_quality_failed",
            "raw_quality_failed",
            "review_ratio_and_interval_anomalies_then_repair_exclude_or_gate_before_factor_builder",
        )
    } else if duplicate_source_row_hash_count > 0 {
        (
            "duplicate_source_rows_failed",
            "duplicate_source_rows_failed",
            "inspect_shareholder_structure_source_row_hash_duplicates_before_feature_builder",
        )
    } else if missing_year_count > 0 {
        (
            "full_history_coverage_failed",
            "full_history_coverage_failed",
            "run_missing_year_shareholder_structure_sync_then_rerun_coverage_audit",
        )
    } else if symbol_coverage_ratio < MIN_BROAD_BASE_SYMBOL_COVERAGE
        || raw_rows < MIN_RAW_ROWS_FOR_P310
    {
        (
            "bounded_sample_or_undercovered",
            "bounded_sample_passed_needs_full_history_sync",
            "run_full_history_bounded_shareholder_structure_sync_then_coverage_audit",
        )
    } else {
        (
            "coverage_readiness_ready_for_p310_diagnostics",
            "coverage_readiness_ready_for_p310_diagnostics",
            "run_p310_rankic_group_decay_turnover_capacity_diagnostics",
        )
    };

    json!({
        "coverage_status": coverage_status,
        "admission_decision": admission_decision,
        "p310_status": if admission_decision == "coverage_readiness_ready_for_p310_diagnostics" {
            "ready_for_p310_diagnostics_only"
        } else {
            "blocked_until_coverage_readiness_passes"
        },
        "wfa_status": "blocked",
        "v19_train_selection": "blocked",
        "raw_rows": raw_rows,
        "symbol_coverage_ratio": symbol_coverage_ratio,
        "pit_violation_rows": pit_violation_rows,
        "data_quality_violation_rows": data_quality_violation_rows,
        "duplicate_source_row_hash_count": duplicate_source_row_hash_count,
        "missing_year_count": missing_year_count,
        "next_step": next_step,
    })
}

pub(crate) fn decide_shareholder_structure_strict_low_fanout_gate(
    schema_passed: bool,
    admissible_rows: i64,
    symbol_coverage_ratio: f64,
    duplicate_source_row_hash_count: i64,
    missing_year_count: i64,
) -> Value {
    const MIN_BROAD_BASE_SYMBOL_COVERAGE: f64 = 0.70;
    const MIN_ADMISSIBLE_ROWS_FOR_P310: i64 = 250_000;

    let (status, admission_decision, next_step) = if !schema_passed {
        (
            "schema_missing_or_invalid",
            "apply_schema_before_strict_low_fanout_gate",
            "apply_sql_phase7_shareholder_structure_source_then_rerun_coverage_audit",
        )
    } else if admissible_rows <= 0 {
        (
            "admissible_rows_missing",
            "bounded_sync_required_before_strict_low_fanout_gate",
            "run_low_fanout_shareholder_structure_sync_then_rerun_coverage_audit",
        )
    } else if duplicate_source_row_hash_count > 0 {
        (
            "duplicate_admissible_rows_failed",
            "duplicate_admissible_rows_failed",
            "inspect_strict_low_fanout_duplicate_source_hashes_before_p310",
        )
    } else if missing_year_count > 0 {
        (
            "full_history_coverage_failed",
            "full_history_coverage_failed",
            "run_missing_year_low_fanout_shareholder_structure_sync_then_rerun_coverage_audit",
        )
    } else if symbol_coverage_ratio < MIN_BROAD_BASE_SYMBOL_COVERAGE
        || admissible_rows < MIN_ADMISSIBLE_ROWS_FOR_P310
    {
        (
            "undercovered_or_too_sparse",
            "bounded_sample_passed_needs_more_admissible_history",
            "increase_strict_low_fanout_admissible_coverage_before_p310",
        )
    } else {
        (
            "strict_low_fanout_ready_for_p310_diagnostics",
            "strict_low_fanout_ready_for_p310_diagnostics",
            "run_p310_rankic_group_decay_turnover_capacity_diagnostics_with_shareholder_structure_gate",
        )
    };

    json!({
        "gate_id": SHAREHOLDER_STRUCTURE_LOW_FANOUT_STRICT_GATE_ID,
        "source_scope": "low_fanout_holder_number_holder_trade",
        "status": status,
        "admission_decision": admission_decision,
        "p310_status": if admission_decision == "strict_low_fanout_ready_for_p310_diagnostics" {
            "ready_for_p310_diagnostics_only"
        } else {
            "blocked_until_strict_low_fanout_gate_passes"
        },
        "wfa_status": "blocked",
        "v19_train_selection": "blocked",
        "admissible_rows": admissible_rows,
        "symbol_coverage_ratio": symbol_coverage_ratio,
        "duplicate_source_row_hash_count": duplicate_source_row_hash_count,
        "missing_year_count": missing_year_count,
        "next_step": next_step,
    })
}

pub(crate) fn shareholder_structure_sync_plan_response(
    start: NaiveDate,
    end: NaiveDate,
    batches: Vec<ShareholderStructureSyncPlanBatch>,
) -> Value {
    const SAFE_FULL_RANGE_UNIT_LIMIT: i64 = 50_000;

    let batch_values = batches
        .iter()
        .map(|batch| {
            let global_ann_date_units = batch.quarter_count * 2;
            let symbol_quarter_units = batch.symbol_count * batch.quarter_count * 2;
            let estimated_units = global_ann_date_units + symbol_quarter_units;
            json!({
                "year": batch.year,
                "start_date": batch.start_date.format("%Y-%m-%d").to_string(),
                "end_date": batch.end_date.format("%Y-%m-%d").to_string(),
                "symbol_count": batch.symbol_count,
                "quarter_count": batch.quarter_count,
                "global_ann_date_units": global_ann_date_units,
                "symbol_quarter_units": symbol_quarter_units,
                "estimated_units": estimated_units,
                "recommended_request": {
                    "start_date": batch.start_date.format("%Y%m%d").to_string(),
                    "end_date": batch.end_date.format("%Y%m%d").to_string(),
                    "data_version_id": format!("shareholder-structure-{}", batch.year),
                    "background": true
                },
                "recommended_low_fanout_request": {
                    "start_date": batch.start_date.format("%Y%m%d").to_string(),
                    "end_date": batch.end_date.format("%Y%m%d").to_string(),
                    "source_filters": ["holder_number", "holder_trade"],
                    "data_version_id": format!("shareholder-structure-{}-low-fanout", batch.year),
                    "background": true
                },
                "recommended_top10_request": {
                    "start_date": batch.start_date.format("%Y%m%d").to_string(),
                    "end_date": batch.end_date.format("%Y%m%d").to_string(),
                    "source_filters": ["top10_holders", "top10_float_holders"],
                    "data_version_id": format!("shareholder-structure-{}-top10", batch.year),
                    "background": true
                }
            })
        })
        .collect::<Vec<_>>();

    let estimated_total_units = batches
        .iter()
        .map(|batch| batch.quarter_count * 2 + batch.symbol_count * batch.quarter_count * 2)
        .sum::<i64>();
    let safe_to_run_full_range = estimated_total_units <= SAFE_FULL_RANGE_UNIT_LIMIT;

    json!({
        "audit_version": "p3.21c-shareholder-structure-sync-plan-v1",
        "source_id": "shareholder_structure",
        "mode": "read_only_bounded_sync_plan",
        "date_range": {
            "start_date": start.format("%Y-%m-%d").to_string(),
            "end_date": end.format("%Y-%m-%d").to_string(),
        },
        "batch_count": batch_values.len(),
        "estimated_total_units": estimated_total_units,
        "safe_full_range_unit_limit": SAFE_FULL_RANGE_UNIT_LIMIT,
        "safe_to_run_full_range": safe_to_run_full_range,
        "recommended_batch_granularity": if safe_to_run_full_range { "full_range" } else { "year" },
        "unit_model": {
            "global_ann_date_units": "quarter_count * 2 for holder_number and holder_trade",
            "symbol_quarter_units": "symbol_count * quarter_count * 2 for top10_holders and top10_floatholders",
            "warning": "plan is read-only and does not call Tushare"
        },
        "batches": batch_values,
        "prohibited": [
            "do_not_run_2014_2026_full_range_without_reviewing_estimated_units",
            "do_not_enter_factor_p310_wfa_until_coverage_readiness_and_anomaly_gates_pass"
        ]
    })
}

pub(crate) fn decide_equity_pledge_coverage_audit(
    schema_passed: bool,
    raw_rows: i64,
    symbol_coverage_ratio: f64,
    pit_violation_rows: i64,
    duplicate_source_row_hash_count: i64,
) -> Value {
    const MIN_BROAD_BASE_SYMBOL_COVERAGE: f64 = 0.30;
    const MIN_RAW_ROWS_FOR_P310: i64 = 10_000;

    let (coverage_status, admission_decision, next_step) = if !schema_passed {
        (
            "schema_missing_or_invalid",
            "apply_schema_before_sync",
            "apply_sql_phase7_equity_pledge_source_then_rerun_coverage_audit",
        )
    } else if raw_rows <= 0 {
        (
            "raw_missing",
            "bounded_sync_required_before_coverage_audit",
            "run_bounded_equity_pledge_pressure_sync_then_coverage_audit",
        )
    } else if pit_violation_rows > 0 {
        (
            "raw_pit_failed",
            "raw_pit_failed",
            "fix_or_delete_bad_equity_pledge_rows_then_rerun_sync_and_audit",
        )
    } else if duplicate_source_row_hash_count > 0 {
        (
            "duplicate_source_rows_failed",
            "duplicate_source_rows_failed",
            "inspect_pledge_detail_source_row_hash_duplicates_before_feature_builder",
        )
    } else if symbol_coverage_ratio < MIN_BROAD_BASE_SYMBOL_COVERAGE
        || raw_rows < MIN_RAW_ROWS_FOR_P310
    {
        (
            "bounded_sample_or_undercovered",
            "bounded_sample_passed_needs_full_history_sync",
            "run_full_history_bounded_equity_pledge_sync_then_coverage_audit",
        )
    } else {
        (
            "coverage_readiness_ready_for_p310_diagnostics",
            "coverage_readiness_ready_for_p310_diagnostics",
            "run_p310_rankic_group_decay_turnover_capacity_diagnostics",
        )
    };

    json!({
        "coverage_status": coverage_status,
        "admission_decision": admission_decision,
        "p310_status": if admission_decision == "coverage_readiness_ready_for_p310_diagnostics" {
            "ready_for_p310_diagnostics_only"
        } else {
            "blocked_until_coverage_readiness_passes"
        },
        "wfa_status": "blocked",
        "v19_train_selection": "blocked",
        "raw_rows": raw_rows,
        "symbol_coverage_ratio": symbol_coverage_ratio,
        "pit_violation_rows": pit_violation_rows,
        "duplicate_source_row_hash_count": duplicate_source_row_hash_count,
        "next_step": next_step,
    })
}

pub(crate) fn decide_futures_price_chain_readiness(
    schema_passed: bool,
    raw_rows: i64,
    mapping_rows: i64,
) -> Value {
    let (schema_status, sync_status, admission_decision, next_step) = if !schema_passed {
        (
            "missing_or_invalid",
            "blocked",
            "apply_schema_before_sync",
            "apply_sql_phase7_futures_price_chain_source_then_rerun_readiness_audit",
        )
    } else if raw_rows == 0 {
        (
            "created",
            "not_started",
            "schema_created_sync_required_before_coverage_audit",
            "run_bounded_futures_price_chain_sync_then_coverage_readiness_audit",
        )
    } else if mapping_rows == 0 {
        (
            "created",
            "raw_synced_mapping_missing",
            "mapping_required_before_feature_or_p310",
            "create_versioned_product_to_industry_mapping_before_factor_backfill",
        )
    } else {
        (
            "created",
            "raw_and_mapping_present",
            "coverage_readiness_audit_required_before_p310",
            "run_year_product_exchange_mapping_market_scope_coverage_audit",
        )
    };

    json!({
        "schema_status": schema_status,
        "sync_status": sync_status,
        "admission_decision": admission_decision,
        "p310_status": "blocked_until_coverage_readiness_passes",
        "wfa_status": "blocked",
        "v19_train_selection": "blocked",
        "raw_rows": raw_rows,
        "mapping_rows": mapping_rows,
        "next_step": next_step,
    })
}

#[cfg(test)]
pub(crate) fn futures_price_chain_product_symbol_from_daily_ts_code(
    ts_code: &str,
) -> Option<String> {
    let value = ts_code.trim();
    let product = value
        .chars()
        .take_while(|ch| ch.is_ascii_alphabetic())
        .collect::<String>();
    let month_code = value
        .chars()
        .skip(product.len())
        .take_while(|ch| ch.is_ascii_digit())
        .collect::<String>();
    if month_code.len() != 4 {
        return None;
    }
    if value.chars().nth(product.len() + month_code.len()) != Some('.') {
        return None;
    }
    futures_price_chain_normalize_raw_product_symbol(&product)
}

pub(crate) fn decide_futures_price_chain_mapping_audit(
    schema_passed: bool,
    raw_product_count: i64,
    mapped_product_count: i64,
    excluded_product_count: i64,
    missing_product_count: i64,
    invalid_interval_rows: i64,
    mapping_pit_violation_rows: i64,
    unsupported_exposure_rows: i64,
    invalid_exclusion_interval_rows: i64,
    exclusion_pit_violation_rows: i64,
) -> Value {
    let covered_product_count = mapped_product_count + excluded_product_count;
    let (mapping_status, admission_decision, next_step) = if !schema_passed {
        (
            "schema_missing_or_invalid",
            "apply_schema_before_mapping_audit",
            "apply_sql_phase7_futures_price_chain_source_then_rerun_mapping_audit",
        )
    } else if raw_product_count == 0 {
        (
            "raw_sync_required",
            "raw_sync_required_before_mapping_audit",
            "run_bounded_futures_price_chain_sync_before_mapping_audit",
        )
    } else if covered_product_count == 0 {
        (
            "mapping_and_exclusion_missing",
            "mapping_required_before_feature_or_p310",
            "create_versioned_product_to_industry_mapping_or_exclusion_gate_before_factor_backfill",
        )
    } else if unsupported_exposure_rows > 0
        || invalid_interval_rows > 0
        || mapping_pit_violation_rows > 0
        || invalid_exclusion_interval_rows > 0
        || exclusion_pit_violation_rows > 0
    {
        (
            "mapping_integrity_failed",
            "mapping_integrity_failed_before_feature_or_p310",
            "repair_mapping_or_exclusion_type_interval_and_available_at_before_coverage_audit",
        )
    } else if missing_product_count > 0 {
        (
            "mapping_coverage_incomplete",
            "mapping_coverage_incomplete_before_feature_or_p310",
            "complete_versioned_mapping_for_remaining_raw_products_or_pre_register_exclusion_gate",
        )
    } else if mapped_product_count == 0 {
        (
            "all_raw_products_excluded",
            "all_products_excluded_no_trainable_price_chain_source",
            "stop_futures_price_chain_factor_source_or_add_evidence_backed_industry_mappings",
        )
    } else {
        (
            "mapping_coverage_ready",
            "coverage_readiness_audit_required_before_p310",
            "run_year_product_exchange_mapping_market_scope_coverage_audit",
        )
    };

    let coverage_ratio = if raw_product_count > 0 {
        covered_product_count as f64 / raw_product_count as f64
    } else {
        0.0
    };

    json!({
        "schema_status": if schema_passed { "created" } else { "missing_or_invalid" },
        "sync_status": if raw_product_count > 0 { "raw_synced" } else { "not_started" },
        "mapping_status": mapping_status,
        "admission_decision": admission_decision,
        "p310_status": "blocked_until_mapping_and_coverage_readiness_pass",
        "wfa_status": "blocked",
        "v19_train_selection": "blocked",
        "raw_product_count": raw_product_count,
        "mapped_product_count": mapped_product_count,
        "excluded_product_count": excluded_product_count,
        "covered_product_count": covered_product_count,
        "missing_product_count": missing_product_count,
        "mapping_coverage_ratio": coverage_ratio,
        "invalid_interval_rows": invalid_interval_rows,
        "mapping_pit_violation_rows": mapping_pit_violation_rows,
        "unsupported_exposure_rows": unsupported_exposure_rows,
        "invalid_exclusion_interval_rows": invalid_exclusion_interval_rows,
        "exclusion_pit_violation_rows": exclusion_pit_violation_rows,
        "next_step": next_step,
    })
}

pub(crate) fn decide_futures_price_chain_coverage_audit(
    schema_passed: bool,
    raw_rows: i64,
    raw_product_count: i64,
    covered_product_count: i64,
    missing_product_count: i64,
    raw_pit_violation_rows: i64,
    failed_sync_attempt_count: i64,
) -> Value {
    let coverage_ratio = if raw_product_count > 0 {
        covered_product_count as f64 / raw_product_count as f64
    } else {
        0.0
    };
    let (coverage_status, admission_decision, next_step) = if !schema_passed {
        (
            "schema_missing_or_invalid",
            "apply_schema_before_coverage_audit",
            "apply_sql_phase7_futures_price_chain_source_then_rerun_coverage_audit",
        )
    } else if raw_rows == 0 {
        (
            "raw_sync_required",
            "raw_sync_required_before_coverage_audit",
            "run_bounded_futures_price_chain_sync_before_coverage_audit",
        )
    } else if raw_pit_violation_rows > 0 {
        (
            "raw_pit_integrity_failed",
            "raw_pit_integrity_failed_before_feature_or_p310",
            "repair_raw_available_at_before_mapping_or_p310",
        )
    } else if failed_sync_attempt_count > 0 {
        (
            "sync_attempt_failures_present",
            "sync_attempt_failures_require_retry_before_feature_or_p310",
            "retry_or_explain_failed_futures_price_chain_sync_attempts_before_p310",
        )
    } else if missing_product_count > 0 {
        (
            "mapping_coverage_incomplete",
            "mapping_coverage_incomplete_before_feature_or_p310",
            "complete_versioned_mapping_for_remaining_raw_products_or_pre_register_exclusion_gate",
        )
    } else {
        (
            "coverage_mapping_ready",
            "coverage_readiness_ready_for_p310_diagnostics",
            "run_p310_rankic_group_decay_turnover_capacity_diagnostics",
        )
    };

    json!({
        "coverage_status": coverage_status,
        "admission_decision": admission_decision,
        "raw_rows": raw_rows,
        "raw_product_count": raw_product_count,
        "covered_product_count": covered_product_count,
        "missing_product_count": missing_product_count,
        "mapping_coverage_ratio": coverage_ratio,
        "raw_pit_violation_rows": raw_pit_violation_rows,
        "failed_sync_attempt_count": failed_sync_attempt_count,
        "p310_status": if admission_decision == "coverage_readiness_ready_for_p310_diagnostics" {
            "ready_for_p310_diagnostics"
        } else {
            "blocked_until_mapping_and_coverage_readiness_pass"
        },
        "wfa_status": "blocked",
        "v19_train_selection": "blocked",
        "next_step": next_step,
    })
}

pub(crate) fn futures_price_chain_coverage_promotion_gate(decision: &Value) -> Value {
    let p310_status = decision
        .get("p310_status")
        .and_then(Value::as_str)
        .unwrap_or("blocked_until_mapping_and_coverage_readiness_pass");
    let p310_ready = p310_status == "ready_for_p310_diagnostics";

    json!({
        "factor_builder": if p310_ready {
            "ready_for_p310_diagnostics"
        } else {
            "blocked_until_coverage_mapping_and_pit_pass"
        },
        "p310_status": p310_status,
        "wfa_status": "blocked",
        "v19_train_selection": "blocked"
    })
}

pub(crate) fn futures_price_chain_raw_product_summary_sql() -> &'static str {
    r#"
    WITH raw_symbol AS (
        SELECT
            'daily' AS endpoint,
            upper(substring(ts_code from '^([A-Za-z]+)[0-9]{4}\.')) AS product_symbol_raw,
            COALESCE(NULLIF(split_part(ts_code, '.', 2), ''), 'UNKNOWN') AS exchange_key,
            trade_date,
            available_at,
            source_published_at
        FROM market_futures_daily
        WHERE substring(ts_code from '^([A-Za-z]+)[0-9]{4}\.') IS NOT NULL
        UNION ALL
        SELECT
            'wsr' AS endpoint,
            upper(substring(symbol from '^[A-Za-z]+')) AS product_symbol_raw,
            COALESCE(NULLIF(trim(exchange), ''), 'UNKNOWN') AS exchange_key,
            trade_date,
            available_at,
            source_published_at
        FROM market_futures_warehouse_receipt
        WHERE substring(symbol from '^[A-Za-z]+') IS NOT NULL
        UNION ALL
        SELECT
            'holding' AS endpoint,
            upper(substring(symbol from '^[A-Za-z]+')) AS product_symbol_raw,
            COALESCE(NULLIF(trim(exchange), ''), 'UNKNOWN') AS exchange_key,
            trade_date,
            available_at,
            source_published_at
        FROM market_futures_holding_rank
        WHERE substring(symbol from '^[A-Za-z]+') IS NOT NULL
    ),
    raw AS (
        SELECT
            endpoint,
            CASE
                WHEN product_symbol_raw = 'PTA' THEN 'TA'
                WHEN length(product_symbol_raw) > 4 AND right(product_symbol_raw, 4) = 'ACTV'
                THEN left(product_symbol_raw, length(product_symbol_raw) - 4)
                WHEN length(product_symbol_raw) > 2 AND right(product_symbol_raw, 1) = 'L'
                THEN left(product_symbol_raw, length(product_symbol_raw) - 1)
                ELSE product_symbol_raw
            END AS product_symbol,
            exchange_key,
            trade_date,
            available_at,
            source_published_at
        FROM raw_symbol
    )
    SELECT
        product_symbol,
        COUNT(*)::bigint AS raw_rows,
        string_agg(DISTINCT endpoint, ',' ORDER BY endpoint) AS endpoints,
        string_agg(DISTINCT exchange_key, ',' ORDER BY exchange_key) AS exchanges,
        MIN(trade_date) AS min_trade_date,
        MAX(trade_date) AS max_trade_date,
        MIN(available_at) AS min_available_at,
        MAX(available_at) AS max_available_at,
        COUNT(*) FILTER (WHERE available_at < trade_date)::bigint AS raw_pit_violation_rows,
        COUNT(*) FILTER (WHERE source_published_at IS NULL)::bigint AS missing_source_published_at_rows
    FROM raw
    GROUP BY product_symbol
    ORDER BY product_symbol
    "#
}

pub(crate) fn futures_price_chain_coverage_breakdown_sql() -> &'static str {
    r#"
    WITH raw_symbol AS (
        SELECT
            'daily' AS endpoint,
            upper(substring(ts_code from '^([A-Za-z]+)[0-9]{4}\.')) AS product_symbol_raw,
            COALESCE(NULLIF(split_part(ts_code, '.', 2), ''), 'UNKNOWN') AS exchange_key,
            trade_date,
            available_at,
            source_published_at
        FROM market_futures_daily
        WHERE substring(ts_code from '^([A-Za-z]+)[0-9]{4}\.') IS NOT NULL
          AND ($1::date IS NULL OR trade_date >= $1::date)
          AND ($2::date IS NULL OR trade_date <= $2::date)
        UNION ALL
        SELECT
            'wsr' AS endpoint,
            upper(substring(symbol from '^[A-Za-z]+')) AS product_symbol_raw,
            COALESCE(NULLIF(trim(exchange), ''), 'UNKNOWN') AS exchange_key,
            trade_date,
            available_at,
            source_published_at
        FROM market_futures_warehouse_receipt
        WHERE substring(symbol from '^[A-Za-z]+') IS NOT NULL
          AND ($1::date IS NULL OR trade_date >= $1::date)
          AND ($2::date IS NULL OR trade_date <= $2::date)
        UNION ALL
        SELECT
            'holding' AS endpoint,
            upper(substring(symbol from '^[A-Za-z]+')) AS product_symbol_raw,
            COALESCE(NULLIF(trim(exchange), ''), 'UNKNOWN') AS exchange_key,
            trade_date,
            available_at,
            source_published_at
        FROM market_futures_holding_rank
        WHERE substring(symbol from '^[A-Za-z]+') IS NOT NULL
          AND ($1::date IS NULL OR trade_date >= $1::date)
          AND ($2::date IS NULL OR trade_date <= $2::date)
    ),
    raw AS (
        SELECT
            endpoint,
            EXTRACT(YEAR FROM trade_date)::int AS trade_year,
            CASE
                WHEN product_symbol_raw = 'PTA' THEN 'TA'
                WHEN length(product_symbol_raw) > 4 AND right(product_symbol_raw, 4) = 'ACTV'
                THEN left(product_symbol_raw, length(product_symbol_raw) - 4)
                WHEN length(product_symbol_raw) > 2 AND right(product_symbol_raw, 1) = 'L'
                THEN left(product_symbol_raw, length(product_symbol_raw) - 1)
                ELSE product_symbol_raw
            END AS product_symbol,
            exchange_key,
            trade_date,
            available_at,
            source_published_at
        FROM raw_symbol
    )
    SELECT
        endpoint,
        trade_year,
        product_symbol,
        exchange_key,
        COUNT(*)::bigint AS raw_rows,
        COUNT(DISTINCT trade_date)::bigint AS trade_date_count,
        MIN(trade_date) AS min_trade_date,
        MAX(trade_date) AS max_trade_date,
        COUNT(*) FILTER (WHERE available_at < trade_date)::bigint AS raw_pit_violation_rows,
        COUNT(*) FILTER (WHERE source_published_at IS NULL)::bigint AS missing_source_published_at_rows
    FROM raw
    GROUP BY endpoint, trade_year, product_symbol, exchange_key
    ORDER BY trade_year, endpoint, product_symbol, exchange_key
    "#
}

pub(crate) fn futures_price_chain_sync_attempt_breakdown_sql() -> &'static str {
    r#"
    WITH futures_calendar AS (
        SELECT DISTINCT trade_date
        FROM market_trade_calendar
        WHERE is_open = true
          AND exchange IN ('SHFE', 'DCE', 'CZCE', 'CFFEX', 'INE')
          AND ($1::date IS NULL OR trade_date >= $1::date)
          AND ($2::date IS NULL OR trade_date <= $2::date)
    ),
    fallback_calendar AS (
        SELECT DISTINCT trade_date
        FROM market_trade_calendar
        WHERE is_open = true
          AND ($1::date IS NULL OR trade_date >= $1::date)
          AND ($2::date IS NULL OR trade_date <= $2::date)
    ),
    open_calendar AS (
        SELECT trade_date FROM futures_calendar
        UNION
        SELECT trade_date FROM fallback_calendar
        WHERE NOT EXISTS (SELECT 1 FROM futures_calendar)
    ),
    attempts AS (
        SELECT
            attempt.*,
            open_calendar.trade_date IS NOT NULL AS is_open_trade_date
        FROM data_sync_attempt attempt
        LEFT JOIN open_calendar
          ON attempt.start_date = open_calendar.trade_date
         AND attempt.end_date = open_calendar.trade_date
        WHERE source IN (
            'futures_price_chain_daily',
            'futures_price_chain_wsr',
            'futures_price_chain_holding'
        )
          AND ($1::date IS NULL OR end_date >= $1::date)
          AND ($2::date IS NULL OR start_date <= $2::date)
    )
    SELECT
        source,
        status,
        COUNT(*) FILTER (WHERE is_open_trade_date)::bigint AS attempt_count,
        COALESCE(SUM(row_count) FILTER (WHERE is_open_trade_date), 0)::bigint AS row_count,
        MIN(start_date) FILTER (WHERE is_open_trade_date) AS min_start_date,
        MAX(end_date) FILTER (WHERE is_open_trade_date) AS max_end_date,
        COUNT(*) FILTER (WHERE is_open_trade_date AND error_message IS NOT NULL)::bigint AS error_attempt_count,
        COUNT(*) FILTER (WHERE NOT is_open_trade_date)::bigint AS non_open_attempt_count,
        COALESCE(SUM(row_count) FILTER (WHERE NOT is_open_trade_date), 0)::bigint AS non_open_row_count
    FROM attempts
    GROUP BY source, status
    ORDER BY source, status
    "#
}

pub(crate) fn futures_price_chain_raw_endpoint_breakdown_sql() -> &'static str {
    r#"
    WITH raw_symbol AS (
        SELECT
            'daily' AS endpoint,
            upper(substring(ts_code from '^([A-Za-z]+)[0-9]{4}\.')) AS product_symbol_raw,
            trade_date,
            available_at,
            source_published_at
        FROM market_futures_daily
        WHERE substring(ts_code from '^([A-Za-z]+)[0-9]{4}\.') IS NOT NULL
        UNION ALL
        SELECT
            'wsr' AS endpoint,
            upper(substring(symbol from '^[A-Za-z]+')) AS product_symbol_raw,
            trade_date,
            available_at,
            source_published_at
        FROM market_futures_warehouse_receipt
        WHERE substring(symbol from '^[A-Za-z]+') IS NOT NULL
        UNION ALL
        SELECT
            'holding' AS endpoint,
            upper(substring(symbol from '^[A-Za-z]+')) AS product_symbol_raw,
            trade_date,
            available_at,
            source_published_at
        FROM market_futures_holding_rank
        WHERE substring(symbol from '^[A-Za-z]+') IS NOT NULL
    ),
    raw AS (
        SELECT
            endpoint,
            CASE
                WHEN product_symbol_raw = 'PTA' THEN 'TA'
                WHEN length(product_symbol_raw) > 4 AND right(product_symbol_raw, 4) = 'ACTV'
                THEN left(product_symbol_raw, length(product_symbol_raw) - 4)
                WHEN length(product_symbol_raw) > 2 AND right(product_symbol_raw, 1) = 'L'
                THEN left(product_symbol_raw, length(product_symbol_raw) - 1)
                ELSE product_symbol_raw
            END AS product_symbol,
            trade_date,
            available_at,
            source_published_at
        FROM raw_symbol
    )
    SELECT
        endpoint,
        COUNT(*)::bigint AS raw_rows,
        COUNT(DISTINCT product_symbol)::bigint AS product_count,
        COUNT(DISTINCT trade_date)::bigint AS trade_date_count,
        MIN(trade_date) AS min_trade_date,
        MAX(trade_date) AS max_trade_date,
        COUNT(*) FILTER (WHERE available_at < trade_date)::bigint AS raw_pit_violation_rows,
        COUNT(*) FILTER (WHERE source_published_at IS NULL)::bigint AS missing_source_published_at_rows
    FROM raw
    GROUP BY endpoint
    ORDER BY endpoint
    "#
}

pub(crate) fn futures_price_chain_mapping_summary_sql() -> &'static str {
    r#"
    SELECT
        COUNT(*)::bigint AS mapping_rows,
        COUNT(DISTINCT upper(trim(product_symbol)))::bigint AS mapping_table_product_count,
        COUNT(*) FILTER (
            WHERE exposure_type NOT IN ('sw_industry', 'stock_symbol')
        )::bigint AS unsupported_exposure_rows,
        COUNT(*) FILTER (
            WHERE valid_to IS NOT NULL AND valid_to < valid_from
        )::bigint AS invalid_interval_rows,
        COUNT(*) FILTER (
            WHERE available_at < valid_from
        )::bigint AS mapping_pit_violation_rows,
        COUNT(*) FILTER (
            WHERE NULLIF(trim(source), '') IS NULL OR evidence = '{}'::jsonb
        )::bigint AS weak_evidence_rows
    FROM market_futures_product_exposure_mapping_pit
    "#
}

pub(crate) fn futures_price_chain_exclusion_summary_sql() -> &'static str {
    r#"
    SELECT
        COUNT(*)::bigint AS exclusion_rows,
        COUNT(DISTINCT upper(trim(product_symbol)))::bigint AS exclusion_table_product_count,
        COUNT(*) FILTER (
            WHERE valid_to IS NOT NULL AND valid_to < valid_from
        )::bigint AS invalid_interval_rows,
        COUNT(*) FILTER (
            WHERE available_at < valid_from
        )::bigint AS exclusion_pit_violation_rows,
        COUNT(*) FILTER (
            WHERE NULLIF(trim(source), '') IS NULL OR evidence = '{}'::jsonb
        )::bigint AS weak_evidence_rows
    FROM market_futures_product_exclusion_gate_pit
    WHERE gate_scope = 'futures_price_chain_factor'
    "#
}

pub(crate) fn futures_price_chain_industry_targets_sql() -> &'static str {
    r#"
    SELECT
        index_code,
        index_name,
        industry_code,
        industry_name,
        COUNT(DISTINCT symbol)::bigint AS current_or_historical_symbols,
        MIN(in_date) AS min_in_date,
        MAX(in_date) AS max_in_date,
        MAX(available_at) AS latest_available_at
    FROM market_stock_industry_membership_pit
    WHERE classification_source = 'SW2021'
      AND industry_level = 'L1'
    GROUP BY index_code, index_name, industry_code, industry_name
    ORDER BY index_code
    "#
}

pub(crate) fn validate_futures_price_chain_mapping_candidate(
    candidate: &FuturesPriceChainMappingCandidate,
    raw_products: &BTreeSet<String>,
    sw2021_l1_targets: &BTreeSet<String>,
) -> FuturesPriceChainMappingCandidateValidation {
    let product_symbol = candidate.product_symbol.trim().to_ascii_uppercase();
    let exposure_type = candidate.exposure_type.trim().to_string();
    let exposure_code = candidate.exposure_code.trim().to_ascii_uppercase();
    let mut errors = Vec::new();
    let mut warnings = Vec::new();

    if product_symbol.is_empty() {
        errors.push("product_symbol_required".to_string());
    } else if !raw_products.contains(&product_symbol) {
        errors.push("unknown_raw_product_symbol".to_string());
    }

    if exposure_type != "sw_industry" {
        if exposure_type == "stock_symbol" {
            errors.push("direct_stock_mapping_requires_separate_evidence_gate".to_string());
        } else {
            errors.push("unsupported_exposure_type".to_string());
        }
    } else if !sw2021_l1_targets.contains(&exposure_code) {
        errors.push("unknown_sw2021_l1_exposure_code".to_string());
    }

    if candidate.direction != -1 && candidate.direction != 1 {
        errors.push("direction_must_be_minus_one_or_one".to_string());
    }
    if !(candidate.weight > 0.0 && candidate.weight <= 1.0) {
        errors.push("weight_must_be_gt_0_and_lte_1".to_string());
    }

    let valid_from = parse_futures_price_chain_mapping_date(&candidate.valid_from);
    let valid_to = candidate
        .valid_to
        .as_deref()
        .map(parse_futures_price_chain_mapping_date)
        .transpose();
    let available_at = parse_futures_price_chain_mapping_date(&candidate.available_at);

    match (&valid_from, &valid_to) {
        (Ok(start), Ok(Some(end))) if end < start => {
            errors.push("valid_to_before_valid_from".to_string())
        }
        _ => {}
    }
    match (&valid_from, &available_at) {
        (Ok(start), Ok(available)) if available < start => {
            errors.push("available_at_before_valid_from".to_string())
        }
        _ => {}
    }
    if valid_from.is_err() {
        errors.push("valid_from_invalid".to_string());
    }
    if valid_to.is_err() {
        errors.push("valid_to_invalid".to_string());
    }
    if available_at.is_err() {
        errors.push("available_at_invalid".to_string());
    }

    if candidate.source.trim().is_empty() {
        errors.push("source_required".to_string());
    }
    if candidate.mapping_version.trim().is_empty() {
        errors.push("mapping_version_required".to_string());
    }
    match &candidate.evidence {
        Value::Object(object) if !object.is_empty() => {}
        _ => errors.push("evidence_required".to_string()),
    }
    if candidate.evidence.get("source_url").is_none()
        && candidate.evidence.get("source_document").is_none()
        && candidate.evidence.get("review_note").is_none()
    {
        warnings.push("evidence_should_include_source_url_or_review_note".to_string());
    }

    FuturesPriceChainMappingCandidateValidation {
        product_symbol,
        exposure_type,
        exposure_code,
        passed: errors.is_empty(),
        errors,
        warnings,
    }
}

pub(crate) fn decide_futures_price_chain_mapping_candidate_validation(
    raw_product_count: i64,
    covered_product_count: i64,
    invalid_row_count: i64,
    missing_product_count: i64,
) -> Value {
    let (admission_decision, next_step) = if raw_product_count == 0 {
        (
            "raw_sync_required_before_mapping_candidate_validation",
            "run_bounded_futures_price_chain_sync_before_mapping_template",
        )
    } else if invalid_row_count > 0 {
        (
            "mapping_candidate_validation_failed",
            "repair_candidate_rows_before_insert_or_review",
        )
    } else if missing_product_count > 0 {
        (
            "mapping_candidate_coverage_incomplete_before_insert",
            "add_candidate_rows_for_all_raw_products_or_pre_register_exclusion_gate",
        )
    } else {
        (
            "mapping_candidate_ready_for_manual_review_before_insert",
            "manual_review_then_insert_versioned_mapping_rows",
        )
    };
    let coverage_ratio = if raw_product_count > 0 {
        covered_product_count as f64 / raw_product_count as f64
    } else {
        0.0
    };

    json!({
        "admission_decision": admission_decision,
        "raw_product_count": raw_product_count,
        "covered_product_count": covered_product_count,
        "missing_product_count": missing_product_count,
        "invalid_row_count": invalid_row_count,
        "mapping_candidate_coverage_ratio": coverage_ratio,
        "write_enabled": false,
        "p310_status": "blocked_until_mapping_insert_and_coverage_readiness_pass",
        "wfa_status": "blocked",
        "v19_train_selection": "blocked",
        "next_step": next_step,
    })
}
