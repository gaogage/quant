    use super::*;

    fn cninfo_operator_evidence_reviewed_pass_manifest_fixture() -> Value {
        let artifact_types = [
            "terms_review_attestation",
            "credential_presence_attestation_without_secret_value",
            "endpoint_dictionary_reference",
            "history_range_attestation",
            "sample_payload_redacted_hash_evidence",
            "symbol_mapping_scope_note",
            "rate_limit_cost_refresh_latency_budget",
        ];
        let artifacts: Vec<Value> = artifact_types
            .iter()
            .enumerate()
            .map(|(idx, artifact_type)| {
                json!({
                    "artifact_id": format!("cninfo-evidence-{:02}", idx + 1),
                    "artifact_type": artifact_type,
                    "owner": "operator",
                    "review_status": "reviewed_pass",
                    "reviewed_at": "2026-06-27T10:00:00Z",
                    "storage_location_type": "external_operator_controlled_redacted_reference",
                    "content_hash": format!("sha256:{:064x}", idx + 1),
                    "redaction_status": "redacted_metadata_only",
                    "source_effective_start_date": "2014-01-01",
                    "source_effective_end_date": "2026-06-27",
                    "pit_relevance": "required_for_cninfo_source_admission",
                    "notes": "redacted evidence metadata only"
                })
            })
            .collect();

        json!({
            "manifest_version": "p3.25g-cninfo-operator-evidence-manifest-v1",
            "source_id": "structured_order_capacity_contract_price_chain_source",
            "candidate_id": "cninfo_data_service",
            "artifacts": artifacts
        })
    }

    #[test]
    fn parse_health_date_accepts_html_and_compact_dates() {
        assert_eq!(
            parse_health_date("2026-05-31").expect("html date"),
            NaiveDate::from_ymd_opt(2026, 5, 31).unwrap()
        );
        assert_eq!(
            parse_health_date("20260531").expect("compact date"),
            NaiveDate::from_ymd_opt(2026, 5, 31).unwrap()
        );
        assert!(parse_health_date("2026/05/31").is_err());
    }

    #[test]
    fn coverage_level_requires_full_range_coverage() {
        assert_eq!(coverage_level(0, 0), "green");
        assert_eq!(coverage_level(10, 10), "green");
        assert_eq!(coverage_level(10, 9), "red");
    }

    #[test]
    fn data_readiness_gate_blocks_required_red_only() {
        let checks = vec![
            json!({"item": "A股日线", "level": "green", "required": true}),
            json!({"item": "涨跌停历史", "level": "red", "required": false}),
            json!({"item": "ML预测", "level": "red", "required": true}),
        ];

        let blocked = data_readiness_blocking_checks(&checks, DataReadinessGate::BlockRequiredRed);

        assert_eq!(blocked.len(), 1);
        assert_eq!(blocked[0]["item"], json!("ML预测"));
    }

    #[test]
    fn data_readiness_strict_gate_blocks_required_yellow() {
        let checks = vec![
            json!({"item": "A股日线", "level": "yellow", "required": true}),
            json!({"item": "可观测性标记", "level": "yellow", "required": false}),
        ];

        let blocked =
            data_readiness_blocking_checks(&checks, DataReadinessGate::BlockRequiredYellow);

        assert_eq!(blocked.len(), 1);
        assert_eq!(blocked[0]["item"], json!("A股日线"));
    }

    #[test]
    fn stale_cleanup_parameters_are_bounded() {
        assert_eq!(stale_cleanup_default_timeout_seconds(None), 3600);
        assert_eq!(stale_cleanup_default_timeout_seconds(Some(10)), 60);
        assert_eq!(stale_cleanup_default_timeout_seconds(Some(100_000)), 86_400);
        assert_eq!(stale_cleanup_limit(None), 100);
        assert_eq!(stale_cleanup_limit(Some(0)), 1);
        assert_eq!(stale_cleanup_limit(Some(10_000)), 1000);
    }

    #[test]
    fn stale_cleanup_terminal_statuses_cover_cancel_requested() {
        assert_eq!(
            stale_sync_task_cleanup_terminal_status("running"),
            Some("failed")
        );
        assert_eq!(
            stale_sync_task_cleanup_terminal_status("cancel_requested"),
            Some("cancelled")
        );
        assert_eq!(
            stale_sync_task_cleanup_action("running"),
            "mark_running_failed"
        );
        assert_eq!(
            stale_sync_task_cleanup_action("cancel_requested"),
            "finalize_cancel_requested"
        );
        assert_eq!(stale_sync_task_cleanup_terminal_status("completed"), None);
        assert_eq!(stale_sync_task_cleanup_action("completed"), "ignore");
    }

    #[test]
    fn event_sync_source_quality_requires_official_source_after_limit_api_earliest_date() {
        let date = NaiveDate::from_ymd_opt(2026, 6, 15).unwrap();

        assert_eq!(
            event_sync_source_quality("limit_daily", Some("tushare:limit_list_d"), date),
            EventSyncSourceQuality::Official
        );
        assert_eq!(
            event_sync_source_quality("limit_daily", Some("derived:limit_existing"), date),
            EventSyncSourceQuality::UnverifiedDerived
        );
        assert_eq!(
            event_sync_source_level("limit_daily", Some("derived:limit_existing"), date),
            "yellow"
        );
    }

    #[test]
    fn event_sync_source_quality_allows_pre_api_limit_derivation_only_before_earliest_date() {
        let pre_api_date = NaiveDate::from_ymd_opt(2017, 1, 3).unwrap();

        assert_eq!(
            event_sync_source_quality("limit_daily", Some("derived:daily_limit"), pre_api_date),
            EventSyncSourceQuality::AcceptedDerived
        );
        assert_eq!(
            event_sync_source_level("limit_daily", Some("derived:daily_limit"), pre_api_date),
            "green"
        );
    }

    #[test]
    fn event_sync_source_quality_flags_suspension_markers_as_unverified() {
        let date = NaiveDate::from_ymd_opt(2026, 6, 15).unwrap();

        assert_eq!(
            event_sync_source_quality("suspension_daily", Some("tushare:suspend_d"), date),
            EventSyncSourceQuality::Official
        );
        assert_eq!(
            event_sync_source_quality("suspension_daily", Some("derived:susp_existing"), date),
            EventSyncSourceQuality::UnverifiedDerived
        );
        assert_eq!(
            event_sync_source_level("suspension_daily", Some("derived:susp_existing"), date),
            "yellow"
        );
    }

    #[test]
    fn event_sync_source_quality_accepts_daily_absence_suspension_derivation() {
        let date = NaiveDate::from_ymd_opt(2015, 7, 9).unwrap();

        assert_eq!(
            event_sync_source_quality(
                "suspension_daily",
                Some("derived:daily_absence_suspension"),
                date
            ),
            EventSyncSourceQuality::AcceptedDerived
        );
        assert_eq!(
            event_sync_source_level(
                "suspension_daily",
                Some("derived:daily_absence_suspension"),
                date
            ),
            "green"
        );
    }

    #[test]
    fn parse_etf_symbols_defaults_when_strategy_field_is_empty() {
        assert_eq!(parse_etf_symbols(None), default_mvo_etfs());
        assert_eq!(
            parse_etf_symbols(Some(json!(["518880.SH", "", " 511010.SH "]))),
            vec!["518880.SH".to_string(), "511010.SH".to_string()]
        );
    }

    #[test]
    fn phase7_coverage_grade_classifies_symbol_breadth() {
        assert_eq!(phase7_coverage_grade(0, 5_000), "missing");
        assert_eq!(phase7_coverage_grade(400, 5_000), "undercovered");
        assert_eq!(phase7_coverage_grade(2_000, 5_000), "partial");
        assert_eq!(phase7_coverage_grade(4_200, 5_000), "broad");
    }

    #[test]
    fn phase7_optional_source_readiness_separates_smoke_from_trainable_coverage() {
        assert_eq!(
            phase7_optional_source_readiness(false, 0, 0, 5_000),
            "schema_missing"
        );
        assert_eq!(
            phase7_optional_source_readiness(true, 0, 0, 5_000),
            "needs_sync"
        );
        assert_eq!(
            phase7_optional_source_readiness(true, 19, 3, 5_000),
            "sample_only_do_not_train"
        );
        assert_eq!(
            phase7_optional_source_readiness(true, 5_000, 2_000, 5_000),
            "partial_feature_candidate"
        );
        assert_eq!(
            phase7_optional_source_readiness(true, 50_000, 4_200, 5_000),
            "ready_for_feature_factory"
        );
    }

    #[test]
    fn phase7_optional_source_sync_limit_is_bounded_for_local_runs() {
        assert_eq!(phase7_optional_source_sync_limit(None), 20);
        assert_eq!(phase7_optional_source_sync_limit(Some(0)), 1);
        assert_eq!(phase7_optional_source_sync_limit(Some(30)), 30);
        assert_eq!(
            phase7_optional_source_sync_limit(Some(10_000)),
            PHASE7_OPTIONAL_SOURCE_SYNC_MAX_SYMBOLS
        );
    }

    #[test]
    fn phase7_optional_source_sync_sources_are_known_deduped_and_defaulted() {
        assert_eq!(
            phase7_optional_source_sync_sources(&[]).expect("default sources"),
            vec!["cashflow", "dividend", "repurchase"]
        );
        assert_eq!(
            phase7_optional_source_sync_sources(&[
                " CashFlow ".to_string(),
                "dividend".to_string(),
                "cashflow".to_string(),
            ])
            .expect("deduped sources"),
            vec!["cashflow", "dividend"]
        );
        assert_eq!(
            phase7_optional_source_sync_sources(&[
                "forecast".to_string(),
                "express".to_string(),
                "disclosure_date".to_string(),
                "forecast".to_string(),
            ])
            .expect("event sources"),
            vec!["forecast", "express", "disclosure_date"]
        );
        assert!(phase7_optional_source_sync_sources(&["unknown".to_string()]).is_err());
    }

    #[test]
    fn phase7_optional_source_tables_include_event_sources() {
        assert_eq!(
            phase7_optional_source_table("forecast"),
            Some("market_stock_forecast")
        );
        assert_eq!(
            phase7_optional_source_table("express"),
            Some("market_stock_express")
        );
        assert_eq!(
            phase7_optional_source_table("disclosure_date"),
            Some("market_stock_disclosure_date")
        );
        assert_eq!(
            phase7_optional_source_table("share_float"),
            Some("market_stock_share_float")
        );
    }

    #[test]
    fn phase7_optional_source_specs_include_financial_and_event_expansion_sources() {
        let specs = phase7_optional_source_specs();
        let sources: Vec<&str> = specs.iter().map(|spec| spec.source).collect();

        assert_eq!(
            sources,
            vec![
                "cashflow",
                "dividend",
                "repurchase",
                "forecast",
                "express",
                "disclosure_date",
                "share_float",
            ]
        );
        assert!(specs
            .iter()
            .any(|spec| spec.table == "market_stock_forecast"
                && spec.next_feature == "event_post_announcement_return_curve_pit_features"));
        assert!(specs
            .iter()
            .any(|spec| spec.table == "market_stock_share_float"
                && spec.next_feature == "unlock_supply_pressure_pit_features"));
    }

    #[test]
    fn phase7_new_alpha_candidate_sources_mark_p315_boundaries() {
        let sources = phase7_new_alpha_candidate_sources();
        let by_source: BTreeMap<&str, &Value> = sources
            .iter()
            .map(|source| {
                (
                    source["source"]
                        .as_str()
                        .expect("candidate source has source"),
                    source,
                )
            })
            .collect();

        assert_eq!(
            by_source["market_margin_regime"]["admission_scope"],
            "regime_or_risk_budget_only"
        );
        assert_eq!(
            by_source["market_moneyflow_hsgt_regime"]["admission_scope"],
            "regime_or_risk_budget_only"
        );
        assert_eq!(
            by_source["industry_prosperity_proxy"]["readiness"],
            "industry_membership_permission_probe_required"
        );
        assert_eq!(
            by_source["block_trade_supply_demand"]["readiness"],
            "stopped_after_p310_economics_weak"
        );
        assert_eq!(
            by_source["equity_pledge_pressure"]["readiness"],
            "permission_smoke_passed_schema_contract_ready"
        );
        assert_eq!(
            by_source["equity_incentive_execution_quality"]["readiness"],
            "schema_and_client_missing"
        );
        assert_eq!(
            by_source["block_trade_supply_demand"]["next_step"],
            "do_not_expand_same_family_shift_to_p320_new_source_admission"
        );
        assert_eq!(
            by_source["equity_pledge_pressure"]["next_step"],
            "review_apply_equity_pledge_schema_then_bounded_sync_plan"
        );
        assert_eq!(
            by_source["industry_prosperity_proxy"]["next_step"],
            "run_industry_membership_permission_smoke_then_schema_available_at_audit"
        );
        assert_eq!(
            by_source["industry_prosperity_proxy"]["why_not_trainable_now"],
            "行业 PIT 原始源接入后仍需通过全历史 membership snapshot/coverage 审计和 P3.10 诊断，不能直接进入训练"
        );
        assert_eq!(
            by_source["industry_prosperity_proxy"]["alpha_admission_gate"]["gate_id"],
            "phase7_industry_membership_market_scope_gate_v1"
        );
        assert_eq!(
            by_source["industry_prosperity_proxy"]["alpha_admission_gate"]
                ["required_universe_profile"],
            "main_chinext_non_st"
        );
        assert_eq!(
            by_source["industry_prosperity_proxy"]["alpha_admission_gate"]["excluded_markets"][0],
            "科创板"
        );
    }

    #[test]
    fn phase7_p319_candidate_admission_tracks_new_sources_and_stop_families() {
        let admission = phase7_p319_candidate_admission_sources(None);
        let candidates = admission["candidates"]
            .as_array()
            .expect("p319 admission has candidates");
        let by_source: BTreeMap<&str, &Value> = candidates
            .iter()
            .map(|source| {
                (
                    source["source_id"]
                        .as_str()
                        .expect("candidate source has source_id"),
                    source,
                )
            })
            .collect();

        assert_eq!(admission["stage"], "P3.24");
        assert_eq!(
            admission["hard_gate"],
            "permission_schema_available_at_first"
        );
        assert_eq!(admission["global_policy"]["pit_required"], true);
        assert_eq!(admission["global_policy"]["no_oos_reverse_tuning"], true);
        assert_eq!(
            admission["global_policy"]["model_algorithm_policy"]
                ["algorithm_is_secondary_to_source_economics"],
            true
        );
        assert_eq!(
            admission["stopped_same_family_sources"][0],
            "industry_prosperity_proxy"
        );
        assert!(admission["stopped_same_family_sources"]
            .as_array()
            .unwrap()
            .iter()
            .any(|source| source == "shareholder_structure_current_low_fanout_sleeve"));
        assert!(admission["stopped_same_family_sources"]
            .as_array()
            .unwrap()
            .iter()
            .any(
                |source| source == "multi_vendor_analyst_revision_current_akshare_cninfo_revision"
            ));

        let p322_inventory = by_source["p322_source_inventory"];
        assert_eq!(
            p322_inventory["admission_decision"],
            "multi_vendor_analyst_revision_stopped_after_p310_shift_to_next_low_correlation_source"
        );
        assert_eq!(
            p322_inventory["next_step"],
            "search_licensed_consensus_revision_or_exchange_announcement_order_capacity_source"
        );
        assert_eq!(
            p322_inventory["ranked_candidates"][0]["source_id"],
            "licensed_broad_base_consensus_revision"
        );
        assert_eq!(
            p322_inventory["ranked_candidates"][0]["status"],
            "source_discovery_required"
        );
        assert_eq!(
            p322_inventory["ranked_candidates"][3]["source_id"],
            "margin_detail_leverage_crowding"
        );
        assert_eq!(
            p322_inventory["ranked_candidates"][3]["status"],
            "stopped_after_p310_economics_failed"
        );

        let announcement = by_source["exchange_announcement_order_capacity_text"];
        assert_eq!(
            announcement["admission_decision"],
            "source_discovery_required_before_schema_or_sync"
        );
        assert_eq!(
            announcement["schema_contract_endpoint"],
            "GET /api/v1/quant/data/exchange-announcement-order-capacity/schema-contract"
        );
        assert_eq!(announcement["pit_required"], true);
        assert_eq!(announcement["factor_builder"], "blocked");
        assert_eq!(announcement["p310_status"], "blocked");
        assert_eq!(announcement["bounded_wfa"], "blocked");
        assert_eq!(announcement["v19_train_selection"], "blocked");

        let margin_detail = by_source["margin_detail_leverage_crowding"];
        assert_eq!(
            margin_detail["admission_decision"],
            "stopped_after_p310_economics_failed"
        );
        assert_eq!(margin_detail["pit_required"], true);
        assert_eq!(
            margin_detail["correlation_status"],
            "completed_but_economics_failed"
        );
        assert_eq!(
            margin_detail["source_discovery_evidence"][0]["diagnostics_summary"]["decision"],
            "do_not_enter_bounded_wfa_or_v19_train_selection"
        );

        let equity = by_source["equity_incentive_execution_quality"];
        assert_eq!(
            equity["admission_decision"],
            "blocked_no_valid_equity_incentive_source"
        );
        assert_eq!(equity["pit_required"], true);
        assert_eq!(
            equity["available_at_policy"],
            "announcement_or_disclosure_date_required_before_event_effective_date"
        );
        assert_eq!(
            equity["blocked_reason"],
            "stk_rewards_is_management_compensation_shareholding_not_equity_incentive_execution_and_stk_reward_is_invalid"
        );
        assert_eq!(
            equity["source_discovery_evidence"][0]["candidate"],
            "tushare:stk_rewards"
        );
        assert_eq!(
            equity["source_discovery_evidence"][0]["status"],
            "rejected_semantic_mismatch"
        );

        let operations = by_source["futures_price_chain"];
        assert_eq!(
            operations["admission_decision"],
            "stopped_after_p310_component_economics_failed"
        );
        assert_eq!(operations["p310_status"], "completed_failed_economics");
        assert_eq!(
            operations["candidate_raw_sources"][0],
            "tushare:fina_mainbz"
        );
        assert_eq!(operations["candidate_raw_sources"][1], "tushare:fut_daily");
        assert_eq!(
            operations["source_discovery_evidence"][0]["status"],
            "stopped_after_p310_economics_weak"
        );
        assert_eq!(
            operations["source_discovery_evidence"][0]["audit_endpoint"],
            "POST /api/v1/quant/data/main-business/available-at-audit"
        );
        assert_eq!(
            operations["source_discovery_evidence"][0]["diagnostics_summary"]["decision"],
            "do_not_enter_bounded_wfa_or_v19_train_selection"
        );
        assert_eq!(
            operations["source_discovery_evidence"][1]["candidate"],
            "tushare:futures_price_chain"
        );
        assert_eq!(
            operations["source_discovery_evidence"][1]["smoke_source"],
            "futures_price_chain"
        );
        assert_eq!(
            operations["source_discovery_evidence"][1]["decision"],
            "stop_futures_price_chain_after_p310_component_economics_failed"
        );
        assert_eq!(
            operations["source_discovery_evidence"][1]["production_smoke"]["status"],
            "available"
        );
        assert_eq!(
            operations["source_discovery_evidence"][1]["diagnostics_summary"]["decision"],
            "do_not_enter_bounded_wfa_or_v19_train_selection"
        );

        let pledge = by_source["equity_pledge_pressure"];
        assert_eq!(
            pledge["admission_decision"],
            "stopped_after_p310_economics_failed"
        );
        assert_eq!(pledge["pit_required"], true);
        assert_eq!(pledge["p310_status"], "completed_failed_economics");
        assert_eq!(
            pledge["diagnostics_summary"]["decision"],
            "do_not_enter_bounded_wfa_or_v19_train_selection"
        );
        assert_eq!(pledge["candidate_raw_sources"][0], "tushare:pledge_stat");
        assert_eq!(pledge["candidate_raw_sources"][1], "tushare:pledge_detail");
        assert_eq!(
            pledge["source_discovery_evidence"][0]["native_available_at_candidate"],
            "not_native_end_date_is_measurement_date"
        );
        assert_eq!(
            pledge["source_discovery_evidence"][0]["status"],
            "production_permission_smoke_passed"
        );
        assert_eq!(
            pledge["source_discovery_evidence"][1]["native_available_at_candidate"],
            "ann_date"
        );
        assert_eq!(
            pledge["source_discovery_evidence"][1]["status"],
            "production_permission_smoke_passed"
        );

        let shareholder = by_source["shareholder_structure"];
        assert_eq!(
            shareholder["admission_decision"],
            "stopped_after_bounded_wfa_train_robustness_failed"
        );
        assert_eq!(
            shareholder["diagnostics_summary"]["wfa_experiment_id"],
            "exp-39d6d820-0073-4a22-889f-e9eb67962785"
        );
        assert_eq!(shareholder["diagnostics_summary"]["stitched_oos"], false);

        let analyst = by_source["broad_analyst_revision"];
        assert_eq!(
            analyst["admission_decision"],
            "blocked_report_rc_current_api_unknown_source_after_permission_smoke"
        );
        assert_eq!(
            analyst["coverage_status"],
            "current_event_bundle_full_history_audit_completed_report_rc_blocked_current_api_unknown_source"
        );
        assert_eq!(analyst["current_tables"][0], "market_stock_forecast");
        assert_eq!(
            analyst["guardrail"],
            "must_be_broad_base_revision_not_sparse_event_post_return_overlay"
        );
        assert_eq!(
            analyst["source_discovery_evidence"][0]["audit_endpoint"],
            "GET /api/v1/quant/data/broad-analyst-revision/audit"
        );
        assert_eq!(
            analyst["source_discovery_evidence"][0]["audit_summary"]["decision"],
            "do_not_enter_p310_wfa_or_v19_train_selection"
        );
        assert_eq!(
            analyst["source_discovery_evidence"][1]["candidate"],
            "tushare:report_rc"
        );
        assert_eq!(
            analyst["source_discovery_evidence"][1]["smoke_source"],
            "report_rc"
        );
        assert_eq!(
            analyst["source_discovery_evidence"][1]["decision"],
            "do_not_build_schema_sync_factor_or_p310_from_report_rc_until_the_callable_api_name_or_permission_path_is_verified"
        );
        assert_eq!(
            analyst["source_discovery_evidence"][1]["production_smoke"]["error_code"],
            "40101"
        );

        let multi_vendor = by_source["multi_vendor_analyst_revision"];
        assert_eq!(
            multi_vendor["admission_decision"],
            "stopped_after_p310_economics_failed"
        );
        assert_eq!(multi_vendor["schema_status"], "completed");
        assert_eq!(
            multi_vendor["sync_status"],
            "full_history_raw_sync_completed"
        );
        assert_eq!(
            multi_vendor["coverage_status"],
            "coverage_pit_quality_correlation_green"
        );
        assert_eq!(multi_vendor["p310_status"], "completed_failed_economics");
        assert_eq!(multi_vendor["bounded_wfa"], "blocked");
        assert_eq!(multi_vendor["v19_train_selection"], "blocked");
        assert_eq!(
            multi_vendor["diagnostics_summary"]["latest_report_id"],
            "exp-0930e5fa-f125-4f22-b9d2-5041da2c44c3"
        );
        assert_eq!(
            multi_vendor["diagnostics_summary"]["passed_horizon_count"],
            0
        );
        assert_eq!(
            multi_vendor["diagnostics_summary"]["daily_weak_day_count"],
            1316
        );
        assert_eq!(
            multi_vendor["source_discovery_evidence"][0]["candidate"],
            "akshare:stock_rank_forecast_cninfo"
        );
        assert_eq!(
            multi_vendor["source_discovery_evidence"][0]["status"],
            "stopped_after_p310_economics_failed"
        );
        assert_eq!(
            multi_vendor["source_discovery_evidence"][0]["native_available_at_candidate"],
            "发布日期"
        );
        assert_eq!(
            multi_vendor["source_discovery_evidence"][1]["candidate"],
            "akshare:stock_research_report_em"
        );
        assert_eq!(
            multi_vendor["source_discovery_evidence"][2]["status"],
            "blocked_snapshot_not_pit_ready"
        );
    }

    #[test]
    fn phase7_p319_candidate_admission_reflects_live_futures_readiness() {
        let readiness = json!({
            "decision": decide_futures_price_chain_readiness(true, 6349, 0),
            "tables": []
        });
        let admission = phase7_p319_candidate_admission_sources(Some(&readiness));
        let candidates = admission["candidates"]
            .as_array()
            .expect("p319 admission has candidates");
        let operations = candidates
            .iter()
            .find(|candidate| {
                candidate["source_id"]
                    .as_str()
                    .map(|source| source == "futures_price_chain")
                    .unwrap_or(false)
            })
            .expect("operations candidate");

        assert_eq!(
            operations["admission_decision"],
            "mapping_required_before_feature_or_p310"
        );
        assert_eq!(operations["sync_status"], "raw_synced_mapping_missing");
        assert_eq!(operations["wfa_status"], "blocked");
        assert_eq!(operations["v19_train_selection"], "blocked");
        assert_eq!(
            operations["source_discovery_evidence"][1]["status"],
            "raw_synced_mapping_missing"
        );
        assert_eq!(operations["source_discovery_evidence"][1]["raw_rows"], 6349);
        assert_eq!(
            operations["futures_price_chain_readiness"]["decision"]["mapping_rows"],
            0
        );
    }

    #[test]
    fn broad_analyst_revision_audit_blocks_available_at_rule_violations() {
        let decision = decide_broad_analyst_revision_audit(1, 0.90, 0.80, 0.70, 0.50);

        assert!(!decision.passed);
        assert_eq!(decision.status, "blocked_available_at_rule_violation");
        assert_eq!(
            decision.admission_decision,
            "blocked_broad_analyst_revision_available_at_rule_failed"
        );
        assert_eq!(decision.p310_status, "not_started");
    }

    #[test]
    fn broad_analyst_revision_audit_stops_sparse_revision_semantics() {
        let decision = decide_broad_analyst_revision_audit(0, 0.56, 0.31, 0.11, 0.06);

        assert!(!decision.passed);
        assert_eq!(decision.status, "blocked_sparse_revision_semantics");
        assert_eq!(
            decision.readiness,
            "stopped_current_raw_bundle_revision_semantics_too_sparse"
        );
        assert_eq!(
            decision.admission_decision,
            "stopped_broad_analyst_revision_current_raw_bundle_after_audit_sparse_revision_semantics"
        );
    }

    #[test]
    fn broad_analyst_revision_audit_treats_narrow_forecast_as_sparse_bundle() {
        let decision = decide_broad_analyst_revision_audit(0, 0.56, 0.23, 0.11, 0.06);

        assert!(!decision.passed);
        assert_eq!(decision.status, "blocked_sparse_revision_semantics");
        assert_eq!(
            decision.blocked_reason,
            "true_forecast_revision_events_are_too_sparse_and_would_degenerate_into_event_overlay"
        );
    }

    #[test]
    fn phase7_block_trade_status_reports_bounded_sample_coverage() {
        let sources = phase7_new_alpha_candidate_sources_with_block_trade_status(
            phase7_new_alpha_candidate_sources(),
            &Phase7BlockTradeSourceAudit {
                data_rows: 2168,
                symbols: 1000,
                covered_trade_days: 14,
                open_days_in_range: 14,
                min_trade_date: Some(NaiveDate::from_ymd_opt(2026, 6, 1).unwrap()),
                latest_trade_date: Some(NaiveDate::from_ymd_opt(2026, 6, 18).unwrap()),
                min_available_at: Some(NaiveDate::from_ymd_opt(2026, 6, 2).unwrap()),
                latest_available_at: Some(NaiveDate::from_ymd_opt(2026, 6, 19).unwrap()),
                pit_violation_rows: 0,
            },
        );
        let by_source: BTreeMap<&str, &Value> = sources
            .iter()
            .map(|source| {
                (
                    source["source"]
                        .as_str()
                        .expect("candidate source has source"),
                    source,
                )
            })
            .collect();
        let block_trade = by_source["block_trade_supply_demand"];

        assert_eq!(
            block_trade["readiness"],
            "stopped_after_p310_economics_weak"
        );
        assert_eq!(
            block_trade["raw_source_readiness"],
            "bounded_sample_ready_needs_history_coverage"
        );
        assert_eq!(
            block_trade["next_step"],
            "do_not_expand_same_family_shift_to_p320_new_source_admission"
        );
        assert_eq!(block_trade["raw_source_status"]["data_rows"], 2168);
        assert_eq!(block_trade["raw_source_status"]["covered_trade_days"], 14);
        assert_eq!(block_trade["raw_source_status"]["open_days_in_range"], 14);
        assert_eq!(block_trade["raw_source_status"]["pit_violation_rows"], 0);
    }

    #[test]
    fn phase7_block_trade_status_blocks_pit_violations() {
        let sources = phase7_new_alpha_candidate_sources_with_block_trade_status(
            phase7_new_alpha_candidate_sources(),
            &Phase7BlockTradeSourceAudit {
                data_rows: 10,
                symbols: 5,
                covered_trade_days: 3,
                open_days_in_range: 3,
                min_trade_date: Some(NaiveDate::from_ymd_opt(2026, 6, 1).unwrap()),
                latest_trade_date: Some(NaiveDate::from_ymd_opt(2026, 6, 3).unwrap()),
                min_available_at: Some(NaiveDate::from_ymd_opt(2026, 6, 1).unwrap()),
                latest_available_at: Some(NaiveDate::from_ymd_opt(2026, 6, 4).unwrap()),
                pit_violation_rows: 2,
            },
        );
        let by_source: BTreeMap<&str, &Value> = sources
            .iter()
            .map(|source| {
                (
                    source["source"]
                        .as_str()
                        .expect("candidate source has source"),
                    source,
                )
            })
            .collect();
        let block_trade = by_source["block_trade_supply_demand"];

        assert_eq!(block_trade["readiness"], "raw_source_pit_failed");
        assert_eq!(
            block_trade["next_step"],
            "repair_available_at_before_any_diagnostics"
        );
        assert_eq!(block_trade["raw_source_status"]["pit_violation_rows"], 2);
    }

    #[test]
    fn phase7_industry_membership_status_requires_bounded_sync_before_factor_design() {
        let sources = phase7_new_alpha_candidate_sources_with_industry_membership_status(
            phase7_new_alpha_candidate_sources(),
            &Phase7IndustryMembershipSourceAudit {
                data_rows: 0,
                symbols: 0,
                index_codes: 0,
                current_active_stock_symbols: 0,
                current_covered_stock_symbols: 0,
                min_in_date: None,
                latest_in_date: None,
                min_out_date: None,
                latest_out_date: None,
                min_available_at: None,
                latest_available_at: None,
                pit_violation_rows: 0,
                invalid_interval_rows: 0,
                duplicate_key_rows: 0,
            },
        );
        let by_source: BTreeMap<&str, &Value> = sources
            .iter()
            .map(|source| {
                (
                    source["source"]
                        .as_str()
                        .expect("candidate source has source"),
                    source,
                )
            })
            .collect();
        let industry = by_source["industry_prosperity_proxy"];

        assert_eq!(
            industry["readiness"],
            "industry_membership_schema_ready_needs_bounded_sync"
        );
        assert_eq!(
            industry["next_step"],
            "run_bounded_industry_membership_sync"
        );
    }

    #[test]
    fn phase7_industry_membership_status_blocks_interval_and_pit_violations() {
        let sources = phase7_new_alpha_candidate_sources_with_industry_membership_status(
            phase7_new_alpha_candidate_sources(),
            &Phase7IndustryMembershipSourceAudit {
                data_rows: 10,
                symbols: 8,
                index_codes: 1,
                current_active_stock_symbols: 10,
                current_covered_stock_symbols: 10,
                min_in_date: Some(NaiveDate::from_ymd_opt(2007, 7, 3).unwrap()),
                latest_in_date: Some(NaiveDate::from_ymd_opt(2025, 8, 13).unwrap()),
                min_out_date: Some(NaiveDate::from_ymd_opt(2009, 6, 1).unwrap()),
                latest_out_date: Some(NaiveDate::from_ymd_opt(2020, 1, 1).unwrap()),
                min_available_at: Some(NaiveDate::from_ymd_opt(2007, 7, 3).unwrap()),
                latest_available_at: Some(NaiveDate::from_ymd_opt(2025, 8, 13).unwrap()),
                pit_violation_rows: 0,
                invalid_interval_rows: 1,
                duplicate_key_rows: 0,
            },
        );
        let by_source: BTreeMap<&str, &Value> = sources
            .iter()
            .map(|source| {
                (
                    source["source"]
                        .as_str()
                        .expect("candidate source has source"),
                    source,
                )
            })
            .collect();
        let industry = by_source["industry_prosperity_proxy"];

        assert_eq!(
            industry["readiness"],
            "industry_membership_raw_source_pit_failed"
        );
        assert_eq!(
            industry["next_step"],
            "repair_industry_membership_intervals_before_factor_design"
        );
    }

    #[test]
    fn phase7_industry_membership_status_blocks_current_coverage_under_threshold() {
        let sources = phase7_new_alpha_candidate_sources_with_industry_membership_status(
            phase7_new_alpha_candidate_sources(),
            &Phase7IndustryMembershipSourceAudit {
                data_rows: 10,
                symbols: 8,
                index_codes: 1,
                current_active_stock_symbols: 100,
                current_covered_stock_symbols: 80,
                min_in_date: Some(NaiveDate::from_ymd_opt(2007, 7, 3).unwrap()),
                latest_in_date: Some(NaiveDate::from_ymd_opt(2025, 8, 13).unwrap()),
                min_out_date: Some(NaiveDate::from_ymd_opt(2009, 6, 1).unwrap()),
                latest_out_date: Some(NaiveDate::from_ymd_opt(2020, 1, 1).unwrap()),
                min_available_at: Some(NaiveDate::from_ymd_opt(2007, 7, 3).unwrap()),
                latest_available_at: Some(NaiveDate::from_ymd_opt(2025, 8, 13).unwrap()),
                pit_violation_rows: 0,
                invalid_interval_rows: 0,
                duplicate_key_rows: 0,
            },
        );
        let by_source: BTreeMap<&str, &Value> = sources
            .iter()
            .map(|source| {
                (
                    source["source"]
                        .as_str()
                        .expect("candidate source has source"),
                    source,
                )
            })
            .collect();
        let industry = by_source["industry_prosperity_proxy"];

        assert_eq!(
            industry["readiness"],
            "industry_membership_current_coverage_undercovered"
        );
        assert_eq!(
            industry["next_step"],
            "run_full_l1_membership_sync_or_repair_missing_symbols"
        );
        assert_eq!(
            industry["raw_source_status"]["current_active_stock_symbols"],
            100
        );
        assert_eq!(
            industry["raw_source_status"]["current_covered_stock_symbols"],
            80
        );
        assert_eq!(
            industry["raw_source_status"]["current_missing_stock_symbols"],
            20
        );
        assert_eq!(
            industry["raw_source_status"]["current_stock_coverage_ratio"],
            json!(0.8)
        );
    }

    #[test]
    fn phase7_industry_membership_snapshot_readiness_blocks_multi_membership_before_coverage() {
        assert_eq!(
            phase7_industry_membership_snapshot_readiness(100, 99, 1, 2, 0, 0, 0, 0),
            "snapshot_multi_membership_blocked"
        );
        assert_eq!(
            phase7_industry_membership_snapshot_readiness(100, 94, 6, 0, 0, 0, 0, 0),
            "snapshot_coverage_gaps_need_review"
        );
        assert_eq!(
            phase7_industry_membership_snapshot_readiness(100, 100, 0, 0, 0, 0, 0, 0),
            "snapshot_ready_for_p310_diagnostics"
        );
    }

    #[test]
    fn phase7_industry_membership_snapshot_sql_uses_pit_interval_contract() {
        let sql = phase7_industry_membership_snapshot_summary_sql();

        assert!(sql.contains("DATE '2021-12-13'"));
        assert!(sql.contains("THEN 'SW2014'"));
        assert!(sql.contains("ELSE 'SW2021'"));
        assert!(sql.contains("membership.available_at <= days.trade_date"));
        assert!(sql.contains("membership.exit_available_at > days.trade_date"));
        assert!(!sql.contains("market_stock.industry"));
    }

    #[test]
    fn phase7_industry_membership_breakdown_sql_uses_same_source_version_gate() {
        for sql in [
            phase7_industry_membership_year_breakdown_sql(),
            phase7_industry_membership_market_breakdown_sql(),
        ] {
            assert!(sql.contains("DATE '2021-12-13'"));
            assert!(sql.contains("THEN 'SW2014'"));
            assert!(sql.contains("ELSE 'SW2021'"));
            assert!(sql.contains("membership.available_at <= days.trade_date"));
            assert!(sql.contains("membership.exit_available_at > days.trade_date"));
            assert!(!sql.contains("market_stock.industry"));
        }
    }

    #[test]
    fn phase7_industry_membership_market_scope_gate_requires_high_coverage_and_no_multi_membership()
    {
        assert!(phase7_industry_membership_market_scope_eligible(
            Some(0.995),
            0
        ));
        assert!(!phase7_industry_membership_market_scope_eligible(
            Some(0.9949),
            0
        ));
        assert!(!phase7_industry_membership_market_scope_eligible(
            Some(0.999),
            1
        ));
    }

    #[test]
    fn p315_market_level_source_status_marks_stale_and_builds_sync_payload() {
        let mut stats = BTreeMap::new();
        stats.insert(
            "market_margin_regime".to_string(),
            Phase7MarketLevelSourceAudit {
                data_rows: 5_846,
                min_trade_date: Some(NaiveDate::from_ymd_opt(2016, 1, 4).unwrap()),
                latest_trade_date: Some(NaiveDate::from_ymd_opt(2026, 5, 29).unwrap()),
                open_day_lag: Some(14),
            },
        );

        let sources = phase7_new_alpha_candidate_sources_with_market_status(
            &stats,
            &BTreeMap::new(),
            NaiveDate::from_ymd_opt(2026, 6, 20).unwrap(),
        );
        let by_source: BTreeMap<&str, &Value> = sources
            .iter()
            .map(|source| {
                (
                    source["source"]
                        .as_str()
                        .expect("candidate source has source"),
                    source,
                )
            })
            .collect();
        let margin = by_source["market_margin_regime"];

        assert_eq!(margin["admission_scope"], "regime_or_risk_budget_only");
        assert_eq!(margin["readiness"], "market_level_stale_needs_sync");
        assert_eq!(margin["market_data_status"]["data_rows"], 5_846);
        assert_eq!(margin["market_data_status"]["min_trade_date"], "2016-01-04");
        assert_eq!(
            margin["market_data_status"]["latest_trade_date"],
            "2026-05-29"
        );
        assert_eq!(margin["market_data_status"]["open_day_lag"], 14);
        assert_eq!(margin["sync_task_payload"]["dataset"], "margin");
        assert_eq!(margin["sync_task_payload"]["source"], "tushare");
        assert_eq!(margin["sync_task_payload"]["start_date"], "20260530");
        assert_eq!(margin["sync_task_payload"]["end_date"], "20260620");
        assert_eq!(margin["sync_task_payload"]["background"], true);
    }

    #[test]
    fn p315_market_level_status_reports_zero_row_upstream_unavailable_after_sync_attempt() {
        let mut stats = BTreeMap::new();
        stats.insert(
            "market_moneyflow_hsgt_regime".to_string(),
            Phase7MarketLevelSourceAudit {
                data_rows: 2_339,
                min_trade_date: Some(NaiveDate::from_ymd_opt(2016, 1, 4).unwrap()),
                latest_trade_date: Some(NaiveDate::from_ymd_opt(2026, 5, 29).unwrap()),
                open_day_lag: Some(14),
            },
        );
        let mut sync_tasks = BTreeMap::new();
        sync_tasks.insert(
            "market_moneyflow_hsgt_regime".to_string(),
            Phase7MarketLevelSyncAudit {
                task_id: "dv-p315-hsgt-20260620".to_string(),
                task_type: "moneyflow_hsgt".to_string(),
                start_date: Some(NaiveDate::from_ymd_opt(2026, 5, 30).unwrap()),
                end_date: Some(NaiveDate::from_ymd_opt(2026, 6, 20).unwrap()),
                status: "completed".to_string(),
                total_count: 0,
                success_count: 0,
                failed_count: 0,
                error_message: None,
                completed_at: None,
            },
        );

        let sources = phase7_new_alpha_candidate_sources_with_market_status(
            &stats,
            &sync_tasks,
            NaiveDate::from_ymd_opt(2026, 6, 20).unwrap(),
        );
        let by_source: BTreeMap<&str, &Value> = sources
            .iter()
            .map(|source| {
                (
                    source["source"]
                        .as_str()
                        .expect("candidate source has source"),
                    source,
                )
            })
            .collect();
        let hsgt = by_source["market_moneyflow_hsgt_regime"];

        assert_eq!(
            hsgt["readiness"],
            "market_level_upstream_zero_rows_unavailable"
        );
        assert_eq!(hsgt["market_data_status"]["freshness_gate"], "failed");
        assert_eq!(hsgt["last_sync_task"]["task_id"], "dv-p315-hsgt-20260620");
        assert_eq!(
            hsgt["sync_remediation"]["status"],
            "not_retriable_until_upstream_resolved"
        );
    }

    #[test]
    fn p315_market_level_sync_task_datasets_are_supported() {
        assert_eq!(
            phase7_market_level_sync_dataset("market_margin_regime"),
            Some("margin")
        );
        assert_eq!(
            phase7_market_level_sync_dataset("market_moneyflow_hsgt_regime"),
            Some("moneyflow_hsgt")
        );
        assert_eq!(
            phase7_market_level_sync_dataset("industry_prosperity_proxy"),
            None
        );
    }

    #[test]
    fn phase7_optional_source_sync_plan_only_defaults_to_safe_mode() {
        assert!(phase7_optional_source_sync_plan_only(None));
        assert!(phase7_optional_source_sync_plan_only(Some(true)));
        assert!(!phase7_optional_source_sync_plan_only(Some(false)));
    }

    #[test]
    fn phase7_optional_source_batch_size_and_count_are_bounded() {
        assert_eq!(phase7_optional_source_batch_size(None), 20);
        assert_eq!(phase7_optional_source_batch_size(Some(0)), 1);
        assert_eq!(phase7_optional_source_batch_size(Some(30)), 30);
        assert_eq!(phase7_optional_source_batch_size(Some(10_000)), 200);

        assert_eq!(phase7_optional_source_batch_count(None), 1);
        assert_eq!(phase7_optional_source_batch_count(Some(0)), 1);
        assert_eq!(phase7_optional_source_batch_count(Some(3)), 3);
        assert_eq!(phase7_optional_source_batch_count(Some(10_000)), 10);
    }

    #[test]
    fn phase7_optional_source_batch_offsets_are_resumable() {
        let offsets = phase7_optional_source_batch_offsets(5, 20, 3);
        assert_eq!(offsets, vec![5, 25, 45]);
        assert_eq!(phase7_optional_source_batch_next_offset(&offsets, 20), 65);
    }

    #[test]
    fn phase7_optional_source_batch_resume_offset_resets_after_execution() {
        let offsets = phase7_optional_source_batch_offsets(0, 20, 3);
        assert_eq!(
            phase7_optional_source_batch_recommended_resume_offset(true, &offsets, 20),
            60
        );
        assert_eq!(
            phase7_optional_source_batch_recommended_resume_offset(false, &offsets, 20),
            0
        );
    }

    #[test]
    fn phase7_optional_source_batch_launches_background_only_when_executing() {
        assert!(!phase7_optional_source_batch_child_background(true));
        assert!(phase7_optional_source_batch_child_background(false));
    }

    #[test]
    fn phase7_coverage_runner_profile_is_bounded_and_plan_only_by_default() {
        assert_eq!(phase7_coverage_runner_profile(None), "local_mac_safe");
        assert_eq!(phase7_coverage_runner_batch_size(None), 50);
        assert_eq!(phase7_coverage_runner_batch_size(Some(10_000)), 100);
        assert_eq!(phase7_coverage_runner_batch_count(None), 4);
        assert_eq!(phase7_coverage_runner_batch_count(Some(10_000)), 10);
        assert_eq!(phase7_coverage_runner_plan_only(None), true);
        assert_eq!(
            phase7_coverage_runner_sources(&[]).unwrap(),
            vec!["cashflow", "dividend", "financial"]
        );
        assert!(phase7_coverage_runner_sources(&["repurchase".to_string()]).is_err());
    }

    #[test]
    fn phase7_bounded_task_id_keeps_database_varchar64_contract() {
        let short = bounded_phase7_task_id(&["dv-phase7", "cashflow", "b001"]);
        assert_eq!(short, "dv-phase7-cashflow-b001");
        assert!(short.len() <= 64);

        let long = bounded_phase7_task_id(&[
            "dv-phase7-ff-full-auto-2016-20260526a",
            "r001",
            "optional",
            "cashflow",
            "b001",
        ]);
        assert!(long.len() <= 64, "task id length was {}", long.len());
        assert!(long.starts_with("dv-phase7-ff-full-auto-2016-20260526a-r001"));
        assert_ne!(
            long,
            "dv-phase7-ff-full-auto-2016-20260526a-r001-optional-cashflow-b001"
        );
    }

    #[test]
    fn phase7_coverage_runner_sources_include_bounded_financial_sync() {
        assert_eq!(
            phase7_coverage_runner_sources(&[
                " financial ".to_string(),
                "cashflow".to_string(),
                "financial".to_string(),
            ])
            .expect("financial source accepted"),
            vec!["financial", "cashflow"]
        );
    }

    #[test]
    fn phase7_coverage_runner_sources_include_bounded_event_sync() {
        assert_eq!(
            phase7_coverage_runner_sources(&[
                " forecast ".to_string(),
                "express".to_string(),
                "disclosure_date".to_string(),
                "forecast".to_string(),
            ])
            .expect("event sources accepted"),
            vec!["forecast", "express", "disclosure_date"]
        );
        assert!(phase7_coverage_runner_sources(&["repurchase".to_string()]).is_err());
        assert!(phase7_coverage_runner_sources(&["share_float".to_string()]).is_err());
    }

    #[test]
    fn share_float_coverage_runner_uses_float_date_chunks_and_safe_defaults() {
        assert_eq!(phase7_share_float_chunk_granularity(None), "year");
        assert_eq!(
            phase7_share_float_chunk_granularity(Some(" month ")),
            "month"
        );
        assert_eq!(
            phase7_share_float_chunk_granularity(Some("quarter")),
            "quarter"
        );
        assert_eq!(phase7_share_float_chunk_granularity(Some("week")), "year");
        assert!(phase7_share_float_coverage_plan_only(None));
        assert_eq!(phase7_share_float_coverage_max_chunks(None), 16);
        assert_eq!(phase7_share_float_coverage_max_chunks(Some(10_000)), 64);

        let start = NaiveDate::from_ymd_opt(2014, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2015, 5, 31).unwrap();
        let chunks = phase7_share_float_date_chunks(start, end, "year", 16);

        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].0, NaiveDate::from_ymd_opt(2014, 1, 1).unwrap());
        assert_eq!(chunks[0].1, NaiveDate::from_ymd_opt(2014, 12, 31).unwrap());
        assert_eq!(chunks[1].0, NaiveDate::from_ymd_opt(2015, 1, 1).unwrap());
        assert_eq!(chunks[1].1, NaiveDate::from_ymd_opt(2015, 5, 31).unwrap());
    }

    #[test]
    fn share_float_readiness_sql_audits_float_date_and_pit_leaks() {
        let sql = phase7_share_float_readiness_sql();

        assert!(sql.contains("MIN(float_date)"));
        assert!(sql.contains("MAX(float_date)"));
        assert!(sql.contains("MIN(available_at)"));
        assert!(sql.contains("MAX(available_at)"));
        assert!(sql.contains("COUNT(*) FILTER (WHERE available_at > float_date)"));
        assert!(sql.contains("float_date BETWEEN $1 AND $2"));
        assert!(sql.contains("available_at <= $2"));
    }

    #[test]
    fn share_float_readiness_requires_requested_float_date_span() {
        let start = NaiveDate::from_ymd_opt(2014, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 6, 19).unwrap();
        let expected_days = phase7_share_float_expected_days(start, end);
        let smoke_covered_days = phase7_share_float_covered_days_from_windows(
            vec![(
                NaiveDate::from_ymd_opt(2026, 6, 19).unwrap(),
                NaiveDate::from_ymd_opt(2026, 6, 19).unwrap(),
            )],
            start,
            end,
        );
        let smoke_only =
            phase7_share_float_feature_readiness(10, smoke_covered_days, expected_days, 0);

        assert_eq!(smoke_only, "needs_float_date_backfill");

        let full_covered_days = phase7_share_float_covered_days_from_windows(
            vec![
                (start, NaiveDate::from_ymd_opt(2015, 12, 31).unwrap()),
                (NaiveDate::from_ymd_opt(2016, 1, 1).unwrap(), end),
            ],
            start,
            end,
        );
        let full_span =
            phase7_share_float_feature_readiness(50_000, full_covered_days, expected_days, 0);
        assert_eq!(full_span, "ready_for_pit_feature_factory");

        let full_span_with_late_rows =
            phase7_share_float_feature_readiness(50_000, full_covered_days, expected_days, 12);
        assert_eq!(
            full_span_with_late_rows,
            "ready_for_pit_feature_factory_with_late_exclusion"
        );
    }

    #[test]
    fn phase7_coverage_runner_auto_continue_targets_full_coverage_with_bounds() {
        assert!(!phase7_coverage_runner_auto_continue(None));
        assert!(phase7_coverage_runner_auto_continue(Some(true)));

        assert_eq!(phase7_coverage_runner_max_rounds(None, false), 1);
        assert_eq!(
            phase7_coverage_runner_max_rounds(None, true),
            PHASE7_COVERAGE_RUNNER_MAX_ROUNDS
        );
        assert_eq!(phase7_coverage_runner_max_rounds(Some(0), true), 1);
        assert_eq!(phase7_coverage_runner_max_rounds(Some(12), true), 12);
        assert_eq!(phase7_coverage_runner_max_rounds(Some(1_000), true), 20);

        assert_eq!(phase7_coverage_runner_target_ratio(None), 1.0);
        assert_eq!(phase7_coverage_runner_target_ratio(Some(0.10)), 0.30);
        assert_eq!(phase7_coverage_runner_target_ratio(Some(0.75)), 0.75);
        assert_eq!(phase7_coverage_runner_target_ratio(Some(2.0)), 1.0);
        assert_eq!(phase7_coverage_runner_target_ratio(Some(f64::NAN)), 1.0);
    }

    #[test]
    fn phase7_coverage_runner_autopilot_uses_round_budget_not_manual_batches() {
        assert!(phase7_coverage_runner_should_build_immediate_batches(
            true, true
        ));
        assert!(phase7_coverage_runner_should_build_immediate_batches(
            true, false
        ));
        assert!(phase7_coverage_runner_should_build_immediate_batches(
            false, false
        ));
        assert!(!phase7_coverage_runner_should_build_immediate_batches(
            false, true
        ));
        assert_eq!(phase7_coverage_autopilot_batch_offsets(3), vec![0, 0, 0]);
    }

    #[test]
    fn phase7_optional_source_symbol_resolver_uses_attempt_ledger() {
        let sql = phase7_optional_source_uncovered_symbols_sql("cashflow").expect("cashflow sql");

        assert!(sql.contains("data_sync_attempt attempt"));
        assert!(sql.contains("attempt.source = 'cashflow'"));
        assert!(sql.contains("attempt.status = 'completed'"));
        assert!(sql.contains("attempt.symbol = stock.symbol"));
        assert!(sql.contains("attempt.start_date <= $1"));
        assert!(sql.contains("attempt.end_date >= $2"));
        assert!(sql.contains("OFFSET $3 LIMIT $4"));
    }

    #[test]
    fn phase7_event_source_symbol_resolver_uses_attempt_ledger() {
        for source in ["forecast", "express", "disclosure_date"] {
            let sql = phase7_optional_source_uncovered_symbols_sql(source).expect("event sql");

            assert!(sql.contains("data_sync_attempt attempt"));
            assert!(sql.contains(&format!("attempt.source = '{}'", source)));
            assert!(sql.contains("attempt.status = 'completed'"));
            assert!(sql.contains("attempt.symbol = stock.symbol"));
            assert!(sql.contains("attempt.start_date <= $1"));
            assert!(sql.contains("attempt.end_date >= $2"));
            assert!(sql.contains("OFFSET $3 LIMIT $4"));
        }
    }

    #[test]
    fn phase7_financial_symbol_resolver_uses_attempt_ledger() {
        let sql = phase7_financial_uncovered_symbols_sql();

        assert!(sql.contains("data_sync_attempt attempt"));
        assert!(sql.contains("attempt.source = 'financial'"));
        assert!(sql.contains("attempt.status = 'completed'"));
        assert!(sql.contains("attempt.symbol = stock.symbol"));
        assert!(sql.contains("attempt.start_date <= $1"));
        assert!(sql.contains("attempt.end_date >= $2"));
        assert!(sql.contains("OFFSET $3 LIMIT $4"));
    }

    #[test]
    fn phase7_coverage_runner_source_state_prefers_requested_window_attempts() {
        let audit = json!({
            "listed_stock_count": 100,
            "optional_data_sources": [
                {
                    "source": "cashflow",
                    "feature_readiness": "ready_for_feature_factory",
                    "symbol_coverage_ratio": 0.99
                }
            ],
            "financial_coverage": [
                {
                    "name": "market_financial_statement",
                    "coverage_grade": "broad",
                    "symbol_coverage_ratio": 0.98
                },
                {
                    "name": "market_financial_indicator",
                    "coverage_grade": "broad",
                    "symbol_coverage_ratio": 0.96
                }
            ]
        });
        let mut window_attempts = BTreeMap::new();
        window_attempts.insert("cashflow".to_string(), 25);
        window_attempts.insert("financial".to_string(), 40);

        let (_, coverage) =
            phase7_coverage_runner_source_state_for_window(&audit, Some(&window_attempts));

        assert_eq!(coverage.get("cashflow"), Some(&0.25));
        assert_eq!(coverage.get("financial"), Some(&0.40));
    }

    #[test]
    fn phase7_coverage_runner_partial_gate_does_not_block_full_coverage_target() {
        assert!(phase7_coverage_runner_should_plan_source_for_target(
            Some("partial_feature_candidate"),
            Some(0.35),
            1.0,
            false,
        ));
        assert!(!phase7_coverage_runner_should_plan_source_for_target(
            Some("partial_feature_candidate"),
            Some(0.35),
            0.30,
            false,
        ));
        assert!(!phase7_coverage_runner_should_plan_source_for_target(
            Some("partial_feature_candidate"),
            Some(0.35),
            1.0,
            true,
        ));
    }

    #[test]
    fn phase7_financial_source_readiness_maps_coverage_to_training_gate() {
        assert_eq!(phase7_financial_source_readiness("missing"), "needs_sync");
        assert_eq!(
            phase7_financial_source_readiness("thin"),
            "undercovered_do_not_train"
        );
        assert_eq!(
            phase7_financial_source_readiness("partial"),
            "partial_feature_candidate"
        );
        assert_eq!(
            phase7_financial_source_readiness("broad"),
            "ready_for_feature_factory"
        );
    }

    #[test]
    fn phase7_coverage_runner_stops_when_source_is_feature_ready() {
        assert!(phase7_coverage_runner_should_plan_source(
            "sample_only_do_not_train"
        ));
        assert!(phase7_coverage_runner_should_plan_source(
            "coverage_reference_missing"
        ));
        assert!(!phase7_coverage_runner_should_plan_source(
            "partial_feature_candidate"
        ));
        assert!(!phase7_coverage_runner_should_plan_source(
            "ready_for_feature_factory"
        ));
    }

    #[test]
    fn phase7_permission_smoke_sources_default_and_deduped() {
        assert_eq!(
            phase7_permission_smoke_sources(&[]),
            vec!["cashflow", "dividend", "repurchase"]
        );

        let requested = vec![
            " CashFlow ".to_string(),
            "dividend".to_string(),
            "cashflow".to_string(),
            "repurchase".to_string(),
        ];
        assert_eq!(
            phase7_permission_smoke_sources(&requested),
            vec!["cashflow", "dividend", "repurchase"]
        );
    }

    #[test]
    fn phase7_permission_smoke_allowlists_industry_membership_probe() {
        assert!(PHASE7_OPTIONAL_SOURCE_SMOKE_ALLOWED.contains(&"industry_membership"));
        assert!(!PHASE7_OPTIONAL_SOURCE_SMOKE_DEFAULTS.contains(&"industry_membership"));

        let requested = vec![
            " Industry_Membership ".to_string(),
            "industry_membership".to_string(),
        ];
        assert_eq!(
            phase7_permission_smoke_sources(&requested),
            vec!["industry_membership"]
        );
    }

    #[test]
    fn phase7_permission_smoke_allowlists_main_business_probe_without_defaulting_it() {
        assert!(PHASE7_OPTIONAL_SOURCE_SMOKE_ALLOWED.contains(&"main_business"));
        assert!(!PHASE7_OPTIONAL_SOURCE_SMOKE_DEFAULTS.contains(&"main_business"));

        let requested = vec![" Main_Business ".to_string(), "main_business".to_string()];
        assert_eq!(
            phase7_permission_smoke_sources(&requested),
            vec!["main_business"]
        );
    }

    #[test]
    fn phase7_permission_smoke_allowlists_report_rc_probe_without_defaulting_it() {
        assert!(PHASE7_OPTIONAL_SOURCE_SMOKE_ALLOWED.contains(&"report_rc"));
        assert!(!PHASE7_OPTIONAL_SOURCE_SMOKE_DEFAULTS.contains(&"report_rc"));

        let requested = vec![" Report_RC ".to_string(), "report_rc".to_string()];
        assert_eq!(
            phase7_permission_smoke_sources(&requested),
            vec!["report_rc"]
        );
    }

    #[test]
    fn phase7_permission_smoke_allowlists_futures_price_chain_without_defaulting_it() {
        assert!(PHASE7_OPTIONAL_SOURCE_SMOKE_ALLOWED.contains(&"futures_price_chain"));
        assert!(!PHASE7_OPTIONAL_SOURCE_SMOKE_DEFAULTS.contains(&"futures_price_chain"));

        let requested = vec![
            " Futures_Price_Chain ".to_string(),
            "futures_price_chain".to_string(),
        ];
        assert_eq!(
            phase7_permission_smoke_sources(&requested),
            vec!["futures_price_chain"]
        );
    }

    #[test]
    fn phase7_permission_smoke_allowlists_equity_pledge_without_defaulting_it() {
        assert!(PHASE7_OPTIONAL_SOURCE_SMOKE_ALLOWED.contains(&"equity_pledge_pressure"));
        assert!(!PHASE7_OPTIONAL_SOURCE_SMOKE_DEFAULTS.contains(&"equity_pledge_pressure"));

        let requested = vec![
            " Equity_Pledge_Pressure ".to_string(),
            "equity_pledge_pressure".to_string(),
        ];
        assert_eq!(
            phase7_permission_smoke_sources(&requested),
            vec!["equity_pledge_pressure"]
        );
    }

    #[test]
    fn phase7_permission_smoke_allowlists_shareholder_structure_without_defaulting_it() {
        assert!(PHASE7_OPTIONAL_SOURCE_SMOKE_ALLOWED.contains(&"shareholder_structure"));
        assert!(!PHASE7_OPTIONAL_SOURCE_SMOKE_DEFAULTS.contains(&"shareholder_structure"));

        let requested = vec![
            " Shareholder_Structure ".to_string(),
            "shareholder_structure".to_string(),
        ];
        assert_eq!(
            phase7_permission_smoke_sources(&requested),
            vec!["shareholder_structure"]
        );
    }

    #[test]
    fn phase7_permission_smoke_allowlists_margin_detail_without_defaulting_it() {
        assert!(PHASE7_OPTIONAL_SOURCE_SMOKE_ALLOWED.contains(&"margin_detail"));
        assert!(!PHASE7_OPTIONAL_SOURCE_SMOKE_DEFAULTS.contains(&"margin_detail"));

        let requested = vec![" Margin_Detail ".to_string(), "margin_detail".to_string()];
        assert_eq!(
            phase7_permission_smoke_sources(&requested),
            vec!["margin_detail"]
        );
    }

    #[test]
    fn akshare_analyst_revision_raw_row_uses_next_open_day_available_at_and_stable_hash() {
        let mut record = serde_json::Map::new();
        record.insert("证券代码".to_string(), json!("000001"));
        record.insert("证券简称".to_string(), json!("平安银行"));
        record.insert("发布日期".to_string(), json!("2026-06-23"));
        record.insert("研究机构简称".to_string(), json!("光大证券"));
        record.insert("研究员名称".to_string(), json!("洪吉然"));
        record.insert("投资评级".to_string(), json!("买入"));
        record.insert("评级变化".to_string(), json!("维持"));
        record.insert("前一次投资评级".to_string(), json!("买入"));
        record.insert("是否首次评级".to_string(), json!("不是首次评级"));
        record.insert("目标价格-下限".to_string(), json!(54.1));
        record.insert("目标价格-上限".to_string(), json!(54.1));
        let open_dates = vec![NaiveDate::from_ymd_opt(2026, 6, 24).unwrap()];

        let row =
            akshare_analyst_revision_raw_row_from_record(&record, "20260623", &open_dates).unwrap();
        let same_row =
            akshare_analyst_revision_raw_row_from_record(&record, "20260623", &open_dates).unwrap();

        assert_eq!(
            row.publication_date,
            NaiveDate::from_ymd_opt(2026, 6, 23).unwrap()
        );
        assert_eq!(
            row.available_at,
            NaiveDate::from_ymd_opt(2026, 6, 24).unwrap()
        );
        assert_eq!(
            row.source_published_at.date_naive(),
            NaiveDate::from_ymd_opt(2026, 6, 24).unwrap()
        );
        assert_eq!(row.raw_payload_hash, same_row.raw_payload_hash);
        assert_eq!(row.rating_change.as_deref(), Some("维持"));
        assert_eq!(row.rating_previous.as_deref(), Some("买入"));
    }

    #[test]
    fn akshare_analyst_revision_raw_row_blocks_publication_date_mismatch() {
        let mut record = serde_json::Map::new();
        record.insert("证券代码".to_string(), json!("000001"));
        record.insert("发布日期".to_string(), json!("2026-06-23"));
        let open_dates = vec![NaiveDate::from_ymd_opt(2026, 6, 24).unwrap()];

        let error = akshare_analyst_revision_raw_row_from_record(&record, "20260624", &open_dates)
            .unwrap_err();

        assert!(error.contains("publication_date_mismatch"));
    }

    #[test]
    fn akshare_analyst_revision_sync_request_builds_bounded_raw_task() {
        let req = AkshareAnalystRevisionSyncReq {
            start_date: Some("20260101".to_string()),
            end_date: Some("20260131".to_string()),
            data_version_id: Some("akshare-analyst-revision-202601".to_string()),
            python: Some("/tmp/akshare-smoke/bin/python".to_string()),
            background: false,
        };

        let sync_req = req.into_sync_task_req();

        assert_eq!(sync_req.dataset, "akshare_analyst_revision");
        assert_eq!(sync_req.source, "akshare_cninfo_revision");
        assert!(sync_req.source.len() <= 32);
        assert_eq!(
            sync_req.mode.as_deref(),
            Some("bounded_calendar_day_raw_sync")
        );
        assert_eq!(
            sync_req.data_version_id.as_deref(),
            Some("akshare-analyst-revision-202601")
        );
        assert!(sync_req.create_data_version);
        assert!(!sync_req.background);
    }

    #[test]
    fn akshare_analyst_revision_sync_range_blocks_large_windows() {
        let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 4, 11).unwrap();

        let error = validate_akshare_analyst_revision_sync_range(start, end).unwrap_err();

        assert!(error.contains("above max 100"));
    }

    #[test]
    fn akshare_analyst_revision_retry_policy_retries_transient_fetch_failures() {
        assert!(akshare_analyst_revision_should_retry_fetch_status(
            "timeout"
        ));
        assert!(akshare_analyst_revision_should_retry_fetch_status("error"));
        assert!(!akshare_analyst_revision_should_retry_fetch_status("ok"));
        assert!(!akshare_analyst_revision_should_retry_fetch_status(
            "ok_empty"
        ));
        assert!(!akshare_analyst_revision_should_retry_fetch_status(
            "runtime_not_configured"
        ));
    }

    #[test]
    fn akshare_analyst_revision_full_fetch_normalizes_empty_dataframe_length_mismatch() {
        let payload = json!({
            "status": "error",
            "permission": "unknown_or_unavailable",
            "error_type": "ValueError",
            "error": "Length mismatch: Expected axis has 0 elements, new values have 11 elements"
        });

        let normalized = normalize_akshare_analyst_revision_full_fetch_payload(payload);

        assert_eq!(normalized["status"], "ok_empty");
        assert_eq!(normalized["permission"], "available");
        assert_eq!(normalized["row_count"], 0);
        assert_eq!(normalized["records"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn akshare_analyst_revision_schema_contract_is_vendor_aware_and_blocks_training() {
        let contract = phase7_akshare_analyst_revision_schema_contract();

        assert_eq!(contract["source_id"], "multi_vendor_analyst_revision");
        assert_eq!(contract["stage"], "P3.23C");
        assert_eq!(
            contract["ddl_path"],
            "sql/phase7_akshare_analyst_revision_source.sql"
        );
        assert_eq!(
            contract["mode"],
            "read_only_vendor_schema_available_at_contract"
        );
        assert_eq!(
            contract["raw_sources"][0]["vendor_endpoint"],
            "stock_rank_forecast_cninfo"
        );
        assert_eq!(
            contract["raw_sources"][0]["native_available_at_candidate"],
            "发布日期"
        );
        assert_eq!(
            contract["raw_sources"][1]["vendor_endpoint"],
            "stock_research_report_em"
        );
        assert!(contract["tables"][0]["required_fields"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| field == "vendor"));
        assert!(contract["tables"][0]["required_fields"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| field == "source_published_at"));
        assert_eq!(contract["promotion_gate"]["factor_builder"], "blocked");
        assert_eq!(contract["promotion_gate"]["p310_status"], "blocked");
        assert_eq!(contract["promotion_gate"]["bounded_wfa"], "blocked");
        assert_eq!(contract["promotion_gate"]["v19_train_selection"], "blocked");
    }

    #[test]
    fn akshare_analyst_revision_sync_plan_uses_calendar_day_quarter_batches() {
        let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 6, 24).unwrap();
        let batches = akshare_analyst_revision_sync_plan_batches(start, end, "quarter").unwrap();
        let plan = akshare_analyst_revision_sync_plan_response(start, end, "quarter", batches);

        assert_eq!(plan["mode"], "read_only_bounded_calendar_day_sync_plan");
        assert_eq!(
            plan["request_key_policy"],
            "calendar publication date; do not restrict to market open days or weekend/holiday analyst reports may be missed"
        );
        assert_eq!(plan["batch_count"], 2);
        assert_eq!(plan["estimated_api_calls"], 175);
        assert_eq!(plan["safe_to_run_full_range"], false);
        assert_eq!(plan["batches"][0]["batch"], "2026Q1");
        assert_eq!(plan["batches"][0]["calendar_day_count"], 90);
        assert_eq!(
            plan["batches"][0]["future_bounded_sync_request"]["data_version_id"],
            "akshare-analyst-revision-2026Q1"
        );
        assert_eq!(plan["promotion_gate"]["p310_status"], "blocked");
        assert_eq!(plan["promotion_gate"]["v19_train_selection"], "blocked");
    }

    #[test]
    fn akshare_analyst_revision_readiness_decision_blocks_until_schema_and_raw_pit_pass() {
        let missing_schema = decide_akshare_analyst_revision_readiness(false, 0, 0, 0, 0, 0, 0);
        assert_eq!(
            missing_schema["admission_decision"],
            "schema_review_apply_required_before_bounded_sync"
        );

        let empty_schema = decide_akshare_analyst_revision_readiness(true, 0, 0, 0, 0, 0, 0);
        assert_eq!(
            empty_schema["admission_decision"],
            "bounded_sync_required_before_coverage_audit"
        );

        let pit_failed = decide_akshare_analyst_revision_readiness(true, 100, 1, 0, 0, 0, 0);
        assert_eq!(
            pit_failed["admission_decision"],
            "raw_pit_or_source_published_at_failed"
        );

        let revision_failed = decide_akshare_analyst_revision_readiness(true, 100, 0, 0, 0, 1, 0);
        assert_eq!(
            revision_failed["admission_decision"],
            "raw_revision_semantics_failed"
        );

        let current_rating_missing =
            decide_akshare_analyst_revision_readiness(true, 100, 0, 0, 10, 0, 0);
        assert_eq!(
            current_rating_missing["admission_decision"],
            "raw_schema_and_pit_ready_for_coverage_audit_only"
        );
        assert_eq!(
            current_rating_missing["row_quality"]["current_rating_usage"],
            "exclude_or_downweight_rows_before_current_rating_factor_use"
        );

        let ready = decide_akshare_analyst_revision_readiness(true, 100, 0, 0, 0, 0, 0);
        assert_eq!(
            ready["admission_decision"],
            "raw_schema_and_pit_ready_for_coverage_audit_only"
        );
        assert_eq!(ready["promotion_gate"]["factor_builder"], "blocked");
        assert_eq!(ready["promotion_gate"]["bounded_wfa"], "blocked");
    }

    #[test]
    fn akshare_analyst_revision_coverage_decision_allows_only_p310_after_full_audit() {
        let incomplete = decide_akshare_analyst_revision_coverage_audit(
            true,
            10_000,
            0.99,
            0,
            1,
            0,
            0,
            0,
            0,
            0,
            "passed_low_linear_correlation_screen",
        );
        assert_eq!(
            incomplete["admission_decision"],
            "blocked_until_full_history_calendar_coverage_passes"
        );

        let duplicate_failed = decide_akshare_analyst_revision_coverage_audit(
            true,
            10_000,
            1.0,
            0,
            0,
            0,
            0,
            0,
            0,
            1,
            "passed_low_linear_correlation_screen",
        );
        assert_eq!(
            duplicate_failed["admission_decision"],
            "blocked_until_pit_source_published_at_revision_semantics_and_duplicate_hash_audit_passes"
        );

        let ready = decide_akshare_analyst_revision_coverage_audit(
            true,
            10_000,
            1.0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            "passed_low_linear_correlation_screen",
        );
        assert_eq!(
            ready["admission_decision"],
            "coverage_pit_quality_correlation_passed_p310_diagnostics_required_next"
        );
        assert_eq!(ready["p310_status"], "ready_for_p310_diagnostics_only");
        assert_eq!(ready["bounded_wfa"], "blocked");
        assert_eq!(ready["v19_train_selection"], "blocked");
    }

    #[test]
    fn akshare_analyst_revision_smoke_sources_are_allowlisted_and_deduped() {
        assert_eq!(
            akshare_analyst_revision_smoke_sources(&[]),
            vec!["stock_rank_forecast_cninfo"]
        );

        let requested = vec![
            " Stock_Rank_Forecast_Cninfo ".to_string(),
            "stock_research_report_em".to_string(),
            "stock_rank_forecast_cninfo".to_string(),
            "stock_profit_forecast_em".to_string(),
        ];
        assert_eq!(
            akshare_analyst_revision_smoke_sources(&requested),
            vec![
                "stock_rank_forecast_cninfo",
                "stock_research_report_em",
                "stock_profit_forecast_em"
            ]
        );
    }

    #[test]
    fn akshare_analyst_revision_available_at_contract_classifies_endpoint_risk() {
        let audit = phase7_akshare_analyst_revision_available_at_contract();
        let endpoints = audit["endpoint_semantics"].as_array().unwrap();
        let by_endpoint: BTreeMap<&str, &Value> = endpoints
            .iter()
            .map(|endpoint| (endpoint["vendor_endpoint"].as_str().unwrap(), endpoint))
            .collect();

        assert_eq!(
            by_endpoint["stock_rank_forecast_cninfo"]["verdict"],
            "history_replay_required_before_raw_sync"
        );
        assert_eq!(
            by_endpoint["stock_rank_forecast_cninfo"]["available_at_candidate"],
            "发布日期"
        );
        assert_eq!(
            by_endpoint["stock_research_report_em"]["verdict"],
            "low_fanout_evidence_layer_only_until_full_symbol_fanout_coverage_passes"
        );
        assert_eq!(
            by_endpoint["stock_profit_forecast_em"]["verdict"],
            "blocked_snapshot_not_pit_ready"
        );
        assert_eq!(audit["promotion_gate"]["p310_status"], "blocked");
        assert_eq!(audit["promotion_gate"]["v19_train_selection"], "blocked");
    }

    #[test]
    fn akshare_analyst_revision_history_replay_decision_allows_schema_review_only_after_clean_sample(
    ) {
        let decision =
            decide_akshare_analyst_revision_history_replay_audit(13, 13, 0, 0, 4_491, 0, 0, 0);

        assert_eq!(decision["passed"], true);
        assert_eq!(
            decision["admission_decision"],
            "history_replay_available_at_sample_passed_schema_review_next"
        );
        assert_eq!(decision["promotion_gate"]["bounded_sync"], "blocked");
        assert_eq!(decision["promotion_gate"]["p310_status"], "blocked");
        assert_eq!(decision["promotion_gate"]["v19_train_selection"], "blocked");
    }

    #[test]
    fn akshare_analyst_revision_history_replay_decision_blocks_publication_date_mismatch() {
        let decision =
            decide_akshare_analyst_revision_history_replay_audit(13, 13, 0, 0, 4_491, 2, 0, 0);

        assert_eq!(decision["passed"], false);
        assert_eq!(
            decision["admission_decision"],
            "blocked_publication_date_mismatch_or_missing"
        );
        assert_eq!(decision["promotion_gate"]["bounded_sync"], "blocked");
    }

    #[test]
    fn akshare_analyst_revision_history_replay_decision_blocks_empty_or_error_dates() {
        let error_decision =
            decide_akshare_analyst_revision_history_replay_audit(13, 12, 1, 0, 4_491, 0, 0, 0);
        let empty_decision =
            decide_akshare_analyst_revision_history_replay_audit(13, 12, 0, 1, 4_491, 0, 0, 0);

        assert_eq!(
            error_decision["admission_decision"],
            "blocked_history_replay_probe_failed"
        );
        assert_eq!(
            empty_decision["admission_decision"],
            "blocked_history_replay_empty_dates"
        );
    }

    #[test]
    fn margin_detail_schema_contract_blocks_training_until_coverage_and_correlation_pass() {
        let contract = phase7_margin_detail_schema_contract();

        assert_eq!(contract["source_id"], "margin_detail_leverage_crowding");
        assert_eq!(contract["stage"], "P3.22C");
        assert_eq!(contract["ddl_path"], "sql/phase7_margin_detail_source.sql");
        assert_eq!(contract["raw_sources"][0]["api"], "margin_detail");
        assert_eq!(contract["tables"][0]["table"], "market_stock_margin_detail");
        assert_eq!(
            contract["available_at_policy"]["default"],
            "conservative next-session availability"
        );
        assert_eq!(
            contract["tables"][0]["quality_rule"],
            "rzche and rqchl may be negative vendor adjustment fields and must be preserved; balances/core activity fields must be nonnegative"
        );
        assert_eq!(
            contract["promotion_gate"]["p310_status"],
            "blocked_until_coverage_pit_quality_and_correlation_readiness_pass"
        );
        assert_eq!(contract["promotion_gate"]["v19_train_selection"], "blocked");

        let ddl = include_str!("../../../../sql/phase7_margin_detail_source.sql");
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS market_stock_margin_detail"));
        assert!(ddl.contains("market_stock_margin_detail_available_at_check"));
        assert!(ddl.contains("available_at > trade_date"));
        assert!(ddl.contains("market_stock_margin_detail_core_nonnegative_check"));
        assert!(!ddl.contains("rzche IS NULL OR rzche >= 0"));
        assert!(!ddl.contains("rqchl IS NULL OR rqchl >= 0"));
    }

    #[test]
    fn exchange_announcement_order_capacity_contract_is_text_evidence_first() {
        let contract = phase7_exchange_announcement_order_capacity_schema_contract();

        assert_eq!(
            contract["source_id"],
            "exchange_announcement_order_capacity_text"
        );
        assert_eq!(contract["stage"], "P3.24A");
        assert_eq!(
            contract["mode"],
            "read_only_source_admission_contract_no_sync"
        );
        assert_eq!(
            contract["ddl_path"],
            "sql/phase7_exchange_announcement_order_capacity_source.sql"
        );
        assert_eq!(
            contract["raw_sources"][0]["vendor_endpoint"],
            "stock_zh_a_disclosure_report_cninfo"
        );
        assert_eq!(
            contract["tables"][0]["table"],
            "market_exchange_announcement_text_raw"
        );
        assert!(contract["tables"][0]["required_fields"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| field == "announcement_id"));
        assert!(contract["tables"][0]["required_fields"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| field == "evidence_spans"));
        assert!(contract["tables"][0]["required_fields"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| field == "pdf_final_url"));
        assert!(contract["tables"][0]["required_fields"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| field == "parser_used"));
        assert!(contract["tables"][0]["required_fields"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| field == "source_published_at_quality"));
        assert_eq!(
            contract["available_at_policy"]["date_only_policy"],
            "if only announcement date is available, set available_at to next open session for trading decisions until source_published_at timestamp is audited"
        );
        assert_eq!(
            contract["pdf_admission"]["runtime_default_python"],
            "~/.local/share/quant-pdf-audit/venv/bin/python"
        );
        assert_eq!(
            contract["pdf_admission"]["pdf_detail_audit_endpoint"],
            "POST /api/v1/quant/data/exchange-announcement-order-capacity/pdf-detail-audit"
        );
        assert_eq!(
            contract["manual_schema_review"]["endpoint"],
            "GET /api/v1/quant/data/exchange-announcement-order-capacity/manual-schema-review"
        );
        assert_eq!(contract["promotion_gate"]["bounded_sync"], "blocked");
        assert_eq!(contract["promotion_gate"]["factor_builder"], "blocked");
        assert_eq!(contract["promotion_gate"]["p310_status"], "blocked");
        assert_eq!(contract["promotion_gate"]["v19_train_selection"], "blocked");
    }

    #[test]
    fn exchange_announcement_order_capacity_next_source_admission_plan_stops_current_pilot() {
        let plan = phase7_exchange_announcement_order_capacity_next_source_admission_plan();

        assert_eq!(
            plan["source_id"],
            "exchange_announcement_order_capacity_text"
        );
        assert_eq!(plan["stage"], "P3.25A");
        assert_eq!(
            plan["current_pilot_decision"],
            "stopped_current_4_symbol_daily_operation_pilot_after_clean_target_recall_failed"
        );
        assert_eq!(
            plan["current_pilot_evidence"]["admissible_target_event_rows"],
            6
        );
        assert_eq!(
            plan["current_pilot_evidence"]["manual_review_sample_shortfall"],
            44
        );
        assert_eq!(
            plan["current_pilot_evidence"]["raw_sync_quality"],
            "passed_after_high_density_landing_cap_fix"
        );
        assert_eq!(
            plan["stop_rules"][0],
            "do_not_continue_month_or_quarter_raw_sync_for_current_4_symbol_daily_operation_pilot"
        );
        assert_eq!(plan["promotion_gate"]["factor_builder"], "blocked");
        assert_eq!(plan["promotion_gate"]["p310_status"], "blocked");
        assert_eq!(plan["promotion_gate"]["bounded_wfa"], "blocked");
        assert_eq!(plan["promotion_gate"]["v19_train_selection"], "blocked");
    }

    #[test]
    fn exchange_announcement_order_capacity_next_source_admission_plan_requires_new_source_gates() {
        let plan = phase7_exchange_announcement_order_capacity_next_source_admission_plan();
        let routes = plan["candidate_routes"].as_array().unwrap();
        let by_route: BTreeMap<&str, &Value> = routes
            .iter()
            .map(|route| (route["route_id"].as_str().unwrap(), route))
            .collect();

        assert_eq!(routes.len(), 2);
        assert_eq!(
            by_route["structured_order_capacity_contract_price_chain_source"]["priority"],
            1
        );
        assert_eq!(
            by_route["announcement_text_broader_universe"]["priority"],
            2
        );
        assert!(
            by_route["structured_order_capacity_contract_price_chain_source"]["required_gates"]
                .as_array()
                .unwrap()
                .iter()
                .any(|gate| gate == "available_at_source_published_at_audit")
        );
        assert!(
            by_route["announcement_text_broader_universe"]["required_gates"]
                .as_array()
                .unwrap()
                .iter()
                .any(|gate| gate == "manual_precision_ge_0_80_with_min_50_clean_target_samples")
        );
        for route in routes {
            assert_eq!(route["promotion_gate"]["schema_apply"], "blocked");
            assert_eq!(route["promotion_gate"]["bounded_sync"], "blocked");
            assert_eq!(route["promotion_gate"]["factor_builder"], "blocked");
            assert_eq!(route["promotion_gate"]["p310_status"], "blocked");
            assert_eq!(route["promotion_gate"]["bounded_wfa"], "blocked");
            assert_eq!(route["promotion_gate"]["v19_train_selection"], "blocked");
        }
    }

    #[test]
    fn structured_order_capacity_price_chain_source_contract_blocks_training_until_vendor_and_pit_pass(
    ) {
        let contract = phase7_structured_order_capacity_price_chain_source_contract();

        assert_eq!(
            contract["source_id"],
            "structured_order_capacity_contract_price_chain_source"
        );
        assert_eq!(contract["stage"], "P3.25B");
        assert_eq!(
            contract["mode"],
            "read_only_structured_source_admission_contract_no_sync"
        );
        assert_eq!(
            contract["admission_decision"],
            "blocked_vendor_permission_and_available_at_contract_required"
        );
        assert_eq!(
            contract["legal_and_vendor_gate"]["status"],
            "required_before_schema_or_sync"
        );
        assert!(contract["required_time_fields"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| field == "source_published_at"));
        assert_eq!(
            contract["available_at_policy"]["intraday_rule"],
            "decision_timestamp must be >= source_published_at; date-only events are daily next-session only"
        );
        assert_eq!(contract["promotion_gate"]["schema_apply"], "blocked");
        assert_eq!(contract["promotion_gate"]["bounded_sync"], "blocked");
        assert_eq!(contract["promotion_gate"]["factor_builder"], "blocked");
        assert_eq!(contract["promotion_gate"]["p310_status"], "blocked");
        assert_eq!(contract["promotion_gate"]["bounded_wfa"], "blocked");
        assert_eq!(contract["promotion_gate"]["v19_train_selection"], "blocked");
    }

    #[test]
    fn structured_order_capacity_price_chain_source_contract_requires_structured_event_schema_and_precision_gates(
    ) {
        let contract = phase7_structured_order_capacity_price_chain_source_contract();
        let event_types = contract["event_schema"].as_array().unwrap();
        let by_type: BTreeMap<&str, &Value> = event_types
            .iter()
            .map(|event_type| (event_type["event_type"].as_str().unwrap(), event_type))
            .collect();

        assert!(by_type.contains_key("order_or_contract_signed"));
        assert!(by_type.contains_key("capacity_expansion_or_commissioning"));
        assert!(by_type.contains_key("product_price_adjustment"));
        assert!(by_type["order_or_contract_signed"]["required_fields"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| field == "counterparty"));
        assert!(
            by_type["capacity_expansion_or_commissioning"]["required_fields"]
                .as_array()
                .unwrap()
                .iter()
                .any(|field| field == "capacity_or_capex")
        );
        assert!(contract["coverage_audit_required"]
            .as_array()
            .unwrap()
            .iter()
            .any(|gate| gate == "manual_precision_ge_0_80_with_min_50_clean_target_samples"));
        assert!(contract["coverage_audit_required"]
            .as_array()
            .unwrap()
            .iter()
            .any(|gate| {
                gate
                == "correlation_vs_existing_moneyflow_liquidity_price_volume_quality_event_sources"
            }));
    }

    #[test]
    fn structured_order_capacity_price_chain_vendor_admission_plan_lists_candidate_smoke_evidence()
    {
        let plan = phase7_structured_order_capacity_price_chain_vendor_admission_plan();

        assert_eq!(
            plan["source_id"],
            "structured_order_capacity_contract_price_chain_source"
        );
        assert_eq!(plan["stage"], "P3.25C");
        assert_eq!(
            plan["mode"],
            "read_only_vendor_source_candidate_discovery_plan_no_schema_no_sync"
        );
        assert_eq!(
            plan["admission_decision"],
            "blocked_until_vendor_permission_history_available_at_and_sample_payload_smoke_pass"
        );
        let candidate_sources = plan["candidate_sources"].as_array().unwrap();
        assert!(candidate_sources.len() >= 3);
        for candidate in candidate_sources {
            assert!(candidate["vendor"].is_string());
            assert!(candidate["source_name"].is_string());
            assert!(candidate["legal_storage_use_status"].is_string());
            assert!(candidate["endpoint_payload_availability"].is_string());
            assert!(candidate["historical_coverage_range"].is_string());
            assert!(candidate["source_published_at_semantics"].is_string());
            assert!(candidate["symbol_mapping_requirement"].is_string());
            assert!(candidate["sample_payload_evidence_required"]
                .as_array()
                .unwrap()
                .iter()
                .any(|field| field == "source_published_at_or_conservative_available_at"));
        }
        assert!(plan["required_sample_payload_fields"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| field == "vendor_event_id"));
        assert!(plan["required_sample_payload_fields"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| field == "raw_payload_hash"));
    }

    #[test]
    fn structured_order_capacity_price_chain_vendor_admission_plan_blocks_all_downstream_gates() {
        let plan = phase7_structured_order_capacity_price_chain_vendor_admission_plan();

        assert_eq!(plan["promotion_gate"]["schema_apply"], "blocked");
        assert_eq!(plan["promotion_gate"]["bounded_sync"], "blocked");
        assert_eq!(plan["promotion_gate"]["factor_builder"], "blocked");
        assert_eq!(plan["promotion_gate"]["p310_status"], "blocked");
        assert_eq!(plan["promotion_gate"]["bounded_wfa"], "blocked");
        assert_eq!(plan["promotion_gate"]["v19_train_selection"], "blocked");
        assert!(plan["stop_rules"]
            .as_array()
            .unwrap()
            .iter()
            .any(|rule| rule == "stop_if_vendor_cannot_provide_historical_payload_samples_with_source_published_at_or_auditable_availability"));
        assert_eq!(
            plan["fallback_if_no_usable_vendor"],
            "switch_to_announcement_text_broader_universe_pre_registered_plan_without_rescuing_current_4_symbol_pilot"
        );
    }

    #[test]
    fn structured_order_capacity_price_chain_source_evidence_inventory_records_real_candidates() {
        let inventory = phase7_structured_order_capacity_price_chain_source_evidence_inventory();

        assert_eq!(
            inventory["source_id"],
            "structured_order_capacity_contract_price_chain_source"
        );
        assert_eq!(inventory["stage"], "P3.25D");
        assert_eq!(
            inventory["mode"],
            "read_only_source_evidence_inventory_no_permission_probe_no_schema_no_sync"
        );
        assert_eq!(
            inventory["admission_decision"],
            "blocked_no_candidate_has_complete_vendor_terms_history_payload_and_available_at_evidence"
        );

        let candidates = inventory["candidate_evidence"].as_array().unwrap();
        let by_id: BTreeMap<&str, &Value> = candidates
            .iter()
            .map(|candidate| (candidate["candidate_id"].as_str().unwrap(), candidate))
            .collect();

        assert!(by_id.contains_key("cninfo_data_service"));
        assert!(by_id.contains_key("eastmoney_major_contracts_public_page"));
        assert!(by_id.contains_key("cnopendata_major_contracts_dataset"));
        assert!(by_id.contains_key("wind_client_api_platform"));
        assert!(by_id.contains_key("choice_dataservice_platform"));
        assert!(by_id.contains_key("juyuan_gildata_platform"));
        assert_eq!(
            by_id["eastmoney_major_contracts_public_page"]["admission_status"],
            "blocked_public_web_page_not_licensed_api"
        );
        assert_eq!(
            by_id["cninfo_data_service"]["admission_status"],
            "candidate_permission_sample_smoke_required"
        );
        for candidate in candidates {
            assert!(candidate["source_url"].is_string());
            assert!(candidate["observed_relevance"].is_string());
            assert!(candidate["missing_required_evidence"]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| item
                    == "license_or_terms_allow_storage_research_and_internal_trading_use"));
            assert!(candidate["missing_required_evidence"]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| item == "read_only_sample_payload_with_source_published_at"));
        }
    }

    #[test]
    fn structured_order_capacity_price_chain_source_evidence_inventory_blocks_downstream_gates() {
        let inventory = phase7_structured_order_capacity_price_chain_source_evidence_inventory();

        assert_eq!(
            inventory["permission_smoke"],
            "blocked_until_candidate_access_configured"
        );
        assert_eq!(inventory["promotion_gate"]["schema_apply"], "blocked");
        assert_eq!(inventory["promotion_gate"]["bounded_sync"], "blocked");
        assert_eq!(inventory["promotion_gate"]["factor_builder"], "blocked");
        assert_eq!(inventory["promotion_gate"]["p310_status"], "blocked");
        assert_eq!(inventory["promotion_gate"]["bounded_wfa"], "blocked");
        assert_eq!(
            inventory["promotion_gate"]["v19_train_selection"],
            "blocked"
        );
        assert!(inventory["hard_stop_if_missing"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "vendor_terms_allowing_storage_research_internal_trading_use"));
        assert_eq!(
            inventory["next_step"],
            "select_one_candidate_with_legal_access_then_run_read_only_permission_and_sample_payload_smoke"
        );
    }

    #[test]
    fn structured_order_capacity_price_chain_cninfo_access_smoke_contract_selects_official_candidate(
    ) {
        let contract = phase7_structured_order_capacity_price_chain_cninfo_access_smoke_contract();

        assert_eq!(
            contract["source_id"],
            "structured_order_capacity_contract_price_chain_source"
        );
        assert_eq!(contract["stage"], "P3.25E");
        assert_eq!(contract["candidate_id"], "cninfo_data_service");
        assert_eq!(
            contract["mode"],
            "read_only_cninfo_access_and_sample_payload_contract_no_network_no_schema_no_sync"
        );
        assert_eq!(
            contract["admission_decision"],
            "blocked_cninfo_terms_credentials_endpoint_dictionary_and_sample_payload_required"
        );
        assert_eq!(
            contract["selected_candidate"]["source_url"],
            "https://webapi.cninfo.com.cn/"
        );
        assert!(contract["required_local_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item
                == "CNINFO authorized account or API token configured outside source control"));
        assert!(contract["required_sample_payload_fields"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "source_published_at"));
        assert!(contract["required_sample_payload_fields"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "raw_payload_hash"));
    }

    #[test]
    fn structured_order_capacity_price_chain_cninfo_access_smoke_contract_blocks_until_evidence_exists(
    ) {
        let contract = phase7_structured_order_capacity_price_chain_cninfo_access_smoke_contract();

        assert_eq!(contract["access_status"], "not_configured_or_not_reviewed");
        assert_eq!(
            contract["permission_smoke"]["status"],
            "blocked_no_authorized_access_or_endpoint_dictionary"
        );
        assert_eq!(contract["promotion_gate"]["schema_apply"], "blocked");
        assert_eq!(contract["promotion_gate"]["bounded_sync"], "blocked");
        assert_eq!(contract["promotion_gate"]["factor_builder"], "blocked");
        assert_eq!(contract["promotion_gate"]["p310_status"], "blocked");
        assert_eq!(contract["promotion_gate"]["bounded_wfa"], "blocked");
        assert_eq!(contract["promotion_gate"]["v19_train_selection"], "blocked");
        assert!(contract["stop_rules"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "stop_if_cninfo_terms_do_not_allow_local_storage_research_and_internal_trading_use"));
    }

    #[test]
    fn structured_order_capacity_price_chain_cninfo_operator_evidence_contract_defines_external_manifest_without_secrets(
    ) {
        let contract =
            phase7_structured_order_capacity_price_chain_cninfo_operator_evidence_contract();

        assert_eq!(
            contract["source_id"],
            "structured_order_capacity_contract_price_chain_source"
        );
        assert_eq!(contract["stage"], "P3.25F");
        assert_eq!(contract["candidate_id"], "cninfo_data_service");
        assert_eq!(
            contract["mode"],
            "read_only_cninfo_operator_evidence_manifest_contract_no_network_no_secret_read_no_schema_no_sync"
        );
        assert_eq!(
            contract["admission_decision"],
            "blocked_until_redacted_operator_evidence_manifest_is_reviewed"
        );
        assert_eq!(
            contract["evidence_manifest_contract"]["manifest_env_var"],
            "QUANT_CNINFO_EVIDENCE_MANIFEST_PATH"
        );
        assert_eq!(
            contract["evidence_manifest_contract"]["repository_storage_policy"],
            "forbid_secrets_raw_payloads_and_vendor_documents_in_repo"
        );
        assert!(
            contract["evidence_manifest_contract"]["accepted_evidence_categories"]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| item == "terms_review_attestation")
        );
        assert!(
            contract["evidence_manifest_contract"]["accepted_evidence_categories"]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| item == "sample_payload_redacted_hash_evidence")
        );
        assert!(
            contract["evidence_manifest_contract"]["required_manifest_fields"]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| item == "artifact_id")
        );
        assert!(contract["forbidden_manifest_contents"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "api_token_or_password"));
        assert!(contract["forbidden_manifest_contents"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "raw_vendor_payload_or_full_vendor_document"));
    }

    #[test]
    fn structured_order_capacity_price_chain_cninfo_operator_evidence_contract_blocks_downstream_until_reviewed(
    ) {
        let contract =
            phase7_structured_order_capacity_price_chain_cninfo_operator_evidence_contract();

        assert_eq!(contract["runtime_actions"]["network_enabled"], false);
        assert_eq!(
            contract["runtime_actions"]["credential_read_enabled"],
            false
        );
        assert_eq!(contract["runtime_actions"]["db_write_enabled"], false);
        assert_eq!(
            contract["promotion_gate"]["permission_smoke"],
            "blocked_until_manifest_exists_and_manual_review_passes"
        );
        assert_eq!(contract["promotion_gate"]["schema_apply"], "blocked");
        assert_eq!(contract["promotion_gate"]["bounded_sync"], "blocked");
        assert_eq!(contract["promotion_gate"]["factor_builder"], "blocked");
        assert_eq!(contract["promotion_gate"]["p310_status"], "blocked");
        assert_eq!(contract["promotion_gate"]["bounded_wfa"], "blocked");
        assert_eq!(contract["promotion_gate"]["v19_train_selection"], "blocked");
        assert!(contract["manual_review_required"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "legal_or_operator_attestation_terms_allow_local_storage_research_and_internal_trading_use"));
        assert!(contract["stop_rules"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "stop_if_manifest_contains_secret_or_raw_vendor_payload"));
        assert_eq!(
            contract["next_step"],
            "prepare_redacted_external_cninfo_evidence_manifest_then_manual_review_before_any_read_only_network_probe"
        );
    }

    #[test]
    fn structured_order_capacity_price_chain_cninfo_operator_evidence_audit_blocks_when_manifest_path_missing(
    ) {
        let audit =
            phase7_structured_order_capacity_price_chain_cninfo_operator_evidence_audit_from_manifest(
                None, None, None,
            );

        assert_eq!(audit["stage"], "P3.25G");
        assert_eq!(audit["candidate_id"], "cninfo_data_service");
        assert_eq!(
            audit["mode"],
            "read_only_cninfo_operator_evidence_manifest_structure_audit_no_network_no_secret_read_no_schema_no_sync"
        );
        assert_eq!(audit["manifest_status"], "missing_env_var");
        assert_eq!(
            audit["admission_decision"],
            "blocked_manifest_env_var_not_configured"
        );
        assert_eq!(audit["runtime_actions"]["network_enabled"], false);
        assert_eq!(audit["runtime_actions"]["credential_read_enabled"], false);
        assert_eq!(audit["runtime_actions"]["db_write_enabled"], false);
        assert_eq!(
            audit["promotion_gate"]["permission_smoke"],
            "blocked_until_manifest_audit_passes"
        );
        assert_eq!(audit["promotion_gate"]["schema_apply"], "blocked");
        assert_eq!(audit["promotion_gate"]["bounded_sync"], "blocked");
        assert_eq!(audit["promotion_gate"]["factor_builder"], "blocked");
        assert_eq!(audit["promotion_gate"]["p310_status"], "blocked");
        assert_eq!(audit["promotion_gate"]["bounded_wfa"], "blocked");
        assert_eq!(audit["promotion_gate"]["v19_train_selection"], "blocked");
    }

    #[test]
    fn structured_order_capacity_price_chain_cninfo_operator_evidence_audit_passes_redacted_manifest_structure_only(
    ) {
        let manifest = cninfo_operator_evidence_reviewed_pass_manifest_fixture();
        let audit =
            phase7_structured_order_capacity_price_chain_cninfo_operator_evidence_audit_from_manifest(
                Some("/Users/gaocheng/.quant/evidence/cninfo_manifest.redacted.json"),
                Some(&manifest),
                None,
            );

        assert_eq!(audit["stage"], "P3.25G");
        assert_eq!(audit["manifest_status"], "structure_passed");
        assert_eq!(
            audit["admission_decision"],
            "passed_for_read_only_permission_sample_smoke_design_only"
        );
        assert_eq!(
            audit["promotion_gate"]["permission_smoke"],
            "allowed_read_only_sample_smoke_design_only"
        );
        assert_eq!(audit["promotion_gate"]["schema_apply"], "blocked");
        assert_eq!(audit["promotion_gate"]["bounded_sync"], "blocked");
        assert_eq!(audit["promotion_gate"]["factor_builder"], "blocked");
        assert_eq!(audit["promotion_gate"]["p310_status"], "blocked");
        assert_eq!(audit["promotion_gate"]["bounded_wfa"], "blocked");
        assert_eq!(audit["promotion_gate"]["v19_train_selection"], "blocked");
        assert_eq!(audit["audit_summary"]["artifact_count"], 7);
        assert_eq!(audit["audit_summary"]["reviewed_pass_count"], 7);
        assert_eq!(audit["audit_summary"]["missing_required_category_count"], 0);
        assert_eq!(audit["audit_summary"]["forbidden_manifest_key_count"], 0);
        assert_eq!(audit["privacy_guards"]["echo_manifest_content"], false);
        assert_eq!(audit["privacy_guards"]["echo_secret_values"], false);
    }

    #[test]
    fn structured_order_capacity_price_chain_cninfo_operator_evidence_audit_blocks_forbidden_manifest_keys(
    ) {
        let mut manifest = cninfo_operator_evidence_reviewed_pass_manifest_fixture();
        let artifacts = manifest["artifacts"].as_array_mut().unwrap();
        artifacts[0]["api_token"] = json!("must_not_be_returned");
        artifacts[1]["raw_payload"] = json!({"vendor": "must_not_be_returned"});

        let audit =
            phase7_structured_order_capacity_price_chain_cninfo_operator_evidence_audit_from_manifest(
                Some("/Users/gaocheng/.quant/evidence/cninfo_manifest.redacted.json"),
                Some(&manifest),
                None,
            );

        assert_eq!(audit["manifest_status"], "forbidden_content_detected");
        assert_eq!(
            audit["admission_decision"],
            "blocked_manifest_contains_forbidden_secret_or_raw_payload_fields"
        );
        assert_eq!(audit["audit_summary"]["forbidden_manifest_key_count"], 2);
        assert!(audit["forbidden_manifest_keys"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "api_token"));
        assert!(audit["forbidden_manifest_keys"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "raw_payload"));
        assert_eq!(audit["privacy_guards"]["echo_manifest_content"], false);
        assert_eq!(audit["privacy_guards"]["echo_secret_values"], false);
        assert_eq!(
            audit["promotion_gate"]["permission_smoke"],
            "blocked_until_manifest_audit_passes"
        );
    }

    #[test]
    fn structured_order_capacity_price_chain_cninfo_permission_sample_smoke_plan_requires_manifest_audit_pass(
    ) {
        let plan =
            phase7_structured_order_capacity_price_chain_cninfo_permission_sample_smoke_plan();

        assert_eq!(plan["stage"], "P3.25H");
        assert_eq!(plan["candidate_id"], "cninfo_data_service");
        assert_eq!(
            plan["mode"],
            "read_only_cninfo_permission_sample_smoke_plan_no_network_no_secret_read_no_db_write_no_schema_no_sync"
        );
        assert_eq!(
            plan["admission_decision"],
            "blocked_until_p3_25g_manifest_audit_passes"
        );
        assert_eq!(plan["preconditions"]["required_previous_gate"], "P3.25G");
        assert_eq!(
            plan["preconditions"]["required_previous_gate_decision"],
            "passed_for_read_only_permission_sample_smoke_design_only"
        );
        assert_eq!(plan["runtime_actions"]["network_enabled"], false);
        assert_eq!(plan["runtime_actions"]["credential_read_enabled"], false);
        assert_eq!(plan["runtime_actions"]["db_write_enabled"], false);
        assert_eq!(
            plan["promotion_gate"]["permission_smoke"],
            "blocked_until_p3_25g_manifest_audit_passes"
        );
        assert_eq!(plan["promotion_gate"]["schema_apply"], "blocked");
        assert_eq!(plan["promotion_gate"]["bounded_sync"], "blocked");
        assert_eq!(plan["promotion_gate"]["factor_builder"], "blocked");
        assert_eq!(plan["promotion_gate"]["p310_status"], "blocked");
        assert_eq!(plan["promotion_gate"]["bounded_wfa"], "blocked");
        assert_eq!(plan["promotion_gate"]["v19_train_selection"], "blocked");
    }

    #[test]
    fn structured_order_capacity_price_chain_cninfo_permission_sample_smoke_plan_is_single_endpoint_read_only(
    ) {
        let plan =
            phase7_structured_order_capacity_price_chain_cninfo_permission_sample_smoke_plan();

        assert_eq!(plan["smoke_plan"]["endpoint_scope"], "single_endpoint_only");
        assert_eq!(plan["smoke_plan"]["max_endpoints_per_run"], 1);
        assert_eq!(plan["smoke_plan"]["max_sample_rows_per_probe"], 20);
        assert_eq!(plan["smoke_plan"]["persist_raw_rows"], false);
        assert_eq!(plan["smoke_plan"]["persist_credentials"], false);
        assert_eq!(plan["smoke_plan"]["capture_raw_payload_in_repo"], false);
        assert!(plan["smoke_plan"]["representative_history_dates"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "2014-01-02"));
        assert!(plan["smoke_plan"]["representative_history_dates"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "latest_completed_publication_or_trading_date"));
        assert!(plan["required_sample_payload_fields"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "source_published_at"));
        assert!(plan["required_sample_payload_fields"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "available_at_rule"));
        assert!(plan["required_sample_payload_fields"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "raw_payload_hash"));
        assert!(plan["forbidden_outputs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "raw_vendor_payload_persistence"));
    }

    #[test]
    fn structured_order_capacity_price_chain_cninfo_operator_evidence_manifest_template_lists_required_categories_without_secrets(
    ) {
        let template =
            phase7_structured_order_capacity_price_chain_cninfo_operator_evidence_manifest_template(
            );

        assert_eq!(template["stage"], "P3.25I");
        assert_eq!(template["candidate_id"], "cninfo_data_service");
        assert_eq!(
            template["mode"],
            "read_only_cninfo_operator_evidence_manifest_template_no_network_no_secret_no_db_write"
        );
        assert_eq!(
            template["admission_decision"],
            "template_only_not_evidence_blocked_until_operator_review_replaces_placeholders"
        );
        assert_eq!(template["runtime_actions"]["network_enabled"], false);
        assert_eq!(
            template["runtime_actions"]["credential_read_enabled"],
            false
        );
        assert_eq!(template["runtime_actions"]["db_write_enabled"], false);
        assert_eq!(
            template["template_policy"]["template_can_pass_p3_25g_without_operator_review"],
            false
        );

        let manifest = &template["manifest_template"];
        assert_eq!(
            manifest["source_id"],
            "structured_order_capacity_contract_price_chain_source"
        );
        assert_eq!(manifest["candidate_id"], "cninfo_data_service");
        let artifacts = manifest["artifacts"].as_array().unwrap();
        assert_eq!(artifacts.len(), 7);
        let by_type: BTreeMap<&str, &Value> = artifacts
            .iter()
            .map(|artifact| (artifact["artifact_type"].as_str().unwrap(), artifact))
            .collect();
        assert!(by_type.contains_key("terms_review_attestation"));
        assert!(by_type.contains_key("credential_presence_attestation_without_secret_value"));
        assert!(by_type.contains_key("endpoint_dictionary_reference"));
        assert!(by_type.contains_key("history_range_attestation"));
        assert!(by_type.contains_key("sample_payload_redacted_hash_evidence"));
        assert!(by_type.contains_key("symbol_mapping_scope_note"));
        assert!(by_type.contains_key("rate_limit_cost_refresh_latency_budget"));
        for artifact in artifacts {
            assert_eq!(artifact["review_status"], "missing");
            assert!(artifact.get("api_token").is_none());
            assert!(artifact.get("raw_payload").is_none());
            assert!(artifact.get("authorization").is_none());
        }
    }

    #[test]
    fn structured_order_capacity_price_chain_cninfo_operator_evidence_manifest_template_does_not_pass_audit(
    ) {
        let template =
            phase7_structured_order_capacity_price_chain_cninfo_operator_evidence_manifest_template(
            );
        let manifest = &template["manifest_template"];
        let audit =
            phase7_structured_order_capacity_price_chain_cninfo_operator_evidence_audit_from_manifest(
                Some("/Users/gaocheng/.quant/evidence/cninfo_manifest.template.json"),
                Some(manifest),
                None,
            );

        assert_eq!(
            audit["manifest_status"],
            "structure_incomplete_or_not_reviewed"
        );
        assert_eq!(
            audit["admission_decision"],
            "blocked_manifest_structure_or_review_incomplete"
        );
        assert_eq!(audit["audit_summary"]["artifact_count"], 7);
        assert_eq!(audit["audit_summary"]["reviewed_pass_count"], 0);
        assert_eq!(audit["audit_summary"]["missing_required_category_count"], 0);
        assert_eq!(audit["audit_summary"]["forbidden_manifest_key_count"], 0);
        assert_eq!(
            audit["promotion_gate"]["permission_smoke"],
            "blocked_until_manifest_audit_passes"
        );
    }

    #[test]
    fn exchange_announcement_order_capacity_manual_schema_review_allows_only_bounded_sync_design() {
        let review = phase7_exchange_announcement_order_capacity_manual_schema_review();

        assert_eq!(
            review["audit_version"],
            "p3.24f-exchange-announcement-order-capacity-manual-schema-review-v1"
        );
        assert_eq!(review["stage"], "P3.24F");
        assert_eq!(review["write_enabled"], false);
        assert_eq!(
            review["ddl_path"],
            "sql/phase7_exchange_announcement_order_capacity_source.sql"
        );
        assert_eq!(
            review["schema_review_decision"],
            "passed_for_bounded_sync_design_only"
        );
        assert_eq!(
            review["promotion_gate"]["schema_apply"],
            "manual_review_passed_apply_still_requires_explicit_operator_action"
        );
        assert_eq!(
            review["promotion_gate"]["bounded_sync"],
            "blocked_until_plan_only_bounded_sync_review"
        );
        assert_eq!(review["promotion_gate"]["factor_builder"], "blocked");
        assert_eq!(review["promotion_gate"]["p310_status"], "blocked");
        assert_eq!(review["promotion_gate"]["bounded_wfa"], "blocked");
        assert_eq!(review["promotion_gate"]["v19_train_selection"], "blocked");
        assert_eq!(
            review["pit_review"]["date_only_next_session_policy"],
            "encoded"
        );
        assert_eq!(review["ddl_checks"]["missing_required_field_count"], 0);
        assert_eq!(review["ddl_checks"]["missing_required_constraint_count"], 0);
        assert_eq!(review["ddl_checks"]["missing_required_index_count"], 0);
        assert!(review["ddl_checks"]["required_fields"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| field == "source_published_at_quality"));
        assert!(
            review["next_required_steps"]
                .as_array()
                .unwrap()
                .iter()
                .any(|step| step
                    == "build_plan_only_calendar_day_symbol_category_bounded_sync_design")
        );
    }

    #[test]
    fn exchange_announcement_order_capacity_sync_plan_is_plan_only_calendar_day_batches() {
        let req = ExchangeAnnouncementOrderCapacitySyncPlanReq {
            symbols: Some("000001.SZ,002459.SZ".to_string()),
            categories: Some("日常经营,重大事项".to_string()),
            market: Some("沪深京".to_string()),
            start_date: Some("20230101".to_string()),
            end_date: Some("20230630".to_string()),
            batch: Some("quarter".to_string()),
        };

        let plan = build_exchange_announcement_order_capacity_sync_plan(req).unwrap();

        assert_eq!(
            plan["audit_version"],
            "p3.24g-exchange-announcement-order-capacity-bounded-sync-plan-v1"
        );
        assert_eq!(plan["stage"], "P3.24G");
        assert_eq!(
            plan["mode"],
            "read_only_plan_only_calendar_day_symbol_category_sync_design"
        );
        assert_eq!(plan["write_enabled"], false);
        assert_eq!(
            plan["request_key_policy"],
            "symbol+category+calendar_date_range; do not restrict to open trading days because weekend/holiday announcements must map to next open session"
        );
        assert_eq!(plan["batch_count"], 2);
        assert_eq!(plan["request_key_count"], 8);
        assert_eq!(plan["query_count"], 244);
        assert_eq!(plan["estimated_api_calls"], 244);
        assert_eq!(
            plan["bounded_runner_query_unit_policy"],
            "query_count equals tiny_slice_count * symbol_count * category_count, matching bounded-sync runner enforcement"
        );
        assert_eq!(plan["bounded_sync_limit"]["would_exceed_limit"], true);
        assert_eq!(plan["batches"][0]["batch"], "2023Q1");
        assert_eq!(plan["batches"][0]["calendar_day_count"], 90);
        assert_eq!(plan["batches"][0]["request_key_count"], 4);
        assert_eq!(plan["batches"][0]["tiny_slice_count"], 30);
        assert_eq!(plan["batches"][0]["query_count"], 120);
        assert_eq!(
            plan["batches"][0]["future_bounded_sync_request"]["plan_only"],
            true
        );
        assert_eq!(
            plan["batches"][0]["future_bounded_sync_request"]["enabled"],
            false
        );
        assert_eq!(
            plan["pit_mapping_policy"]["date_only_next_session"],
            "announcementTime/date-only source_published_at must be mapped to the next open session before any trading feature can use it"
        );
        assert_eq!(
            plan["raw_landing_policy"]["failure_sample_retention"],
            "retain parser errors, ok_empty category outcomes, scanned_pdf_ocr_required and incomplete metadata as auditable raw outcomes"
        );
        assert_eq!(
            plan["sync_endpoint_status"],
            "blocked_plan_exceeds_bounded_runner_query_unit_limit"
        );
        assert_eq!(
            plan["promotion_gate"]["bounded_sync"],
            "blocked_until_operator_schema_apply_and_small_batch_review"
        );
        assert_eq!(plan["promotion_gate"]["factor_builder"], "blocked");
        assert_eq!(plan["promotion_gate"]["p310_status"], "blocked");
        assert_eq!(plan["promotion_gate"]["bounded_wfa"], "blocked");
        assert_eq!(plan["promotion_gate"]["v19_train_selection"], "blocked");
    }

    #[test]
    fn exchange_announcement_order_capacity_sync_plan_uses_bounded_runner_query_units() {
        let req = ExchangeAnnouncementOrderCapacitySyncPlanReq {
            symbols: Some("002459.SZ,600519.SH,300750.SZ,000001.SZ".to_string()),
            categories: Some("重大事项,股权激励".to_string()),
            market: Some("沪深京".to_string()),
            start_date: Some("20231001".to_string()),
            end_date: Some("20231231".to_string()),
            batch: Some("quarter".to_string()),
        };

        let plan = build_exchange_announcement_order_capacity_sync_plan(req).unwrap();

        assert_eq!(plan["batch_count"], 1);
        assert_eq!(plan["request_key_count"], 8);
        assert_eq!(plan["query_count"], 248);
        assert_eq!(plan["estimated_api_calls"], 248);
        assert_eq!(
            plan["bounded_runner_query_unit_policy"],
            "query_count equals tiny_slice_count * symbol_count * category_count, matching bounded-sync runner enforcement"
        );
        assert_eq!(plan["bounded_sync_limit"]["max_query_units"], 120);
        assert_eq!(plan["bounded_sync_limit"]["would_exceed_limit"], true);
        assert_eq!(
            plan["sync_endpoint_status"],
            "blocked_plan_exceeds_bounded_runner_query_unit_limit"
        );
        assert_eq!(plan["batches"][0]["request_key_count"], 8);
        assert_eq!(plan["batches"][0]["tiny_slice_count"], 31);
        assert_eq!(plan["batches"][0]["query_count"], 248);
        assert_eq!(plan["batches"][0]["estimated_api_calls"], 248);
        assert_eq!(plan["batches"][0]["would_exceed_limit"], true);
    }

    #[test]
    fn exchange_announcement_order_capacity_coverage_quality_contract_blocks_training_until_small_batch_audits_pass(
    ) {
        let contract =
            phase7_exchange_announcement_order_capacity_coverage_quality_audit_contract();

        assert_eq!(
            contract["audit_version"],
            "p3.24h-exchange-announcement-order-capacity-coverage-quality-audit-contract-v1"
        );
        assert_eq!(contract["stage"], "P3.24H");
        assert_eq!(
            contract["mode"],
            "read_only_coverage_pit_quality_contract_no_raw_sync"
        );
        assert_eq!(contract["write_enabled"], false);
        assert_eq!(
            contract["coverage_breakdowns"][0],
            "year_market_category_symbol"
        );
        assert_eq!(
            contract["pit_audit"]["date_only_next_session_mapping"],
            "required_for_every_date_only_or_weekend_holiday_publication"
        );
        assert_eq!(contract["quality_thresholds"]["pit_violation_rows"], 0);
        assert_eq!(
            contract["quality_thresholds"]["duplicate_announcement_id_rows"],
            0
        );
        assert_eq!(
            contract["quality_thresholds"]["evidence_span_precision_manual_sample_min"],
            "0.80"
        );
        assert!(contract["required_small_batch_audit_fields"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| field == "source_published_at_quality_distribution"));
        assert!(contract["required_small_batch_audit_fields"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| field == "trainable_scanned_pdf_blocking_rows"));
        assert_eq!(
            contract["quality_thresholds"]["trainable_scanned_pdf_blocking_rows"],
            0
        );
        assert_eq!(
            contract["promotion_gate"]["bounded_sync"],
            "blocked_until_operator_schema_apply_and_one_small_batch_raw_sync_review"
        );
        assert_eq!(
            contract["promotion_gate"]["coverage_quality_audit"],
            "required_after_each_small_batch_and_before_any_full_history_sync"
        );
        assert_eq!(contract["promotion_gate"]["factor_builder"], "blocked");
        assert_eq!(contract["promotion_gate"]["p310_status"], "blocked");
        assert_eq!(contract["promotion_gate"]["bounded_wfa"], "blocked");
        assert_eq!(contract["promotion_gate"]["v19_train_selection"], "blocked");
    }

    #[test]
    fn exchange_announcement_order_capacity_sync_request_is_tiny_and_synchronous() {
        let req = ExchangeAnnouncementOrderCapacitySyncReq {
            symbols: Some("000001.SZ,002459.SZ".to_string()),
            categories: Some("日常经营".to_string()),
            market: Some("沪深京".to_string()),
            start_date: Some("20231031".to_string()),
            end_date: Some("20231031".to_string()),
            data_version_id: Some("exchange-announcement-smoke".to_string()),
            python: None,
            pdf_python: None,
            background: false,
        };

        let validated = validate_exchange_announcement_order_capacity_sync_request(&req).unwrap();

        assert_eq!(validated.symbols, vec!["000001", "002459"]);
        assert_eq!(validated.categories, vec!["日常经营"]);
        assert_eq!(validated.calendar_day_count, 1);
        assert_eq!(validated.query_count, 2);
        assert_eq!(validated.data_version_id, "exchange-announcement-smoke");

        let too_many_days = ExchangeAnnouncementOrderCapacitySyncReq {
            end_date: Some("20231110".to_string()),
            ..req.clone()
        };
        assert!(
            validate_exchange_announcement_order_capacity_sync_request(&too_many_days)
                .unwrap_err()
                .contains("above max")
        );

        let background = ExchangeAnnouncementOrderCapacitySyncReq {
            background: true,
            ..req
        };
        assert!(
            validate_exchange_announcement_order_capacity_sync_request(&background)
                .unwrap_err()
                .contains("background=false")
        );
    }

    #[test]
    fn exchange_announcement_order_capacity_bounded_sync_request_is_month_or_quarter_but_tiny_sliced(
    ) {
        let req = ExchangeAnnouncementOrderCapacityBoundedSyncReq {
            symbols: Some("002459.SZ".to_string()),
            categories: Some("日常经营".to_string()),
            market: Some("沪深京".to_string()),
            start_date: Some("20231001".to_string()),
            end_date: Some("20231031".to_string()),
            batch: Some("month".to_string()),
            data_version_id: Some("exann-p324j-202310".to_string()),
            python: None,
            pdf_python: None,
            background: false,
            stop_on_audit_failure: None,
        };

        let validated =
            validate_exchange_announcement_order_capacity_bounded_sync_request(&req).unwrap();

        assert_eq!(validated.batch_mode, "month");
        assert_eq!(validated.symbols, vec!["002459"]);
        assert_eq!(validated.categories, vec!["日常经营"]);
        assert_eq!(validated.calendar_day_count, 31);
        assert_eq!(validated.slices.len(), 11);
        assert_eq!(validated.total_query_units, 11);
        assert_eq!(
            validated.slices[0].start_date.format("%Y%m%d").to_string(),
            "20231001"
        );
        assert_eq!(
            validated.slices[0].end_date.format("%Y%m%d").to_string(),
            "20231003"
        );
        assert!(validated.stop_on_audit_failure);

        let crossing_month = ExchangeAnnouncementOrderCapacityBoundedSyncReq {
            end_date: Some("20231101".to_string()),
            ..req.clone()
        };
        assert!(
            validate_exchange_announcement_order_capacity_bounded_sync_request(&crossing_month)
                .unwrap_err()
                .contains("single month")
        );

        let too_many_units = ExchangeAnnouncementOrderCapacityBoundedSyncReq {
            symbols: Some("000001.SZ,000002.SZ,000063.SZ,002459.SZ,300750.SZ".to_string()),
            categories: Some("日常经营,重大事项,股权激励,并购重组".to_string()),
            ..req
        };
        assert!(
            validate_exchange_announcement_order_capacity_bounded_sync_request(&too_many_units)
                .unwrap_err()
                .contains("query units")
        );
    }

    #[test]
    fn exchange_announcement_order_capacity_probe_truncation_blocks_partial_raw_landing() {
        let truncated = json!({
            "status": "ok",
            "row_count": 21,
            "sample_rows": vec![json!({"公告链接": "a"}); 20],
        });
        assert!(exchange_announcement_order_capacity_probe_is_truncated(
            &truncated
        ));

        let complete = json!({
            "status": "ok",
            "row_count": 2,
            "sample_rows": [json!({"公告链接": "a"}), json!({"公告链接": "b"})],
        });
        assert!(!exchange_announcement_order_capacity_probe_is_truncated(
            &complete
        ));
    }

    #[test]
    fn exchange_announcement_order_capacity_raw_sync_uses_audited_landing_cap_above_smoke_sample_limit(
    ) {
        let raw_sync_limit = exchange_announcement_order_capacity_raw_sync_row_limit();
        assert!(raw_sync_limit > EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_MAX_ROWS);
        assert!(raw_sync_limit >= 30);

        let high_density_cninfo_day = json!({
            "status": "ok",
            "row_count": 23,
            "sample_rows": vec![json!({"公告链接": "a"}); 23],
        });
        assert!(!exchange_announcement_order_capacity_probe_is_truncated(
            &high_density_cninfo_day
        ));

        let still_too_dense = json!({
            "status": "ok",
            "row_count": (raw_sync_limit as i64) + 1,
            "sample_rows": vec![json!({"公告链接": "a"}); raw_sync_limit],
        });
        assert!(exchange_announcement_order_capacity_probe_is_truncated(
            &still_too_dense
        ));
    }

    #[test]
    fn exchange_announcement_order_capacity_probe_normalizes_only_akshare_empty_dataframe_key_error(
    ) {
        let empty_payload = json!({
            "status": "error",
            "permission": "unknown_or_unavailable",
            "error_type": "KeyError",
            "error": "\"None of [Index(['代码', '简称', '公告标题', '公告时间', 'announcementId', 'orgId'], dtype='str')] are in the [columns]\""
        });

        let empty = normalize_exchange_announcement_order_capacity_probe_payload(empty_payload);

        assert_eq!(empty["status"], "ok_empty");
        assert_eq!(empty["permission"], "available");
        assert_eq!(empty["row_count"], 0);
        assert_eq!(
            empty["normalized_from_error"]["reason"],
            "akshare_empty_dataframe_missing_expected_columns"
        );

        let unsupported_category_payload = json!({
            "status": "error",
            "permission": "unknown_or_unavailable",
            "error_type": "KeyError",
            "error": "'重大事项'"
        });

        let unsupported = normalize_exchange_announcement_order_capacity_probe_payload(
            unsupported_category_payload,
        );

        assert_eq!(unsupported["status"], "category_parser_error");
        assert_eq!(unsupported["parser_reliability"], "blocked");
    }

    #[test]
    fn exchange_announcement_order_capacity_db_source_codes_fit_metadata_columns() {
        assert!(
            EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_TASK_SOURCE
                .chars()
                .count()
                <= 32,
            "data_sync_task.source and data_version.source are VARCHAR(32)"
        );
        assert!(
            EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_ATTEMPT_SOURCE
                .chars()
                .count()
                <= 64,
            "data_sync_attempt.source is VARCHAR(64)"
        );
    }

    fn exchange_announcement_order_capacity_test_metrics(
        row_count: i64,
        target_event_rows: i64,
        target_event_missing_evidence_span_rows: i64,
        completed_attempts: i64,
        completed_empty_attempts: i64,
        failed_attempts: i64,
    ) -> ExchangeAnnouncementOrderCapacityCoverageQualityMetrics {
        ExchangeAnnouncementOrderCapacityCoverageQualityMetrics {
            table_exists: true,
            row_count,
            distinct_symbol_count: if row_count > 0 { 1 } else { 0 },
            distinct_category_count: if row_count > 0 { 1 } else { 0 },
            evidence_span_rows: target_event_rows - target_event_missing_evidence_span_rows,
            target_event_rows,
            target_event_missing_evidence_span_rows,
            completed_attempts,
            completed_empty_attempts,
            failed_attempts,
            ..Default::default()
        }
    }

    #[test]
    fn exchange_announcement_order_capacity_coverage_quality_decision_blocks_until_small_batch_rows_exist(
    ) {
        let empty = decide_exchange_announcement_order_capacity_coverage_quality_audit(
            exchange_announcement_order_capacity_test_metrics(0, 0, 0, 0, 0, 0),
        );

        assert_eq!(
            empty["admission_decision"],
            "bounded_sync_required_before_coverage_quality_audit"
        );
        assert_eq!(empty["p310_status"], "blocked");
        assert_eq!(empty["bounded_wfa"], "blocked");
        assert_eq!(empty["v19_train_selection"], "blocked");

        let passed = decide_exchange_announcement_order_capacity_coverage_quality_audit(
            exchange_announcement_order_capacity_test_metrics(3, 2, 0, 1, 0, 0),
        );
        assert_eq!(
            passed["admission_decision"],
            "small_batch_coverage_pit_quality_passed_expand_bounded_sync_only"
        );
        assert_eq!(passed["p310_status"], "blocked");
        assert_eq!(
            passed["next_step"],
            "expand_by_month_or_quarter_then_rerun_coverage_quality_audit"
        );
    }

    #[test]
    fn exchange_announcement_order_capacity_coverage_quality_allows_non_target_announcements_for_taxonomy_accounting_only(
    ) {
        let non_target = decide_exchange_announcement_order_capacity_coverage_quality_audit(
            exchange_announcement_order_capacity_test_metrics(1, 0, 0, 1, 0, 0),
        );

        assert_eq!(
            non_target["admission_decision"],
            "raw_coverage_passed_no_target_event_rows_for_taxonomy_accounting_only"
        );
        assert_eq!(
            non_target["bounded_sync"],
            "continue_bounded_sync_and_track_target_event_yield"
        );
        assert_eq!(non_target["summary"]["target_event_rows"], json!(0));
        assert_eq!(
            non_target["summary"]["target_event_missing_evidence_span_rows"],
            json!(0)
        );
        assert_eq!(non_target["factor_builder"], "blocked");
        assert_eq!(non_target["p310_status"], "blocked");
        assert_eq!(non_target["bounded_wfa"], "blocked");
        assert_eq!(non_target["v19_train_selection"], "blocked");

        let bad_target = decide_exchange_announcement_order_capacity_coverage_quality_audit(
            exchange_announcement_order_capacity_test_metrics(1, 1, 1, 1, 0, 0),
        );
        assert_eq!(
            bad_target["admission_decision"],
            "blocked_until_target_event_evidence_spans_are_repaired"
        );
        assert_eq!(
            bad_target["bounded_sync"],
            "blocked_until_small_batch_audit_passes"
        );
    }

    #[test]
    fn exchange_announcement_order_capacity_coverage_quality_surfaces_taxonomy_and_ocr_gates() {
        let passed = decide_exchange_announcement_order_capacity_coverage_quality_audit(
            exchange_announcement_order_capacity_test_metrics(3, 2, 0, 1, 0, 0),
        );

        assert_eq!(
            passed["ocr_quality_gate"]["status"],
            "passed_or_not_observed"
        );
        assert_eq!(
            passed["taxonomy_precision_gate"]["status"],
            "passed_or_not_observed"
        );
        assert_eq!(
            passed["taxonomy_precision_gate"]["admissible_target_event_rows"],
            json!(2)
        );
    }

    #[test]
    fn exchange_announcement_order_capacity_event_type_blocks_admin_finance_titles_before_target_labeling(
    ) {
        let noisy_spans = json!([
            {"theme": "order_contract", "keyword": "协议", "snippet": "签订募集资金专户存储监管协议"},
            {"theme": "capacity", "keyword": "项目", "snippet": "募集资金投资项目实施方式"}
        ]);

        assert_eq!(
            exchange_announcement_event_type_from_title_and_spans(
                "关于设立募集资金专户并授权签署募集资金专户存储监管协议的公告",
                &noisy_spans,
            ),
            None
        );

        assert_eq!(
            exchange_announcement_event_type_from_title_and_spans(
                "关于签订日常经营重大合同的公告",
                &noisy_spans,
            ),
            Some("order_or_contract_signed".to_string())
        );
    }

    #[test]
    fn exchange_announcement_order_capacity_coverage_quality_blocks_taxonomy_risk_and_ocr_rows() {
        let ocr_blocked = decide_exchange_announcement_order_capacity_coverage_quality_audit(
            ExchangeAnnouncementOrderCapacityCoverageQualityMetrics {
                row_count: 9,
                distinct_symbol_count: 4,
                distinct_category_count: 1,
                evidence_span_rows: 4,
                target_event_rows: 4,
                scanned_pdf_ocr_required_rows: 2,
                trainable_scanned_pdf_blocking_rows: 2,
                completed_attempts: 9,
                completed_empty_attempts: 7,
                ..exchange_announcement_order_capacity_test_metrics(9, 4, 0, 9, 7, 0)
            },
        );
        assert_eq!(
            ocr_blocked["admission_decision"],
            "blocked_until_scanned_pdf_ocr_runtime_and_audit_pass"
        );
        assert_eq!(ocr_blocked["ocr_quality_gate"]["status"], "blocked");
        assert_eq!(
            ocr_blocked["ocr_quality_gate"]["scanned_pdf_ocr_required_rows"],
            json!(2)
        );
        assert_eq!(
            ocr_blocked["bounded_sync"],
            "blocked_until_small_batch_audit_passes"
        );

        let taxonomy_blocked = decide_exchange_announcement_order_capacity_coverage_quality_audit(
            ExchangeAnnouncementOrderCapacityCoverageQualityMetrics {
                row_count: 9,
                distinct_symbol_count: 4,
                distinct_category_count: 1,
                evidence_span_rows: 4,
                target_event_rows: 4,
                taxonomy_blocked_target_event_rows: 4,
                taxonomy_risk_category_rows: 9,
                completed_attempts: 9,
                completed_empty_attempts: 7,
                ..exchange_announcement_order_capacity_test_metrics(9, 4, 0, 9, 7, 0)
            },
        );
        assert_eq!(
            taxonomy_blocked["admission_decision"],
            "blocked_until_category_aware_taxonomy_precision_manual_review_passes"
        );
        assert_eq!(
            taxonomy_blocked["taxonomy_precision_gate"]["status"],
            "blocked"
        );
        assert_eq!(
            taxonomy_blocked["taxonomy_precision_gate"]["taxonomy_blocked_target_event_rows"],
            json!(4)
        );
        assert_eq!(
            taxonomy_blocked["taxonomy_precision_gate"]["admissible_target_event_rows"],
            json!(0)
        );
    }

    #[test]
    fn exchange_announcement_order_capacity_coverage_quality_keeps_excluded_category_parser_failures_non_blocking(
    ) {
        assert!(
            exchange_announcement_order_capacity_is_excluded_unsupported_category_attempt(
                "000001:重大事项",
                "'重大事项'",
            )
        );
        assert!(
            !exchange_announcement_order_capacity_is_excluded_unsupported_category_attempt(
                "000001:日常经营",
                "'重大事项'",
            )
        );
        assert!(
            !exchange_announcement_order_capacity_is_excluded_unsupported_category_attempt(
                "000001:重大事项",
                "timeout fetching cninfo",
            )
        );

        let passed_with_excluded_failure =
            decide_exchange_announcement_order_capacity_coverage_quality_audit(
                ExchangeAnnouncementOrderCapacityCoverageQualityMetrics {
                    total_failed_attempts: 4,
                    excluded_unsupported_category_failed_attempts: 4,
                    ..exchange_announcement_order_capacity_test_metrics(5, 2, 0, 4, 2, 0)
                },
            );
        assert_eq!(
            passed_with_excluded_failure["admission_decision"],
            "small_batch_coverage_pit_quality_passed_expand_bounded_sync_only"
        );
        assert_eq!(
            passed_with_excluded_failure["summary"]["failed_attempts"],
            json!(0)
        );
        assert_eq!(
            passed_with_excluded_failure["summary"]["total_failed_attempts"],
            json!(4)
        );
        assert_eq!(
            passed_with_excluded_failure["summary"]
                ["excluded_unsupported_category_failed_attempts"],
            json!(4)
        );

        let blocked_with_real_failure =
            decide_exchange_announcement_order_capacity_coverage_quality_audit(
                ExchangeAnnouncementOrderCapacityCoverageQualityMetrics {
                    total_failed_attempts: 5,
                    excluded_unsupported_category_failed_attempts: 4,
                    ..exchange_announcement_order_capacity_test_metrics(5, 2, 0, 4, 2, 1)
                },
            );
        assert_eq!(
            blocked_with_real_failure["admission_decision"],
            "blocked_until_failed_small_batch_attempts_are_repaired"
        );
    }

    #[test]
    fn exchange_announcement_order_capacity_admission_readiness_blocks_p310_until_manual_precision_and_correlation_pass(
    ) {
        let coverage = json!({
            "decision": {
                "admission_decision": "small_batch_coverage_pit_quality_passed_expand_bounded_sync_only",
                "status": "small_batch_coverage_pit_quality_passed_expand_bounded_sync_only",
            },
            "summary": {
                "row_count": 155,
                "target_event_rows": 91,
                "admissible_target_event_rows": 91,
                "pit_violation_rows": 0,
                "duplicate_announcement_id_rows": 0,
                "duplicate_raw_payload_hash_groups": 0,
                "failed_attempts": 0,
                "trainable_scanned_pdf_blocking_rows": 0,
                "target_event_missing_evidence_span_rows": 0,
            },
            "target_event_yield": {
                "target_event_evidence_span_coverage_ratio": 1.0,
            },
            "year_category_breakdown": [
                {"year": 2024, "category": "日常经营", "rows": 155, "symbols": 4}
            ],
            "symbol_event_breakdown": [
                {"symbol": "002459", "rows": 30, "target_event_rows": 17},
                {"symbol": "600519", "rows": 30, "target_event_rows": 7},
                {"symbol": "300750", "rows": 70, "target_event_rows": 50},
                {"symbol": "000001", "rows": 25, "target_event_rows": 17}
            ]
        });

        let readiness = exchange_announcement_order_capacity_admission_readiness_report(&coverage);

        assert_eq!(
            readiness["coverage_pit_quality_gate"]["status"],
            "passed_pilot_scope"
        );
        assert_eq!(
            readiness["manual_evidence_span_precision_gate"]["status"],
            "blocked_manual_review_required"
        );
        assert_eq!(
            readiness["event_taxonomy_precision_gate"]["status"],
            "blocked_manual_review_required"
        );
        assert_eq!(
            readiness["correlation_gate"]["status"],
            "blocked_correlation_audit_required"
        );
        assert_eq!(
            readiness["effective_coverage_gate"]["status"],
            "pilot_scope_only_not_full_history"
        );
        assert_eq!(
            readiness["promotion_gate"]["p310_status"],
            "blocked_until_manual_precision_effective_coverage_and_correlation_pass"
        );
    }

    #[test]
    fn exchange_announcement_order_capacity_manual_precision_sample_report_requires_human_review_before_p310(
    ) {
        let report = exchange_announcement_order_capacity_manual_precision_sample_report(
            91,
            50,
            50,
            5,
            vec![
                json!({
                    "symbol": "300750",
                    "announcement_title": "宁德时代：关于签订日常经营重大合同的公告",
                    "event_type": "order_or_contract_signed",
                    "evidence_spans": [{"keyword": "合同", "snippet": "签订日常经营重大合同"}],
                }),
                json!({
                    "symbol": "002459",
                    "announcement_title": "晶澳科技：关于投资建设产能项目的公告",
                    "event_type": "capacity_expansion_or_commissioning",
                    "evidence_spans": [{"keyword": "产能", "snippet": "投资建设产能项目"}],
                }),
            ],
        );

        assert_eq!(report["status"], "manual_review_sample_ready");
        assert_eq!(report["required_target_sample_size_min"], json!(50));
        assert_eq!(report["target_sample_rows"], json!(50));
        assert_eq!(report["negative_sample_rows"], json!(5));
        assert_eq!(
            report["manual_evidence_span_precision_gate"]["status"],
            "blocked_until_human_labels_are_recorded"
        );
        assert_eq!(
            report["event_taxonomy_precision_gate"]["status"],
            "blocked_until_human_labels_are_recorded"
        );
        assert_eq!(
            report["promotion_gate"]["p310_status"],
            "blocked_until_manual_precision_review_passes"
        );

        let undersized = exchange_announcement_order_capacity_manual_precision_sample_report(
            20,
            50,
            20,
            3,
            vec![],
        );
        assert_eq!(
            undersized["status"],
            "blocked_insufficient_target_review_sample"
        );
        assert_eq!(undersized["target_sample_shortfall"], json!(30));
        assert_eq!(
            undersized["promotion_gate"]["p310_status"],
            "blocked_until_manual_precision_review_passes"
        );
    }

    #[test]
    fn exchange_announcement_order_capacity_taxonomy_risk_title_filter_blocks_admin_finance_false_positives(
    ) {
        let false_positive_titles = [
            "关于2023年度计提减值准备的公告",
            "关于部分募投项目延期的公告",
            "关联交易公告",
            "关于2024年度日常关联交易预计的公告",
            "关于向金融机构申请2024年度综合授信额度的公告",
            "关于完成注册资本工商变更登记的公告",
            "关于实际控制人解除一致行动关系暨变更实际控制人的提示性公告",
            "2024年半年度财务报告",
            "关于参与投资福建时代泽远碳中和股权投资基金合伙企业（有限合伙）的公告",
            "关于股东部分股份质押的公告",
            "贵州茅台2024年第三季度主要经营数据公告",
            "贵州茅台关于贵州茅台集团财务有限公司的风险评估报告",
            "关于聘请H股发行并上市审计机构的公告",
            "贵州茅台章程（修订草案）",
            "2023年年度审计报告英文版（2023 Audit Report）",
            "关于开展基础设施公募REITs申报发行工作的公告",
            "关于收购控股子公司部分股权的公告",
            "关于2023年度计提资产减值准备的公告",
            "关于续聘2024年度审计机构的公告",
            "2023年度董事会工作报告",
            "内部控制自我评价报告",
            "关于续聘2024年度会计师事务所的公告",
            "关于2024年度委托理财计划的公告",
            "关于2025年度公司与下属公司担保额度预计的公告",
            "关于2024年半年度募集资金存放与使用情况的专项报告",
            "公司章程修正案",
            "独立董事候选人声明与承诺",
            "关于会计政策变更的公告",
            "2023年环境、社会及治理（ESG）报告",
            "2024年可持续发展报告",
            "平安银行股份有限公司估值提升计划",
            "关于制定及修订公司制度的公告",
            "关于就公司发行H股股票制定及修订公司制度的公告",
            "关于境外全资子公司在境外发行债券并由公司提供担保的公告",
        ];
        for title in false_positive_titles {
            assert_eq!(
                exchange_announcement_order_capacity_taxonomy_risk_title_reason("日常经营", title),
                Some("admin_finance_governance_false_positive")
            );
        }

        let true_target_titles = [
            "关于签署《关于进一步加强和深化合作的协议》的公告",
            "关于公司与Stellantis合资建厂的公告",
            "关于投资建设高效电池产能项目的公告",
            "关于签订日常经营重大合同的公告",
            "贵州茅台重大事项公告",
        ];
        for title in true_target_titles {
            assert_eq!(
                exchange_announcement_order_capacity_taxonomy_risk_title_reason("日常经营", title),
                None
            );
        }
    }

    #[test]
    fn exchange_announcement_order_capacity_ocr_taxonomy_exclusion_is_narrow_for_special_reports() {
        assert_eq!(
            exchange_announcement_order_capacity_ocr_taxonomy_exclusion_reason(
                "日常经营",
                "贵州茅台：关于贵州茅台酒股份有限公司控股股东及其他关联方资金占用情况的专项说明"
            ),
            Some("special_report_related_party_funds")
        );
        assert_eq!(
            exchange_announcement_order_capacity_ocr_taxonomy_exclusion_reason(
                "日常经营",
                "关于宁德时代新能源科技股份有限公司2024年度募集资金存放与使用情况鉴证报告",
            ),
            Some("special_report_fundraising_use_assurance")
        );
        assert_eq!(
            exchange_announcement_order_capacity_ocr_taxonomy_exclusion_reason(
                "日常经营",
                "半年度非经营性资金占用及其他关联资金往来情况汇总表"
            ),
            Some("special_report_related_party_funds")
        );
        assert_eq!(
            exchange_announcement_order_capacity_ocr_taxonomy_exclusion_reason(
                "日常经营",
                "贵州茅台：关于贵州茅台酒股份有限公司2023年度涉及财务公司关联交易的存款、贷款等金融业务的专项说明"
            ),
            Some("special_report_related_party_finance")
        );
        assert_eq!(
            exchange_announcement_order_capacity_ocr_taxonomy_exclusion_reason(
                "日常经营",
                "某公司：关于签订重大销售合同的公告"
            ),
            None
        );
    }

    #[test]
    fn exchange_announcement_order_capacity_coverage_quality_excludes_ocr_special_reports_without_unblocking_training(
    ) {
        let excluded_only = decide_exchange_announcement_order_capacity_coverage_quality_audit(
            ExchangeAnnouncementOrderCapacityCoverageQualityMetrics {
                table_exists: true,
                row_count: 2,
                distinct_symbol_count: 1,
                distinct_category_count: 1,
                scanned_pdf_ocr_required_rows: 2,
                ocr_taxonomy_excluded_rows: 2,
                trainable_scanned_pdf_blocking_rows: 0,
                completed_attempts: 1,
                ..Default::default()
            },
        );

        assert_eq!(
            excluded_only["admission_decision"],
            "raw_coverage_passed_no_target_event_rows_for_taxonomy_accounting_only"
        );
        assert_eq!(
            excluded_only["ocr_quality_gate"]["status"],
            "passed_with_taxonomy_exclusions_only"
        );
        assert_eq!(
            excluded_only["ocr_quality_gate"]["ocr_taxonomy_excluded_rows"],
            json!(2)
        );
        assert_eq!(
            excluded_only["ocr_quality_gate"]["trainable_scanned_pdf_blocking_rows"],
            json!(0)
        );
        assert_eq!(excluded_only["factor_builder"], "blocked");
        assert_eq!(excluded_only["p310_status"], "blocked");
        assert_eq!(excluded_only["bounded_wfa"], "blocked");
        assert_eq!(excluded_only["v19_train_selection"], "blocked");

        let one_unresolved = decide_exchange_announcement_order_capacity_coverage_quality_audit(
            ExchangeAnnouncementOrderCapacityCoverageQualityMetrics {
                table_exists: true,
                row_count: 3,
                distinct_symbol_count: 2,
                distinct_category_count: 1,
                scanned_pdf_ocr_required_rows: 3,
                ocr_taxonomy_excluded_rows: 2,
                trainable_scanned_pdf_blocking_rows: 1,
                completed_attempts: 1,
                ..Default::default()
            },
        );
        assert_eq!(
            one_unresolved["admission_decision"],
            "blocked_until_scanned_pdf_ocr_runtime_and_audit_pass"
        );
        assert_eq!(one_unresolved["ocr_quality_gate"]["status"], "blocked");
        assert_eq!(
            one_unresolved["ocr_quality_gate"]["trainable_scanned_pdf_blocking_rows"],
            json!(1)
        );
    }

    #[test]
    fn exchange_announcement_order_capacity_target_event_yield_report_blocks_training() {
        let report =
            exchange_announcement_order_capacity_target_event_yield_report(5, 2, 2, 0, 1, 1, 0, 1);

        assert_eq!(report["raw_row_count"], json!(5));
        assert_eq!(report["target_event_rows"], json!(2));
        assert_eq!(report["admissible_target_event_rows"], json!(1));
        assert_eq!(report["taxonomy_blocked_target_event_rows"], json!(1));
        assert_eq!(report["scanned_pdf_ocr_required_rows"], json!(1));
        assert_eq!(report["ocr_taxonomy_excluded_rows"], json!(0));
        assert_eq!(report["trainable_scanned_pdf_blocking_rows"], json!(1));
        assert_eq!(report["target_event_with_evidence_span_rows"], json!(2));
        assert_eq!(report["target_event_missing_evidence_span_rows"], json!(0));
        assert_eq!(report["target_event_yield_ratio"], json!(0.4));
        assert_eq!(report["admissible_target_event_yield_ratio"], json!(0.2));
        assert_eq!(
            report["target_event_evidence_span_coverage_ratio"],
            json!(1.0)
        );
        assert_eq!(
            report["admission_scope"],
            "raw_coverage_taxonomy_accounting_only"
        );
        assert_eq!(report["factor_builder"], "blocked");
        assert_eq!(report["p310_status"], "blocked");
        assert_eq!(report["bounded_wfa"], "blocked");
        assert_eq!(report["v19_train_selection"], "blocked");

        let empty =
            exchange_announcement_order_capacity_target_event_yield_report(0, 0, 0, 0, 0, 0, 0, 0);
        assert_eq!(empty["target_event_yield_ratio"], Value::Null);
        assert_eq!(empty["admissible_target_event_yield_ratio"], Value::Null);
        assert_eq!(
            empty["target_event_evidence_span_coverage_ratio"],
            Value::Null
        );
    }

    #[test]
    fn exchange_announcement_order_capacity_coverage_quality_allows_synced_empty_windows_for_accounting_only(
    ) {
        let synced_empty = decide_exchange_announcement_order_capacity_coverage_quality_audit(
            exchange_announcement_order_capacity_test_metrics(0, 0, 0, 1, 1, 0),
        );

        assert_eq!(
            synced_empty["admission_decision"],
            "synced_empty_no_event_rows_passed_for_coverage_accounting_only"
        );
        assert_eq!(
            synced_empty["bounded_sync"],
            "continue_bounded_sync_for_coverage_accounting_only"
        );
        assert_eq!(
            synced_empty["next_step"],
            "continue_next_tiny_slice_or_batch_then_rerun_full_window_audit"
        );
        assert_eq!(synced_empty["factor_builder"], "blocked");
        assert_eq!(synced_empty["p310_status"], "blocked");
        assert_eq!(synced_empty["bounded_wfa"], "blocked");
        assert_eq!(synced_empty["v19_train_selection"], "blocked");
        assert_eq!(synced_empty["summary"]["completed_attempts"], json!(1));
        assert_eq!(
            synced_empty["summary"]["completed_empty_attempts"],
            json!(1)
        );
    }

    #[test]
    fn exchange_announcement_order_capacity_smoke_plan_blocks_schema_and_training() {
        let req = ExchangeAnnouncementOrderCapacitySmokeReq {
            symbols: vec!["000001.SZ".to_string(), "600000.SH".to_string()],
            categories: vec!["日常经营".to_string(), "股权激励".to_string()],
            market: Some("沪深京".to_string()),
            start_date: Some("20230101".to_string()),
            end_date: Some("20230331".to_string()),
            limit: Some(3),
            python: Some("/tmp/akshare-smoke/bin/python".to_string()),
        };

        let plan = exchange_announcement_order_capacity_smoke_plan(&req).unwrap();

        assert_eq!(
            plan["audit_version"],
            "p3.24b-exchange-announcement-order-capacity-permission-history-category-smoke-v1"
        );
        assert_eq!(
            plan["mode"],
            "read_only_permission_history_category_smoke_no_write"
        );
        assert_eq!(plan["vendor"], "akshare");
        assert_eq!(
            plan["vendor_endpoint"],
            "stock_zh_a_disclosure_report_cninfo"
        );
        assert_eq!(plan["write_enabled"], false);
        assert_eq!(plan["market"], "沪深京");
        assert_eq!(plan["query_count"], 4);
        assert_eq!(plan["promotion_gate"]["schema_apply"], "blocked");
        assert_eq!(plan["promotion_gate"]["bounded_sync"], "blocked");
        assert_eq!(plan["promotion_gate"]["factor_builder"], "blocked");
        assert_eq!(plan["promotion_gate"]["p310_status"], "blocked");
        assert_eq!(plan["promotion_gate"]["v19_train_selection"], "blocked");
        assert_eq!(
            plan["next_step"],
            "run_this_read_only_smoke_then_audit_cninfo_detail_text_timestamp_and_text_hash_before_schema_apply"
        );
    }

    #[test]
    fn exchange_announcement_order_capacity_parses_cninfo_link_metadata() {
        let parsed = parse_cninfo_announcement_link_metadata("https://static.cninfo.com.cn/finalpage/2023-03-31/1216340215.PDF?announcementId=1216340215&orgId=gssz0000001&stockCode=000001&announcementTime=2023-03-31");

        assert_eq!(parsed["announcement_id"], "1216340215");
        assert_eq!(parsed["org_id"], "gssz0000001");
        assert_eq!(parsed["stock_code"], "000001");
        assert_eq!(parsed["announcement_time"], "2023-03-31");
        assert_eq!(parsed["metadata_complete"], true);
        assert!(parsed["missing_fields"].as_array().unwrap().is_empty());

        let incomplete = parse_cninfo_announcement_link_metadata(
            "https://static.cninfo.com.cn/finalpage/2023-03-31/1216340215.PDF",
        );
        assert_eq!(incomplete["announcement_id"], "1216340215");
        assert_eq!(incomplete["metadata_complete"], false);
        assert!(incomplete["missing_fields"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| field == "orgId"));
    }

    #[test]
    fn exchange_announcement_order_capacity_raw_row_preserves_cninfo_timestamp_param() {
        let list_row = json!({
            "代码": "300750",
            "简称": "宁德时代",
            "公告标题": "关于项目投资合作的公告",
            "公告时间": "2023-11-02 19:30:21",
            "公告链接": "https://static.cninfo.com.cn/finalpage/2023-11-02/1218233333.PDF?announcementId=1218233333&orgId=gssz300750&stockCode=300750&announcementTime=2023-11-02%2019:30:21"
        });
        let pdf_probe = json!({
            "status": "ok",
            "source_published_at": "2023-11-02 19:30:21",
            "source_published_at_quality": "timestamp",
            "timestamp_candidates": [{
                "source": "announcement_time_param",
                "value": "2023-11-02 19:30:21",
                "quality": "timestamp"
            }],
            "evidence_spans": [{
                "theme": "capacity",
                "keyword": "项目",
                "page": 1,
                "snippet": "关于项目投资合作的公告"
            }],
            "parser_errors": [],
            "pdf_metadata_keys": [],
            "text_hash": "abc123",
            "text_sample": "关于项目投资合作的公告"
        });
        let open_dates = vec![NaiveDate::from_ymd_opt(2023, 11, 3).unwrap()];

        let row = exchange_announcement_raw_row_from_list_and_pdf_probe(
            &list_row,
            "日常经营",
            "300750:日常经营",
            &pdf_probe,
            &open_dates,
            "p324-timestamp-test",
        )
        .unwrap();

        assert_eq!(
            row.announcement_time,
            NaiveDate::from_ymd_opt(2023, 11, 2).unwrap()
        );
        assert_eq!(
            row.available_at,
            NaiveDate::from_ymd_opt(2023, 11, 3).unwrap()
        );
        assert_eq!(row.source_published_at_quality, "timestamp");
        assert_eq!(row.source_published_date, None);
        assert_eq!(
            row.source_published_at_ts.unwrap().to_rfc3339(),
            "2023-11-02T11:30:21+00:00"
        );
    }

    #[test]
    fn exchange_announcement_order_capacity_detail_audit_plan_blocks_training() {
        let req = ExchangeAnnouncementOrderCapacityDetailAuditReq {
            announcement_links: vec![
                "http://www.cninfo.com.cn/new/disclosure/detail?stockCode=000001&announcementId=1216072959&orgId=gssz0000001&announcementTime=2023-03-09".to_string(),
            ],
            limit: Some(1),
            python: Some("/tmp/akshare-smoke/bin/python".to_string()),
        };

        let plan = exchange_announcement_order_capacity_detail_audit_plan(&req).unwrap();

        assert_eq!(
            plan["audit_version"],
            "p3.24c-exchange-announcement-order-capacity-detail-text-timestamp-audit-v1"
        );
        assert_eq!(plan["stage"], "P3.24C");
        assert_eq!(
            plan["mode"],
            "read_only_cninfo_detail_text_timestamp_hash_audit_no_write"
        );
        assert_eq!(plan["write_enabled"], false);
        assert_eq!(plan["link_count"], 1);
        assert_eq!(
            plan["promotion_gate"]["schema_apply"],
            "blocked_until_detail_audit_passes"
        );
        assert_eq!(plan["promotion_gate"]["bounded_sync"], "blocked");
        assert_eq!(plan["promotion_gate"]["factor_builder"], "blocked");
        assert_eq!(plan["promotion_gate"]["p310_status"], "blocked");
        assert_eq!(plan["promotion_gate"]["v19_train_selection"], "blocked");
    }

    #[test]
    fn exchange_announcement_order_capacity_detail_audit_decision_requires_text_hash_and_timestamp()
    {
        let passed = decide_exchange_announcement_detail_audit(3, 3, 3, 3, 0);
        assert_eq!(
            passed["admission_decision"],
            "detail_text_timestamp_hash_audit_passed_schema_review_allowed_next"
        );
        assert_eq!(
            passed["promotion_gate"]["schema_apply"],
            "manual_review_required"
        );
        assert_eq!(passed["promotion_gate"]["bounded_sync"], "blocked");

        let missing_timestamp = decide_exchange_announcement_detail_audit(3, 3, 3, 1, 0);
        assert_eq!(
            missing_timestamp["admission_decision"],
            "blocked_missing_source_published_at_timestamp"
        );

        let incomplete_metadata = decide_exchange_announcement_detail_audit(3, 3, 3, 3, 1);
        assert_eq!(
            incomplete_metadata["admission_decision"],
            "blocked_incomplete_announcement_link_metadata"
        );
    }

    #[test]
    fn exchange_announcement_order_capacity_detail_probe_summary_rejects_pdf_binary_as_text() {
        let probes = vec![json!({
            "status": "pdf_text_parser_required",
            "text_length": 222260,
            "text_hash": "4555445a49d356b6",
            "source_published_at_quality": "missing_or_date_only",
            "link_metadata": {
                "metadata_complete": true
            }
        })];

        let summary = summarize_exchange_announcement_detail_probes(&probes);

        assert_eq!(summary.fetched_text_count, 0);
        assert_eq!(summary.text_hash_count, 0);
        assert_eq!(summary.source_published_at_count, 0);
        assert_eq!(summary.pdf_parser_required_count, 1);
        assert_eq!(summary.incomplete_link_metadata_count, 0);
    }

    #[test]
    fn exchange_announcement_order_capacity_pdf_parser_readiness_blocks_when_no_parser() {
        let readiness = exchange_announcement_order_capacity_pdf_parser_readiness_report(
            "/tmp/akshare-smoke/bin/python",
            vec![
                json!({"tool": "pdftotext", "kind": "binary", "available": false}),
                json!({"tool": "pypdf", "kind": "python_module", "available": false}),
                json!({"tool": "PyPDF2", "kind": "python_module", "available": false}),
                json!({"tool": "pdfplumber", "kind": "python_module", "available": false}),
                json!({"tool": "fitz", "kind": "python_module", "available": false}),
            ],
        );

        assert_eq!(readiness["stage"], "P3.24D");
        assert_eq!(
            readiness["mode"],
            "read_only_pdf_parser_source_timestamp_readiness_no_write"
        );
        assert_eq!(readiness["write_enabled"], false);
        assert_eq!(readiness["parser_status"], "missing");
        assert_eq!(
            readiness["admission_decision"],
            "blocked_pdf_parser_missing"
        );
        assert_eq!(readiness["promotion_gate"]["bounded_sync"], "blocked");
        assert_eq!(readiness["promotion_gate"]["factor_builder"], "blocked");
        assert_eq!(readiness["promotion_gate"]["p310_status"], "blocked");
        assert_eq!(
            readiness["promotion_gate"]["v19_train_selection"],
            "blocked"
        );
    }

    #[test]
    fn exchange_announcement_order_capacity_pdf_parser_readiness_requires_timestamp_audit_even_when_parser_exists(
    ) {
        let readiness = exchange_announcement_order_capacity_pdf_parser_readiness_report(
            "/tmp/akshare-smoke/bin/python",
            vec![
                json!({"tool": "pdftotext", "kind": "binary", "available": true}),
                json!({"tool": "pypdf", "kind": "python_module", "available": false}),
            ],
        );

        assert_eq!(readiness["parser_status"], "available");
        assert_eq!(
            readiness["admission_decision"],
            "pdf_parser_available_detail_timestamp_audit_required_next"
        );
        assert_eq!(
            readiness["promotion_gate"]["schema_apply"],
            "blocked_until_pdf_text_and_source_published_at_audit_passes"
        );
        assert_eq!(readiness["promotion_gate"]["bounded_sync"], "blocked");
    }

    #[test]
    fn exchange_announcement_order_capacity_pdf_detail_audit_plan_uses_isolated_runtime() {
        let req = ExchangeAnnouncementOrderCapacityPdfDetailAuditReq {
            announcement_links: vec![
                "https://static.cninfo.com.cn/finalpage/2023-03-31/1216340215.PDF?announcementId=1216340215&orgId=gssz0000001&stockCode=000001&announcementTime=2023-03-31".to_string(),
            ],
            limit: Some(1),
            python: Some("/Users/gaocheng/.local/share/quant-pdf-audit/venv/bin/python".to_string()),
        };

        let plan = exchange_announcement_order_capacity_pdf_detail_audit_plan(&req).unwrap();

        assert_eq!(
            plan["audit_version"],
            "p3.24e-exchange-announcement-order-capacity-pdf-detail-audit-v1"
        );
        assert_eq!(plan["stage"], "P3.24E");
        assert_eq!(
            plan["mode"],
            "read_only_pdf_detail_text_timestamp_span_audit_no_write"
        );
        assert_eq!(plan["write_enabled"], false);
        assert_eq!(
            plan["python"],
            "/Users/gaocheng/.local/share/quant-pdf-audit/venv/bin/python"
        );
        assert_eq!(
            plan["promotion_gate"]["schema_apply"],
            "blocked_until_pdf_detail_audit_passes"
        );
        assert_eq!(plan["promotion_gate"]["bounded_sync"], "blocked");
        assert_eq!(plan["promotion_gate"]["factor_builder"], "blocked");
        assert_eq!(plan["promotion_gate"]["p310_status"], "blocked");
    }

    #[test]
    fn exchange_announcement_order_capacity_pdf_detail_audit_decision_requires_hash_availability_and_spans(
    ) {
        let passed =
            decide_exchange_announcement_order_capacity_pdf_detail_audit(2, 2, 2, 2, 2, 0, 0);
        assert_eq!(
            passed["admission_decision"],
            "pdf_detail_audit_passed_manual_schema_review_allowed_next"
        );
        assert_eq!(
            passed["promotion_gate"]["schema_apply"],
            "manual_review_required"
        );

        let missing_availability =
            decide_exchange_announcement_order_capacity_pdf_detail_audit(2, 2, 2, 1, 2, 0, 0);
        assert_eq!(
            missing_availability["admission_decision"],
            "blocked_missing_pdf_source_published_at_or_next_session_policy"
        );

        let missing_spans =
            decide_exchange_announcement_order_capacity_pdf_detail_audit(2, 2, 2, 2, 1, 0, 0);
        assert_eq!(
            missing_spans["admission_decision"],
            "blocked_missing_relevant_evidence_spans"
        );

        let scanned =
            decide_exchange_announcement_order_capacity_pdf_detail_audit(2, 2, 2, 2, 2, 1, 0);
        assert_eq!(
            scanned["admission_decision"],
            "blocked_scanned_pdf_ocr_required"
        );
    }

    #[test]
    fn exchange_announcement_order_capacity_ocr_blocked_row_audit_plan_is_read_only() {
        let req = ExchangeAnnouncementOrderCapacityOcrBlockedRowAuditReq {
            start_date: Some("20240401".to_string()),
            end_date: Some("20240403".to_string()),
            symbols: Some("600519.SH,300750.SZ".to_string()),
            limit: Some(2),
            python: Some(
                "/Users/gaocheng/.local/share/quant-pdf-audit/venv/bin/python".to_string(),
            ),
        };

        let plan = exchange_announcement_order_capacity_ocr_blocked_row_audit_plan(&req).unwrap();

        assert_eq!(
            plan["audit_version"],
            "p3.24t-exchange-announcement-order-capacity-scanned-pdf-ocr-audit-v1"
        );
        assert_eq!(plan["stage"], "P3.24T");
        assert_eq!(
            plan["mode"],
            "read_only_scanned_pdf_ocr_candidate_audit_no_write"
        );
        assert_eq!(plan["write_enabled"], false);
        assert_eq!(plan["symbols"], json!(["600519", "300750"]));
        assert_eq!(plan["row_limit"], 2);
        assert_eq!(plan["promotion_gate"]["factor_builder"], "blocked");
        assert_eq!(plan["promotion_gate"]["p310_status"], "blocked");
        assert_eq!(plan["promotion_gate"]["v19_train_selection"], "blocked");
    }

    #[test]
    fn exchange_announcement_order_capacity_ocr_blocked_row_audit_decision_blocks_missing_runtime()
    {
        let decision = decide_exchange_announcement_order_capacity_ocr_blocked_row_audit(
            2, 0, 0, 0, 0, 0, 2, 0, 0, 0,
        );

        assert_eq!(
            decision["admission_decision"],
            "blocked_ocr_runtime_missing_or_incomplete"
        );
        assert_eq!(decision["ocr_quality_gate"]["status"], "blocked");
        assert_eq!(decision["promotion_gate"]["factor_builder"], "blocked");
        assert_eq!(decision["promotion_gate"]["bounded_wfa"], "blocked");
        assert_eq!(decision["promotion_gate"]["v19_train_selection"], "blocked");
    }

    #[test]
    fn exchange_announcement_order_capacity_ocr_blocked_row_audit_decision_allows_manual_review_only(
    ) {
        let decision = decide_exchange_announcement_order_capacity_ocr_blocked_row_audit(
            2, 2, 2, 2, 2, 1, 0, 0, 1, 0,
        );

        assert_eq!(
            decision["admission_decision"],
            "ocr_text_quality_audit_passed_manual_taxonomy_review_required"
        );
        assert_eq!(
            decision["ocr_quality_gate"]["status"],
            "passed_for_manual_review_only"
        );
        assert_eq!(
            decision["promotion_gate"]["coverage_quality_audit"],
            "manual_review_required_before_unblocking_scanned_pdf_rows"
        );
        assert_eq!(decision["promotion_gate"]["factor_builder"], "blocked");
        assert_eq!(decision["promotion_gate"]["p310_status"], "blocked");
    }

    #[test]
    fn margin_detail_sync_request_forces_bounded_raw_dataset() {
        let req = MarginDetailSyncReq {
            symbols: vec!["000001.SZ".to_string()],
            start_date: Some("20260623".to_string()),
            end_date: Some("20260623".to_string()),
            data_version_id: Some("margin-detail-smoke".to_string()),
            background: true,
        }
        .into_sync_task_req();

        assert_eq!(req.dataset, "margin_detail");
        assert_eq!(req.source, "tushare:margin_detail");
        assert_eq!(req.mode.as_deref(), Some("bounded_raw_sync"));
        assert_eq!(req.symbols, vec!["000001.SZ".to_string()]);
        assert_eq!(
            req.reason.as_deref(),
            Some("p3.22 margin detail bounded raw sync")
        );
        assert!(req.background);
    }

    #[test]
    fn margin_detail_readiness_and_coverage_gate_before_p310() {
        assert_eq!(
            decide_margin_detail_readiness(false, 0, 0, 0, 0)["admission_decision"],
            "apply_schema_before_sync"
        );
        assert_eq!(
            decide_margin_detail_readiness(true, 0, 0, 0, 0)["admission_decision"],
            "bounded_sync_required_before_coverage_audit"
        );
        assert_eq!(
            decide_margin_detail_readiness(true, 4_000, 1, 0, 0)["admission_decision"],
            "raw_pit_failed"
        );
        assert_eq!(
            decide_margin_detail_readiness(true, 4_000, 0, 1, 0)["admission_decision"],
            "raw_publication_time_failed"
        );
        assert_eq!(
            decide_margin_detail_readiness(true, 4_000, 0, 0, 1)["admission_decision"],
            "raw_core_nonnegative_failed"
        );

        assert_eq!(
            decide_margin_detail_coverage_audit(
                true,
                1_500_000,
                3_030,
                3_030,
                1.0,
                0.35,
                0,
                0,
                0,
                0,
                "blocked_same_family_high_correlation",
            )["admission_decision"],
            "correlation_readiness_not_passed"
        );
        assert_eq!(
            decide_margin_detail_coverage_audit(
                true,
                1_500_000,
                3_030,
                3_030,
                1.0,
                0.35,
                0,
                0,
                0,
                1,
                "passed_low_linear_correlation_screen",
            )["admission_decision"],
            "full_history_coverage_failed"
        );
        assert_eq!(
            decide_margin_detail_coverage_audit(
                true,
                1_500_000,
                2_920,
                3_030,
                0.9636963696369637,
                0.35,
                0,
                0,
                0,
                0,
                "passed_low_linear_correlation_screen",
            )["admission_decision"],
            "full_history_coverage_failed"
        );
        assert_eq!(
            decide_margin_detail_coverage_audit(
                true,
                1_500_000,
                3_030,
                3_030,
                1.0,
                0.35,
                0,
                0,
                0,
                0,
                "passed_low_linear_correlation_screen",
            )["p310_status"],
            "ready_for_p310_diagnostics_only"
        );
    }

    #[test]
    fn margin_detail_correlation_decision_blocks_same_family_signal() {
        assert_eq!(
            margin_detail_correlation_decision(Some(0.72)),
            "blocked_same_family_high_correlation"
        );
        assert_eq!(
            margin_detail_correlation_decision(Some(0.55)),
            "caution_same_family_medium_correlation_requires_manual_review"
        );
        assert_eq!(
            margin_detail_correlation_decision(Some(0.30)),
            "passed_low_linear_correlation_screen"
        );
        assert_eq!(
            margin_detail_correlation_decision(None),
            "blocked_until_correlation_sample_available"
        );
    }

    #[test]
    fn futures_price_chain_schema_contract_blocks_training_until_mapping_and_pit_audit() {
        let contract = phase7_futures_price_chain_schema_contract();

        assert_eq!(contract["source_id"], "futures_price_chain");
        assert_eq!(
            contract["admission_decision"],
            "schema_mapping_available_at_audit_required_before_sync"
        );
        assert_eq!(
            contract["pit_policy"]["intraday_stock_decision_rule"],
            "use_previous_available_futures_trade_date_until_source_published_at_is_audited"
        );
        assert_eq!(contract["promotion_gate"]["p310_status"], "not_started");
        assert_eq!(contract["raw_tables"][0]["table"], "market_futures_daily");
        assert_eq!(
            contract["mapping_tables"][0]["table"],
            "market_futures_product_exposure_mapping_pit"
        );
        assert_eq!(
            contract["mapping_tables"][1]["table"],
            "market_futures_product_exclusion_gate_pit"
        );
    }

    #[test]
    fn equity_pledge_schema_contract_separates_stat_snapshot_from_detail_available_at() {
        let contract = phase7_equity_pledge_schema_contract();

        assert_eq!(contract["source_id"], "equity_pledge_pressure");
        assert_eq!(contract["stage"], "P3.20B");
        assert_eq!(contract["ddl_path"], "sql/phase7_equity_pledge_source.sql");
        assert_eq!(
            contract["raw_sources"][0]["native_available_at_candidate"],
            "not_native_end_date_is_measurement_date"
        );
        assert_eq!(
            contract["raw_sources"][1]["native_available_at_candidate"],
            "ann_date"
        );
        assert_eq!(contract["tables"][0]["table"], "market_stock_pledge_stat");
        assert_eq!(contract["tables"][1]["table"], "market_stock_pledge_detail");
        assert_eq!(
            contract["available_at_policy"]["pledge_detail"],
            "ann_date is native available_at candidate and must be persisted as available_at."
        );
        assert_eq!(contract["promotion_gate"]["v19_train_selection"], "blocked");

        let ddl = include_str!("../../../../sql/phase7_equity_pledge_source.sql");
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS market_stock_pledge_stat"));
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS market_stock_pledge_detail"));
        assert!(ddl.contains("available_at >= end_date"));
        assert!(ddl.contains("available_at >= ann_date"));
        assert!(ddl.contains("PRIMARY KEY (symbol, ann_date, source_row_hash)"));
        assert_eq!(
            contract["tables"][1]["natural_key"],
            json!(["symbol", "ann_date", "source_row_hash"])
        );
        assert!(contract["tables"][1]["nullable_source_fields"]
            .as_array()
            .unwrap()
            .contains(&json!("pledge_start_date")));
    }

    #[test]
    fn shareholder_structure_schema_contract_requires_four_ann_date_tables() {
        let contract = phase7_shareholder_structure_schema_contract();

        assert_eq!(contract["source_id"], "shareholder_structure");
        assert_eq!(contract["stage"], "P3.21B");
        assert_eq!(
            contract["ddl_path"],
            "sql/phase7_shareholder_structure_source.sql"
        );
        assert_eq!(contract["raw_sources"][0]["api"], "stk_holdernumber");
        assert_eq!(contract["raw_sources"][1]["api"], "top10_holders");
        assert_eq!(contract["raw_sources"][2]["api"], "top10_floatholders");
        assert_eq!(contract["raw_sources"][3]["api"], "stk_holdertrade");
        assert_eq!(
            contract["available_at_policy"]["default"],
            "available_at equals native ann_date for all four shareholder_structure raw tables; end_date is a measurement period only."
        );
        assert_eq!(contract["promotion_gate"]["v19_train_selection"], "blocked");

        let ddl = include_str!("../../../../sql/phase7_shareholder_structure_source.sql");
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS market_stock_holder_number"));
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS market_stock_top10_holders"));
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS market_stock_top10_float_holders"));
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS market_stock_holder_trade"));
        assert!(ddl.contains("PRIMARY KEY (symbol, ann_date, end_date)"));
        assert!(ddl.contains("PRIMARY KEY (symbol, ann_date, end_date, source_row_hash)"));
        assert!(ddl.contains("available_at >= ann_date"));
    }

    #[test]
    fn shareholder_structure_raw_schema_lands_period_anomalies_for_audit() {
        let contract = phase7_shareholder_structure_schema_contract();
        assert_eq!(
            contract["available_at_policy"]["period_snapshot_rule"],
            "holdernumber/top10/top10_float rows with ann_date before end_date land as raw source anomalies, but block factor/P3.10/WFA until repaired, excluded, or gated."
        );

        let ddl = include_str!("../../../../sql/phase7_shareholder_structure_source.sql");
        assert!(!ddl.contains("CONSTRAINT market_stock_holder_number_period_pit_check"));
        assert!(!ddl.contains("CONSTRAINT market_stock_top10_holders_period_pit_check"));
        assert!(!ddl.contains("CONSTRAINT market_stock_top10_float_holders_period_pit_check"));
        assert!(
            ddl.contains("DROP CONSTRAINT IF EXISTS market_stock_holder_number_period_pit_check")
        );

        let expected_constraints = shareholder_structure_expected_schema();
        let holder_number_constraints = expected_constraints
            .iter()
            .find(|(table, _)| *table == "market_stock_holder_number")
            .map(|(_, constraints)| constraints)
            .expect("holder number schema");
        assert!(!holder_number_constraints
            .iter()
            .any(|constraint| constraint.contains("period_pit_check")));
    }

    #[test]
    fn shareholder_structure_sync_request_forces_bounded_raw_dataset() {
        let req = ShareholderStructureSyncReq {
            symbols: vec!["000001.SZ".to_string()],
            source_filters: Vec::new(),
            start_date: Some("20140101".to_string()),
            end_date: Some("20260623".to_string()),
            data_version_id: Some("holder-smoke".to_string()),
            background: true,
        }
        .into_sync_task_req();

        assert_eq!(req.dataset, "shareholder_structure");
        assert_eq!(req.source, "tushare:shareholder_structure");
        assert_eq!(req.mode.as_deref(), Some("bounded_raw_sync"));
        assert_eq!(req.symbols, vec!["000001.SZ".to_string()]);
        assert_eq!(
            req.reason.as_deref(),
            Some("p3.21 shareholder structure bounded raw sync")
        );
        assert!(req.background);
    }

    #[test]
    fn shareholder_structure_sync_request_preserves_source_filters_for_staged_history_sync() {
        let req = ShareholderStructureSyncReq {
            symbols: Vec::new(),
            source_filters: vec!["holder_number".to_string(), "holder_trade".to_string()],
            start_date: Some("20140101".to_string()),
            end_date: Some("20260623".to_string()),
            data_version_id: Some("shareholder-low-fanout".to_string()),
            background: true,
        }
        .into_sync_task_req();

        assert_eq!(
            req.source_filters,
            vec!["holder_number".to_string(), "holder_trade".to_string()]
        );
        assert_eq!(req.dataset, "shareholder_structure");
    }

    #[test]
    fn shareholder_structure_coverage_decision_requires_breadth_pit_and_duplicates() {
        assert_eq!(
            decide_shareholder_structure_coverage_audit(false, 0, 0.0, 0, 0, 0, 0)
                ["admission_decision"],
            "apply_schema_before_sync"
        );
        assert_eq!(
            decide_shareholder_structure_coverage_audit(true, 0, 0.0, 0, 0, 0, 0)
                ["admission_decision"],
            "bounded_sync_required_before_coverage_audit"
        );
        assert_eq!(
            decide_shareholder_structure_coverage_audit(true, 10_000, 0.5, 1, 0, 0, 0)
                ["admission_decision"],
            "raw_pit_failed"
        );
        assert_eq!(
            decide_shareholder_structure_coverage_audit(true, 10_000, 0.5, 0, 1, 0, 0)
                ["admission_decision"],
            "duplicate_source_rows_failed"
        );
        assert_eq!(
            decide_shareholder_structure_coverage_audit(true, 10_000, 0.5, 0, 0, 1, 0)
                ["admission_decision"],
            "raw_quality_failed"
        );
        assert_eq!(
            decide_shareholder_structure_coverage_audit(true, 500_000, 0.80, 0, 0, 0, 1)
                ["admission_decision"],
            "full_history_coverage_failed"
        );
        assert_eq!(
            decide_shareholder_structure_coverage_audit(true, 100, 0.01, 0, 0, 0, 0)
                ["admission_decision"],
            "bounded_sample_passed_needs_full_history_sync"
        );
        assert_eq!(
            decide_shareholder_structure_coverage_audit(true, 500_000, 0.80, 0, 0, 0, 0)
                ["admission_decision"],
            "coverage_readiness_ready_for_p310_diagnostics"
        );
    }

    #[test]
    fn shareholder_structure_strict_low_fanout_gate_allows_clean_exclusion_scope_only() {
        assert_eq!(
            decide_shareholder_structure_strict_low_fanout_gate(false, 0, 0.0, 0, 0)
                ["admission_decision"],
            "apply_schema_before_strict_low_fanout_gate"
        );
        assert_eq!(
            decide_shareholder_structure_strict_low_fanout_gate(true, 500_000, 0.80, 1, 0)
                ["admission_decision"],
            "duplicate_admissible_rows_failed"
        );
        assert_eq!(
            decide_shareholder_structure_strict_low_fanout_gate(true, 500_000, 0.80, 0, 1)
                ["admission_decision"],
            "full_history_coverage_failed"
        );
        assert_eq!(
            decide_shareholder_structure_strict_low_fanout_gate(true, 100, 0.80, 0, 0)
                ["admission_decision"],
            "bounded_sample_passed_needs_more_admissible_history"
        );
        let ready = decide_shareholder_structure_strict_low_fanout_gate(true, 500_000, 0.80, 0, 0);
        assert_eq!(
            ready["gate_id"],
            "shareholder_structure_low_fanout_strict_pit_gate_v1"
        );
        assert_eq!(
            ready["admission_decision"],
            "strict_low_fanout_ready_for_p310_diagnostics"
        );
        assert_eq!(ready["p310_status"], "ready_for_p310_diagnostics_only");
        assert_eq!(ready["v19_train_selection"], "blocked");
    }

    #[test]
    fn shareholder_structure_sync_plan_estimates_year_batches_and_blocks_full_range_blast() {
        let batches = (2014..=2026)
            .map(|year| ShareholderStructureSyncPlanBatch {
                year,
                start_date: NaiveDate::from_ymd_opt(year, 1, 1).unwrap(),
                end_date: NaiveDate::from_ymd_opt(year, 12, 31).unwrap(),
                symbol_count: 4_300,
                quarter_count: 4,
            })
            .collect::<Vec<_>>();

        let plan = shareholder_structure_sync_plan_response(
            NaiveDate::from_ymd_opt(2014, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2026, 12, 31).unwrap(),
            batches,
        );

        assert_eq!(plan["mode"], "read_only_bounded_sync_plan");
        assert_eq!(plan["safe_to_run_full_range"], false);
        assert_eq!(plan["recommended_batch_granularity"], "year");
        assert_eq!(plan["batch_count"], 13);
        assert_eq!(plan["estimated_total_units"], 447304);
        assert_eq!(
            plan["batches"][0]["recommended_request"]["data_version_id"],
            "shareholder-structure-2014"
        );
        assert_eq!(
            plan["batches"][0]["recommended_low_fanout_request"]["source_filters"],
            json!(["holder_number", "holder_trade"])
        );
        assert_eq!(
            plan["batches"][0]["recommended_top10_request"]["source_filters"],
            json!(["top10_holders", "top10_float_holders"])
        );
        assert_eq!(plan["batches"][0]["estimated_units"], 34408);
        assert_eq!(plan["batches"][0]["symbol_quarter_units"], 34400);
        assert_eq!(plan["batches"][0]["global_ann_date_units"], 8);
    }

    #[test]
    fn futures_price_chain_readiness_decision_blocks_empty_schema_from_training() {
        let decision = decide_futures_price_chain_readiness(true, 0, 0);

        assert_eq!(decision["schema_status"], "created");
        assert_eq!(decision["sync_status"], "not_started");
        assert_eq!(
            decision["admission_decision"],
            "schema_created_sync_required_before_coverage_audit"
        );
        assert_eq!(
            decision["p310_status"],
            "blocked_until_coverage_readiness_passes"
        );
    }

    #[test]
    fn futures_price_chain_readiness_blocks_raw_synced_without_mapping() {
        let decision = decide_futures_price_chain_readiness(true, 100, 0);

        assert_eq!(decision["schema_status"], "created");
        assert_eq!(decision["sync_status"], "raw_synced_mapping_missing");
        assert_eq!(
            decision["admission_decision"],
            "mapping_required_before_feature_or_p310"
        );
        assert_eq!(decision["v19_train_selection"], "blocked");
    }

    #[test]
    fn futures_price_chain_extracts_product_symbol_from_contract_codes() {
        assert_eq!(
            futures_price_chain_product_symbol_from_daily_ts_code("CU1811.SHF"),
            Some("CU".to_string())
        );
        assert_eq!(
            futures_price_chain_product_symbol_from_daily_ts_code("if1811.CFX"),
            Some("IF".to_string())
        );
        assert_eq!(
            futures_price_chain_product_symbol_from_daily_ts_code("  rb2405.SHFE "),
            Some("RB".to_string())
        );
        assert_eq!(
            futures_price_chain_product_symbol_from_daily_ts_code("A1901.DCE"),
            Some("A".to_string())
        );
        assert_eq!(
            futures_price_chain_product_symbol_from_daily_ts_code("AGL.SHF"),
            None
        );
        assert_eq!(
            futures_price_chain_product_symbol_from_daily_ts_code("PTA.ZCE"),
            None
        );
        assert_eq!(
            futures_price_chain_product_symbol_from_daily_ts_code("TL.CFX"),
            None
        );
        assert_eq!(
            futures_price_chain_product_symbol_from_daily_ts_code("TL1.CFX"),
            None
        );
        assert_eq!(
            futures_price_chain_product_symbol_from_daily_ts_code("SCTAS2011.INE"),
            Some("SCTAS".to_string())
        );
        assert_eq!(
            futures_price_chain_product_symbol_from_daily_ts_code("1811.SHF"),
            None
        );
    }

    #[test]
    fn futures_price_chain_mapping_audit_blocks_zero_mapping_rows() {
        let decision = decide_futures_price_chain_mapping_audit(true, 42, 0, 0, 42, 0, 0, 0, 0, 0);

        assert_eq!(
            decision["admission_decision"],
            "mapping_required_before_feature_or_p310"
        );
        assert_eq!(decision["raw_product_count"], 42);
        assert_eq!(decision["mapped_product_count"], 0);
        assert_eq!(decision["excluded_product_count"], 0);
        assert_eq!(decision["covered_product_count"], 0);
        assert_eq!(decision["missing_product_count"], 42);
        assert_eq!(decision["v19_train_selection"], "blocked");
        assert_eq!(decision["wfa_status"], "blocked");
    }

    #[test]
    fn futures_price_chain_mapping_audit_counts_exclusion_gate_as_covered_not_mapped() {
        let decision = decide_futures_price_chain_mapping_audit(true, 56, 0, 6, 50, 0, 0, 0, 0, 0);

        assert_eq!(
            decision["admission_decision"],
            "mapping_coverage_incomplete_before_feature_or_p310"
        );
        assert_eq!(decision["mapped_product_count"], 0);
        assert_eq!(decision["excluded_product_count"], 6);
        assert_eq!(decision["covered_product_count"], 6);
        assert_eq!(decision["missing_product_count"], 50);
        assert_eq!(
            decision["next_step"],
            "complete_versioned_mapping_for_remaining_raw_products_or_pre_register_exclusion_gate"
        );
    }

    #[test]
    fn futures_price_chain_mapping_audit_sql_covers_raw_sources_and_pit_mapping() {
        let raw_sql = futures_price_chain_raw_product_summary_sql();
        assert!(raw_sql.contains("market_futures_daily"));
        assert!(raw_sql.contains("market_futures_warehouse_receipt"));
        assert!(raw_sql.contains("market_futures_holding_rank"));
        assert!(raw_sql.contains("substring(ts_code from '^([A-Za-z]+)[0-9]{4}"));
        assert!(raw_sql.contains("substring(symbol from '^[A-Za-z]+'"));
        assert!(raw_sql.contains("length(product_symbol_raw) > 2"));
        assert!(raw_sql.contains("right(product_symbol_raw, 1) = 'L'"));
        assert!(raw_sql.contains("available_at < trade_date"));

        let mapping_sql = futures_price_chain_mapping_summary_sql();
        assert!(mapping_sql.contains("market_futures_product_exposure_mapping_pit"));
        assert!(mapping_sql.contains("exposure_type NOT IN ('sw_industry', 'stock_symbol')"));
        assert!(mapping_sql.contains("valid_to IS NOT NULL AND valid_to < valid_from"));
        assert!(mapping_sql.contains("available_at < valid_from"));

        let exclusion_sql = futures_price_chain_exclusion_summary_sql();
        assert!(exclusion_sql.contains("market_futures_product_exclusion_gate_pit"));
        assert!(exclusion_sql.contains("gate_scope = 'futures_price_chain_factor'"));
        assert!(exclusion_sql.contains("valid_to IS NOT NULL AND valid_to < valid_from"));
    }

    #[test]
    fn futures_price_chain_raw_product_sql_normalizes_known_aliases_before_mapping_gate() {
        for sql in [
            futures_price_chain_raw_product_summary_sql(),
            futures_price_chain_coverage_breakdown_sql(),
            futures_price_chain_raw_endpoint_breakdown_sql(),
        ] {
            assert!(sql.contains("right(product_symbol_raw, 4) = 'ACTV'"));
            assert!(sql.contains("left(product_symbol_raw, length(product_symbol_raw) - 4)"));
            assert!(sql.contains("WHEN product_symbol_raw = 'PTA' THEN 'TA'"));
        }
    }

    #[test]
    fn futures_price_chain_exclusion_seed_blocks_non_industry_derivatives() {
        let sql = include_str!("../../../../sql/phase7_futures_price_chain_source.sql");

        assert!(sql.contains("('IM', 'futures_price_chain_factor', 'financial_index_future'"));
        assert!(sql.contains("中证1000股指期货"));
        assert!(sql.contains("('IO', 'futures_price_chain_factor', 'non_industry_derivative'"));
        assert!(sql.contains("沪深300股指期权"));
        assert!(sql.contains("('SCTAS', 'futures_price_chain_factor', 'non_industry_derivative'"));
        assert!(sql.contains("原油 TAS"));
    }

    #[test]
    fn futures_price_chain_mapping_seed_contains_first_high_confidence_product_batch() {
        let sql = include_str!("../../../../sql/phase7_futures_price_chain_source.sql");

        assert!(sql.contains("INSERT INTO market_futures_product_exposure_mapping_pit"));
        assert!(sql.contains("('CU', 'sw_industry', '801050.SI'"));
        assert!(sql.contains("('RB', 'sw_industry', '801040.SI'"));
        assert!(sql.contains("('SC', 'sw_industry', '801960.SI'"));
        assert!(sql.contains("('TA', 'sw_industry', '801030.SI'"));
        assert!(sql.contains("('FG', 'sw_industry', '801710.SI'"));
        assert!(sql.contains("p319q-product-sw2021-l1-direct-commodity-v1"));
    }

    #[test]
    fn futures_price_chain_mapping_seed_contains_second_evidence_backed_product_batch() {
        let sql = include_str!("../../../../sql/phase7_futures_price_chain_source.sql");

        assert!(sql.contains("('A', 'sw_industry', '801010.SI', 1"));
        assert!(sql.contains("('CF', 'sw_industry', '801130.SI', 1"));
        assert!(sql.contains("('I', 'sw_industry', '801040.SI', -1"));
        assert!(sql.contains("('PS', 'sw_industry', '801730.SI', 1"));
        assert!(sql.contains("('SP', 'sw_industry', '801140.SI', -1"));
        assert!(sql.contains("p319q-product-sw2021-l1-evidence-backed-v1"));
    }

    #[test]
    fn futures_price_chain_mapping_seed_covers_officially_sourced_residual_products() {
        let sql = include_str!("../../../../sql/phase7_futures_price_chain_source.sql");

        assert!(sql.contains("('EC', 'sw_industry', '801170.SI', 1"));
        assert!(sql.contains("('OP', 'sw_industry', '801140.SI', 1"));
        assert!(sql.contains("('ME', 'sw_industry', '801030.SI', 1"));
        assert!(sql.contains("('TC', 'sw_industry', '801950.SI', 1"));
        assert!(sql.contains("('ER', 'sw_industry', '801010.SI', 1"));
        assert!(sql.contains("('WS', 'sw_industry', '801010.SI', 1"));
        assert!(sql.contains("p319q-product-sw2021-l1-official-residual-v1"));
    }

    #[test]
    fn futures_price_chain_coverage_audit_sql_breaks_down_year_endpoint_product_exchange() {
        let coverage_sql = futures_price_chain_coverage_breakdown_sql();

        assert!(coverage_sql.contains("market_futures_daily"));
        assert!(coverage_sql.contains("market_futures_warehouse_receipt"));
        assert!(coverage_sql.contains("market_futures_holding_rank"));
        assert!(coverage_sql.contains("EXTRACT(YEAR FROM trade_date)"));
        assert!(
            coverage_sql.contains("GROUP BY endpoint, trade_year, product_symbol, exchange_key")
        );
        assert!(coverage_sql.contains("available_at < trade_date"));

        let attempt_sql = futures_price_chain_sync_attempt_breakdown_sql();
        assert!(attempt_sql.contains("data_sync_attempt"));
        assert!(attempt_sql.contains("futures_price_chain_daily"));
        assert!(attempt_sql.contains("futures_price_chain_wsr"));
        assert!(attempt_sql.contains("futures_price_chain_holding"));
        assert!(attempt_sql.contains("error_message IS NOT NULL"));
        assert!(attempt_sql.contains("futures_calendar"));
        assert!(attempt_sql.contains("NOT EXISTS (SELECT 1 FROM futures_calendar)"));
        assert!(attempt_sql.contains("non_open_attempt_count"));
    }

    #[test]
    fn futures_price_chain_coverage_audit_blocks_until_mapping_is_complete() {
        let decision = decide_futures_price_chain_coverage_audit(true, 6_349, 56, 6, 50, 0, 0);

        assert_eq!(
            decision["admission_decision"],
            "mapping_coverage_incomplete_before_feature_or_p310"
        );
        assert_eq!(
            decision["p310_status"],
            "blocked_until_mapping_and_coverage_readiness_pass"
        );
        assert_eq!(decision["wfa_status"], "blocked");
        assert_eq!(decision["v19_train_selection"], "blocked");
    }

    #[test]
    fn futures_price_chain_coverage_promotion_gate_opens_only_p310_when_ready() {
        let decision = decide_futures_price_chain_coverage_audit(true, 27_279_522, 94, 94, 0, 0, 0);
        let promotion_gate = futures_price_chain_coverage_promotion_gate(&decision);

        assert_eq!(promotion_gate["p310_status"], "ready_for_p310_diagnostics");
        assert_eq!(
            promotion_gate["factor_builder"],
            "ready_for_p310_diagnostics"
        );
        assert_eq!(promotion_gate["wfa_status"], "blocked");
        assert_eq!(promotion_gate["v19_train_selection"], "blocked");
    }

    #[test]
    fn futures_price_chain_mapping_template_sql_uses_sw2021_l1_targets() {
        let sql = futures_price_chain_industry_targets_sql();

        assert!(sql.contains("market_stock_industry_membership_pit"));
        assert!(sql.contains("classification_source = 'SW2021'"));
        assert!(sql.contains("industry_level = 'L1'"));
        assert!(sql.contains("index_code"));
        assert!(sql.contains("industry_code"));
    }

    #[test]
    fn futures_price_chain_mapping_candidate_validation_rejects_invalid_target_and_empty_evidence()
    {
        let raw_products = BTreeSet::from(["CU".to_string()]);
        let sw2021_targets = BTreeSet::from(["801050.SI".to_string()]);
        let candidate = FuturesPriceChainMappingCandidate {
            product_symbol: "CU".to_string(),
            exposure_type: "sw_industry".to_string(),
            exposure_code: "801999.SI".to_string(),
            direction: 1,
            weight: 1.0,
            valid_from: "2014-01-01".to_string(),
            valid_to: None,
            available_at: "2014-01-01".to_string(),
            source: "manual-review".to_string(),
            mapping_version: "p319n-test".to_string(),
            evidence: json!({}),
        };

        let result = validate_futures_price_chain_mapping_candidate(
            &candidate,
            &raw_products,
            &sw2021_targets,
        );

        assert!(!result.passed);
        assert!(result
            .errors
            .contains(&"unknown_sw2021_l1_exposure_code".to_string()));
        assert!(result.errors.contains(&"evidence_required".to_string()));
    }

    #[test]
    fn futures_price_chain_mapping_candidate_decision_blocks_incomplete_coverage() {
        let decision = decide_futures_price_chain_mapping_candidate_validation(57, 56, 0, 1);

        assert_eq!(
            decision["admission_decision"],
            "mapping_candidate_coverage_incomplete_before_insert"
        );
        assert_eq!(decision["write_enabled"], false);
        assert_eq!(decision["v19_train_selection"], "blocked");
    }

    #[test]
    fn phase7_p319_candidate_admission_reflects_mapping_audit_missing_products() {
        let mapping_audit = json!({
            "decision": decide_futures_price_chain_mapping_audit(true, 12, 8, 0, 4, 0, 0, 0, 0, 0),
            "raw_product_count": 12,
            "mapped_product_count": 8,
            "excluded_product_count": 0,
            "missing_product_count": 4,
            "missing_products": ["AL", "CU", "IF", "RB"],
        });
        let admission = phase7_p319_candidate_admission_sources(Some(&mapping_audit));
        let candidates = admission["candidates"]
            .as_array()
            .expect("p319 admission has candidates");
        let operations = candidates
            .iter()
            .find(|candidate| {
                candidate["source_id"]
                    .as_str()
                    .map(|source| source == "futures_price_chain")
                    .unwrap_or(false)
            })
            .expect("operations candidate");

        assert_eq!(
            operations["admission_decision"],
            "mapping_coverage_incomplete_before_feature_or_p310"
        );
        assert_eq!(operations["wfa_status"], "blocked");
        assert_eq!(operations["v19_train_selection"], "blocked");
        assert_eq!(
            operations["futures_price_chain_readiness"]["missing_product_count"],
            4
        );
    }

    #[test]
    fn phase7_p319_candidate_admission_keeps_futures_stopped_after_data_gate_ready() {
        let coverage_ready = json!({
            "decision": decide_futures_price_chain_coverage_audit(true, 1000, 10, 10, 0, 0, 0),
            "raw_product_count": 10,
            "covered_product_count": 10,
        });
        let admission = phase7_p319_candidate_admission_sources(Some(&coverage_ready));
        let candidates = admission["candidates"]
            .as_array()
            .expect("p319 admission has candidates");
        let operations = candidates
            .iter()
            .find(|candidate| {
                candidate["source_id"]
                    .as_str()
                    .map(|source| source == "futures_price_chain")
                    .unwrap_or(false)
            })
            .expect("operations candidate");

        assert_eq!(
            operations["admission_decision"],
            "stopped_after_p310_component_economics_failed"
        );
        assert_eq!(operations["p310_status"], "completed_failed_economics");
        assert_eq!(
            operations["futures_price_chain_readiness"]["decision"]["admission_decision"],
            "coverage_readiness_ready_for_p310_diagnostics"
        );
    }

    #[test]
    fn futures_price_chain_sync_request_forces_bounded_raw_dataset() {
        let req = FuturesPriceChainSyncReq {
            symbols: vec!["CU".to_string()],
            exchanges: vec!["SHFE".to_string()],
            start_date: Some("20181113".to_string()),
            end_date: Some("20181113".to_string()),
            data_version_id: Some("task-1".to_string()),
            background: true,
        }
        .into_sync_task_req();

        assert_eq!(req.dataset, "futures_price_chain");
        assert_eq!(req.source, "tushare:futures_price_chain");
        assert_eq!(req.mode.as_deref(), Some("bounded_raw_sync"));
        assert_eq!(req.symbols, vec!["CU".to_string()]);
        assert_eq!(req.exchanges, vec!["SHFE".to_string()]);
        assert!(req.background);
    }

    #[test]
    fn equity_pledge_sync_request_forces_bounded_raw_dataset() {
        let req = EquityPledgePressureSyncReq {
            symbols: vec!["000001.SZ".to_string()],
            start_date: Some("20240101".to_string()),
            end_date: Some("20260623".to_string()),
            data_version_id: Some("pledge-smoke".to_string()),
            background: true,
        }
        .into_sync_task_req();

        assert_eq!(req.dataset, "equity_pledge_pressure");
        assert_eq!(req.source, "tushare:equity_pledge_pressure");
        assert_eq!(req.mode.as_deref(), Some("bounded_raw_sync"));
        assert_eq!(req.symbols, vec!["000001.SZ".to_string()]);
        assert_eq!(
            req.reason.as_deref(),
            Some("p3.20 equity pledge pressure bounded raw sync")
        );
        assert!(req.background);
    }

    #[test]
    fn equity_pledge_readiness_blocks_until_schema_and_raw_pit_pass() {
        assert_eq!(
            decide_equity_pledge_readiness(false, 0, 0, 0)["admission_decision"],
            "apply_schema_before_sync"
        );
        assert_eq!(
            decide_equity_pledge_readiness(true, 0, 0, 0)["admission_decision"],
            "bounded_sync_required_before_coverage_audit"
        );
        assert_eq!(
            decide_equity_pledge_readiness(true, 10, 5, 2)["admission_decision"],
            "raw_pit_failed"
        );
        assert_eq!(
            decide_equity_pledge_readiness(true, 10, 5, 0)["admission_decision"],
            "coverage_readiness_audit_required_before_p310"
        );
    }

    #[test]
    fn equity_pledge_status_reflects_readiness_audit_in_candidate_lists() {
        let readiness = json!({
            "decision": decide_equity_pledge_readiness(true, 10, 5, 0),
            "schema_passed": true,
            "tables": [],
        });

        let sources = phase7_new_alpha_candidate_sources_with_equity_pledge_status(
            phase7_new_alpha_candidate_sources(),
            Some(&readiness),
        );
        let by_source: BTreeMap<&str, &Value> = sources
            .iter()
            .map(|source| (source["source"].as_str().unwrap(), source))
            .collect();

        assert_eq!(
            by_source["equity_pledge_pressure"]["readiness"],
            "raw_source_ready_for_coverage_audit"
        );

        let mut admission = phase7_p319_candidate_admission_sources(None);
        apply_p320_equity_pledge_readiness(&mut admission, &readiness);
        let pledge = admission["candidates"]
            .as_array()
            .unwrap()
            .iter()
            .find(|candidate| candidate["source_id"] == "equity_pledge_pressure")
            .unwrap();

        assert_eq!(
            pledge["admission_decision"],
            "stopped_after_p310_economics_failed"
        );
        assert_eq!(pledge["sync_status"], "full_history_raw_sync_completed");
        assert_eq!(pledge["p310_status"], "completed_failed_economics");
        assert_eq!(
            pledge["latest_raw_readiness"]["decision"]["admission_decision"],
            "coverage_readiness_audit_required_before_p310"
        );
    }

    #[test]
    fn equity_pledge_coverage_decision_requires_breadth_and_pit_before_p310() {
        assert_eq!(
            decide_equity_pledge_coverage_audit(false, 0, 0.0, 0, 0)["admission_decision"],
            "apply_schema_before_sync"
        );
        assert_eq!(
            decide_equity_pledge_coverage_audit(true, 0, 0.0, 0, 0)["admission_decision"],
            "bounded_sync_required_before_coverage_audit"
        );
        assert_eq!(
            decide_equity_pledge_coverage_audit(true, 100, 0.5, 1, 0)["admission_decision"],
            "raw_pit_failed"
        );
        assert_eq!(
            decide_equity_pledge_coverage_audit(true, 100, 0.01, 0, 0)["admission_decision"],
            "bounded_sample_passed_needs_full_history_sync"
        );
        assert_eq!(
            decide_equity_pledge_coverage_audit(true, 50_000, 0.35, 0, 0)["admission_decision"],
            "coverage_readiness_ready_for_p310_diagnostics"
        );
    }

    #[test]
    fn main_business_available_at_join_decision_passes_only_complete_pit_mapping() {
        let q1 = NaiveDate::from_ymd_opt(2023, 3, 31).unwrap();
        let ann = NaiveDate::from_ymd_opt(2023, 4, 25).unwrap();
        let mappings = vec![MainBusinessPeriodMapping {
            ts_code: "000001.SZ".to_string(),
            end_date: q1,
            available_at: Some(ann),
            source: Some("financial_statement".to_string()),
        }];

        let decision = decide_main_business_available_at_join_audit(1, &mappings);

        assert!(decision.passed);
        assert_eq!(decision.status, "passed");
        assert_eq!(
            decision.readiness,
            "available_at_join_ready_for_schema_design"
        );
        assert_eq!(decision.missing_mapping_count, 0);
        assert_eq!(decision.pit_violation_count, 0);
        assert_eq!(decision.source_counts.get("financial_statement"), Some(&1));
    }

    #[test]
    fn main_business_available_at_join_decision_blocks_missing_mapping() {
        let q1 = NaiveDate::from_ymd_opt(2023, 3, 31).unwrap();
        let mappings = vec![MainBusinessPeriodMapping {
            ts_code: "000001.SZ".to_string(),
            end_date: q1,
            available_at: None,
            source: None,
        }];

        let decision = decide_main_business_available_at_join_audit(1, &mappings);

        assert!(!decision.passed);
        assert_eq!(decision.status, "blocked_available_at_join_gaps");
        assert_eq!(decision.missing_mapping_count, 1);
        assert_eq!(decision.pit_violation_count, 0);
    }

    #[test]
    fn main_business_available_at_join_decision_blocks_available_at_before_period_end() {
        let q1 = NaiveDate::from_ymd_opt(2023, 3, 31).unwrap();
        let impossible_available_at = NaiveDate::from_ymd_opt(2023, 3, 1).unwrap();
        let mappings = vec![MainBusinessPeriodMapping {
            ts_code: "000001.SZ".to_string(),
            end_date: q1,
            available_at: Some(impossible_available_at),
            source: Some("financial_statement".to_string()),
        }];

        let decision = decide_main_business_available_at_join_audit(1, &mappings);

        assert!(!decision.passed);
        assert_eq!(decision.status, "blocked_pit_available_at_violations");
        assert_eq!(decision.missing_mapping_count, 0);
        assert_eq!(decision.pit_violation_count, 1);
    }

    #[test]
    fn main_business_readiness_requires_rows_periods_and_clean_pit() {
        assert_eq!(
            main_business_raw_source_readiness(0, 4, 4, 0, 0),
            "raw_source_missing"
        );
        assert_eq!(
            main_business_raw_source_readiness(100, 4, 4, 0, 2),
            "raw_source_pit_failed"
        );
        assert_eq!(
            main_business_raw_source_readiness(100, 4, 3, 0, 0),
            "period_sync_incomplete"
        );
        assert_eq!(
            main_business_raw_source_readiness(100, 4, 4, 1, 0),
            "period_sync_failed"
        );
        assert_eq!(
            main_business_raw_source_readiness(100, 4, 4, 0, 0),
            "raw_source_ready_for_full_history_coverage_audit"
        );
    }

    #[test]
    fn main_business_readiness_sql_audits_type_and_pit_source_table() {
        let sql = main_business_readiness_summary_sql();

        assert!(sql.contains("FROM market_stock_main_business"));
        assert!(sql.contains("business_type = $3"));
        assert!(sql.contains("available_at < end_date"));
        assert!(sql.contains("COUNT(DISTINCT end_date)"));
    }

    #[test]
    fn main_business_readiness_parses_available_at_mapping_gaps() {
        assert_eq!(main_business_missing_available_at_rows(None), 0);
        assert_eq!(main_business_missing_available_at_rows(Some("")), 0);
        assert_eq!(
            main_business_missing_available_at_rows(Some(
                "missing_available_at_rows=5907, out_of_universe_rows=42, raw_rows=37766"
            )),
            5907
        );
        assert_eq!(
            main_business_out_of_universe_rows(Some(
                "missing_available_at_rows=5907, out_of_universe_rows=42, raw_rows=37766"
            )),
            42
        );
    }

    #[test]
    fn phase7_permission_smoke_limit_is_bounded() {
        assert_eq!(phase7_permission_smoke_limit(None), 1);
        assert_eq!(phase7_permission_smoke_limit(Some(0)), 1);
        assert_eq!(phase7_permission_smoke_limit(Some(3)), 3);
        assert_eq!(
            phase7_permission_smoke_limit(Some(100)),
            PHASE7_PERMISSION_SMOKE_MAX_ROWS
        );
    }

    #[test]
    fn tushare_permission_error_classifier_recognizes_permission_and_auth() {
        assert_eq!(
            classify_tushare_permission_error("API error (code=2002): 没有权限"),
            "permission_denied"
        );
        assert_eq!(
            classify_tushare_permission_error("Authentication error: TUSHARE_TOKEN 未设置"),
            "auth_error"
        );
        assert_eq!(classify_tushare_permission_error("timeout"), "error");
    }

    #[test]
    fn optional_source_full_market_sync_requires_explicit_mode() {
        assert!(!optional_source_all_symbols_allowed(None));
        assert!(!optional_source_all_symbols_allowed(Some("")));
        assert!(!optional_source_all_symbols_allowed(Some("full")));
        assert!(optional_source_all_symbols_allowed(Some("full_market")));
    }

    #[test]
    fn cancel_transition_matches_task_lifecycle() {
        assert_eq!(sync_task_cancel_transition("pending"), Some("cancelled"));
        assert_eq!(
            sync_task_cancel_transition("running"),
            Some("cancel_requested")
        );
        assert_eq!(
            sync_task_cancel_transition("cancel_requested"),
            Some("cancel_requested")
        );
        assert_eq!(sync_task_cancel_transition("completed"), None);
        assert_eq!(sync_task_cancel_transition("failed"), None);
        assert_eq!(sync_task_cancel_transition("cancelled"), None);
    }
