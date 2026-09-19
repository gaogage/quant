//! phase7 数据源准入决策：公告订单容量 OCR/分析师修正 akshare/主营业务
//! available_at 的 readiness 判定与审计辅助。
use super::*;

pub(crate) fn decide_exchange_announcement_order_capacity_ocr_blocked_row_audit(
    total_rows: usize,
    ocr_text_count: usize,
    stable_hash_count: usize,
    availability_count: usize,
    quality_pass_count: usize,
    evidence_span_count: usize,
    runtime_missing_count: usize,
    ocr_error_count: usize,
    no_target_span_count: usize,
    incomplete_raw_link_count: usize,
) -> Value {
    let admission_decision = if total_rows == 0 {
        "blocked_no_scanned_pdf_rows_in_scope"
    } else if incomplete_raw_link_count > 0 {
        "blocked_incomplete_raw_pdf_link_metadata"
    } else if runtime_missing_count > 0 || ocr_text_count < total_rows {
        "blocked_ocr_runtime_missing_or_incomplete"
    } else if ocr_error_count > 0 {
        "blocked_ocr_runtime_errors"
    } else if stable_hash_count < total_rows {
        "blocked_unstable_ocr_text_hash"
    } else if availability_count < total_rows {
        "blocked_missing_raw_source_published_at_or_available_at_policy"
    } else if quality_pass_count < total_rows {
        "blocked_low_ocr_text_quality"
    } else if evidence_span_count == 0 {
        "blocked_ocr_text_has_no_order_capacity_evidence_spans"
    } else {
        "ocr_text_quality_audit_passed_manual_taxonomy_review_required"
    };
    let ocr_quality_status =
        if admission_decision == "ocr_text_quality_audit_passed_manual_taxonomy_review_required" {
            "passed_for_manual_review_only"
        } else {
            "blocked"
        };

    json!({
        "admission_decision": admission_decision,
        "total_rows": total_rows,
        "ocr_text_count": ocr_text_count,
        "stable_hash_count": stable_hash_count,
        "availability_count": availability_count,
        "quality_pass_count": quality_pass_count,
        "evidence_span_count": evidence_span_count,
        "runtime_missing_count": runtime_missing_count,
        "ocr_error_count": ocr_error_count,
        "no_target_span_count": no_target_span_count,
        "incomplete_raw_link_count": incomplete_raw_link_count,
        "ocr_quality_gate": {
            "status": ocr_quality_status,
            "policy": "OCR output is admission evidence only; it cannot unblock trainable rows until manual taxonomy precision review passes"
        },
        "promotion_gate": {
            "raw_backfill": if admission_decision == "ocr_text_quality_audit_passed_manual_taxonomy_review_required" {
                "manual_review_required"
            } else {
                "blocked"
            },
            "coverage_quality_audit": if admission_decision == "ocr_text_quality_audit_passed_manual_taxonomy_review_required" {
                "manual_review_required_before_unblocking_scanned_pdf_rows"
            } else {
                "blocked"
            },
            "factor_builder": "blocked",
            "p310_status": "blocked",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        },
        "next_step": if admission_decision == "ocr_text_quality_audit_passed_manual_taxonomy_review_required" {
            "manual_review_ocr_text_and_evidence_spans_then_design_auditable_raw_backfill_or_exclusion_policy"
        } else if runtime_missing_count > 0 {
            "install_or_configure_isolated_ocr_runtime_then_rerun_this_read_only_audit"
        } else {
            "repair_ocr_quality_or_pre_register_scanned_pdf_exclusion_scope_then_rerun"
        }
    })
}

pub(crate) fn summarize_exchange_announcement_detail_probes(
    probes: &[Value],
) -> ExchangeAnnouncementDetailProbeSummary {
    let fetched_text_count = probes
        .iter()
        .filter(|probe| {
            probe.get("status").and_then(Value::as_str) == Some("ok")
                && probe
                    .get("text_content_type")
                    .and_then(Value::as_str)
                    .map(|content_type| content_type != "pdf")
                    .unwrap_or(true)
                && probe
                    .get("text_length")
                    .and_then(Value::as_u64)
                    .map(|length| length > 0)
                    .unwrap_or(false)
        })
        .count();
    let text_hash_count = probes
        .iter()
        .filter(|probe| {
            probe.get("status").and_then(Value::as_str) == Some("ok")
                && probe
                    .get("text_content_type")
                    .and_then(Value::as_str)
                    .map(|content_type| content_type != "pdf")
                    .unwrap_or(true)
                && probe.get("text_hash").and_then(Value::as_str).is_some()
        })
        .count();
    let source_published_at_count = probes
        .iter()
        .filter(|probe| {
            probe
                .get("source_published_at_quality")
                .and_then(Value::as_str)
                == Some("timestamp")
        })
        .count();
    let incomplete_link_metadata_count = probes
        .iter()
        .filter(|probe| {
            probe.get("status").and_then(Value::as_str) == Some("incomplete_link_metadata")
                || probe
                    .get("link_metadata")
                    .and_then(|metadata| metadata.get("metadata_complete"))
                    .and_then(Value::as_bool)
                    .map(|complete| !complete)
                    .unwrap_or(false)
        })
        .count();
    let pdf_parser_required_count = probes
        .iter()
        .filter(|probe| {
            probe.get("status").and_then(Value::as_str) == Some("pdf_text_parser_required")
                || probe.get("text_content_type").and_then(Value::as_str) == Some("pdf")
        })
        .count();

    ExchangeAnnouncementDetailProbeSummary {
        fetched_text_count,
        text_hash_count,
        source_published_at_count,
        incomplete_link_metadata_count,
        pdf_parser_required_count,
    }
}

pub(crate) fn validate_exchange_announcement_order_capacity_sync_request(
    req: &ExchangeAnnouncementOrderCapacitySyncReq,
) -> Result<ExchangeAnnouncementOrderCapacityValidatedSyncRequest, String> {
    if req.background {
        return Err(
            "exchange announcement order capacity P3.24I sync requires background=false".into(),
        );
    }

    let start_date = req
        .start_date
        .clone()
        .ok_or_else(|| "start_date is required".to_string())?;
    let end_date = req
        .end_date
        .clone()
        .ok_or_else(|| "end_date is required".to_string())?;
    let start = parse_optional_date(Some(start_date.as_str()))?
        .ok_or_else(|| "start_date is required".to_string())?;
    let end = parse_optional_date(Some(end_date.as_str()))?
        .ok_or_else(|| "end_date is required".to_string())?;
    if start > end {
        return Err("exchange announcement sync start_date cannot be after end_date".into());
    }
    let calendar_day_count = (end - start).num_days() + 1;
    if calendar_day_count > EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_SYNC_MAX_CALENDAR_DAYS {
        return Err(format!(
            "exchange announcement bounded sync resolved {} calendar days, above max {}. Use one tiny manually reviewed batch first.",
            calendar_day_count, EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_SYNC_MAX_CALENDAR_DAYS
        ));
    }

    let requested_symbols = exchange_announcement_order_capacity_csv_values(req.symbols.clone());
    let symbols = exchange_announcement_order_capacity_symbols(&requested_symbols);
    if symbols.is_empty() {
        return Err("exchange announcement sync requires at least one symbol".into());
    }
    let requested_categories =
        exchange_announcement_order_capacity_csv_values(req.categories.clone());
    let categories = exchange_announcement_order_capacity_categories(&requested_categories);
    if categories.is_empty() {
        return Err("exchange announcement sync requires at least one category".into());
    }
    let market = req
        .market
        .clone()
        .filter(|market| !market.trim().is_empty())
        .unwrap_or_else(|| "沪深京".to_string());
    let data_version_id = req.data_version_id.clone().unwrap_or_else(|| {
        format!(
            "exchange-announcement-order-capacity-{}-{}",
            start.format("%Y%m%d"),
            end.format("%Y%m%d")
        )
    });
    let python = akshare_analyst_revision_python_path(req.python.clone());
    let pdf_python =
        exchange_announcement_order_capacity_pdf_audit_python_path(req.pdf_python.clone());
    let query_count = symbols.len() * categories.len();

    Ok(ExchangeAnnouncementOrderCapacityValidatedSyncRequest {
        symbols,
        categories,
        market,
        start,
        end,
        calendar_day_count,
        query_count,
        data_version_id,
        python,
        pdf_python,
    })
}

pub(crate) fn quarter_index(date: NaiveDate) -> u32 {
    ((date.month() - 1) / 3) + 1
}

pub(crate) fn validate_exchange_announcement_order_capacity_bounded_sync_request(
    req: &ExchangeAnnouncementOrderCapacityBoundedSyncReq,
) -> Result<ExchangeAnnouncementOrderCapacityValidatedBoundedSyncRequest, String> {
    if req.background {
        return Err(
            "exchange announcement order capacity P3.24J bounded sync requires background=false"
                .into(),
        );
    }

    let start_date = req
        .start_date
        .clone()
        .ok_or_else(|| "start_date is required".to_string())?;
    let end_date = req
        .end_date
        .clone()
        .ok_or_else(|| "end_date is required".to_string())?;
    let start = parse_optional_date(Some(start_date.as_str()))?
        .ok_or_else(|| "start_date is required".to_string())?;
    let end = parse_optional_date(Some(end_date.as_str()))?
        .ok_or_else(|| "end_date is required".to_string())?;
    if start > end {
        return Err(
            "exchange announcement bounded sync start_date cannot be after end_date".into(),
        );
    }
    let batch_mode = req
        .batch
        .clone()
        .unwrap_or_else(|| "month".to_string())
        .trim()
        .to_ascii_lowercase();
    exchange_announcement_order_capacity_validate_bounded_window(start, end, &batch_mode)?;

    let requested_symbols = exchange_announcement_order_capacity_csv_values(req.symbols.clone());
    let symbols = exchange_announcement_order_capacity_symbols(&requested_symbols);
    if symbols.is_empty() {
        return Err("exchange announcement bounded sync requires at least one symbol".into());
    }
    let requested_categories =
        exchange_announcement_order_capacity_csv_values(req.categories.clone());
    let categories = exchange_announcement_order_capacity_categories(&requested_categories);
    if categories.is_empty() {
        return Err("exchange announcement bounded sync requires at least one category".into());
    }
    let slices = exchange_announcement_order_capacity_tiny_slices(start, end);
    let total_query_units = slices.len() * symbols.len() * categories.len();
    if total_query_units > EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_BOUNDED_SYNC_MAX_QUERY_UNITS {
        return Err(format!(
            "exchange announcement bounded sync resolved {} query units, above max {}. Narrow symbols/categories or run smaller month/quarter batches.",
            total_query_units, EXCHANGE_ANNOUNCEMENT_ORDER_CAPACITY_BOUNDED_SYNC_MAX_QUERY_UNITS
        ));
    }

    let market = req
        .market
        .clone()
        .filter(|market| !market.trim().is_empty())
        .unwrap_or_else(|| "沪深京".to_string());
    let data_version_id = req.data_version_id.clone().unwrap_or_else(|| {
        format!(
            "exann-oc-{}-{}",
            start.format("%Y%m%d"),
            end.format("%Y%m%d")
        )
    });
    let python = akshare_analyst_revision_python_path(req.python.clone());
    let pdf_python =
        exchange_announcement_order_capacity_pdf_audit_python_path(req.pdf_python.clone());
    let stop_on_audit_failure = req.stop_on_audit_failure.unwrap_or(true);

    Ok(
        ExchangeAnnouncementOrderCapacityValidatedBoundedSyncRequest {
            symbols,
            categories,
            market,
            start,
            end,
            calendar_day_count: (end - start).num_days() + 1,
            batch_mode,
            slices,
            total_query_units,
            data_version_id,
            python,
            pdf_python,
            stop_on_audit_failure,
        },
    )
}

pub(crate) fn exchange_announcement_event_type_from_title_and_spans(
    title: &str,
    spans: &Value,
) -> Option<String> {
    if exchange_announcement_order_capacity_taxonomy_risk_title_reason("日常经营", title).is_some()
    {
        return None;
    }
    exchange_announcement_event_type_from_spans(spans)
}

// 测试辅助函数：仅在 sync/tests.rs 的单元测试中调用，非测试编译时标记为允许死代码。
#[allow(dead_code)]
pub(crate) fn exchange_announcement_order_capacity_ocr_taxonomy_exclusion_reason(
    category: &str,
    title: &str,
) -> Option<&'static str> {
    let text = format!("{category}{title}");
    if text.contains("控股股东及其他关联方资金占用")
        || text.contains("非经营性资金占用")
        || text.contains("关联资金往来情况汇总表")
    {
        return Some("special_report_related_party_funds");
    }
    if text.contains("财务公司关联交易")
        || text.contains("存款、贷款等金融业务")
        || text.contains("金融业务的专项说明")
    {
        return Some("special_report_related_party_finance");
    }
    if text.contains("专项说明")
        && (text.contains("审计")
            || text.contains("资金占用")
            || text.contains("关联方")
            || text.contains("关联交易")
            || text.contains("财务公司"))
    {
        return Some("special_report_audit_or_related_party");
    }
    if text.contains("募集资金")
        && (text.contains("存放与使用") || text.contains("存放和使用"))
        && (text.contains("鉴证报告") || text.contains("专项核查报告") || text.contains("专项报告"))
    {
        return Some("special_report_fundraising_use_assurance");
    }
    if text.contains("审计报告") {
        return Some("audit_report");
    }
    if text.contains("法律意见书") {
        return Some("legal_opinion");
    }
    if text.contains("财务顾问报告") {
        return Some("financial_advisor_report");
    }
    None
}

pub(crate) fn exchange_announcement_order_capacity_taxonomy_risk_title_reason(
    category: &str,
    title: &str,
) -> Option<&'static str> {
    let text = format!("{category}{title}");
    let is_true_operating_target = text.contains("签署《关于进一步加强和深化合作的协议》")
        || text.contains("合资建厂")
        || text.contains("签订日常经营重大合同")
        || text.contains("投资建设高效电池产能")
        || text.contains("投资建设产能项目");
    if is_true_operating_target {
        return None;
    }

    if text.contains("计提减值准备")
        || text.contains("募投项目")
        || text.contains("募集资金")
        || text.contains("关联交易")
        || text.contains("授信额度")
        || text.contains("注册资本")
        || text.contains("工商变更")
        || text.contains("实际控制人")
        || text.contains("控制权")
        || text.contains("股份质押")
        || text.contains("财务报告")
        || text.contains("年度报告")
        || text.contains("半年度报告")
        || text.contains("季度报告")
        || text.contains("主要经营数据")
        || text.contains("股权投资基金")
        || text.contains("投资基金")
        || text.contains("风险评估报告")
        || text.contains("H股发行")
        || text.contains("发行H股")
        || text.contains("H股股票")
        || text.contains("上市审计机构")
        || text.contains("章程")
        || text.contains("审计报告")
        || text.contains("审计机构")
        || text.contains("资产减值")
        || text.contains("公募REITs")
        || text.contains("REITs")
        || text.contains("收购控股子公司")
        || text.contains("董事会工作报告")
        || text.contains("监事会工作报告")
        || text.contains("内部控制")
        || text.contains("会计师事务所")
        || text.contains("审计委员会")
        || text.contains("委托理财")
        || text.contains("套期保值")
        || text.contains("担保额度")
        || text.contains("担保的进展")
        || text.contains("提供担保")
        || text.contains("发行债券")
        || text.contains("公司章程")
        || text.contains("公司制度")
        || text.contains("制定及修订")
        || text.contains("独立董事")
        || text.contains("会计政策变更")
        || text.contains("社会责任报告")
        || text.contains("可持续发展报告")
        || text.contains("可持续发展")
        || text.contains("环境、社会及治理")
        || text.contains("ESG")
        || text.contains("估值提升计划")
        || text.contains("市值管理")
        || text.contains("质量回报双提升")
        || text.contains("履职情况")
        || text.contains("履行监督职责")
    {
        return Some("admin_finance_governance_false_positive");
    }

    None
}

pub(crate) fn exchange_announcement_order_capacity_probe_is_truncated(probe: &Value) -> bool {
    let row_count = probe.get("row_count").and_then(Value::as_i64).unwrap_or(0);
    let sample_count = probe
        .get("sample_rows")
        .and_then(Value::as_array)
        .map(|rows| rows.len() as i64)
        .unwrap_or(0);
    row_count > sample_count
}

pub(crate) fn exchange_announcement_raw_row_from_list_and_pdf_probe(
    list_row: &Value,
    category: &str,
    request_key: &str,
    pdf_probe: &Value,
    open_dates: &[NaiveDate],
    data_version_id: &str,
) -> Result<ExchangeAnnouncementOrderCapacityRawRow, String> {
    let announcement_url = list_row
        .get("公告链接")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "missing_announcement_url".to_string())?
        .trim()
        .to_string();
    let metadata = parse_cninfo_announcement_link_metadata(&announcement_url);
    if !metadata
        .get("metadata_complete")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Err("incomplete_link_metadata".to_string());
    }
    let announcement_id = metadata
        .get("announcement_id")
        .and_then(Value::as_str)
        .ok_or_else(|| "missing_announcement_id".to_string())?
        .to_string();
    let org_id = metadata
        .get("org_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let symbol = metadata
        .get("stock_code")
        .and_then(Value::as_str)
        .or_else(|| list_row.get("代码").and_then(Value::as_str))
        .ok_or_else(|| "missing_symbol".to_string())?
        .to_string();
    let announcement_time_raw = metadata
        .get("announcement_time")
        .and_then(Value::as_str)
        .or_else(|| list_row.get("公告时间").and_then(Value::as_str))
        .ok_or_else(|| "missing_announcement_time".to_string())?;
    let announcement_time = parse_exchange_announcement_date(announcement_time_raw)
        .ok_or_else(|| format!("invalid_announcement_time:{announcement_time_raw}"))?;
    let available_at = exchange_announcement_next_open_date(announcement_time, open_dates);

    let raw_payload = list_row.clone();
    let raw_payload_hash = akshare_stable_hash(&[
        "akshare".to_string(),
        "stock_zh_a_disclosure_report_cninfo".to_string(),
        announcement_id.clone(),
        symbol.clone(),
        serde_json::to_string(&raw_payload).unwrap_or_default(),
    ]);
    let evidence_spans = pdf_probe
        .get("evidence_spans")
        .cloned()
        .unwrap_or_else(|| json!([]));
    let announcement_title = list_row
        .get("公告标题")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let event_type =
        exchange_announcement_event_type_from_title_and_spans(&announcement_title, &evidence_spans);
    let parser_errors = pdf_probe
        .get("parser_errors")
        .cloned()
        .unwrap_or_else(|| json!([]));
    let timestamp_candidates = pdf_probe
        .get("timestamp_candidates")
        .cloned()
        .unwrap_or_else(|| json!([]));
    let pdf_metadata_keys = pdf_probe
        .get("pdf_metadata_keys")
        .cloned()
        .unwrap_or_else(|| json!([]));
    let source_published_at = pdf_probe
        .get("source_published_at")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(announcement_time_raw)
        .to_string();
    let parsed_source_published_at_ts = parse_exchange_announcement_timestamp(&source_published_at);
    let declared_source_published_at_quality = pdf_probe
        .get("source_published_at_quality")
        .and_then(Value::as_str)
        .filter(|quality| matches!(*quality, "timestamp" | "date_only_next_session"))
        .unwrap_or("date_only_next_session");
    let source_published_at_quality = if parsed_source_published_at_ts.is_some() {
        "timestamp".to_string()
    } else {
        declared_source_published_at_quality.to_string()
    };
    let (source_published_at_ts, source_published_date) =
        if source_published_at_quality == "timestamp" {
            (parsed_source_published_at_ts, None)
        } else {
            (None, Some(announcement_time))
        };
    let text_content = pdf_probe
        .get("text_sample")
        .and_then(Value::as_str)
        .map(str::to_string);
    let text_hash = pdf_probe
        .get("text_hash")
        .and_then(Value::as_str)
        .map(str::to_string);
    let parser_used = pdf_probe
        .get("parser_used")
        .and_then(Value::as_str)
        .map(str::to_string);
    let pdf_final_url = pdf_probe
        .get("final_url")
        .and_then(Value::as_str)
        .map(str::to_string);

    Ok(ExchangeAnnouncementOrderCapacityRawRow {
        vendor: "akshare".to_string(),
        vendor_endpoint: "stock_zh_a_disclosure_report_cninfo".to_string(),
        request_key: request_key.to_string(),
        symbol,
        symbol_name: list_row
            .get("简称")
            .and_then(Value::as_str)
            .map(str::to_string),
        announcement_id,
        org_id,
        announcement_category: category.to_string(),
        announcement_title,
        announcement_time,
        source_published_at,
        source_published_at_ts,
        source_published_date,
        source_published_at_quality,
        available_at,
        announcement_url,
        pdf_final_url,
        text_content,
        text_hash,
        timestamp_candidates,
        pdf_metadata_keys,
        raw_payload,
        raw_payload_hash,
        parser_used,
        parser_version: None,
        parser_errors,
        pdf_parse_status: exchange_announcement_pdf_parse_status(pdf_probe),
        event_type,
        evidence_spans,
    })
    .map(|mut row| {
        row.raw_payload = json!({
            "list_row": row.raw_payload,
            "pdf_probe_status": pdf_probe.get("status").cloned().unwrap_or(Value::Null),
            "data_version_id": data_version_id,
        });
        row
    })
}

pub(crate) fn decide_exchange_announcement_order_capacity_coverage_quality_audit(
    metrics: ExchangeAnnouncementOrderCapacityCoverageQualityMetrics,
) -> Value {
    let raw_quality_failed = metrics.pit_violation_rows > 0
        || metrics.missing_available_at_rows > 0
        || metrics.missing_source_published_at_quality_rows > 0
        || metrics.duplicate_announcement_id_rows > 0
        || metrics.duplicate_raw_payload_hash_groups > 0;
    let admissible_target_event_rows =
        (metrics.target_event_rows - metrics.taxonomy_blocked_target_event_rows).max(0);

    let (status, admission_decision, next_step) = if !metrics.table_exists {
        (
            "blocked_raw_schema_not_applied",
            "blocked_raw_schema_not_applied_no_coverage_to_audit",
            "apply_sql_phase7_exchange_announcement_order_capacity_source_then_rerun_audit",
        )
    } else if metrics.failed_attempts > 0 {
        (
            "blocked_failed_sync_attempts_present",
            "blocked_until_failed_small_batch_attempts_are_repaired",
            "repair_failed_request_keys_then_rerun_coverage_quality_audit",
        )
    } else if metrics.row_count <= 0 && metrics.completed_attempts <= 0 {
        (
            "raw_table_present_bounded_sync_required",
            "bounded_sync_required_before_coverage_quality_audit",
            "run_one_tiny_exchange_announcement_raw_sync_then_rerun_audit",
        )
    } else if metrics.row_count <= 0 {
        (
            "synced_empty_no_event_rows_passed_for_coverage_accounting_only",
            "synced_empty_no_event_rows_passed_for_coverage_accounting_only",
            "continue_next_tiny_slice_or_batch_then_rerun_full_window_audit",
        )
    } else if raw_quality_failed {
        (
            "blocked_raw_pit_or_quality_failed",
            "blocked_until_pit_duplicate_or_source_quality_is_repaired",
            "repair_or_exclude_bad_raw_rows_before_expanding_sync",
        )
    } else if metrics.trainable_scanned_pdf_blocking_rows > 0 {
        (
            "blocked_scanned_pdf_ocr_required_rows_present",
            "blocked_until_scanned_pdf_ocr_runtime_and_audit_pass",
            "run_separate_ocr_runtime_quality_audit_or_exclude_scanned_pdf_rows",
        )
    } else if metrics.taxonomy_blocked_target_event_rows > 0 {
        (
            "blocked_event_taxonomy_precision_gate_failed",
            "blocked_until_category_aware_taxonomy_precision_manual_review_passes",
            "manually_review_or_exclude_taxonomy_risk_category_target_events_before_expansion",
        )
    } else if metrics.target_event_missing_evidence_span_rows > 0 {
        (
            "blocked_target_event_rows_missing_text_evidence_spans",
            "blocked_until_target_event_evidence_spans_are_repaired",
            "repair_or_exclude_target_event_rows_without_evidence_spans",
        )
    } else if metrics.target_event_rows <= 0 {
        (
            "raw_coverage_passed_no_target_event_rows_for_taxonomy_accounting_only",
            "raw_coverage_passed_no_target_event_rows_for_taxonomy_accounting_only",
            "continue_bounded_sync_and_track_target_event_yield",
        )
    } else {
        (
            "small_batch_coverage_pit_quality_passed_expand_bounded_sync_only",
            "small_batch_coverage_pit_quality_passed_expand_bounded_sync_only",
            "expand_by_month_or_quarter_then_rerun_coverage_quality_audit",
        )
    };

    json!({
        "status": status,
        "admission_decision": admission_decision,
        "next_step": next_step,
        "summary": {
            "table_exists": metrics.table_exists,
            "row_count": metrics.row_count,
            "distinct_symbol_count": metrics.distinct_symbol_count,
            "distinct_category_count": metrics.distinct_category_count,
            "pit_violation_rows": metrics.pit_violation_rows,
            "missing_available_at_rows": metrics.missing_available_at_rows,
            "missing_source_published_at_quality_rows": metrics.missing_source_published_at_quality_rows,
            "duplicate_announcement_id_rows": metrics.duplicate_announcement_id_rows,
            "duplicate_raw_payload_hash_groups": metrics.duplicate_raw_payload_hash_groups,
            "evidence_span_rows": metrics.evidence_span_rows,
            "target_event_rows": metrics.target_event_rows,
            "target_event_missing_evidence_span_rows": metrics.target_event_missing_evidence_span_rows,
            "scanned_pdf_ocr_required_rows": metrics.scanned_pdf_ocr_required_rows,
            "ocr_taxonomy_excluded_rows": metrics.ocr_taxonomy_excluded_rows,
            "trainable_scanned_pdf_blocking_rows": metrics.trainable_scanned_pdf_blocking_rows,
            "taxonomy_blocked_target_event_rows": metrics.taxonomy_blocked_target_event_rows,
            "taxonomy_risk_category_rows": metrics.taxonomy_risk_category_rows,
            "admissible_target_event_rows": admissible_target_event_rows,
            "completed_attempts": metrics.completed_attempts,
            "completed_empty_attempts": metrics.completed_empty_attempts,
            "failed_attempts": metrics.failed_attempts,
            "total_failed_attempts": metrics.total_failed_attempts,
            "excluded_unsupported_category_failed_attempts": metrics.excluded_unsupported_category_failed_attempts,
        },
        "attempt_failure_gate": {
            "status": if metrics.failed_attempts > 0 {
                "blocked"
            } else if metrics.excluded_unsupported_category_failed_attempts > 0 {
                "passed_with_excluded_unsupported_category_parser_failures"
            } else {
                "passed_or_not_observed"
            },
            "blocking_failed_attempts": metrics.failed_attempts,
            "total_failed_attempts": metrics.total_failed_attempts,
            "excluded_unsupported_category_failed_attempts": metrics.excluded_unsupported_category_failed_attempts,
            "policy": "failed attempts in the admitted symbol/category/date scope block admission; pre-registered unsupported category parser failures are retained as evidence but do not block the current admitted scope"
        },
        "ocr_quality_gate": {
            "status": if metrics.trainable_scanned_pdf_blocking_rows > 0 {
                "blocked"
            } else if metrics.ocr_taxonomy_excluded_rows > 0 {
                "passed_with_taxonomy_exclusions_only"
            } else {
                "passed_or_not_observed"
            },
            "scanned_pdf_ocr_required_rows": metrics.scanned_pdf_ocr_required_rows,
            "ocr_taxonomy_excluded_rows": metrics.ocr_taxonomy_excluded_rows,
            "trainable_scanned_pdf_blocking_rows": metrics.trainable_scanned_pdf_blocking_rows,
            "policy": "scanned_pdf rows are retained as raw evidence; only narrow, audited non-target OCR taxonomy exclusions stop blocking coverage, unresolved scanned PDFs remain blocked"
        },
        "taxonomy_precision_gate": {
            "status": if metrics.taxonomy_blocked_target_event_rows > 0 { "blocked" } else { "passed_or_not_observed" },
            "taxonomy_risk_category_rows": metrics.taxonomy_risk_category_rows,
            "taxonomy_blocked_target_event_rows": metrics.taxonomy_blocked_target_event_rows,
            "admissible_target_event_rows": admissible_target_event_rows,
            "policy": "category-risk target events are taxonomy precision audit material only and cannot be counted as trainable positive labels"
        },
        "bounded_sync": if admission_decision == "small_batch_coverage_pit_quality_passed_expand_bounded_sync_only" {
            "expand_bounded_sync_by_month_or_quarter_only"
        } else if admission_decision == "synced_empty_no_event_rows_passed_for_coverage_accounting_only" {
            "continue_bounded_sync_for_coverage_accounting_only"
        } else if admission_decision == "raw_coverage_passed_no_target_event_rows_for_taxonomy_accounting_only" {
            "continue_bounded_sync_and_track_target_event_yield"
        } else {
            "blocked_until_small_batch_audit_passes"
        },
        "factor_builder": "blocked",
        "p310_status": "blocked",
        "bounded_wfa": "blocked",
        "v19_train_selection": "blocked",
    })
}

pub(crate) fn exchange_announcement_order_capacity_target_event_yield_report(
    raw_row_count: i64,
    target_event_rows: i64,
    target_event_with_evidence_span_rows: i64,
    target_event_missing_evidence_span_rows: i64,
    taxonomy_blocked_target_event_rows: i64,
    scanned_pdf_ocr_required_rows: i64,
    ocr_taxonomy_excluded_rows: i64,
    trainable_scanned_pdf_blocking_rows: i64,
) -> Value {
    let admissible_target_event_rows =
        (target_event_rows - taxonomy_blocked_target_event_rows).max(0);
    json!({
        "raw_row_count": raw_row_count,
        "target_event_rows": target_event_rows,
        "admissible_target_event_rows": admissible_target_event_rows,
        "taxonomy_blocked_target_event_rows": taxonomy_blocked_target_event_rows,
        "scanned_pdf_ocr_required_rows": scanned_pdf_ocr_required_rows,
        "ocr_taxonomy_excluded_rows": ocr_taxonomy_excluded_rows,
        "trainable_scanned_pdf_blocking_rows": trainable_scanned_pdf_blocking_rows,
        "non_target_event_rows": (raw_row_count - target_event_rows).max(0),
        "target_event_with_evidence_span_rows": target_event_with_evidence_span_rows,
        "target_event_missing_evidence_span_rows": target_event_missing_evidence_span_rows,
        "target_event_yield_ratio": phase7_ratio(target_event_rows, raw_row_count),
        "admissible_target_event_yield_ratio": phase7_ratio(
            admissible_target_event_rows,
            raw_row_count,
        ),
        "target_event_evidence_span_coverage_ratio": phase7_ratio(
            target_event_with_evidence_span_rows,
            target_event_rows,
        ),
        "admission_scope": "raw_coverage_taxonomy_accounting_only",
        "factor_builder": "blocked",
        "p310_status": "blocked",
        "bounded_wfa": "blocked",
        "v19_train_selection": "blocked",
    })
}

pub(crate) fn exchange_announcement_order_capacity_admission_readiness_report(
    coverage: &Value,
) -> Value {
    let row_count =
        exchange_announcement_order_capacity_json_path_i64(coverage, &["summary", "row_count"]);
    let target_event_rows = exchange_announcement_order_capacity_json_path_i64(
        coverage,
        &["summary", "target_event_rows"],
    );
    let admissible_target_event_rows = exchange_announcement_order_capacity_json_path_i64(
        coverage,
        &["summary", "admissible_target_event_rows"],
    );
    let pit_violation_rows = exchange_announcement_order_capacity_json_path_i64(
        coverage,
        &["summary", "pit_violation_rows"],
    );
    let duplicate_announcement_id_rows = exchange_announcement_order_capacity_json_path_i64(
        coverage,
        &["summary", "duplicate_announcement_id_rows"],
    );
    let duplicate_raw_payload_hash_groups = exchange_announcement_order_capacity_json_path_i64(
        coverage,
        &["summary", "duplicate_raw_payload_hash_groups"],
    );
    let failed_attempts = exchange_announcement_order_capacity_json_path_i64(
        coverage,
        &["summary", "failed_attempts"],
    );
    let trainable_scanned_pdf_blocking_rows = exchange_announcement_order_capacity_json_path_i64(
        coverage,
        &["summary", "trainable_scanned_pdf_blocking_rows"],
    );
    let missing_evidence_rows = exchange_announcement_order_capacity_json_path_i64(
        coverage,
        &["summary", "target_event_missing_evidence_span_rows"],
    );
    let evidence_span_coverage = exchange_announcement_order_capacity_json_path_f64(
        coverage,
        &[
            "target_event_yield",
            "target_event_evidence_span_coverage_ratio",
        ],
    )
    .unwrap_or(0.0);
    let year_category_count =
        exchange_announcement_order_capacity_array_len(coverage, "year_category_breakdown");
    let symbol_breakdown_count =
        exchange_announcement_order_capacity_array_len(coverage, "symbol_event_breakdown");

    let raw_gate_passed = row_count > 0
        && target_event_rows > 0
        && admissible_target_event_rows > 0
        && pit_violation_rows == 0
        && duplicate_announcement_id_rows == 0
        && duplicate_raw_payload_hash_groups == 0
        && failed_attempts == 0
        && trainable_scanned_pdf_blocking_rows == 0
        && missing_evidence_rows == 0
        && evidence_span_coverage >= 1.0;

    let effective_coverage_status =
        if raw_gate_passed && year_category_count >= 1 && symbol_breakdown_count >= 4 {
            "pilot_scope_only_not_full_history"
        } else {
            "blocked_until_broader_pre_registered_coverage_passes"
        };

    json!({
        "audit_version": "p3.24w-exchange-announcement-order-capacity-admission-readiness-audit-v1",
        "source_id": "exchange_announcement_order_capacity_text",
        "stage": "P3.24W",
        "mode": "read_only_admission_readiness_no_factor_no_p310_no_wfa",
        "coverage_pit_quality_gate": {
            "status": if raw_gate_passed {
                "passed_pilot_scope"
            } else {
                "blocked_until_coverage_pit_quality_passes"
            },
            "row_count": row_count,
            "target_event_rows": target_event_rows,
            "admissible_target_event_rows": admissible_target_event_rows,
            "pit_violation_rows": pit_violation_rows,
            "duplicate_announcement_id_rows": duplicate_announcement_id_rows,
            "duplicate_raw_payload_hash_groups": duplicate_raw_payload_hash_groups,
            "failed_attempts": failed_attempts,
            "trainable_scanned_pdf_blocking_rows": trainable_scanned_pdf_blocking_rows,
            "target_event_missing_evidence_span_rows": missing_evidence_rows,
            "target_event_evidence_span_coverage_ratio": evidence_span_coverage,
        },
        "effective_coverage_gate": {
            "status": effective_coverage_status,
            "year_category_breakdown_count": year_category_count,
            "symbol_breakdown_count": symbol_breakdown_count,
            "current_scope": "bounded pilot scope; not yet full-history or formally pre-registered broad coverage",
            "required_before_p310": "pre-register target universe/date range/category scope and prove coverage/readiness across that scope"
        },
        "manual_evidence_span_precision_gate": {
            "status": "blocked_manual_review_required",
            "required_precision_min": 0.80,
            "required_sample_size_min": 50,
            "current_machine_evidence_coverage": evidence_span_coverage,
            "policy": "machine spans prove text anchoring, not human semantic precision"
        },
        "event_taxonomy_precision_gate": {
            "status": "blocked_manual_review_required",
            "required_precision_min": 0.80,
            "required_sample_size_min": 50,
            "policy": "manual review must confirm order/capacity/price/commissioning labels and negative exclusions before trainable rows"
        },
        "correlation_gate": {
            "status": "blocked_correlation_audit_required",
            "max_abs_correlation_threshold": 0.30,
            "reference_families": [
                "moneyflow_congestion",
                "liquidity",
                "price_volume",
                "financial_quality_change",
                "earnings_recovery_persistence",
                "event_overlay",
                "shareholder_structure"
            ],
            "pit_alignment": "must join raw event features by conservative available_at, not announcement_time"
        },
        "promotion_gate": {
            "factor_builder": "blocked_until_admission_readiness_passes",
            "p310_status": "blocked_until_manual_precision_effective_coverage_and_correlation_pass",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        },
        "next_step": if raw_gate_passed {
            "run_manual_evidence_span_and_taxonomy_precision_review_then_low_correlation_audit_before_p310"
        } else {
            "repair_or_extend_raw_coverage_pit_quality_before_admission_readiness"
        },
        "coverage_audit": coverage,
    })
}

pub(crate) fn exchange_announcement_order_capacity_manual_precision_sample_report(
    admissible_target_event_rows: i64,
    required_target_sample_size: i64,
    target_sample_rows: i64,
    negative_sample_rows: i64,
    review_items: Vec<Value>,
) -> Value {
    let target_sample_shortfall = (required_target_sample_size - target_sample_rows).max(0);
    let status = if target_sample_shortfall > 0 {
        "blocked_insufficient_target_review_sample"
    } else {
        "manual_review_sample_ready"
    };

    json!({
        "audit_version": "p3.24x-exchange-announcement-order-capacity-manual-precision-sample-audit-v1",
        "source_id": "exchange_announcement_order_capacity_text",
        "stage": "P3.24X",
        "mode": "read_only_manual_review_sample_no_labels_no_factor_no_p310",
        "status": status,
        "admissible_target_event_rows": admissible_target_event_rows,
        "required_target_sample_size_min": required_target_sample_size,
        "target_sample_rows": target_sample_rows,
        "negative_sample_rows": negative_sample_rows,
        "target_sample_shortfall": target_sample_shortfall,
        "manual_evidence_span_precision_gate": {
            "status": "blocked_until_human_labels_are_recorded",
            "required_precision_min": 0.80,
            "required_sample_size_min": required_target_sample_size,
            "review_labels_required": [
                "evidence_span_correct",
                "evidence_span_wrong_or_too_broad",
                "insufficient_context"
            ],
            "policy": "this endpoint creates a deterministic review sample only; it cannot certify precision without persisted human labels"
        },
        "event_taxonomy_precision_gate": {
            "status": "blocked_until_human_labels_are_recorded",
            "required_precision_min": 0.80,
            "required_sample_size_min": required_target_sample_size,
            "review_labels_required": [
                "taxonomy_correct_target_event",
                "taxonomy_false_positive",
                "taxonomy_uncertain"
            ],
            "policy": "target-event labels and negative exclusions require human review before trainable rows"
        },
        "promotion_gate": {
            "factor_builder": "blocked_until_manual_precision_review_passes",
            "p310_status": "blocked_until_manual_precision_review_passes",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        },
        "next_step": if status == "manual_review_sample_ready" {
            "record_human_labels_for_sample_then_compute_precision_before_correlation_audit"
        } else {
            "expand_pre_registered_coverage_until_minimum_target_review_sample_is_available"
        },
        "review_items": review_items,
    })
}

pub(crate) fn last_day_of_month(year: i32, month: u32) -> NaiveDate {
    let (next_year, next_month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    NaiveDate::from_ymd_opt(next_year, next_month, 1).unwrap() - Duration::days(1)
}

pub(crate) fn akshare_analyst_revision_sync_plan_batches(
    start: NaiveDate,
    end: NaiveDate,
    batch_mode: &str,
) -> Result<Vec<AkshareAnalystRevisionSyncPlanBatch>, String> {
    if start > end {
        return Err("start_date must be <= end_date".to_string());
    }
    if !matches!(batch_mode, "year" | "quarter" | "month") {
        return Err(
            "AkShare analyst revision sync-plan batch must be year, quarter, or month".to_string(),
        );
    }

    let mut batches = Vec::new();
    let mut cursor = start;
    while cursor <= end {
        let batch_end = akshare_analyst_revision_batch_end(cursor, batch_mode).min(end);
        batches.push(AkshareAnalystRevisionSyncPlanBatch {
            label: akshare_analyst_revision_batch_label(cursor, batch_mode),
            start_date: cursor,
            end_date: batch_end,
            calendar_day_count: (batch_end - cursor).num_days() + 1,
        });
        cursor = batch_end + Duration::days(1);
    }

    if batches.len() > AKSHARE_ANALYST_REVISION_SYNC_PLAN_MAX_BATCHES {
        return Err(format!(
            "AkShare analyst revision sync-plan resolved {} batches, above max {}",
            batches.len(),
            AKSHARE_ANALYST_REVISION_SYNC_PLAN_MAX_BATCHES
        ));
    }
    Ok(batches)
}

pub(crate) fn akshare_stable_hash(parts: &[String]) -> String {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    let mut hash = FNV_OFFSET;
    for part in parts {
        for byte in part.as_bytes() {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    format!("{hash:016x}")
}

pub(crate) fn akshare_analyst_revision_raw_row_from_record(
    record: &serde_json::Map<String, Value>,
    request_key: &str,
    open_dates: &[NaiveDate],
) -> Result<AkshareAnalystRevisionRawRow, String> {
    let symbol = akshare_value_key_part(record, "证券代码");
    if symbol.is_empty() {
        return Err("missing_symbol".to_string());
    }
    let publication_date =
        parse_akshare_publication_date(&akshare_value_key_part(record, "发布日期"))
            .ok_or_else(|| "missing_or_invalid_publication_date".to_string())?;
    if publication_date.format("%Y%m%d").to_string() != request_key {
        return Err(format!(
            "publication_date_mismatch:{}",
            publication_date.format("%Y%m%d")
        ));
    }
    let available_at = akshare_next_open_date(publication_date, open_dates);
    let raw_payload = Value::Object(record.clone());
    let raw_payload_string = serde_json::to_string(&raw_payload).unwrap_or_default();
    let raw_payload_hash = akshare_stable_hash(&[
        "akshare".to_string(),
        "stock_rank_forecast_cninfo".to_string(),
        request_key.to_string(),
        symbol.clone(),
        raw_payload_string,
    ]);

    Ok(AkshareAnalystRevisionRawRow {
        vendor: "akshare".to_string(),
        vendor_source: "akshare".to_string(),
        vendor_endpoint: "stock_rank_forecast_cninfo".to_string(),
        request_key: request_key.to_string(),
        symbol,
        symbol_name: akshare_optional_string(record, "证券简称"),
        publication_date,
        source_published_at: akshare_source_published_at(available_at),
        available_at,
        institution_name: akshare_optional_string(record, "研究机构简称"),
        analyst_name: akshare_optional_string(record, "研究员名称"),
        rating_current: akshare_optional_string(record, "投资评级"),
        rating_previous: akshare_optional_string(record, "前一次投资评级"),
        rating_change: akshare_optional_string(record, "评级变化"),
        is_first_rating: akshare_optional_string(record, "是否首次评级"),
        target_price_min: akshare_optional_decimal(record, "目标价格-下限"),
        target_price_max: akshare_optional_decimal(record, "目标价格-上限"),
        raw_payload,
        raw_payload_hash,
    })
}

pub(crate) fn validate_akshare_analyst_revision_sync_range(
    start: NaiveDate,
    end: NaiveDate,
) -> Result<i64, String> {
    if start > end {
        return Err("start_date must be <= end_date".to_string());
    }
    let calendar_day_count = (end - start).num_days() + 1;
    if calendar_day_count > AKSHARE_ANALYST_REVISION_SYNC_MAX_CALENDAR_DAYS {
        return Err(format!(
            "AkShare analyst revision bounded sync resolved {} calendar days, above max {}. Use monthly or <=100-day batches.",
            calendar_day_count, AKSHARE_ANALYST_REVISION_SYNC_MAX_CALENDAR_DAYS
        ));
    }
    Ok(calendar_day_count)
}

pub(crate) fn akshare_analyst_revision_should_retry_fetch_status(status: &str) -> bool {
    matches!(status, "timeout" | "error")
}

pub(crate) fn safe_ratio(numerator: i64, denominator: i64) -> Option<f64> {
    (denominator > 0).then_some(numerator as f64 / denominator as f64)
}

pub(crate) fn decide_broad_analyst_revision_audit(
    available_at_rule_violations: i64,
    union_symbol_coverage_ratio: f64,
    forecast_symbol_coverage_ratio: f64,
    revised_symbol_coverage_ratio: f64,
    revised_symbol_period_ratio: f64,
) -> BroadAnalystRevisionAuditDecision {
    if available_at_rule_violations > 0 {
        BroadAnalystRevisionAuditDecision {
            passed: false,
            status: "blocked_available_at_rule_violation",
            readiness: "blocked_pit_available_at_repair_required",
            admission_decision: "blocked_broad_analyst_revision_available_at_rule_failed",
            p310_status: "not_started",
            blocked_reason:
                "one_or_more_event_source_rows_do_not_follow_the_registered_available_at_policy",
        }
    } else if union_symbol_coverage_ratio < BROAD_ANALYST_REVISION_MIN_UNION_SYMBOL_COVERAGE {
        BroadAnalystRevisionAuditDecision {
            passed: false,
            status: "blocked_undercovered_for_broad_base_revision",
            readiness: "blocked_full_history_coverage_not_broad_enough",
            admission_decision: "blocked_broad_analyst_revision_after_full_history_coverage_audit",
            p310_status: "not_started",
            blocked_reason: "forecast_express_disclosure_sources_do_not_cover_enough_symbols_for_broad_base_revision",
        }
    } else if forecast_symbol_coverage_ratio < BROAD_ANALYST_REVISION_MIN_FORECAST_SYMBOL_COVERAGE
        || revised_symbol_coverage_ratio < BROAD_ANALYST_REVISION_MIN_REVISED_SYMBOL_COVERAGE
        || revised_symbol_period_ratio < BROAD_ANALYST_REVISION_MIN_REVISED_PERIOD_RATIO
    {
        BroadAnalystRevisionAuditDecision {
            passed: false,
            status: "blocked_sparse_revision_semantics",
            readiness: "stopped_current_raw_bundle_revision_semantics_too_sparse",
            admission_decision: "stopped_broad_analyst_revision_current_raw_bundle_after_audit_sparse_revision_semantics",
            p310_status: "not_started",
            blocked_reason: "true_forecast_revision_events_are_too_sparse_and_would_degenerate_into_event_overlay",
        }
    } else {
        BroadAnalystRevisionAuditDecision {
            passed: true,
            status: "full_history_revision_semantics_audit_passed",
            readiness: "ready_for_p310_diagnostics_only",
            admission_decision:
                "coverage_available_at_revision_semantics_passed_p310_required_next",
            p310_status: "not_started",
            blocked_reason: "",
        }
    }
}

pub(crate) fn decide_akshare_analyst_revision_history_replay_audit(
    requested_date_count: usize,
    available_date_count: usize,
    error_date_count: usize,
    empty_date_count: usize,
    row_count: i64,
    publication_date_mismatch_rows: i64,
    missing_publication_date_rows: i64,
    missing_revision_semantics_rows: i64,
) -> Value {
    let (passed, status, admission_decision, blocked_reason) = if requested_date_count == 0 {
        (
            false,
            "blocked_no_history_dates_requested",
            "blocked_no_history_dates_requested",
            "history replay needs explicit dates or a market-calendar year range",
        )
    } else if error_date_count > 0 {
        (
            false,
            "blocked_history_replay_probe_failed",
            "blocked_history_replay_probe_failed",
            "one or more AkShare history-date probes failed or timed out",
        )
    } else if empty_date_count > 0 || available_date_count < requested_date_count {
        (
            false,
            "blocked_history_replay_empty_dates",
            "blocked_history_replay_empty_dates",
            "one or more representative history dates returned no analyst revision rows",
        )
    } else if row_count <= 0 {
        (
            false,
            "blocked_history_replay_no_rows",
            "blocked_history_replay_no_rows",
            "history replay returned no rows",
        )
    } else if publication_date_mismatch_rows > 0 || missing_publication_date_rows > 0 {
        (
            false,
            "blocked_publication_date_mismatch_or_missing",
            "blocked_publication_date_mismatch_or_missing",
            "source publication date must equal the requested history date and be non-null",
        )
    } else if missing_revision_semantics_rows > 0 {
        (
            false,
            "blocked_revision_semantics_missing_fields",
            "blocked_revision_semantics_missing_fields",
            "rating_change and previous_rating fields must be populated before schema review",
        )
    } else {
        (
            true,
            "history_replay_available_at_sample_passed",
            "history_replay_available_at_sample_passed_schema_review_next",
            "",
        )
    };

    json!({
        "passed": passed,
        "status": status,
        "admission_decision": admission_decision,
        "blocked_reason": if blocked_reason.is_empty() { Value::Null } else { json!(blocked_reason) },
        "promotion_gate": {
            "schema_apply": if passed {
                "schema_review_allowed_next"
            } else {
                "blocked_until_history_replay_audit_passes"
            },
            "bounded_sync": "blocked",
            "factor_builder": "blocked",
            "p310_status": "blocked",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        }
    })
}

pub(crate) fn decide_akshare_analyst_revision_readiness(
    schema_exists: bool,
    row_count: i64,
    pit_violation_rows: i64,
    missing_source_published_at_rows: i64,
    missing_current_rating_rows: i64,
    missing_revision_semantics_rows: i64,
    duplicate_key_rows: i64,
) -> Value {
    let (passed, status, admission_decision, blocked_reason) = if !schema_exists {
        (
            false,
            "schema_not_applied",
            "schema_review_apply_required_before_bounded_sync",
            "market_vendor_analyst_revision_raw does not exist",
        )
    } else if row_count <= 0 {
        (
            false,
            "schema_created_sync_not_started",
            "bounded_sync_required_before_coverage_audit",
            "raw schema exists but contains no analyst revision rows",
        )
    } else if pit_violation_rows > 0 || missing_source_published_at_rows > 0 {
        (
            false,
            "raw_pit_failed",
            "raw_pit_or_source_published_at_failed",
            "raw rows must have publication_date/source_published_at and available_at >= publication_date",
        )
    } else if missing_revision_semantics_rows > 0 {
        (
            false,
            "raw_revision_semantics_failed",
            "raw_revision_semantics_failed",
            "rating_previous/rating_change must be present before coverage admission; missing current ratings are audited separately and must be excluded or downweighted before current-rating factor use",
        )
    } else if duplicate_key_rows > 0 {
        (
            false,
            "raw_duplicate_key_failed",
            "raw_duplicate_key_failed",
            "natural key plus raw_payload_hash must not produce duplicate rows",
        )
    } else {
        (
            true,
            "raw_readiness_passed_coverage_audit_required_next",
            "raw_schema_and_pit_ready_for_coverage_audit_only",
            "",
        )
    };

    json!({
        "passed": passed,
        "status": status,
        "admission_decision": admission_decision,
        "blocked_reason": if blocked_reason.is_empty() { Value::Null } else { json!(blocked_reason) },
        "row_quality": {
            "missing_current_rating_rows": missing_current_rating_rows,
            "current_rating_usage": if missing_current_rating_rows > 0 {
                "exclude_or_downweight_rows_before_current_rating_factor_use"
            } else {
                "fully_populated"
            },
            "revision_semantics_required_fields": ["rating_previous", "rating_change"]
        },
        "promotion_gate": {
            "bounded_sync": if schema_exists {
                "schema_exists_bounded_sync_can_be_considered"
            } else {
                "blocked_until_schema_review_and_apply"
            },
            "coverage_audit": if passed {
                "coverage_audit_required_next"
            } else {
                "blocked_until_readiness_passes"
            },
            "factor_builder": "blocked",
            "p310_status": "blocked",
            "bounded_wfa": "blocked",
            "v19_train_selection": "blocked"
        }
    })
}

pub(crate) fn decide_akshare_analyst_revision_coverage_audit(
    table_exists: bool,
    row_count: i64,
    coverage_ratio: f64,
    failed_attempt_dates: i64,
    missing_year_count: i64,
    pit_violation_rows: i64,
    missing_source_published_at_rows: i64,
    missing_revision_semantics_rows: i64,
    duplicate_key_rows: i64,
    duplicate_payload_hash_rows: i64,
    correlation_decision: &str,
) -> Value {
    let raw_quality_failed = pit_violation_rows > 0
        || missing_source_published_at_rows > 0
        || missing_revision_semantics_rows > 0
        || duplicate_key_rows > 0
        || duplicate_payload_hash_rows > 0;
    let (status, admission_decision, p310_status, next_step) = if !table_exists {
        (
            "blocked_no_raw_schema_or_full_history_sync",
            "blocked_raw_schema_not_applied_no_coverage_to_audit",
            "blocked",
            "apply_sql_phase7_akshare_analyst_revision_source_then_rerun_coverage_audit",
        )
    } else if row_count <= 0 {
        (
            "raw_table_present_bounded_sync_required",
            "bounded_sync_required_before_coverage_audit",
            "blocked",
            "run_bounded_calendar_day_raw_sync_before_coverage_audit",
        )
    } else if failed_attempt_dates > 0 {
        (
            "blocked_failed_sync_attempts_present",
            "blocked_until_failed_dates_are_repaired_and_rerun",
            "blocked",
            "repair_failed_dates_with_same_sync_endpoint_then_rerun_coverage_audit",
        )
    } else if coverage_ratio + f64::EPSILON < 1.0 || missing_year_count > 0 {
        (
            "blocked_incomplete_calendar_coverage",
            "blocked_until_full_history_calendar_coverage_passes",
            "blocked",
            "continue_month_or_quarter_bounded_raw_sync_then_rerun_coverage_audit",
        )
    } else if raw_quality_failed {
        (
            "blocked_raw_pit_source_revision_or_duplicate_quality_failed",
            "blocked_until_pit_source_published_at_revision_semantics_and_duplicate_hash_audit_passes",
            "blocked",
            "repair_or_exclude_bad_raw_rows_before_p310_diagnostics",
        )
    } else if correlation_decision != "passed_low_linear_correlation_screen" {
        (
            "blocked_correlation_screen_not_passed_or_needs_review",
            "blocked_until_moneyflow_liquidity_price_volume_correlation_audit_passes",
            "blocked",
            "complete_low_correlation_review_before_p310_diagnostics",
        )
    } else {
        (
            "coverage_pit_quality_correlation_ready_for_p310_diagnostics",
            "coverage_pit_quality_correlation_passed_p310_diagnostics_required_next",
            "ready_for_p310_diagnostics_only",
            "run_p310a_d_rankic_group_decay_turnover_capacity_regime_exposure_diagnostics",
        )
    };

    json!({
        "status": status,
        "admission_decision": admission_decision,
        "p310_status": p310_status,
        "next_step": next_step,
        "bounded_wfa": "blocked",
        "v19_train_selection": "blocked",
        "factor_builder": "blocked_until_p310_diagnostics_passes",
    })
}

pub(crate) fn main_business_raw_source_readiness(
    row_count: i64,
    expected_periods: usize,
    completed_periods: usize,
    failed_periods: usize,
    pit_violation_rows: i64,
) -> &'static str {
    if row_count <= 0 {
        return "raw_source_missing";
    }
    if pit_violation_rows > 0 {
        return "raw_source_pit_failed";
    }
    if completed_periods < expected_periods {
        return "period_sync_incomplete";
    }
    if failed_periods > 0 {
        return "period_sync_failed";
    }
    "raw_source_ready_for_full_history_coverage_audit"
}

pub(crate) fn main_business_readiness_summary_sql() -> &'static str {
    r#"
    SELECT
        COUNT(*)::bigint AS row_count,
        COUNT(DISTINCT symbol)::bigint AS symbol_count,
        COUNT(DISTINCT end_date)::bigint AS distinct_periods,
        MIN(end_date) AS min_end_date,
        MAX(end_date) AS max_end_date,
        MIN(available_at) AS min_available_at,
        MAX(available_at) AS max_available_at,
        COUNT(*) FILTER (WHERE available_at < end_date)::bigint AS pit_violation_rows
    FROM market_stock_main_business
    WHERE end_date BETWEEN $1 AND $2
      AND business_type = $3
    "#
}

pub(crate) fn main_business_missing_available_at_rows(error_message: Option<&str>) -> i64 {
    main_business_attempt_metric(error_message, "missing_available_at_rows=")
}

pub(crate) fn main_business_out_of_universe_rows(error_message: Option<&str>) -> i64 {
    main_business_attempt_metric(error_message, "out_of_universe_rows=")
}

pub(crate) fn decide_main_business_available_at_join_audit(
    total_periods: usize,
    mappings: &[MainBusinessPeriodMapping],
) -> MainBusinessAvailableAtJoinDecision {
    let explicit_missing = mappings
        .iter()
        .filter(|mapping| mapping.available_at.is_none())
        .count();
    let implicit_missing = total_periods.saturating_sub(mappings.len());
    let missing_mapping_count = explicit_missing + implicit_missing;
    let pit_violation_count = mappings
        .iter()
        .filter(|mapping| {
            mapping
                .available_at
                .map(|available_at| available_at < mapping.end_date)
                .unwrap_or(false)
        })
        .count();
    let mut source_counts = BTreeMap::new();
    for mapping in mappings
        .iter()
        .filter(|mapping| mapping.available_at.is_some())
    {
        let source = mapping
            .source
            .clone()
            .unwrap_or_else(|| "unknown".to_string());
        *source_counts.entry(source).or_insert(0) += 1;
    }

    let (passed, status, readiness) = if total_periods == 0 {
        (
            false,
            "blocked_no_sample_periods",
            "blocked_available_at_join_audit_required",
        )
    } else if pit_violation_count > 0 {
        (
            false,
            "blocked_pit_available_at_violations",
            "blocked_available_at_join_audit_required",
        )
    } else if missing_mapping_count > 0 {
        (
            false,
            "blocked_available_at_join_gaps",
            "blocked_available_at_join_audit_required",
        )
    } else {
        (true, "passed", "available_at_join_ready_for_schema_design")
    };

    MainBusinessAvailableAtJoinDecision {
        passed,
        status,
        readiness,
        missing_mapping_count,
        pit_violation_count,
        source_counts,
    }
}
