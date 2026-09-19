//! phase7 覆盖率批次构建与 autopilot 执行：optional_source/share_float/
//! industry_membership/financial 的 coverage 批次、autopilot 轮次与符号解析。
use super::*;

pub(crate) async fn build_phase7_optional_source_coverage_sync(
    state: Arc<AppState>,
    req: Phase7OptionalSourceCoverageSyncReq,
) -> Result<Value, String> {
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
        return Err("start_date must be <= end_date".to_string());
    }

    let sources = phase7_optional_source_sync_sources(&req.sources)?;
    let max_symbols = phase7_optional_source_sync_limit(req.max_symbols);
    let offset_symbols = req.offset_symbols.unwrap_or_default();
    let plan_only = phase7_optional_source_sync_plan_only(req.plan_only);
    let data_version_prefix = req.data_version_prefix.clone().unwrap_or_else(|| {
        chrono::Utc::now()
            .format("dv-phase7-optional-%Y%m%d-%H%M%S%3f")
            .to_string()
    });

    let mut source_results = Vec::new();
    for source in sources {
        let table = phase7_optional_source_table(&source)
            .ok_or_else(|| format!("unsupported optional source: {}", source))?;
        let selected_symbols = resolve_phase7_optional_source_sync_symbols(
            &state,
            &source,
            &req.symbols,
            start,
            end,
            max_symbols,
            offset_symbols,
        )
        .await?;
        if selected_symbols.is_empty() {
            source_results.push(json!({
                "source": source,
                "table": table,
                "status": "skipped_no_symbols",
                "selected_count": 0,
                "selected_symbols": selected_symbols,
            }));
            continue;
        }

        let task_id = bounded_phase7_task_id(&[data_version_prefix.as_str(), source.as_str()]);
        let sync_req = DataSyncTaskReq {
            dataset: source.clone(),
            source: "tushare".to_string(),
            mode: Some("bounded_symbols".to_string()),
            symbols: selected_symbols.clone(),
            source_filters: Vec::new(),
            index_codes: Vec::new(),
            exchanges: Vec::new(),
            start_date: Some(start_date.clone()),
            end_date: Some(end_date.clone()),
            data_version_id: Some(task_id.clone()),
            background: req.background,
            quality_check: false,
            create_data_version: true,
            retry_of_task_id: None,
            reason: Some("phase7 optional source bounded coverage expansion".to_string()),
        };

        if plan_only {
            source_results.push(json!({
                "source": source,
                "table": table,
                "task_id": task_id,
                "status": "planned",
                "selected_count": selected_symbols.len(),
                "selected_symbols": selected_symbols,
            }));
        } else if req.background {
            register_sync_task(&state, &task_id, &sync_req, "running").await?;
            let state_for_task = state.clone();
            let task_id_for_task = task_id.clone();
            let req_for_task = sync_req.clone();
            crate::sync_task_registry::spawn_sync_task(
                state.sync_tasks.clone(),
                task_id.clone(),
                async move {
                    if let Err(message) = execute_sync_task(
                        state_for_task.clone(),
                        task_id_for_task.clone(),
                        req_for_task,
                    )
                    .await
                    {
                        let _ = quant_data::repository::fail_sync_task(
                            &state_for_task.db,
                            &task_id_for_task,
                            &message,
                        )
                        .await;
                        tracing::error!(task_id = %task_id_for_task, error = %message, "Phase 7 可选源 bounded 补数失败");
                    }
                },
            )
            .await;
            source_results.push(json!({
                "source": source,
                "table": table,
                "task_id": task_id,
                "status": "running",
                "selected_count": selected_symbols.len(),
                "selected_symbols": selected_symbols,
            }));
        } else {
            let execution = execute_sync_task(state.clone(), task_id.clone(), sync_req).await?;
            source_results.push(json!({
                "source": source,
                "table": table,
                "task_id": task_id,
                "status": "completed",
                "selected_count": selected_symbols.len(),
                "selected_symbols": selected_symbols,
                "execution": execution,
            }));
        }
    }

    Ok(json!({
        "audit_version": "phase7-ff-bounded-sync-v1",
        "mode": if plan_only {
            "plan_only"
        } else if req.background {
            "background"
        } else {
            "synchronous"
        },
        "plan_only": plan_only,
        "max_symbols": max_symbols,
        "offset_symbols": offset_symbols,
        "date_range": {
            "start_date": start_date,
            "end_date": end_date,
        },
        "data_version_prefix": data_version_prefix,
        "sources": source_results,
        "notes": [
            "This endpoint never performs full-market sync through empty symbols; it always resolves a bounded symbol list first.",
            "plan_only defaults to true. Set plan_only=false only for bounded smoke or controlled background expansion.",
            "Run phase7-feasibility-audit after completion and do not build optional-source factors while readiness remains sample_only_do_not_train."
        ],
    }))
}

pub(crate) async fn build_phase7_optional_source_coverage_batches(
    state: Arc<AppState>,
    req: Phase7OptionalSourceCoverageBatchReq,
) -> Result<Value, String> {
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
        return Err("start_date must be <= end_date".to_string());
    }

    let sources = phase7_optional_source_sync_sources(&req.sources)?;
    let batch_size = phase7_optional_source_batch_size(req.batch_size);
    let batch_count = phase7_optional_source_batch_count(req.batch_count);
    let start_offset = req.start_offset.unwrap_or_default();
    let plan_only = phase7_optional_source_sync_plan_only(req.plan_only);
    let child_background = phase7_optional_source_batch_child_background(plan_only);
    let offsets = phase7_optional_source_batch_offsets(start_offset, batch_size, batch_count);
    let planned_next_offset = phase7_optional_source_batch_next_offset(&offsets, batch_size);
    let recommended_resume_offset =
        phase7_optional_source_batch_recommended_resume_offset(plan_only, &offsets, batch_size);
    let data_version_prefix = req.data_version_prefix.clone().unwrap_or_else(|| {
        chrono::Utc::now()
            .format("dv-phase7-optional-batch-%Y%m%d-%H%M%S%3f")
            .to_string()
    });

    let mut batches = Vec::new();
    for (batch_index, offset_symbols) in offsets.iter().copied().enumerate() {
        let batch_label = format!("b{:03}", batch_index + 1);
        let batch_prefix =
            bounded_phase7_task_id(&[data_version_prefix.as_str(), batch_label.as_str()]);
        let child_req = Phase7OptionalSourceCoverageSyncReq {
            sources: sources.clone(),
            symbols: Vec::new(),
            start_date: Some(start_date.clone()),
            end_date: Some(end_date.clone()),
            max_symbols: Some(batch_size),
            offset_symbols: Some(offset_symbols),
            plan_only: Some(plan_only),
            background: child_background,
            data_version_prefix: Some(batch_prefix.clone()),
        };
        let batch_result =
            build_phase7_optional_source_coverage_sync(state.clone(), child_req).await?;
        batches.push(json!({
            "batch_index": batch_index + 1,
            "offset_symbols": offset_symbols,
            "batch_size": batch_size,
            "data_version_prefix": batch_prefix,
            "status": if plan_only { "planned" } else { "launched" },
            "result": batch_result,
        }));
    }

    Ok(json!({
        "audit_version": "phase7-ff-bounded-batch-sync-v1",
        "mode": if plan_only { "plan_only" } else { "background" },
        "plan_only": plan_only,
        "child_background": child_background,
        "batch_size": batch_size,
        "batch_count": batch_count,
        "start_offset": start_offset,
        "next_offset": recommended_resume_offset,
        "planned_next_offset": planned_next_offset,
        "date_range": {
            "start_date": start_date,
            "end_date": end_date,
        },
        "sources": sources,
        "data_version_prefix": data_version_prefix,
        "batches": batches,
        "notes": [
            "This endpoint orchestrates bounded optional-source sync batches only; it never trains or backfills factors.",
            "plan_only defaults to true. Set plan_only=false to launch child bounded sync tasks in background mode.",
            "For plan-only pagination, next_offset advances through the planned uncovered set.",
            "After a real execution, next_offset resets to 0 because the uncovered set changes as rows are written.",
            "Rerun phase7-feasibility-audit after tasks complete before feature training."
        ],
    }))
}

pub(crate) async fn build_phase7_share_float_coverage_batches(
    state: Arc<AppState>,
    req: Phase7ShareFloatCoverageReq,
) -> Result<Value, String> {
    let today = chrono::Utc::now().date_naive();
    let start_date = req
        .start_date
        .clone()
        .unwrap_or_else(|| "20140101".to_string());
    let end_date = req
        .end_date
        .clone()
        .unwrap_or_else(|| today.format("%Y%m%d").to_string());
    let start = parse_optional_date(Some(start_date.as_str()))?
        .ok_or_else(|| "start_date is required".to_string())?;
    let end = parse_optional_date(Some(end_date.as_str()))?
        .ok_or_else(|| "end_date is required".to_string())?;
    if start > end {
        return Err("start_date must be <= end_date".to_string());
    }

    let granularity = phase7_share_float_chunk_granularity(req.chunk_granularity.as_deref());
    let max_chunks = phase7_share_float_coverage_max_chunks(req.max_chunks);
    let plan_only = phase7_share_float_coverage_plan_only(req.plan_only);
    let chunks = phase7_share_float_date_chunks(start, end, granularity, max_chunks);
    let truncated = chunks
        .last()
        .map(|(_, chunk_end)| *chunk_end < end)
        .unwrap_or(false);
    let data_version_prefix = req.data_version_prefix.clone().unwrap_or_else(|| {
        chrono::Utc::now()
            .format("dv-p7-share-float-%Y%m%d-%H%M%S%3f")
            .to_string()
    });

    let mut batch_results = Vec::new();
    for (index, (chunk_start, chunk_end)) in chunks.iter().copied().enumerate() {
        let batch_label = format!("f{:03}", index + 1);
        let task_id = bounded_phase7_task_id(&[data_version_prefix.as_str(), batch_label.as_str()]);
        let chunk_start_s = chunk_start.format("%Y%m%d").to_string();
        let chunk_end_s = chunk_end.format("%Y%m%d").to_string();
        let sync_req = DataSyncTaskReq {
            dataset: "share_float".to_string(),
            source: "tushare".to_string(),
            mode: Some("full_market".to_string()),
            symbols: Vec::new(),
            source_filters: Vec::new(),
            index_codes: Vec::new(),
            exchanges: Vec::new(),
            start_date: Some(chunk_start_s.clone()),
            end_date: Some(chunk_end_s.clone()),
            data_version_id: Some(task_id.clone()),
            background: req.background,
            quality_check: false,
            create_data_version: true,
            retry_of_task_id: None,
            reason: Some("phase7 share_float float_date coverage expansion".to_string()),
        };

        if plan_only {
            batch_results.push(json!({
                "batch_index": index + 1,
                "task_id": task_id,
                "status": "planned",
                "dataset": "share_float",
                "mode": "full_market",
                "query_basis": "float_date",
                "date_range": {
                    "start_date": chunk_start_s,
                    "end_date": chunk_end_s,
                }
            }));
        } else if req.background {
            register_sync_task(&state, &task_id, &sync_req, "running").await?;
            let state_for_task = state.clone();
            let task_id_for_task = task_id.clone();
            let req_for_task = sync_req.clone();
            crate::sync_task_registry::spawn_sync_task(
                state.sync_tasks.clone(),
                task_id.clone(),
                async move {
                    if let Err(message) = execute_sync_task(
                        state_for_task.clone(),
                        task_id_for_task.clone(),
                        req_for_task,
                    )
                    .await
                    {
                        let _ = quant_data::repository::fail_sync_task(
                            &state_for_task.db,
                            &task_id_for_task,
                            &message,
                        )
                        .await;
                        tracing::error!(task_id = %task_id_for_task, error = %message, "Phase 7 share_float float_date补数失败");
                    }
                },
            )
            .await;
            batch_results.push(json!({
                "batch_index": index + 1,
                "task_id": task_id,
                "status": "running",
                "dataset": "share_float",
                "mode": "full_market",
                "query_basis": "float_date",
                "date_range": {
                    "start_date": chunk_start_s,
                    "end_date": chunk_end_s,
                }
            }));
        } else {
            let execution = execute_sync_task(state.clone(), task_id.clone(), sync_req).await?;
            batch_results.push(json!({
                "batch_index": index + 1,
                "task_id": task_id,
                "status": "completed",
                "dataset": "share_float",
                "mode": "full_market",
                "query_basis": "float_date",
                "date_range": {
                    "start_date": chunk_start_s,
                    "end_date": chunk_end_s,
                },
                "execution": execution,
            }));
        }
    }

    Ok(json!({
        "audit_version": "phase7-share-float-float-date-coverage-v1",
        "mode": if plan_only {
            "plan_only"
        } else if req.background {
            "background"
        } else {
            "synchronous"
        },
        "plan_only": plan_only,
        "background": req.background,
        "dataset": "share_float",
        "query_basis": "float_date",
        "pit_available_at": "ann_date",
        "chunk_granularity": granularity,
        "max_chunks": max_chunks,
        "chunk_count": chunks.len(),
        "truncated_by_max_chunks": truncated,
        "date_range": {
            "start_date": start_date,
            "end_date": end_date,
        },
        "data_version_prefix": data_version_prefix,
        "batches": batch_results,
        "notes": [
            "share_float must be expanded by unlock float_date windows because the Tushare interface does not support reliable symbol-filtered full-history expansion.",
            "ann_date is persisted as available_at; downstream PIT features must require available_at <= trade_date.",
            "plan_only defaults to true. Set plan_only=false only for controlled historical repair runs.",
            "Run phase7-share-float-readiness-audit after completion before building unlock pressure factors."
        ],
    }))
}

pub(crate) async fn build_phase7_share_float_readiness_audit(
    state: &AppState,
    req: Phase7ShareFloatReadinessAuditReq,
) -> Result<Value, String> {
    let today = chrono::Utc::now().date_naive();
    let start_date = req.start_date.unwrap_or_else(|| "20140101".to_string());
    let end_date = req
        .end_date
        .unwrap_or_else(|| today.format("%Y%m%d").to_string());
    let start = parse_optional_date(Some(start_date.as_str()))?
        .ok_or_else(|| "start_date is required".to_string())?;
    let end = parse_optional_date(Some(end_date.as_str()))?
        .ok_or_else(|| "end_date is required".to_string())?;
    if start > end {
        return Err("start_date must be <= end_date".to_string());
    }

    let row = sqlx::query_as::<
        _,
        (
            i64,
            i64,
            Option<NaiveDate>,
            Option<NaiveDate>,
            Option<NaiveDate>,
            Option<NaiveDate>,
            i64,
            i64,
        ),
    >(phase7_share_float_readiness_sql())
    .bind(start)
    .bind(end)
    .fetch_one(&state.db)
    .await
    .map_err(|error| format!("Failed to audit share_float readiness: {}", error))?;

    let (
        row_count,
        symbol_count,
        min_float_date,
        max_float_date,
        min_available_at,
        max_available_at,
        late_or_invalid_count,
        distinct_float_dates,
    ) = row;

    let completed_windows = sqlx::query_as::<_, (NaiveDate, NaiveDate)>(
        r#"
        SELECT start_date, end_date
        FROM data_sync_task
        WHERE task_type = 'share_float'
          AND status = 'completed'
          AND start_date IS NOT NULL
          AND end_date IS NOT NULL
          AND start_date <= $2
          AND end_date >= $1
        ORDER BY start_date, end_date
        "#,
    )
    .bind(start)
    .bind(end)
    .fetch_all(&state.db)
    .await
    .map_err(|error| format!("Failed to audit share_float sync windows: {}", error))?;
    let expected_days = phase7_share_float_expected_days(start, end);
    let covered_days =
        phase7_share_float_covered_days_from_windows(completed_windows.clone(), start, end);
    let sync_coverage_ratio = if expected_days > 0 {
        Some(covered_days as f64 / expected_days as f64)
    } else {
        None
    };
    let coverage_grade = if covered_days >= expected_days {
        phase7_coverage_grade(symbol_count, 1.max(symbol_count))
    } else {
        "incomplete_range"
    };
    let readiness = phase7_share_float_feature_readiness(
        row_count,
        covered_days,
        expected_days,
        late_or_invalid_count,
    );
    let completed_windows_json: Vec<Value> = completed_windows
        .into_iter()
        .map(|(window_start, window_end)| {
            json!({
                "start_date": window_start,
                "end_date": window_end,
            })
        })
        .collect();

    Ok(json!({
        "audit_version": "phase7-share-float-readiness-v1",
        "dataset": "share_float",
        "query_basis": "float_date",
        "pit_available_at": "ann_date",
        "date_range": {
            "start_date": start_date,
            "end_date": end_date,
        },
        "row_count": row_count,
        "symbol_count": symbol_count,
        "distinct_float_dates": distinct_float_dates,
        "expected_days": expected_days,
        "covered_days": covered_days,
        "sync_coverage_ratio": sync_coverage_ratio,
        "completed_sync_windows": completed_windows_json,
        "min_float_date": min_float_date,
        "max_float_date": max_float_date,
        "min_available_at": min_available_at,
        "max_available_at": max_available_at,
        "late_or_invalid_count": late_or_invalid_count,
        "coverage_grade": coverage_grade,
        "feature_readiness": readiness,
        "pit_contract": {
            "source_event_date": "float_date",
            "source_available_at": "ann_date",
            "feature_filter": "available_at <= trade_date AND float_date >= trade_date"
        },
        "late_announcement_policy": {
            "late_or_invalid_count": late_or_invalid_count,
            "feature_handling": "exclude_from_pre_unlock_pressure",
            "rationale": "late source announcements are not PIT-available before unlock and must not be backdated"
        },
        "notes": [
            "Rows with available_at after float_date are retained as raw source records but excluded from pre-unlock pressure by the PIT feature filter.",
            "This audit is source readiness only; RankIC/group return/turnover/capacity still require alpha-source diagnostics after factor backfill."
        ]
    }))
}

pub(crate) async fn build_phase7_industry_membership_coverage_audit(
    state: &AppState,
    req: Phase7IndustryMembershipCoverageAuditReq,
) -> Result<Value, String> {
    let latest_open_date: Option<NaiveDate> = sqlx::query_scalar(
        r#"
        SELECT MAX(trade_date)
        FROM market_trade_calendar
        WHERE exchange = 'SSE'
          AND is_open
          AND trade_date <= CURRENT_DATE
        "#,
    )
    .fetch_one(&state.db)
    .await
    .map_err(|error| format!("Failed to resolve latest open trade date: {}", error))?;
    let default_end = latest_open_date.unwrap_or_else(|| chrono::Utc::now().date_naive());
    let requested_start_date = req.start_date.unwrap_or_else(|| "20140101".to_string());
    let requested_end_date = req
        .end_date
        .unwrap_or_else(|| default_end.format("%Y%m%d").to_string());
    let start = parse_optional_date(Some(requested_start_date.as_str()))?
        .ok_or_else(|| "start_date is required".to_string())?;
    let requested_end = parse_optional_date(Some(requested_end_date.as_str()))?
        .ok_or_else(|| "end_date is required".to_string())?;
    let end = requested_end.min(default_end);
    if start > end {
        return Err("start_date must be <= effective end_date".to_string());
    }
    let limit = phase7_industry_membership_audit_limit(req.limit);

    let summary = sqlx::query_as::<
        _,
        (
            i64,
            i64,
            i64,
            i64,
            i64,
            Option<NaiveDate>,
            Option<NaiveDate>,
            i64,
            i64,
            i64,
            i64,
        ),
    >(phase7_industry_membership_snapshot_summary_sql())
    .bind(start)
    .bind(end)
    .fetch_one(&state.db)
    .await
    .map_err(|error| {
        format!(
            "Failed to audit industry membership snapshot coverage: {}",
            error
        )
    })?;
    let (
        trade_days,
        expected_symbol_days,
        covered_symbol_days,
        missing_symbol_days,
        multi_membership_symbol_days,
        min_trade_date,
        max_trade_date,
        pit_violation_rows,
        invalid_interval_rows,
        duplicate_key_rows,
        missing_exit_available_at_rows,
    ) = summary;
    let coverage_ratio = phase7_ratio(covered_symbol_days, expected_symbol_days);
    let readiness = phase7_industry_membership_snapshot_readiness(
        expected_symbol_days,
        covered_symbol_days,
        missing_symbol_days,
        multi_membership_symbol_days,
        pit_violation_rows,
        invalid_interval_rows,
        duplicate_key_rows,
        missing_exit_available_at_rows,
    );

    let missing_symbol_rows = sqlx::query_as::<
        _,
        (
            String,
            String,
            String,
            String,
            bool,
            Option<NaiveDate>,
            Option<NaiveDate>,
            i64,
        ),
    >(
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
        SELECT joined.symbol,
               COALESCE(stock.name, '') AS name,
               COALESCE(stock.list_status, '') AS list_status,
               COALESCE(stock.market, '') AS market,
               COALESCE(stock.is_st, false) AS is_st,
               MIN(joined.trade_date) AS first_missing_date,
               MAX(joined.trade_date) AS latest_missing_date,
               COUNT(*)::bigint AS missing_days
        FROM joined
        LEFT JOIN market_stock stock ON stock.symbol = joined.symbol
        WHERE joined.active_index_count = 0
        GROUP BY joined.symbol, stock.name, stock.list_status, stock.market, stock.is_st
        ORDER BY missing_days DESC, joined.symbol
        LIMIT $3
        "#,
    )
    .bind(start)
    .bind(end)
    .bind(limit)
    .fetch_all(&state.db)
    .await
    .map_err(|error| format!("Failed to sample missing industry memberships: {}", error))?;
    let missing_symbols: Vec<Value> = missing_symbol_rows
        .into_iter()
        .map(
            |(
                symbol,
                name,
                list_status,
                market,
                is_st,
                first_missing_date,
                latest_missing_date,
                missing_days,
            )| {
                json!({
                    "symbol": symbol,
                    "name": name,
                    "list_status": list_status,
                    "market": market,
                    "is_st": is_st,
                    "first_missing_date": first_missing_date,
                    "latest_missing_date": latest_missing_date,
                    "missing_days": missing_days,
                })
            },
        )
        .collect();

    let multi_membership_rows = sqlx::query_as::<
        _,
        (
            String,
            String,
            Option<NaiveDate>,
            Option<NaiveDate>,
            i64,
            i64,
            Option<String>,
        ),
    >(
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
        membership_counts AS (
            SELECT days.trade_date,
                   membership.symbol,
                   COUNT(DISTINCT membership.index_code)::bigint AS active_index_count,
                   STRING_AGG(DISTINCT membership.index_code, ',' ORDER BY membership.index_code)
                       AS index_codes
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
            HAVING COUNT(DISTINCT membership.index_code) > 1
        )
        SELECT membership_counts.symbol,
               COALESCE(stock.name, '') AS name,
               MIN(membership_counts.trade_date) AS first_multi_date,
               MAX(membership_counts.trade_date) AS latest_multi_date,
               COUNT(*)::bigint AS multi_days,
               MAX(membership_counts.active_index_count)::bigint AS max_active_index_count,
               MIN(membership_counts.index_codes) AS sample_index_codes
        FROM membership_counts
        LEFT JOIN market_stock stock ON stock.symbol = membership_counts.symbol
        GROUP BY membership_counts.symbol, stock.name
        ORDER BY multi_days DESC, membership_counts.symbol
        LIMIT $3
        "#,
    )
    .bind(start)
    .bind(end)
    .bind(limit)
    .fetch_all(&state.db)
    .await
    .map_err(|error| format!("Failed to sample multi-membership symbols: {}", error))?;
    let multi_membership_symbols: Vec<Value> = multi_membership_rows
        .into_iter()
        .map(
            |(
                symbol,
                name,
                first_multi_date,
                latest_multi_date,
                multi_days,
                max_active_index_count,
                sample_index_codes,
            )| {
                json!({
                    "symbol": symbol,
                    "name": name,
                    "first_multi_date": first_multi_date,
                    "latest_multi_date": latest_multi_date,
                    "multi_days": multi_days,
                    "max_active_index_count": max_active_index_count,
                    "sample_index_codes": sample_index_codes,
                })
            },
        )
        .collect();

    let worst_day_rows = sqlx::query_as::<_, (NaiveDate, i64, i64, i64, Option<f64>)>(
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
        SELECT joined.trade_date,
               COUNT(*)::bigint AS expected_symbols,
               COUNT(*) FILTER (WHERE joined.active_index_count = 0)::bigint AS missing_symbols,
               COUNT(*) FILTER (WHERE joined.active_index_count > 1)::bigint AS multi_membership_symbols,
               (COUNT(*) FILTER (WHERE joined.active_index_count = 1))::double precision
                   / NULLIF(COUNT(*), 0)::double precision AS coverage_ratio
        FROM joined
        GROUP BY joined.trade_date
        ORDER BY (COUNT(*) FILTER (WHERE joined.active_index_count = 0)
                  + COUNT(*) FILTER (WHERE joined.active_index_count > 1)) DESC,
                 joined.trade_date
        LIMIT $3
        "#,
    )
    .bind(start)
    .bind(end)
    .bind(limit)
    .fetch_all(&state.db)
    .await
    .map_err(|error| format!("Failed to sample worst industry membership days: {}", error))?;
    let worst_days: Vec<Value> = worst_day_rows
        .into_iter()
        .map(
            |(
                trade_date,
                expected_symbols,
                missing_symbols,
                multi_membership_symbols,
                daily_coverage_ratio,
            )| {
                json!({
                    "trade_date": trade_date,
                    "expected_symbols": expected_symbols,
                    "missing_symbols": missing_symbols,
                    "multi_membership_symbols": multi_membership_symbols,
                    "coverage_ratio": daily_coverage_ratio,
                })
            },
        )
        .collect();

    let year_breakdown_rows = sqlx::query_as::<_, (NaiveDate, i64, i64, i64, i64, Option<f64>)>(
        phase7_industry_membership_year_breakdown_sql(),
    )
    .bind(start)
    .bind(end)
    .fetch_all(&state.db)
    .await
    .map_err(|error| {
        format!(
            "Failed to build industry membership year breakdown: {}",
            error
        )
    })?;
    let by_year: Vec<Value> = year_breakdown_rows
        .into_iter()
        .map(
            |(
                period_start,
                expected_symbol_days,
                covered_symbol_days,
                missing_symbol_days,
                multi_membership_symbol_days,
                period_coverage_ratio,
            )| {
                json!({
                    "year": period_start.year(),
                    "period_start": period_start,
                    "expected_symbol_days": expected_symbol_days,
                    "covered_symbol_days": covered_symbol_days,
                    "missing_symbol_days": missing_symbol_days,
                    "multi_membership_symbol_days": multi_membership_symbol_days,
                    "coverage_ratio": period_coverage_ratio,
                })
            },
        )
        .collect();

    let market_breakdown_rows = sqlx::query_as::<_, (String, i64, i64, i64, i64, Option<f64>)>(
        phase7_industry_membership_market_breakdown_sql(),
    )
    .bind(start)
    .bind(end)
    .fetch_all(&state.db)
    .await
    .map_err(|error| {
        format!(
            "Failed to build industry membership market breakdown: {}",
            error
        )
    })?;
    let mut eligible_markets = Vec::new();
    let mut excluded_markets = Vec::new();
    let by_market: Vec<Value> = market_breakdown_rows
        .into_iter()
        .map(
            |(
                market,
                expected_symbol_days,
                covered_symbol_days,
                missing_symbol_days,
                multi_membership_symbol_days,
                market_coverage_ratio,
            )| {
                if phase7_industry_membership_market_scope_eligible(
                    market_coverage_ratio,
                    multi_membership_symbol_days,
                ) {
                    eligible_markets.push(market.clone());
                } else {
                    excluded_markets.push(market.clone());
                }
                json!({
                    "market": market,
                    "expected_symbol_days": expected_symbol_days,
                    "covered_symbol_days": covered_symbol_days,
                    "missing_symbol_days": missing_symbol_days,
                    "multi_membership_symbol_days": multi_membership_symbol_days,
                    "coverage_ratio": market_coverage_ratio,
                })
            },
        )
        .collect();
    let alpha_admission_gate = industry_prosperity_alpha_admission_policy(
        eligible_markets.clone(),
        excluded_markets.clone(),
    );

    Ok(json!({
        "audit_version": "phase7-industry-membership-coverage-v1",
        "dataset": "market_stock_industry_membership_pit",
        "classification_source": "SW2014_until_2021_12_12_then_SW2021",
        "source_version_gate": {
            "SW2014": "trade_date < 2021-12-13",
            "SW2021": "trade_date >= 2021-12-13"
        },
        "industry_level": "L1",
        "date_range": {
            "requested_start_date": requested_start_date,
            "requested_end_date": requested_end_date,
            "start_date": start,
            "end_date": end,
            "latest_open_trade_date": latest_open_date,
            "capped_by_latest_open_trade_date": requested_end > end,
        },
        "trade_days": trade_days,
        "expected_symbol_days": expected_symbol_days,
        "covered_symbol_days": covered_symbol_days,
        "missing_symbol_days": missing_symbol_days,
        "multi_membership_symbol_days": multi_membership_symbol_days,
        "coverage_ratio": coverage_ratio,
        "min_trade_date": min_trade_date,
        "max_trade_date": max_trade_date,
        "raw_source_checks": {
            "pit_violation_rows": pit_violation_rows,
            "invalid_interval_rows": invalid_interval_rows,
            "duplicate_key_rows": duplicate_key_rows,
            "missing_exit_available_at_rows": missing_exit_available_at_rows,
        },
        "breakdown": {
            "by_year": by_year,
            "by_market": by_market,
        },
        "market_scope_gate_candidate": {
            "coverage_threshold": INDUSTRY_MEMBERSHIP_COVERAGE_THRESHOLD,
            "eligible_markets": eligible_markets,
            "excluded_markets": excluded_markets,
            "required_universe_profile": INDUSTRY_PROSPERITY_REQUIRED_UNIVERSE_PROFILE,
            "gate_rule": "Only evaluate industry prosperity proxy for markets with coverage_ratio >= 0.995 and multi_membership_symbol_days = 0; excluded markets must not be statically backfilled."
        },
        "alpha_admission_gate": alpha_admission_gate,
        "readiness": readiness,
        "next_step": match readiness {
            "snapshot_ready_for_p310_diagnostics" => {
                "run_p310_rankic_group_decay_turnover_capacity_diagnostics"
            }
            "snapshot_multi_membership_blocked" => {
                "repair_sw2021_retroactive_current_memberships_or_add_source_version_gate"
            }
            "snapshot_raw_source_pit_failed" => "repair_raw_interval_available_at_contract",
            "snapshot_coverage_gaps_need_review" => "classify_missing_symbol_days_before_factor_design",
            _ => "repair_universe_or_calendar_inputs",
        },
        "samples": {
            "limit": limit,
            "top_missing_symbols": missing_symbols,
            "top_multi_membership_symbols": multi_membership_symbols,
            "worst_days": worst_days,
        },
        "pit_contract": {
            "entry_filter": "available_at <= trade_date AND in_date <= trade_date",
            "exit_filter": "exit_available_at IS NULL OR exit_available_at > trade_date",
            "forbidden_inputs": ["market_stock.industry static snapshot"]
        },
        "notes": [
            "This audit is source coverage/readiness only; it does not construct an alpha factor and does not unlock bounded WFA.",
            "Multi-membership symbol-days are blocking because a cross-sectional industry proxy cannot choose between overlapping L1 memberships without an explicit PIT source-version rule.",
            "Coverage gaps may be acceptable only after they are classified as ST/special shares or otherwise outside the intended tradable universe."
        ]
    }))
}

async fn build_phase7_financial_coverage_batches(
    state: Arc<AppState>,
    start_date: &str,
    end_date: &str,
    batch_size: usize,
    batch_count: usize,
    plan_only: bool,
    data_version_prefix: &str,
) -> Result<Value, String> {
    let start = parse_optional_date(Some(start_date))?
        .ok_or_else(|| "start_date is required".to_string())?;
    let end =
        parse_optional_date(Some(end_date))?.ok_or_else(|| "end_date is required".to_string())?;
    let child_background = phase7_optional_source_batch_child_background(plan_only);
    let offsets = phase7_optional_source_batch_offsets(0, batch_size, batch_count);
    let planned_next_offset = phase7_optional_source_batch_next_offset(&offsets, batch_size);
    let recommended_resume_offset =
        phase7_optional_source_batch_recommended_resume_offset(plan_only, &offsets, batch_size);

    let mut batches = Vec::new();
    for (batch_index, offset_symbols) in offsets.iter().copied().enumerate() {
        let symbols =
            resolve_phase7_financial_sync_symbols(&state, start, end, batch_size, offset_symbols)
                .await?;
        let batch_label = format!("b{:03}", batch_index + 1);
        let task_id =
            bounded_phase7_task_id(&[data_version_prefix, "financial", batch_label.as_str()]);
        let sync_req = DataSyncTaskReq {
            dataset: "financial".to_string(),
            source: "tushare".to_string(),
            mode: Some("bounded_symbols".to_string()),
            symbols: symbols.clone(),
            source_filters: Vec::new(),
            index_codes: Vec::new(),
            exchanges: Vec::new(),
            start_date: Some(start_date.to_string()),
            end_date: Some(end_date.to_string()),
            data_version_id: Some(task_id.clone()),
            background: child_background,
            quality_check: false,
            create_data_version: true,
            retry_of_task_id: None,
            reason: Some("phase7 financial bounded coverage expansion".to_string()),
        };

        let status;
        let mut execution = None;
        if symbols.is_empty() {
            status = "skipped_no_symbols";
        } else if plan_only {
            status = "planned";
        } else if child_background {
            register_sync_task(&state, &task_id, &sync_req, "running").await?;
            let state_for_task = state.clone();
            let task_id_for_task = task_id.clone();
            let req_for_task = sync_req.clone();
            crate::sync_task_registry::spawn_sync_task(
                state.sync_tasks.clone(),
                task_id.clone(),
                async move {
                    if let Err(message) = execute_sync_task(
                        state_for_task.clone(),
                        task_id_for_task.clone(),
                        req_for_task,
                    )
                    .await
                    {
                        let _ = quant_data::repository::fail_sync_task(
                            &state_for_task.db,
                            &task_id_for_task,
                            &message,
                        )
                        .await;
                        tracing::error!(task_id = %task_id_for_task, error = %message, "Phase 7 financial bounded 补数失败");
                    }
                },
            )
            .await;
            status = "running";
        } else {
            execution = Some(execute_sync_task(state.clone(), task_id.clone(), sync_req).await?);
            status = "completed";
        }

        batches.push(json!({
            "batch_index": batch_index + 1,
            "offset_symbols": offset_symbols,
            "batch_size": batch_size,
            "task_id": task_id,
            "status": status,
            "selected_count": symbols.len(),
            "selected_symbols": symbols,
            "execution": execution,
        }));
    }

    Ok(json!({
        "audit_version": "phase7-ff-financial-bounded-batch-sync-v1",
        "mode": if plan_only { "plan_only" } else { "background" },
        "plan_only": plan_only,
        "child_background": child_background,
        "batch_size": batch_size,
        "batch_count": batch_count,
        "next_offset": recommended_resume_offset,
        "planned_next_offset": planned_next_offset,
        "date_range": {
            "start_date": start_date,
            "end_date": end_date,
        },
        "source": "financial",
        "data_version_prefix": data_version_prefix,
        "batches": batches,
        "notes": [
            "This financial runner resolves a bounded stock list before syncing; it never uses an empty symbols full-market request.",
            "It reuses the existing financial dataset sync for market_financial_statement and market_financial_indicator.",
            "Run phase7-feasibility-audit after completion and keep financial alpha blocked until coverage reaches partial_feature_candidate."
        ],
    }))
}

fn build_phase7_bounded_sync_req(
    dataset: &str,
    symbols: Vec<String>,
    start_date: &str,
    end_date: &str,
    task_id: &str,
    reason: &str,
) -> DataSyncTaskReq {
    DataSyncTaskReq {
        dataset: dataset.to_string(),
        source: "tushare".to_string(),
        mode: Some("bounded_symbols".to_string()),
        symbols,
        source_filters: Vec::new(),
        index_codes: Vec::new(),
        exchanges: Vec::new(),
        start_date: Some(start_date.to_string()),
        end_date: Some(end_date.to_string()),
        data_version_id: Some(task_id.to_string()),
        background: false,
        quality_check: false,
        create_data_version: true,
        retry_of_task_id: None,
        reason: Some(reason.to_string()),
    }
}

async fn run_phase7_autopilot_bounded_sync(
    state: Arc<AppState>,
    dataset: &str,
    symbols: Vec<String>,
    start_date: &str,
    end_date: &str,
    task_id: String,
    reason: &str,
) -> Result<Value, String> {
    let sync_req =
        build_phase7_bounded_sync_req(dataset, symbols, start_date, end_date, &task_id, reason);
    execute_sync_task(state, task_id, sync_req).await
}

async fn run_phase7_autopilot_optional_round(
    state: Arc<AppState>,
    sources: &[String],
    start_date: &str,
    end_date: &str,
    batch_size: usize,
    batch_count: usize,
    round_prefix: &str,
) -> Result<(), String> {
    for (batch_index, offset_symbols) in phase7_coverage_autopilot_batch_offsets(batch_count)
        .into_iter()
        .enumerate()
    {
        for source in sources {
            let symbols = resolve_phase7_optional_source_sync_symbols(
                &state,
                source,
                &[],
                parse_optional_date(Some(start_date))?
                    .ok_or_else(|| "start_date is required".to_string())?,
                parse_optional_date(Some(end_date))?
                    .ok_or_else(|| "end_date is required".to_string())?,
                batch_size,
                offset_symbols,
            )
            .await?;
            if symbols.is_empty() {
                tracing::info!(
                    source,
                    round_prefix,
                    "Phase 7 coverage autopilot optional source has no selected symbols"
                );
                continue;
            }
            let batch_label = format!("b{:03}", batch_index + 1);
            let task_id =
                bounded_phase7_task_id(&[round_prefix, source.as_str(), batch_label.as_str()]);
            run_phase7_autopilot_bounded_sync(
                state.clone(),
                source,
                symbols,
                start_date,
                end_date,
                task_id,
                "phase7 coverage autopilot bounded optional source expansion",
            )
            .await?;
        }
    }
    Ok(())
}

async fn run_phase7_autopilot_financial_round(
    state: Arc<AppState>,
    start_date: &str,
    end_date: &str,
    batch_size: usize,
    batch_count: usize,
    round_prefix: &str,
) -> Result<(), String> {
    for (batch_index, offset_symbols) in phase7_coverage_autopilot_batch_offsets(batch_count)
        .into_iter()
        .enumerate()
    {
        let symbols = resolve_phase7_financial_sync_symbols(
            &state,
            parse_optional_date(Some(start_date))?
                .ok_or_else(|| "start_date is required".to_string())?,
            parse_optional_date(Some(end_date))?
                .ok_or_else(|| "end_date is required".to_string())?,
            batch_size,
            offset_symbols,
        )
        .await?;
        if symbols.is_empty() {
            tracing::info!(
                round_prefix,
                "Phase 7 coverage autopilot financial source has no selected symbols"
            );
            continue;
        }
        let batch_label = format!("b{:03}", batch_index + 1);
        let task_id = bounded_phase7_task_id(&[round_prefix, "financial", batch_label.as_str()]);
        run_phase7_autopilot_bounded_sync(
            state.clone(),
            "financial",
            symbols,
            start_date,
            end_date,
            task_id,
            "phase7 coverage autopilot bounded financial expansion",
        )
        .await?;
    }
    Ok(())
}

async fn run_phase7_coverage_autopilot_background(
    state: Arc<AppState>,
    start_date: String,
    end_date: String,
    requested_sources: Vec<String>,
    batch_size: usize,
    batch_count: usize,
    max_rounds: usize,
    target_ratio: f64,
    data_version_prefix: String,
) {
    for round_index in 0..max_rounds {
        let round_audit = match build_phase7_feasibility_audit(&state).await {
            Ok(audit) => audit,
            Err(error) => {
                tracing::error!(error = %error, "Phase 7 coverage autopilot audit failed");
                break;
            }
        };
        let start = match parse_optional_date(Some(&start_date))
            .and_then(|value| value.ok_or_else(|| "start_date is required".to_string()))
        {
            Ok(value) => value,
            Err(error) => {
                tracing::error!(error = %error, "Phase 7 coverage autopilot start date parse failed");
                break;
            }
        };
        let end = match parse_optional_date(Some(&end_date))
            .and_then(|value| value.ok_or_else(|| "end_date is required".to_string()))
        {
            Ok(value) => value,
            Err(error) => {
                tracing::error!(error = %error, "Phase 7 coverage autopilot end date parse failed");
                break;
            }
        };
        let window_attempts = match phase7_completed_attempts_by_source_for_window(
            &state,
            &requested_sources,
            start,
            end,
        )
        .await
        {
            Ok(attempts) => attempts,
            Err(error) => {
                tracing::error!(error = %error, "Phase 7 coverage autopilot attempt audit failed");
                break;
            }
        };
        let (readiness, coverage) =
            phase7_coverage_runner_source_state_for_window(&round_audit, Some(&window_attempts));
        let planned_sources: Vec<String> = requested_sources
            .iter()
            .filter(|source| {
                phase7_coverage_runner_should_plan_source_for_target(
                    readiness.get(*source).map(String::as_str),
                    coverage.get(*source).copied(),
                    target_ratio,
                    false,
                )
            })
            .cloned()
            .collect();
        if planned_sources.is_empty() {
            tracing::info!(
                round = round_index + 1,
                target_ratio,
                "Phase 7 coverage autopilot reached target"
            );
            break;
        }

        let round_label = format!("r{:03}", round_index + 1);
        let round_prefix =
            bounded_phase7_task_id(&[data_version_prefix.as_str(), round_label.as_str()]);
        let optional_sources: Vec<String> = planned_sources
            .iter()
            .filter(|source| source.as_str() != "financial")
            .cloned()
            .collect();
        if !optional_sources.is_empty() {
            if let Err(error) = run_phase7_autopilot_optional_round(
                state.clone(),
                &optional_sources,
                &start_date,
                &end_date,
                batch_size,
                batch_count,
                &bounded_phase7_task_id(&[round_prefix.as_str(), "optional"]),
            )
            .await
            {
                tracing::error!(round = round_index + 1, error = %error, "Phase 7 optional coverage autopilot round failed");
                break;
            }
        }

        if planned_sources.iter().any(|source| source == "financial") {
            if let Err(error) = run_phase7_autopilot_financial_round(
                state.clone(),
                &start_date,
                &end_date,
                batch_size,
                batch_count,
                &bounded_phase7_task_id(&[round_prefix.as_str(), "financial"]),
            )
            .await
            {
                tracing::error!(round = round_index + 1, error = %error, "Phase 7 financial coverage autopilot round failed");
                break;
            }
        }
    }
}

pub(crate) async fn build_phase7_coverage_expansion_runner(
    state: Arc<AppState>,
    req: Phase7CoverageExpansionRunnerReq,
) -> Result<Value, String> {
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
        return Err("start_date must be <= end_date".to_string());
    }

    let profile = phase7_coverage_runner_profile(req.profile.as_deref());
    let requested_sources = phase7_coverage_runner_sources(&req.sources)?;
    let batch_size = phase7_coverage_runner_batch_size(req.batch_size);
    let batch_count = phase7_coverage_runner_batch_count(req.batch_count);
    let plan_only = phase7_coverage_runner_plan_only(req.plan_only);
    let auto_continue = phase7_coverage_runner_auto_continue(req.auto_continue);
    let max_rounds = phase7_coverage_runner_max_rounds(req.max_rounds, auto_continue);
    let target_ratio = phase7_coverage_runner_target_ratio(req.target_coverage_ratio);
    let stop_when_ready = req.stop_when_readiness_at_least_partial.unwrap_or(false);
    let data_version_prefix = req.data_version_prefix.clone().unwrap_or_else(|| {
        chrono::Utc::now()
            .format("dv-phase7-ff-coverage-runner-%Y%m%d-%H%M%S%3f")
            .to_string()
    });

    let audit = build_phase7_feasibility_audit(&state).await?;
    let window_attempts =
        phase7_completed_attempts_by_source_for_window(&state, &requested_sources, start, end)
            .await?;
    let (source_readiness, source_coverage_ratio) =
        phase7_coverage_runner_source_state_for_window(&audit, Some(&window_attempts));

    let planned_sources: Vec<String> = requested_sources
        .iter()
        .filter(|source| {
            phase7_coverage_runner_should_plan_source_for_target(
                source_readiness.get(*source).map(String::as_str),
                source_coverage_ratio.get(*source).copied(),
                target_ratio,
                stop_when_ready,
            )
        })
        .cloned()
        .collect();

    let skipped_sources: Vec<Value> = requested_sources
        .iter()
        .filter(|source| !planned_sources.contains(source))
        .map(|source| {
            json!({
                "source": source,
                "reason": "readiness_at_least_partial",
                "feature_readiness": source_readiness.get(source).cloned().unwrap_or_else(|| "unknown".to_string()),
            })
        })
        .collect();

    let optional_sources: Vec<String> = planned_sources
        .iter()
        .filter(|source| source.as_str() != "financial")
        .cloned()
        .collect();
    let build_immediate_batches =
        phase7_coverage_runner_should_build_immediate_batches(plan_only, auto_continue);
    let optional_batch_result = if optional_sources.is_empty() || !build_immediate_batches {
        None
    } else {
        let batch_req = Phase7OptionalSourceCoverageBatchReq {
            sources: optional_sources.clone(),
            start_date: Some(start_date.clone()),
            end_date: Some(end_date.clone()),
            batch_size: Some(batch_size),
            batch_count: Some(batch_count),
            start_offset: Some(0),
            plan_only: Some(plan_only),
            data_version_prefix: Some(data_version_prefix.clone()),
        };
        Some(build_phase7_optional_source_coverage_batches(state.clone(), batch_req).await?)
    };
    let financial_batch_result =
        if planned_sources.iter().any(|source| source == "financial") && build_immediate_batches {
            Some(
                build_phase7_financial_coverage_batches(
                    state.clone(),
                    &start_date,
                    &end_date,
                    batch_size,
                    batch_count,
                    plan_only,
                    &data_version_prefix,
                )
                .await?,
            )
        } else {
            None
        };
    if auto_continue && !plan_only && !planned_sources.is_empty() {
        let state_for_task = state.clone();
        let start_date_for_task = start_date.clone();
        let end_date_for_task = end_date.clone();
        let requested_sources_for_task = requested_sources.clone();
        let data_version_prefix_for_task = data_version_prefix.clone();
        // 合成 task_id 用于 registry 取消(phase7 autopilot 不注册 data_sync_task)
        let autopilot_task_id = format!("phase7-autopilot-{}", data_version_prefix);
        crate::sync_task_registry::spawn_sync_task(
            state.sync_tasks.clone(),
            autopilot_task_id.clone(),
            async move {
                run_phase7_coverage_autopilot_background(
                    state_for_task,
                    start_date_for_task,
                    end_date_for_task,
                    requested_sources_for_task,
                    batch_size,
                    batch_count,
                    max_rounds,
                    target_ratio,
                    data_version_prefix_for_task,
                )
                .await;
            },
        )
        .await;
    }

    Ok(json!({
        "audit_version": "phase7-ff-coverage-runner-v1",
        "profile": profile,
        "mode": if plan_only { "plan_only" } else if auto_continue { "autopilot_background" } else { "background" },
        "plan_only": plan_only,
        "auto_continue": auto_continue,
        "max_rounds": max_rounds,
        "target_coverage_ratio": target_ratio,
        "partial_feature_candidate_gate_ratio": PHASE7_COVERAGE_RUNNER_PARTIAL_GATE_RATIO,
        "coverage_target_policy": if stop_when_ready { "legacy_stop_at_partial_gate" } else { "full_available_coverage_by_default" },
        "stop_when_readiness_at_least_partial": stop_when_ready,
        "batch_size": batch_size,
        "batch_count": batch_count,
        "max_child_tasks": planned_sources.len() * batch_count * max_rounds,
        "date_range": {
            "start_date": start_date,
            "end_date": end_date,
        },
        "requested_sources": requested_sources,
        "planned_sources": planned_sources,
        "skipped_sources": skipped_sources,
        "optional_source_readiness": source_readiness.clone(),
        "source_readiness": source_readiness,
        "source_coverage_ratio": source_coverage_ratio,
        "window_completed_attempts": window_attempts,
        "data_version_prefix": data_version_prefix,
        "optional_batch_plan": optional_batch_result,
        "financial_batch_plan": financial_batch_result,
        "notes": [
            "This runner orchestrates bounded coverage expansion only; it never builds features or runs strategy discovery.",
            "Default profile is local_mac_safe and defaults to plan_only=true.",
            "The runner supports symbol-filtered cashflow/dividend and bounded financial coverage sync; repurchase is excluded because the Tushare API is announcement-date-range based.",
            "The 30% partial_feature_candidate threshold is only a research unlock gate; the default target_coverage_ratio is 1.0 for full available coverage.",
            "When auto_continue=true and plan_only=false, one background autopilot task runs bounded child syncs synchronously round by round, then re-audits before selecting the next uncovered batch.",
            "Feature factories remain blocked while readiness is sample_only_do_not_train."
        ],
    }))
}

pub(crate) async fn resolve_phase7_permission_smoke_symbols(
    state: &AppState,
    requested: &[String],
) -> Result<Vec<String>, String> {
    let mut seen = BTreeSet::new();
    let symbols: Vec<String> = requested
        .iter()
        .map(|symbol| symbol.trim().to_ascii_uppercase())
        .filter(|symbol| !symbol.is_empty())
        .filter(|symbol| seen.insert(symbol.clone()))
        .take(PHASE7_PERMISSION_SMOKE_MAX_SYMBOLS)
        .collect();
    if !symbols.is_empty() {
        return Ok(symbols);
    }

    let preferred = sqlx::query_scalar::<_, String>(
        r#"
        SELECT symbol
        FROM market_stock
        WHERE list_status = 'L'
          AND symbol IN ('000001.SZ', '600000.SH', '000333.SZ')
        ORDER BY CASE symbol
            WHEN '000001.SZ' THEN 1
            WHEN '600000.SH' THEN 2
            WHEN '000333.SZ' THEN 3
            ELSE 99
        END
        LIMIT 3
        "#,
    )
    .fetch_all(&state.db)
    .await
    .map_err(|error| error.to_string())?;

    let mut resolved = Vec::new();
    let mut seen = BTreeSet::new();
    for symbol in preferred {
        if seen.insert(symbol.clone()) {
            resolved.push(symbol);
        }
    }

    if resolved.len() < PHASE7_PERMISSION_SMOKE_MAX_SYMBOLS {
        let fallback = sqlx::query_scalar::<_, String>(
            r#"
            SELECT symbol
            FROM market_stock
            WHERE list_status = 'L'
            ORDER BY symbol
            LIMIT 3
            "#,
        )
        .fetch_all(&state.db)
        .await
        .map_err(|error| error.to_string())?;

        for symbol in fallback {
            if seen.insert(symbol.clone()) {
                resolved.push(symbol);
            }
            if resolved.len() >= PHASE7_PERMISSION_SMOKE_MAX_SYMBOLS {
                break;
            }
        }
    }

    if resolved.is_empty() {
        Err("no sample symbols available for Tushare permission smoke".to_string())
    } else {
        Ok(resolved)
    }
}

async fn resolve_phase7_optional_source_sync_symbols(
    state: &AppState,
    source: &str,
    requested: &[String],
    start: NaiveDate,
    end: NaiveDate,
    max_symbols: usize,
    offset_symbols: usize,
) -> Result<Vec<String>, String> {
    let mut seen = BTreeSet::new();
    let symbols: Vec<String> = requested
        .iter()
        .map(|symbol| symbol.trim().to_ascii_uppercase())
        .filter(|symbol| !symbol.is_empty())
        .filter(|symbol| seen.insert(symbol.clone()))
        .take(max_symbols)
        .collect();
    if !symbols.is_empty() {
        return Ok(symbols);
    }

    let limit = max_symbols as i64;
    let offset = offset_symbols as i64;
    let table = phase7_optional_source_table(source)
        .ok_or_else(|| format!("unsupported optional source: {}", source))?;
    let table_exists: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = $1
        )
        "#,
    )
    .bind(table)
    .fetch_one(&state.db)
    .await
    .map_err(|error| error.to_string())?;

    let uncovered = if table_exists {
        let sql = phase7_optional_source_uncovered_symbols_sql(source)
            .ok_or_else(|| format!("unsupported optional source: {}", source))?;
        sqlx::query_scalar::<_, String>(sql)
            .bind(start)
            .bind(end)
            .bind(offset)
            .bind(limit)
            .fetch_all(&state.db)
            .await
            .map_err(|error| error.to_string())?
    } else {
        Vec::new()
    };

    if !uncovered.is_empty() {
        return Ok(uncovered);
    }
    if table_exists {
        return Ok(Vec::new());
    }

    Ok(Vec::new())
}

pub(crate) fn phase7_optional_source_uncovered_symbols_sql(source: &str) -> Option<&'static str> {
    match source {
        "cashflow" => Some(
            r#"
            SELECT stock.symbol
            FROM market_stock stock
            WHERE stock.list_status = 'L'
              AND NOT EXISTS (
                  SELECT 1 FROM data_sync_attempt attempt
                  WHERE attempt.source = 'cashflow'
                    AND attempt.symbol = stock.symbol
                    AND attempt.status = 'completed'
                    AND attempt.start_date <= $1
                    AND attempt.end_date >= $2
              )
            ORDER BY stock.symbol
            OFFSET $3 LIMIT $4
            "#,
        ),
        "dividend" => Some(
            r#"
            SELECT stock.symbol
            FROM market_stock stock
            WHERE stock.list_status = 'L'
              AND NOT EXISTS (
                  SELECT 1 FROM data_sync_attempt attempt
                  WHERE attempt.source = 'dividend'
                    AND attempt.symbol = stock.symbol
                    AND attempt.status = 'completed'
                    AND attempt.start_date <= $1
                    AND attempt.end_date >= $2
              )
            ORDER BY stock.symbol
            OFFSET $3 LIMIT $4
            "#,
        ),
        "repurchase" => Some(
            r#"
            SELECT stock.symbol
            FROM market_stock stock
            WHERE stock.list_status = 'L'
              AND NOT EXISTS (
                  SELECT 1 FROM data_sync_attempt attempt
                  WHERE attempt.source = 'repurchase'
                    AND attempt.symbol = stock.symbol
                    AND attempt.status = 'completed'
                    AND attempt.start_date <= $1
                    AND attempt.end_date >= $2
              )
            ORDER BY stock.symbol
            OFFSET $3 LIMIT $4
            "#,
        ),
        "forecast" => Some(
            r#"
            SELECT stock.symbol
            FROM market_stock stock
            WHERE stock.list_status = 'L'
              AND NOT EXISTS (
                  SELECT 1 FROM data_sync_attempt attempt
                  WHERE attempt.source = 'forecast'
                    AND attempt.symbol = stock.symbol
                    AND attempt.status = 'completed'
                    AND attempt.start_date <= $1
                    AND attempt.end_date >= $2
              )
            ORDER BY stock.symbol
            OFFSET $3 LIMIT $4
            "#,
        ),
        "express" => Some(
            r#"
            SELECT stock.symbol
            FROM market_stock stock
            WHERE stock.list_status = 'L'
              AND NOT EXISTS (
                  SELECT 1 FROM data_sync_attempt attempt
                  WHERE attempt.source = 'express'
                    AND attempt.symbol = stock.symbol
                    AND attempt.status = 'completed'
                    AND attempt.start_date <= $1
                    AND attempt.end_date >= $2
              )
            ORDER BY stock.symbol
            OFFSET $3 LIMIT $4
            "#,
        ),
        "disclosure_date" => Some(
            r#"
            SELECT stock.symbol
            FROM market_stock stock
            WHERE stock.list_status = 'L'
              AND NOT EXISTS (
                  SELECT 1 FROM data_sync_attempt attempt
                  WHERE attempt.source = 'disclosure_date'
                    AND attempt.symbol = stock.symbol
                    AND attempt.status = 'completed'
                    AND attempt.start_date <= $1
                    AND attempt.end_date >= $2
              )
            ORDER BY stock.symbol
            OFFSET $3 LIMIT $4
            "#,
        ),
        "share_float" => Some(
            r#"
            SELECT stock.symbol
            FROM market_stock stock
            WHERE stock.list_status = 'L'
              AND NOT EXISTS (
                  SELECT 1 FROM data_sync_attempt attempt
                  WHERE attempt.source = 'share_float'
                    AND attempt.symbol = stock.symbol
                    AND attempt.status = 'completed'
                    AND attempt.start_date <= $1
                    AND attempt.end_date >= $2
              )
            ORDER BY stock.symbol
            OFFSET $3 LIMIT $4
            "#,
        ),
        _ => None,
    }
}

async fn resolve_phase7_financial_sync_symbols(
    state: &AppState,
    start: NaiveDate,
    end: NaiveDate,
    max_symbols: usize,
    offset_symbols: usize,
) -> Result<Vec<String>, String> {
    let limit = max_symbols as i64;
    let offset = offset_symbols as i64;
    let symbols = sqlx::query_scalar::<_, String>(phase7_financial_uncovered_symbols_sql())
        .bind(start)
        .bind(end)
        .bind(offset)
        .bind(limit)
        .fetch_all(&state.db)
        .await
        .map_err(|error| error.to_string())?;
    Ok(symbols)
}

pub(crate) fn phase7_financial_uncovered_symbols_sql() -> &'static str {
    r#"
    SELECT stock.symbol
    FROM market_stock stock
    WHERE stock.list_status = 'L'
      AND NOT EXISTS (
          SELECT 1 FROM data_sync_attempt attempt
          WHERE attempt.source = 'financial'
            AND attempt.symbol = stock.symbol
            AND attempt.status = 'completed'
            AND attempt.start_date <= $1
            AND attempt.end_date >= $2
      )
    ORDER BY stock.symbol
    OFFSET $3 LIMIT $4
    "#
}
