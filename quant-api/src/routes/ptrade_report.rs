//! PTrade 实盘回报抓取（B1' 回报回流链, 2026-09-11）
//!
//! 16:30 定时任务(scheduled_task_config: ptrade_report_fetch):
//! 拉 imap.qq.com 当日 ptrade_exec_*/ptrade_heartbeat_* 邮件 →
//! exec 附件解析入库(ptrade_execution_report) → 钉钉实盘日报。
//! 心跳缺失 = 策略挂了或通道故障 → 告警(区分"无交易"与"故障")。
//!
//! 任务75(2026-09-22) 全 Rust 化: 原 Python 脚本(include_str! 落盘 spawn python3)
//! 重写为 imap 2.4 + rustls-connector + mail-parser 原生实现, 附件内存传递——
//! 正式运行链路不再依赖 Python, 亦无 /tmp 中转文件。

use sqlx::PgPool;
use tracing::error;

use crate::routes::shared::{PaperPositionRepository, PgPaperPositionRepo};

/// 定时任务入口(scheduler.rs "ptrade_report_fetch" 分支调用)。
/// 任务76(2026-09-22): 逐 enabled 通道拉各自邮箱(执行器级配置), 单通道故障
/// 不阻断其它通道; 逐通道段落合并一条钉钉日报。
pub async fn run_ptrade_report_fetch(db: &PgPool) {
    if let Err(e) = ensure_report_table(db).await {
        error!("[PTrade回报] 建表失败: {}", e);
        crate::routes::shared::send_dingtalk_alert(db, &format!("⛔ [PTrade回报] 建表失败: {}", e))
            .await;
        return;
    }
    let channels = match load_channels(db).await {
        Ok(c) => c,
        Err(e) => {
            crate::routes::shared::send_dingtalk_alert(
                db,
                &format!("⛔ [PTrade回报] 通道配置加载失败: {}", e),
            )
            .await;
            return;
        }
    };
    let mut sections: Vec<String> = Vec::new();
    for ch in &channels {
        match fetch_mail_reports(ch).await {
            Ok(summary) => sections.push(ingest_reports(db, &summary, ch).await),
            Err(e) => sections.push(format!(
                "⛔ 通道[{}] 邮件拉取失败(IMAP 通道故障?): {}",
                ch.channel_name, e
            )),
        }
    }
    crate::routes::shared::send_dingtalk_alert(db, &sections.join("\n")).await;
}

/// 执行器通道配置(ptrade_channel_config 一行 = 一个执行器通道)。
/// 邮箱配置与执行器绑定: 执行器 send_email 的 EMAIL_FROM 即本通道拉取邮箱。
struct ChannelConfig {
    account_id: String,
    channel_name: String,
    is_production: bool,
    imap_host: String,
    imap_port: i32,
    imap_user: Option<String>,
    imap_pwd: Option<String>,
}

async fn load_channels(db: &PgPool) -> Result<Vec<ChannelConfig>, String> {
    let rows = sqlx::query_as::<
        _,
        (
            String,
            String,
            bool,
            String,
            i32,
            Option<String>,
            Option<String>,
        ),
    >(
        "SELECT paper_account_id, channel_name, is_production,
                imap_host, imap_port, imap_user, imap_pwd
         FROM ptrade_channel_config WHERE enabled = true ORDER BY is_production",
    )
    .fetch_all(db)
    .await
    .map_err(|e| format!("channel config: {}", e))?;
    Ok(rows
        .into_iter()
        .map(
            |(
                account_id,
                channel_name,
                is_production,
                imap_host,
                imap_port,
                imap_user,
                imap_pwd,
            )| {
                ChannelConfig {
                    account_id,
                    channel_name,
                    is_production,
                    imap_host,
                    imap_port,
                    imap_user,
                    imap_pwd,
                }
            },
        )
        .collect())
}

#[derive(Default)]
struct FetchSummary {
    execs: Vec<ExecMail>,
    heartbeats: Vec<String>,
    error: Option<String>,
}

/// exec 邮件的 .json 附件(内存持有, 不落盘)。
struct ExecMail {
    subject: String,
    filename: String,
    bytes: Vec<u8>,
}

async fn fetch_mail_reports(ch: &ChannelConfig) -> Result<FetchSummary, String> {
    if ch.imap_user.is_none() || ch.imap_pwd.is_none() {
        // 未配置邮箱的通道(如 live 未启用邮件回报): 告警行而非任务失败
        return Err(format!(
            "通道[{}] 未配置 imap_user/imap_pwd, 跳过拉取(在 ptrade_channel_config 配置后启用)",
            ch.channel_name
        ));
    }
    let host = ch.imap_host.clone();
    let port = ch.imap_port as u16;
    let user = ch.imap_user.clone().unwrap_or_default();
    let pwd = ch.imap_pwd.clone().unwrap_or_default();
    // IMAP 会话是同步短任务(一日一次, 秒级), 阻塞实现包 spawn_blocking 即可
    tokio::task::spawn_blocking(move || fetch_mail_reports_blocking(host, port, user, pwd))
        .await
        .map_err(|e| format!("blocking join: {}", e))?
}

/// 同步 IMAP 拉取(连接参数来自通道行配置, 任务76 迁 DB): 连通道邮箱端点 →
/// 搜索昨日以来 subject 含 ptrade_ 的邮件 → heartbeat 记主题 / exec 提取
/// .json 附件。通道级故障(连接/登录/网络)返回 Err 由外层钉钉告警;
/// 单封邮件解析失败记 error 降级行继续(部分成功优于全失败)。
fn fetch_mail_reports_blocking(
    host: String,
    port: u16,
    user: String,
    pwd: String,
) -> Result<FetchSummary, String> {
    use mail_parser::MimeHeaders;

    // rustls 0.23 在 ring/aws-lc-rs 双 feature 共存的依赖树里无法自动选定
    // CryptoProvider(首次 TLS 握手 panic, 2026-09-22 实测)——进程级显式安装
    // ring(纯 Rust, 环境风险最小); install_default 幂等, 已被其它组件安装则沿用
    static INSTALL_PROVIDER: std::sync::Once = std::sync::Once::new();
    INSTALL_PROVIDER.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });

    // webpki 根证书编译期内嵌, 不依赖目标机系统 CA 装配差异
    let tls = rustls_connector::RustlsConnector::new_with_webpki_root_certs()
        .map_err(|e| format!("tls roots: {}", e))?;
    let stream = std::net::TcpStream::connect((host.as_str(), port))
        .map_err(|e| format!("tcp {}:{}: {}", host, port, e))?;
    let tls_stream = tls
        .connect(&host, stream)
        .map_err(|e| format!("tls: {}", e))?;
    let mut session = imap::Client::new(tls_stream)
        .login(&user, &pwd)
        .map_err(|e| format!("login: {}", e.0))?;
    let result = (|| -> Result<FetchSummary, String> {
        let mut summary = FetchSummary::default();
        // 只搜昨天以来的(周六跑不到周五邮件无妨——周一 16:30 拉不到周五回报会触发
        // 心跳缺失告警, 符合"区分无交易与故障"设计; 必要时人工查邮箱)
        session
            .select("INBOX")
            .map_err(|e| format!("select: {}", e))?;
        let yesterday = chrono::Local::now().date_naive() - chrono::Duration::days(1);
        let criteria = format!(
            "(SUBJECT \"ptrade_\" SINCE \"{}\")",
            imap_search_date(yesterday)
        );
        let ids = session
            .search(criteria.as_str())
            .map_err(|e| format!("search: {}", e))?;
        // HashSet 无序: 排序拼序列集一次性 FETCH, 减少往返
        let mut seq: Vec<u32> = ids.into_iter().collect();
        seq.sort_unstable();
        if seq.is_empty() {
            return Ok(summary);
        }
        let seq_set = seq
            .iter()
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let fetches = session
            .fetch(&seq_set, "RFC822")
            .map_err(|e| format!("fetch: {}", e))?;
        for f in fetches.iter() {
            let raw = match f.body() {
                Some(b) => b,
                None => continue,
            };
            let msg = match mail_parser::MessageParser::default().parse(raw) {
                Some(m) => m,
                None => {
                    summary
                        .error
                        .get_or_insert_with(|| "部分邮件解析失败".into());
                    continue;
                }
            };
            let subject = msg.subject().unwrap_or_default().to_string();
            // QQ IMAP 的 SINCE 匹配宽松(2026-09-22 实测: SINCE 21-Sep 带出 9/17
            // 旧邮件重放, 刷缺均价 ERROR 噪音且镜像有倒刷风险)——本地按 subject
            // 尾段日期二次过滤, 只留 >= 昨日; 无日期段的异常主题一并跳过
            if subject_date(&subject).is_none_or(|d| d < yesterday) {
                continue;
            }
            if subject.starts_with("ptrade_heartbeat_") {
                summary.heartbeats.push(subject);
                continue;
            }
            if !subject.starts_with("ptrade_exec_") {
                continue;
            }
            // 取第一个 .json 附件(执行器 send_email 单附件契约)
            let att = msg.attachments().find(|p| {
                p.attachment_name()
                    .is_some_and(|n| n.to_ascii_lowercase().ends_with(".json"))
            });
            match att {
                Some(part) => {
                    let filename = part.attachment_name().unwrap_or_default().to_string();
                    summary.execs.push(ExecMail {
                        subject,
                        filename,
                        bytes: part.contents().to_vec(),
                    });
                }
                None => {
                    summary.error = Some(format!("exec 邮件无 .json 附件: {}", subject));
                }
            }
        }
        Ok(summary)
    })();
    // logout best-effort: 会话即将 drop, 失败不影响结果
    let _ = session.logout();
    result
}

/// subject 尾段 8 位日期解析(`ptrade_exec_sim_20260922` → 2026-09-22)。
/// 用于 SINCE 宽松匹配后的本地二次过滤; 无日期段(旧格式/异常主题)返回 None。
fn subject_date(subject: &str) -> Option<chrono::NaiveDate> {
    let tail = subject.rsplit('_').next()?;
    chrono::NaiveDate::parse_from_str(tail, "%Y%m%d").ok()
}

/// IMAP SEARCH 的日期格式 `DD-Mon-YYYY`——英文月名手写表, 不依赖 chrono %b 的
/// locale 行为(pure-rust-locales 特性)。
fn imap_search_date(d: chrono::NaiveDate) -> String {
    use chrono::Datelike;
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    format!(
        "{:02}-{}-{}",
        d.day(),
        MONTHS[(d.month() as usize) - 1],
        d.year()
    )
}

/// exec json 入库 + 组装日报文本(通道段落)。镜像路由依据 = 拉取邮箱归属的通道行,
/// subject tag 仅作一致性校验(防执行器贴错 tag/串邮箱, 不一致告警不拒收)。
async fn ingest_reports(db: &PgPool, summary: &FetchSummary, ch: &ChannelConfig) -> String {
    let mut lines: Vec<String> = Vec::new();
    if let Some(e) = &summary.error {
        lines.push(format!("⚠️ 拉取部分异常: {}", e));
    }
    if summary.execs.is_empty() && summary.heartbeats.is_empty() {
        // 当日既无回报也无心跳: 策略未运行/邮件未发/通道故障——按设计告警
        return format!(
            "⚠️ [PTrade实盘日报·{}] 当日无 exec 回报且无心跳邮件——请检查 PTrade 策略状态与邮件通道(策略挂了? 15:00 后未发?)",
            ch.channel_name
        );
    }
    for hb in &summary.heartbeats {
        lines.push(format!("🫀 心跳: {}", hb));
    }
    for m in &summary.execs {
        match ingest_one(db, m, ch).await {
            Ok(desc) => lines.push(desc),
            Err(e) => lines.push(format!("⚠️ {} 入库失败: {}", m.filename, e)),
        }
    }
    format!(
        "📊 [PTrade实盘日报·{}]\n{}",
        ch.channel_name,
        lines.join("\n")
    )
}

/// 从邮件主题提取通道tag: ptrade_exec_{sim|live}_{date} → "sim"/"live"。
fn channel_tag(subject: &str) -> Option<&str> {
    let rest = subject.strip_prefix("ptrade_exec_")?;
    match rest.split('_').next() {
        Some(t @ ("sim" | "live")) => Some(t),
        _ => None, // 旧格式(无tag)或未知
    }
}

/// 镜像账户回写: 同步 nav/cash/持仓到通道行指定的镜像账户(任务76: 调用方
/// 传入通道配置, 不再按 tag 反查)。首次回写且账户为空仓时顺带校准
/// initial_capital(以 PTrade 真实值为基准)。
async fn sync_mirror_account(
    db: &PgPool,
    ch: &ChannelConfig,
    v: &serde_json::Value,
) -> Result<Option<String>, String> {
    let account_id = ch.account_id.as_str();
    let nav = v.get("nav_after").and_then(|x| x.as_f64());
    let cash = v.get("cash_after").and_then(|x| x.as_f64());
    let pos_arr = v.get("positions").and_then(|x| x.as_array());
    let trade_date = v
        .get("trade_date")
        .and_then(|x| x.as_str())
        .and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y%m%d").ok());
    // PTrade .SS → quant .SH(quant 体系统一 .SH/.SZ, bar 表/估值组件依赖此格式)
    let norm_sym = |s: &str| -> String {
        if let Some(c) = s.strip_suffix(".SS") {
            format!("{}.SH", c)
        } else {
            s.to_string()
        }
    };
    let dec = |x: f64| rust_decimal::Decimal::from_f64_retain(x).unwrap_or_default();

    // 有效持仓数(回报 positions 含当日清仓的 amount=0 零头行, 不能用数组 is_empty 判断)
    let n_valid_pos = pos_arr
        .map(|a| {
            a.iter()
                .filter(|p| p.get("amount").and_then(|x| x.as_f64()).unwrap_or(0.0) > 0.0)
                .count()
        })
        .unwrap_or(0);
    let empty_before: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM paper_position WHERE paper_account_id=$1 AND quantity>0",
    )
    .bind(account_id)
    .fetch_one(db)
    .await
    .map_err(|e| format!("mirror count: {}", e))?;
    sqlx::query(
        "UPDATE paper_account SET current_nav=$2, cash=$3, updated_at=now(),
                 initial_capital = CASE WHEN $4 AND $5 THEN $2 ELSE initial_capital END,
                 total_trades = total_trades + $6
                 WHERE paper_account_id=$1",
    )
    .bind(account_id)
    .bind(nav.map(dec))
    .bind(cash.map(dec))
    // 首次回写校准: 账户空仓且回报也无有效持仓(以 PTrade 真实值为基准)
    .bind(empty_before.0 == 0 && n_valid_pos == 0)
    .bind(nav.is_some())
    .bind(
        v.get("orders")
            .and_then(|x| x.as_array())
            .map(|a| a.len() as i32)
            .unwrap_or(0),
    )
    .execute(db)
    .await
    .map_err(|e| format!("mirror nav: {}", e))?;

    // ── 持仓重建(幂等: DELETE+INSERT; symbol 规范化 .SS→.SH) ──
    let mut n_pos = 0i32;
    if let Some(positions) = pos_arr {
        PgPaperPositionRepo::new(db)
            .delete_all_positions(account_id)
            .await
            .map_err(|e| format!("mirror del: {}", e))?;
        for p in positions {
            let sym = p.get("symbol").and_then(|x| x.as_str()).unwrap_or("");
            let qty = p.get("amount").and_then(|x| x.as_f64()).unwrap_or(0.0);
            let px = p
                .get("last_sale_price")
                .and_then(|x| x.as_f64())
                .unwrap_or(0.0);
            if sym.is_empty() || qty <= 0.0 {
                continue;
            }
            // avg_cost 用现价近似(回报无成本字段); 绩效口径以 NAV 为准
            PgPaperPositionRepo::new(db)
                .insert_mirror_position(
                    &format!("pp-{}", uuid::Uuid::new_v4()),
                    account_id,
                    &norm_sym(sym),
                    dec(qty),
                    dec(px),
                    dec(qty * px),
                )
                .await
                .map_err(|e| format!("mirror pos {}: {}", sym, e))?;
            n_pos += 1;
        }
    }

    // ── 交易明细回写(2026-09-17 v2: orders→paper_fill, 镜像账户与 PTrade 全量一致) ──
    // 成交价优先级: 执行器 avg_price(新版) > limit_price > 当日收盘价兜底。
    let mut missing_px = 0i32;
    // (2026-09-18: 原元组解构绑定 td 仅作存在性门禁, 成交价改宁缺毋假后无实际
    // 依赖——fill_time 用回报处理时刻, 明细回写不需要交易日。)
    if let Some(orders) = v.get("orders").and_then(|x| x.as_array()) {
        for o in orders {
            let sym = o.get("symbol").and_then(|x| x.as_str()).unwrap_or("");
            let filled = o.get("filled").and_then(|x| x.as_f64()).unwrap_or(0.0);
            let status = o.get("status").and_then(|x| x.as_str()).unwrap_or("");
            if sym.is_empty() || filled.abs() < 1.0 || status != "8" {
                continue; // 只回写已全部成交的委托(状态8)
            }
            let entrust = o.get("entrust_no").and_then(|x| x.as_str()).unwrap_or("");
            // 成交价唯一真源 = 执行器回报 avg_price(PTrade Order 真实成交均价)。
            // 缺价即拒绝写明细并告警——宁缺毋假: limit_price 是委托限价、bar close 是
            // 收盘价, 都不是成交价, 近似冒充会造成对账永差与绩效失真
            // (2026-09-17 曾用前日收盘冒充被用户纠正)。
            let px = o.get("avg_price").and_then(|x| x.as_f64()).unwrap_or(0.0);
            if px <= 0.0 {
                missing_px += 1;
                tracing::error!(
                    "[mirror] {} {} 回报缺成交均价(avg_price), 跳过该笔明细——执行器需升级(orders 带 avg_price)",
                    sym, entrust
                );
                continue;
            }
            let qty = dec(filled.abs());
            let amt = dec(filled.abs() * px);
            // FK 约束(fk_paper_fill_order): fill 须先有对应 order 行
            sqlx::query(
                "INSERT INTO paper_order (order_id, paper_account_id, symbol, side, order_type,
                   quantity, limit_price, status, created_at)
                 VALUES ($1,$2,$3,$4,'market',$5,$6,'filled',now())
                 ON CONFLICT (order_id) DO NOTHING",
            )
            .bind(format!("po-{}", entrust))
            .bind(account_id)
            .bind(norm_sym(sym))
            .bind(if filled > 0.0 { "buy" } else { "sell" })
            .bind(qty)
            .bind(o.get("limit_price").and_then(|x| x.as_f64()).map(dec))
            .execute(db)
            .await
            .map_err(|e| format!("mirror order {}: {}", sym, e))?;
            sqlx::query(
                "INSERT INTO paper_fill (fill_id, order_id, paper_account_id, symbol, fill_time,
                   side, quantity, price, amount, commission, tax, slippage)
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,0,0,0)
                 ON CONFLICT (fill_id, fill_time) DO NOTHING", // 分区表唯一键须含 fill_time
            )
            .bind(format!(
                "pf-{}-{}",
                v.get("signal_id").and_then(|x| x.as_str()).unwrap_or(""),
                entrust
            ))
            .bind(format!("po-{}", entrust))
            .bind(account_id)
            .bind(norm_sym(sym))
            .bind(chrono::Utc::now()) // 回报处理时刻(成交时点回报未提供, 用入库时间)
            .bind(if filled > 0.0 { "buy" } else { "sell" })
            .bind(qty)
            .bind(dec(px))
            .bind(amt)
            .execute(db)
            .await
            .map_err(|e| format!("mirror fill {}: {}", sym, e))?;
        }
    }

    if missing_px > 0 {
        tracing::error!(
            "[mirror] {} 当日回报 {} 笔缺成交均价, 明细未写入(持仓/NAV 不受影响)——升级执行器后次日自愈",
            account_id, missing_px
        );
    }

    // ── NAV 快照回写(v2: nav-history 曲线/日报依赖, 与模拟盘同表) ──
    if let (Some(td), Some(nav_v)) = (trade_date, nav) {
        let prev_nav: Option<rust_decimal::Decimal> = sqlx::query_scalar(
            "SELECT nav FROM paper_nav_snapshot WHERE paper_account_id=$1 AND snapshot_date < $2
             ORDER BY snapshot_date DESC LIMIT 1",
        )
        .bind(account_id)
        .bind(td)
        .fetch_optional(db)
        .await
        .map_err(|e| format!("mirror prev nav: {}", e))?
        .flatten();
        let daily_ret = prev_nav
            .and_then(|p| p.to_string().parse::<f64>().ok().filter(|p| *p > 0.0))
            .map(|p| nav_v / p - 1.0);
        let mv = positions_value(v);
        sqlx::query(
            "INSERT INTO paper_nav_snapshot (nav_snapshot_id, paper_account_id, snapshot_date,
               nav, cash, market_value, position_count, daily_return)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
             ON CONFLICT (paper_account_id, snapshot_date) DO UPDATE SET
               nav = EXCLUDED.nav, cash = EXCLUDED.cash,
               market_value = EXCLUDED.market_value, position_count = EXCLUDED.position_count,
               daily_return = EXCLUDED.daily_return",
        )
        .bind(format!("pns-{}-{}", account_id, td.format("%Y%m%d")))
        .bind(account_id)
        .bind(td)
        .bind(dec(nav_v))
        .bind(cash.map(dec))
        .bind(dec(mv))
        .bind(n_pos)
        .bind(daily_ret.map(dec))
        .execute(db)
        .await
        .map_err(|e| format!("mirror snapshot: {}", e))?;
    }
    Ok(Some(account_id.to_string()))
}

/// 回报 positions_value 字段(持仓市值合计), 缺失时由 positions 求和兜底。
fn positions_value(v: &serde_json::Value) -> f64 {
    if let Some(mv) = v.get("positions_value").and_then(|x| x.as_f64()) {
        return mv;
    }
    v.get("positions")
        .and_then(|x| x.as_array())
        .map(|a| {
            a.iter()
                .map(|p| {
                    p.get("market_value")
                        .and_then(|x| x.as_f64())
                        .unwrap_or(0.0)
                })
                .sum()
        })
        .unwrap_or(0.0)
}

async fn ingest_one(db: &PgPool, mail: &ExecMail, ch: &ChannelConfig) -> Result<String, String> {
    let v: serde_json::Value =
        serde_json::from_slice(&mail.bytes).map_err(|e| format!("json: {}", e))?;
    let signal_id = v
        .get("signal_id")
        .and_then(|x| x.as_str())
        .unwrap_or("unknown")
        .to_string();
    let trade_date = v
        .get("trade_date")
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string();
    let num = |k: &str| {
        v.get(k)
            .and_then(|x| x.as_f64())
            .and_then(rust_decimal::Decimal::from_f64_retain)
    };
    let orders = v.get("orders").cloned().unwrap_or(serde_json::json!([]));
    let positions = v.get("positions").cloned().unwrap_or(serde_json::json!([]));
    let unfilled = v.get("unfilled").cloned().unwrap_or(serde_json::json!([]));
    let n_orders = orders.as_array().map(|a| a.len()).unwrap_or(0);
    let n_unfilled = unfilled.as_array().map(|a| a.len()).unwrap_or(0);

    sqlx::query(
        "INSERT INTO ptrade_execution_report
           (signal_id, trade_date, nav_after, cash_after, turnover_today,
            orders, positions, unfilled, raw)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
         ON CONFLICT (signal_id) DO UPDATE SET
           nav_after=EXCLUDED.nav_after, cash_after=EXCLUDED.cash_after,
           turnover_today=EXCLUDED.turnover_today, orders=EXCLUDED.orders,
           positions=EXCLUDED.positions, unfilled=EXCLUDED.unfilled, raw=EXCLUDED.raw",
    )
    .bind(&signal_id)
    .bind(&trade_date)
    .bind(num("nav_after"))
    .bind(num("cash_after"))
    .bind(num("turnover_today"))
    .bind(&orders)
    .bind(&positions)
    .bind(&unfilled)
    .bind(&v)
    .execute(db)
    .await
    .map_err(|e| format!("db: {}", e))?;

    // 镜像路由依据 = 拉取邮箱归属的通道; subject tag 仅一致性校验
    // (执行器贴错 tag/串邮箱时告警, 不拒收——入库以邮箱归属为准)
    if let Some(tag) = channel_tag(&mail.subject) {
        let expect_prod = tag == "live";
        if expect_prod != ch.is_production {
            error!(
                "[PTrade回报] 通道[{}] 拉到 tag={} 的邮件(与通道 production={} 不符, 疑贴错 tag/串邮箱)",
                ch.channel_name, tag, ch.is_production
            );
        }
    }
    let mirror = sync_mirror_account(db, ch, &v).await?;
    Ok(format!(
        "📈 {} ({}){}: 订单 {} 笔, 未成交 {}, NAV {:.0}, 换手 {:.0}",
        signal_id,
        trade_date,
        mirror.map(|a| format!(" →已同步{}", a)).unwrap_or_default(),
        n_orders,
        n_unfilled,
        v.get("nav_after").and_then(|x| x.as_f64()).unwrap_or(0.0),
        v.get("turnover_today")
            .and_then(|x| x.as_f64())
            .unwrap_or(0.0),
    ))
}

async fn ensure_report_table(db: &PgPool) -> Result<(), String> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS ptrade_execution_report (
           id BIGSERIAL PRIMARY KEY,
           signal_id TEXT NOT NULL UNIQUE,
           trade_date TEXT NOT NULL,
           received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
           nav_after NUMERIC,
           cash_after NUMERIC,
           turnover_today NUMERIC,
           orders JSONB NOT NULL DEFAULT '[]',
           positions JSONB NOT NULL DEFAULT '[]',
           unfilled JSONB NOT NULL DEFAULT '[]',
           raw JSONB NOT NULL
         )",
    )
    .execute(db)
    .await
    .map_err(|e| e.to_string())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn imap_search_date_english_month_names() {
        let fmt = |y, m, d| {
            chrono::NaiveDate::from_ymd_opt(y, m, d)
                .map(imap_search_date)
                .unwrap()
        };
        assert_eq!(fmt(2026, 9, 21), "21-Sep-2026");
        assert_eq!(fmt(2026, 1, 1), "01-Jan-2026");
        assert_eq!(fmt(2026, 12, 31), "31-Dec-2026");
        // 跨年边界: 昨天=12-31 时 SINCE 串仍须正确
        assert_eq!(fmt(2025, 12, 31), "31-Dec-2025");
    }

    #[test]
    fn subject_date_parses_tail_segment() {
        assert_eq!(
            subject_date("ptrade_exec_sim_20260922"),
            chrono::NaiveDate::from_ymd_opt(2026, 9, 22)
        );
        // 旧格式(无 tag)同样可解析
        assert_eq!(
            subject_date("ptrade_exec_20260917"),
            chrono::NaiveDate::from_ymd_opt(2026, 9, 17)
        );
        // 无日期段/垃圾尾段: None(过滤时跳过)
        assert_eq!(subject_date("ptrade_heartbeat_sim"), None);
        assert_eq!(subject_date(""), None);
    }

    #[test]
    fn channel_tag_routes_sim_and_live() {
        assert_eq!(channel_tag("ptrade_exec_sim_20260922"), Some("sim"));
        assert_eq!(channel_tag("ptrade_exec_live_20260922"), Some("live"));
        // 旧格式(无 tag)与无关主题: 不路由
        assert_eq!(channel_tag("ptrade_exec_20260917"), None);
        assert_eq!(channel_tag("re: ptrade_exec_sim_x"), None);
        assert_eq!(channel_tag(""), None);
    }

    #[test]
    fn exec_mail_carries_attachment_in_memory() {
        let m = ExecMail {
            subject: "ptrade_exec_sim_20260922".into(),
            filename: "exec_20260922.json".into(),
            bytes: br#"{"signal_id":"v24_20260922_001"}"#.to_vec(),
        };
        let v: serde_json::Value = serde_json::from_slice(&m.bytes).unwrap();
        assert_eq!(v["signal_id"], "v24_20260922_001");
        assert!(m.filename.to_ascii_lowercase().ends_with(".json"));
    }
}
