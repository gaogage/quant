#!/usr/bin/env python3
"""report_rc 全历史回填（方向C broad-base analyst revision 源）。

用法（quant/.venv）：
  .venv/bin/python3 scripts/backfill_report_rc.py --start 20100101 --end 20260903
  .venv/bin/python3 scripts/backfill_report_rc.py --resume          # 跳过已有月份

特性：
- 按月分块 + offset 翻页（单次 5000，has_more 继续）
- 1 次/分钟限流（API 侧硬限，脚本 sleep 61s）
- 幂等：raw_payload_hash 主键，重复行 DO NOTHING
- 断点可恢复：逐月提交，月度进度打印到 stderr
"""
import argparse
import hashlib
import json
import sys
import time
from datetime import date, timedelta

import psycopg2
import urllib.request

API = "http://api.tushare.pro"
TOKEN_ENV_FALLBACK = None  # token 由 --token 或环境变量提供

FIELDS = "ts_code,name,report_date,report_title,report_type,classify,org_name,author_name,quarter,op_rt,op_pr,tp,np,eps,pe,rd,roe,ev_ebitda,rating,max_price,min_price"

NUM_COLS = {"tp", "np", "eps", "pe", "rd", "roe", "ev_ebitda", "max_price", "min_price"}
TEXT_COLS = {"name", "report_title", "report_type", "classify", "org_name", "author_name", "quarter", "op_rt", "op_pr", "rating"}


def month_ranges(start: str, end: str):
    y, m = int(start[:4]), int(start[4:6])
    ey, em = int(end[:4]), int(end[4:6])
    while (y, m) <= (ey, em):
        ny, nm = (y + 1, 1) if m == 12 else (y, m + 1)
        yield f"{y:04d}{m:02d}01", f"{ny:04d}{nm:02d}01"
        y, m = ny, nm


def call_api(token: str, start: str, end: str, offset: int):
    body = json.dumps({
        "api_name": "report_rc",
        "token": token,
        "params": {"start_date": start, "end_date": end, "limit": 5000, "offset": offset},
        "fields": FIELDS,
    }).encode()
    req = urllib.request.Request(API, data=body, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())


def to_row(fields, item):
    rec = dict(zip(fields, item))
    payload = json.dumps(rec, ensure_ascii=False, sort_keys=True)
    h = hashlib.sha256(payload.encode()).hexdigest()
    row = {
        "ts_code": rec.get("ts_code"),
        "report_date": rec.get("report_date"),
        "rating": rec.get("rating"),
        "org_name": rec.get("org_name"),
        "available_at": rec.get("report_date"),
        "raw_payload": payload,
        "raw_payload_hash": h,
    }
    for c in NUM_COLS:
        v = rec.get(c)
        row[c] = float(v) if v not in (None, "", "None") else None
    for c in TEXT_COLS:
        v = rec.get(c)
        row[c] = str(v) if v is not None else None
    return row


INSERT_SQL = """
INSERT INTO market_stock_analyst_report_rc_raw
  (ts_code, report_date, name, report_title, report_type, classify, org_name, author_name,
   quarter, op_rt, op_pr, tp, np, eps, pe, rd, roe, ev_ebitda, rating, max_price, min_price,
   available_at, raw_payload, raw_payload_hash, source)
VALUES (%(ts_code)s, %(report_date)s, %(name)s, %(report_title)s, %(report_type)s, %(classify)s,
  %(org_name)s, %(author_name)s, %(quarter)s, %(op_rt)s, %(op_pr)s, %(tp)s, %(np)s, %(eps)s,
  %(pe)s, %(rd)s, %(roe)s, %(ev_ebitda)s, %(rating)s, %(max_price)s, %(min_price)s,
  %(available_at)s, %(raw_payload)s, %(raw_payload_hash)s, 'tushare')
ON CONFLICT (raw_payload_hash) DO NOTHING
"""


def sync_month(conn, token, start, end):
    total, offset = 0, 0
    while True:
        resp = None
        for attempt in range(8):
            resp = call_api(token, start, end, offset)
            if resp.get("code") == 0:
                break
            msg = resp.get("msg", "")
            if "频率超限" in msg:
                print(f"[rate] {start}-{end} off={offset} 第{attempt+1}次限流, sleep 65", file=sys.stderr, flush=True)
                time.sleep(65)
                continue
            raise RuntimeError(f"API error code={resp.get('code')}: {msg}")
        if resp.get("code") != 0:
            raise RuntimeError(f"限流重试 8 次仍失败: {start}-{end} off={offset}")
        data = resp["data"]
        rows = [to_row(data["fields"], it) for it in data["items"]]
        with conn.cursor() as cur:
            cur.executemany(INSERT_SQL, rows)
        conn.commit()
        total += len(rows)
        offset += len(rows)
        print(f"[page] {start[:6]} off={offset} got={len(rows)}", file=sys.stderr, flush=True)
        if len(rows) < 5000:
            return total
        time.sleep(2)  # 8000积分档限流放宽，翻页2s足够


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="20100101")
    ap.add_argument("--end", default=time.strftime("%Y%m%d"))
    ap.add_argument("--resume", action="store_true", help="跳过 DB 已有数据的月份")
    ap.add_argument("--token", default=None)
    args = ap.parse_args()

    import os
    token = args.token or os.environ.get("TUSHARE_TOKEN_ALT") or os.environ.get("TUSHARE_TOKEN")
    if not token:
        sys.exit("需要 --token 或 TUSHARE_TOKEN_ALT")

    conn = psycopg2.connect(os.environ.get("DATABASE_URL", "postgres://gaocheng@localhost/quant"))

    for ms, me in month_ranges(args.start, args.end):
        m = ms[:6]
        if args.resume:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM market_stock_analyst_report_rc_raw WHERE report_date >= %s AND report_date < %s", (ms, me))
                if cur.fetchone()[0] > 0:
                    print(f"[skip] {m} 已有数据", file=sys.stderr)
                    continue
        try:
            n = sync_month(conn, token, ms, me)
            print(f"[done] {m}: {n} rows ({time.strftime('%H:%M:%S')})", file=sys.stderr, flush=True)
        except Exception as e:
            conn.rollback()
            print(f"[FAIL] {m}: {e} —— 跳过续跑", file=sys.stderr, flush=True)


if __name__ == "__main__":
    main()
