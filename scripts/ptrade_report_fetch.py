#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ptrade_report_fetch.py — 拉 QQ 邮箱的 PTrade 回报邮件(B1' 回报回流链, 16:30)

职责(单一): 连 imap.qq.com 搜索当日 ptrade_exec_* / ptrade_heartbeat_* 邮件,
exec 附件落盘 /tmp/quant_reports/, stdout 输出 JSON 摘要供 Rust 侧消费。

环境变量: PTRADE_IMAP_USER(默认 455510687@qq.com) / PTRADE_IMAP_PWD(QQ授权码,必填)
输出契约: {"exec_files": [绝对路径...], "heartbeats": [主题...], "error": null|str}
"""
import email
import imaplib
import json
import os
import sys
from datetime import date, timedelta
from email.header import decode_header

OUT_DIR = '/tmp/quant_reports'


def main():
    user = os.environ.get('PTRADE_IMAP_USER', '455510687@qq.com')
    pwd = os.environ.get('PTRADE_IMAP_PWD', '')
    if not pwd:
        print(json.dumps({'exec_files': [], 'heartbeats': [],
                          'error': 'PTRADE_IMAP_PWD 未配置'}))
        sys.exit(0)
    os.makedirs(OUT_DIR, exist_ok=True)

    result = {'exec_files': [], 'heartbeats': [], 'error': None}
    try:
        m = imaplib.IMAP4_SSL('imap.qq.com', 993)
        m.login(user, pwd)
        # 只搜今天的(周六跑不到周五邮件无妨——周一 16:30 拉不到周五回报会触发
        # 心跳缺失告警,符合"区分无交易与故障"设计; 必要时人工查邮箱)
        m.select('INBOX')
        since = (date.today() - timedelta(days=1)).strftime('%d-%b-%Y')
        _, data = m.search(None, f'(SUBJECT "ptrade_" SINCE "{since}")')
        ids = data[0].split()
        for mid in ids:
            _, msg_data = m.fetch(mid, '(RFC822)')
            raw = msg_data[0][1]
            msg = email.message_from_bytes(raw)
            subj = ''
            for part, enc in decode_header(msg.get('Subject', '')):
                subj += part.decode(enc or 'utf-8') if isinstance(part, bytes) else part
            if subj.startswith('ptrade_heartbeat_'):
                result['heartbeats'].append(subj)
                continue
            if not subj.startswith('ptrade_exec_'):
                continue
            for part in msg.walk():
                fname = part.get_filename()
                if not fname:
                    continue
                # 附件文件名解码(山西PTrade发的是文件名而非路径, send_email 文档注明)
                if isinstance(fname, str):
                    for seg, enc in decode_header(fname):
                        if isinstance(seg, bytes):
                            fname = seg.decode(enc or 'utf-8')
                            break
                if not fname.endswith('.json'):
                    continue
                path = os.path.join(OUT_DIR, os.path.basename(fname))
                with open(path, 'wb') as f:
                    f.write(part.get_payload(decode=True))
                result['exec_files'].append(path)
        m.logout()
    except Exception as e:
        result['error'] = f'{type(e).__name__}: {e}'
    print(json.dumps(result, ensure_ascii=False))


if __name__ == '__main__':
    main()
