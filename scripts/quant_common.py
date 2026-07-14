#!/usr/bin/env python3
"""
quant_common — 量化分析公共函数库

共享函数：数据库连接、绩效指标计算、CSV 读取、格式化输出。
所有 quant_* 脚本通过 `from quant_common import ...` 引入。

运行环境：quant/ 根目录下的 .venv（uv 管理，Python 3.14+）
"""

import math
import os
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# 路径与依赖
# ---------------------------------------------------------------------------

# 确保同目录下的 quant_common 可以被其他脚本 import
_THIS_DIR = Path(__file__).resolve().parent
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))

try:
    import psycopg2  # type: ignore
except ImportError as e:
    raise SystemExit(
        "错误：缺少 psycopg2，请先在 quant/.venv 中安装：\n"
        "  cd /Users/gaocheng/workspace/quant && uv pip install psycopg2-binary"
    ) from e

try:
    import numpy as np
except ImportError as e:
    raise SystemExit(
        "错误：缺少 numpy，请先在 quant/.venv 中安装：\n"
        "  cd /Users/gaocheng/workspace/quant && uv pip install numpy"
    ) from e


# ---------------------------------------------------------------------------
# 默认配置
# ---------------------------------------------------------------------------

# PostgreSQL 连接 URL（可从环境变量覆盖）
DEFAULT_DB_URL = os.environ.get("QUANT_DB_URL", "postgres://gaocheng@localhost/quant")

# 年化交易日数
ANNUAL_TRADING_DAYS = 252

# 无风险利率（年化）
DEFAULT_RISK_FREE_RATE = 0.02


# ---------------------------------------------------------------------------
# 数据库连接
# ---------------------------------------------------------------------------

def db_connect(db_url: str = None):
    """创建 PostgreSQL 连接。

    Args:
        db_url: PostgreSQL 连接字符串，默认读取环境变量 QUANT_DB_URL 或使用
                postgres://gaocheng@localhost/quant。传 None 时使用默认值。

    Returns:
        psycopg2 连接对象
    """
    return psycopg2.connect(db_url or DEFAULT_DB_URL)


def db_query(sql: str, params=None, db_url: str = DEFAULT_DB_URL):
    """执行 SQL 查询并返回结果（cursor.description 元组形式）。

    Args:
        sql: SELECT 查询语句
        params: 查询参数（可选）
        db_url: 数据库连接 URL

    Returns:
        (columns, rows) 元组：
        - columns: 列名列表
        - rows: 行数据列表（每行为 tuple）
    """
    conn = db_connect(db_url)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            columns = [desc[0] for desc in cur.description] if cur.description else []
            rows = cur.fetchall()
        return columns, rows
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 绩效指标计算
# ---------------------------------------------------------------------------

def compute_sharpe(
    returns,
    annualize: int = ANNUAL_TRADING_DAYS,
    risk_free: float = DEFAULT_RISK_FREE_RATE,
):
    """计算年化 Sharpe 比率。

    Sharpe = (mean(returns) - rf/annualize) / std(returns) * sqrt(annualize)

    Args:
        returns: 日收益率序列（numpy array 或 list）
        annualize: 年化交易日数（默认 252）
        risk_free: 年化无风险利率（默认 2%）

    Returns:
        Sharpe 比率（float）；样本不足时返回 0.0
    """
    arr = np.asarray(returns, dtype=float)
    arr = arr[~np.isnan(arr)]
    if len(arr) < 2:
        return 0.0
    daily_rf = risk_free / annualize
    excess = arr - daily_rf
    mean_ex = float(np.mean(excess))
    std_ex = float(np.std(excess, ddof=1))
    if std_ex == 0:
        return 0.0
    return mean_ex / std_ex * math.sqrt(annualize)


def compute_sortino(
    returns,
    annualize: int = ANNUAL_TRADING_DAYS,
    risk_free: float = DEFAULT_RISK_FREE_RATE,
):
    """计算年化 Sortino 比率（只考虑下行波动）。

    Sortino = (mean(returns) - rf/annualize) / downside_std * sqrt(annualize)

    Args:
        returns: 日收益率序列
        annualize: 年化交易日数
        risk_free: 年化无风险利率

    Returns:
        Sortino 比率（float）；无下行波动时返回 0.0
    """
    arr = np.asarray(returns, dtype=float)
    arr = arr[~np.isnan(arr)]
    if len(arr) < 2:
        return 0.0
    daily_rf = risk_free / annualize
    excess = arr - daily_rf
    mean_ex = float(np.mean(excess))
    downside = excess[excess < 0]
    if len(downside) == 0:
        return float("inf")  # 无下行波动，Sortino 为正无穷
    downside_std = float(np.std(downside, ddof=1))
    if downside_std == 0:
        return 0.0
    return mean_ex / downside_std * math.sqrt(annualize)


def compute_maxdd(portfolio_values):
    """计算最大回撤（MaxDD）。

    MaxDD = min((peak - trough) / peak) 对所有 (peak, trough) 组合

    Args:
        portfolio_values: 净值序列（按时间顺序）

    Returns:
        最大回撤（float，负值）；例如 -0.25 表示 -25%
    """
    arr = np.asarray(portfolio_values, dtype=float)
    arr = arr[~np.isnan(arr)]
    if len(arr) < 2:
        return 0.0
    # 累积最大值
    peak = np.maximum.accumulate(arr)
    # 回撤序列
    dd = (arr - peak) / peak
    return float(np.min(dd))


def compute_calmar(annualized_return: float, maxdd: float):
    """计算 Calmar 比率。

    Calmar = 年化收益率 / |最大回撤|

    Args:
        annualized_return: 年化收益率（如 0.15 表示 15%）
        maxdd: 最大回撤（如 -0.25 表示 -25%）

    Returns:
        Calmar 比率（float）；回撤为 0 时返回 0.0
    """
    if maxdd == 0:
        return 0.0
    return annualized_return / abs(maxdd)


def compute_annualized_return(
    portfolio_values,
    annualize: int = ANNUAL_TRADING_DAYS,
):
    """根据净值序列计算年化收益率。

    年化收益率 = (最终净值 / 起始净值) ^ (annualize / 天数) - 1

    Args:
        portfolio_values: 净值序列（按时间顺序）
        annualize: 年化交易日数

    Returns:
        年化收益率（float）
    """
    arr = np.asarray(portfolio_values, dtype=float)
    arr = arr[~np.isnan(arr)]
    if len(arr) < 2:
        return 0.0
    start = arr[0]
    end = arr[-1]
    if start <= 0:
        return 0.0
    n_days = len(arr) - 1
    if n_days == 0:
        return 0.0
    return (end / start) ** (annualize / n_days) - 1.0


def compute_annualized_vol(returns, annualize: int = ANNUAL_TRADING_DAYS):
    """计算年化波动率。

    年化波动率 = std(returns) * sqrt(annualize)

    Args:
        returns: 日收益率序列
        annualize: 年化交易日数

    Returns:
        年化波动率（float）
    """
    arr = np.asarray(returns, dtype=float)
    arr = arr[~np.isnan(arr)]
    if len(arr) < 2:
        return 0.0
    return float(np.std(arr, ddof=1)) * math.sqrt(annualize)


def compute_full_metrics(portfolio_values, returns, annualize: int = ANNUAL_TRADING_DAYS):
    """计算完整绩效指标集。

    Args:
        portfolio_values: 净值序列
        returns: 日收益率序列
        annualize: 年化交易日数

    Returns:
        dict: 包含以下键
        - n_days: 样本天数
        - ann_return: 年化收益率
        - ann_vol: 年化波动率
        - sharpe: Sharpe 比率
        - sortino: Sortino 比率
        - maxdd: 最大回撤（负值）
        - calmar: Calmar 比率
    """
    ann_ret = compute_annualized_return(portfolio_values, annualize)
    ann_vol = compute_annualized_vol(returns, annualize)
    sharpe = compute_sharpe(returns, annualize)
    sortino = compute_sortino(returns, annualize)
    maxdd = compute_maxdd(portfolio_values)
    calmar = compute_calmar(ann_ret, maxdd)

    return {
        "n_days": int(len(np.asarray(returns))),
        "ann_return": ann_ret,
        "ann_vol": ann_vol,
        "sharpe": sharpe,
        "sortino": sortino,
        "maxdd": maxdd,
        "calmar": calmar,
    }


# ---------------------------------------------------------------------------
# CSV 读取
# ---------------------------------------------------------------------------

def read_csv_returns(path: str, return_col: str = "daily_return", date_col: str = "trade_date"):
    """读取 CSV 文件中的日收益率序列。

    Args:
        path: CSV 文件路径
        return_col: 收益率列名（默认 daily_return，兼容 strategy_return）
        date_col: 日期列名（默认 trade_date，兼容 snapshot_date）

    Returns:
        (dates, returns) 元组：
        - dates: 日期列表（字符串）
        - returns: numpy array（日收益率，已过滤 NaN）
    """
    import csv

    dates = []
    rets = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        # 兼容不同列名
        ret_key = return_col if return_col in reader.fieldnames else (
            "strategy_return" if "strategy_return" in reader.fieldnames else return_col
        )
        date_key = date_col if date_col in reader.fieldnames else (
            "snapshot_date" if "snapshot_date" in reader.fieldnames else date_col
        )
        for row in reader:
            try:
                v = float(row[ret_key])
                if not math.isnan(v):
                    dates.append(row.get(date_key, ""))
                    rets.append(v)
            except (ValueError, KeyError):
                continue
    return dates, np.array(rets, dtype=float)


def read_csv_nav(path: str, nav_col: str = "portfolio_value"):
    """读取 CSV 文件中的净值序列。

    Args:
        path: CSV 文件路径
        nav_col: 净值列名（默认 portfolio_value）

    Returns:
        (dates, nav_values) 元组
    """
    import csv

    dates = []
    navs = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        nav_key = nav_col if nav_col in reader.fieldnames else "nav"
        date_key = "trade_date" if "trade_date" in reader.fieldnames else "snapshot_date"
        for row in reader:
            try:
                v = float(row[nav_key])
                if not math.isnan(v):
                    dates.append(row.get(date_key, ""))
                    navs.append(v)
            except (ValueError, KeyError):
                continue
    return dates, np.array(navs, dtype=float)


# ---------------------------------------------------------------------------
# 格式化输出
# ---------------------------------------------------------------------------

def format_pct(value: float, digits: int = 2) -> str:
    """格式化百分比。

    >>> format_pct(0.1234)
    '12.34%'
    """
    return f"{value * 100:.{digits}f}%"


def format_ci(lo: float, hi: float, digits: int = 4) -> str:
    """格式化置信区间。

    >>> format_ci(0.3349, 1.8904)
    '[0.3349, 1.8904]'
    """
    return f"[{lo:.{digits}f}, {hi:.{digits}f}]"


def format_metrics_table(metrics: dict, title: str = "") -> str:
    """格式化绩效指标为可读的 Markdown 表格。

    Args:
        metrics: compute_full_metrics() 返回的字典
        title: 可选标题

    Returns:
        Markdown 表格字符串
    """
    lines = []
    if title:
        lines.append(f"### {title}")
        lines.append("")
    lines.append("| 指标 | 值 |")
    lines.append("|------|-----|")
    lines.append(f"| 样本天数 | {metrics['n_days']} |")
    lines.append(f"| 年化收益率 | {format_pct(metrics['ann_return'])} |")
    lines.append(f"| 年化波动率 | {format_pct(metrics['ann_vol'])} |")
    lines.append(f"| Sharpe | {metrics['sharpe']:.4f} |")
    lines.append(f"| Sortino | {metrics['sortino']:.4f} |")
    lines.append(f"| 最大回撤 | {format_pct(metrics['maxdd'])} |")
    lines.append(f"| Calmar | {metrics['calmar']:.4f} |")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 蓝图目标判定
# ---------------------------------------------------------------------------

# 蓝图目标参照表
BLUEPRINT_TARGETS = {
    "sharpe": {"threshold": 1.0, "operator": ">=", "desc": "组合层 Sharpe（2.5x 杠杆）"},
    "ann_return": {"threshold": 0.15, "operator": ">=", "desc": "组合层年化收益率"},
    "maxdd": {"threshold": -0.35, "operator": ">", "desc": "组合层最大回撤"},
    "calmar": {"threshold": 1.5, "operator": ">=", "desc": "Calmar 比率"},
    "sharpe_ci_lower": {"threshold": 1.0, "operator": ">=", "desc": "Bootstrap Sharpe CI 下界"},
    "positive_ci_lower": {"threshold": 0.0, "operator": ">", "desc": "正收益显著性（CI 下界 > 0）"},
}


def judge_metric(metric_name: str, value: float, targets: dict = None) -> dict:
    """判断指标是否达到蓝图目标。

    Args:
        metric_name: 指标名（对应 BLUEPRINT_TARGETS 键）
        value: 指标值
        targets: 可选自定义目标（覆盖默认）

    Returns:
        dict: {passed, metric_name, value, threshold, operator, desc}
    """
    t = (targets or BLUEPRINT_TARGETS).get(metric_name)
    if t is None:
        return {"passed": None, "metric_name": metric_name, "value": value, "error": "unknown metric"}

    threshold = t["threshold"]
    op = t["operator"]
    if op == ">=":
        passed = value >= threshold
    elif op == ">":
        passed = value > threshold
    elif op == "<":
        passed = value < threshold
    elif op == "<=":
        passed = value <= threshold
    else:
        passed = None

    return {
        "passed": passed,
        "metric_name": metric_name,
        "value": value,
        "threshold": threshold,
        "operator": op,
        "desc": t["desc"],
    }


# ---------------------------------------------------------------------------
# 便捷函数：Bootstrap 重采样核心
# ---------------------------------------------------------------------------

def bootstrap_metric(
    returns,
    metric_fn,
    n_bootstrap: int = 2000,
    seed: int = 20260713,
    confidence: float = 95,
):
    """Bootstrap 重采样计算指标置信区间。

    Args:
        returns: 日收益率序列（numpy array）
        metric_fn: 计算指标的函数，签名 metric_fn(returns) -> float
        n_bootstrap: 重采样次数
        seed: 随机种子
        confidence: 置信度（0-100，默认 95）

    Returns:
        dict: {
            raw_value: float,
            ci_lower: float,
            ci_upper: float,
            ci_percentiles: dict,
            median: float,
            n_samples: int,
            n_bootstrap: int,
        }
    """
    arr = np.asarray(returns, dtype=float)
    arr = arr[~np.isnan(arr)]
    n = len(arr)
    if n < 2:
        return {"error": "样本不足", "n_samples": n}

    raw_value = float(metric_fn(arr))

    rng = np.random.default_rng(seed)
    boot_values = np.empty(n_bootstrap, dtype=float)
    for i in range(n_bootstrap):
        sample = rng.choice(arr, size=n, replace=True)
        boot_values[i] = metric_fn(sample)

    boot_values.sort()
    alpha = (100 - confidence) / 2
    lo_idx = int(alpha / 100 * n_bootstrap)
    hi_idx = int((100 - alpha) / 100 * n_bootstrap) - 1

    return {
        "raw_value": raw_value,
        "ci_lower": float(boot_values[lo_idx]),
        "ci_upper": float(boot_values[hi_idx]),
        "ci_percentiles": {
            "p5": float(boot_values[int(0.05 * n_bootstrap)]),
            "p25": float(boot_values[int(0.25 * n_bootstrap)]),
            "p50": float(boot_values[int(0.50 * n_bootstrap)]),
            "p75": float(boot_values[int(0.75 * n_bootstrap)]),
            "p95": float(boot_values[int(0.95 * n_bootstrap)]),
        },
        "median": float(boot_values[int(0.50 * n_bootstrap)]),
        "n_samples": n,
        "n_bootstrap": n_bootstrap,
    }


# ---------------------------------------------------------------------------
# 入口（仅用于自测）
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # 自测：连接数据库、计算一些指标
    print("=== quant_common 自测 ===")
    try:
        cols, rows = db_query("SELECT COUNT(*) FROM backtest_equity_curve")
        print(f"✅ 数据库连接正常：backtest_equity_curve 共 {rows[0][0]} 行")
    except Exception as e:
        print(f"❌ 数据库连接失败：{e}")
        sys.exit(1)

    print(f"✅ 计算函数已加载")
    print(f"✅ 蓝图目标：{list(BLUEPRINT_TARGETS.keys())}")
    print("=== 自测通过 ===")
