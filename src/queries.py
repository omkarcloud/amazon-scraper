"""
Query functions for the Streamlit dashboard.

All queries read from the gurysk_app layer (views over gurysk_dmt tables).
"""

from collections import defaultdict
from typing import Any, Dict, List, Optional

import pandas as pd
import pymysql


def get_filter_options(connection: pymysql.Connection) -> Dict[str, Any]:
    """Return available filter values with marketplace-ASIN cascading map."""
    months_df = pd.read_sql(
        "SELECT DISTINCT observed_month FROM gurysk_app.v_outin_sales_dashboard "
        "ORDER BY observed_month",
        connection,
    )
    dates_df = pd.read_sql(
        "SELECT DISTINCT observed_date FROM gurysk_app.v_outin_daily_sales_dashboard "
        "ORDER BY observed_date",
        connection,
    )
    asin_df = pd.read_sql(
        "SELECT DISTINCT marketplace, asin, product_title "
        "FROM gurysk_app.v_outin_daily_sales_dashboard "
        "WHERE sales_volume_num IS NOT NULL AND sales_volume_num > 0 "
        "ORDER BY marketplace, asin",
        connection,
    )

    marketplace_asin_map: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for _, r in asin_df.iterrows():
        marketplace_asin_map[r["marketplace"]].append(
            {"asin": r["asin"], "title": r["product_title"]}
        )

    return {
        "months": months_df["observed_month"].tolist(),
        "dates": [d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)
                  for d in dates_df["observed_date"].tolist()],
        "marketplace_asin_map": dict(marketplace_asin_map),
    }


def get_outin_daily_sales(
    connection: pymysql.Connection,
    marketplace: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    asins: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Fetch OutIn daily sales data (only rows with sales > 0)."""
    sql = (
        "SELECT * FROM gurysk_app.v_outin_daily_sales_dashboard "
        "WHERE sales_volume_num IS NOT NULL AND sales_volume_num > 0"
    )
    params: List[Any] = []

    if marketplace:
        sql += " AND marketplace = %s"
        params.append(marketplace)
    if start_date:
        sql += " AND observed_date >= %s"
        params.append(start_date)
    if end_date:
        sql += " AND observed_date <= %s"
        params.append(end_date)
    if asins:
        placeholders = ", ".join(["%s"] * len(asins))
        sql += f" AND asin IN ({placeholders})"
        params.extend(asins)

    sql += " ORDER BY observed_date, asin"
    return pd.read_sql(sql, connection, params=params or None)


def get_outin_monthly_sales(
    connection: pymysql.Connection,
    marketplace: Optional[str] = None,
    start_month: Optional[str] = None,
    end_month: Optional[str] = None,
    asins: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Fetch OutIn monthly sales data."""
    sql = "SELECT * FROM gurysk_app.v_outin_sales_dashboard WHERE 1=1"
    params: List[Any] = []

    if marketplace:
        sql += " AND marketplace = %s"
        params.append(marketplace)
    if start_month:
        sql += " AND observed_month >= %s"
        params.append(start_month)
    if end_month:
        sql += " AND observed_month <= %s"
        params.append(end_month)
    if asins:
        placeholders = ", ".join(["%s"] * len(asins))
        sql += f" AND asin IN ({placeholders})"
        params.extend(asins)

    sql += " ORDER BY observed_month, asin"
    return pd.read_sql(sql, connection, params=params or None)


def get_outin_review_trend(
    connection: pymysql.Connection,
    marketplace: Optional[str] = None,
    start_month: Optional[str] = None,
    end_month: Optional[str] = None,
    asins: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Fetch OutIn review trend data."""
    sql = "SELECT * FROM gurysk_app.v_outin_review_dashboard WHERE 1=1"
    params: List[Any] = []

    if marketplace:
        sql += " AND marketplace = %s"
        params.append(marketplace)
    if start_month:
        sql += " AND observed_month >= %s"
        params.append(start_month)
    if end_month:
        sql += " AND observed_month <= %s"
        params.append(end_month)
    if asins:
        placeholders = ", ".join(["%s"] * len(asins))
        sql += f" AND asin IN ({placeholders})"
        params.extend(asins)

    sql += " ORDER BY observed_month, asin"
    return pd.read_sql(sql, connection, params=params or None)


def get_market_share(
    connection: pymysql.Connection,
    marketplace: Optional[str] = None,
    month: Optional[str] = None,
) -> pd.DataFrame:
    """Fetch market share data for a specific month (or latest)."""
    if month:
        sql = (
            "SELECT * FROM gurysk_app.v_market_share_dashboard "
            "WHERE observed_month = %s"
        )
        params: List[Any] = [month]
    else:
        sql = (
            "SELECT * FROM gurysk_app.v_market_share_dashboard "
            "WHERE observed_month = ("
            "  SELECT MAX(observed_month) FROM gurysk_app.v_market_share_dashboard"
            ")"
        )
        params = []

    if marketplace:
        sql += " AND marketplace = %s"
        params.append(marketplace)

    sql += " ORDER BY rating_share_pct DESC"
    return pd.read_sql(sql, connection, params=params or None)


def get_market_share_trend(
    connection: pymysql.Connection,
    brand: str = "OUTIN",
    marketplace: Optional[str] = None,
    start_month: Optional[str] = None,
    end_month: Optional[str] = None,
) -> pd.DataFrame:
    """Fetch market share trend for a specific brand over time."""
    sql = (
        "SELECT * FROM gurysk_app.v_market_share_dashboard "
        "WHERE LOWER(brand) LIKE %s"
    )
    params: List[Any] = [f"%{brand.lower()}%"]

    if marketplace:
        sql += " AND marketplace = %s"
        params.append(marketplace)
    if start_month:
        sql += " AND observed_month >= %s"
        params.append(start_month)
    if end_month:
        sql += " AND observed_month <= %s"
        params.append(end_month)

    sql += " ORDER BY observed_month"
    return pd.read_sql(sql, connection, params=params or None)
