"""
Query functions for the Streamlit dashboard.

All queries read from the gurysk_app layer (views over gurysk_dmt tables).
"""

from typing import Any, Dict, List, Optional

import pandas as pd
import pymysql


def get_filter_options(connection: pymysql.Connection) -> Dict[str, List[str]]:
    """Return available filter values for the dashboard sidebar."""
    months_df = pd.read_sql(
        "SELECT DISTINCT observed_month FROM gurysk_app.v_outin_sales_dashboard "
        "ORDER BY observed_month",
        connection,
    )
    marketplaces_df = pd.read_sql(
        "SELECT DISTINCT marketplace FROM gurysk_app.v_outin_sales_dashboard "
        "ORDER BY marketplace",
        connection,
    )
    asins_df = pd.read_sql(
        "SELECT DISTINCT asin, product_title FROM gurysk_app.v_outin_sales_dashboard "
        "ORDER BY asin",
        connection,
    )
    return {
        "months": months_df["observed_month"].tolist(),
        "marketplaces": marketplaces_df["marketplace"].tolist(),
        "asins": [
            {"asin": r["asin"], "title": r["product_title"]}
            for _, r in asins_df.iterrows()
        ],
    }


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
