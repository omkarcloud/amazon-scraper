"""
Query functions for the Streamlit dashboard.

Reads from gurysk_app views for the unified brand/category analysis page.
"""

from collections import defaultdict
from typing import Any, Dict, List, Optional

import pandas as pd
import pymysql

def get_generic_filter_options(
    connection: pymysql.Connection,
    brand: Optional[str] = None,
    category_name: Optional[str] = None,
) -> Dict[str, Any]:
    """Return filter options for the generic dashboard."""
    where = "WHERE 1=1"
    params: List[Any] = []
    if brand:
        where += " AND LOWER(COALESCE(brand,'')) LIKE %s"
        params.append(f"%{brand.lower()}%")
    if category_name:
        where += " AND LOWER(COALESCE(category_name,'')) = %s"
        params.append(category_name.lower())

    dates_df = pd.read_sql(
        f"SELECT DISTINCT observed_date FROM gurysk_app.v_product_daily_metrics {where} "
        f"ORDER BY observed_date",
        connection, params=params or None,
    )
    asin_df = pd.read_sql(
        f"SELECT DISTINCT marketplace, asin, brand, product_title "
        f"FROM gurysk_app.v_product_daily_metrics {where} "
        f"ORDER BY marketplace, brand, asin",
        connection, params=params or None,
    )

    brands_df = pd.read_sql(
        "SELECT DISTINCT brand FROM gurysk_app.v_product_daily_metrics "
        "WHERE brand IS NOT NULL ORDER BY brand",
        connection,
    )
    categories_df = pd.read_sql(
        f"SELECT DISTINCT category_name FROM gurysk_app.v_product_daily_metrics {where} "
        "AND category_name IS NOT NULL ORDER BY category_name",
        connection,
        params=params or None,
    )

    mp_asin_map: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for _, r in asin_df.iterrows():
        mp_asin_map[r["marketplace"]].append({
            "asin": r["asin"],
            "brand": r.get("brand", ""),
            "title": r.get("product_title", ""),
        })

    return {
        "dates": [d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)
                  for d in dates_df["observed_date"].tolist()],
        "brands": brands_df["brand"].tolist(),
        "categories": categories_df["category_name"].tolist(),
        "marketplace_asin_map": dict(mp_asin_map),
    }


def get_segment_filter_options(
    connection: pymysql.Connection,
    brand: Optional[str] = None,
    segment_name: Optional[str] = None,
) -> Dict[str, Any]:
    where = "WHERE 1=1"
    params: List[Any] = []
    if brand:
        where += " AND LOWER(COALESCE(brand,'')) LIKE %s"
        params.append(f"%{brand.lower()}%")
    if segment_name:
        where += " AND LOWER(COALESCE(segment_name,'')) = %s"
        params.append(segment_name.lower())

    dates_df = pd.read_sql(
        f"SELECT DISTINCT observed_date FROM gurysk_app.v_segment_product_daily_metrics {where} "
        f"ORDER BY observed_date",
        connection, params=params or None,
    )
    asin_df = pd.read_sql(
        f"SELECT DISTINCT marketplace, asin, brand, product_title "
        f"FROM gurysk_app.v_segment_product_daily_metrics {where} "
        f"ORDER BY marketplace, brand, asin",
        connection, params=params or None,
    )
    brands_df = pd.read_sql(
        "SELECT DISTINCT brand FROM gurysk_app.v_segment_product_daily_metrics "
        "WHERE brand IS NOT NULL ORDER BY brand",
        connection,
    )
    segments_df = pd.read_sql(
        f"SELECT DISTINCT segment_name FROM gurysk_app.v_segment_product_daily_metrics {where} "
        "AND segment_name IS NOT NULL ORDER BY segment_name",
        connection, params=params or None,
    )

    mp_asin_map: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for _, r in asin_df.iterrows():
        mp_asin_map[r["marketplace"]].append({
            "asin": r["asin"],
            "brand": r.get("brand", ""),
            "title": r.get("product_title", ""),
        })

    return {
        "dates": [d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)
                  for d in dates_df["observed_date"].tolist()],
        "brands": brands_df["brand"].tolist(),
        "segments": segments_df["segment_name"].tolist(),
        "marketplace_asin_map": dict(mp_asin_map),
    }


def get_product_daily_metrics(
    connection: pymysql.Connection,
    brand: Optional[str] = None,
    category_name: Optional[str] = None,
    marketplace: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    asins: Optional[List[str]] = None,
) -> pd.DataFrame:
    sql = "SELECT * FROM gurysk_app.v_product_daily_metrics WHERE 1=1"
    params: List[Any] = []

    if brand:
        sql += " AND LOWER(COALESCE(brand,'')) LIKE %s"
        params.append(f"%{brand.lower()}%")
    if category_name:
        sql += " AND LOWER(COALESCE(category_name,'')) = %s"
        params.append(category_name.lower())
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


def get_daily_sales_estimates(
    connection: pymysql.Connection,
    brand: Optional[str] = None,
    category_name: Optional[str] = None,
    marketplace: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    asins: Optional[List[str]] = None,
) -> pd.DataFrame:
    sql = "SELECT * FROM gurysk_app.v_daily_sales_estimate WHERE 1=1"
    params: List[Any] = []

    if brand:
        sql += " AND LOWER(COALESCE(brand,'')) LIKE %s"
        params.append(f"%{brand.lower()}%")
    if category_name:
        sql += " AND LOWER(COALESCE(category_name,'')) = %s"
        params.append(category_name.lower())
    if marketplace:
        sql += " AND marketplace = %s"
        params.append(marketplace)
    if start_date:
        sql += " AND estimate_date >= %s"
        params.append(start_date)
    if end_date:
        sql += " AND estimate_date <= %s"
        params.append(end_date)
    if asins:
        placeholders = ", ".join(["%s"] * len(asins))
        sql += f" AND asin IN ({placeholders})"
        params.extend(asins)

    sql += " ORDER BY estimate_date, asin"
    return pd.read_sql(sql, connection, params=params or None)


def get_brand_market_share(
    connection: pymysql.Connection,
    category_name: Optional[str] = None,
    marketplace: Optional[str] = None,
    target_date: Optional[str] = None,
) -> pd.DataFrame:
    """Fetch brand market share for a category on a date (latest if omitted)."""
    sql = "SELECT * FROM gurysk_app.v_brand_market_share WHERE 1=1"
    params: List[Any] = []

    if category_name:
        sql += " AND LOWER(COALESCE(category_name,'')) = %s"
        params.append(category_name.lower())
    if marketplace:
        sql += " AND marketplace = %s"
        params.append(marketplace)
    if target_date:
        sql += " AND observed_date = %s"
        params.append(target_date)
    else:
        sql += " AND observed_date = (SELECT MAX(observed_date) FROM gurysk_app.v_brand_market_share"
        if category_name:
            sql += " WHERE LOWER(COALESCE(category_name,'')) = %s"
            params.append(category_name.lower())
        sql += ")"

    sql += " ORDER BY sales_share_pct DESC"
    return pd.read_sql(sql, connection, params=params or None)


def get_brand_market_share_trend(
    connection: pymysql.Connection,
    brand: str,
    category_name: Optional[str] = None,
    marketplace: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    sql = (
        "SELECT * FROM gurysk_app.v_brand_market_share "
        "WHERE LOWER(brand) LIKE %s"
    )
    params: List[Any] = [f"%{brand.lower()}%"]

    if category_name:
        sql += " AND LOWER(COALESCE(category_name,'')) = %s"
        params.append(category_name.lower())
    if marketplace:
        sql += " AND marketplace = %s"
        params.append(marketplace)
    if start_date:
        sql += " AND observed_date >= %s"
        params.append(start_date)
    if end_date:
        sql += " AND observed_date <= %s"
        params.append(end_date)

    sql += " ORDER BY observed_date"
    return pd.read_sql(sql, connection, params=params or None)


def get_trend_alerts(
    connection: pymysql.Connection,
    brand: Optional[str] = None,
    marketplace: Optional[str] = None,
    start_date: Optional[str] = None,
    limit: int = 50,
) -> pd.DataFrame:
    sql = "SELECT * FROM gurysk_app.v_trend_alert WHERE 1=1"
    params: List[Any] = []

    if brand:
        sql += " AND LOWER(dimension_value) LIKE %s"
        params.append(f"%{brand.lower()}%")
    if marketplace:
        sql += " AND marketplace = %s"
        params.append(marketplace)
    if start_date:
        sql += " AND alert_date >= %s"
        params.append(start_date)

    sql += " ORDER BY alert_date DESC, ABS(z_score) DESC LIMIT %s"
    params.append(limit)

    return pd.read_sql(sql, connection, params=params or None)


def get_segment_product_daily_metrics(
    connection: pymysql.Connection,
    brand: Optional[str] = None,
    segment_name: Optional[str] = None,
    marketplace: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    asins: Optional[List[str]] = None,
) -> pd.DataFrame:
    sql = "SELECT * FROM gurysk_app.v_segment_product_daily_metrics WHERE 1=1"
    params: List[Any] = []

    if brand:
        sql += " AND LOWER(COALESCE(brand,'')) LIKE %s"
        params.append(f"%{brand.lower()}%")
    if segment_name:
        sql += " AND LOWER(COALESCE(segment_name,'')) = %s"
        params.append(segment_name.lower())
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


def get_segment_daily_sales_estimates(
    connection: pymysql.Connection,
    brand: Optional[str] = None,
    segment_name: Optional[str] = None,
    marketplace: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    asins: Optional[List[str]] = None,
) -> pd.DataFrame:
    sql = """
        SELECT
            m.observed_date AS estimate_date,
            m.marketplace,
            m.segment_name,
            m.segment_keyword,
            m.asin,
            m.brand,
            m.product_title,
            COALESCE(e.sales_volume_num, m.sales_volume_num) AS sales_volume_num,
            e.bsr_rank,
            COALESCE(e.num_ratings, m.num_ratings) AS num_ratings,
            e.num_ratings_delta,
            COALESCE(e.price, m.price) AS price,
            COALESCE(e.star_rating, m.star_rating) AS star_rating,
            e.estimated_daily_sales,
            e.estimate_lower_bound,
            e.estimate_upper_bound,
            e.estimation_method,
            e.confidence_score
        FROM gurysk_app.v_segment_product_daily_metrics m
        LEFT JOIN gurysk_app.v_daily_sales_estimate e
          ON m.asin = e.asin
         AND m.marketplace = e.marketplace
         AND m.observed_date = e.estimate_date
        WHERE 1=1
    """
    params: List[Any] = []

    if brand:
        sql += " AND LOWER(COALESCE(m.brand,'')) LIKE %s"
        params.append(f"%{brand.lower()}%")
    if segment_name:
        sql += " AND LOWER(COALESCE(m.segment_name,'')) = %s"
        params.append(segment_name.lower())
    if marketplace:
        sql += " AND m.marketplace = %s"
        params.append(marketplace)
    if start_date:
        sql += " AND m.observed_date >= %s"
        params.append(start_date)
    if end_date:
        sql += " AND m.observed_date <= %s"
        params.append(end_date)
    if asins:
        placeholders = ", ".join(["%s"] * len(asins))
        sql += f" AND m.asin IN ({placeholders})"
        params.extend(asins)

    sql += " ORDER BY estimate_date, asin"
    return pd.read_sql(sql, connection, params=params or None)


def get_segment_market_share(
    connection: pymysql.Connection,
    segment_name: Optional[str] = None,
    marketplace: Optional[str] = None,
    target_date: Optional[str] = None,
) -> pd.DataFrame:
    sql = "SELECT * FROM gurysk_app.v_segment_market_share WHERE 1=1"
    params: List[Any] = []

    if segment_name:
        sql += " AND LOWER(COALESCE(segment_name,'')) = %s"
        params.append(segment_name.lower())
    if marketplace:
        sql += " AND marketplace = %s"
        params.append(marketplace)
    if target_date:
        sql += " AND observed_date = %s"
        params.append(target_date)
    else:
        sql += " AND observed_date = (SELECT MAX(observed_date) FROM gurysk_app.v_segment_market_share"
        if segment_name:
            sql += " WHERE LOWER(COALESCE(segment_name,'')) = %s"
            params.append(segment_name.lower())
        sql += ")"

    sql += " ORDER BY sales_share_pct DESC"
    return pd.read_sql(sql, connection, params=params or None)


def get_segment_market_share_trend(
    connection: pymysql.Connection,
    brand: str,
    segment_name: Optional[str] = None,
    marketplace: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    sql = (
        "SELECT * FROM gurysk_app.v_segment_market_share "
        "WHERE LOWER(brand) LIKE %s"
    )
    params: List[Any] = [f"%{brand.lower()}%"]

    if segment_name:
        sql += " AND LOWER(COALESCE(segment_name,'')) = %s"
        params.append(segment_name.lower())
    if marketplace:
        sql += " AND marketplace = %s"
        params.append(marketplace)
    if start_date:
        sql += " AND observed_date >= %s"
        params.append(start_date)
    if end_date:
        sql += " AND observed_date <= %s"
        params.append(end_date)

    sql += " ORDER BY observed_date"
    return pd.read_sql(sql, connection, params=params or None)


def get_segment_estimation_stats(
    connection: pymysql.Connection,
    segment_name: Optional[str] = None,
    marketplace: Optional[str] = None,
    target_date: Optional[str] = None,
) -> pd.DataFrame:
    sql = "SELECT * FROM gurysk_app.v_segment_estimation_stats WHERE 1=1"
    params: List[Any] = []

    if segment_name:
        sql += " AND LOWER(COALESCE(segment_name,'')) = %s"
        params.append(segment_name.lower())
    if marketplace:
        sql += " AND marketplace = %s"
        params.append(marketplace)
    if target_date:
        sql += " AND observed_date = %s"
        params.append(target_date)

    sql += " ORDER BY observed_date DESC, marketplace"
    return pd.read_sql(sql, connection, params=params or None)
