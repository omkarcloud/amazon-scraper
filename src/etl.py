"""
ETL pipeline: SRC (amazon_product_raw) -> DWH -> DMT -> APP.

Handles table creation, JSON parsing, incremental extraction,
daily/monthly aggregation, market-share computation, and Bayesian
daily sales estimation.

Supports both legacy hard-coded brand/category pipelines (OutIn / Coffee)
and the new generic brand/category pipeline driven by parameters.
"""

import html
import json
import logging
import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pymysql

from .sales_estimator import (
    BayesianDailySalesEstimator,
    CategoryParams,
    estimate_daily_sales_batch,
    get_category_params,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DDL constants
# ---------------------------------------------------------------------------

DWH_SNAPSHOT_DDL = """
CREATE TABLE IF NOT EXISTS gurysk_dwh.dwh_amazon_product_snapshot (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    src_id          BIGINT NOT NULL COMMENT '关联 amazon_product_raw.id',

    observed_at     DATETIME NOT NULL COMMENT '精确观测时间戳',
    observed_date   DATE NOT NULL COMMENT '观测日期',
    observed_month  CHAR(7) NOT NULL COMMENT '观测月份 YYYY-MM',

    asin            VARCHAR(32) NOT NULL,
    marketplace     VARCHAR(8) NOT NULL,
    source_endpoint VARCHAR(64) NOT NULL,
    search_query    TEXT,
    category_name   VARCHAR(255),
    segment_name    VARCHAR(255),
    segment_keyword VARCHAR(255),

    product_title   VARCHAR(1024),
    brand           VARCHAR(256),
    price           DECIMAL(10,2),
    currency        VARCHAR(8),
    star_rating     DECIMAL(3,2),
    num_ratings     INT,
    num_reviews     INT,
    sales_volume_raw VARCHAR(128),
    sales_volume_num INT,
    is_best_seller  TINYINT(1),
    is_prime        TINYINT(1),
    product_url     TEXT,
    image_url       TEXT,

    etl_loaded_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    UNIQUE KEY uk_src_id (src_id),
    INDEX idx_asin_observed (asin, observed_date),
    INDEX idx_asin_month (asin, observed_month),
    INDEX idx_observed_date (observed_date),
    INDEX idx_brand_month (brand, observed_month)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

DMT_OUTIN_MONTHLY_SALES_DDL = """
CREATE TABLE IF NOT EXISTS gurysk_dmt.dmt_outin_monthly_sales (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    observed_month  CHAR(7) NOT NULL,
    marketplace     VARCHAR(8) NOT NULL,
    asin            VARCHAR(32) NOT NULL,
    product_title   VARCHAR(1024),

    month_end_price          DECIMAL(10,2),
    month_end_sales_volume   INT,
    month_end_num_ratings    INT,

    sales_volume_mom_change  INT,
    num_ratings_mom_delta    INT,

    avg_price       DECIMAL(10,2),
    observation_count INT,
    first_observed_at DATETIME,
    last_observed_at  DATETIME,

    etl_loaded_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_month_asin (observed_month, asin, marketplace)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

DMT_OUTIN_REVIEW_TREND_DDL = """
CREATE TABLE IF NOT EXISTS gurysk_dmt.dmt_outin_review_trend (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    observed_month  CHAR(7) NOT NULL,
    marketplace     VARCHAR(8) NOT NULL,
    asin            VARCHAR(32) NOT NULL,
    product_title   VARCHAR(1024),

    month_end_star_rating  DECIMAL(3,2),
    month_end_num_ratings  INT,
    month_end_num_reviews  INT,

    new_ratings_this_month INT,
    star_rating_mom_change DECIMAL(3,2),

    avg_star_rating DECIMAL(3,2),
    min_star_rating DECIMAL(3,2),
    max_star_rating DECIMAL(3,2),
    observation_count INT,
    last_observed_at  DATETIME,

    etl_loaded_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_month_asin (observed_month, asin, marketplace)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

DMT_OUTIN_DAILY_SALES_DDL = """
CREATE TABLE IF NOT EXISTS gurysk_dmt.dmt_outin_daily_sales (
    id                      BIGINT AUTO_INCREMENT PRIMARY KEY,
    observed_date           DATE NOT NULL,
    marketplace             VARCHAR(8) NOT NULL,
    asin                    VARCHAR(32) NOT NULL,
    product_title           VARCHAR(1024),
    price                   DECIMAL(10,2),
    sales_volume_num        INT,
    num_ratings             INT,
    star_rating             DECIMAL(3,2),
    sales_volume_dod_change INT COMMENT '日销量环比变化 (当日 - 前一日)',
    num_ratings_dod_change  INT COMMENT '日评价数环比变化',
    observation_count       INT,
    etl_loaded_at           DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_date_asin (observed_date, asin, marketplace),
    INDEX idx_asin_date (asin, observed_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

DMT_COFFEE_MARKET_SHARE_DDL = """
CREATE TABLE IF NOT EXISTS gurysk_dmt.dmt_coffee_market_share (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    observed_month  CHAR(7) NOT NULL,
    marketplace     VARCHAR(8) NOT NULL,
    brand           VARCHAR(256) NOT NULL,

    distinct_asins      INT,
    total_num_ratings   BIGINT,
    total_sales_volume  BIGINT,
    avg_price           DECIMAL(10,2),
    avg_star_rating     DECIMAL(3,2),

    rating_share_pct    DECIMAL(5,2),
    sales_share_pct     DECIMAL(5,2),
    asin_share_pct      DECIMAL(5,2),

    last_observed_at    DATETIME,
    etl_loaded_at       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_month_brand (observed_month, brand, marketplace)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


# ---------------------------------------------------------------------------
# New generic DWH tables
# ---------------------------------------------------------------------------

DWH_REVIEW_SNAPSHOT_DDL = """
CREATE TABLE IF NOT EXISTS gurysk_dwh.dwh_amazon_review_snapshot (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    src_id          BIGINT NOT NULL,
    observed_at     DATETIME NOT NULL,
    observed_date   DATE NOT NULL,
    asin            VARCHAR(32) NOT NULL,
    marketplace     VARCHAR(8) NOT NULL,

    review_id       VARCHAR(64),
    review_title    TEXT,
    review_comment  TEXT,
    review_star_rating DECIMAL(3,2),
    review_date     VARCHAR(64),
    is_verified_purchase TINYINT(1),
    helpful_count   INT,

    etl_loaded_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_src_review (src_id, review_id),
    INDEX idx_asin_date (asin, observed_date),
    INDEX idx_asin_rating (asin, review_star_rating)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

DWH_OFFER_SNAPSHOT_DDL = """
CREATE TABLE IF NOT EXISTS gurysk_dwh.dwh_amazon_offer_snapshot (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    src_id          BIGINT NOT NULL,
    observed_at     DATETIME NOT NULL,
    observed_date   DATE NOT NULL,
    asin            VARCHAR(32) NOT NULL,
    marketplace     VARCHAR(8) NOT NULL,

    offer_price     DECIMAL(10,2),
    original_price  DECIMAL(10,2),
    discount_pct    DECIMAL(5,2),
    product_condition VARCHAR(32),
    is_prime        TINYINT(1),

    etl_loaded_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_asin_date (asin, observed_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

DWH_ASIN_HIERARCHY_DDL = """
CREATE TABLE IF NOT EXISTS gurysk_dwh.dwh_asin_hierarchy (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    parent_asin     VARCHAR(32) NOT NULL COMMENT 'SPU',
    child_asin      VARCHAR(32) NOT NULL COMMENT 'SKU',
    marketplace     VARCHAR(8) NOT NULL,
    variation_type  VARCHAR(64),
    variation_value VARCHAR(256),
    first_seen      DATE,
    last_seen       DATE,
    UNIQUE KEY uk_parent_child (parent_asin, child_asin, marketplace),
    INDEX idx_child (child_asin)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

# ---------------------------------------------------------------------------
# New generic DMT tables
# ---------------------------------------------------------------------------

DMT_PRODUCT_DAILY_METRICS_DDL = """
CREATE TABLE IF NOT EXISTS gurysk_dmt.dmt_product_daily_metrics (
    id                      BIGINT AUTO_INCREMENT PRIMARY KEY,
    observed_date           DATE NOT NULL,
    marketplace             VARCHAR(8) NOT NULL,
    asin                    VARCHAR(32) NOT NULL,
    parent_asin             VARCHAR(32),
    brand                   VARCHAR(256),
    product_title           VARCHAR(1024),
    category_id             VARCHAR(64),
    category_name           VARCHAR(255),

    price                   DECIMAL(10,2),
    original_price          DECIMAL(10,2),
    discount_pct            DECIMAL(5,2),
    sales_volume_raw        VARCHAR(128),
    sales_volume_num        INT,
    star_rating             DECIMAL(3,2),
    num_ratings             INT,
    num_reviews             INT,
    bsr_rank                INT,
    is_best_seller          TINYINT(1),

    price_dod_change        DECIMAL(10,2),
    discount_pct_dod_change DECIMAL(5,2),
    sales_volume_dod_change INT,
    num_ratings_dod_change  INT,
    bsr_rank_dod_change     INT,

    observation_count       INT,
    etl_loaded_at           DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_date_asin (observed_date, asin, marketplace),
    INDEX idx_brand_date (brand, observed_date),
    INDEX idx_category_date (category_id, observed_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

DMT_DAILY_SALES_ESTIMATE_DDL = """
CREATE TABLE IF NOT EXISTS gurysk_dmt.dmt_daily_sales_estimate (
    id                      BIGINT AUTO_INCREMENT PRIMARY KEY,
    estimate_date           DATE NOT NULL,
    marketplace             VARCHAR(8) NOT NULL,
    asin                    VARCHAR(32) NOT NULL,
    brand                   VARCHAR(256),
    category_id             VARCHAR(64),
    category_name           VARCHAR(255),

    sales_volume_num        INT,
    bsr_rank                INT,
    num_ratings             INT,
    num_ratings_delta       INT,
    price                   DECIMAL(10,2),
    star_rating             DECIMAL(3,2),

    estimated_daily_sales   INT,
    estimate_lower_bound    INT,
    estimate_upper_bound    INT,
    estimation_method       VARCHAR(32),
    confidence_score        DECIMAL(3,2),

    etl_loaded_at           DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_date_asin (estimate_date, asin, marketplace),
    INDEX idx_brand_date (brand, estimate_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

DMT_BRAND_MARKET_SHARE_DDL = """
CREATE TABLE IF NOT EXISTS gurysk_dmt.dmt_brand_market_share (
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    observed_date       DATE NOT NULL,
    marketplace         VARCHAR(8) NOT NULL,
    category_id         VARCHAR(64) NOT NULL,
    category_name       VARCHAR(255),
    brand               VARCHAR(256) NOT NULL,

    distinct_asins      INT,
    total_estimated_daily_sales BIGINT,
    total_num_ratings   BIGINT,
    total_sales_volume  BIGINT,
    avg_price           DECIMAL(10,2),
    avg_star_rating     DECIMAL(3,2),

    sales_share_pct     DECIMAL(5,2),
    rating_share_pct    DECIMAL(5,2),
    asin_share_pct      DECIMAL(5,2),

    last_observed_at    DATETIME,
    etl_loaded_at       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_date_cat_brand (observed_date, category_id, brand, marketplace),
    INDEX idx_brand (brand),
    INDEX idx_category (category_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

DMT_SEGMENT_PRODUCT_DAILY_METRICS_DDL = """
CREATE TABLE IF NOT EXISTS gurysk_dmt.dmt_segment_product_daily_metrics (
    id                      BIGINT AUTO_INCREMENT PRIMARY KEY,
    observed_date           DATE NOT NULL,
    marketplace             VARCHAR(8) NOT NULL,
    segment_name            VARCHAR(255) NOT NULL,
    segment_keyword         VARCHAR(255),
    asin                    VARCHAR(32) NOT NULL,
    parent_asin             VARCHAR(32),
    brand                   VARCHAR(256),
    product_title           VARCHAR(1024),

    price                   DECIMAL(10,2),
    original_price          DECIMAL(10,2),
    discount_pct            DECIMAL(5,2),
    sales_volume_raw        VARCHAR(128),
    sales_volume_num        INT,
    star_rating             DECIMAL(3,2),
    num_ratings             INT,
    num_reviews             INT,
    bsr_rank                INT,
    is_best_seller          TINYINT(1),

    price_dod_change        DECIMAL(10,2),
    discount_pct_dod_change DECIMAL(5,2),
    sales_volume_dod_change INT,
    num_ratings_dod_change  INT,
    bsr_rank_dod_change     INT,

    observation_count       INT,
    etl_loaded_at           DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_date_segment_asin (observed_date, marketplace, segment_name, asin),
    INDEX idx_segment_date (segment_name, observed_date),
    INDEX idx_brand_segment_date (brand, segment_name, observed_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

DMT_SEGMENT_MARKET_SHARE_DDL = """
CREATE TABLE IF NOT EXISTS gurysk_dmt.dmt_segment_market_share (
    id                      BIGINT AUTO_INCREMENT PRIMARY KEY,
    observed_date           DATE NOT NULL,
    marketplace             VARCHAR(8) NOT NULL,
    segment_name            VARCHAR(255) NOT NULL,
    brand                   VARCHAR(256) NOT NULL,

    distinct_asins          INT,
    total_estimated_daily_sales BIGINT,
    total_num_ratings       BIGINT,
    total_sales_volume      BIGINT,
    avg_price               DECIMAL(10,2),
    avg_star_rating         DECIMAL(3,2),

    sales_share_pct         DECIMAL(6,2),
    rating_share_pct        DECIMAL(6,2),
    asin_share_pct          DECIMAL(6,2),
    eb_adjusted_share_pct   DECIMAL(6,2),

    sample_asin_count       INT,
    estimated_market_asin_count INT,
    bootstrap_mean_sales    DECIMAL(14,2),
    bootstrap_lower_bound   DECIMAL(14,2),
    bootstrap_upper_bound   DECIMAL(14,2),
    coverage_ratio          DECIMAL(6,4),
    stability_score         DECIMAL(6,4),

    last_observed_at        DATETIME,
    etl_loaded_at           DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_date_segment_brand (observed_date, marketplace, segment_name, brand),
    INDEX idx_segment (segment_name),
    INDEX idx_segment_date (segment_name, observed_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

DMT_SEGMENT_ESTIMATION_STATS_DDL = """
CREATE TABLE IF NOT EXISTS gurysk_dmt.dmt_segment_estimation_stats (
    id                      BIGINT AUTO_INCREMENT PRIMARY KEY,
    observed_date           DATE NOT NULL,
    marketplace             VARCHAR(8) NOT NULL,
    segment_name            VARCHAR(255) NOT NULL,

    sample_asin_count       INT,
    estimated_market_asin_count INT,
    sample_sales_sum        DECIMAL(14,2),
    bootstrap_mean_sales    DECIMAL(14,2),
    bootstrap_lower_bound   DECIMAL(14,2),
    bootstrap_upper_bound   DECIMAL(14,2),
    coverage_ratio          DECIMAL(6,4),
    stability_score         DECIMAL(6,4),
    eb_prior_strength       DECIMAL(14,2),

    last_observed_at        DATETIME,
    etl_loaded_at           DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_date_segment_stats (observed_date, marketplace, segment_name),
    INDEX idx_segment_date (segment_name, observed_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

DMT_TREND_ALERT_DDL = """
CREATE TABLE IF NOT EXISTS gurysk_dmt.dmt_trend_alert (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    alert_date      DATE NOT NULL,
    marketplace     VARCHAR(8) NOT NULL,
    dimension_type  VARCHAR(32) NOT NULL COMMENT 'brand / asin / category',
    dimension_value VARCHAR(256) NOT NULL,
    metric_name     VARCHAR(64) NOT NULL,

    current_value   DECIMAL(12,2),
    baseline_value  DECIMAL(12,2),
    change_pct      DECIMAL(8,2),
    z_score         DECIMAL(6,2),
    alert_level     VARCHAR(16) COMMENT 'info / warning / critical',
    alert_message   TEXT,

    etl_loaded_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_alert_date (alert_date),
    INDEX idx_dimension (dimension_type, dimension_value)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

# ---------------------------------------------------------------------------
# APP views (legacy + generic)
# ---------------------------------------------------------------------------

APP_VIEWS_DDL = [
    """
    CREATE OR REPLACE VIEW gurysk_app.v_outin_sales_dashboard AS
    SELECT * FROM gurysk_dmt.dmt_outin_monthly_sales;
    """,
    """
    CREATE OR REPLACE VIEW gurysk_app.v_outin_daily_sales_dashboard AS
    SELECT * FROM gurysk_dmt.dmt_outin_daily_sales;
    """,
    """
    CREATE OR REPLACE VIEW gurysk_app.v_outin_review_dashboard AS
    SELECT * FROM gurysk_dmt.dmt_outin_review_trend;
    """,
    """
    CREATE OR REPLACE VIEW gurysk_app.v_market_share_dashboard AS
    SELECT * FROM gurysk_dmt.dmt_coffee_market_share;
    """,
    """
    CREATE OR REPLACE VIEW gurysk_app.v_product_daily_metrics AS
    SELECT * FROM gurysk_dmt.dmt_product_daily_metrics;
    """,
    """
    CREATE OR REPLACE VIEW gurysk_app.v_daily_sales_estimate AS
    SELECT * FROM gurysk_dmt.dmt_daily_sales_estimate;
    """,
    """
    CREATE OR REPLACE VIEW gurysk_app.v_brand_market_share AS
    SELECT * FROM gurysk_dmt.dmt_brand_market_share;
    """,
    """
    CREATE OR REPLACE VIEW gurysk_app.v_segment_product_daily_metrics AS
    SELECT * FROM gurysk_dmt.dmt_segment_product_daily_metrics;
    """,
    """
    CREATE OR REPLACE VIEW gurysk_app.v_segment_market_share AS
    SELECT * FROM gurysk_dmt.dmt_segment_market_share;
    """,
    """
    CREATE OR REPLACE VIEW gurysk_app.v_segment_estimation_stats AS
    SELECT * FROM gurysk_dmt.dmt_segment_estimation_stats;
    """,
    """
    CREATE OR REPLACE VIEW gurysk_app.v_trend_alert AS
    SELECT * FROM gurysk_dmt.dmt_trend_alert;
    """,
]


def _column_exists(
    connection: pymysql.Connection,
    schema: str,
    table: str,
    column: str,
) -> bool:
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s AND column_name = %s "
            "LIMIT 1",
            (schema, table, column),
        )
        return cursor.fetchone() is not None


def ensure_all_tables(connection: pymysql.Connection) -> None:
    """Create all DWH / DMT tables and APP views if they don't exist."""
    with connection.cursor() as cursor:
        for ddl in [
            DWH_SNAPSHOT_DDL,
            DWH_REVIEW_SNAPSHOT_DDL,
            DWH_OFFER_SNAPSHOT_DDL,
            DWH_ASIN_HIERARCHY_DDL,
            DMT_OUTIN_MONTHLY_SALES_DDL,
            DMT_OUTIN_DAILY_SALES_DDL,
            DMT_OUTIN_REVIEW_TREND_DDL,
            DMT_COFFEE_MARKET_SHARE_DDL,
            DMT_PRODUCT_DAILY_METRICS_DDL,
            DMT_DAILY_SALES_ESTIMATE_DDL,
            DMT_BRAND_MARKET_SHARE_DDL,
            DMT_SEGMENT_PRODUCT_DAILY_METRICS_DDL,
            DMT_SEGMENT_MARKET_SHARE_DDL,
            DMT_SEGMENT_ESTIMATION_STATS_DDL,
            DMT_TREND_ALERT_DDL,
        ]:
            cursor.execute(ddl)
        for view_ddl in APP_VIEWS_DDL:
            cursor.execute(view_ddl)
    if not _column_exists(connection, "gurysk_dwh", "dwh_amazon_product_snapshot", "category_name"):
        with connection.cursor() as cursor:
            cursor.execute(
                "ALTER TABLE gurysk_dwh.dwh_amazon_product_snapshot "
                "ADD COLUMN category_name VARCHAR(255) AFTER search_query"
            )
    if not _column_exists(connection, "gurysk_dwh", "dwh_amazon_product_snapshot", "segment_name"):
        with connection.cursor() as cursor:
            cursor.execute(
                "ALTER TABLE gurysk_dwh.dwh_amazon_product_snapshot "
                "ADD COLUMN segment_name VARCHAR(255) AFTER category_name"
            )
    if not _column_exists(connection, "gurysk_dwh", "dwh_amazon_product_snapshot", "segment_keyword"):
        with connection.cursor() as cursor:
            cursor.execute(
                "ALTER TABLE gurysk_dwh.dwh_amazon_product_snapshot "
                "ADD COLUMN segment_keyword VARCHAR(255) AFTER segment_name"
            )
    if not _column_exists(connection, "gurysk_dmt", "dmt_product_daily_metrics", "category_name"):
        with connection.cursor() as cursor:
            cursor.execute(
                "ALTER TABLE gurysk_dmt.dmt_product_daily_metrics "
                "ADD COLUMN category_name VARCHAR(255) AFTER category_id"
            )
    if not _column_exists(connection, "gurysk_dmt", "dmt_daily_sales_estimate", "category_name"):
        with connection.cursor() as cursor:
            cursor.execute(
                "ALTER TABLE gurysk_dmt.dmt_daily_sales_estimate "
                "ADD COLUMN category_name VARCHAR(255) AFTER category_id"
            )
    if not _column_exists(connection, "gurysk_dmt", "dmt_brand_market_share", "category_name"):
        with connection.cursor() as cursor:
            cursor.execute(
                "ALTER TABLE gurysk_dmt.dmt_brand_market_share "
                "ADD COLUMN category_name VARCHAR(255) AFTER category_id"
            )
    with connection.cursor() as cursor:
        for view_ddl in APP_VIEWS_DDL:
            cursor.execute(view_ddl)
    connection.commit()
    logger.info("All DWH/DMT/APP tables and views ensured.")


# ---------------------------------------------------------------------------
# Helpers: text cleaning
# ---------------------------------------------------------------------------

_MARKETPLACE_PREFIX_RE = re.compile(
    r"【[^】]*(?:限定|限り|セール|特選)[^】]*】\s*",
)

_CURLY_QUOTE_MAP = str.maketrans({
    "\u2018": "'", "\u2019": "'",  # ' '
    "\u201C": '"', "\u201D": '"',  # " "
})

_BRAND_ALIASES: Dict[str, str] = {
    "de'longhi": "De'Longhi",
    "delonghi": "De'Longhi",
    "de longhi": "De'Longhi",
    "nescafé": "NESCAFÉ",
    "nescafe": "NESCAFÉ",
    "black+decker": "BLACK+DECKER",
}

_BRAND_PREFIX_RE = re.compile(
    r"^(de'?longhi|delonghi|デロンギ)", re.IGNORECASE,
)


def clean_text(text: Optional[str]) -> Optional[str]:
    """Decode HTML entities and strip marketplace-specific noise from text."""
    if not text:
        return text
    text = html.unescape(str(text))
    text = text.translate(_CURLY_QUOTE_MAP)
    text = _MARKETPLACE_PREFIX_RE.sub("", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text if text else None


def clean_brand(brand: Optional[str]) -> Optional[str]:
    """Normalize a brand name: decode entities, unify known aliases."""
    if not brand:
        return brand
    brand = html.unescape(str(brand)).strip()
    brand = brand.translate(_CURLY_QUOTE_MAP)
    brand = _MARKETPLACE_PREFIX_RE.sub("", brand).strip()
    lookup = brand.lower()
    if lookup in _BRAND_ALIASES:
        return _BRAND_ALIASES[lookup]
    if _BRAND_PREFIX_RE.match(lookup):
        return "De'Longhi"
    return brand if brand else None


# ---------------------------------------------------------------------------
# Helpers: JSON field extraction
# ---------------------------------------------------------------------------

_SALES_VOL_RE = re.compile(
    r"([\d,.]+)\s*([kKmM]?)\+?\s*(?:bought|sold)?",
    re.IGNORECASE,
)


def parse_sales_volume(text: Optional[str]) -> Optional[int]:
    """Parse sales volume text like '1K+ bought in past month' into an int."""
    if not text:
        return None
    m = _SALES_VOL_RE.search(str(text))
    if not m:
        return None
    try:
        value = float(m.group(1).replace(",", ""))
    except ValueError:
        return None
    suffix = m.group(2).upper()
    if suffix == "K":
        value *= 1_000
    elif suffix == "M":
        value *= 1_000_000
    return int(value)


def extract_brand(payload: Dict[str, Any]) -> Optional[str]:
    """Extract brand from an API payload dict."""
    product_info = payload.get("product_information", {})
    if isinstance(product_info, dict):
        brand = product_info.get("Brand") or product_info.get("brand")
        if brand:
            return str(brand).strip()

    brand = payload.get("brand")
    if brand:
        return str(brand).strip()

    title = payload.get("product_title") or payload.get("title") or ""
    if title:
        first_word = str(title).split()[0].strip(",.-") if str(title).split() else None
        if first_word and len(first_word) > 1:
            return first_word
    return None


def _safe_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    if isinstance(val, str):
        val = re.sub(r"[^\d.]", "", val)
    try:
        return float(val) if val else None
    except (ValueError, TypeError):
        return None


def _safe_int(val: Any) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _bool_to_tinyint(val: Any) -> Optional[int]:
    if val is None:
        return None
    return 1 if val else 0


def _extract_payload_fields(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Extract structured fields from product_search or product_details payload."""
    title = payload.get("product_title") or payload.get("title")
    sales_vol_raw = payload.get("sales_volume")
    if isinstance(sales_vol_raw, dict):
        sales_vol_raw = sales_vol_raw.get("text") or str(sales_vol_raw)
    elif sales_vol_raw is not None:
        sales_vol_raw = str(sales_vol_raw)

    images = payload.get("images")
    image_url = (
        payload.get("product_photo")
        or payload.get("featured_image")
        or (images[0] if isinstance(images, list) and images else None)
    )

    return {
        "product_title": clean_text(str(title)[:1024]) if title else None,
        "brand": clean_brand(extract_brand(payload)),
        "price": _safe_float(payload.get("product_price") or payload.get("price")),
        "currency": (str(payload["currency"])[:8] if payload.get("currency") else None),
        "star_rating": _safe_float(
            payload.get("product_star_rating") or payload.get("rating")
        ),
        "num_ratings": _safe_int(
            payload.get("product_num_ratings") or payload.get("reviews")
        ),
        "num_reviews": _safe_int(payload.get("product_num_reviews")),
        "sales_volume_raw": str(sales_vol_raw)[:128] if sales_vol_raw else None,
        "sales_volume_num": parse_sales_volume(sales_vol_raw),
        "is_best_seller": _bool_to_tinyint(payload.get("is_best_seller")),
        "is_prime": _bool_to_tinyint(payload.get("is_prime")),
        "product_url": payload.get("product_url") or payload.get("link"),
        "image_url": image_url,
    }


# ---------------------------------------------------------------------------
# ETL Step 1: SRC -> DWH  (incremental)
# ---------------------------------------------------------------------------

_DWH_INSERT_SQL = """
    INSERT IGNORE INTO gurysk_dwh.dwh_amazon_product_snapshot (
        src_id, observed_at, observed_date, observed_month,
        asin, marketplace, source_endpoint, search_query, category_name, segment_name, segment_keyword,
        product_title, brand, price, currency,
        star_rating, num_ratings, num_reviews,
        sales_volume_raw, sales_volume_num,
        is_best_seller, is_prime, product_url, image_url
    ) VALUES (
        %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s,
        %s, %s,
        %s, %s, %s, %s
    )
"""


def _to_observed_date(dt: Any) -> date:
    if isinstance(dt, datetime):
        return dt.date()
    if isinstance(dt, date):
        return dt
    return datetime.fromisoformat(str(dt)).date()


def _to_observed_month(dt: Any) -> str:
    if hasattr(dt, "strftime"):
        return dt.strftime("%Y-%m")
    return str(dt)[:7]


def _extract_leaf_category_from_payload(payload: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    if not isinstance(payload, dict):
        return None, None

    category_path = payload.get("category_path")
    if isinstance(category_path, list) and category_path:
        leaf = category_path[-1]
        if isinstance(leaf, dict):
            return (
                str(leaf.get("id")).strip() if leaf.get("id") else None,
                str(leaf.get("name")).strip() if leaf.get("name") else None,
            )

    category = payload.get("category")
    if isinstance(category, dict):
        return (
            str(category.get("id")).strip() if category.get("id") else None,
            str(category.get("name")).strip() if category.get("name") else None,
        )
    return None, None


def extract_to_dwh(
    connection: pymysql.Connection,
    batch_size: int = 500,
) -> int:
    """Incrementally extract from SRC to DWH. Returns rows inserted."""
    ensure_all_tables(connection)

    with connection.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(
            "SELECT COALESCE(MAX(src_id), 0) AS max_id "
            "FROM gurysk_dwh.dwh_amazon_product_snapshot"
        )
        max_src_id = cur.fetchone()["max_id"]

        cur.execute(
            "SELECT id, asin, record_create_timestamp, source_endpoint, "
            "       marketplace_country, search_query, request_metadata, api_payload "
            "FROM gurysk_src.amazon_product_raw "
            "WHERE id > %s ORDER BY id",
            (max_src_id,),
        )
        src_rows = cur.fetchall()

    if not src_rows:
        logger.info("extract_to_dwh: no new SRC rows.")
        return 0

    params_batch: List[Tuple] = []
    for row in src_rows:
        request_metadata = row.get("request_metadata")
        if isinstance(request_metadata, str):
            request_metadata = json.loads(request_metadata)
        if not isinstance(request_metadata, dict):
            request_metadata = {}

        payload = row["api_payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        inner = payload.get("data", payload) if isinstance(payload, dict) else payload
        if not isinstance(inner, dict):
            inner = {}

        payload_category_id, payload_category_name = _extract_leaf_category_from_payload(inner)
        effective_search_query = row.get("search_query")
        if not effective_search_query and payload_category_id:
            effective_search_query = f"category:{payload_category_id}"
        effective_category_name = (
            request_metadata.get("category_name")
            or payload_category_name
        )
        effective_segment_name = request_metadata.get("segment_name")
        effective_segment_keyword = request_metadata.get("segment_keyword")

        observed_at = row["record_create_timestamp"]
        fields = _extract_payload_fields(inner)

        params_batch.append((
            row["id"],
            observed_at,
            _to_observed_date(observed_at),
            _to_observed_month(observed_at),
            row["asin"],
            row["marketplace_country"],
            row["source_endpoint"],
            effective_search_query,
            effective_category_name,
            effective_segment_name,
            effective_segment_keyword,
            fields["product_title"],
            fields["brand"],
            fields["price"],
            fields["currency"],
            fields["star_rating"],
            fields["num_ratings"],
            fields["num_reviews"],
            fields["sales_volume_raw"],
            fields["sales_volume_num"],
            fields["is_best_seller"],
            fields["is_prime"],
            fields["product_url"],
            fields["image_url"],
        ))

        if len(params_batch) >= batch_size:
            with connection.cursor() as cur:
                cur.executemany(_DWH_INSERT_SQL, params_batch)
            connection.commit()
            params_batch.clear()

    if params_batch:
        with connection.cursor() as cur:
            cur.executemany(_DWH_INSERT_SQL, params_batch)
        connection.commit()

    total = len(src_rows)
    logger.info("extract_to_dwh: %d rows processed.", total)
    return total


# ---------------------------------------------------------------------------
# ETL Step 2a-daily: DWH -> DMT  (OutIn daily sales)
# ---------------------------------------------------------------------------

_OUTIN_FILTER = (
    "LOWER(COALESCE(brand,'')) LIKE '%%outin%%' "
    "OR LOWER(COALESCE(product_title,'')) LIKE '%%outin%%'"
)


def _apply_text_cleaning(df: pd.DataFrame) -> pd.DataFrame:
    """Apply clean_text / clean_brand to a DWH DataFrame in-place."""
    if "product_title" in df.columns:
        df["product_title"] = df["product_title"].apply(clean_text)
    if "brand" in df.columns:
        df["brand"] = df["brand"].apply(clean_brand)
    return df


def clean_dwh_data(connection: pymysql.Connection) -> int:
    """One-time fix: update existing DWH rows with cleaned text/brand."""
    with connection.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(
            "SELECT id, product_title, brand "
            "FROM gurysk_dwh.dwh_amazon_product_snapshot "
            "WHERE product_title LIKE '%%&#%%' "
            "   OR product_title LIKE '%%&amp;%%' "
            "   OR product_title LIKE '%%【%%' "
            "   OR product_title LIKE '%%\u2019%%' "
            "   OR product_title LIKE '%%\u2018%%' "
            "   OR brand LIKE '%%&#%%' "
            "   OR brand LIKE '%%&amp;%%' "
            "   OR brand LIKE '%%【%%' "
            "   OR brand LIKE '%%\u2019%%' "
            "   OR brand LIKE '%%\u2018%%' "
            "   OR brand LIKE '%%DELONGHI%%' "
            "   OR brand LIKE '%%DeLonghi%%' "
            "   OR brand LIKE '%%De%%Longhi%%' "
            "   OR brand LIKE '%%デロンギ%%'"
        )
        dirty_rows = cur.fetchall()

    if not dirty_rows:
        logger.info("clean_dwh_data: no dirty rows found.")
        return 0

    updates: List[Tuple] = []
    for row in dirty_rows:
        new_title = clean_text(row["product_title"])
        new_brand = clean_brand(row["brand"])
        if new_title != row["product_title"] or new_brand != row["brand"]:
            updates.append((new_title, new_brand, row["id"]))

    if updates:
        with connection.cursor() as cur:
            cur.executemany(
                "UPDATE gurysk_dwh.dwh_amazon_product_snapshot "
                "SET product_title = %s, brand = %s WHERE id = %s",
                updates,
            )
        connection.commit()

    logger.info("clean_dwh_data: %d rows cleaned.", len(updates))
    return len(updates)


def build_outin_daily_sales(connection: pymysql.Connection) -> int:
    """Aggregate DWH snapshots into dmt_outin_daily_sales (one row per asin/date)."""
    df = pd.read_sql(
        f"SELECT * FROM gurysk_dwh.dwh_amazon_product_snapshot WHERE {_OUTIN_FILTER}",
        connection,
    )
    if df.empty:
        logger.info("build_outin_daily_sales: no OutIn data in DWH.")
        return 0

    df = _apply_text_cleaning(df)
    df = df.sort_values("observed_at")

    day_end = (
        df.groupby(["asin", "observed_date", "marketplace"])
        .last()
        .reset_index()
    )

    agg = (
        df.groupby(["asin", "observed_date", "marketplace"])
        .agg(observation_count=("id", "count"))
        .reset_index()
    )

    result = day_end[
        ["asin", "observed_date", "marketplace", "product_title",
         "price", "sales_volume_num", "num_ratings", "star_rating"]
    ].merge(agg, on=["asin", "observed_date", "marketplace"])

    result = result.sort_values(["asin", "marketplace", "observed_date"])

    result["sales_volume_dod_change"] = (
        result.groupby(["asin", "marketplace"])["sales_volume_num"].diff()
    )
    result["num_ratings_dod_change"] = (
        result.groupby(["asin", "marketplace"])["num_ratings"].diff()
    )

    result = result.dropna(subset=["sales_volume_num"])
    result = result[result["sales_volume_num"] > 0]

    _upsert_dmt_rows(
        connection,
        table="gurysk_dmt.dmt_outin_daily_sales",
        columns=[
            "observed_date", "marketplace", "asin", "product_title",
            "price", "sales_volume_num", "num_ratings", "star_rating",
            "sales_volume_dod_change", "num_ratings_dod_change",
            "observation_count",
        ],
        df=result,
    )

    count = len(result)
    logger.info("build_outin_daily_sales: %d rows upserted.", count)
    return count


# ---------------------------------------------------------------------------
# ETL Step 2a: DWH -> DMT  (OutIn monthly sales)
# ---------------------------------------------------------------------------


def build_outin_monthly_sales(connection: pymysql.Connection) -> int:
    """Aggregate DWH snapshots into dmt_outin_monthly_sales."""
    df = pd.read_sql(
        f"SELECT * FROM gurysk_dwh.dwh_amazon_product_snapshot WHERE {_OUTIN_FILTER}",
        connection,
    )
    if df.empty:
        logger.info("build_outin_monthly_sales: no OutIn data in DWH.")
        return 0

    df = _apply_text_cleaning(df)
    df = df.sort_values("observed_at")

    month_end = (
        df.sort_values("observed_at")
        .groupby(["asin", "observed_month", "marketplace"])
        .last()
        .reset_index()
    )

    valid_prices = df.dropna(subset=["price"])
    agg = (
        df.groupby(["asin", "observed_month", "marketplace"])
        .agg(
            observation_count=("id", "count"),
            first_observed_at=("observed_at", "min"),
            last_observed_at=("observed_at", "max"),
        )
        .reset_index()
    )
    avg_price_agg = (
        valid_prices.groupby(["asin", "observed_month", "marketplace"])
        .agg(avg_price=("price", "mean"))
        .reset_index()
    )
    agg = agg.merge(avg_price_agg, on=["asin", "observed_month", "marketplace"], how="left")

    result = month_end[
        ["asin", "observed_month", "marketplace", "product_title",
         "price", "sales_volume_num", "num_ratings"]
    ].rename(columns={
        "price": "month_end_price",
        "sales_volume_num": "month_end_sales_volume",
        "num_ratings": "month_end_num_ratings",
    })

    result = result.merge(agg, on=["asin", "observed_month", "marketplace"])
    result = result.sort_values(["asin", "marketplace", "observed_month"])

    result["sales_volume_mom_change"] = (
        result.groupby(["asin", "marketplace"])["month_end_sales_volume"]
        .diff()
    )
    result["num_ratings_mom_delta"] = (
        result.groupby(["asin", "marketplace"])["month_end_num_ratings"]
        .diff()
    )

    _upsert_dmt_rows(
        connection,
        table="gurysk_dmt.dmt_outin_monthly_sales",
        columns=[
            "observed_month", "marketplace", "asin", "product_title",
            "month_end_price", "month_end_sales_volume", "month_end_num_ratings",
            "sales_volume_mom_change", "num_ratings_mom_delta",
            "avg_price", "observation_count", "first_observed_at", "last_observed_at",
        ],
        df=result,
    )

    count = len(result)
    logger.info("build_outin_monthly_sales: %d rows upserted.", count)
    return count


# ---------------------------------------------------------------------------
# ETL Step 2b: DWH -> DMT  (OutIn review trend)
# ---------------------------------------------------------------------------

def build_outin_review_trend(connection: pymysql.Connection) -> int:
    """Aggregate DWH snapshots into dmt_outin_review_trend."""
    df = pd.read_sql(
        f"SELECT * FROM gurysk_dwh.dwh_amazon_product_snapshot WHERE {_OUTIN_FILTER}",
        connection,
    )
    if df.empty:
        logger.info("build_outin_review_trend: no OutIn data in DWH.")
        return 0

    df = _apply_text_cleaning(df)
    df = df.sort_values("observed_at")

    month_end = (
        df.sort_values("observed_at")
        .groupby(["asin", "observed_month", "marketplace"])
        .last()
        .reset_index()
    )[["asin", "observed_month", "marketplace", "product_title",
       "star_rating", "num_ratings", "num_reviews"]].rename(columns={
        "star_rating": "month_end_star_rating",
        "num_ratings": "month_end_num_ratings",
        "num_reviews": "month_end_num_reviews",
    })

    valid_ratings = df.dropna(subset=["star_rating"])
    agg = (
        df.groupby(["asin", "observed_month", "marketplace"])
        .agg(
            observation_count=("id", "count"),
            last_observed_at=("observed_at", "max"),
        )
        .reset_index()
    )
    rating_agg = (
        valid_ratings.groupby(["asin", "observed_month", "marketplace"])
        .agg(
            avg_star_rating=("star_rating", "mean"),
            min_star_rating=("star_rating", "min"),
            max_star_rating=("star_rating", "max"),
        )
        .reset_index()
    )
    agg = agg.merge(rating_agg, on=["asin", "observed_month", "marketplace"], how="left")

    result = month_end.merge(agg, on=["asin", "observed_month", "marketplace"])
    result = result.sort_values(["asin", "marketplace", "observed_month"])

    result["new_ratings_this_month"] = (
        result.groupby(["asin", "marketplace"])["month_end_num_ratings"].diff()
    )
    result["star_rating_mom_change"] = (
        result.groupby(["asin", "marketplace"])["month_end_star_rating"].diff()
    )

    _upsert_dmt_rows(
        connection,
        table="gurysk_dmt.dmt_outin_review_trend",
        columns=[
            "observed_month", "marketplace", "asin", "product_title",
            "month_end_star_rating", "month_end_num_ratings", "month_end_num_reviews",
            "new_ratings_this_month", "star_rating_mom_change",
            "avg_star_rating", "min_star_rating", "max_star_rating",
            "observation_count", "last_observed_at",
        ],
        df=result,
    )

    count = len(result)
    logger.info("build_outin_review_trend: %d rows upserted.", count)
    return count


# ---------------------------------------------------------------------------
# ETL Step 2c: DWH -> DMT  (Coffee market share)
# ---------------------------------------------------------------------------

_COFFEE_QUERY_FILTER = (
    "LOWER(COALESCE(search_query,'')) LIKE '%%coffee%%' "
    "OR LOWER(COALESCE(search_query,'')) LIKE '%%espresso%%' "
    "OR LOWER(COALESCE(search_query,'')) LIKE '%%咖啡%%'"
)


def build_coffee_market_share(connection: pymysql.Connection) -> int:
    """Aggregate DWH snapshots into dmt_coffee_market_share."""
    df = pd.read_sql(
        "SELECT * FROM gurysk_dwh.dwh_amazon_product_snapshot "
        f"WHERE {_COFFEE_QUERY_FILTER}",
        connection,
    )
    if df.empty:
        logger.info("build_coffee_market_share: no coffee data in DWH.")
        return 0

    df = _apply_text_cleaning(df)
    df = df.sort_values("observed_at")
    df["brand"] = df["brand"].fillna("Unknown")

    deduped = (
        df.sort_values("observed_at")
        .groupby(["asin", "observed_month", "marketplace"])
        .last()
        .reset_index()
    )

    brand_agg = (
        deduped.groupby(["observed_month", "marketplace", "brand"])
        .agg(
            distinct_asins=("asin", "nunique"),
            total_num_ratings=("num_ratings", "sum"),
            total_sales_volume=("sales_volume_num", "sum"),
            avg_price=("price", "mean"),
            avg_star_rating=("star_rating", "mean"),
            last_observed_at=("observed_at", "max"),
        )
        .reset_index()
    )

    totals = (
        brand_agg.groupby(["observed_month", "marketplace"])
        .agg(
            market_total_ratings=("total_num_ratings", "sum"),
            market_total_sales=("total_sales_volume", "sum"),
            market_total_asins=("distinct_asins", "sum"),
        )
        .reset_index()
    )

    result = brand_agg.merge(totals, on=["observed_month", "marketplace"])

    result["rating_share_pct"] = result.apply(
        lambda r: round(r["total_num_ratings"] * 100.0 / r["market_total_ratings"], 2)
        if r["market_total_ratings"] > 0 else 0,
        axis=1,
    )
    result["sales_share_pct"] = result.apply(
        lambda r: round(r["total_sales_volume"] * 100.0 / r["market_total_sales"], 2)
        if r["market_total_sales"] > 0 else 0,
        axis=1,
    )
    result["asin_share_pct"] = result.apply(
        lambda r: round(r["distinct_asins"] * 100.0 / r["market_total_asins"], 2)
        if r["market_total_asins"] > 0 else 0,
        axis=1,
    )

    _upsert_dmt_rows(
        connection,
        table="gurysk_dmt.dmt_coffee_market_share",
        columns=[
            "observed_month", "marketplace", "brand",
            "distinct_asins", "total_num_ratings", "total_sales_volume",
            "avg_price", "avg_star_rating",
            "rating_share_pct", "sales_share_pct", "asin_share_pct",
            "last_observed_at",
        ],
        df=result,
    )

    count = len(result)
    logger.info("build_coffee_market_share: %d rows upserted.", count)
    return count


# ---------------------------------------------------------------------------
# Shared UPSERT helper
# ---------------------------------------------------------------------------

def _upsert_dmt_rows(
    connection: pymysql.Connection,
    table: str,
    columns: List[str],
    df: pd.DataFrame,
) -> None:
    """INSERT ... ON DUPLICATE KEY UPDATE from a DataFrame."""
    if df.empty:
        return

    placeholders = ", ".join(["%s"] * len(columns))
    col_list = ", ".join(columns)
    uk_fields = {
        "observed_month", "observed_date", "estimate_date", "alert_date",
        "marketplace", "asin", "brand", "category_id", "segment_name",
        "dimension_type", "dimension_value", "metric_name",
    }
    update_clause = ", ".join(
        f"{c} = VALUES({c})" for c in columns
        if c not in uk_fields
    )
    update_clause += ", etl_loaded_at = CURRENT_TIMESTAMP"

    sql = (
        f"INSERT INTO {table} ({col_list}) "
        f"VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {update_clause}"
    )

    rows_to_insert = []
    for _, row in df.iterrows():
        vals = []
        for c in columns:
            v = row.get(c)
            try:
                if v is None or v is pd.NaT or pd.isna(v):
                    vals.append(None)
                    continue
            except (ValueError, TypeError):
                pass
            if isinstance(v, pd.Timestamp):
                vals.append(v.to_pydatetime())
            elif isinstance(v, (np.integer,)):
                vals.append(int(v))
            elif isinstance(v, (np.floating,)):
                vals.append(float(v))
            else:
                vals.append(v)
        rows_to_insert.append(tuple(vals))

    with connection.cursor() as cur:
        cur.executemany(sql, rows_to_insert)
    connection.commit()


# =========================================================================
# GENERIC PIPELINE: any brand / category (parameter-driven)
# =========================================================================

# ---------------------------------------------------------------------------
# Extract reviews and offers to new DWH tables
# ---------------------------------------------------------------------------

_DWH_REVIEW_INSERT_SQL = """
    INSERT IGNORE INTO gurysk_dwh.dwh_amazon_review_snapshot (
        src_id, observed_at, observed_date, asin, marketplace,
        review_id, review_title, review_comment, review_star_rating,
        review_date, is_verified_purchase, helpful_count
    ) VALUES (
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s
    )
"""

_DWH_OFFER_INSERT_SQL = """
    INSERT IGNORE INTO gurysk_dwh.dwh_amazon_offer_snapshot (
        src_id, observed_at, observed_date, asin, marketplace,
        offer_price, original_price, discount_pct, product_condition, is_prime
    ) VALUES (
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s
    )
"""


def extract_reviews_to_dwh(
    connection: pymysql.Connection,
    batch_size: int = 500,
) -> int:
    """Extract review SRC rows into dwh_amazon_review_snapshot."""
    ensure_all_tables(connection)

    with connection.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(
            "SELECT COALESCE(MAX(src_id), 0) AS max_id "
            "FROM gurysk_dwh.dwh_amazon_review_snapshot"
        )
        max_src_id = cur.fetchone()["max_id"]

        cur.execute(
            "SELECT id, asin, record_create_timestamp, marketplace_country, api_payload "
            "FROM gurysk_src.amazon_product_raw "
            "WHERE source_endpoint = 'top_product_reviews' AND id > %s ORDER BY id",
            (max_src_id,),
        )
        src_rows = cur.fetchall()

    if not src_rows:
        logger.info("extract_reviews_to_dwh: no new review rows.")
        return 0

    params_batch: List[Tuple] = []
    for row in src_rows:
        payload = row["api_payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        if not isinstance(payload, dict):
            continue

        observed_at = row["record_create_timestamp"]
        params_batch.append((
            row["id"],
            observed_at,
            _to_observed_date(observed_at),
            row["asin"],
            row["marketplace_country"],
            payload.get("review_id"),
            clean_text(payload.get("review_title")),
            clean_text(payload.get("review_comment")),
            _safe_float(payload.get("review_star_rating")),
            payload.get("review_date"),
            _bool_to_tinyint(payload.get("is_verified_purchase")),
            _safe_int(payload.get("helpful_count")),
        ))

        if len(params_batch) >= batch_size:
            with connection.cursor() as cur:
                cur.executemany(_DWH_REVIEW_INSERT_SQL, params_batch)
            connection.commit()
            params_batch.clear()

    if params_batch:
        with connection.cursor() as cur:
            cur.executemany(_DWH_REVIEW_INSERT_SQL, params_batch)
        connection.commit()

    total = len(src_rows)
    logger.info("extract_reviews_to_dwh: %d rows processed.", total)
    return total


def extract_offers_to_dwh(
    connection: pymysql.Connection,
    batch_size: int = 500,
) -> int:
    """Extract offer/pricing SRC rows into dwh_amazon_offer_snapshot."""
    ensure_all_tables(connection)

    with connection.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(
            "SELECT COALESCE(MAX(src_id), 0) AS max_id "
            "FROM gurysk_dwh.dwh_amazon_offer_snapshot"
        )
        max_src_id = cur.fetchone()["max_id"]

        cur.execute(
            "SELECT id, asin, record_create_timestamp, marketplace_country, api_payload "
            "FROM gurysk_src.amazon_product_raw "
            "WHERE source_endpoint = 'product_offers' AND id > %s ORDER BY id",
            (max_src_id,),
        )
        src_rows = cur.fetchall()

    if not src_rows:
        logger.info("extract_offers_to_dwh: no new offer rows.")
        return 0

    params_batch: List[Tuple] = []
    for row in src_rows:
        payload = row["api_payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        if not isinstance(payload, dict):
            continue

        observed_at = row["record_create_timestamp"]
        offers = payload.get("offers", [payload])
        if not isinstance(offers, list):
            offers = [offers]

        for offer in offers:
            if not isinstance(offer, dict):
                continue
            offer_price = _safe_float(offer.get("product_price") or offer.get("price"))
            orig_price = _safe_float(offer.get("product_original_price") or offer.get("list_price"))
            disc_pct = None
            if offer_price and orig_price and orig_price > 0:
                disc_pct = round((1 - offer_price / orig_price) * 100, 2)

            params_batch.append((
                row["id"],
                observed_at,
                _to_observed_date(observed_at),
                row["asin"],
                row["marketplace_country"],
                offer_price,
                orig_price,
                disc_pct,
                offer.get("product_condition"),
                _bool_to_tinyint(offer.get("is_prime")),
            ))

        if len(params_batch) >= batch_size:
            with connection.cursor() as cur:
                cur.executemany(_DWH_OFFER_INSERT_SQL, params_batch)
            connection.commit()
            params_batch.clear()

    if params_batch:
        with connection.cursor() as cur:
            cur.executemany(_DWH_OFFER_INSERT_SQL, params_batch)
        connection.commit()

    total = len(src_rows)
    logger.info("extract_offers_to_dwh: %d rows processed.", total)
    return total


def extract_asin_hierarchy(
    connection: pymysql.Connection,
) -> int:
    """Parse product_details payloads for variation data and upsert into dwh_asin_hierarchy."""
    ensure_all_tables(connection)

    with connection.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(
            "SELECT COALESCE(MAX(last_seen), '2000-01-01') AS cutoff "
            "FROM gurysk_dwh.dwh_asin_hierarchy"
        )
        cutoff = cur.fetchone()["cutoff"]

        cur.execute(
            "SELECT asin, marketplace_country, api_payload, record_create_timestamp "
            "FROM gurysk_src.amazon_product_raw "
            "WHERE source_endpoint = 'product_details' "
            "  AND record_create_timestamp >= %s "
            "ORDER BY record_create_timestamp",
            (cutoff,),
        )
        src_rows = cur.fetchall()

    if not src_rows:
        logger.info("extract_asin_hierarchy: no new data.")
        return 0

    upsert_sql = """
        INSERT INTO gurysk_dwh.dwh_asin_hierarchy
            (parent_asin, child_asin, marketplace, variation_type, variation_value, first_seen, last_seen)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            variation_value = VALUES(variation_value),
            last_seen = VALUES(last_seen)
    """

    count = 0
    for row in src_rows:
        payload = row["api_payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        inner = payload.get("data", payload) if isinstance(payload, dict) else payload
        if not isinstance(inner, dict):
            continue

        parent_asin = row["asin"]
        marketplace = row["marketplace_country"]
        obs_date = _to_observed_date(row["record_create_timestamp"])
        variations = inner.get("product_variations", inner.get("variations", []))
        if not isinstance(variations, list):
            continue

        for var in variations:
            if not isinstance(var, dict):
                continue
            child = var.get("asin")
            if not child or child == parent_asin:
                continue
            var_dimensions = var.get("dimensions", {})
            if isinstance(var_dimensions, dict):
                for vtype, vval in var_dimensions.items():
                    with connection.cursor() as cur:
                        cur.execute(upsert_sql, (
                            parent_asin, child, marketplace, str(vtype)[:64],
                            str(vval)[:256], obs_date, obs_date,
                        ))
                    count += 1
            else:
                with connection.cursor() as cur:
                    cur.execute(upsert_sql, (
                        parent_asin, child, marketplace, None, None, obs_date, obs_date,
                    ))
                count += 1

    connection.commit()
    logger.info("extract_asin_hierarchy: %d relationships upserted.", count)
    return count


# ---------------------------------------------------------------------------
# Generic DMT: daily product metrics (any brand/category)
# ---------------------------------------------------------------------------

def build_product_daily_metrics(
    connection: pymysql.Connection,
    brand_filter: Optional[str] = None,
    category_filter: Optional[str] = None,
) -> int:
    """
    Build dmt_product_daily_metrics from DWH snapshots.

    If brand_filter is given, only process products matching that brand.
    If category_filter is given, only process search_query containing category:xxx.
    If both are None, processes ALL products.
    """
    where_parts = ["1=1"]
    if brand_filter:
        where_parts.append(
            f"LOWER(COALESCE(brand,'')) LIKE '%%{brand_filter.lower()}%%'"
        )
    if category_filter:
        where_parts.append(
            f"LOWER(COALESCE(search_query,'')) LIKE '%%{category_filter.lower()}%%'"
        )

    where_clause = " AND ".join(where_parts)
    df = pd.read_sql(
        f"SELECT * FROM gurysk_dwh.dwh_amazon_product_snapshot WHERE {where_clause}",
        connection,
    )
    if df.empty:
        logger.info("build_product_daily_metrics: no matching data.")
        return 0

    df = _apply_text_cleaning(df)
    df = df.sort_values("observed_at")

    # Extract category_id from search_query like "category:12345"
    df["category_id"] = df["search_query"].apply(_extract_category_id)

    day_end = (
        df.groupby(["asin", "observed_date", "marketplace"])
        .last()
        .reset_index()
    )

    agg = (
        df.groupby(["asin", "observed_date", "marketplace"])
        .agg(observation_count=("id", "count"))
        .reset_index()
    )

    # Compute discount_pct from offers if available
    offer_df = pd.read_sql(
        "SELECT asin, observed_date, marketplace, "
        "  AVG(offer_price) as avg_offer_price, "
        "  AVG(original_price) as avg_original_price, "
        "  AVG(discount_pct) as avg_discount_pct "
        "FROM gurysk_dwh.dwh_amazon_offer_snapshot "
        "GROUP BY asin, observed_date, marketplace",
        connection,
    )

    result = day_end[
        ["asin", "observed_date", "marketplace", "brand", "product_title",
         "category_id", "category_name", "price", "sales_volume_raw", "sales_volume_num",
         "star_rating", "num_ratings", "num_reviews", "is_best_seller"]
    ].merge(agg, on=["asin", "observed_date", "marketplace"])

    if not offer_df.empty:
        result = result.merge(
            offer_df[["asin", "observed_date", "marketplace",
                       "avg_original_price", "avg_discount_pct"]],
            on=["asin", "observed_date", "marketplace"],
            how="left",
        )
        result.rename(columns={
            "avg_original_price": "original_price",
            "avg_discount_pct": "discount_pct",
        }, inplace=True)
    else:
        result["original_price"] = None
        result["discount_pct"] = None

    # Look up parent_asin
    try:
        hierarchy_df = pd.read_sql(
            "SELECT child_asin, parent_asin FROM gurysk_dwh.dwh_asin_hierarchy",
            connection,
        )
        result = result.merge(
            hierarchy_df, left_on="asin", right_on="child_asin", how="left",
        )
        result.drop(columns=["child_asin"], inplace=True, errors="ignore")
    except Exception:
        result["parent_asin"] = None

    result = result.sort_values(["asin", "marketplace", "observed_date"])

    for col, delta_col in [
        ("price", "price_dod_change"),
        ("discount_pct", "discount_pct_dod_change"),
        ("sales_volume_num", "sales_volume_dod_change"),
        ("num_ratings", "num_ratings_dod_change"),
    ]:
        if col in result.columns:
            result[delta_col] = result.groupby(["asin", "marketplace"])[col].diff()

    result["bsr_rank"] = None
    result["bsr_rank_dod_change"] = None

    _upsert_dmt_rows(
        connection,
        table="gurysk_dmt.dmt_product_daily_metrics",
        columns=[
            "observed_date", "marketplace", "asin", "parent_asin", "brand",
            "product_title", "category_id", "category_name",
            "price", "original_price", "discount_pct",
            "sales_volume_raw", "sales_volume_num",
            "star_rating", "num_ratings", "num_reviews",
            "bsr_rank", "is_best_seller",
            "price_dod_change", "discount_pct_dod_change",
            "sales_volume_dod_change", "num_ratings_dod_change",
            "bsr_rank_dod_change",
            "observation_count",
        ],
        df=result,
    )

    count = len(result)
    logger.info("build_product_daily_metrics: %d rows upserted.", count)
    return count


def build_segment_product_daily_metrics(
    connection: pymysql.Connection,
    segment_name: Optional[str] = None,
) -> int:
    where_parts = ["segment_name IS NOT NULL", "TRIM(segment_name) <> ''"]
    if segment_name:
        safe_name = segment_name.replace("'", "''")
        where_parts.append(f"LOWER(segment_name) = '{safe_name.lower()}'")

    where_clause = " AND ".join(where_parts)
    df = pd.read_sql(
        f"SELECT * FROM gurysk_dwh.dwh_amazon_product_snapshot WHERE {where_clause}",
        connection,
    )
    if df.empty:
        logger.info("build_segment_product_daily_metrics: no matching data.")
        return 0

    df = _apply_text_cleaning(df)
    df = df.sort_values("observed_at")

    day_end = (
        df.groupby(["asin", "observed_date", "marketplace", "segment_name"])
        .last()
        .reset_index()
    )

    agg = (
        df.groupby(["asin", "observed_date", "marketplace", "segment_name"])
        .agg(observation_count=("id", "count"))
        .reset_index()
    )

    offer_df = pd.read_sql(
        "SELECT asin, observed_date, marketplace, "
        "  AVG(offer_price) as avg_offer_price, "
        "  AVG(original_price) as avg_original_price, "
        "  AVG(discount_pct) as avg_discount_pct "
        "FROM gurysk_dwh.dwh_amazon_offer_snapshot "
        "GROUP BY asin, observed_date, marketplace",
        connection,
    )

    result = day_end[
        ["asin", "observed_date", "marketplace", "segment_name", "segment_keyword",
         "brand", "product_title", "price", "sales_volume_raw", "sales_volume_num",
         "star_rating", "num_ratings", "num_reviews", "is_best_seller"]
    ].merge(agg, on=["asin", "observed_date", "marketplace", "segment_name"])

    if not offer_df.empty:
        result = result.merge(
            offer_df[["asin", "observed_date", "marketplace",
                      "avg_original_price", "avg_discount_pct"]],
            on=["asin", "observed_date", "marketplace"],
            how="left",
        )
        result.rename(columns={
            "avg_original_price": "original_price",
            "avg_discount_pct": "discount_pct",
        }, inplace=True)
    else:
        result["original_price"] = None
        result["discount_pct"] = None

    try:
        hierarchy_df = pd.read_sql(
            "SELECT child_asin, parent_asin FROM gurysk_dwh.dwh_asin_hierarchy",
            connection,
        )
        result = result.merge(
            hierarchy_df, left_on="asin", right_on="child_asin", how="left",
        )
        result.drop(columns=["child_asin"], inplace=True, errors="ignore")
    except Exception:
        result["parent_asin"] = None

    result = result.sort_values(["segment_name", "asin", "marketplace", "observed_date"])
    for col, delta_col in [
        ("price", "price_dod_change"),
        ("discount_pct", "discount_pct_dod_change"),
        ("sales_volume_num", "sales_volume_dod_change"),
        ("num_ratings", "num_ratings_dod_change"),
    ]:
        if col in result.columns:
            result[delta_col] = result.groupby(["segment_name", "asin", "marketplace"])[col].diff()

    result["bsr_rank"] = None
    result["bsr_rank_dod_change"] = None

    _upsert_dmt_rows(
        connection,
        table="gurysk_dmt.dmt_segment_product_daily_metrics",
        columns=[
            "observed_date", "marketplace", "segment_name", "segment_keyword",
            "asin", "parent_asin", "brand", "product_title",
            "price", "original_price", "discount_pct",
            "sales_volume_raw", "sales_volume_num",
            "star_rating", "num_ratings", "num_reviews",
            "bsr_rank", "is_best_seller",
            "price_dod_change", "discount_pct_dod_change",
            "sales_volume_dod_change", "num_ratings_dod_change",
            "bsr_rank_dod_change", "observation_count",
        ],
        df=result,
    )

    count = len(result)
    logger.info("build_segment_product_daily_metrics: %d rows upserted.", count)
    return count


def _extract_category_id(search_query: Optional[str]) -> Optional[str]:
    if not search_query:
        return None
    if search_query.startswith("category:"):
        return search_query.split(":", 1)[1].strip()
    if search_query.startswith("bestseller:"):
        return search_query.split(":", 1)[1].strip()
    return None


def _compute_jaccard_similarity(current_asins: set, previous_asins: set) -> Optional[float]:
    if not current_asins or not previous_asins:
        return None
    union = current_asins | previous_asins
    if not union:
        return None
    return len(current_asins & previous_asins) / len(union)


def _bootstrap_scaled_total_sales(
    sales_values: np.ndarray,
    estimated_market_asin_count: int,
    iterations: int = 300,
) -> Tuple[float, float, float]:
    if sales_values.size == 0:
        return 0.0, 0.0, 0.0

    sample_count = len(sales_values)
    scale = max(float(estimated_market_asin_count) / max(sample_count, 1), 1.0)
    samples = np.random.choice(sales_values, size=(iterations, sample_count), replace=True)
    totals = samples.sum(axis=1) * scale
    return (
        float(np.mean(totals)),
        float(np.percentile(totals, 2.5)),
        float(np.percentile(totals, 97.5)),
    )


# ---------------------------------------------------------------------------
# Generic DMT: Bayesian daily sales estimation
# ---------------------------------------------------------------------------

def build_daily_sales_estimates(
    connection: pymysql.Connection,
    brand_filter: Optional[str] = None,
    category_hint: Optional[str] = None,
) -> int:
    """
    Run Bayesian daily sales estimation on dmt_product_daily_metrics
    and write results to dmt_daily_sales_estimate.
    """
    where_parts = ["1=1"]
    if brand_filter:
        where_parts.append(
            f"LOWER(COALESCE(brand,'')) LIKE '%%{brand_filter.lower()}%%'"
        )
    where_clause = " AND ".join(where_parts)

    df = pd.read_sql(
        f"SELECT * FROM gurysk_dmt.dmt_product_daily_metrics WHERE {where_clause}",
        connection,
    )
    if df.empty:
        logger.info("build_daily_sales_estimates: no data.")
        return 0

    df["num_ratings_delta"] = df.get("num_ratings_dod_change")

    est_df = estimate_daily_sales_batch(df, category_hint=category_hint)

    result = est_df[[
        "observed_date", "marketplace", "asin", "brand", "category_id", "category_name",
        "sales_volume_num", "bsr_rank", "num_ratings", "num_ratings_delta",
        "price", "star_rating",
        "estimated_daily_sales", "estimate_lower_bound", "estimate_upper_bound",
        "estimation_method", "confidence_score",
    ]].copy()
    result.rename(columns={"observed_date": "estimate_date"}, inplace=True)

    _upsert_dmt_rows(
        connection,
        table="gurysk_dmt.dmt_daily_sales_estimate",
        columns=[
            "estimate_date", "marketplace", "asin", "brand", "category_id", "category_name",
            "sales_volume_num", "bsr_rank", "num_ratings", "num_ratings_delta",
            "price", "star_rating",
            "estimated_daily_sales", "estimate_lower_bound", "estimate_upper_bound",
            "estimation_method", "confidence_score",
        ],
        df=result,
    )

    count = len(result)
    logger.info("build_daily_sales_estimates: %d rows upserted.", count)
    return count


# ---------------------------------------------------------------------------
# Generic DMT: brand market share
# ---------------------------------------------------------------------------

def build_brand_market_share(
    connection: pymysql.Connection,
    category_id: str,
    target_date: Optional[str] = None,
) -> int:
    """
    Compute market share for all brands within a category on a given date.

    Uses dmt_daily_sales_estimate if available, falling back to
    dmt_product_daily_metrics.sales_volume_num / 30.
    """
    if not target_date:
        with connection.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(
                "SELECT MAX(observed_date) AS d FROM gurysk_dmt.dmt_product_daily_metrics "
                "WHERE category_id = %s",
                (category_id,),
            )
            row = cur.fetchone()
            target_date = str(row["d"]) if row and row["d"] else None
    if not target_date:
        logger.info("build_brand_market_share: no data for category %s.", category_id)
        return 0

    sql = """
        SELECT m.observed_date, m.marketplace, m.asin, m.brand,
               m.category_name,
               m.num_ratings, m.sales_volume_num, m.price, m.star_rating,
               m.category_id,
               COALESCE(e.estimated_daily_sales, ROUND(m.sales_volume_num / 30)) AS daily_sales
        FROM gurysk_dmt.dmt_product_daily_metrics m
        LEFT JOIN gurysk_dmt.dmt_daily_sales_estimate e
            ON m.asin = e.asin AND m.observed_date = e.estimate_date
               AND m.marketplace = e.marketplace
        WHERE m.category_id = %s AND m.observed_date = %s
    """
    df = pd.read_sql(sql, connection, params=(category_id, target_date))
    if df.empty:
        logger.info("build_brand_market_share: no data for %s on %s.", category_id, target_date)
        return 0

    df["brand"] = df["brand"].fillna("Unknown")
    df["daily_sales"] = pd.to_numeric(df["daily_sales"], errors="coerce").fillna(0)
    category_name = None
    non_null_category_names = df["category_name"].dropna()
    if not non_null_category_names.empty:
        category_name = non_null_category_names.iloc[0]

    brand_agg = (
        df.groupby(["observed_date", "marketplace", "brand"])
        .agg(
            distinct_asins=("asin", "nunique"),
            total_estimated_daily_sales=("daily_sales", "sum"),
            total_num_ratings=("num_ratings", "sum"),
            total_sales_volume=("sales_volume_num", "sum"),
            avg_price=("price", "mean"),
            avg_star_rating=("star_rating", "mean"),
        )
        .reset_index()
    )

    totals = (
        brand_agg.groupby(["observed_date", "marketplace"])
        .agg(
            market_sales=("total_estimated_daily_sales", "sum"),
            market_ratings=("total_num_ratings", "sum"),
            market_asins=("distinct_asins", "sum"),
        )
        .reset_index()
    )

    result = brand_agg.merge(totals, on=["observed_date", "marketplace"])

    for pct_col, num_col, denom_col in [
        ("sales_share_pct", "total_estimated_daily_sales", "market_sales"),
        ("rating_share_pct", "total_num_ratings", "market_ratings"),
        ("asin_share_pct", "distinct_asins", "market_asins"),
    ]:
        result[pct_col] = result.apply(
            lambda r: round(r[num_col] * 100.0 / r[denom_col], 2)
            if r[denom_col] and r[denom_col] > 0 else 0,
            axis=1,
        )

    result["category_id"] = category_id
    result["category_name"] = category_name
    result["last_observed_at"] = datetime.utcnow()

    _upsert_dmt_rows(
        connection,
        table="gurysk_dmt.dmt_brand_market_share",
        columns=[
            "observed_date", "marketplace", "category_id", "category_name", "brand",
            "distinct_asins", "total_estimated_daily_sales",
            "total_num_ratings", "total_sales_volume",
            "avg_price", "avg_star_rating",
            "sales_share_pct", "rating_share_pct", "asin_share_pct",
            "last_observed_at",
        ],
        df=result,
    )

    count = len(result)
    logger.info("build_brand_market_share: %d rows for category %s.", count, category_id)
    return count


def build_segment_market_share(
    connection: pymysql.Connection,
    segment_name: str,
    target_date: Optional[str] = None,
) -> int:
    if not target_date:
        with connection.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(
                "SELECT MAX(observed_date) AS d FROM gurysk_dmt.dmt_segment_product_daily_metrics "
                "WHERE segment_name = %s",
                (segment_name,),
            )
            row = cur.fetchone()
            target_date = str(row["d"]) if row and row["d"] else None
    if not target_date:
        logger.info("build_segment_market_share: no data for segment %s.", segment_name)
        return 0

    sql = """
        SELECT m.observed_date, m.marketplace, m.segment_name, m.segment_keyword,
               m.asin, m.parent_asin, m.brand, m.product_title,
               m.num_ratings, m.sales_volume_num, m.price, m.star_rating,
               COALESCE(e.estimated_daily_sales, ROUND(m.sales_volume_num / 30)) AS daily_sales
        FROM gurysk_dmt.dmt_segment_product_daily_metrics m
        LEFT JOIN gurysk_dmt.dmt_daily_sales_estimate e
            ON m.asin = e.asin AND m.observed_date = e.estimate_date
               AND m.marketplace = e.marketplace
        WHERE m.segment_name = %s AND m.observed_date = %s
    """
    df = pd.read_sql(sql, connection, params=(segment_name, target_date))
    if df.empty:
        logger.info("build_segment_market_share: no data for %s on %s.", segment_name, target_date)
        return 0

    history_sql = """
        SELECT observed_date, marketplace, segment_name, COUNT(DISTINCT asin) AS asin_count
        FROM gurysk_dmt.dmt_segment_product_daily_metrics
        WHERE segment_name = %s
          AND observed_date >= DATE_SUB(%s, INTERVAL 30 DAY)
          AND observed_date <= %s
        GROUP BY observed_date, marketplace, segment_name
        ORDER BY observed_date
    """
    history_df = pd.read_sql(sql=history_sql, con=connection, params=(segment_name, target_date, target_date))

    share_rows: List[Dict[str, Any]] = []
    stats_rows: List[Dict[str, Any]] = []

    df["brand"] = df["brand"].fillna("Unknown")
    df["daily_sales"] = pd.to_numeric(df["daily_sales"], errors="coerce").fillna(0.0)

    for marketplace, group in df.groupby("marketplace"):
        group = group.copy()
        sample_asin_count = int(group["asin"].nunique())

        mp_history = history_df[history_df["marketplace"] == marketplace].copy()
        estimated_market_asin_count = sample_asin_count
        if not mp_history.empty:
            estimated_market_asin_count = int(max(mp_history["asin_count"].max(), sample_asin_count))

        coverage_ratio = 1.0 if estimated_market_asin_count <= 0 else min(
            sample_asin_count / float(estimated_market_asin_count), 1.0
        )

        previous_dates = sorted(d for d in mp_history["observed_date"].tolist() if str(d) < str(target_date))
        previous_asins: set = set()
        if previous_dates:
            prev_date = previous_dates[-1]
            with connection.cursor(pymysql.cursors.DictCursor) as cur:
                cur.execute(
                    "SELECT DISTINCT asin FROM gurysk_dmt.dmt_segment_product_daily_metrics "
                    "WHERE segment_name = %s AND marketplace = %s AND observed_date = %s",
                    (segment_name, marketplace, prev_date),
                )
                previous_asins = {r["asin"] for r in cur.fetchall()}
        current_asins = set(group["asin"].dropna().tolist())
        jaccard = _compute_jaccard_similarity(current_asins, previous_asins)
        stability_score = coverage_ratio if jaccard is None else (coverage_ratio + jaccard) / 2.0

        sales_values = group["daily_sales"].to_numpy(dtype=float)
        sample_sales_sum = float(group["daily_sales"].sum())
        bootstrap_mean_sales, bootstrap_lower_bound, bootstrap_upper_bound = _bootstrap_scaled_total_sales(
            sales_values=sales_values,
            estimated_market_asin_count=estimated_market_asin_count,
        )

        prior_sql = """
            SELECT m.brand AS brand,
                   SUM(COALESCE(e.estimated_daily_sales, ROUND(m.sales_volume_num / 30))) AS total_daily_sales
            FROM gurysk_dmt.dmt_segment_product_daily_metrics m
            LEFT JOIN gurysk_dmt.dmt_daily_sales_estimate e
              ON m.asin = e.asin AND m.observed_date = e.estimate_date AND m.marketplace = e.marketplace
            WHERE m.segment_name = %s
              AND m.marketplace = %s
              AND m.observed_date >= DATE_SUB(%s, INTERVAL 30 DAY)
              AND m.observed_date < %s
            GROUP BY m.brand
        """
        prior_df = pd.read_sql(
            prior_sql, connection, params=(segment_name, marketplace, target_date, target_date)
        )
        if prior_df.empty:
            prior_share_map = (
                group.groupby("brand")["daily_sales"].sum() / max(sample_sales_sum, 1.0)
            ).to_dict()
        else:
            total_prior_sales = float(prior_df["total_daily_sales"].sum()) or 1.0
            prior_share_map = {
                (row["brand"] or "Unknown"): float(row["total_daily_sales"]) / total_prior_sales
                for _, row in prior_df.iterrows()
            }

        prior_strength = max(10.0, sample_sales_sum * (1.0 - coverage_ratio + 0.1))

        brand_agg = (
            group.groupby("brand")
            .agg(
                distinct_asins=("asin", "nunique"),
                total_estimated_daily_sales=("daily_sales", "sum"),
                total_num_ratings=("num_ratings", "sum"),
                total_sales_volume=("sales_volume_num", "sum"),
                avg_price=("price", "mean"),
                avg_star_rating=("star_rating", "mean"),
            )
            .reset_index()
        )

        total_ratings = float(brand_agg["total_num_ratings"].fillna(0).sum()) or 0.0
        total_asins = float(brand_agg["distinct_asins"].fillna(0).sum()) or 0.0

        for _, row in brand_agg.iterrows():
            brand = row["brand"] or "Unknown"
            observed_sales = float(row["total_estimated_daily_sales"] or 0.0)
            prior_share = prior_share_map.get(brand)
            if prior_share is None:
                prior_share = 1.0 / max(len(prior_share_map), 1)

            eb_share = (
                observed_sales + prior_share * prior_strength
            ) / max(sample_sales_sum + prior_strength, 1.0)
            adjusted_sales = bootstrap_mean_sales * eb_share

            share_rows.append({
                "observed_date": target_date,
                "marketplace": marketplace,
                "segment_name": segment_name,
                "brand": brand,
                "distinct_asins": int(row["distinct_asins"] or 0),
                "total_estimated_daily_sales": round(adjusted_sales),
                "total_num_ratings": int(row["total_num_ratings"] or 0),
                "total_sales_volume": int(row["total_sales_volume"] or 0) if pd.notna(row["total_sales_volume"]) else None,
                "avg_price": row["avg_price"],
                "avg_star_rating": row["avg_star_rating"],
                "sales_share_pct": round(eb_share * 100.0, 2),
                "rating_share_pct": round((float(row["total_num_ratings"] or 0) * 100.0 / total_ratings), 2) if total_ratings > 0 else 0.0,
                "asin_share_pct": round((float(row["distinct_asins"] or 0) * 100.0 / total_asins), 2) if total_asins > 0 else 0.0,
                "eb_adjusted_share_pct": round(eb_share * 100.0, 2),
                "sample_asin_count": sample_asin_count,
                "estimated_market_asin_count": estimated_market_asin_count,
                "bootstrap_mean_sales": bootstrap_mean_sales,
                "bootstrap_lower_bound": bootstrap_lower_bound,
                "bootstrap_upper_bound": bootstrap_upper_bound,
                "coverage_ratio": coverage_ratio,
                "stability_score": stability_score,
                "last_observed_at": datetime.utcnow(),
            })

        stats_rows.append({
            "observed_date": target_date,
            "marketplace": marketplace,
            "segment_name": segment_name,
            "sample_asin_count": sample_asin_count,
            "estimated_market_asin_count": estimated_market_asin_count,
            "sample_sales_sum": sample_sales_sum,
            "bootstrap_mean_sales": bootstrap_mean_sales,
            "bootstrap_lower_bound": bootstrap_lower_bound,
            "bootstrap_upper_bound": bootstrap_upper_bound,
            "coverage_ratio": coverage_ratio,
            "stability_score": stability_score,
            "eb_prior_strength": prior_strength,
            "last_observed_at": datetime.utcnow(),
        })

    share_result = pd.DataFrame(share_rows)
    stats_result = pd.DataFrame(stats_rows)

    _upsert_dmt_rows(
        connection,
        table="gurysk_dmt.dmt_segment_market_share",
        columns=[
            "observed_date", "marketplace", "segment_name", "brand",
            "distinct_asins", "total_estimated_daily_sales",
            "total_num_ratings", "total_sales_volume",
            "avg_price", "avg_star_rating",
            "sales_share_pct", "rating_share_pct", "asin_share_pct",
            "eb_adjusted_share_pct",
            "sample_asin_count", "estimated_market_asin_count",
            "bootstrap_mean_sales", "bootstrap_lower_bound", "bootstrap_upper_bound",
            "coverage_ratio", "stability_score", "last_observed_at",
        ],
        df=share_result,
    )

    _upsert_dmt_rows(
        connection,
        table="gurysk_dmt.dmt_segment_estimation_stats",
        columns=[
            "observed_date", "marketplace", "segment_name",
            "sample_asin_count", "estimated_market_asin_count",
            "sample_sales_sum",
            "bootstrap_mean_sales", "bootstrap_lower_bound", "bootstrap_upper_bound",
            "coverage_ratio", "stability_score", "eb_prior_strength",
            "last_observed_at",
        ],
        df=stats_result,
    )

    count = len(share_result)
    logger.info("build_segment_market_share: %d rows for segment %s.", count, segment_name)
    return count


# ---------------------------------------------------------------------------
# Generic DMT: trend detection & alerts
# ---------------------------------------------------------------------------

def detect_and_store_trend_alerts(
    connection: pymysql.Connection,
    brand: Optional[str] = None,
    lookback_days: int = 30,
    z_threshold: float = 2.0,
) -> int:
    """
    Detect significant changes in daily sales, market share, price, rating
    using Z-score against rolling 30-day baseline. Writes to dmt_trend_alert.
    """
    alerts: List[Dict] = []

    # --- Daily sales trend ---
    where = "WHERE 1=1"
    params: List[Any] = []
    if brand:
        where += " AND LOWER(COALESCE(brand,'')) LIKE %s"
        params.append(f"%{brand.lower()}%")

    df = pd.read_sql(
        f"SELECT estimate_date, marketplace, brand, "
        f"  SUM(estimated_daily_sales) AS total_daily_sales, "
        f"  AVG(price) AS avg_price, "
        f"  AVG(star_rating) AS avg_star_rating "
        f"FROM gurysk_dmt.dmt_daily_sales_estimate {where} "
        f"GROUP BY estimate_date, marketplace, brand "
        f"ORDER BY estimate_date",
        connection,
        params=params or None,
    )

    if len(df) >= 7:
        for (mp, br), group in df.groupby(["marketplace", "brand"]):
            group = group.sort_values("estimate_date")
            for metric, col in [
                ("daily_sales", "total_daily_sales"),
                ("avg_price", "avg_price"),
                ("avg_rating", "avg_star_rating"),
            ]:
                if col not in group.columns or group[col].dropna().empty:
                    continue
                ma = group[col].rolling(lookback_days, min_periods=7).mean()
                std = group[col].rolling(lookback_days, min_periods=7).std()
                if std.iloc[-1] is None or std.iloc[-1] == 0 or pd.isna(std.iloc[-1]):
                    continue
                z = (group[col].iloc[-1] - ma.iloc[-1]) / std.iloc[-1]
                if abs(z) >= z_threshold:
                    direction = "上升" if z > 0 else "下降"
                    level = "critical" if abs(z) > 3 else "warning"
                    change_pct = (
                        (group[col].iloc[-1] - ma.iloc[-1]) / ma.iloc[-1] * 100
                        if ma.iloc[-1] and ma.iloc[-1] != 0 else 0
                    )
                    alerts.append({
                        "alert_date": group["estimate_date"].iloc[-1],
                        "marketplace": mp,
                        "dimension_type": "brand",
                        "dimension_value": br,
                        "metric_name": metric,
                        "current_value": float(group[col].iloc[-1]),
                        "baseline_value": float(ma.iloc[-1]),
                        "change_pct": round(float(change_pct), 2),
                        "z_score": round(float(z), 2),
                        "alert_level": level,
                        "alert_message": f"{br} {metric} 显著{direction}，Z={z:.1f}",
                    })

    if not alerts:
        logger.info("detect_and_store_trend_alerts: no alerts generated.")
        return 0

    alert_df = pd.DataFrame(alerts)
    _upsert_dmt_rows(
        connection,
        table="gurysk_dmt.dmt_trend_alert",
        columns=[
            "alert_date", "marketplace", "dimension_type", "dimension_value",
            "metric_name", "current_value", "baseline_value", "change_pct",
            "z_score", "alert_level", "alert_message",
        ],
        df=alert_df,
    )

    logger.info("detect_and_store_trend_alerts: %d alerts stored.", len(alerts))
    return len(alerts)
