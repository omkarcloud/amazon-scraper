"""
ETL pipeline: SRC (amazon_product_raw) -> DWH -> DMT -> APP.

Handles table creation, JSON parsing, incremental extraction,
monthly aggregation, and market-share computation.
"""

import html
import json
import logging
import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import pymysql

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
]


def ensure_all_tables(connection: pymysql.Connection) -> None:
    """Create all DWH / DMT tables and APP views if they don't exist."""
    with connection.cursor() as cursor:
        for ddl in [
            DWH_SNAPSHOT_DDL,
            DMT_OUTIN_MONTHLY_SALES_DDL,
            DMT_OUTIN_DAILY_SALES_DDL,
            DMT_OUTIN_REVIEW_TREND_DDL,
            DMT_COFFEE_MARKET_SHARE_DDL,
        ]:
            cursor.execute(ddl)
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
        asin, marketplace, source_endpoint, search_query,
        product_title, brand, price, currency,
        star_rating, num_ratings, num_reviews,
        sales_volume_raw, sales_volume_num,
        is_best_seller, is_prime, product_url, image_url
    ) VALUES (
        %s, %s, %s, %s,
        %s, %s, %s, %s,
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
            "       marketplace_country, search_query, api_payload "
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
        payload = row["api_payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        inner = payload.get("data", payload) if isinstance(payload, dict) else payload
        if not isinstance(inner, dict):
            inner = {}

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
            row.get("search_query"),
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
    uk_fields = {"observed_month", "observed_date", "marketplace", "asin", "brand"}
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
            else:
                vals.append(v)
        rows_to_insert.append(tuple(vals))

    with connection.cursor() as cur:
        cur.executemany(sql, rows_to_insert)
    connection.commit()
