import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any, Optional, Tuple

import pymysql

from src import Amazon, create_db_connection, close_tunnel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

ALL_COUNTRIES = [
    "US", "AU", "BR", "CA", "CN", "FR", "DE", "IN", "IT", "MX",
    "NL", "SG", "ES", "TR", "AE", "GB", "JP", "SA", "PL", "SE",
    "BE", "EG", "ZA", "IE",
]

CONFIG_DB = "gurysk_app"
CONFIG_TABLE = "app_scraper_config"


@dataclass
class ScrapeTask:
    """A single scraping task."""
    query_type: str
    keyword: str
    countries: list[str] = field(default_factory=lambda: ["US"])
    pages: int = 1
    extra: dict = field(default_factory=dict)


def _resolve_countries(raw: str) -> list[str]:
    if raw.strip().upper() == "ALL":
        return list(ALL_COUNTRIES)
    return [c.strip().upper() for c in raw.split(",") if c.strip()]


def _parse_csv(env_var: str, default: str = "") -> list[str]:
    value = os.getenv(env_var, default).strip()
    if not value:
        return []
    return [v.strip() for v in value.split(",") if v.strip()]


def load_config_from_db(profile: str) -> Optional[Tuple[list[ScrapeTask], float]]:
    try:
        conn = create_db_connection(database=CONFIG_DB)
    except Exception:
        logger.warning("Cannot connect to %s to read config, falling back to env vars", CONFIG_DB)
        return None

    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(
                f"SELECT config_type, config_key, config_value, countries, pages "
                f"FROM {CONFIG_TABLE} "
                f"WHERE is_active = 1 AND schedule_profile IN (%s, 'both') "
                f"ORDER BY config_type, id",
                (profile,),
            )
            rows = cur.fetchall()

        if not rows:
            logger.warning("No active config rows for profile=%s, falling back to env vars", profile)
            return None

        tasks: list[ScrapeTask] = []
        delay = 1.0

        for r in rows:
            ct = r["config_type"]
            extra = {}
            if r.get("config_value"):
                try:
                    extra = json.loads(r["config_value"])
                except (json.JSONDecodeError, TypeError):
                    extra = {"value": r["config_value"]}

            if ct == "setting":
                if r["config_key"] == "request_delay":
                    delay = float(r.get("config_value") or "1")
            else:
                tasks.append(ScrapeTask(
                    query_type=ct,
                    keyword=r["config_key"],
                    countries=_resolve_countries(r["countries"]),
                    pages=r["pages"],
                    extra=extra,
                ))

        logger.info("Loaded %d tasks from DB (profile=%s)", len(tasks), profile)
        return tasks, delay

    except Exception:
        logger.warning("Failed to read %s.%s, falling back to env vars", CONFIG_DB, CONFIG_TABLE, exc_info=True)
        return None
    finally:
        conn.close()


def build_tasks_from_env() -> tuple[list[ScrapeTask], float]:
    queries = _parse_csv("AMAZON_QUERIES") or _parse_csv("AMAZON_QUERY", "outin")
    asins = _parse_csv("AMAZON_ASINS")
    raw_countries = os.getenv("AMAZON_COUNTRIES", os.getenv("AMAZON_COUNTRY", "US"))
    countries = _resolve_countries(raw_countries)
    pages = int(os.getenv("AMAZON_PAGES", "1"))
    delay = float(os.getenv("AMAZON_REQUEST_DELAY", "1"))

    tasks: list[ScrapeTask] = []
    for q in queries:
        tasks.append(ScrapeTask("product_query", q, countries, pages))
    for a in asins:
        tasks.append(ScrapeTask("target_asin", a, countries, 1))

    return tasks, delay


def _estimate_api_calls(tasks: list[ScrapeTask]) -> int:
    """Estimate total API calls to help with budget awareness."""
    total = 0
    asin_tasks = [t for t in tasks if t.query_type == "target_asin"]
    if asin_tasks:
        all_asins = [t.keyword for t in asin_tasks]
        batches = (len(all_asins) + 9) // 10
        total += batches * len(asin_tasks[0].countries)

    for t in tasks:
        if t.query_type in ("product_query", "category_query", "brand_search", "segment_scan"):
            total += len(t.countries) * t.pages
        elif t.query_type == "category_scan":
            total += len(t.countries) * t.pages
        elif t.query_type == "bestseller_scan":
            total += len(t.countries) * t.pages
        elif t.query_type == "review_scan":
            total += len(t.countries)
        elif t.query_type == "offer_scan":
            total += len(t.countries)
    return total


def run_tasks(tasks: list[ScrapeTask], api_key: str, delay: float, connection) -> tuple[int, int]:
    total_rows = 0
    total_errors = 0

    search_tasks = [t for t in tasks if t.query_type in ("product_query", "category_query")]
    brand_search_tasks = [t for t in tasks if t.query_type == "brand_search"]
    segment_tasks = [t for t in tasks if t.query_type == "segment_scan"]
    asin_tasks = [t for t in tasks if t.query_type == "target_asin"]
    category_scan_tasks = [t for t in tasks if t.query_type == "category_scan"]
    bestseller_tasks = [t for t in tasks if t.query_type == "bestseller_scan"]
    review_tasks = [t for t in tasks if t.query_type == "review_scan"]
    offer_tasks = [t for t in tasks if t.query_type == "offer_scan"]

    # --- product_query / category_query (legacy search) ---
    for task in search_tasks:
        for country in task.countries:
            for page in range(1, task.pages + 1):
                try:
                    logger.info("[%s] Search '%s' in %s (page %d)...",
                                task.query_type, task.keyword, country, page)
                    if connection:
                        rows = Amazon.fetch_and_store_search_results(
                            query=task.keyword, connection=connection,
                            key=api_key, country=country, page=page,
                        )
                        total_rows += len(rows)
                        logger.info("  -> %d rows stored", len(rows))
                    else:
                        products = Amazon.search(
                            query=task.keyword, key=api_key, country=country, page=page,
                        )
                        logger.info("  -> %d products found", len(products))
                    time.sleep(delay)
                except Exception:
                    logger.exception("  -> Failed: query='%s' country=%s page=%d",
                                     task.keyword, country, page)
                    total_errors += 1
                    time.sleep(delay)

    # --- brand_search (search with brand filter) ---
    for task in brand_search_tasks:
        brand_name = task.extra.get("brand", task.keyword)
        query = task.extra.get("query", task.keyword)
        for country in task.countries:
            for page in range(1, task.pages + 1):
                try:
                    logger.info("[brand_search] '%s' brand=%s in %s (page %d)...",
                                query, brand_name, country, page)
                    if connection:
                        rows = Amazon.fetch_and_store_search_results(
                            query=query, connection=connection,
                            key=api_key, country=country, page=page,
                            brand=brand_name,
                        )
                        total_rows += len(rows)
                        logger.info("  -> %d rows stored", len(rows))
                    time.sleep(delay)
                except Exception:
                    logger.exception("  -> Failed: brand_search '%s'", brand_name)
                    total_errors += 1
                    time.sleep(delay)

    # --- segment_scan (custom keyword-defined segment search) ---
    for task in segment_tasks:
        segment_name = task.extra.get("segment_name", task.keyword)
        for country in task.countries:
            for page in range(1, task.pages + 1):
                try:
                    logger.info("[segment_scan] '%s' segment=%s in %s (page %d)...",
                                task.keyword, segment_name, country, page)
                    if connection:
                        rows = Amazon.fetch_and_store_segment_products(
                            segment_keyword=task.keyword,
                            segment_name=segment_name,
                            connection=connection,
                            key=api_key,
                            country=country,
                            page=page,
                        )
                        total_rows += len(rows)
                        logger.info("  -> %d rows stored", len(rows))
                    else:
                        products = Amazon.search(
                            query=task.keyword, key=api_key, country=country, page=page,
                        )
                        logger.info("  -> %d products found", len(products))
                    time.sleep(delay)
                except Exception:
                    logger.exception("  -> Failed: segment_scan '%s'", task.keyword)
                    total_errors += 1
                    time.sleep(delay)

    # --- target_asin (batch details, 10 per call) ---
    if asin_tasks:
        all_asins = [t.keyword for t in asin_tasks]
        asin_countries = asin_tasks[0].countries
        for country in asin_countries:
            try:
                logger.info("[target_asin] Fetch %d ASINs in %s...", len(all_asins), country)
                if connection:
                    rows = Amazon.fetch_and_store_product_details(
                        asin=all_asins, connection=connection,
                        key=api_key, country=country,
                    )
                    total_rows += len(rows)
                    logger.info("  -> %d rows stored", len(rows))
                else:
                    products = Amazon.get_products(
                        asin=all_asins, key=api_key, country=country,
                    )
                    logger.info("  -> %d product details found", len(products))
                time.sleep(delay)
            except Exception:
                logger.exception("  -> Failed: asins=%s country=%s", all_asins, country)
                total_errors += 1
                time.sleep(delay)

    # --- category_scan (/products-by-category) ---
    for task in category_scan_tasks:
        for country in task.countries:
            for page in range(1, task.pages + 1):
                try:
                    logger.info("[category_scan] cat=%s in %s (page %d)...",
                                task.keyword, country, page)
                    if connection:
                        rows = Amazon.fetch_and_store_category_products(
                            category_id=task.keyword, connection=connection,
                            key=api_key, country=country, page=page,
                            category_name=task.extra.get("category_name"),
                        )
                        total_rows += len(rows)
                        logger.info("  -> %d rows stored", len(rows))
                    time.sleep(delay)
                except Exception:
                    logger.exception("  -> Failed: category_scan '%s'", task.keyword)
                    total_errors += 1
                    time.sleep(delay)

    # --- bestseller_scan (/best-sellers) ---
    for task in bestseller_tasks:
        for country in task.countries:
            for page in range(1, task.pages + 1):
                try:
                    logger.info("[bestseller_scan] cat=%s in %s (page %d)...",
                                task.keyword, country, page)
                    if connection:
                        rows = Amazon.fetch_and_store_best_sellers(
                            category=task.keyword, connection=connection,
                            key=api_key, country=country, page=page,
                        )
                        total_rows += len(rows)
                        logger.info("  -> %d rows stored", len(rows))
                    time.sleep(delay)
                except Exception:
                    logger.exception("  -> Failed: bestseller_scan '%s'", task.keyword)
                    total_errors += 1
                    time.sleep(delay)

    # --- review_scan (/top-product-reviews, 1 call per ASIN) ---
    if review_tasks:
        review_asins = [t.keyword for t in review_tasks]
        review_countries = review_tasks[0].countries
        for country in review_countries:
            for asin_val in review_asins:
                try:
                    logger.info("[review_scan] ASIN=%s in %s...", asin_val, country)
                    if connection:
                        rows = Amazon.fetch_and_store_reviews(
                            asin=asin_val, connection=connection,
                            key=api_key, country=country,
                        )
                        total_rows += len(rows)
                        logger.info("  -> %d review rows stored", len(rows))
                    time.sleep(delay)
                except Exception:
                    logger.exception("  -> Failed: review_scan '%s'", asin_val)
                    total_errors += 1
                    time.sleep(delay)

    # --- offer_scan (/product-offers, batch 10) ---
    if offer_tasks:
        offer_asins = [t.keyword for t in offer_tasks]
        offer_countries = offer_tasks[0].countries
        for country in offer_countries:
            try:
                logger.info("[offer_scan] %d ASINs in %s...", len(offer_asins), country)
                if connection:
                    rows = Amazon.fetch_and_store_offers(
                        asin=offer_asins, connection=connection,
                        key=api_key, country=country,
                    )
                    total_rows += len(rows)
                    logger.info("  -> %d offer rows stored", len(rows))
                time.sleep(delay)
            except Exception:
                logger.exception("  -> Failed: offer_scan country=%s", country)
                total_errors += 1
                time.sleep(delay)

    return total_rows, total_errors


if __name__ == "__main__":
    profile = os.getenv("SCRAPE_PROFILE", "daily")
    api_key = os.getenv("RAPIDAPI_KEY")
    store_to_db = os.getenv("STORE_TO_DB", "false").lower() == "true"

    db_config = load_config_from_db(profile)
    if db_config:
        tasks, delay = db_config
        logger.info("Config source: DATABASE (profile=%s)", profile)
    else:
        tasks, delay = build_tasks_from_env()
        logger.info("Config source: ENV VARS")

    est_calls = _estimate_api_calls(tasks)
    logger.info("Estimated API calls this run: %d", est_calls)

    for t in tasks:
        logger.info("  Task: [%s] '%s' -> %d countries, %d pages, extra=%s",
                     t.query_type, t.keyword, len(t.countries), t.pages, t.extra)

    connection = None
    if store_to_db:
        connection = create_db_connection()

    try:
        total_rows, total_errors = run_tasks(tasks, api_key, delay, connection)
        logger.info("Done. total_rows=%d, total_errors=%d", total_rows, total_errors)
    finally:
        if connection:
            connection.close()
            close_tunnel()
