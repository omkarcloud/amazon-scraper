import logging
import os
import time
from dataclasses import dataclass, field

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
    """A single search or ASIN-detail task with its own countries/pages."""
    query_type: str          # 'product_query', 'category_query', 'target_asin'
    keyword: str             # search keyword or ASIN
    countries: list[str] = field(default_factory=lambda: list(ALL_COUNTRIES))
    pages: int = 1


def _resolve_countries(raw: str) -> list[str]:
    if raw.strip().upper() == "ALL":
        return list(ALL_COUNTRIES)
    return [c.strip().upper() for c in raw.split(",") if c.strip()]


def _parse_csv(env_var: str, default: str = "") -> list[str]:
    value = os.getenv(env_var, default).strip()
    if not value:
        return []
    return [v.strip() for v in value.split(",") if v.strip()]


def load_config_from_db(profile: str) -> tuple[list[ScrapeTask], float] | None:
    """Load scraper config from gurysk_app.app_scraper_config.

    Returns (tasks, delay) or None if DB is unreachable / table missing.
    """
    try:
        conn = pymysql.connect(
            host=os.getenv("DB_HOST", ""),
            port=int(os.getenv("DB_PORT", "3306")),
            user=os.getenv("DB_USER", ""),
            password=os.getenv("DB_PASSWORD", ""),
            database=CONFIG_DB,
            charset="utf8mb4",
            connect_timeout=10,
        )
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
            if ct in ("product_query", "category_query", "target_asin"):
                tasks.append(ScrapeTask(
                    query_type=ct,
                    keyword=r["config_key"],
                    countries=_resolve_countries(r["countries"]),
                    pages=r["pages"],
                ))
            elif ct == "setting" and r["config_key"] == "request_delay":
                delay = float(r["config_value"] or "1")

        logger.info("Loaded %d tasks from DB (profile=%s)", len(tasks), profile)
        return tasks, delay

    except Exception:
        logger.warning("Failed to read %s.%s, falling back to env vars", CONFIG_DB, CONFIG_TABLE, exc_info=True)
        return None
    finally:
        conn.close()


def build_tasks_from_env() -> tuple[list[ScrapeTask], float]:
    """Fallback: build tasks from environment variables."""
    queries = _parse_csv("AMAZON_QUERIES") or _parse_csv("AMAZON_QUERY", "Macbook")
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


def run_tasks(tasks: list[ScrapeTask], api_key: str, delay: float, connection) -> tuple[int, int]:
    total_rows = 0
    total_errors = 0

    search_tasks = [t for t in tasks if t.query_type in ("product_query", "category_query")]
    asin_tasks = [t for t in tasks if t.query_type == "target_asin"]

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

    if asin_tasks:
        all_asins = [t.keyword for t in asin_tasks]
        asin_countries = asin_tasks[0].countries
        for country in asin_countries:
            try:
                logger.info("[target_asin] Fetch %s in %s...", all_asins, country)
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

    for t in tasks:
        logger.info("  Task: [%s] '%s' -> %d countries, %d pages",
                     t.query_type, t.keyword, len(t.countries), t.pages)

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
