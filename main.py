import logging
import os
import time

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


def _parse_csv(env_var: str, default: str = "") -> list[str]:
    value = os.getenv(env_var, default).strip()
    if not value:
        return []
    return [v.strip() for v in value.split(",") if v.strip()]


def _get_countries() -> list[str]:
    raw = os.getenv("AMAZON_COUNTRIES", os.getenv("AMAZON_COUNTRY", "US")).strip()
    if raw.upper() == "ALL":
        return list(ALL_COUNTRIES)
    return [c.strip().upper() for c in raw.split(",") if c.strip()]


if __name__ == "__main__":
    queries = _parse_csv("AMAZON_QUERIES") or _parse_csv("AMAZON_QUERY", "Macbook")
    asins = _parse_csv("AMAZON_ASINS")
    countries = _get_countries()
    pages = int(os.getenv("AMAZON_PAGES", "1"))
    api_key = os.getenv("RAPIDAPI_KEY")
    store_to_db = os.getenv("STORE_TO_DB", "false").lower() == "true"
    delay = float(os.getenv("AMAZON_REQUEST_DELAY", "1"))

    logger.info(
        "Config: queries=%s, asins=%s, countries=%s (%d), pages=%d, store_to_db=%s",
        queries, asins, countries, len(countries), pages, store_to_db,
    )

    total_rows = 0
    total_errors = 0
    connection = None

    if store_to_db:
        connection = create_db_connection()

    try:
        for query in queries:
            for country in countries:
                for page in range(1, pages + 1):
                    try:
                        logger.info("Search '%s' in %s (page %d)...", query, country, page)
                        if connection:
                            rows = Amazon.fetch_and_store_search_results(
                                query=query, connection=connection,
                                key=api_key, country=country, page=page,
                            )
                            total_rows += len(rows)
                            logger.info("  -> %d rows stored", len(rows))
                        else:
                            products = Amazon.search(
                                query=query, key=api_key, country=country, page=page,
                            )
                            logger.info("  -> %d products found", len(products))
                        time.sleep(delay)
                    except Exception:
                        logger.exception("  -> Failed for query='%s' country=%s page=%d", query, country, page)
                        total_errors += 1
                        time.sleep(delay)

        if asins:
            for country in countries:
                try:
                    logger.info("Fetch ASIN details %s in %s...", asins, country)
                    if connection:
                        rows = Amazon.fetch_and_store_product_details(
                            asin=asins, connection=connection,
                            key=api_key, country=country,
                        )
                        total_rows += len(rows)
                        logger.info("  -> %d rows stored", len(rows))
                    else:
                        products = Amazon.get_products(
                            asin=asins, key=api_key, country=country,
                        )
                        logger.info("  -> %d product details found", len(products))
                    time.sleep(delay)
                except Exception:
                    logger.exception("  -> Failed for asins=%s country=%s", asins, country)
                    total_errors += 1
                    time.sleep(delay)

        logger.info("Done. total_rows=%d, total_errors=%d", total_rows, total_errors)

    finally:
        if connection:
            connection.close()
            close_tunnel()
