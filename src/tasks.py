"""
Airflow-callable entry point functions.

Usage in an Airflow DAG (inside etlprocessor):

    from amazon_scraper.src.tasks import (
        task_fetch_and_store_search,
        task_fetch_and_store_product_details,
        task_extract_to_dwh,
        task_build_outin_sales,
        task_build_review_trend,
        task_build_market_share,
    )

    with DAG(...) as dag:
        PythonOperator(
            task_id="amazon_search",
            python_callable=task_fetch_and_store_search,
            op_kwargs={"query": "Macbook", "country": "US"},
        )

        # ETL pipeline: SRC -> DWH -> DMT
        extract = PythonOperator(
            task_id="extract_to_dwh",
            python_callable=task_extract_to_dwh,
        )
        sales = PythonOperator(
            task_id="build_outin_sales",
            python_callable=task_build_outin_sales,
        )
        reviews = PythonOperator(
            task_id="build_review_trend",
            python_callable=task_build_review_trend,
        )
        share = PythonOperator(
            task_id="build_market_share",
            python_callable=task_build_market_share,
        )
        extract >> [sales, reviews, share]

Each task function manages its own DB connection lifecycle.
"""

import logging
from typing import Any, Dict, List, Optional, Sequence, Union

from .amazon_scraper import Amazon
from .database import (
    DEFAULT_RAW_PRODUCTS_TABLE,
    close_tunnel,
    create_db_connection,
)
from .etl import (
    build_coffee_market_share,
    build_outin_daily_sales,
    build_outin_monthly_sales,
    build_outin_review_trend,
    extract_to_dwh,
)

logger = logging.getLogger(__name__)


def task_fetch_and_store_search(
    query: Union[str, Sequence[str]],
    key: Optional[str] = None,
    country: str = "US",
    page: int = 1,
    database: Optional[str] = None,
    table_name: str = DEFAULT_RAW_PRODUCTS_TABLE,
    **filters: Any,
) -> List[Dict[str, Any]]:
    connection = create_db_connection(database=database)
    try:
        rows = Amazon.fetch_and_store_search_results(
            query=query,
            connection=connection,
            key=key,
            country=country,
            page=page,
            table_name=table_name,
            create_table=True,
            commit=True,
            **filters,
        )
        logger.info(
            "task_fetch_and_store_search completed: %d rows inserted for query=%s",
            len(rows),
            query,
        )
        return rows
    finally:
        connection.close()
        close_tunnel()


def task_fetch_and_store_product_details(
    asin: Union[str, Sequence[str]],
    key: Optional[str] = None,
    country: str = "US",
    database: Optional[str] = None,
    table_name: str = DEFAULT_RAW_PRODUCTS_TABLE,
    **filters: Any,
) -> List[Dict[str, Any]]:
    connection = create_db_connection(database=database)
    try:
        rows = Amazon.fetch_and_store_product_details(
            asin=asin,
            connection=connection,
            key=key,
            country=country,
            table_name=table_name,
            create_table=True,
            commit=True,
            **filters,
        )
        logger.info(
            "task_fetch_and_store_product_details completed: %d rows inserted for asin=%s",
            len(rows),
            asin,
        )
        return rows
    finally:
        connection.close()
        close_tunnel()


# ---------------------------------------------------------------------------
# ETL pipeline tasks: SRC -> DWH -> DMT
# ---------------------------------------------------------------------------

def task_extract_to_dwh(**kwargs: Any) -> int:
    """Airflow task: incrementally extract SRC -> DWH."""
    connection = create_db_connection()
    try:
        count = extract_to_dwh(connection)
        logger.info("task_extract_to_dwh completed: %d rows.", count)
        return count
    finally:
        connection.close()
        close_tunnel()


def task_build_outin_daily_sales(**kwargs: Any) -> int:
    """Airflow task: aggregate DWH -> DMT (OutIn daily sales)."""
    connection = create_db_connection()
    try:
        count = build_outin_daily_sales(connection)
        logger.info("task_build_outin_daily_sales completed: %d rows.", count)
        return count
    finally:
        connection.close()
        close_tunnel()


def task_build_outin_sales(**kwargs: Any) -> int:
    """Airflow task: aggregate DWH -> DMT (OutIn monthly sales)."""
    connection = create_db_connection()
    try:
        count = build_outin_monthly_sales(connection)
        logger.info("task_build_outin_sales completed: %d rows.", count)
        return count
    finally:
        connection.close()
        close_tunnel()


def task_build_review_trend(**kwargs: Any) -> int:
    """Airflow task: aggregate DWH -> DMT (OutIn review trend)."""
    connection = create_db_connection()
    try:
        count = build_outin_review_trend(connection)
        logger.info("task_build_review_trend completed: %d rows.", count)
        return count
    finally:
        connection.close()
        close_tunnel()


def task_build_market_share(**kwargs: Any) -> int:
    """Airflow task: aggregate DWH -> DMT (coffee market share)."""
    connection = create_db_connection()
    try:
        count = build_coffee_market_share(connection)
        logger.info("task_build_market_share completed: %d rows.", count)
        return count
    finally:
        connection.close()
        close_tunnel()
