"""
Airflow-callable entry point functions.

Usage in an Airflow DAG (inside etlprocessor):

    from amazon_scraper.src.tasks import (
        task_fetch_and_store_search,
        task_fetch_and_store_product_details,
    )

    with DAG(...) as dag:
        PythonOperator(
            task_id="amazon_search",
            python_callable=task_fetch_and_store_search,
            op_kwargs={"query": "Macbook", "country": "US"},
        )

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
