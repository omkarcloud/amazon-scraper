from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Union

from .database import (
    DEFAULT_RAW_PRODUCTS_TABLE,
    ensure_raw_products_table,
    insert_raw_product_rows,
)
from .search import get_product, search


def _ensure_list(values: Union[str, Sequence[str]]) -> List[str]:
    if isinstance(values, str):
        return [values]
    return list(values)


def _extract_search_products(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    data = payload.get("data", payload)
    products = data.get("products", [])
    if not isinstance(products, list):
        return []
    return products


def _extract_asin(record: Dict[str, Any]) -> Optional[str]:
    asin = record.get("asin")
    if asin:
        return asin

    product_information = record.get("product_information", {})
    if isinstance(product_information, dict):
        return product_information.get("ASIN")

    return None


class Amazon:
    @staticmethod
    def search(
        query: Union[str, Sequence[str]],
        key: Optional[str] = None,
        country: str = "US",
        page: int = 1,
        **filters: Any,
    ) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        for query_value in _ensure_list(query):
            payload = search(
                query=query_value,
                key=key,
                country=country,
                page=page,
                **filters,
            )
            results.extend(_extract_search_products(payload))
        return results

    @staticmethod
    def get_products(
        asin: Union[str, Sequence[str]],
        key: Optional[str] = None,
        country: str = "US",
        **filters: Any,
    ) -> List[Dict[str, Any]]:
        payload = get_product(asin=asin, key=key, country=country, **filters)
        if isinstance(payload, list):
            return payload
        data = payload.get("data", payload)
        if isinstance(data, list):
            return data
        return [data]

    @staticmethod
    def build_raw_product_rows(
        records: Sequence[Dict[str, Any]],
        source_endpoint: str,
        country: str,
        search_query: Optional[str] = None,
        request_metadata: Optional[Dict[str, Any]] = None,
        record_create_timestamp: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        created_at = record_create_timestamp or datetime.now(timezone.utc)
        base_metadata = request_metadata or {}
        rows: List[Dict[str, Any]] = []

        for record in records:
            asin = _extract_asin(record)
            if not asin:
                continue

            rows.append(
                {
                    "asin": asin,
                    "record_create_timestamp": created_at,
                    "source_endpoint": source_endpoint,
                    "marketplace_country": country,
                    "search_query": search_query,
                    "request_metadata": base_metadata,
                    "api_payload": record,
                }
            )

        return rows

    @staticmethod
    def fetch_search_raw_rows(
        query: Union[str, Sequence[str]],
        key: Optional[str] = None,
        country: str = "US",
        page: int = 1,
        **filters: Any,
    ) -> List[Dict[str, Any]]:
        all_rows: List[Dict[str, Any]] = []
        for query_value in _ensure_list(query):
            payload = search(
                query=query_value,
                key=key,
                country=country,
                page=page,
                **filters,
            )
            rows = Amazon.build_raw_product_rows(
                records=_extract_search_products(payload),
                source_endpoint="product_search",
                country=country,
                search_query=query_value,
                request_metadata={"page": page, **filters},
            )
            all_rows.extend(rows)
        return all_rows

    @staticmethod
    def fetch_product_raw_rows(
        asin: Union[str, Sequence[str]],
        key: Optional[str] = None,
        country: str = "US",
        **filters: Any,
    ) -> List[Dict[str, Any]]:
        records = Amazon.get_products(asin=asin, key=key, country=country, **filters)
        return Amazon.build_raw_product_rows(
            records=records,
            source_endpoint="product_details",
            country=country,
            request_metadata={**filters},
        )

    @staticmethod
    def store_raw_rows(
        connection,
        rows: Sequence[Dict[str, Any]],
        table_name: str = DEFAULT_RAW_PRODUCTS_TABLE,
        create_table: bool = True,
        commit: bool = True,
    ) -> int:
        if create_table:
            ensure_raw_products_table(connection, table_name=table_name)
        return insert_raw_product_rows(
            connection,
            rows=rows,
            table_name=table_name,
            commit=commit,
        )

    @staticmethod
    def fetch_and_store_search_results(
        query: Union[str, Sequence[str]],
        connection,
        key: Optional[str] = None,
        country: str = "US",
        page: int = 1,
        table_name: str = DEFAULT_RAW_PRODUCTS_TABLE,
        create_table: bool = True,
        commit: bool = True,
        **filters: Any,
    ) -> List[Dict[str, Any]]:
        rows = Amazon.fetch_search_raw_rows(
            query=query,
            key=key,
            country=country,
            page=page,
            **filters,
        )
        if rows:
            Amazon.store_raw_rows(
                connection=connection,
                rows=rows,
                table_name=table_name,
                create_table=create_table,
                commit=commit,
            )
        return rows

    @staticmethod
    def fetch_and_store_product_details(
        asin: Union[str, Sequence[str]],
        connection,
        key: Optional[str] = None,
        country: str = "US",
        table_name: str = DEFAULT_RAW_PRODUCTS_TABLE,
        create_table: bool = True,
        commit: bool = True,
        **filters: Any,
    ) -> List[Dict[str, Any]]:
        rows = Amazon.fetch_product_raw_rows(
            asin=asin,
            key=key,
            country=country,
            **filters,
        )
        if rows:
            Amazon.store_raw_rows(
                connection=connection,
                rows=rows,
                table_name=table_name,
                create_table=create_table,
                commit=commit,
            )
        return rows
