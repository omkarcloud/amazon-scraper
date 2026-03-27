from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Union

from .database import (
    DEFAULT_RAW_PRODUCTS_TABLE,
    ensure_raw_products_table,
    insert_raw_product_rows,
)
from .search import (
    get_best_sellers,
    get_product,
    get_product_offers,
    get_products_by_category,
    get_top_product_reviews,
    search,
)


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
        request_metadata: Optional[Dict[str, Any]] = None,
        source_endpoint: str = "product_search",
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
                source_endpoint=source_endpoint,
                country=country,
                search_query=query_value,
                request_metadata={**(request_metadata or {}), "page": page, **filters},
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
        request_metadata: Optional[Dict[str, Any]] = None,
        source_endpoint: str = "product_search",
        **filters: Any,
    ) -> List[Dict[str, Any]]:
        rows = Amazon.fetch_search_raw_rows(
            query=query,
            key=key,
            country=country,
            page=page,
            request_metadata=request_metadata,
            source_endpoint=source_endpoint,
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
    def fetch_segment_raw_rows(
        segment_keyword: str,
        segment_name: str,
        key: Optional[str] = None,
        country: str = "US",
        page: int = 1,
        **filters: Any,
    ) -> List[Dict[str, Any]]:
        return Amazon.fetch_search_raw_rows(
            query=segment_keyword,
            key=key,
            country=country,
            page=page,
            request_metadata={
                "segment_name": segment_name,
                "segment_keyword": segment_keyword,
                "segment_type": "custom",
            },
            source_endpoint="segment_search",
            **filters,
        )

    @staticmethod
    def fetch_and_store_segment_products(
        segment_keyword: str,
        segment_name: str,
        connection,
        key: Optional[str] = None,
        country: str = "US",
        page: int = 1,
        table_name: str = DEFAULT_RAW_PRODUCTS_TABLE,
        create_table: bool = True,
        commit: bool = True,
        **filters: Any,
    ) -> List[Dict[str, Any]]:
        rows = Amazon.fetch_segment_raw_rows(
            segment_keyword=segment_keyword,
            segment_name=segment_name,
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

    # ------------------------------------------------------------------
    # Products by Category
    # ------------------------------------------------------------------

    @staticmethod
    def fetch_category_raw_rows(
        category_id: str,
        key: Optional[str] = None,
        country: str = "US",
        page: int = 1,
        category_name: Optional[str] = None,
        **filters: Any,
    ) -> List[Dict[str, Any]]:
        payload = get_products_by_category(
            category_id=category_id, key=key, country=country, page=page, **filters,
        )
        records = _extract_search_products(payload)
        return Amazon.build_raw_product_rows(
            records=records,
            source_endpoint="products_by_category",
            country=country,
            search_query=f"category:{category_id}",
            request_metadata={
                "category_id": category_id,
                "category_name": category_name,
                "page": page,
                **filters,
            },
        )

    @staticmethod
    def fetch_and_store_category_products(
        category_id: str,
        connection,
        key: Optional[str] = None,
        country: str = "US",
        page: int = 1,
        table_name: str = DEFAULT_RAW_PRODUCTS_TABLE,
        category_name: Optional[str] = None,
        **filters: Any,
    ) -> List[Dict[str, Any]]:
        rows = Amazon.fetch_category_raw_rows(
            category_id=category_id,
            key=key,
            country=country,
            page=page,
            category_name=category_name,
            **filters,
        )
        if rows:
            Amazon.store_raw_rows(connection=connection, rows=rows, table_name=table_name)
        return rows

    # ------------------------------------------------------------------
    # Best Sellers
    # ------------------------------------------------------------------

    @staticmethod
    def fetch_bestseller_raw_rows(
        category: str,
        key: Optional[str] = None,
        country: str = "US",
        page: int = 1,
        type_: str = "BEST_SELLERS",
        **filters: Any,
    ) -> List[Dict[str, Any]]:
        payload = get_best_sellers(
            category=category, key=key, country=country, page=page, type_=type_, **filters,
        )
        data = payload.get("data", payload) if isinstance(payload, dict) else payload
        records = data if isinstance(data, list) else data.get("products", []) if isinstance(data, dict) else []
        return Amazon.build_raw_product_rows(
            records=records,
            source_endpoint="best_sellers",
            country=country,
            search_query=f"bestseller:{category}",
            request_metadata={"category": category, "page": page, "type": type_, **filters},
        )

    @staticmethod
    def fetch_and_store_best_sellers(
        category: str,
        connection,
        key: Optional[str] = None,
        country: str = "US",
        page: int = 1,
        type_: str = "BEST_SELLERS",
        table_name: str = DEFAULT_RAW_PRODUCTS_TABLE,
        **filters: Any,
    ) -> List[Dict[str, Any]]:
        rows = Amazon.fetch_bestseller_raw_rows(
            category=category, key=key, country=country, page=page, type_=type_, **filters,
        )
        if rows:
            Amazon.store_raw_rows(connection=connection, rows=rows, table_name=table_name)
        return rows

    # ------------------------------------------------------------------
    # Top Product Reviews (no cookie needed)
    # ------------------------------------------------------------------

    @staticmethod
    def fetch_review_raw_rows(
        asin: str,
        key: Optional[str] = None,
        country: str = "US",
        **filters: Any,
    ) -> List[Dict[str, Any]]:
        payload = get_top_product_reviews(asin=asin, key=key, country=country, **filters)
        data = payload.get("data", payload) if isinstance(payload, dict) else payload
        reviews = data if isinstance(data, list) else data.get("reviews", []) if isinstance(data, dict) else []
        created_at = datetime.now(timezone.utc)
        rows: List[Dict[str, Any]] = []
        for review in reviews:
            if not isinstance(review, dict):
                continue
            rows.append({
                "asin": asin,
                "record_create_timestamp": created_at,
                "source_endpoint": "top_product_reviews",
                "marketplace_country": country,
                "search_query": None,
                "request_metadata": {**filters},
                "api_payload": review,
            })
        return rows

    @staticmethod
    def fetch_and_store_reviews(
        asin: Union[str, Sequence[str]],
        connection,
        key: Optional[str] = None,
        country: str = "US",
        table_name: str = DEFAULT_RAW_PRODUCTS_TABLE,
        **filters: Any,
    ) -> List[Dict[str, Any]]:
        """Fetch top reviews for one or more ASINs (1 API call per ASIN)."""
        asins = _ensure_list(asin) if not isinstance(asin, str) else [asin]
        all_rows: List[Dict[str, Any]] = []
        for a in asins:
            rows = Amazon.fetch_review_raw_rows(a, key=key, country=country, **filters)
            all_rows.extend(rows)
        if all_rows:
            Amazon.store_raw_rows(connection=connection, rows=all_rows, table_name=table_name)
        return all_rows

    # ------------------------------------------------------------------
    # Product Offers (multi-seller pricing)
    # ------------------------------------------------------------------

    @staticmethod
    def fetch_offer_raw_rows(
        asin: Union[str, Sequence[str]],
        key: Optional[str] = None,
        country: str = "US",
        **filters: Any,
    ) -> List[Dict[str, Any]]:
        records = get_product_offers(asin=asin, key=key, country=country, **filters)
        if isinstance(records, dict):
            records = [records]
        return Amazon.build_raw_product_rows(
            records=records,
            source_endpoint="product_offers",
            country=country,
            request_metadata={**filters},
        )

    @staticmethod
    def fetch_and_store_offers(
        asin: Union[str, Sequence[str]],
        connection,
        key: Optional[str] = None,
        country: str = "US",
        table_name: str = DEFAULT_RAW_PRODUCTS_TABLE,
        **filters: Any,
    ) -> List[Dict[str, Any]]:
        rows = Amazon.fetch_offer_raw_rows(asin=asin, key=key, country=country, **filters)
        if rows:
            Amazon.store_raw_rows(connection=connection, rows=rows, table_name=table_name)
        return rows
