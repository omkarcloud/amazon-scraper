from typing import Any, Dict, List, Optional, Sequence, Union

from .rapidapi_client import RapidAmazonDataClient


def _client(key: Optional[str] = None) -> RapidAmazonDataClient:
    return RapidAmazonDataClient(api_key=key)


def search(
    query: str,
    key: Optional[str] = None,
    country: str = "US",
    page: int = 1,
    **filters: Any,
) -> Dict[str, Any]:
    return _client(key).search_products(query=query, country=country, page=page, **filters)


def get_product(
    asin: Union[str, Sequence[str]],
    key: Optional[str] = None,
    country: str = "US",
    **filters: Any,
):
    return _client(key).get_product_details(asin=asin, country=country, **filters)


def get_products_by_category(
    category_id: str,
    key: Optional[str] = None,
    country: str = "US",
    page: int = 1,
    **filters: Any,
) -> Dict[str, Any]:
    return _client(key).get_products_by_category(
        category_id=category_id, country=country, page=page, **filters,
    )


def get_best_sellers(
    category: str,
    key: Optional[str] = None,
    country: str = "US",
    page: int = 1,
    type_: str = "BEST_SELLERS",
    **filters: Any,
) -> Dict[str, Any]:
    return _client(key).get_best_sellers(
        category=category, country=country, page=page, type_=type_, **filters,
    )


def get_top_product_reviews(
    asin: str,
    key: Optional[str] = None,
    country: str = "US",
    **filters: Any,
) -> Dict[str, Any]:
    return _client(key).get_top_product_reviews(asin=asin, country=country, **filters)


def get_product_offers(
    asin: Union[str, Sequence[str]],
    key: Optional[str] = None,
    country: str = "US",
    **filters: Any,
) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
    return _client(key).get_product_offers(asin=asin, country=country, **filters)

