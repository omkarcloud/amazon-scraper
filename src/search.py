from typing import Any, Dict, Optional, Sequence, Union

from .rapidapi_client import RapidAmazonDataClient


def search(
    query: str,
    key: Optional[str] = None,
    country: str = "US",
    page: int = 1,
    **filters: Any,
) -> Dict[str, Any]:
    client = RapidAmazonDataClient(api_key=key)
    return client.search_products(query=query, country=country, page=page, **filters)


def get_product(
    asin: Union[str, Sequence[str]],
    key: Optional[str] = None,
    country: str = "US",
    **filters: Any,
):
    client = RapidAmazonDataClient(api_key=key)
    return client.get_product_details(asin=asin, country=country, **filters)

