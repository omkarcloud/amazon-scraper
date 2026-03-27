from typing import Any, Dict, Iterable, List, Optional, Sequence, Union

import requests

from .config import get_rapidapi_settings


class RapidAPIError(RuntimeError):
    pass


def _chunked(values: Sequence[str], size: int) -> Iterable[Sequence[str]]:
    for index in range(0, len(values), size):
        yield values[index : index + size]


class RapidAmazonDataClient:
    def __init__(self, api_key: Optional[str] = None) -> None:
        settings = get_rapidapi_settings(api_key=api_key)
        self.api_key = settings["api_key"]
        self.api_host = settings["api_host"]
        self.base_url = settings["base_url"].rstrip("/")
        self.timeout = settings["timeout"]

    def _request(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        filtered_params = {
            key: value for key, value in params.items() if value is not None and value != ""
        }
        response = requests.get(
            f"{self.base_url}{endpoint}",
            headers={
                "X-RapidAPI-Key": self.api_key,
                "X-RapidAPI-Host": self.api_host,
            },
            params=filtered_params,
            timeout=self.timeout,
        )

        try:
            payload = response.json()
        except ValueError as exc:
            raise RapidAPIError(
                f"RapidAPI returned a non-JSON response with status {response.status_code}."
            ) from exc

        if not response.ok:
            message = payload.get("message") if isinstance(payload, dict) else str(payload)
            raise RapidAPIError(
                f"RapidAPI request failed with status {response.status_code}: {message}"
            )

        return payload

    def search_products(
        self,
        query: str,
        country: str = "US",
        page: int = 1,
        **filters: Any,
    ) -> Dict[str, Any]:
        params = {"query": query, "country": country, "page": page, **filters}
        return self._request("/search", params)

    def get_product_details(
        self,
        asin: Union[str, Sequence[str]],
        country: str = "US",
        **filters: Any,
    ) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        if isinstance(asin, str):
            return self._request("/product-details", {"asin": asin, "country": country, **filters})

        all_results: List[Dict[str, Any]] = []
        for asin_batch in _chunked(list(asin), 10):
            payload = self._request(
                "/product-details",
                {"asin": ",".join(asin_batch), "country": country, **filters},
            )
            all_results.extend(self._normalize_product_list(payload))

        return all_results

    def get_products_by_category(
        self,
        category_id: str,
        country: str = "US",
        page: int = 1,
        **filters: Any,
    ) -> Dict[str, Any]:
        params = {"category_id": category_id, "country": country, "page": page, **filters}
        return self._request("/products-by-category", params)

    def get_best_sellers(
        self,
        category: str,
        country: str = "US",
        page: int = 1,
        type_: str = "BEST_SELLERS",
        **filters: Any,
    ) -> Dict[str, Any]:
        params = {"category": category, "country": country, "page": page, "type": type_, **filters}
        return self._request("/best-sellers", params)

    def get_top_product_reviews(
        self,
        asin: str,
        country: str = "US",
        **filters: Any,
    ) -> Dict[str, Any]:
        params = {"asin": asin, "country": country, **filters}
        return self._request("/top-product-reviews", params)

    def get_product_offers(
        self,
        asin: Union[str, Sequence[str]],
        country: str = "US",
        **filters: Any,
    ) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        if isinstance(asin, str):
            return self._request("/product-offers", {"asin": asin, "country": country, **filters})

        all_results: List[Dict[str, Any]] = []
        for asin_batch in _chunked(list(asin), 10):
            payload = self._request(
                "/product-offers",
                {"asin": ",".join(asin_batch), "country": country, **filters},
            )
            all_results.extend(self._normalize_product_list(payload))
        return all_results

    @staticmethod
    def _normalize_product_list(payload: Any) -> List[Dict[str, Any]]:
        """Extract a flat list of product dicts from various API response shapes."""
        if isinstance(payload, dict) and "data" in payload:
            data = payload["data"]
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                return [data]
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict) and isinstance(payload.get("products"), list):
            return payload["products"]
        if isinstance(payload, dict):
            return [payload]
        return []
