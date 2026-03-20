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

            if isinstance(payload, list):
                all_results.extend(payload)
            elif isinstance(payload, dict) and isinstance(payload.get("products"), list):
                all_results.extend(payload["products"])
            else:
                all_results.append(payload)

        return all_results
