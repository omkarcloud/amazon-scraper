"""Backward compatibility for the retired Amazon Scraper API (omkar.cloud
tool "amazon-scraper-api", host amazon-scraper-api.omkar.cloud, served by
the rapidapi gateway's amazon_scraper module until 2026-09-23).

That host is a js-scraper LEGACY_HOST, so its /amazon/* paths now reach
this service. Its four endpoints keep working here:

    /amazon/search?query&page&country_code&sort_by        -> search.search
    /amazon/products/category?category_id&page&country_code&sort_by
                                                          -> search.category_products
    /amazon/product-details?asin&country_code             -> products.details
    /amazon/product-reviews/top?asin&country_code         -> products.reviews

/amazon/search is shared with the new API: `legacy_aliases` (a
MarketplaceSchema pre_load hook) renames the old `country_code` /
`sort_by` params to the new `country` / `sort` before validation, so both
spellings are accepted without exposing sibling params in the public spec.
The other three old paths are registered by `register()` (called from
routes.py) and stay out of the public API documentation. Responses use the
new shapes.
"""
from marshmallow import pre_load

from amazon import products, search

LEGACY_SORTS = {"relevance": "relevance", "lowest_price": "price_low_to_high", "highest_price": "price_high_to_low",
                "reviews": "reviews", "newest": "newest", "best_sellers": "best_sellers"}
_RENAMES = {"country_code": "country", "sort_by": "sort", "asin": "product", "category_id": "category"}


def legacy_aliases(data, fields):
    """Rename old param names to new ones (only for fields the schema has
    and only when the new name is absent); map old sort values."""
    data = dict(data)
    for old, new in _RENAMES.items():
        if old in data and new in fields and new not in data:
            value = data.pop(old)
            if old == "sort_by" and isinstance(value, str):
                value = LEGACY_SORTS.get(value.strip().lower(), value)
            data[new] = value
    return data


class LegacyAliasMixin:
    @pre_load
    def rename_legacy_params(self, data, **kwargs):
        return legacy_aliases(data, self.fields)


def register(route_fn, schemas):
    """Mount the retired paths (not part of the public spec)."""
    route_fn("/amazon/products/category", schemas.CategoryProductsSchema, search.category_products, paginated=True)
    route_fn("/amazon/product-details", schemas.ProductDetailsSchema, products.details)
    route_fn("/amazon/product-reviews/top", schemas.ProductSchema, products.reviews)
