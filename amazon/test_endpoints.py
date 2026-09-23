"""Live endpoint smoke tests: one call per /amazon/* route against a running
service, with example values proven to return data (2026-09-23). The
listing tooling reads each route's FIRST call from this file's AST as its
working example, so the values stay literals.

Skipped unless AMAZON_BASE points at a running service:

    ONLY_SCRAPER=amazon python run.py            # or any bottle runner
    AMAZON_BASE=http://127.0.0.1:6002 python -m pytest amazon/test_endpoints.py -q
"""
import os

import pytest

BASE = os.environ.get("AMAZON_BASE", "").rstrip("/")

pytestmark = pytest.mark.skipif(not BASE, reason="set AMAZON_BASE to run live endpoint tests")


def call(path, **params):
    from curl_cffi import requests
    resp = requests.get(BASE + path, params=params, timeout=300)
    assert resp.status_code == 200, f"{path} {params} -> {resp.status_code} {resp.text[:300]}"
    body = resp.json()
    assert body, f"{path} returned an empty body"
    return body


def status(path, **params):
    from curl_cffi import requests
    return requests.get(BASE + path, params=params, timeout=300).status_code


def test_search():
    body = call("/amazon/search", query="raspberry pi")
    assert body["results"] and body["count"] > 1000 and body["next"] and body["results"][0]["asin"]
    body = call("/amazon/search", query="laptop", page="2", sort="price_low_to_high", min_price="300", max_price="800", brand="Lenovo")
    prices = [r["price"]["amount"] for r in body["results"] if r["price"] and not r["is_sponsored"]]
    assert prices and prices[0] <= prices[-1] and sum(300 <= p <= 800 for p in prices) >= len(prices) * 0.8
    body = call("/amazon/search", query="coffee maker", is_prime="true", four_stars_and_up="true")
    assert body["applied_refinements"] and body["results"]
    body = call("/amazon/search", query="https://www.amazon.de/s?k=laptop")
    assert body["country"] == "DE" and body["results"][0]["price"]["currency"] == "EUR"
    assert call("/amazon/search/autocomplete", query="raspberry")["suggestions"][0]["value"].startswith("raspberry")
    assert any(c["id"] == "electronics" for c in call("/amazon/categories")["categories"])
    tree = call("/amazon/categories/tree", category="172282")
    assert tree["name"] == "Electronics" and tree["children"][0]["node_id"]
    body = call("/amazon/categories/products", category="541966", sort="best_sellers")
    assert body["results"] and body["node_id"] == "541966"
    body = call("/amazon/products/lookup", identifier="9780735211292")
    assert body["results"] and "Atomic Habits" in body["results"][0]["title"]


def test_products():
    body = call("/amazon/products/details", product="B07QSFHT27")
    assert body["title"].startswith("PAVOI") and body["price"]["currency"] == "USD" and body["rating"]["count"] > 10000
    assert body["images"] and body["bullets"] and body["variations"]["products"]
    body = call("/amazon/products/details", product="B00939I7EK")
    assert body["best_sellers_rank"]
    assert body["buybox"]["seller"]["name"] and body["delivery"]["location"].startswith("New York")
    body = call("/amazon/products/details", product="https://www.amazon.de/dp/B0DS2LB2DT")
    assert body["country"] == "DE" and body["price"]["currency"] == "EUR"
    body = call("/amazon/products/details", product="B096Y264MM", country="JP")
    assert body["price"]["currency"] == "JPY"
    assert status("/amazon/products/details", product="B0C1H26C46") == 404
    body = call("/amazon/products/offers", product="B07QSFHT27")
    assert body["offers"] and body["offers"][0]["is_pinned"] and body["offers"][0]["price"]["amount"]
    body = call("/amazon/products/variations", product="B07QSFHT27")
    assert body["count"] >= 2 and body["dimensions"]
    body = call("/amazon/products/reviews", product="B07QSFHT27")
    assert body["rating"]["histogram"] and body["customers_say"]["summary"]
    assert body["reviews"] or body["is_partial"]
    body = call("/amazon/products/bulk", products="B07QSFHT27,B0CRSNCJ6Y,0735211299")
    assert len(body["products"]) == 3 and all(p.get("title") for p in body["products"])
    body = call("/amazon/scrape", url="https://www.amazon.com/dp/B0CRSNCJ6Y")
    assert body["page_type"] == "product" and body["title"]
    body = call("/amazon/scrape", url="https://www.amazon.com/gp/bestsellers/software/")
    assert body["page_type"] == "bestsellers" and body["items"]


def test_rankings_and_deals():
    body = call("/amazon/best-sellers", category="electronics")
    assert len(body["items"]) == 50 and body["items"][0]["rank"] == 1 and body["items"][-1]["rank"] == 50 and body["next"]
    body = call("/amazon/best-sellers", category="electronics", page="2")
    assert body["items"][0]["rank"] == 51
    body = call("/amazon/best-sellers", category="electronics", list_type="new_releases")
    assert body["items"] and body["list_type"] == "new_releases"
    body = call("/amazon/best-sellers", category="ce-de", country="DE")
    assert body["items"][0]["price"]["currency"] == "EUR"
    body = call("/amazon/best-sellers/categories")
    assert body["count"] > 30 and any(c["path"] == "electronics" for c in body["categories"])
    body = call("/amazon/best-sellers/categories", category="electronics")
    assert any(c["path"].startswith("electronics/") for c in body["categories"] if c["path"])
    body = call("/amazon/deals")
    assert body["deals"] and body["collections"] and body["deals"][0]["deal"] and body["deals"][0]["deal"]["price"]
    body = call("/amazon/deals", min_discount="50", sort="discount")
    assert all(d["deal"]["savings_percent"] >= 50 for d in body["deals"] if d["deal"] and d["deal"].get("savings_percent"))
    body = call("/amazon/deals", country="DE", include_prices="false", page="2")
    assert body["deals"] and body["currency"] == "EUR"


def test_legacy_paths():
    body = call("/amazon/product-details", asin="B07QSFHT27", country_code="US")
    assert body["title"].startswith("PAVOI")
    body = call("/amazon/products/category", category_id="172282", sort_by="lowest_price")
    assert body["results"]
    body = call("/amazon/search", query="laptop", country_code="GB", sort_by="newest")
    assert body["country"] == "GB" and body["results"]
    assert call("/amazon/product-reviews/top", asin="B07QSFHT27")["asin"] == "B07QSFHT27"


def test_sellers_and_influencers():
    body = call("/amazon/sellers/details", seller="A1H1EU8178QTRH")
    assert body["name"] == "PAVOI Jewelry" and body["ratings"]["lifetime"]["count"] > 100
    body = call("/amazon/sellers/feedback", seller="A1H1EU8178QTRH")
    assert body["feedback"] and body["feedback"][0]["rating"] and body["feedback"][0]["date"]
    body = call("/amazon/sellers/products", seller="A1H1EU8178QTRH")
    assert body["results"] and body["seller_id"] == "A1H1EU8178QTRH"
    body = call("/amazon/influencers/details", influencer="tastemade")
    assert body["name"] == "Tastemade" and body["is_top_creator"] and body["posts"]
    body = call("/amazon/influencers/posts", influencer="tastemade", post_type="list")
    assert body["posts"] and all(p["type"] == "list" for p in body["posts"])
    body = call("/amazon/influencers/posts/products", post="https://www.amazon.com/shop/tastemade/list/1N84PQ3CI4NW0")
    assert body["products"] and body["products"][0]["asin"] and body["title"]
