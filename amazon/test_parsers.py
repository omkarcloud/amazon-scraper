"""Offline tests for the Amazon refs, marshmallow schemas and parsers on
the captured pages (amazon/fixtures/*.gz, raw pages captured 2026-09-23
with the fetch layer: US-located sessions, USD prices) — no network.

    python -m pytest amazon/test_parsers.py -q
"""
import gzip
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from schema_fields import load_query  # noqa: E402
from amazon import parsers as P, refs, schemas as S, sites  # noqa: E402

FX = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")
US = sites.site("US")


def raw(name):
    with gzip.open(os.path.join(FX, name + ".gz"), "rt", encoding="utf-8") as f:
        return f.read()


# ---- refs ---------------------------------------------------------------------------------------------------

def test_resolve_product():
    assert refs.resolve_product("b00939i7ek") == "B00939I7EK"
    assert refs.resolve_product("0132350882") == "0132350882"
    assert refs.resolve_product("https://www.amazon.com/Ninja-Kitchen-System/dp/B00939I7EK/ref=sr_1_1?keywords=x") == "B00939I7EK"
    assert refs.resolve_product("https://www.amazon.de/gp/product/B0DS2LB2DT") == "B0DS2LB2DT"
    assert refs.resolve_product("amazon.co.uk/product-reviews/B0HGXMZ2SD") == "B0HGXMZ2SD"
    with pytest.raises(ValueError):
        refs.resolve_product("not-an-asin")
    with pytest.raises(ValueError):
        refs.resolve_product("https://www.amazon.com/s?k=laptop")


def test_resolve_seller_and_influencer():
    assert refs.resolve_seller("A1D09S7Q0OD6TH") == "A1D09S7Q0OD6TH"
    assert refs.resolve_seller("https://www.amazon.com/sp?ie=UTF8&seller=A02211013Q5HP3OMSZC7W&isAmazonFulfilled=1") == "A02211013Q5HP3OMSZC7W"
    assert refs.resolve_seller("https://www.amazon.com/s?me=A1OFNH7X27CXTU&marketplaceID=ATVPDKIKX0DER") == "A1OFNH7X27CXTU"
    assert refs.resolve_influencer("@tastemade") == "tastemade"
    assert refs.resolve_influencer("https://www.amazon.com/shop/tastemade?tag=x") == "tastemade"
    assert refs.resolve_post("https://www.amazon.com/shop/tastemade/list/1N84PQ3CI4NW0?tag=x") == {"handle": "tastemade", "id": "1N84PQ3CI4NW0"}
    assert refs.resolve_post("1N84PQ3CI4NW0") == {"handle": None, "id": "1N84PQ3CI4NW0"}


def test_resolve_node_and_bestseller_category():
    assert refs.resolve_node("172282") == "172282"
    assert refs.resolve_node("https://www.amazon.com/s?k=laptop&i=computers&rh=n%3A172282%2Cn%3A541966&dc") == "541966"
    assert refs.resolve_node("https://www.amazon.com/b?node=172282") == "172282"
    assert refs.resolve_bestseller_category("electronics") == "electronics"
    assert refs.resolve_bestseller_category("") == ""
    assert refs.resolve_bestseller_category("https://www.amazon.com/Best-Sellers-Software-Business-Office/zgbs/software/229535/ref=zg_bs_nav") == "software/229535"
    assert refs.resolve_bestseller_category("https://www.amazon.com/gp/new-releases/electronics/") == "electronics"
    assert refs.resolve_query("https://www.amazon.co.uk/s?k=head+phones&i=electronics") == "head phones"
    assert refs.country_of("https://www.amazon.co.uk/dp/B0HGXMZ2SD") == "GB"
    assert refs.country_of("B0HGXMZ2SD") is None
    assert refs.page_type("https://www.amazon.com/dp/B00939I7EK") == "product"
    assert refs.page_type("https://www.amazon.com/gp/bestsellers/software/") == "bestsellers"
    assert refs.page_type("https://www.amazon.com/s?k=laptop") == "search"


# ---- schemas ----------------------------------------------------------------------------------------------

def test_schemas_infer_country_from_links():
    data, err = load_query(S.ProductDetailsSchema, {"product": "https://www.amazon.co.uk/dp/B0HGXMZ2SD"})
    assert err is None and data["country"] == "GB" and data["product"] == "B0HGXMZ2SD"
    data, _ = load_query(S.ProductDetailsSchema, {"product": "https://www.amazon.co.uk/dp/B0HGXMZ2SD", "country": "de"})
    assert data["country"] == "DE"
    data, _ = load_query(S.SearchSchema, {"query": "laptop", "country": "uk", "is_prime": "1", "max_price": "500"})
    assert data["country"] == "GB" and data["is_prime"] is True and data["max_price"] == 500.0
    data, _ = load_query(S.BulkSchema, {"products": "B00939I7EK, https://www.amazon.com/dp/B09SM24S8C,b00939i7ek"})
    assert data["products"] == ["B00939I7EK", "B09SM24S8C"]
    _, err = load_query(S.BulkSchema, {})
    assert err and "products" in err["errors"]
    _, err = load_query(S.SearchSchema, {"query": "x", "sort": "nope"})
    assert err and "sort" in err["errors"]
    _, err = load_query(S.SearchSchema, {"query": "x", "bogus": "1"})
    assert err and "bogus" in err["errors"]
    data, _ = load_query(S.CategoryProductsSchema, {"category": "https://www.amazon.com/s?rh=n:172282,n:541966"})
    assert data["node"] == "541966" and "category" not in data


# ---- parsers: helpers --------------------------------------------------------------------------------------

def test_helpers():
    assert P.money("$1,299.99", "USD") == {"amount": 1299.99, "currency": "USD"}
    assert P.money("€749.00", "USD")["currency"] == "EUR"
    assert P.money("INR 372,862.44", "USD") == {"amount": 372862.44, "currency": "INR"}
    assert P.money("$4,699.00", "AUD") == {"amount": 4699.0, "currency": "AUD"}
    assert P.money("C$19.99", "USD")["currency"] == "CAD"
    assert P.rating_of("4.7 out of 5 stars") == 4.7
    assert P.rating_of("4,2 von 5 Sternen") == 4.2
    assert P.to_int("(43,049)") == 43049
    assert P.bought_past_month("6K+ bought in past month") == 6000
    assert P.bought_past_month("200+ bought in past month") == 200
    assert P.iso_date("July 26, 2026") == "2026-07-26"
    assert P.iso_date("27 August 2026") == "2026-08-27"
    assert P.full_image("https://m.media-amazon.com/images/I/61kp09SeFyL._AC_UY218_.jpg") == "https://m.media-amazon.com/images/I/61kp09SeFyL.jpg"
    assert P.absolute(US, "/Lenovo-X/dp/B0CBJ46QZX/ref=sr_1_5?dib=abc&keywords=laptop&qid=1&sr=8-5") == "https://www.amazon.com/Lenovo-X/dp/B0CBJ46QZX"
    assert P.node_in("/s?k=laptop&i=computers&rh=n%3A172282%2Cn%3A541966") == "541966"
    assert P.snake("Best Sellers Rank") == "best_sellers_rank"


# ---- parsers: search ---------------------------------------------------------------------------------------

def test_search_page():
    page = P.search_page(raw("search_laptop_p2.html"), US)
    assert page["total_count"] == 100000 and page["is_total_approximate"] and page["current_page"] == 2 and page["total_pages"] == 20
    results = page["results"]
    assert len(results) >= 16
    organic = [r for r in results if not r["is_sponsored"]]
    assert organic
    card = next(r for r in organic if r["price"] and r["rating"])
    assert card["asin"] and card["title"] and card["link"] == f"https://www.amazon.com/dp/{card['asin']}"
    assert card["price"]["currency"] == "USD" and card["price"]["amount"] > 0
    assert 0 < card["rating"]["average"] <= 5 and card["rating"]["count"] > 0
    assert card["image"].endswith(".jpg") and "._" not in card["image"]
    deal = next(r for r in results if r["badges"]["is_limited_time_deal"])
    assert deal["price"]["list_price"] and deal["price"]["list_price_type"] in ("Typical", "List")
    assert any(r["bought_past_month"] for r in results)
    brands = next(f for f in page["refinements"] if f["id"] == "p_123")
    assert brands["name"] == "Brands" and brands["options"][0]["value"] and brands["options"][0]["link"]
    assert any(a["id"] == "aps" for a in page["search_aliases"])


def test_search_page_de_and_isbn():
    page = P.search_page(raw("de_search_laptop.html"), sites.site("DE"))
    assert page["results"] and all(r["price"]["currency"] == "EUR" for r in page["results"] if r["price"])
    assert any(r["badges"]["is_amazons_choice"] for r in page["results"])
    assert any(r["is_sponsored"] for r in page["results"]) and any(not r["is_sponsored"] for r in page["results"])
    isbn = P.search_page(raw("search_isbn.html"), US)
    assert isbn["results"] and "Clean Code" in isbn["results"][0]["title"]
    node = P.search_page(raw("browse_node_172282.html"), US)
    assert node["departments"][0]["is_selected"] and node["departments"][1]["node_id"] == "281407"


# ---- parsers: product --------------------------------------------------------------------------------------

def test_product_page_blender():
    d = P.product_page(raw("dp_B00939I7EK_reviews.html"), US)
    assert d["asin"] == "B00939I7EK" and d["title"].startswith("Ninja Kitchen System Pro")
    assert d["brand"]["name"] == "Ninja"
    assert d["price"]["amount"] == 179.99 and d["price"]["currency"] == "USD"
    assert d["availability"]["is_in_stock"] is True
    assert d["buybox"]["is_sold_by_amazon"] and d["buybox"]["ships_from"] == "Amazon.com"
    assert d["delivery"]["text"].startswith("FREE delivery") and d["delivery"]["location"].startswith("New York")
    assert d["rating"]["average"] == 4.7 and d["rating"]["count"] == 43049
    assert d["rating"]["histogram"]["5_star"] == 85
    assert d["bought_past_month"] == 6000
    assert [c["name"] for c in d["categories"]][:2] == ["Home & Kitchen", "Kitchen & Dining"]
    assert d["best_sellers_rank"][0]["rank"] == 649 and d["best_sellers_rank"][0]["link"]
    assert d["best_sellers_rank"][1] == {"rank": 6, "category": "Food Processors", "link": "https://www.amazon.com/gp/bestsellers/kitchen/289920", "category_id": "289920"}
    assert len(d["bullets"]) >= 8 and d["overview"]["brand"] == "Ninja"
    assert d["details"]["model_number"] == "BL770" and d["identifiers"]["upc"].startswith("622356532419")
    assert "best_sellers_rank" not in d["details"]
    assert d["images"][0]["link"].startswith("https://m.media-amazon.com/") and d["images"][0]["variant"] == "MAIN"
    assert d["videos"] and d["videos"][0]["duration_seconds"] and d["videos"][0]["link"]
    assert d["variations"]["current_asin"] == "B00939I7EK" and d["variations"]["dimensions"][0]["name"] == "Style"
    assert any(p["asin"] == "B00939FV8K" for p in d["variations"]["products"])
    assert d["customers_say"]["summary"].startswith("Customers find") and d["customers_say"]["aspects"][0]["sentiment"]
    assert len(d["top_reviews"]) == 13 and d["has_inline_reviews"]
    review = d["top_reviews"][0]
    assert review["id"] == "RVNTQROOLDP79" and review["rating"] == 5.0 and review["date"] == "2026-07-26"
    assert review["country"] == "United States" and review["is_verified_purchase"] and review["helpful_votes"] == 40
    assert review["author"]["name"] and review["author"]["link"].startswith("https://www.amazon.com/gp/profile/")
    assert review["text"] and "double tap" not in review["text"]


def test_product_page_variants_book_gb():
    d = P.product_page(raw("dp_B09SM24S8C.html"), US)
    assert d["brand"]["name"] == "Samsung" and d["brand"]["byline"] == "Amazon Renewed"
    assert d["availability"]["quantity_left"] == 2
    assert d["buybox"]["seller"] == {"name": "BesTechDeal", "id": "A1OFNH7X27CXTU", "link": "https://www.amazon.com/sp?seller=A1OFNH7X27CXTU"}
    assert {dim["name"] for dim in d["variations"]["dimensions"]} == {"Color", "Size"}
    assert d["variations"]["products"][0]["attributes"]["Size"] == "32GB"
    assert d["important_information"]
    book = P.product_page(raw("dp_0132350882.html"), US)
    assert book["authors"][0]["name"] == "Robert C. Martin" and book["authors"][0]["role"] == "Author"
    assert book["details"]["isbn_13"] == "978-0132350884" and book["identifiers"]["isbn_10"]
    assert book["best_sellers_rank"][0]["category"] == "Books"
    gb = P.product_page(raw("gb_dp_B0HGXMZ2SD.html"), sites.site("GB"))
    assert gb["price"]["currency"] == "GBP" and gb["link"] == "https://www.amazon.co.uk/dp/B0HGXMZ2SD"
    assert gb["top_reviews"][0]["country"] == "United Kingdom" and gb["top_reviews"][0]["images"]
    assert P.review_treatment(raw("dp_B00939I7EK.html")) in ("C", "T1")


# ---- parsers: offers / best sellers ---------------------------------------------------------------------------

AIRPODS_IMAGES = ["61SUj2aKoEL._AC_SL1500_.jpg", "611pEx7220L._AC_SL1500_.jpg", "81Zg3Ev2M5L._AC_SL1500_.jpg",
                  "41QztOA1UuL._AC_SL1200_.jpg", "71d1WoCdwwL._AC_SL1500_.jpg", "71wkJxedzjL._AC_SL1500_.jpg",
                  "51RcxVAXDWL._AC_SL1500_.jpg"]


@pytest.mark.parametrize("country,asin", [("DE", "B0D1XD1ZV3"), ("GB", "B0D1XD1ZV3"), ("US", "B0D1XD1ZV3"),
                                          ("DE", "B0FRN2RD1H"), ("GB", "B0FRN2RD1H"), ("US", "B0FRN2RD1H")])
def test_product_page_images_from_the_image_block(country, asin):
    """Pages captured 2026-11-25 (trimmed to the image blocks): the gallery is
    the ImageBlockATF JS literal ('colorImages': {'initial':
    A.$.parseJSON('[…]')}), and the variation widget's "colorImages" JSON is
    {} on DE/GB and on products without colour variations — those answered
    "images": [] before. Hi-res first, MAIN first, each picture once."""
    d = P.product_page(raw(f"{country.lower()}_dp_{asin}_images.html"), sites.site(country))
    images = d["images"]
    assert len(images) == 7
    assert [i["variant"] for i in images] == ["MAIN", "PT01", "PT02", "PT03", "PT04", "PT05", "PT06"]
    assert all(i["link"].startswith("https://m.media-amazon.com/images/I/") and "_SL1" in i["link"] for i in images)
    assert all(i["thumbnail"] and i["large"] for i in images)
    assert len({i["link"] for i in images}) == 7
    if asin == "B0D1XD1ZV3":
        assert [i["link"].rsplit("/", 1)[1] for i in images] == AIRPODS_IMAGES


def test_images_fallbacks():
    """Without the gallery block: the variation widget's group of the colour
    on the page, then the main <img> whatever its attribute order (Amazon
    writes data-old-hires BEFORE id), its biggest data-a-dynamic-image entry
    when it has no data-old-hires; a \\' inside the parseJSON string."""
    widget = ('"landingAsinColor":"Red","colorImages":{"Blue":[{"hiRes":"https://m.media-amazon.com/images/I/B1.jpg"}],'
              '"Red":[{"hiRes":null,"large":"https://m.media-amazon.com/images/I/R1._AC_.jpg",'
              '"main":{"https://m.media-amazon.com/images/I/R1._AC_SX300_.jpg":[300,300],'
              '"https://m.media-amazon.com/images/I/R1._AC_SX679_.jpg":[679,679]},"variant":"MAIN"}]}')
    assert [i["link"] for i in P._images(widget)] == ["https://m.media-amazon.com/images/I/R1._AC_SX679_.jpg"]
    tag = ('<img alt="x" src="https://m.media-amazon.com/images/I/M1._AC_SX342_.jpg" '
           'data-old-hires="https://m.media-amazon.com/images/I/M1._AC_SL1500_.jpg" class="a-dynamic-image" id="landingImage">')
    assert P._images('"colorImages":{}' + tag) == [{"link": "https://m.media-amazon.com/images/I/M1._AC_SL1500_.jpg",
                                                     "thumbnail": None, "large": "https://m.media-amazon.com/images/I/M1._AC_SX342_.jpg",
                                                     "variant": "MAIN"}]
    dynamic = ('<img src="https://m.media-amazon.com/images/I/D1._SX38_.jpg" data-old-hires="" id="imgBlkFront" '
               'data-a-dynamic-image="{&quot;https://m.media-amazon.com/images/I/D1._SX38_.jpg&quot;:[38,50],'
               '&quot;https://m.media-amazon.com/images/I/D1._SY466_.jpg&quot;:[350,466]}">')
    assert P._images(dynamic)[0]["link"] == "https://m.media-amazon.com/images/I/D1._SY466_.jpg"
    quoted = ("'colorImages': { 'initial': A.$.parseJSON('[{\"hiRes\":\"https://m.media-amazon.com/images/I/Q1.jpg\","
              "\"altText\":\"Kid\\'s set\"},{\"hiRes\":\"https://m.media-amazon.com/images/I/Q1._AC_SL1000_.jpg\"}]')}")
    assert [i["link"] for i in P._images(quoted)] == ["https://m.media-amazon.com/images/I/Q1.jpg"]
    assert P._images("") == [] and P._images('"colorImages":{}') == []


def test_product_page_jewelry_facts_layout():
    d = P.product_page(raw("dp_B07QSFHT27.html"), US)
    assert d["title"].startswith("PAVOI") and d["bullets"][0].startswith("HIGHLIGHTS")
    assert d["details"]["metal_type"] == "14k gold plated" and d["brand"]["name"]


def test_offers_page():
    o = P.offers_page(raw("aod_B00939I7EK.html"), US)
    assert o["title"].startswith("Ninja") and o["rating"]["count"] == 43049 and o["other_offers_count"] == 4
    offers = o["offers"]
    assert offers[0]["is_pinned"] and offers[0]["condition"] == "New" and offers[0]["price"]["amount"] == 179.99
    assert offers[0]["price"]["list_price"] == 219.99 and offers[0]["price"]["savings_percent"] == 18.0
    used = next(x for x in offers if x["condition"].startswith("Used"))
    assert used["condition_note"] and used["price"]["amount"] and used["seller"]["name"]
    third = next(x for x in offers if x["seller"] and x["seller"]["id"])
    assert third["seller"]["link"].endswith(third["seller"]["id"]) and third["seller"]["positive_percent"]
    assert all(x["delivery"]["text"] for x in offers)


def test_bestsellers_page_and_acp():
    b = P.bestsellers_page(raw("bestsellers_electronics.html"), US)
    assert b["title"] == "Amazon Best Sellers" and len(b["items"]) == 30 and b["expected_count"] == 50
    first = b["items"][0]
    assert first["rank"] == 1 and first["asin"] == "B08JHCVHTY" and first["link"] == "https://www.amazon.com/dp/B08JHCVHTY"
    second = b["items"][1]
    assert second["rating"]["average"] == 4.5 and second["rating"]["count"] == 18166 and second["price"] == {"amount": 19.0, "currency": "USD"}
    assert b["acp"]["path"].startswith("/acp/") and b["acp"]["params"].startswith("tok=") and len(b["acp"]["entries"]) == 50
    tree = b["tree"]
    assert tree[0]["name"] == "Any Department" and tree[0]["is_root"]
    assert next(t for t in tree if t["is_selected"])["name"] == "Electronics"
    assert any(t["path"] == "electronics/281407" for t in tree)
    meta = {e["id"]: e["metadataMap"] for e in b["acp"]["entries"]}
    more = P.bestsellers_acp_page(raw("bestsellers_electronics_acp.html"), US, meta)
    assert len(more) == 20 and more[0]["rank"] == 31 and more[-1]["rank"] == 50
    root = P.bestsellers_page(raw("bestsellers_root.html"), US)
    assert len([t for t in root["tree"] if t["path"]]) >= 30


# ---- parsers: sellers / influencers / deals / autocomplete ------------------------------------------------------

def test_seller_page_and_feedback():
    s = P.seller_page(raw("seller_A1D09S7Q0OD6TH.html"), US)
    assert s["id"] == "A1D09S7Q0OD6TH" and s["name"] == "FBA Top G"
    assert s["storefront_link"].endswith("me=A1D09S7Q0OD6TH")
    assert s["rating"]["positive_percent"] == 84.0 and s["rating"]["count"] == 304
    assert s["ratings"]["lifetime"]["stars"]["5_star"] == 235 and s["ratings"]["one_month"]["count"] == 0
    assert s["business"]["name"] == "Fba fund llc" and "NJ" in s["business"]["address"]
    assert s["about"].startswith("FBA Top G is committed")
    fb = json.loads(raw("seller_feedback_A1D09S7Q0OD6TH.json"))
    item = P.feedback_item(fb["details"][0], US)
    assert item["rating"] == 1 and item["date"] == "2024-08-08" and item["author"]["name"] == "Terry Brogan"


def test_influencer_pages():
    inf = P.influencer_page(raw("influencer_tastemade.html"), US, "tastemade")
    assert inf["name"] == "Tastemade" and inf["is_top_creator"] and inf["description"].startswith("A global community")
    assert inf["posts_count_on_page"] == 20 and inf["has_more_posts"] and inf["next_page_token"]
    post = inf["posts"][0]
    assert post["id"] == "amzn1.ideas.2H2INLQR7GDF2" and post["type"] == "list" and post["items_count"] == 15 and post["likes"] == 94
    assert post["link"] == "https://www.amazon.com/shop/tastemade/list/2H2INLQR7GDF2" and post["product_asins"][0].startswith("B0")
    assert any(p["type"] == "video" for p in inf["posts"])
    lst = P.influencer_list_page(raw("influencer_list_1N84PQ3CI4NW0.html"), US)
    assert lst["title"] and len(lst["products"]) >= 10
    assert lst["products"][0]["asin"] == "B0BFDZXVZ1" and lst["products"][0]["price"]["amount"] == 14.99 and lst["products"][0]["brand"]


def test_deal_product_and_suggestion():
    html = raw("deals.html")
    cfg = P.json_after(html, "assets.mountWidget('slot-14',", "{")
    assert cfg and cfg["csrfToken"] and len(cfg["symphonyConfig"]["bubbles"]) > 20
    product = P.deal_product(cfg["productSearchResponse"]["products"][0], US)
    assert product["asin"] and product["title"] and product["link"].startswith("https://www.amazon.com/") and product["image"]
    assert product["deal"] is None
    ac = json.loads(raw("autocomplete_lap.json"))
    rows = [P.suggestion(s) for s in ac["suggestions"]]
    assert rows[0]["value"] == "laptop" and rows[0]["type"] == "keyword"


def test_aapi_product_shape():
    raw_entity = {"entity": {"buyingOptions": [{"price": {"entity": {"priceToPay": {"moneyValueOrRange": {"value": {"amount": "104.99", "unit": "USD"}}},
                                                                     "basisPrice": {"moneyValueOrRange": {"value": {"amount": "299.99", "unit": "USD"}}},
                                                                     "savings": {"percentage": {"value": 65}}}},
                                                 "dealBadge": {"entity": {"label": {"content": {"fragments": [{"text": "65% off"}]}},
                                                                          "messaging": {"content": {"fragments": [{"text": "Limited time deal"}]}}}},
                                                 "dealDetails": {"entity": {"type": "BEST_DEAL", "state": "AVAILABLE", "id": "52f15f5c", "endTime": "2026-10-08T06:59:59.000Z"}}}]}}
    deal = P.aapi_product(raw_entity, US)
    assert deal == {"price": 104.99, "currency": "USD", "list_price": 299.99, "savings_percent": 65.0, "badge": "65% off",
                    "message": "Limited time deal", "type": "BEST_DEAL", "state": "AVAILABLE", "ends_at": "2026-10-08T06:59:59.000Z", "id": "52f15f5c"}


def test_parsers_never_crash_on_empty_pages():
    for fn in (P.search_page, P.product_page, P.offers_page, P.bestsellers_page, P.seller_page, P.influencer_list_page):
        assert isinstance(fn("<html></html>", US), dict)
    assert P.influencer_page("<html></html>", US, "x")["posts"] == []
    assert P.bestsellers_acp_page("", US) == []


# ---- amazon.de (2026-10-16 fixes) -----------------------------------------------------------------------------

DE = sites.site("DE")


def test_locale_cookie_and_accept_language():
    # the language cookie is per marketplace; only amazon.com uses lc-main
    assert sites.locale_cookie(sites.site("US")) == "lc-main"
    assert sites.locale_cookie(DE) == "lc-acbde"
    assert sites.locale_cookie(sites.site("GB")) == "lc-acbuk"
    assert sites.locale_cookie(sites.site("JP")) == "lc-acbjp"
    assert sites.accept_language("de_DE") == "de-DE,de;q=0.9,en;q=0.5"
    assert sites.accept_language("en_GB") == "en-GB,en;q=0.9"


def test_click_through_page_detected():
    from amazon import fetch
    page = raw("de_click_through.html")
    assert fetch.is_click_through(page) and fetch.is_captcha(page)
    form = fetch._CLICK_FORM_RE.search(page)
    assert form.group(1) == "/errors_page/validateCaptcha"
    assert {name for name, _ in fetch._HIDDEN_RE.findall(form.group(2))} == {"amzn", "amzn-r", "field-keywords"}
    assert not fetch.is_click_through(raw("dp_B00939I7EK.html"))


def test_product_page_de_rrp_and_unit_price():
    # Berlin-located German page: price, UVP (strike) and the per-kg price,
    # which precedes the UVP in the markup and used to be read as list_price
    data = P.product_page(raw("de_dp_B0057ED9AM.html"), DE)
    assert data["title"].startswith("ESN Ultrapure Creatine Monohydrate")
    assert data["bullets"][0].startswith("Mit 3 g Kreatin")
    price = data["price"]
    assert price["currency"] == "EUR" and price["amount"] and price["unit_price"]["per"] == "kg"
    assert price["list_price"] is None or price["list_price"] > price["amount"]
    assert price["unit_price"]["amount"] == pytest.approx(price["amount"] * 2, rel=0.01)   # 500 g tub


def test_product_page_de_subscribe_and_save():
    # one-time price == "Einmaliger Preis" basis; the -5 % badge is the Spar-Abo price
    price = P.product_page(raw("de_dp_B09JGFTTRX.html"), DE)["price"]
    assert price["amount"] and price["list_price"] is None and price["savings_percent"] is None
    assert price["unit_price"] == {"amount": price["amount"], "per": "kg"}   # 1 kg bag


def test_product_page_de_sessionless_render_hides_price():
    # Amazon's degraded render (no session-id cookie): the one-time price is
    # "add to cart to see", only the Spar-Abo (-15 %) price is in #buybox.
    # It must not be reported as the buybox price (24.75 was, 2026-10-19).
    data = P.product_page(raw("de_dp_B0057ED9AM_sessionless.html"), DE)
    assert data["title"].startswith("ESN Ultrapure Creatine Monohydrate")   # title / bullets still parse
    assert data["bullets"]
    assert data["price"] is None and data["availability"] is None
    data = P.product_page(raw("de_dp_B0G58MSZYD_sessionless.html"), DE)
    assert data["title"].startswith("Vitamin D3 K2 Hevert") and data["price"] is None
    # the good render of the same product still yields the one-time price (29.12), not the Spar-Abo one (24.75)
    good = P.product_page(raw("de_dp_B0057ED9AM.html"), DE)["price"]
    assert good["amount"] == 29.12


def test_fallback_render_detection():
    from types import SimpleNamespace
    from amazon import fetch

    class Cookie:
        def __init__(self, name):
            self.name = name

    def sess(*names):
        return SimpleNamespace(curl=SimpleNamespace(cookies=SimpleNamespace(jar=[Cookie(n) for n in names])))

    html = "<!doctype html><html lang=de-de>" + "x" * 60_000
    # the real thing: fallback header, no content-type, no session cookie
    degraded = SimpleNamespace(status_code=200, headers={"x-edgex-ers-fallback-status": "success"}, text=html)
    assert fetch.is_fallback_render(sess("ak_bmsc", "i18n-prefs", "lc-acbde"), degraded)
    # header alone is enough, even if a cookie slipped in
    assert fetch.is_fallback_render(sess("session-id"), degraded)
    # no header, but a full page that opened no session
    quiet = SimpleNamespace(status_code=200, headers={}, text=html)
    assert fetch.is_fallback_render(sess("i18n-prefs", "lc-acbde"), quiet)
    # normal render
    good = SimpleNamespace(status_code=200, headers={"content-type": "text/html;charset=UTF-8"}, text=html)
    assert not fetch.is_fallback_render(sess("i18n-prefs", "session-id", "ubid-acbde"), good)
    # stubs / interstitials and JSON are judged elsewhere
    assert not fetch.is_fallback_render(sess(), SimpleNamespace(status_code=200, headers={}, text="<html>" + "x" * 2_000))
    assert not fetch.is_fallback_render(sess(), SimpleNamespace(status_code=200, headers={}, text="{" + "x" * 60_000))
    assert not fetch.is_fallback_render(sess(), SimpleNamespace(status_code=503, headers={}, text=html))


def test_egress_policy_datacenter_first_then_residential_for_good(monkeypatch):
    from types import SimpleNamespace
    from amazon import fetch, sites

    monkeypatch.setattr(fetch.config, "AMAZON_EGRESS", "datacenter")
    monkeypatch.setattr(fetch.config, "AMAZON_DATACENTER_FAILURES", 2)
    monkeypatch.setattr(fetch.config, "AMAZON_PROXY_COUNTRY", None)
    monkeypatch.setattr(fetch.config, "amazon_datacenter_proxy", lambda: "http://dc", raising=False)
    monkeypatch.setattr(fetch.config, "amazon_fallback_proxy", lambda c: f"http://resi-{c.lower()}")
    monkeypatch.setattr(fetch.config, "amazon_country_proxy", lambda c: f"http://pinned-{c.lower()}")
    monkeypatch.setattr(fetch, "_dc_failures", {})
    monkeypatch.setattr(fetch, "_residential", set())
    de, us, jp = sites.site("DE"), sites.site("US"), sites.site("JP")

    assert fetch._proxy_for(de) == ("http://dc", "datacenter")          # never the pod's own IP
    assert fetch._proxy_for(jp) == ("http://pinned-jp", "residential")   # refuses foreign exits: pinned
    dc_sess = SimpleNamespace(site=de, egress="datacenter", proxy="http://dc")
    fetch._note_failure(dc_sess, "captcha")
    assert fetch._proxy_for(de) == ("http://dc", "datacenter")          # one failure: fresh DC exit
    fetch._note_failure(dc_sess, "edge fallback render")
    assert fetch._proxy_for(de) == ("http://resi-de", "residential")     # second: residential, for good
    assert fetch._proxy_for(us) == ("http://dc", "datacenter")          # other marketplaces unaffected
    resi_sess = SimpleNamespace(site=de, egress="residential", proxy="http://resi-de")
    for _ in range(5):
        fetch._note_failure(resi_sess, "503 dogs page")                  # nowhere further to go
    assert fetch._proxy_for(de) == ("http://resi-de", "residential")

    monkeypatch.setattr(fetch.config, "AMAZON_EGRESS", "residential")
    assert fetch._proxy_for(us) == ("http://resi-us", "residential")
    monkeypatch.setattr(fetch.config, "amazon_fallback_proxy", lambda c: None)   # switch disabled ("")
    assert fetch._proxy_for(us) == ("http://pinned-us", "residential")   # still never the process's IP
    assert fetch._proxy_for(de) == ("http://pinned-de", "residential")
    monkeypatch.setattr(fetch.config, "AMAZON_EGRESS", "datacenter")
    assert fetch._proxy_for(de) == ("http://dc", "datacenter")          # moved, but nowhere to move to
    monkeypatch.setattr(fetch.config, "amazon_fallback_proxy", lambda c: f"http://resi-{c.lower()}")
    monkeypatch.setattr(fetch.config, "AMAZON_EGRESS", "direct")
    assert fetch._proxy_for(us) == (None, "direct")                      # the open-source default
    assert fetch._proxy_for(de) == ("http://resi-de", "residential")     # a moved marketplace stays moved


# ---- search by image ----------------------------------------------------------------------

def test_image_search_answer():
    """A real StyleSnap upload answer (amazon.com, 2026-11-20: a laptop photo):
    57 matches, details for the first 30."""
    out = P.image_search(json.loads(raw("stylesnap_us.json")), sites.site("US"))
    assert out["matches_count"] == 57 and len(out["results"]) == 30 and len(out["more_results"]) == 27
    first = out["results"][0]
    assert first["asin"] == "B0CBJ46QZX" and first["brand"] == "Lenovo" and first["title"].startswith("Lenovo")
    assert first["price"] == {"amount": 238.95, "currency": "USD", "list_price": None}
    assert first["rating"] == {"average": 4.3, "count": 1436} and first["availability"] == "in_stock_scarce"
    assert first["position"] == 1 and first["match_score"] == 0.7771 and first["link"] == "https://www.amazon.com/dp/B0CBJ46QZX"
    assert first["image"] == "https://m.media-amazon.com/images/I/71pLH5eO6jL.jpg" and len(first["variation_asins"]) == 3
    assert [r["position"] for r in out["results"]] == list(range(1, 31))
    assert [m["position"] for m in out["more_results"]] == list(range(31, 58))
    assert out["more_results"][0]["link"].startswith("https://www.amazon.com/dp/") and out["more_results"][0]["match_score"]
    assert out["detected_object"] == {"x": 0.1, "y": 0.1, "width": 0.8, "height": 0.8}
    assert out["search_link"].startswith("https://www.amazon.com/s?") and "p_78:B0CBJ46QZX|" in out["search_link"]


def test_image_search_details_out_of_order_and_nothing_found():
    payload = {"searchResults": [{"bbxAsinList": ["B0000000A1", "B0000000B2", "B0000000C3"],
                                  "subContent": [{"contentType": "ASIN", "content": "B0000000C3", "properties": {"score": "0.5"}}],
                                  "bbxAsinMetadataList": [{"asin": "B0000000C3", "price": "€1.234,50", "totalReviewCount": None}]}]}
    out = P.image_search(payload, sites.site("DE"))
    assert out["results"][0]["position"] == 3 and out["results"][0]["price"]["amount"] == 1234.5
    assert out["results"][0]["price"]["currency"] == "EUR" and out["results"][0]["rating"] is None
    assert [m["asin"] for m in out["more_results"]] == ["B0000000A1", "B0000000B2"]
    assert P.image_search({"queryId": "x", "searchResults": []}, sites.site("US")) == {
        "detected_object": None, "matches_count": 0, "results": [], "more_results": [], "search_link": None}
    # a detailed match Amazon left out of its order is counted (after the ordered ones)
    payload["searchResults"][0]["bbxAsinMetadataList"].append({"asin": "B0000000D4"})
    out = P.image_search(payload, sites.site("DE"))
    assert out["matches_count"] == 4 and [(r["asin"], r["position"]) for r in out["results"]] == [
        ("B0000000C3", 3), ("B0000000D4", None)]
    only_details = {"searchResults": [{"bbxAsinMetadataList": [{"asin": "B0000000D4"}]}]}
    assert P.image_search(only_details, sites.site("DE"))["matches_count"] == 1


def test_image_search_schema():
    assert S.ImageSearchSchema().load({"image": "m.media-amazon.com/images/I/71pLH5eO6jL.jpg", "country": "uk"}) == {
        "image": "https://m.media-amazon.com/images/I/71pLH5eO6jL.jpg", "country": "GB", "language": None}
    with pytest.raises(Exception, match="these marketplaces"):
        S.ImageSearchSchema().load({"image": "https://example.com/a.jpg", "country": "CA"})
    with pytest.raises(Exception, match="data: URI"):
        S.ImageSearchSchema().load({"image": "data:image/jpeg;base64,AAAA"})
