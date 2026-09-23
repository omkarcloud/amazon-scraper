"""/amazon/best-sellers, /amazon/best-sellers/categories, /amazon/deals —
the zeitgeist lists (best sellers, new releases, movers & shakers, most
wished for, most gifted) and the Today's Deals grid.

Best sellers: each list is 100 products over 2 pages of 50. The page
renders 30 cards server-side and the client fetches the rest through the
card's ACP endpoint (`<acp-path>nextPage` with the recs-list entries as
ids) — replayed here so a page always carries all 50.

Deals: the /deals page embeds the grid widget config (csrf token, the
filter "bubbles", the first 30 products) and pages through
/d2b/api/v1/products/search. Neither carries prices — those come from the
data.amazon.<tld> product API (one call per ASIN, HTTP/1.1 session).
"""
import json

from amazon import fetch, parsers as P
from amazon.shared import context, fan_out, paged_result

LIST_TYPES = {"best_sellers": "bestsellers", "new_releases": "new-releases", "movers_and_shakers": "movers-and-shakers",
              "most_wished_for": "most-wished-for", "most_gifted": "most-gifted"}
RANK_PAGE_SIZE = 50
RANK_PAGES = 2
DEALS_PAGE_SIZE = 30
DEAL_SORTS = {"featured": None, "discount": "DISCOUNT_DESC", "price_low_to_high": "PRICE_ASC", "price_high_to_low": "PRICE_DESC",
              "newest": "NEWEST"}
AAPI_EXPAND = ("title(product.offer.title/v1),buyingOptions[].dealBadge(product.deal-badge/v1),"
               "buyingOptions[].dealDetails(product.deal-details/v1),buyingOptions[].price(product.price/v1)")


def _acp_hydrate(site, path, acp, start, count, locale=None):
    entries = acp["entries"][start:start + count]
    if not entries:
        return []
    body = {"faceoutkataname": acp["faceout"], "ids": [json.dumps(e, separators=(",", ":")) for e in entries],
            "indexes": list(range(start, start + len(entries))), "linkparameters": "{}", "offset": str(start),
            "reftagprefix": acp["reftag"], "isQuickViewEnabled": False}
    html = fetch.ajax(site["country"], acp["path"] + "nextPage", {"page-type": "zeitgeist-lists"}, method="POST", json_body=body,
                      headers={"x-amz-acp-params": acp["params"], "content-type": "application/json",
                               "accept": "text/html, application/json"},
                      referer=site["base"] + path, language=locale, label="best sellers page")
    meta = {e.get("id"): e.get("metadataMap") or {} for e in acp["entries"] if isinstance(e, dict)}
    return P.bestsellers_acp_page(html, site, meta)


def bestsellers(category="", list_type="best_sellers", country=None, language=None, page=1):
    site, locale = context(country, language)
    segment = LIST_TYPES[list_type]
    path = f"/gp/{segment}/{category}/".replace("//", "/") if category else f"/gp/{segment}/"
    params = {"pg": str(page)} if page > 1 else None
    html = fetch.page(site["country"], path, params, language=locale, label=f"{list_type} {category or 'root'}")
    data = P.bestsellers_page(html, site)
    items = data["items"]
    acp = data.get("acp")
    expected = data.get("expected_count") or 0
    if acp and expected > len(items):
        rendered = len(items)
        while rendered < expected:
            more = _acp_hydrate(site, path, acp, rendered, 30, locale)
            if not more:
                break
            items.extend(more)
            rendered += len(more)
    seen = set()
    unique = []
    for item in items:
        if item["asin"] in seen:
            continue
        seen.add(item["asin"])
        unique.append(item)
    unique.sort(key=lambda i: i.get("rank") or 999)
    has_more = page < RANK_PAGES and len(unique) >= RANK_PAGE_SIZE
    out = paged_result("items", unique, page, RANK_PAGE_SIZE, has_more=has_more,
                       title=data.get("title"), category=category or None, list_type=list_type,
                       link=site["base"] + path, categories=data.get("tree"), related_lists=data.get("tabs"),
                       country=site["country"], domain=site["host"])
    out["pagination"]["total_pages"] = RANK_PAGES if len(unique) >= RANK_PAGE_SIZE else page
    out["pagination"]["total_count"] = RANK_PAGE_SIZE * RANK_PAGES if len(unique) >= RANK_PAGE_SIZE else len(unique)
    if not unique:
        # category aliases differ per marketplace (amazon.de: ce-de, not
        # electronics) and some lists are empty at times (movers & shakers)
        out["note"] = ("Amazon returned no items for this list; check `categories` for the aliases this marketplace uses, "
                       "or the list may be empty right now.")
    return out


def bestseller_categories(category="", country=None, language=None):
    """The category tree shown beside a best sellers list: the
    marketplace's departments for the root, a department's subcategories
    otherwise."""
    site, locale = context(country, language)
    path = f"/gp/bestsellers/{category}/" if category else "/gp/bestsellers/"
    html = fetch.page(site["country"], path, language=locale, label="best sellers categories")
    data = P.bestsellers_page(html, site)
    rows = data.get("tree") or []
    selected = next((r for r in rows if r["is_selected"]), None)
    return {"category": category or None, "name": selected["name"] if selected else data.get("title"),
            "categories": [r for r in rows if not r["is_selected"]], "count": len([r for r in rows if not r["is_selected"]]),
            "country": site["country"], "domain": site["host"]}


def _deals_config(site, locale, collection=None):
    params = {"bubble-id": collection} if collection else None
    html = fetch.page(site["country"], "/deals", params, language=locale, label="deals")
    cfg = P.json_after(html, "assets.mountWidget('slot-14',", "{")
    if not isinstance(cfg, dict):
        import re
        match = re.search(r"assets\.mountWidget\('[\w\-]+',\s*(\{)", html)
        cfg = P.balanced_json(html, match.start(1)) if match else None
    if not isinstance(cfg, dict) or "productSearchResponse" not in cfg:
        raise fetch.AmazonUpstreamError("deals: grid widget config not found on the page")
    return cfg


def _hydrate_prices(site, csrf, products, currency, locale=None):
    accept = f'application/vnd.com.amazon.api+json; type="product/v2"; expand="{AAPI_EXPAND}"'
    # the cookies of the session that loaded /deals (keyed by its language)
    cookies = fetch.page_cookies(site["country"], locale)

    def one(product):
        raw = fetch.aapi(site["country"], f"/api/marketplaces/{site['marketplace_id']}/products/{product['asin']}",
                         accept=accept, csrf_token=csrf, currency=currency, cookies=cookies, label=f"deal price {product['asin']}")
        return P.aapi_product(raw, site)
    for product, result in zip(products, fan_out(one, products, workers=4)):
        product["deal"] = None if isinstance(result, Exception) else result
    return products


def deals(country=None, language=None, collection=None, department_id=None, brand=None, min_discount=None, max_discount=None,
          min_rating=None, prime_early_access=None, sort="featured", page=1, include_prices=True):
    site, locale = context(country, language)
    cfg = _deals_config(site, locale, collection)
    symphony = cfg.get("symphonyConfig") or {}
    bubbles = symphony.get("bubbles") or []
    bubble = next((b for b in bubbles if b.get("id") == collection), None) if collection else None
    if collection and bubble is None:
        raise fetch.AmazonBadRequest(f"unknown deals collection '{collection}'; one of: " + ", ".join(b.get("id") for b in bubbles if b.get("id")))
    attributes = (bubble or {}).get("attributes") or {}
    filter_info = attributes.get("filterInfo") or symphony.get("filterInfo") or {}
    rank_group = attributes.get("rankingStrategy") or symphony.get("rankingStrategy")
    refinement_filters = []
    if department_id:
        refinement_filters.append({"id": "departments", "value": [str(department_id)]})
    if brand:
        # brand refinement values are "<brand id>|<name>"; a bare name is
        # matched against the brands the grid currently lists
        value = brand
        if "|" not in brand:
            options = next((r.get("options") or [] for r in (cfg.get("productSearchResponse") or {}).get("refinements") or []
                            if r.get("id") == "brands"), [])
            for option in options:
                if (option.get("value") or "").split("|", 1)[-1].lower() == brand.lower():
                    value = option["value"]
                    break
        refinement_filters.append({"id": "brands", "value": [value]})
    if min_rating:
        refinement_filters.append({"id": "reviewRating", "value": [str(int(min_rating))]})
    if prime_early_access:
        refinement_filters.append({"id": "accessType", "value": ["2"]})
    range_filters = []
    if min_discount is not None or max_discount is not None:
        range_filters.append({"id": "percentOff", "min": int(min_discount or 0), "max": int(max_discount or 100)})
    # The page embeds the first 30 products of the default grid only; a
    # collection page renders its grid client-side, so every filtered /
    # sorted / paged / collection request goes through the d2b search API.
    use_embedded = page == 1 and not collection and not refinement_filters and not range_filters and sort in (None, "featured")
    if use_embedded:
        response = cfg.get("productSearchResponse") or {}
    else:
        params = {"pageSize": str(DEALS_PAGE_SIZE), "startIndex": str((page - 1) * DEALS_PAGE_SIZE), "calculateRefinements": "true",
                  "rankingContext": json.dumps({"pageTypeId": "deals", "rankGroup": rank_group}, separators=(",", ":")),
                  "filters": json.dumps(filter_info, separators=(",", ":"))}
        if refinement_filters:
            params["refinementFilters"] = json.dumps(refinement_filters, separators=(",", ":"))
        if range_filters:
            params["rangeRefinementFilters"] = json.dumps(range_filters, separators=(",", ":"))
        if DEAL_SORTS.get(sort):
            params["sortOrder"] = DEAL_SORTS[sort]
        response = fetch.ajax_json(site["country"], "/d2b/api/v1/products/search", params, referer=site["base"] + "/deals",
                                   headers={"accept": "application/json, text/plain, */*"}, language=locale, label="deals search")
        if not isinstance(response, dict) or "products" not in response:
            raise fetch.AmazonBadRequest(f"deals search rejected the filters: {str(response)[:120]}")
    products = [P.deal_product(p, site) for p in response.get("products") or [] if isinstance(p, dict)]
    if include_prices and products:
        _hydrate_prices(site, cfg.get("csrfToken"), products, site["currency"], locale)
    refinements = []
    for ref in (response.get("refinements") or cfg.get("productSearchResponse", {}).get("refinements") or []):
        refinements.append({"id": ref.get("id"), "name": ref.get("label"), "type": ref.get("type"),
                            "options": [{"name": " ".join(t.get("text") or "" for t in o.get("label") or []).strip(), "value": o.get("value"),
                                         "is_selected": o.get("isSelected")} for o in ref.get("options") or []],
                            "range": ref.get("availableRange")})
    has_more = bool(response.get("nextIndex")) and len(products) >= DEALS_PAGE_SIZE
    return paged_result("deals", products, page, DEALS_PAGE_SIZE, has_more=has_more,
                        collection=collection, collections=[{"id": b.get("id"), "name": b.get("label")} for b in bubbles if b.get("id")],
                        refinements=refinements, country=site["country"], domain=site["host"], currency=site["currency"])
