"""/amazon/search, /amazon/categories/products, /amazon/products/lookup,
/amazon/search/autocomplete, /amazon/categories, /amazon/categories/tree —
the /s result pages and the completion service.

Filters that Amazon encodes as marketplace-specific refinement ids
(Prime, 4 stars & up, condition, deals) are resolved from the page's own
refinement sidebar: the first fetch reads the ids for this marketplace and
keyword, the second applies them. Universal params (keyword, department
alias, browse node, price bounds, brand name, seller, sort, page) go
straight into the URL.
"""
import re

from amazon import fetch, parsers as P, refs
from amazon.shared import context, paged_result

PER_PAGE = 24            # organic cards per page on the desktop layout (16-60 depending on layout)
MAX_PAGES = 400

SORTS = {
    "relevance": "relevanceblender",
    "price_low_to_high": "price-asc-rank",
    "price_high_to_low": "price-desc-rank",
    "reviews": "review-rank",
    "newest": "date-desc-rank",
    "best_sellers": "exact-aware-popularity-rank",
}
CONDITIONS = ("new", "used", "renewed", "collectible", "all")
DEAL_TYPES = ("all_discounts", "todays_deals", "coupons", "lightning_deals")
_CONDITION_LABELS = {"new": ("new", "neu", "nuevo", "neuf", "nuovo"), "used": ("used", "gebraucht", "usado", "d'occasion", "usato"),
                     "renewed": ("renewed", "refurbished", "generalüberholt", "reconditionné", "ricondizionato"),
                     "collectible": ("collectible", "sammler")}
_DEAL_LABELS = {"all_discounts": ("all discounts", "all savings", "alle angebote", "toutes les"), "todays_deals": ("today's deals", "angebote des tages", "offres du jour"),
                "coupons": ("coupons", "gutscheine"), "lightning_deals": ("lightning", "blitz")}


def _refinement_ids(site, params, wants):
    """Read the refinement ids this marketplace uses for the requested
    marketplace-specific filters from an unfiltered result page."""
    html = fetch.page(site["country"], "/s", params, label="search refinements")
    page = P.search_page(html, site)
    picks = []
    by_id = {f["id"]: f for f in page["refinements"]}

    def option_value(facet_id, labels=None, index=None):
        facet = by_id.get(facet_id)
        if not facet:
            return None
        if index is not None and facet["options"]:
            option = facet["options"][index]
            return f"{facet_id}:{option['value']}" if option.get("value") else None
        for option in facet["options"]:
            name = (option.get("name") or "").lower()
            if any(lbl in name for lbl in labels or ()):
                return f"{facet_id}:{option['value']}" if option.get("value") else None
        return None
    if wants.get("is_prime"):
        picks.append(option_value("p_85", index=0))
    if wants.get("four_stars_and_up"):
        picks.append(option_value("p_72", index=0))
    if wants.get("condition") and wants["condition"] != "all":
        picks.append(option_value("p_n_condition-type", labels=_CONDITION_LABELS.get(wants["condition"], ())))
    if wants.get("deals"):
        picks.append(option_value("p_n_deal_type", labels=_DEAL_LABELS.get(wants["deals"], ())))
    return [p for p in picks if p], page


def _search_params(query=None, node=None, alias=None, page=1, sort=None, min_price=None, max_price=None, brand=None,
                   seller=None, extra_rh=None):
    params = {}
    if query:
        params["k"] = query
    if alias:
        params["i"] = alias
    if seller:
        params["me"] = seller
    rh = []
    if node:
        rh.append(f"n:{node}")
    if brand:
        rh.append(f"p_89:{brand}")
    rh.extend(extra_rh or [])
    if rh:
        params["rh"] = ",".join(rh)
        if node and not query:
            params["fs"] = "true"
    if min_price is not None:
        params["low-price"] = f"{min_price:g}"
    if max_price is not None:
        params["high-price"] = f"{max_price:g}"
    if sort and sort != "relevance":
        params["s"] = SORTS[sort]
    if page and page > 1:
        params["page"] = str(page)
    return params


def _run_search(site, locale, base_params, wants, page, key="results", **extra):
    unfiltered = {k: v for k, v in base_params.items() if k != "page"}
    extra_rh = []
    if any(wants.values()):
        extra_rh, _ = _refinement_ids(site, unfiltered, wants)
        if extra_rh:
            rh = base_params.get("rh")
            base_params["rh"] = ",".join(([rh] if rh else []) + extra_rh)
    html = fetch.page(site["country"], "/s", base_params, language=locale, label="search")
    parsed = P.search_page(html, site)
    results = parsed.pop("results")
    total_pages = parsed.pop("total_pages", None)
    current = parsed.pop("current_page", page)
    total = parsed.pop("total_count", None)
    per_page = len(results) or PER_PAGE
    out = paged_result(key, results, current or page, per_page, total=None, has_more=(total_pages or 0) > (current or page), **extra)
    out["pagination"]["total_pages"] = total_pages or (1 if results else 0)
    out["pagination"]["total_count"] = total
    out["is_total_approximate"] = parsed.get("is_total_approximate")
    out["results_text"] = parsed.get("results_text")
    out["departments"] = parsed.get("departments")
    out["refinements"] = parsed.get("refinements")
    out["applied_refinements"] = extra_rh or None
    return out


def search(query, country=None, language=None, page=1, sort="relevance", category=None, node=None, min_price=None,
           max_price=None, brand=None, condition=None, is_prime=None, four_stars_and_up=None, deals=None, seller=None):
    site, locale = context(country, language)
    params = _search_params(query=query, node=node, alias=category, page=page, sort=sort, min_price=min_price,
                            max_price=max_price, brand=brand, seller=seller)
    wants = {"is_prime": is_prime, "four_stars_and_up": four_stars_and_up, "condition": condition, "deals": deals}
    return _run_search(site, locale, params, wants, page, query=query, country=site["country"], domain=site["host"])


def category_products(node, country=None, language=None, page=1, sort="relevance", min_price=None, max_price=None,
                      brand=None, condition=None, is_prime=None, four_stars_and_up=None, deals=None):
    site, locale = context(country, language)
    params = _search_params(node=node, page=page, sort=sort, min_price=min_price, max_price=max_price, brand=brand)
    wants = {"is_prime": is_prime, "four_stars_and_up": four_stars_and_up, "condition": condition, "deals": deals}
    return _run_search(site, locale, params, wants, page, node_id=node, country=site["country"], domain=site["host"])


def lookup(identifier, country=None, language=None):
    """GTIN / UPC / EAN / ISBN (or any exact identifier) -> the products
    Amazon resolves it to (the search index matches identifiers exactly)."""
    site, locale = context(country, language)
    html = fetch.page(site["country"], "/s", {"k": identifier}, language=locale, label="lookup")
    parsed = P.search_page(html, site)
    results = [r for r in parsed["results"] if not r["is_sponsored"]]
    return {"identifier": identifier, "results": results, "count": len(results), "country": site["country"], "domain": site["host"]}


def autocomplete(query, country=None, category=None, limit=11):
    site, _ = context(country)
    body = fetch.completion(site["country"], {"prefix": query, "alias": category or "aps", "limit": str(limit)})
    rows = [P.suggestion(s) for s in (body or {}).get("suggestions") or []]
    return {"query": query, "suggestions": rows[:limit], "count": len(rows[:limit]), "country": site["country"]}


def categories(country=None, language=None):
    """The search departments (`category` aliases) of a marketplace, from
    the search box dropdown."""
    site, locale = context(country, language)
    html = fetch.page(site["country"], "/s", {"k": "amazon"}, language=locale, label="categories")
    parsed = P.search_page(html, site)
    rows = [a for a in parsed["search_aliases"] if a.get("id")]
    return {"categories": rows, "count": len(rows), "country": site["country"], "domain": site["host"]}


def category_tree(node, country=None, language=None):
    """A browse node's place in the department tree (ancestors, itself,
    children) plus the facets shown for it, from its /s?rh=n: page."""
    site, locale = context(country, language)
    html = fetch.page(site["country"], "/s", {"rh": f"n:{node}", "fs": "true"}, language=locale, label="category tree")
    parsed = P.search_page(html, site)
    departments = parsed["departments"]
    selected = next((d for d in departments if d["is_selected"]), None)
    idx = departments.index(selected) if selected else -1
    ancestors = [d for d in departments[:idx] if d.get("node_id") or d["name"].lower().startswith("any")] if idx >= 0 else []
    children = departments[idx + 1:] if idx >= 0 else departments
    return {
        "node_id": node,
        "name": selected["name"] if selected else None,
        "link": f"{site['base']}/s?rh=n:{node}",
        "ancestors": [{"name": a["name"], "node_id": a.get("node_id"), "link": a.get("link")} for a in ancestors],
        "children": [{"name": c["name"], "node_id": c.get("node_id"), "link": c.get("link")} for c in children],
        "products_count": parsed.get("total_count"),
        "refinements": parsed.get("refinements"),
        "country": site["country"],
        "domain": site["host"],
    }
