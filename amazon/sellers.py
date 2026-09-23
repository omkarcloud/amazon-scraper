"""/amazon/sellers/* — the seller profile page (/sp?seller=), its feedback
(the /sp/ajax/feedback JSON the page's histogram uses) and the seller's
storefront listings (/s?me=)."""
from amazon import fetch, parsers as P
from amazon.shared import context, paged_result

FEEDBACK_PER_PAGE = 5
FEEDBACK_FILTERS = {"all": "all", "positive": "all_positive", "critical": "all_critical", "5_stars": "5", "4_stars": "4",
                    "3_stars": "3", "2_stars": "2", "1_star": "1"}
FEEDBACK_PERIODS = {"lifetime": "lifetime", "12_months": "twelveMonth", "3_months": "threeMonth", "1_month": "oneMonth"}
FEEDBACK_SORTS = {"recent": "recent", "helpful": "helpful"}


def profile(seller, country=None, language=None):
    site, locale = context(country, language)
    html = fetch.page(site["country"], "/sp", {"seller": seller}, language=locale, label=f"seller {seller}")
    data = P.seller_page(html, site)
    if not data.get("name"):
        raise fetch.AmazonNotFound(f"seller {seller}: no profile page")
    data["id"] = data.get("id") or seller
    return data


def feedback(seller, country=None, language=None, page=1, rating="all", period="lifetime", sort="recent"):
    site, locale = context(country, language)
    params = {"seller": seller, "marketplaceID": site["marketplace_id"], "feedbackKey": FEEDBACK_PERIODS.get(period, "lifetime"),
              "pageNumber": str(page), "filter": FEEDBACK_FILTERS.get(rating, "all"), "sortBy": FEEDBACK_SORTS.get(sort, "recent")}
    body = fetch.ajax_json(site["country"], "/sp/ajax/feedback", params, referer=f"{site['base']}/sp?seller={seller}",
                           language=locale, label=f"seller feedback {seller}")
    if not isinstance(body, dict):
        raise fetch.AmazonUpstreamError("seller feedback: unexpected response")
    rows = [P.feedback_item(r, site) for r in body.get("details") or [] if isinstance(r, dict)]
    total = P.to_int(body.get("totalcount")) or None
    return paged_result("feedback", rows, page, FEEDBACK_PER_PAGE, total=total,
                        has_more=bool(body.get("hasNextPage")), seller_id=seller, rating=rating, period=period,
                        country=site["country"], domain=site["host"])


def products(seller, country=None, language=None, page=1, sort="relevance", query=None):
    from amazon import search as S
    site, locale = context(country, language)
    params = S._search_params(query=query, seller=seller, page=page, sort=sort)
    params["marketplaceID"] = site["marketplace_id"]
    out = S._run_search(site, locale, params, {}, page, seller_id=seller, query=query, country=site["country"], domain=site["host"])
    out.pop("departments", None)
    return out
