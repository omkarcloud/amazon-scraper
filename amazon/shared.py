"""Helpers shared by the Amazon endpoint modules: the paged-result
envelope route_glue lifts, the marketplace context, and the fan-out
executor for per-ASIN hydration."""
from concurrent.futures import ThreadPoolExecutor

from amazon import fetch, sites


def context(country=None, language=None):
    """(site row, storefront locale) for a request."""
    site = sites.site(country)
    locale = None
    if language:
        lang, _, region = language.replace("_", "-").partition("-")
        locale = f"{lang.lower()}_{region.upper() if region else site['language'].split('_')[-1]}"
    return site, locale


def pagination(page, per_page, total=None, has_more=None):
    if total is not None and per_page:
        total_pages = max((total + per_page - 1) // per_page, 1 if total else 0)
    elif has_more is not None:
        total_pages = page + 1 if has_more else page
    else:
        total_pages = page
    return {"page": page, "items_per_page": per_page, "total_pages": total_pages, "total_count": total}


def paged_result(key, results, page, per_page, total=None, has_more=None, **extra):
    """Endpoint result with a `pagination` block route_glue lifts into the
    flat gateway shape (count / per_page / current_page / total_pages /
    next / previous)."""
    if has_more is None and total is None:
        has_more = len(results) >= per_page
    out = dict(extra)
    out[key] = results
    out["pagination"] = pagination(page, per_page, total, has_more)
    return out


def fan_out(fn, items, workers=None):
    """Run fn(item) for every item on a small thread pool, in order; a
    failed item yields its exception object instead of a result."""
    def safe(item):
        try:
            return fn(item)
        except Exception as e:  # reported per item by the caller
            return e
    with ThreadPoolExecutor(max_workers=min(workers or fetch.FANOUT_WORKERS, max(len(items), 1))) as pool:
        return list(pool.map(safe, items))
