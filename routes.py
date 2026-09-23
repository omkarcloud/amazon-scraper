"""The 21 Amazon endpoints. Every path is served with and without the
`/amazon` prefix, so code generated against the hosted API (paths like
/products/details) runs unchanged against this server.

Params are validated by the package's own marshmallow schemas
(amazon/schemas.py): ONE param per input — `product` takes an ASIN or a
product link, `seller` an id or a seller link, and so on — and a pasted
amazon.de / amazon.co.uk … link picks that marketplace by itself.
"""
import json
from urllib.parse import urlencode

from bottle import request, response, route

from amazon import influencers, products, rankings, schemas, search, sellers
from schema_fields import load_query
from scraper_errors import BadRequest, Blocked, NotFound, UpstreamError


def json_response(data, status=200):
    response.status = status
    response.content_type = "application/json"
    return json.dumps(data, ensure_ascii=False)


def query_dict():
    """The query as unicode strings (bottle 0.12's .get() hands back latin-1
    decoded bytes, so a UTF-8 "Café" would arrive as "CafÃ©")."""
    return {key: request.query.getunicode(key) for key in request.query.keys()}


def _page_link(path, params, page):
    if not page:
        return None
    query = {k: v for k, v in params.items() if v not in (None, "")}
    query["page"] = page
    return f"{request.urlparts.scheme}://{request.urlparts.netloc}{path}?{urlencode(query)}"


def paginate(result, path, params):
    """Lift the endpoint's `pagination` block into the flat shape the hosted
    API returns: count / per_page / current_page / total_pages / next / previous."""
    pagination = result.pop("pagination", None) or {}
    page = int(pagination.get("page") or params.get("page") or 1)
    total_pages = int(pagination.get("total_pages") or 0)
    out = {
        "count": pagination.get("total_count"),
        "per_page": pagination.get("items_per_page"),
        "current_page": page,
        "total_pages": total_pages,
        "next": _page_link(path, params, page + 1 if page < total_pages else None),
        "previous": _page_link(path, params, page - 1 if page > 1 else None),
    }
    out.update(result)
    return out


def handle(schema, impl, paginated):
    """Validate -> call -> map errors: bad params 400, missing entity 404,
    Amazon blocks / transport failures 502, anything else 500."""
    raw = query_dict()
    data, error = load_query(schema, raw)
    if error:
        return json_response(error, 400)
    try:
        result = impl(**data)
    except ValueError as e:
        return json_response({"error": str(e)}, 400)
    except BadRequest as e:
        return json_response({"error": f"amazon rejected the request: {e}"}, 400)
    except NotFound as e:
        return json_response({"error": str(e) or "not found"}, 404)
    except Blocked as e:
        return json_response({"error": f"amazon blocked the request, retry later: {e}"}, 502)
    except UpstreamError as e:
        return json_response({"error": f"amazon request failed: {e}"}, 502)
    except Exception as e:
        return json_response({"error": f"{type(e).__name__}: {e}"}, 500)
    if paginated:
        result = paginate(result, request.path, raw)
    return json_response(result)


ENDPOINTS = [
    # path, schema, function, paginated
    ("/products/details", schemas.ProductDetailsSchema, products.details, False),
    ("/search/autocomplete", schemas.AutocompleteSchema, search.autocomplete, False),
    ("/search", schemas.SearchSchema, search.search, True),
    ("/products/reviews", schemas.ProductSchema, products.reviews, False),
    ("/products/offers", schemas.OffersSchema, products.offers, True),
    ("/products/variations", schemas.ProductSchema, products.variations, False),
    ("/products/bulk", schemas.BulkSchema, products.bulk, False),
    ("/products/lookup", schemas.LookupSchema, search.lookup, False),
    ("/best-sellers", schemas.BestsellersSchema, rankings.bestsellers, True),
    ("/best-sellers/categories", schemas.BestsellerCategoriesSchema, rankings.bestseller_categories, False),
    ("/deals", schemas.DealsSchema, rankings.deals, True),
    ("/categories", schemas.CategoriesSchema, search.categories, False),
    ("/categories/tree", schemas.CategoryTreeSchema, search.category_tree, False),
    ("/categories/products", schemas.CategoryProductsSchema, search.category_products, True),
    ("/sellers/details", schemas.SellerSchema, sellers.profile, False),
    ("/sellers/feedback", schemas.SellerFeedbackSchema, sellers.feedback, True),
    ("/sellers/products", schemas.SellerProductsSchema, sellers.products, True),
    ("/influencers/details", schemas.InfluencerSchema, influencers.profile, False),
    ("/influencers/posts", schemas.InfluencerPostsSchema, influencers.posts, False),
    ("/influencers/posts/products", schemas.InfluencerPostSchema, influencers.post_products, False),
    ("/scrape", schemas.ScrapeUrlSchema, products.scrape_url, False),
]


def mount(path, schema, impl, paginated):
    """Serve an endpoint at /path and /amazon/path."""
    def handler():
        return handle(schema, impl, paginated)
    handler.__name__ = "amazon_" + path.strip("/").replace("/", "_").replace("-", "_")
    route(path, method="GET")(handler)
    route("/amazon" + path, method="GET")(handler)


for _path, _schema, _impl, _paginated in ENDPOINTS:
    mount(_path, _schema, _impl, _paginated)


@route("/", method="GET")
@route("/health", method="GET")
def health():
    return json_response({"status": "ok", "endpoints": [e[0] for e in ENDPOINTS]})
