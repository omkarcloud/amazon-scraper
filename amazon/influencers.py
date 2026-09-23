"""/amazon/influencers/* — Amazon Influencer storefronts (/shop/<name>):
the profile with its first page of posts, and a list post's products
(/shop/<name>/list/<id>).

Post pagination beyond the first 20 cards is not exposed: the storefront
loads more through a token-signed ajax that the page's JS derives from
its own state; `has_more_posts` / `next_page_token` are reported so the
caller knows the page is partial.
"""
from amazon import fetch, parsers as P
from amazon.shared import context


def profile(influencer, country=None, language=None):
    site, locale = context(country, language)
    html = fetch.page(site["country"], f"/shop/{influencer}", language=locale, label=f"influencer {influencer}")
    data = P.influencer_page(html, site, influencer)
    if not data.get("name") and not data.get("posts"):
        raise fetch.AmazonNotFound(f"influencer {influencer}: no storefront page")
    data["country"] = site["country"]
    data["domain"] = site["host"]
    return data


def posts(influencer, country=None, language=None, post_type=None):
    data = profile(influencer, country, language)
    rows = data.pop("posts") or []
    if post_type and post_type != "all":
        rows = [p for p in rows if p.get("type") == post_type]
    return {"influencer": {"name": data.get("name"), "handle": data.get("handle"), "link": data.get("link")},
            "posts": rows, "count": len(rows), "has_more": data.get("has_more_posts"),
            "next_page_token": data.get("next_page_token"), "country": data["country"], "domain": data["domain"]}


def post_products(post, country=None, language=None, influencer=None):
    site, locale = context(country, language)
    handle = post.get("handle") or influencer
    if not handle:
        raise fetch.AmazonBadRequest("post products need the influencer name too (pass `influencer`, or the full list link)")
    path = f"/shop/{handle}/list/{post['id']}"
    html = fetch.page(site["country"], path, language=locale, label=f"influencer list {post['id']}")
    data = P.influencer_list_page(html, site)
    if not data.get("products"):
        raise fetch.AmazonNotFound(f"list {post['id']}: no products found (not a list post, or removed)")
    return {"id": post["id"], "title": data.get("title"), "description": data.get("description"),
            "link": site["base"] + path, "influencer": {"handle": handle, "link": f"{site['base']}/shop/{handle}"},
            "products": data["products"], "count": len(data["products"]), "country": site["country"], "domain": site["host"]}
