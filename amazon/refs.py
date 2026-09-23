"""Amazon reference parsing: ONE param per input that auto-detects its
forms (tripadvisor QueryOrLinkField convention — never a sibling `url`/`id`
pair). Every resolver returns a plain value so the validated params stay
usable as a response-cache key:

  product     B00939I7EK | 0132350882 (ISBN-10) | any amazon.<tld> link with
              /dp/<asin>, /gp/product/<asin>, /gp/aw/d/<asin>,
              /product-reviews/<asin>, /<slug>/dp/<asin>  -> the ASIN
  seller      A1D09S7Q0OD6TH | amazon.com/sp?seller=<id> | amazon.com/s?me=<id>
              | any link carrying seller=/me=/marketplaceID  -> the seller id
  influencer  tastemade | amazon.com/shop/tastemade[/…]  -> the handle
  post        1N84PQ3CI4NW0 | amazon.com/shop/tastemade/list/1N84PQ3CI4NW0
              -> {"handle": …|None, "id": …}
  node        172282 | amazon.com/s?rh=n:172282 | amazon.com/b?node=172282
              | …/s?k=x&i=electronics&rh=n:172282,n:541966 (last node wins)
              -> the numeric browse node id
  bestseller  electronics | electronics/172541 | amazon.com/gp/bestsellers/
  category    electronics/172541 | amazon.com/Best-Sellers-…/zgbs/electronics/172541
              | gp/new-releases/… | gp/movers-and-shakers/… -> "electronics/172541"
  query       laptop | amazon.com/s?k=laptop -> the keyword text
  link        any amazon.<tld> link (scrape-by-url) -> the link itself

`country_of(value)` tells the schema which marketplace a pasted link
belongs to (amazon.de -> DE) so the caller need not repeat `country`.
"""
import re
from urllib.parse import parse_qs, unquote, urlparse

from amazon import sites

_ASIN_RE = re.compile(r"^[A-Z0-9]{10}$")
_ASIN_PATH_RE = re.compile(r"/(?:dp|gp/product|gp/aw/d|product-reviews|gp/offer-listing|dp/product)/([A-Z0-9]{10})(?=[/?#]|$)", re.I)
_ASIN_SLUG_RE = re.compile(r"/dp/([A-Z0-9]{10})", re.I)
_SELLER_RE = re.compile(r"^A[A-Z0-9]{9,24}$")
_HANDLE_RE = re.compile(r"^[A-Za-z0-9._\-]{1,80}$")
_POST_ID_RE = re.compile(r"^[A-Z0-9]{8,20}$")
_BS_PATH_RE = re.compile(r"/(?:zgbs|gp/(?:bestsellers|new-releases|movers-and-shakers|most-wished-for|most-gifted))/([^?#]*)", re.I)


def _is_link(value):
    low = value.lower()
    return low.startswith(("http://", "https://", "//")) or "amazon." in low


def _parsed(value):
    url = value if "://" in value else "https://" + value.lstrip("/")
    return urlparse(url)


def country_of(value):
    """ISO country of an Amazon link, None for bare ids / other hosts."""
    value = (value or "").strip()
    if not value or not _is_link(value):
        return None
    return sites.country_for_host(_parsed(value).hostname or "")


def resolve_product(value):
    value = (value or "").strip()
    if _ASIN_RE.match(value.upper()) and not value.isalpha():
        return value.upper()
    if _is_link(value):
        parsed = _parsed(value)
        match = _ASIN_PATH_RE.search(parsed.path) or _ASIN_SLUG_RE.search(parsed.path)
        if match:
            return match.group(1).upper()
        asin = parse_qs(parsed.query).get("asin", [None])[0]
        if asin and _ASIN_RE.match(asin.upper()):
            return asin.upper()
    raise ValueError("product must be an ASIN (e.g. B00939I7EK) or an Amazon product link (…/dp/<asin>)")


def resolve_seller(value):
    value = (value or "").strip()
    if _SELLER_RE.match(value.upper()) and not _is_link(value):
        return value.upper()
    if _is_link(value):
        query = parse_qs(_parsed(value).query)
        for key in ("seller", "me", "sellerID", "merchant"):
            sid = query.get(key, [None])[0]
            if sid and _SELLER_RE.match(sid.upper()):
                return sid.upper()
    raise ValueError("seller must be an Amazon seller id (e.g. A1D09S7Q0OD6TH) or a seller link (amazon.com/sp?seller=<id>)")


def resolve_influencer(value):
    value = (value or "").strip()
    if _is_link(value):
        parts = [p for p in _parsed(value).path.split("/") if p]
        if len(parts) >= 2 and parts[0].lower() == "shop":
            return parts[1]
        raise ValueError("influencer link must look like amazon.com/shop/<name>")
    value = value.lstrip("@")
    if _HANDLE_RE.match(value):
        return value
    raise ValueError("influencer must be an Amazon storefront name (e.g. tastemade) or an amazon.com/shop/<name> link")


def resolve_post(value):
    """-> {"handle": …, "id": …} (handle None when only the id was given)."""
    value = (value or "").strip()
    if _is_link(value):
        parts = [p for p in _parsed(value).path.split("/") if p]
        if len(parts) >= 4 and parts[0].lower() == "shop" and parts[2].lower() in ("list", "post", "photo", "video", "idea"):
            return {"handle": parts[1], "id": parts[3]}
        raise ValueError("post link must look like amazon.com/shop/<name>/list/<id>")
    if _POST_ID_RE.match(value.upper()):
        return {"handle": None, "id": value}
    raise ValueError("post must be an influencer list / post id (e.g. 1N84PQ3CI4NW0) or its amazon.com/shop/<name>/list/<id> link")


def resolve_node(value):
    value = (value or "").strip()
    if value.isdigit():
        return value
    if _is_link(value):
        parsed = _parsed(value)
        query = parse_qs(unquote(parsed.query))
        nodes = re.findall(r"(?:^|,)n:(\d+)", query.get("rh", [""])[0])
        if nodes:
            return nodes[-1]
        node = query.get("node", [None])[0]
        if node and node.isdigit():
            return node
        match = re.search(r"/b/(?:[^/]+/)?(\d+)", parsed.path) or re.search(r"node=(\d+)", value)
        if match:
            return match.group(1)
    raise ValueError("category must be a numeric Amazon browse node id (e.g. 172282) or a category link (amazon.com/s?rh=n:172282, amazon.com/b?node=172282)")


def resolve_bestseller_category(value):
    """'electronics' | 'electronics/172541' | a best-sellers style link ->
    'electronics' | 'electronics/172541' ('' = the marketplace root)."""
    value = (value or "").strip()
    if _is_link(value):
        match = _BS_PATH_RE.search(_parsed(value).path)
        if not match:
            raise ValueError("category link must be an Amazon best sellers / new releases / movers & shakers page")
        path = re.sub(r"/ref=.*$", "", match.group(1)).strip("/")
        return path
    path = value.strip("/")
    if path in ("", "all", "any"):
        return ""
    if re.match(r"^[a-z0-9\-]+(/\d+)?$", path, re.I):
        return path
    raise ValueError("category must be a best sellers category alias (e.g. electronics or electronics/172541) or its link")


def resolve_query(value):
    """Keyword text, or an Amazon search link -> its k= keyword."""
    value = (value or "").strip()
    if _is_link(value) and "amazon." in value.lower():
        query = parse_qs(_parsed(value).query)
        keyword = (query.get("k") or query.get("keywords") or query.get("field-keywords") or [None])[0]
        if keyword:
            return keyword.strip()
        raise ValueError("search link must carry a k= keyword (amazon.com/s?k=laptop)")
    if not value:
        raise ValueError("query must not be empty")
    return value


def resolve_link(value):
    value = (value or "").strip()
    if not _is_link(value) or not country_of(value):
        raise ValueError("url must be a link on an Amazon marketplace (amazon.com, amazon.co.uk, amazon.de, …)")
    return value if "://" in value else "https://" + value.lstrip("/")


def page_type(link):
    """What an Amazon link points at (scrape-by-url dispatch)."""
    parsed = _parsed(link)
    path = parsed.path.lower()
    query = parse_qs(parsed.query)
    if _ASIN_PATH_RE.search(parsed.path) or _ASIN_SLUG_RE.search(parsed.path):
        return "product"
    if path.startswith("/sp") and query.get("seller"):
        return "seller"
    if path.startswith("/shop/"):
        parts = [p for p in path.split("/") if p]
        return "influencer_post" if len(parts) >= 4 else "influencer"
    if _BS_PATH_RE.search(parsed.path):
        return "bestsellers"
    if path.startswith("/deals") or path.startswith("/gp/goldbox"):
        return "deals"
    if path.startswith("/s") or path.startswith("/s/") or query.get("k") or query.get("me") or query.get("rh") or path.startswith("/b"):
        return "search"
    return None
