"""/amazon/products/search-by-image — Amazon's own search by photo (the
StyleSnap "Shop the look" page, amazon.<tld>/stylesnap), over plain HTTP:

  1. GET /stylesnap on a page session (like every storefront page): the page
     carries the upload token, <input type='hidden' name='stylesnap' value='…'/>;
  2. POST /stylesnap/upload?stylesnapToken=<token>, multipart with one part
     named and filed "explore-looks.jpg" (image/jpeg) — the page's own upload
     — carrying the token page's cookies, on an upload session (amazon/fetch.py
     UPLOADS: the gateway exit re-encodes binary bodies, so never there);
  3. the JSON answer: the object Amazon matched in the picture (its bounding
     box), every matched ASIN with a score (~50-70) and details for some of
     them (amazon/parsers.py image_search).

The page's other mode, a `url` form field, takes only pictures hosted on
Amazon (anything else answers 400), so the picture is downloaded here and
sent as bytes, re-encoded as JPEG (the page re-encodes too: a 1000x740 photo
left the browser as a 1000x740 JPEG, a 1582 px one at 1582 px).

The token and its cookies are kept per marketplace and language: one token
served six uploads over 25 minutes (2026-11-20). A wrong or expired token, or
the token without its cookies, answers 403 (a 2 KB page), which fetches a new
token once.

StyleSnap exists on amazon.com, .co.uk, .de, .fr, .it, .es, .in and .co.jp;
the other 15 marketplaces answer 404 for /stylesnap (2026-11-20).
"""
import html as html_lib
import re
import threading
import time

import fastjson
from amazon import fetch, parsers as P
from amazon.shared import context
from cache import DontCache
from scraper_utils import pictures

COUNTRIES = ("US", "GB", "DE", "FR", "IT", "ES", "IN", "JP")
TOKEN_TTL = 20 * 60          # seconds; a token served uploads for 25+ minutes
MAX_SIDE = 1600              # the page uploaded a 1582 px photo at full size
JPEG_QUALITY = 85
UPLOAD_PART = "explore-looks.jpg"
# The plain-client fallback of the picture download names us (some hosts
# refuse browser TLS and serve a named client; scraper_utils/pictures.py).
PICTURE_USER_AGENT = "AmazonScraper/1.0 (https://amazon-scraper.omkar.cloud; picture download for a search by image)"

_TOKEN_RE = re.compile(r"""name=['"]stylesnap['"][^>]*?value=['"]([^'"]+)['"]""")
_tokens = {}                 # (country, locale) -> (token, cookies, expires at)
_tokens_lock = threading.Lock()


def _token(country, locale, fresh=False):
    """(token, cookies) of the marketplace's StyleSnap page; kept TOKEN_TTL."""
    key = (country, locale)
    if not fresh:
        with _tokens_lock:
            held = _tokens.get(key)
        if held and time.monotonic() < held[2]:
            return held[0], held[1]
    page = fetch.page(country, "/stylesnap", language=locale, label="search-by-image page")
    match = _TOKEN_RE.search(page)
    if not match:
        fetch.dump_debug("stylesnap_page", page)
        raise fetch.AmazonUpstreamError("the search-by-image page carried no upload token")
    token, cookies = html_lib.unescape(match.group(1)), fetch.page_cookies(country, language=locale)
    with _tokens_lock:
        _tokens[key] = (token, cookies, time.monotonic() + TOKEN_TTL)
    return token, cookies


def _picture(image):
    try:
        return pictures.fetch_jpeg(image, user_agent=PICTURE_USER_AGENT, max_side=MAX_SIDE, quality=JPEG_QUALITY)
    except pictures.PictureError as e:
        raise fetch.AmazonBadRequest(str(e))


def search_by_image(image, country=None, language=None):
    """Products that look like the picture at `image` (a picture link)."""
    site, locale = context(country, language)
    if site["country"] not in COUNTRIES:
        raise fetch.AmazonBadRequest(f"search by image works on these marketplaces only: {', '.join(COUNTRIES)}")
    jpeg = _picture(image)
    files = [(UPLOAD_PART, UPLOAD_PART, "image/jpeg", jpeg)]
    headers = {"accept": "*/*", "origin": site["base"], "referer": site["base"] + "/stylesnap?q=local"}
    resp = None
    for fresh in (False, True):
        token, cookies = _token(site["country"], locale, fresh=fresh)
        resp = fetch.upload(site["country"], "/stylesnap/upload", files, params={"stylesnapToken": token},
                            headers=headers, cookies=cookies, language=locale, label="search-by-image upload")
        if resp.status_code != 403:
            break
    if resp.status_code == 403:
        raise fetch.AmazonBlocked("search-by-image upload: refused (403) with a fresh token")
    if resp.status_code != 200:
        raise fetch.AmazonUpstreamError(f"search-by-image upload: HTTP {resp.status_code}")
    try:
        payload = fastjson.loads(resp.text or "")
    except ValueError:
        fetch.dump_debug("stylesnap_upload", resp.text or "")
        raise fetch.AmazonUpstreamError(f"search-by-image upload: non-JSON body ({(resp.text or '')[:80]!r})")
    out = {"image": image, **P.image_search(payload, site), "country": site["country"], "domain": site["host"]}
    # nothing matched is a real answer, but a passing upload hiccup looks the
    # same: not kept for the cache's hours
    return out if out["matches_count"] else DontCache(out)
