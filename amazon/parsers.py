"""Amazon page/JSON parsers -> the public response shapes (spec in
scrapers/prompts.md: snake_case, `link` not url, is_* booleans, numeric
amounts + `currency`, nested objects, identity keys first, metadata last,
null for missing, no tracking junk).

Surfaces parsed here:
  search_page(html, site)         /s result pages (also browse nodes, seller
                                  storefronts, GTIN lookups): cards, facets,
                                  total, pagination
  product_page(html, site)        /dp pages: everything above the fold plus
                                  details tables, images / videos JSON, the
                                  variation matrix, inline top reviews, the
                                  "Customers say" AI summary
  offers_page(html, site)         the aodAjaxMain offer list
  bestsellers_page(html, site)    /gp/bestsellers | new-releases |
                                  movers-and-shakers | most-wished-for |
                                  most-gifted grids (+ the ACP page for ranks
                                  31-50) and the category tree
  seller_page(html, site)         /sp seller profile
  feedback_item(raw, site)        /sp/ajax/feedback JSON rows
  influencer_page(html, site)     /shop/<name> storefront + post cards
  influencer_list_page(html, site) /shop/<name>/list/<id> products
  deal_product(raw, site)         the deals grid / d2b product objects
  aapi_product(raw, site)         data.amazon.* product/v2 (deal prices)
  suggestion(raw)                 completion.amazon.* rows

Skipped on purpose (present in the raw pages): csa/csm tracking attributes,
impression-logger props, sponsored-ad feedback payloads, A/B weblab state,
the "share" widgets, Prime sign-up / credit-card upsell copy, add-to-cart
forms and their tokens, presentation-only strings ("Brief content visible,
double tap…"), duplicated price renderings (a-offscreen vs whole/fraction).
"""
import html as _html
import json
import re
from datetime import datetime
from urllib.parse import parse_qs, unquote, urljoin, urlparse

from bs4 import BeautifulSoup



# Localized price text -> float. Same algorithm as airbnb.parsers.parse_amount,
# kept here so the amazon package runs standalone (open-source kit).
_AMOUNT_START_RE = re.compile(r"\d[\d.,  \s]*")


def parse_amount(text):
    """Localized price text -> float: "₹40,524" -> 40524.0,
    "8,104.64" -> 8104.64, "1 353 €" -> 1353.0, "8.104,64" -> 8104.64.
    None when no number is present."""
    if not text:
        return None
    text = str(text)
    m = _AMOUNT_START_RE.search(text)
    if not m:
        return None
    negative = bool(re.search(r"[-−–]\s*[^\d]*$", text[:m.start()]))
    s = re.sub(r"[\s  ]", "", m.group())
    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):          # 8.104,64 (comma decimal)
            s = s.replace(".", "").replace(",", ".")
        else:                                     # 8,104.64
            s = s.replace(",", "")
    elif "," in s:
        head, _, tail = s.rpartition(",")
        if "," not in head and len(tail) in (1, 2):   # 1353,5 (comma decimal)
            s = head + "." + tail
        else:                                     # 40,524 / 1,00,524 (grouping)
            s = s.replace(",", "")
    elif s.count(".") > 1 or (s.count(".") == 1 and len(s.rpartition(".")[2]) == 3):
        s = s.replace(".", "")                    # 8.104 (dot grouping)
    try:
        value = float(s)
    except ValueError:
        return None
    return -value if negative else value

_WS_RE = re.compile(r"\s+")
_ASIN_RE = re.compile(r"/(?:dp|gp/product|gp/aw/d|product-reviews)/([A-Z0-9]{10})(?:[/?]|$)")
_ASIN_ANY_RE = re.compile(r"\b([A-Z0-9]{10})\b")
_NODE_RE = re.compile(r"(?:node=|rh=n(?:%3A|:))(\d+)")
_RATING_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*(?:out of|von|sur|su|de|of)\s*5", re.I)
_COUNT_RE = re.compile(r"(\d[\d,.\s ]*)")
_PERCENT_RE = re.compile(r"(-?\d+(?:[.,]\d+)?)\s*%")
_RANK_RE = re.compile(r"#?\s*([\d,. ]+)\s+(?:in|en|dans|in der|su)\s+([^(#\n]+?)(?:\s*\(|\s*$|\s+#)")
_HIST_RE = re.compile(r"(\d+)\s*(?:percent|%)[^\d]*(\d)\s*star", re.I)
_BOUGHT_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*([KkMm])?\+?\s*(?:bought|purchased|gekauft|comprado|achetés|acquistato)", re.I)
_CURRENCY_SYMBOLS = {"$": "USD", "US$": "USD", "£": "GBP", "€": "EUR", "₹": "INR", "¥": "JPY", "￥": "JPY",
                     "C$": "CAD", "CDN$": "CAD", "A$": "AUD", "AU$": "AUD", "S$": "SGD", "R$": "BRL", "MX$": "MXN",
                     "kr": "SEK", "zł": "PLN", "TL": "TRY", "AED": "AED", "SAR": "SAR", "EGP": "EGP", "R": "ZAR",
                     "SEK": "SEK", "PLN": "PLN", "USD": "USD", "EUR": "EUR", "GBP": "GBP", "INR": "INR", "JPY": "JPY",
                     "CAD": "CAD", "AUD": "AUD", "SGD": "SGD", "BRL": "BRL", "MXN": "MXN", "TRY": "TRY", "ZAR": "ZAR"}
_DOLLAR_CURRENCIES = {"USD", "AUD", "CAD", "SGD", "MXN"}
_DATE_FORMATS = ("%B %d, %Y", "%d %B %Y", "%B %d %Y", "%d. %B %Y", "%Y年%m月%d日", "%d de %B de %Y", "%d %b %Y",
                 "%b %d, %Y", "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y")
_IMAGE_SIZE_RE = re.compile(r"\._[^./]+_(?=\.[a-z]+$)")


# ---- generic helpers ---------------------------------------------------------------

def soup(html_text):
    return BeautifulSoup(html_text or "", "lxml")


def clean(value):
    if value is None:
        return None
    value = _WS_RE.sub(" ", _html.unescape(str(value)).replace("‏", "").replace("‎", "").replace("‌", "").replace("​", "")).strip()
    return value or None


def text(el, sep=" "):
    return clean(el.get_text(sep, strip=True)) if el is not None else None


def first(el, selector):
    return el.select_one(selector) if el is not None else None


def first_text(el, selector):
    return text(first(el, selector))


def attr(el, name):
    if el is None:
        return None
    value = el.get(name)
    if isinstance(value, list):
        value = " ".join(value)
    return clean(value)


def to_int(value):
    """'43,049' / '(21)' / '1 353' / 12 -> int; None when nothing numeric."""
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    match = _COUNT_RE.search(str(value))
    if not match:
        return None
    digits = re.sub(r"[^\d]", "", match.group(1))
    return int(digits) if digits else None


def to_float(value, digits=2):
    if value is None:
        return None
    try:
        return round(float(str(value).replace(",", ".")), digits)
    except ValueError:
        return None


def rating_of(value):
    """'4.7 out of 5 stars' / '4,2 von 5 Sternen' / '4.5' -> 4.7."""
    if value is None:
        return None
    match = _RATING_RE.search(str(value))
    if match:
        return to_float(match.group(1), 1)
    match = re.match(r"\s*(\d(?:[.,]\d)?)\s*$", str(value))
    return to_float(match.group(1), 1) if match else None


def percent_of(value):
    match = _PERCENT_RE.search(str(value or ""))
    return to_float(match.group(1), 1) if match else None


def money(value, currency=None):
    """Localized price text -> {amount, currency}; None when no number.
    The currency comes from a symbol / code in the text, else `currency`."""
    if value is None:
        return None
    raw = clean(value)
    amount = parse_amount(raw)
    if amount is None:
        return None
    code = currency
    for symbol, iso in sorted(_CURRENCY_SYMBOLS.items(), key=lambda kv: -len(kv[0])):
        if symbol in raw:
            # a bare "$" is every dollar marketplace's own symbol (amazon.com.au,
            # .ca, .sg, .com.mx show "$4,699.00"): keep the marketplace currency
            if symbol == "$" and currency in _DOLLAR_CURRENCIES:
                break
            code = iso
            break
    return {"amount": amount, "currency": code}


def amount_of(value):
    block = money(value)
    return block["amount"] if block else None


def iso_date(value):
    """'July 26, 2026' / '27 August 2026' -> '2026-07-26'; None when unparsed."""
    value = clean(value)
    if not value:
        return None
    value = re.sub(r"\s+", " ", value.replace(".", ". ").replace("  ", " ")).strip()
    candidates = {value, value.replace(". ", "."), value.replace(",", "")}
    for candidate in candidates:
        for fmt in _DATE_FORMATS:
            try:
                return datetime.strptime(candidate, fmt).date().isoformat()
            except ValueError:
                continue
    return None


def bought_past_month(value):
    """'6K+ bought in past month' -> 6000 (lower bound); None otherwise."""
    match = _BOUGHT_RE.search(value or "")
    if not match:
        return None
    number = to_float(match.group(1), 2) or 0
    unit = (match.group(2) or "").upper()
    return int(number * (1000 if unit == "K" else 1000000 if unit == "M" else 1))


def snake(label):
    label = clean(label) or ""
    label = re.sub(r"[^\w]+", "_", label.replace("&", "and")).strip("_").lower()
    return re.sub(r"_+", "_", label) or None


def absolute(site, href):
    """Site-relative href -> absolute link without the ref= tracking tail."""
    href = clean(href)
    if not href or href.startswith(("javascript:", "#")):
        return None
    if href.startswith("/sspa/click"):          # sponsored redirect: unwrap the target url
        target = parse_qs(urlparse(href).query).get("url", [None])[0]
        href = unquote(target) if target else href
    link = urljoin(site["base"] + "/", href)
    link = re.sub(r"/ref=[^/?#]*", "", link)
    parsed = urlparse(link)
    keep = {k: v for k, v in parse_qs(parsed.query).items()
            if k not in ("ref", "ref_", "qid", "sr", "dib", "dib_tag", "keywords", "sp_csd", "psc", "th", "pd_rd_i",
                         "pd_rd_r", "pd_rd_w", "pd_rd_wg", "pf_rd_p", "pf_rd_r", "tag", "linkCode", "creativeASIN",
                         "_encoding", "ie", "crid", "sprefix", "content-id", "smid", "pdp_new", "ds", "rnid", "dc",
                         "field-lbr_brands_browse-bin", "lp_asin", "store_ref")}
    query = "&".join(f"{k}={v[0]}" for k, v in keep.items())
    return parsed._replace(query=query).geturl().rstrip("?")


def product_link(site, asin):
    return f"{site['base']}/dp/{asin}" if asin else None


def asin_in(href):
    match = _ASIN_RE.search(href or "")
    return match.group(1) if match else None


def node_in(href):
    """The browse node a link points at: the LAST n:<id> of its rh= list
    (rh=n:172282,n:541966 is Electronics > Computers), else node=<id>."""
    query = parse_qs(urlparse(unquote(href or "")).query)
    rh = query.get("rh", [""])[0]
    nodes = re.findall(r"(?:^|,)n:(\d+)", rh)
    if nodes:
        return nodes[-1]
    node = query.get("node", [None])[0]
    if node and node.isdigit():
        return node
    match = _NODE_RE.search(unquote(href or ""))
    return match.group(1) if match else None


def full_image(src):
    """Strip the size modifier from a media-amazon image link
    (…/71kEunh4iBL._AC_UY218_.jpg -> …/71kEunh4iBL.jpg)."""
    src = clean(src)
    if not src:
        return None
    return _IMAGE_SIZE_RE.sub("", src)


def balanced_json(text_, start):
    """Parse the JSON object/array whose opening bracket is at `start`."""
    opener = text_[start]
    closer = {"{": "}", "[": "]"}[opener]
    depth = 0
    in_str = False
    esc = False
    for i in range(start, len(text_)):
        ch = text_[i]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
        elif ch == opener:
            depth += 1
        elif ch == closer:
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(text_[start:i + 1])
                except ValueError:
                    return None
    return None


def json_after(text_, marker, opener="{"):
    """The JSON value that follows the first `marker` (e.g. '"colorImages":')
    that is actually followed by `opener` (the same key can appear earlier
    as a plain string, e.g. "videos":"Videos")."""
    start = 0
    while True:
        i = text_.find(marker, start)
        if i < 0:
            return None
        j = i + len(marker)
        while j < len(text_) and text_[j] in " :\n\t\r":
            j += 1
        if j < len(text_) and text_[j] == opener:
            value = balanced_json(text_, j)
            if value is not None:
                return value
        start = i + len(marker)


def a_state(soup_, key):
    """The JSON of a <script type="a-state" data-a-state='{"key":…}'> block."""
    for script in soup_.select('script[type="a-state"]'):
        try:
            meta = json.loads(_html.unescape(script.get("data-a-state") or "{}"))
        except ValueError:
            continue
        if meta.get("key") == key:
            try:
                return json.loads(script.get_text() or "null")
            except ValueError:
                return None
    return None


def compact(block):
    """Drop None values from a dict (used for optional sub-objects)."""
    return {k: v for k, v in block.items() if v is not None}


# ---- search result cards ------------------------------------------------------------

_BADGE_KEYS = (("best seller", "is_best_seller"), ("amazon's choice", "is_amazons_choice"),
               ("overall pick", "is_overall_pick"), ("limited time deal", "is_limited_time_deal"),
               ("climate pledge", "is_climate_pledge_friendly"), ("small business", "is_small_business"),
               ("new on amazon", "is_new_arrival"), ("lowest price", "is_lowest_price_in_days"))


def _price_block(el, currency):
    """{amount, currency, list_price, unit_price, unit, text} from any
    element holding .a-price spans (search card, product page, offer)."""
    if el is None:
        return None
    prices = [p for p in el.select(".a-price") if p.find_parent(class_="a-price") is None]
    current = None
    strike = None
    for price in prices:
        offscreen = text(first(price, ".a-offscreen")) or text(price)
        if price.get("data-a-strike") == "true" or "a-text-price" in (price.get("class") or []):
            strike = strike or offscreen
        else:
            current = current or offscreen
    if current is None and strike is None:
        return None
    block = money(current, currency) or {"amount": None, "currency": currency}
    block["list_price"] = amount_of(strike)
    unit = None
    unit_text = text(el)
    match = re.search(r"\(\s*([^()]*?\d[^()]*?)\s*/\s*([^()]+?)\s*\)", unit_text or "")
    if match:
        unit = {"amount": amount_of(match.group(1)), "per": clean(match.group(2))}
    block["unit_price"] = unit
    label = re.search(r"\b(Typical|List|RRP|Was|UVP|Prix conseillé|Prezzo consigliato|Precio recomendado)\b", unit_text or "")
    block["list_price_type"] = label.group(1) if label and block["list_price"] is not None else None
    return block


def search_card(card, site):
    asin = attr(card, "data-asin")
    if not asin:
        return None
    currency = site["currency"]
    # Fashion / jewelry cards carry TWO h2s: the brand line first, then the
    # product title inside the product link.
    title_el = first(card, "[data-cy=title-recipe] a h2") or first(card, "h2[aria-label]") or first(card, "h2 span") \
        or first(card, "h2") or first(card, "[data-cy=title-recipe] a")
    brand_el = first(card, "[data-cy=title-recipe] > .a-row h2")
    link_el = first(card, "[data-cy=title-recipe] a[href]") or first(card, "h2 a[href]") or first(card, "a.s-no-outline[href]")
    link = product_link(site, asin)
    img = first(card, "img.s-image")
    reviews = first(card, "[data-cy=reviews-block]")
    rating_el = first(reviews, "[data-cy=reviews-ratings-slot] .a-icon-alt") or first(reviews, "i.a-icon-star-small .a-icon-alt")
    rating = rating_of(text(rating_el)) or rating_of(first_text(reviews, "span.a-size-small.a-color-base"))
    ratings_count = None
    for el in (reviews.select("[aria-label]") if reviews is not None else []):
        label = attr(el, "aria-label") or ""
        if re.match(r"^[\d,.\u00a0]+\s+(ratings?|Bewertungen|évaluations|valutazioni|calificaciones|reseñas)$", label, re.I):
            ratings_count = to_int(label)
            break
    if ratings_count is None:
        ratings_count = to_int(first_text(reviews, "span.s-underline-text"))
    price_block = _price_block(first(card, "[data-cy=price-recipe]"), currency)
    secondary = first_text(card, "[data-cy=secondary-offer-recipe]")
    more = None
    if secondary:
        more = {"lowest_price": amount_of(secondary), "offers_count": to_int(re.search(r"\((\d[\d,]*)", secondary).group(1))
                if re.search(r"\((\d[\d,]*)", secondary) else None, "text": secondary}
    badge_text = " ".join(t for t in (text(b) for b in card.select(".a-badge-text, [data-cy=s-pc-faceout-badge]")) if t)
    faceout_text = text(card) or ""
    low = (badge_text + " " + faceout_text[:600]).lower()
    badges = {key: (label in low) for label, key in _BADGE_KEYS}
    badges["is_prime"] = bool(first(card, ".a-icon-prime, .s-prime"))
    delivery = first_text(card, "[data-cy=delivery-recipe]")
    coupon = first_text(card, ".s-coupon-unclipped, [data-cy=coupon-recipe]")
    options = re.search(r"Options:\s*([^.\n]+?)(?:\s+See options|\s+\d|$)", faceout_text)
    bought = bought_past_month(text(reviews))
    return {
        "asin": asin,
        "title": text(title_el),
        "brand": text(brand_el) if brand_el is not None and brand_el is not title_el else None,
        "link": link,
        "image": full_image(attr(img, "src")),
        "thumbnail": attr(img, "src"),
        "is_sponsored": "AdHolder" in (card.get("class") or []) or bool(first(card, ".AdHolder, .puis-sponsored-label-text, .s-sponsored-label-text"))
        or card.find_parent(class_="AdHolder") is not None,
        "price": price_block,
        "rating": {"average": rating, "count": ratings_count} if rating is not None or ratings_count is not None else None,
        "bought_past_month": bought,
        "bought_past_month_text": (re.search(r"[\d.,]+[KkMm]?\+?\s*bought in past month", text(reviews) or "") or [None])[0]
        if isinstance(re.search(r"[\d.,]+[KkMm]?\+?\s*bought in past month", text(reviews) or ""), re.Match) else None,
        "badges": badges,
        "delivery": delivery,
        "coupon": coupon,
        "variations_text": clean(options.group(1)) if options else None,
        "has_variations": bool(options) or "See options" in faceout_text,
        "more_buying_choices": more,
        "position": to_int(attr(card, "data-index")),
    }


def _facet(ul, site):
    """One refinement group: the <ul id="filter-p_123"> plus the title div
    before it -> {id, name, options[{name, value, link, is_selected}]}."""
    fid = (ul.get("id") or "").replace("filter-", "")
    title_div = ul.find_previous_sibling("div")
    name = text(title_div) if title_div is not None else None
    options = []
    for li in ul.select("li"):
        a = first(li, "a[href]")
        label = text(first(li, "span.a-size-base")) or text(li)
        if not label:
            continue
        li_id = li.get("id") or ""
        value = li_id.split("/", 1)[1] if "/" in li_id else None
        href = attr(a, "href") if a else None
        if value is None and href:
            rh = parse_qs(urlparse(href).query).get("rh", [""])[0]
            value = rh.split(",")[-1].split(":", 1)[-1] if rh else None
        options.append({"name": label, "value": value, "link": absolute(site, href) if href else None,
                        "is_selected": (a is None) or (attr(a, "aria-current") == "true") or "a-text-bold" in " ".join(a.get("class") or [])})
    return {"id": fid or None, "name": name, "options": options}


def search_page(html_text, site):
    """/s page -> {results, total_count, refinements, pagination}."""
    doc = soup(html_text)
    results = []
    for card in doc.select('div[data-component-type="s-search-result"]'):
        parsed = search_card(card, site)
        if parsed:
            results.append(parsed)
    info = first_text(doc, '[data-component-type="s-result-info-bar"] span.a-size-base, [data-component-type="s-result-info-bar"]') or ""
    total_match = re.search(r"of\s+(?:over\s+|more than\s+)?([\d,. ]+)\s+results", info) or \
        re.search(r"([\d,. ]+)\s+results", info)
    total = to_int(total_match.group(1)) if total_match else None
    is_over = bool(re.search(r"\b(over|more than)\b", info))
    pages = [to_int(text(p)) for p in doc.select(".s-pagination-item") if text(p) and text(p).isdigit()]
    total_pages = max(pages) if pages else (1 if results else 0)
    current = to_int(first_text(doc, ".s-pagination-item.s-pagination-selected")) or 1
    refinements = []
    for ul in doc.select("#s-refinements ul[id^=filter-]"):
        facet = _facet(ul, site)
        if facet["options"] and facet["id"] not in ("departments",):
            refinements.append(facet)
    departments = []
    for li in doc.select("#departments li, #filter-departments li"):
        a = first(li, "a[href]")
        name = text(li)
        if not name:
            continue
        departments.append({"name": name, "node_id": node_in(attr(a, "href")) if a else None,
                            "link": absolute(site, attr(a, "href")) if a else None,
                            "is_selected": a is None or "a-text-bold" in (a.get("class") or [])})
    aliases = [{"id": (o.get("value") or "").replace("search-alias=", ""), "name": text(o)}
               for o in doc.select("#searchDropdownBox option") if o.get("value")]
    return {
        "results": results,
        "total_count": total,
        "is_total_approximate": is_over,
        "results_text": clean(info.split("Sort by")[0]) if info else None,
        "departments": departments,
        "refinements": refinements,
        "search_aliases": aliases,
        "current_page": current,
        "total_pages": total_pages,
    }


# ---- product page ------------------------------------------------------------------

def _details_rows(doc):
    """Every label/value pair from the product-information tables, the
    detail bullets and the "product facts" grid -> ordered dict of raw
    (label, value) keeping the first occurrence."""
    rows = []
    for tr in doc.select("#prodDetails tr, #productDetails_techSpec_section_1 tr, #productDetails_techSpec_section_2 tr, "
                         "#productDetails_detailBullets_sections1 tr, #productDetails_db_sections tr"):
        th, td = first(tr, "th"), first(tr, "td")
        if th is not None and td is not None:
            rows.append((text(th), text(td)))
    for li in doc.select("#detailBullets_feature_div li, #detailBulletsWrapper_feature_div li"):
        label = first(li, "span.a-text-bold")
        if label is not None:
            value = clean(text(li).replace(text(label) or "", "", 1))
            rows.append((clean((text(label) or "").rstrip(": ")), value))
    for grid in doc.select("#productFactsDesktopExpander .a-fixed-left-grid"):
        cols = grid.select(".a-fixed-left-grid-col")
        if len(cols) >= 2:
            rows.append((text(cols[0]), text(cols[1])))
    out = {}
    for label, value in rows:
        label = clean((label or "").rstrip(":"))
        if label and value and label not in out:
            out[label] = value
    return out


def _best_sellers_rank(doc, site):
    node = doc.find(string=re.compile(r"Best[- ]?Sellers? Rank|Bestseller-Rang|Classement des meilleures|Posizione nella|Clasificación", re.I))
    if node is None:
        return []
    container = node.find_parent(["tr", "li", "div"])
    if container is None:
        return []
    block = text(container) or ""
    block = re.sub(r"\(\s*See Top \d+[^)]*\)", " ", block)
    links = {clean(a.get_text()): absolute(site, a.get("href")) for a in container.select("a[href]")}
    ranks = []
    for match in re.finditer(r"#?\s*([\d,. ]+)\s+(?:in|en|dans|su)\s+([^(#]+?)(?=\s*\(|\s*#|$)", block):
        rank = to_int(match.group(1))
        category = clean(match.group(2))
        if not rank or not category or category.lower().startswith("see top"):
            continue
        link = next((v for k, v in links.items() if k and (k == category or category in k)), None)
        ranks.append({"rank": rank, "category": category, "link": link,
                      "category_id": (re.search(r"/(\d+)(?:/|$)", link) or [None, None])[1] if link else None})
    return ranks


def _histogram(doc):
    out = {}
    for el in doc.select("#histogramTable [aria-label], #histogramTable a[title]"):
        match = _HIST_RE.search(el.get("aria-label") or el.get("title") or "")
        if match:
            out[f"{match.group(2)}_star"] = to_int(match.group(1))
    return out or None


def _variations(html_text, site):
    display = json_after(html_text, '"dimensionValuesDisplayData"')
    if not isinstance(display, dict) or not display:
        return None
    labels = json_after(html_text, '"variationDisplayLabels"') or {}
    dims = json_after(html_text, '"dimensions"', "[") or list(labels.keys())
    values = json_after(html_text, '"variationValues"') or {}
    current = (re.search(r'"currentAsin"\s*:\s*"([A-Z0-9]{10})"', html_text) or [None, None])[1]
    parent = (re.search(r'"parentAsin"\s*:\s*"([A-Z0-9]{10})"', html_text) or [None, None])[1]
    dimensions = [{"key": key, "name": labels.get(key, key), "values": values.get(key) or []} for key in dims]
    products = []
    for asin, vals in display.items():
        attrs = {labels.get(dims[i], dims[i]) if i < len(dims) else f"dimension_{i}": v for i, v in enumerate(vals)}
        products.append({"asin": asin, "attributes": attrs, "link": product_link(site, asin), "is_current": asin == current})
    return {"parent_asin": parent, "current_asin": current, "dimensions": dimensions, "products": products,
            "count": len(products)}


def _images(html_text):
    color_images = json_after(html_text, '"colorImages"')
    images = []
    seen = set()
    if isinstance(color_images, dict):
        groups = [color_images["initial"]] if "initial" in color_images else list(color_images.values())
        for group in groups:
            for entry in group or []:
                link = entry.get("hiRes") or entry.get("large")
                if not link or link in seen:
                    continue
                seen.add(link)
                images.append({"link": link, "thumbnail": entry.get("thumb"), "large": entry.get("large"),
                               "variant": entry.get("variant")})
    if not images:
        landing = re.search(r'id="landingImage"[^>]*\sdata-old-hires="([^"]+)"', html_text) or \
            re.search(r'id="landingImage"[^>]*\ssrc="([^"]+)"', html_text)
        if landing:
            images.append({"link": landing.group(1), "thumbnail": None, "large": landing.group(1), "variant": "MAIN"})
    return images


def _videos(html_text):
    raw = json_after(html_text, '"videos":', "[")
    out = []
    for v in raw or []:
        if not isinstance(v, dict) or not v.get("url"):
            continue
        out.append(compact({"title": v.get("title"), "link": v.get("url"), "thumbnail": v.get("thumb"),
                            "duration_seconds": v.get("durationSeconds"), "language": v.get("languageCode"),
                            "is_hero": v.get("isHeroVideo"), "creator": (v.get("creatorProfile") or {}).get("name")}))
    return out


def _review(card, site):
    rid = (card.get("id") or "").replace("customer_review-", "") or None
    profile = first(card, "a.a-profile")
    profile_href = attr(profile, "href")
    date_text = first_text(card, "[data-hook=review-date]") or ""
    match = re.search(r"Reviewed in (?:the )?(.+?) on (.+)$", date_text)
    body = first(card, "[data-hook=reviewText] [data-hook=reviewRichContentContainer]") or first(card, "[data-hook=reviewText]") \
        or first(card, "[data-hook=review-body]")
    body_text = None
    if body is not None:
        body_text = clean(" ".join(p.get_text(" ", strip=True) for p in (body.select("p") or [body])))
        body_text = clean(re.sub(r"(Brief|Full) content visible, double tap to read (full|brief) content\.", "", body_text or ""))
    helpful = first_text(card, "[data-hook=helpful-vote-statement]")
    helpful_votes = to_int(helpful) if helpful else 0
    if helpful and helpful.lower().startswith("one person"):
        helpful_votes = 1
    title_el = first(card, "[data-hook=reviewTitle]") or first(card, "[data-hook=review-title]")
    title_link = first(card, "a[href*='customer-reviews/srp'], a[data-hook=review-title]")
    return {
        "id": rid,
        "title": text(title_el),
        "link": absolute(site, attr(title_link, "href")) if title_link else (f"{site['base']}/gp/customer-reviews/{rid}" if rid else None),
        "rating": rating_of(first_text(card, "[data-hook=review-star-rating] .a-icon-alt, [data-hook=cmps-review-star-rating] .a-icon-alt")),
        "date": iso_date(match.group(2)) if match else iso_date(date_text),
        "date_text": date_text or None,
        "country": clean(match.group(1)) if match else None,
        "text": body_text,
        "author": {"name": first_text(card, ".a-profile-name"), "link": absolute(site, profile_href),
                   "id": (re.search(r"profile/([\w.]+)", profile_href or "") or [None, None])[1]},
        "variant": first_text(card, "[data-hook=format-strip]"),
        "is_verified_purchase": bool(first(card, "[data-hook=avp-badge]")),
        "is_vine": "vine" in (first_text(card, "[data-hook=review-badges]") or "").lower(),
        "helpful_votes": helpful_votes,
        "images": [full_image(attr(img, "src")) for img in card.select("img[data-hook=review-image-tile]") if attr(img, "src")],
        "has_video": bool(first(card, "[data-hook=reviewVideo], video")),
    }


def _customers_say(html_text):
    """The AI review summary + aspect sentiments from the k-injected
    (base64 JSON) component blobs of the Customers say block."""
    import base64
    summary = None
    aspects = []
    for token in re.findall(r"k\+b64 ([A-Za-z0-9+/=]+)", html_text):
        try:
            blob = json.loads(base64.b64decode(token).decode("utf-8", "ignore"))
        except Exception:
            continue
        component = str(blob.get("k-component") or "")
        data = blob.get("k-data") or {}
        if component.startswith("SummaryFragments") and summary is None:
            summary = clean(" ".join(f.get("inertText") or "" for f in data.get("fragments") or []))
        elif component.startswith("AspectList") and not aspects:
            for aspect in data.get("aspectsFlattened") or []:
                snippets = []
                for snip in aspect.get("snippets") or []:
                    frag = "".join((f.get("text") or ((f.get("semanticContent") or {}).get("content") or {}).get("text") or "")
                                   for f in ((snip.get("text") or {}).get("fragments") or []))
                    review = (snip.get("review") or {}).get("url") or ""
                    rid = (re.search(r"/-/(\w+)", review) or [None, None])[1]
                    snippets.append({"text": clean(frag), "review_id": rid})
                aspects.append({"name": aspect.get("label"), "sentiment": aspect.get("sentiment"),
                                "summary": aspect.get("summary"), "mentions": aspect.get("mentions"),
                                "mentions_percentage": aspect.get("mentionsPercentage"), "snippets": snippets})
    if summary is None and not aspects:
        return None
    return {"summary": summary, "aspects": aspects, "is_ai_generated": True}


def _delivery(doc):
    slots = []
    for el in doc.select("[data-csa-c-delivery-time]"):
        slots.append(compact({"type": attr(el, "data-csa-c-delivery-type"), "price_text": attr(el, "data-csa-c-delivery-price"),
                              "time": attr(el, "data-csa-c-delivery-time"), "cutoff": attr(el, "data-csa-c-delivery-cutoff"),
                              "text": text(el)}))
    seen = set()
    unique = []
    for s in slots:
        key = (s.get("type"), s.get("time"))
        if key in seen:
            continue
        seen.add(key)
        unique.append(s)
    primary = first_text(doc, "#mir-layout-DELIVERY_BLOCK-slot-PRIMARY_DELIVERY_MESSAGE_LARGE, #deliveryBlockMessage")
    secondary = first_text(doc, "#mir-layout-DELIVERY_BLOCK-slot-SECONDARY_DELIVERY_MESSAGE_LARGE")
    location = first_text(doc, "#glow-ingress-line2, #contextualIngressPtLabel_deliveryShortLine")
    if not (primary or unique or location):
        return None
    return {"text": primary, "fastest_text": secondary, "options": unique, "location": location}


def _buybox(doc, site):
    seller_link = first(doc, "#sellerProfileTriggerId")
    merchant = first_text(doc, "#merchantInfoFeature_feature_div, #merchant-info") or ""
    ships_from = None
    sold_by = None
    for cell in doc.select("#tabular-buybox [tabular-attribute-name]"):
        label = (cell.get("tabular-attribute-name") or "").lower()
        value = first_text(cell, ".tabular-buybox-text-message") or text(cell)
        if "ships from" in label or "dispatches from" in label:
            ships_from = ships_from or value
        elif "sold by" in label:
            sold_by = sold_by or value
    both = re.search(r"Shipper\s*/\s*Seller\s+(.+?)(?:\s+Shipper|\s*$)", merchant)
    if both:
        name = clean(both.group(1))
        if name and len(name.split()) >= 2 and name.split()[0] == name.split()[1]:
            name = name.split()[0]           # the page repeats the name twice
        ships_from = ships_from or name
        sold_by = sold_by or name
    for lab, pattern in (("ships_from", r"(?:Ships from|Dispatches from)\s*:?\s*(.+?)(?=\s+Sold by|\s*\.|\s*$)"),
                         ("sold_by", r"Sold by\s*:?\s*(.+?)(?:\s+and\s+Fulfilled|\s*\.|$)")):
        match = re.search(pattern, merchant)
        if match:
            if lab == "ships_from" and not ships_from:
                ships_from = clean(match.group(1))
            if lab == "sold_by" and not sold_by:
                sold_by = clean(match.group(1))
    seller_id = None
    if seller_link is not None:
        seller_id = parse_qs(urlparse(attr(seller_link, "href") or "").query).get("seller", [None])[0]
        sold_by = text(seller_link) or sold_by
    if not (sold_by or ships_from or merchant):
        return None
    return {
        "seller": {"name": sold_by, "id": seller_id,
                   "link": f"{site['base']}/sp?seller={seller_id}" if seller_id else None} if sold_by else None,
        "ships_from": ships_from,
        "is_fulfilled_by_amazon": "fulfilled by amazon" in merchant.lower() or (ships_from or "").lower().startswith("amazon"),
        "is_sold_by_amazon": (sold_by or "").lower().startswith("amazon"),
    }


def _availability(doc):
    el = first(doc, "#availability")
    status = first_text(el, "span") or text(el)
    if not status:
        return None
    low = status.lower()
    in_stock = None
    if any(k in low for k in ("in stock", "only", "usually ships", "available", "verfügbar", "en stock", "disponible")):
        in_stock = True
    if any(k in low for k in ("unavailable", "out of stock", "not available", "nicht verfügbar", "indisponible")):
        in_stock = False
    left = re.search(r"only\s+(\d+)\s+left", low)
    return {"text": status, "is_in_stock": in_stock, "quantity_left": to_int(left.group(1)) if left else None}


def _byline(doc, site):
    byline = first(doc, "#bylineInfo")
    if byline is None:
        return None, []
    label = text(byline) or ""
    href = attr(byline, "href")
    authors = []
    for a in doc.select("#bylineInfo .author a.a-link-normal, #bylineInfo .author a.contributorNameID"):
        name = text(a)
        if name:
            role = first_text(a.find_parent(class_="author"), ".contribution") if a.find_parent(class_="author") else None
            authors.append({"name": name, "role": clean((role or "").strip("()")), "link": absolute(site, attr(a, "href"))})
    name = re.sub(r"^(Visit the|Brand:|Marke:|Marca:|Marque\s*:)\s*", "", label, flags=re.I)
    name = re.sub(r"\s+Store$", "", name).strip() or None
    brand = None
    if href and "/stores/" in href:
        brand = {"name": name, "link": absolute(site, href), "store_link": absolute(site, href)}
    elif href:
        brand = {"name": name, "link": absolute(site, href), "store_link": None}
    elif name and not authors:
        brand = {"name": name, "link": None, "store_link": None}
    return brand, authors


def _price(doc, currency):
    scopes = [first(doc, sel) for sel in ("#apex_desktop", "#corePriceDisplay_desktop_feature_div", "#corePrice_feature_div",
                                          "#corePrice_desktop", "#price", "#buybox")]
    scopes = [sc for sc in scopes if sc is not None]
    current = strike = scope = None
    for scope in scopes:
        current = first(scope, ".priceToPay .a-offscreen, .apexPriceToPay .a-offscreen, .a-price:not([data-a-strike=true]) .a-offscreen") \
            or first(scope, ".aok-offscreen")
        if current is not None and amount_of(text(current)) is not None:
            strike = first(scope, ".basisPrice .a-offscreen, [data-a-strike=true] .a-offscreen, .a-text-price .a-offscreen")
            break
        current = None
    if scope is None:
        return None
    block = money(text(current), currency) if current is not None else None
    if block is None:
        block = {"amount": None, "currency": currency}
    block["list_price"] = amount_of(text(strike)) if strike is not None else None
    savings = first_text(scope, ".savingsPercentage")
    block["savings_percent"] = abs(percent_of(savings)) if savings and percent_of(savings) is not None else (
        round((1 - block["amount"] / block["list_price"]) * 100) if block.get("amount") and block.get("list_price") and block["list_price"] > block["amount"] else None)
    block["savings_amount"] = round(block["list_price"] - block["amount"], 2) if block.get("amount") and block.get("list_price") and block["list_price"] > block["amount"] else None
    unit = first_text(scope, ".pricePerUnit, #pricePerUnit") or ""
    match = re.search(r"\(?\s*([^()/]*?\d[^()/]*?)\s*/\s*([^()]+?)\s*\)?$", unit)
    block["unit_price"] = {"amount": amount_of(match.group(1)), "per": clean(match.group(2))} if match else None
    if block["amount"] is None and block["list_price"] is None:
        return None
    return block


def product_page(html_text, site):
    doc = soup(html_text)
    currency = site["currency"]
    asin = (re.search(r'"currentAsin"\s*:\s*"([A-Z0-9]{10})"', html_text) or re.search(r'name="ASIN"\s+value="([A-Z0-9]{10})"', html_text)
            or re.search(r'data-asin="([A-Z0-9]{10})"', html_text) or [None, None])[1]
    details_raw = _details_rows(doc)
    details = {}
    for label, value in details_raw.items():
        key = snake(label)
        if key in ("best_sellers_rank", "customer_reviews", "customer_reviews_rank", "asin") or not key:
            continue
        details[key] = value
    identifiers = {}
    for key in ("upc", "ean", "isbn_10", "isbn_13", "gtin", "global_trade_identification_number", "model_number",
                "item_model_number", "part_number", "manufacturer_part_number"):
        if details.get(key):
            identifiers[key.replace("global_trade_identification_number", "gtin").replace("item_model_number", "model_number")
                        .replace("manufacturer_part_number", "part_number")] = details[key]
    brand, authors = _byline(doc, site)
    overview = {}
    for tr in doc.select("#productOverview_feature_div tr"):
        cells = tr.select("td")
        if len(cells) >= 2 and text(cells[0]):
            overview[snake(text(cells[0]))] = text(cells[1])
    brand_name = overview.get("brand") or details.get("brand_name") or details.get("brand")
    if brand is None and brand_name:
        brand = {"name": brand_name, "link": None, "store_link": None}
    if brand is not None:
        brand = {"name": brand_name or brand.get("name"), "byline": brand.get("name"), "link": brand.get("link"),
                 "store_link": brand.get("store_link")}
    bullets = [t for t in (text(li) for li in doc.select("#feature-bullets li span.a-list-item")) if t]
    if not bullets:   # softlines layout: "About this item" inside the product-facts expander
        bullets = [t for t in (text(li) for li in doc.select("#productFactsDesktopExpander ul li, #pqv-feature-bullets li")) if t]
        bullets = list(dict.fromkeys(bullets))
    rating = rating_of(attr(first(doc, "#acrPopover"), "title")) or rating_of(first_text(doc, "#acrPopover"))
    ratings_count = to_int(first_text(doc, "#acrCustomerReviewText")) or to_int(first_text(doc, "[data-hook=total-review-count]"))
    reviews = [_review(card, site) for card in doc.select("[data-hook=review]")]
    reviews = [r for r in reviews if r.get("id")]
    badge_text = " ".join(t for t in (text(b) for b in doc.select("#zeitgeistBadge_feature_div, #acBadge_feature_div, .badge-wrapper")) if t)
    deal_badge = first_text(doc, "#dealBadgeSupportingText") or first_text(doc, "#dealBadge_feature_div span")
    if deal_badge:
        deal_badge = clean(re.split(r"\s+NO_OF_|\s+Limited time deal\s+NO_OF", deal_badge)[0]) if "NO_OF_" in deal_badge else deal_badge
    coupon = first_text(doc, "#promoPriceBlockMessage_feature_div label, #promoPriceBlockMessage_feature_div .a-color-success")
    breadcrumbs = [{"name": text(a), "link": absolute(site, attr(a, "href")), "node_id": node_in(attr(a, "href"))}
                   for a in doc.select("#wayfinding-breadcrumbs_feature_div li a") if text(a)]
    videos = _videos(html_text)
    return {
        "asin": asin,
        "title": first_text(doc, "#productTitle") or first_text(doc, "#title"),
        "link": product_link(site, asin),
        "brand": brand,
        "authors": authors or None,
        "price": _price(doc, currency),
        "deal_badge": deal_badge,
        "coupon": coupon,
        "availability": _availability(doc),
        "buybox": _buybox(doc, site),
        "delivery": _delivery(doc),
        "is_prime": bool(first(doc, "#desktop_buybox .a-icon-prime, #buybox .a-icon-prime, #deliveryBlockMessage .a-icon-prime, "
                                  "#mir-layout-DELIVERY_BLOCK .a-icon-prime, #prime-badge, #primeExclusivePricingMessage")),
        "rating": {"average": rating, "count": ratings_count, "histogram": _histogram(doc)} if rating is not None or ratings_count else None,
        "bought_past_month": bought_past_month(first_text(doc, "#social-proofing-faceout-title-tk_bought")),
        "badges": {"is_best_seller": "best seller" in badge_text.lower(), "is_amazons_choice": "amazon's choice" in badge_text.lower(),
                   "best_seller_text": clean(badge_text) or None,
                   "is_climate_pledge_friendly": bool(first(doc, "#climatePledgeFriendly, #climatePledgeFriendlyBadge"))},
        "categories": breadcrumbs,
        "best_sellers_rank": _best_sellers_rank(doc, site),
        "bullets": bullets,
        "overview": overview or None,
        "description": first_text(doc, "#productDescription") or first_text(doc, "#bookDescription_feature_div"),
        "aplus_description": (lambda t: t if t and len(t) > 60 else None)(first_text(doc, "#aplus_feature_div, #aplus")),
        "details": details or None,
        "identifiers": identifiers or None,
        "images": _images(html_text),
        "videos": videos,
        "videos_count": to_int(first_text(doc, "#videoCount")) or (len(videos) or None),
        "variations": _variations(html_text, site),
        "customers_say": _customers_say(html_text),
        "top_reviews": reviews,
        "has_inline_reviews": bool(reviews),
        "important_information": first_text(doc, "#important-information"),
        "warranty": first_text(doc, "#warrantyInfo"),
        "country": site["country"],
        "domain": site["host"],
    }


def review_treatment(html_text):
    """The reviews A/B treatment of the session that rendered this page:
    "C" renders the top reviews inline, "T1" does not."""
    match = re.search(r'cr-weblab-state&quot;\}">\{"[0-9a-f]+":"(\w+)"\}', html_text)
    return match.group(1) if match else None


# ---- offers ------------------------------------------------------------------------

def _offer(block, site, is_pinned):
    currency = site["currency"]
    price_el = first(block, "#aod-offer-price") or block
    current = first_text(price_el, ".a-price:not([data-a-strike]) .a-offscreen") or first_text(price_el, ".aok-offscreen")
    price = money(current, currency) if current else None
    strike = first_text(block, "[data-a-strike=true] .a-offscreen, .basisPrice .a-offscreen")
    if price is not None:
        price["list_price"] = amount_of(strike)
        savings = first_text(block, ".savingsPercentage, .a-color-price")
        price["savings_percent"] = abs(percent_of(savings)) if savings and "%" in savings else None
    sold_by_el = first(block, "#aod-offer-soldBy")
    seller_a = first(sold_by_el, "a")
    seller_href = attr(seller_a, "href")
    seller_name = text(seller_a) or clean(re.sub(r"^(Sold by|Verkauf durch|Vendu par)\s*", "", text(sold_by_el) or ""))
    seller_id = parse_qs(urlparse(seller_href or "").query).get("seller", [None])[0]
    ships_from = clean(re.sub(r"^(Ships from|Dispatches from|Versand durch|Expédié par)\s*", "", first_text(block, "#aod-offer-shipsFrom") or ""))
    condition = first_text(block, "#aod-offer-heading") or ("New" if is_pinned else None)
    note = first_text(block, "#aod-condition-container")
    if note:
        note = clean(re.sub(r"^(Condition|Zustand|État)\s*", "", note))
    delivery = [text(el) for el in block.select("[data-csa-c-delivery-price]") if text(el)]
    qty = [to_int(text(o)) for o in block.select("select[id^=aod-qty] option, #aod-qty-option option") if to_int(text(o))]
    rating_text = first_text(block, "#aod-offer-seller-rating") or ""
    return {
        "condition": condition,
        "condition_note": note or None,
        "price": price,
        "seller": {"name": seller_name or None, "id": seller_id, "link": f"{site['base']}/sp?seller={seller_id}" if seller_id else absolute(site, seller_href),
                   "rating": rating_of(rating_text), "positive_percent": percent_of(rating_text),
                   "ratings_count": to_int(re.search(r"\((\d[\d,]*)", rating_text).group(1)) if re.search(r"\((\d[\d,]*)", rating_text) else None}
        if (seller_name or seller_id) else None,
        "ships_from": ships_from or None,
        "is_fulfilled_by_amazon": (ships_from or "").lower().startswith("amazon"),
        "is_sold_by_amazon": (seller_name or "").lower().startswith("amazon"),
        "is_prime": bool(first(block, ".a-icon-prime")),
        "delivery": {"text": delivery[0] if delivery else None, "fastest_text": delivery[1] if len(delivery) > 1 else None} if delivery else None,
        "max_quantity": max(qty) if qty else None,
        "promotion": first_text(block, "[id^=aod-offer-promotion]") or None,
        "is_pinned": is_pinned,
    }


def offers_page(html_text, site):
    doc = soup(html_text)
    pinned = first(doc, "#aod-pinned-offer")
    offers = []
    if pinned is not None and (first(pinned, ".a-price") or first(pinned, ".aok-offscreen")):
        offers.append(_offer(pinned, site, True))
    for block in doc.select("#aod-offer"):
        offers.append(_offer(block, site, False))
    total = to_int(attr(first(doc, "#aod-total-offer-count"), "value"))
    return {
        "asin": attr(first(doc, "#aod-asin-block-asin"), "value") or (re.search(r'data-asin="([A-Z0-9]{10})"', html_text) or [None, None])[1],
        "title": first_text(doc, "#aod-asin-title-text"),
        "rating": {"average": rating_of(first_text(doc, "#aod-asin-reviews-star .a-icon-alt")), "count": to_int(first_text(doc, "#aod-asin-reviews-count-title"))}
        if first(doc, "#aod-asin-reviews") else None,
        "offers": offers,
        "other_offers_count": total,
        "has_more": "aod-end-of-results" not in html_text and total is not None and total > len([o for o in offers if not o["is_pinned"]]),
    }


# ---- best sellers ------------------------------------------------------------------

def _recs_list(doc):
    grid = first(doc, "[data-client-recs-list]")
    if grid is None:
        return []
    try:
        return json.loads(_html.unescape(grid.get("data-client-recs-list") or "[]"))
    except ValueError:
        return []


def bestseller_card(card, site, meta=None):
    asin = attr(first(card, "[data-asin]"), "data-asin") or attr(card, "data-asin")
    link_el = first(card, "a.a-link-normal[href*='/dp/']")
    rating_el = first(card, "a[aria-label*='out of 5']") or first(card, "i.a-icon-star-small")
    rating_label = attr(rating_el, "aria-label") or first_text(rating_el, ".a-icon-alt")
    count = to_int(re.search(r"stars?,\s*([\d,. ]+)", rating_label or "").group(1)) if rating_label and re.search(r"stars?,\s*([\d,. ]+)", rating_label) else None
    if count is None:
        count = to_int(first_text(card, "span.a-size-small"))
    price_text = first_text(card, ".p13n-sc-price, ._cDEzb_p13n-sc-price_3mJ9Z, span.a-color-price, .a-price .a-offscreen")
    meta = meta or {}
    rank = to_int(first_text(card, ".zg-bdg-text")) or to_int(meta.get("render.zg.rank"))
    change = percent_of(meta.get("render.zg.bsms.percentageChange") or "")
    return {
        "rank": rank,
        "asin": asin,
        "title": text(first(card, "[class*=line-clamp]")) or attr(first(card, "img"), "alt"),
        "link": product_link(site, asin),
        "image": full_image(attr(first(card, "img"), "src")),
        "rating": {"average": rating_of(rating_label), "count": count} if rating_label else None,
        "price": money(price_text, site["currency"]) if price_text and any(ch.isdigit() for ch in price_text) else None,
        "price_text": price_text,
        "rank_change_percent": change,
        "previous_rank": to_int(meta.get("render.zg.bsms.twentyFourHourOldSalesRank")),
        "sales_rank": to_int(meta.get("render.zg.bsms.currentSalesRank")),
    }


def bestsellers_page(html_text, site):
    doc = soup(html_text)
    recs = _recs_list(doc)
    meta_by_asin = {r.get("id"): r.get("metadataMap") or {} for r in recs if isinstance(r, dict)}
    items = []
    for card in doc.select("#gridItemRoot"):
        asin = attr(first(card, "[data-asin]"), "data-asin")
        parsed = bestseller_card(card, site, meta_by_asin.get(asin))
        if parsed["asin"]:
            items.append(parsed)
    tree = []
    for li in doc.select("[class*=zg-browse-item], [class*=zg-root-browse-item]"):
        a = first(li, "a[href]")
        name = text(li)
        if not name:
            continue
        href = attr(a, "href") if a else None
        clean_href = re.sub(r"/ref=[^/?#]*", "", href or "")
        path = re.search(r"/(?:zgbs|gp/(?:bestsellers|new-releases|movers-and-shakers|most-wished-for|most-gifted))/([^?#]*)", clean_href)
        name = clean(re.sub(r"^[‹<]\s*|\s*\(Current\)$", "", name))
        tree.append({"name": name, "path": (path.group(1).strip("/") or None) if path else None,
                     "link": absolute(site, href) if href else None,
                     "is_selected": a is None, "is_root": "zg-root-browse-item" in " ".join(li.get("class") or [])})
    card_root = first(doc, "[data-acp-path]")
    grid = first(doc, ".p13n-desktop-grid")
    acp = None
    if card_root is not None:
        acp = {"path": card_root.get("data-acp-path"), "params": _html.unescape(card_root.get("data-acp-params") or ""),
               "reftag": (grid.get("data-reftag") if grid is not None else None),
               "faceout": (grid.get("data-faceoutkataname") if grid is not None else None) or "GeneralFaceout",
               "rendered": len(items), "entries": recs}
    tabs = [{"name": text(a), "link": absolute(site, attr(a, "href"))} for a in doc.select("[class*=mlt-list-type] a, .zg-tabs a") if text(a)]
    return {
        "title": first_text(doc, "h1"),
        "items": items,
        "expected_count": len(recs) or len(items),
        "tree": tree,
        "tabs": tabs,
        "acp": acp,
    }


def bestsellers_acp_page(html_text, site, meta_by_asin=None):
    doc = soup(html_text)
    out = []
    for card in doc.select("#gridItemRoot, .p13n-grid-content"):
        asin = attr(first(card, "[data-asin]"), "data-asin")
        if not asin:
            continue
        parsed = bestseller_card(card, site, (meta_by_asin or {}).get(asin))
        if not any(o["asin"] == asin for o in out):
            out.append(parsed)
    return out


# ---- sellers -------------------------------------------------------------------------

def _ratings_period(raw):
    if not isinstance(raw, dict):
        return None
    count = raw.get("ratingCount")
    stars = {f"{n}_star": raw.get(f"star{n}Count") for n in range(5, 0, -1)}
    percents = {f"{n}_star": raw.get(f"star{n}") for n in range(5, 0, -1)}
    positive = (raw.get("star5Count") or 0) + (raw.get("star4Count") or 0)
    return {"count": count, "positive_percent": round(positive * 100 / count, 1) if count else None,
            "stars": stars, "stars_percent": percents}


def seller_page(html_text, site):
    doc = soup(html_text)
    state = a_state(doc, "spp-page-var-page-state") or {}
    seller_id = state.get("sellerID") or (re.search(r"seller=([A-Z0-9]+)", html_text) or [None, None])[1]
    header = first_text(doc, "#page-section-seller-header") or ""
    info = {}
    current = None
    for row in doc.select("#page-section-detail-seller-info .a-row"):
        if first(row, "h3") is not None or row.get("id") == "page-section-detail-seller-info":
            continue
        label = first_text(row, "span.a-text-bold")
        if label:
            current = snake(label.rstrip(": "))
            value = clean((text(row) or "").replace(label, "", 1))
            info[current] = value
        elif current and text(row):
            info[current] = clean(" ".join(v for v in (info.get(current), text(row)) if v))
    info = {k: v for k, v in info.items() if k}
    about = first_text(doc, "#page-section-about-seller")
    if about:
        about = clean(re.sub(r"^About Seller\s*", "", about))
        about = clean(re.sub(r"Have a question for .+?\?$", "", about))
    logo = first(doc, "#seller-logo img, #seller-profile-container img[src*='seller']")
    storefront = first(doc, "#seller-info-storefront-link a")
    return {
        "id": seller_id,
        "name": first_text(doc, "#seller-name") or first_text(doc, "h1"),
        "link": f"{site['base']}/sp?seller={seller_id}" if seller_id else None,
        "storefront_link": absolute(site, attr(storefront, "href")) if storefront else (f"{site['base']}/s?me={seller_id}" if seller_id else None),
        "logo": attr(logo, "src") if logo is not None and "loading" not in (attr(logo, "src") or "") else None,
        "rating": {"average": rating_of(first_text(doc, "#effective-timeperiod-rating-lifetime-description")) or rating_of(header),
                   "positive_percent": percent_of(header), "count": to_int(re.search(r"\(([\d,.]+)\s+total", header).group(1)) if re.search(r"\(([\d,.]+)\s+total", header) else to_int(first_text(doc, "#rating-lifetime-num"))},
        "ratings": {"lifetime": _ratings_period(a_state(doc, "lifetimeRatingsData")),
                    "twelve_months": _ratings_period(a_state(doc, "twelveMonthRatingsData")),
                    "three_months": _ratings_period(a_state(doc, "threeMonthRatingsData")),
                    "one_month": _ratings_period(a_state(doc, "oneMonthRatingsData"))},
        "business": {"name": info.pop("business_name", None), "address": info.pop("business_address", None)},
        "about": about or None,
        "info": info or None,
        "country": site["country"],
        "domain": site["host"],
    }


def feedback_item(raw, site):
    data = raw.get("ratingData") or {}
    text_block = data.get("text") or {}
    response = raw.get("responseRatingData") or {}
    return {
        "rating": raw.get("rating"),
        "text": clean(text_block.get("expandedText") or text_block.get("truncatedText")),
        "date": iso_date(data.get("date")),
        "date_text": data.get("date"),
        "author": {"name": raw.get("rater"), "link": absolute(site, raw.get("raterProfileUrl")), "avatar": raw.get("raterAvatarUrl")},
        "is_fulfilled_by_amazon": raw.get("hasFulfillmentBadge") or (raw.get("fulfillmentChannel") == "AFN"),
        "has_response": bool(raw.get("hasResponse")),
        "response": clean(((response.get("text") or {}).get("expandedText")) if isinstance(response, dict) else None),
        "is_suppressed": bool(data.get("wasSuppressed")),
    }


# ---- influencers --------------------------------------------------------------------

def influencer_page(html_text, site, handle=None):
    doc = soup(html_text)
    title = first_text(doc, "title") or ""
    name = re.sub(r"'s Amazon Page$|\s*-\s*Amazon.*$", "", title).strip() or None
    desc_el = doc.find(class_=re.compile("profile-description|description-text|profile-bio|storefront-bio"))
    description = text(desc_el)
    if description is None:
        toggle = first(doc, "[data-action=see-more-toggle]")
        if toggle is not None:
            description = clean(toggle.find_previous(string=True))
    top = doc.find(class_=re.compile("top-creator"))
    img = first(doc, "img[src*='influencer-profile-image']")
    posts = [influencer_post(card, site, handle) for card in doc.select("[data-aci]")]
    posts = [p for p in posts if p]
    socials = sorted({a.get("href") for a in doc.select("a[href]")
                      if re.search(r"^https?://(www\.)?(instagram\.com|youtube\.com|youtu\.be|tiktok\.com|facebook\.com|twitter\.com|x\.com|pinterest\.com|threads\.net)/", a.get("href") or "")})
    page_token = attr(first(doc, ".shop-ajax-state input[name=pageToken]"), "value")
    return {
        "name": name,
        "handle": handle,
        "link": f"{site['base']}/shop/{handle}" if handle else None,
        "description": description,
        "image": full_image(attr(img, "src")),
        "is_top_creator": bool(top),
        "badge": first_text(doc, "[class*=badge-text], [class*=top-creator] .a-text-bold") if top else None,
        "social_links": socials,
        "posts_count_on_page": len(posts),
        "has_more_posts": (attr(first(doc, ".shop-ajax-state input[name=shouldLoadMoreFlag]"), "value") == "true"),
        "next_page_token": page_token if page_token and page_token != "0" else None,
        "posts": posts,
    }


def influencer_post(card, site, handle=None):
    aci = attr(card, "data-aci")
    if not aci:
        return None
    classes = " ".join(card.get("class") or [])
    kind = "list" if "list-item" in classes else "video" if "video" in classes else "photo" if "photo" in classes or "image" in classes else "post"
    link_el = first(card, "a[href*='/shop/']")
    items = first_text(card, ".list-itemcount")
    likes = first_text(card, ".heart-count")
    return {
        "id": aci,
        "type": kind,
        "title": first_text(card, ".list-title, .item-title, [class*=title]") or None,
        "link": absolute(site, attr(link_el, "href")) if link_el else None,
        "image": full_image(attr(first(card, "img.list-image, img[class*=image]"), "src")),
        "items_count": to_int(items) if items else None,
        "likes": to_int(likes) if likes else None,
        "is_pinned": bool(first(card, ".full-bleed-pinned-badge")),
        "product_asins": sorted({(attr(x, "data-asin") or "").replace("amzn1.asin.", "") for x in card.select("[data-asin]") if attr(x, "data-asin")}),
    }


def influencer_list_page(html_text, site):
    doc = soup(html_text)
    items = []
    for a in doc.select("a.single-product-item-link[href], [data-asin] a.single-product-item-link"):
        href = attr(a, "href")
        asin = asin_in(href) or attr(a.find_parent(attrs={"data-asin": True}) or a, "data-asin")
        price = first(a, ".product-price-container")
        items.append({
            "asin": asin,
            "title": first_text(a, ".product-title-text"),
            "brand": first_text(a, ".product-brand-text"),
            "link": product_link(site, asin) if asin else absolute(site, href),
            "image": full_image(attr(first(a, "img.product-image"), "src")),
            "price": _price_block(price, site["currency"]),
            "delivery": first_text(a, ".delivery-block-container"),
        })
    seen = set()
    unique = []
    for item in items:
        if item["asin"] in seen:
            continue
        seen.add(item["asin"])
        unique.append(item)
    title = first_text(doc, ".list-title, h1, [class*=list-name]")
    return {"title": title, "description": first_text(doc, "[class*=list-description]"), "products": unique}


# ---- deals ----------------------------------------------------------------------------

def deal_product(raw, site):
    image = raw.get("image") or {}
    hi = image.get("hiRes") or {}
    lo = image.get("lowRes") or {}
    reviews = raw.get("customerReviews") or {}
    twister = raw.get("twisterVariations") or {}
    category = raw.get("productCategory") or {}
    brand_logo = raw.get("brandLogo") or {}
    logo_asset = brand_logo.get("mediaAsset") or {}
    meta = raw.get("meta") or {}

    def media_link(asset):
        if not asset or not asset.get("physicalId"):
            return None
        base = asset.get("baseUrl") or f"https://m.media-amazon.com/images/I/{asset['physicalId']}"
        return f"{base}.{asset.get('extension') or 'jpg'}"
    return {
        "asin": raw.get("asin"),
        "title": raw.get("title"),
        "link": absolute(site, raw.get("link")) if raw.get("link") else product_link(site, raw.get("asin")),
        "image": media_link(hi) or media_link(lo),
        "thumbnail": media_link(lo),
        "brand": {"name": brand_logo.get("altText"), "id": meta.get("brandId"), "logo": media_link(logo_asset)}
        if (brand_logo.get("altText") or meta.get("brandId")) else None,
        "rating": {"average": to_float((reviews.get("rating") or {}).get("shortDisplayString"), 1),
                   "count": (reviews.get("count") or {}).get("value")} if reviews else None,
        "category": {"id": category.get("id"), "product_type": category.get("productType"), "group": category.get("symbol"),
                     "department_ids": meta.get("departmentIds") or []} if category else None,
        "variations_count": (twister.get("swatches") or {}).get("totalCount") or (len(twister.get("asinByVariation") or {}) or None),
        "is_pinned": bool(meta.get("isPinned")),
        "deal": None,
    }


def aapi_product(raw, site):
    """data.amazon.* product/v2 entity -> the `deal` block for a grid product."""
    entity = (raw or {}).get("entity") or {}
    options = entity.get("buyingOptions") or []
    option = options[0] if options else {}
    price_entity = ((option.get("price") or {}).get("entity")) or {}
    badge_entity = ((option.get("dealBadge") or {}).get("entity")) or {}
    details_entity = ((option.get("dealDetails") or {}).get("entity")) or {}

    def frag_text(block):
        content = (block or {}).get("content") or {}
        return clean(" ".join(f.get("text") or "" for f in content.get("fragments") or [])) or None

    def amount(block):
        block = block or {}
        money_block = block.get("moneyValueOrRange") or {}
        value = money_block.get("value") or block.get("value") or {}
        if isinstance(value, dict) and value.get("amount") is not None:
            return {"amount": to_float(value.get("amount")), "currency": value.get("unit") or site["currency"]}
        display = block.get("displayString") or block.get("price")
        return money(display, site["currency"]) if display else None
    price = amount(price_entity.get("priceToPay")) or {}
    basis = amount(price_entity.get("basisPrice"))
    savings = price_entity.get("savings") or {}
    return compact({
        "price": price.get("amount") if price else None,
        "currency": price.get("currency") if price else None,
        "list_price": basis.get("amount") if basis else None,
        "savings_percent": to_float((savings.get("percentage") or {}).get("value") if isinstance(savings.get("percentage"), dict) else savings.get("percentage"), 1),
        "badge": frag_text(badge_entity.get("label")),
        "message": frag_text(badge_entity.get("messaging")),
        "type": details_entity.get("type"),
        "state": details_entity.get("state"),
        "percent_claimed": details_entity.get("percentClaimed"),
        "ends_at": details_entity.get("endTime") or details_entity.get("endsAt"),
        "id": details_entity.get("id"),
    }) or None


# ---- autocomplete ---------------------------------------------------------------------

def suggestion(raw):
    return compact({
        "value": raw.get("value"),
        "type": (raw.get("type") or "").lower() or None,
        "department": (raw.get("scopes") or [{}])[0].get("name") if raw.get("scopes") else None,
        "department_alias": (raw.get("scopes") or [{}])[0].get("alias") if raw.get("scopes") else None,
        "is_ghost": bool(raw.get("ghost")) or None,
    })
