"""Amazon transport: plain curl_cffi with browser-impersonated TLS, no
browser (validated 2026-09-22 from direct Indian egress and residential proxy
exits). Four tricks make the storefronts answer reliably, all applied here
so the endpoint modules only ask for pages:

  1. WARM SESSION — every thread keeps one session per marketplace and
     GETs the homepage first (session-id cookie); cold requests are a coin
     flip (503 "dogs" page / captcha).
  2. REFERER on every navigation (a search URL on the same host) — product
     pages answer a captcha to a chrome fingerprint without it. Marketplaces
     other than .com/.ca captcha the chrome fingerprint regardless and pass
     safari17_0 (sites.py picks per storefront).
  3. AKAMAI PROOF-OF-WORK — search pages (/s) come back as a 2 KB
     `bm-verify` interstitial: the page computes i + Number(a + b) and POSTs
     it with the token to /_sec/verify?provider=interstitial, which sets
     the cookie that unlocks the real page. Solved inline, once per session.
  4. DELIVERY LOCATION — from a foreign exit the buybox says "cannot be
     shipped", the offers ajax says "No featured offers" and prices go
     missing. One POST to the "Deliver to" (glow) address-change endpoint
     with a local postal code (sites.py) makes the session shop as a local.

Surfaces (each returns text or parsed JSON and raises the taxonomy below):
  page(country, path, params)          storefront HTML (dp, s, gp/bestsellers,
                                       deals, sp, shop, …)
  ajax(country, path, params, method)  same-host XHR (aodAjaxMain offers,
                                       sp/ajax/feedback, d2b deals search)
  aapi(country, path, headers)         data.amazon.<tld> product API (the
                                       deals grid's price source) — served
                                       over HTTP/1.1: over h2 CloudFront
                                       answers 503 "Error from cloudfront"
                                       for ~70% of calls, over HTTP/1.1
                                       every call succeeds (measured)
  completion(country, params)          completion.amazon.<tld> suggestions

  5. LANGUAGE — the storefront language is the per-marketplace cookie
     `lc-acb<cc>` (`lc-main` only on amazon.com, sites.locale_cookie) plus
     a matching Accept-Language. Until 2026-10-16 every store got `lc-main`
     (ignored outside .com) and an English header, so foreign exits saw
     English pages whatever `language` asked for.
  6. CURRENT USER-AGENT — curl_cffi's impersonations still announce Safari
     17.0 / Chrome 124. Since ~2026-10 Amazon treats those as stale bots:
     product pages answer the click-through below first and the "Deliver
     to" modal renders sign-in only (no postcode field), so the location
     change silently failed and prices went missing for items that do not
     ship to the exit's country. USER_AGENTS sends a current version over
     the same TLS fingerprint (measured: no click-through, postcode modal,
     isAddressUpdated 1).
  7. CLICK-THROUGH — since ~2026-10 most product requests first answer a
     3.8 KB "Klicke auf die Schaltfläche unten" page: a GET form to
     /errors_page/validateCaptcha (amzn, amzn-r, field-keywords) with no
     image to solve. Submitting it sets x-amz-captcha-1/2 and the same
     session then gets real pages (measured from direct, US, DE, GB exits).

Blocks (image captcha / 503 dogs / AWS WAF 202 JavaScript challenge) are
handled per thread: the session is thrown away and rebuilt; if the rebuilt
session is blocked too
the thread moves that marketplace to a residential exit (config.
amazon_fallback_proxy) for AMAZON_DIRECT_COOLDOWN seconds. Marketplaces
that never answer foreign exits (sites.py `exit_country`: JP, AU) always
run through an exit pinned to their country.

FALLBACK HOOK: if Amazon ever starts serving a JavaScript challenge that
no fingerprint passes, escalate the way g2 does — a patchright pool with
flag "amazon" in config.CHROME_POOLS whose Chrome loads the storefront
and replays these same GETs through the in-page fetch
(patchright_driver.FetchResponse). Not built: dead code while plain HTTP
works.

Failure taxonomy (scraper_errors, mapped to HTTP by route_glue):
  AmazonUpstreamError  transport failure / 5xx / unparsable body   — retryable
  AmazonBlocked        captcha / dogs page after the fallback exit  — retryable
  AmazonBadRequest     upstream rejected the params                 — never retried
  AmazonNotFound       ASIN / seller / page does not exist          — never retried
"""
import json
import os
import re
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
from amazon import sites
from scraper_errors import BadRequest, Blocked, NotFound, UpstreamError

TIMEOUT = 45
RETRIES = 2                    # transport failures / 5xx per call
BLOCK_RETRIES = 4              # fresh session, then fallback exit(s)
CLICK_THROUGHS = 2             # "continue shopping" submits per call before it counts as a block

# User-Agent per curl_cffi fingerprint (sites.py `fingerprint`): a current
# browser version over the fingerprint's TLS. Bump when Amazon starts
# answering the click-through / sign-in-only glow modal again.
USER_AGENTS = {
    "safari17_0": {
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) "
                      "Version/26.0 Safari/605.1.15",
    },
    "chrome": {
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/155.0.0.0 Safari/537.36",
        "sec-ch-ua": '"Google Chrome";v="155", "Chromium";v="155", "Not_A Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
    },
}
FANOUT_WORKERS = 6

PAGE_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.9",
}
AJAX_HEADERS = {
    "accept": "text/html,*/*",
    "accept-language": "en-US,en;q=0.9",
    "x-requested-with": "XMLHttpRequest",
}

_POW_RE = re.compile(r'var i = (\d+); var j = i \+ Number\("(\d+)" \+ "(\d+)"\)')
_POW_TOKEN_RE = re.compile(r'"bm-verify": "([^"]+)"')
_GLOW_TOKEN_RE = re.compile(r'anti-csrftoken-a2z(?:&quot;|"):(?:&quot;|")([^&"]+)')
_MODAL_TOKEN_RE = re.compile(r'CSRF_TOKEN\s*:\s*"([^"]+)"')
_CLICK_FORM_RE = re.compile(r'<form[^>]*action="(/errors[^"]*/validateCaptcha)"[^>]*>(.*?)</form>', re.S | re.I)
_HIDDEN_RE = re.compile(r'<input[^>]*type="hidden"[^>]*name="([^"]+)"[^>]*value="([^"]*)"', re.I)


class AmazonUpstreamError(UpstreamError):
    """Transport failure, 5xx or an unparsable body — retryable."""


class AmazonBlocked(AmazonUpstreamError, Blocked):
    """Captcha / dogs page even after a fresh session and the fallback exit — retryable."""


class AmazonBadRequest(BadRequest):
    """Upstream rejected the request — never retried."""


class AmazonNotFound(NotFound):
    """The ASIN / seller / page does not exist — never retried."""


# ---- per-thread sessions ------------------------------------------------------------------

_local = threading.local()
_fallback_lock = threading.Lock()
_fallback_until = {}    # (thread ident, country) -> time direct egress may be retried


class _Session:
    """One curl_cffi session plus the per-session state the tricks need."""

    def __init__(self, site, proxy, http1=False, language=None):
        from curl_cffi import requests as curl_requests
        kwargs = {"impersonate": site["fingerprint"], "timeout": TIMEOUT}
        if http1:
            kwargs["http_version"] = 1
        self.curl = curl_requests.Session(**kwargs)
        if proxy:
            self.curl.proxies = {"http": proxy, "https": proxy}
        # Display currency + language are cookies, not the exit's geo: without
        # them a session from India sees every .com price converted to INR,
        # and a US exit sees amazon.de in English.
        self.domain = "." + site["domain"]
        self.language = language or site["language"]
        self.locale_cookie = sites.locale_cookie(site)
        self.accept_language = sites.accept_language(self.language)
        self.curl.cookies.set("i18n-prefs", site["currency"], domain=self.domain)
        self.curl.cookies.set(self.locale_cookie, self.language, domain=self.domain)
        self.site = site
        self.proxy = proxy
        self.warmed = False
        self.located = None          # the postal code the session shops from
        self.pow_solved = 0
        self.glow_token = None

    def headers(self, base):
        """`base` with this session's Accept-Language and User-Agent."""
        out = dict(base)
        out.update(USER_AGENTS.get(self.site["fingerprint"], {}))
        out["accept-language"] = self.accept_language
        return out

    def keep_language(self):
        """Put the locale cookie back when a response rewrote it (Amazon
        sets lc-acb<cc> from Accept-Language on the first real page and
        after a captcha). Set-Cookie lands on the bare host while ours sits
        on the dot-domain, so every copy is replaced."""
        jar = self.curl.cookies.jar
        stale = [c for c in jar if c.name == self.locale_cookie and c.value != self.language]
        for c in stale:
            try:
                jar.clear(c.domain, c.path, c.name)
            except KeyError:
                pass
        if stale or not any(c.name == self.locale_cookie for c in jar):
            self.curl.cookies.set(self.locale_cookie, self.language, domain=self.domain)

    def close(self):
        try:
            self.curl.close()
        except Exception:
            pass


def _store():
    store = getattr(_local, "sessions", None)
    if store is None:
        store = _local.sessions = {}
    return store


def _on_fallback(country):
    until = _fallback_until.get((threading.get_ident(), country), 0.0)
    return bool(until) and time.time() < until


def _enter_fallback(country):
    with _fallback_lock:
        _fallback_until[(threading.get_ident(), country)] = time.time() + config.AMAZON_DIRECT_COOLDOWN


def _proxy_for(site):
    """The exit a NEW session for `site` should use: a pinned country exit
    for storefronts that refuse foreign IPs, the configured global exit,
    the thread's fallback exit while its cooldown runs, else direct."""
    if site["exit_country"]:
        return config.amazon_country_proxy(site["exit_country"])
    if config.AMAZON_PROXY_COUNTRY:
        return config.amazon_country_proxy(config.AMAZON_PROXY_COUNTRY)
    if _on_fallback(site["country"]):
        return config.amazon_fallback_proxy(site["country"])
    return None


def session(country, purpose="page", language=None):
    """The calling thread's session for a marketplace (+ purpose: "page" for
    the storefront, "aapi" for the HTTP/1.1 data host; + the storefront
    language when not the marketplace default), created on demand."""
    site = sites.site(country)
    language = language or site["language"]
    key = (site["country"], purpose, language)
    store = _store()
    sess = store.get(key)
    if sess is None:
        sess = store[key] = _Session(site, _proxy_for(site), http1=(purpose == "aapi"), language=language)
    return sess


def drop_session(country, purpose="page", language=None):
    site = sites.site(country)
    sess = _store().pop((site["country"], purpose, language or site["language"]), None)
    if sess is not None:
        sess.close()


def dump_debug(name, text):
    """Write a raw response to $AMAZON_DEBUG_DIR/<name>.txt."""
    dbg = os.environ.get("AMAZON_DEBUG_DIR", "")
    if dbg and text:
        try:
            os.makedirs(dbg, exist_ok=True)
            with open(os.path.join(dbg, name + ".txt"), "w") as f:
                f.write(text)
        except OSError:
            pass


# ---- block detection ------------------------------------------------------------------------

def is_captcha(text):
    return "validateCaptcha" in text or "Enter the characters you see below" in text


def is_dogs(text):
    return "Sorry! Something went wrong" in text and len(text) < 20000


def is_pow(text):
    return "bm-verify" in text and "_sec/verify" in text and len(text) < 20000


def is_signin(url):
    return "/ap/signin" in (url or "")


def is_click_through(text):
    """The "continue shopping" captcha: a button-only form, no image."""
    return "validateCaptcha" in text and "captchacharacters" not in text and len(text) < 20000 \
        and _CLICK_FORM_RE.search(text) is not None


def is_waf_challenge(resp):
    """CloudFront / AWS WAF JavaScript challenge (202, ~2 KB, gokuProps +
    challenge.js). Needs a browser-minted aws-waf-token; a fresh session
    usually is not challenged."""
    text = resp.text or ""
    return resp.status_code == 202 and ("gokuProps" in text or "awsWafCookieDomainList" in text or len(text) < 2000)


def _blocked_reason(resp):
    text = resp.text or ""
    if resp.status_code == 503 and (is_dogs(text) or len(text) < 3000):
        return "503 dogs page"
    if is_captcha(text):
        return "captcha"
    if is_waf_challenge(resp):
        return "202 AWS WAF challenge"
    return None


FALLBACK_RENDER_HEADER = "x-edgex-ers-fallback-status"
FALLBACK_MIN_BYTES = 50_000        # a full storefront page; fragments / stubs are judged elsewhere


def is_fallback_render(sess, resp):
    """Amazon's edge fallback: when the origin throttles an IP (bursts from
    the pod's egress, 2026-10-19; a home IP never saw it) the CDN answers a
    200 storefront page from its error-recovery path — header
    `x-edgex-ers-fallback-status: success`, NO content-type, NO session
    cookies — rendered for nobody: no delivery widget, the one-time price
    replaced by "add this item to your cart to see details" (the Subscribe &
    Save price still shows), no availability. ~1 in 5 fresh sessions at
    normal load, every request during a burst, and a session that got it
    keeps getting it. The caller treats it as a block (fresh session, then
    the fallback exit, which renders fine meanwhile). Two signals, either
    suffices: the header, or a full HTML page that left the jar without a
    `session-id` (a real render always sets one on the first page)."""
    if resp.status_code != 200:
        return False
    if resp.headers.get(FALLBACK_RENDER_HEADER):
        return True
    text = resp.text or ""
    if len(text) < FALLBACK_MIN_BYTES or "<html" not in text[:4000].lower():
        return False
    return not any(c.name == "session-id" for c in sess.curl.cookies.jar)


# ---- the tricks --------------------------------------------------------------------------------

def _warm(sess):
    """Homepage GET for the session cookies (+ the glow token for the
    delivery-location change). The homepage now mostly answers the 2 KB WAF
    challenge (no cookies, harmless: the product pages are not challenged)
    or the click-through (submitted here); a dogs page / image captcha
    raises AmazonBlocked so the caller rotates the session."""
    if sess.warmed:
        return
    resp = sess.curl.get(sess.site["base"] + "/", headers=sess.headers(PAGE_HEADERS))
    if is_click_through(resp.text or ""):
        click_through(sess, resp)
    elif not is_waf_challenge(resp):
        reason = _blocked_reason(resp)
        if reason:
            raise AmazonBlocked(f"warm-up on {sess.site['host']}: {reason}")
    sess.warmed = True
    sess.keep_language()
    match = _GLOW_TOKEN_RE.search(resp.text or "")
    if match:
        sess.glow_token = match.group(1)


def click_through(sess, resp):
    """Submit the "continue shopping" form (GET /errors_page/validateCaptcha
    with its hidden fields) on `sess`. True when Amazon accepted it (it
    redirects and sets x-amz-captcha-1/2); the caller then repeats its
    request on the same session."""
    import html as html_lib
    match = _CLICK_FORM_RE.search(resp.text or "")
    if not match:
        return False
    fields = {name: html_lib.unescape(value) for name, value in _HIDDEN_RE.findall(match.group(2))}
    try:
        answer = sess.curl.get(sess.site["base"] + match.group(1), params=fields,
                               headers={**sess.headers(PAGE_HEADERS), "referer": str(resp.url)}, allow_redirects=False)
    except Exception:
        return False
    sess.keep_language()
    return answer.status_code in (200, 301, 302, 303) and not is_captcha(answer.text or "")


def _solve_pow(sess, text):
    """Akamai interstitial: POST the proof-of-work with the bm-verify token."""
    match = _POW_RE.search(text)
    token = _POW_TOKEN_RE.search(text)
    if not (match and token):
        return False
    pow_value = int(match.group(1)) + int(match.group(2) + match.group(3))
    resp = sess.curl.post(sess.site["base"] + "/_sec/verify?provider=interstitial",
                          json={"bm-verify": token.group(1), "pow": pow_value},
                          headers={**sess.headers(PAGE_HEADERS), "content-type": "application/json", "origin": sess.site["base"]})
    sess.pow_solved += 1
    return resp.status_code == 200


def set_location(sess, postal_code=None):
    """Make the session shop from a local address (the "Deliver to" glow
    modal flow): a page token (any storefront page carries one in its
    nav data; the warm-up homepage answer is a 2 KB stub without it)
    unlocks the rendered modal, whose own token unlocks the address change.
    Best-effort: any failure leaves the session as it was (prices then
    follow the exit's country). Returns True when the address changed."""
    postal_code = postal_code or sess.site["postal_code"]
    if not postal_code or sess.located == postal_code or not sess.glow_token:
        return False
    base = sess.site["base"]
    ok = False
    try:
        headers = {**sess.headers(AJAX_HEADERS), "anti-csrftoken-a2z": sess.glow_token, "referer": base + "/"}
        resp = sess.curl.get(base + "/portal-migration/hz/glow/get-rendered-address-selections",
                             params={"deviceType": "desktop", "pageType": "Detail", "storeContext": "generic",
                                     "actionSource": "desktop-modal"},
                             headers=headers)
        match = _MODAL_TOKEN_RE.search(resp.text or "")
        token = match.group(1) if match else sess.glow_token
        resp = sess.curl.post(base + "/portal-migration/hz/glow/address-change", params={"actionSource": "glow"},
                              json={"locationType": "LOCATION_INPUT", "zipCode": postal_code, "deviceType": "web",
                                    "storeContext": "generic", "pageType": "Detail", "actionSource": "glow"},
                              headers={**headers, "anti-csrftoken-a2z": token, "content-type": "application/json"})
        body = resp.json() if resp.status_code == 200 and (resp.text or "").startswith("{") else {}
        ok = bool(body.get("isAddressUpdated") or body.get("successful"))
    except Exception:
        ok = False
    sess.located = postal_code   # never retried within a session; a new session tries again
    return ok


def _locate_from(sess, text, postal_code=None):
    """Harvest the glow token from a page body and change the delivery
    address; True when the caller should re-fetch (the address changed)."""
    if not sess.glow_token:
        match = _GLOW_TOKEN_RE.search(text or "")
        if not match:
            return False
        sess.glow_token = match.group(1)
    return set_location(sess, postal_code)


# ---- requests ------------------------------------------------------------------------------------

def _referer(site, referer):
    return referer or f"{site['base']}/s?k=amazon"


def _request(country, method, path, *, params=None, data=None, json_body=None, headers=None, purpose="page",
             locate=True, postal_code=None, language=None, referer=None, label=None, allow_redirects=True):
    """One storefront call with the block handling: fresh session on a
    captcha / dogs page, fallback exit after that, transport retries on
    5xx / network errors, inline PoW solving, and (page purpose) the
    one-off delivery-location change harvested from the first page body —
    which re-fetches that page so it renders for the local address."""
    site = sites.site(country)
    url = path if path.startswith("http") else site["base"] + path
    label = label or url.split("?")[0]
    attempt = 0
    blocks = 0
    relocated = False
    clicks = 0
    while True:
        sess = session(country, purpose, language)
        try:
            if purpose == "page":
                _warm(sess)
            hdrs = sess.headers(headers or PAGE_HEADERS)
            hdrs.setdefault("referer", _referer(site, referer))
            resp = sess.curl.request(method, url, params=params or None, data=data, json=json_body, headers=hdrs,
                                     allow_redirects=allow_redirects)
            if purpose == "page" and is_pow(resp.text or ""):
                if _solve_pow(sess, resp.text):
                    resp = sess.curl.request(method, url, params=params or None, data=data, json=json_body,
                                             headers=hdrs, allow_redirects=allow_redirects)
            while purpose != "aapi" and clicks < CLICK_THROUGHS and is_click_through(resp.text or ""):
                clicks += 1
                if not click_through(sess, resp):
                    break
                resp = sess.curl.request(method, url, params=params or None, data=data, json=json_body,
                                         headers=hdrs, allow_redirects=allow_redirects)
            if purpose != "aapi":
                sess.keep_language()
            reason = _blocked_reason(resp)
            if reason is None and purpose == "page" and is_pow(resp.text or ""):
                reason = "unsolved bm-verify interstitial"   # never hand the 2 KB stub to a parser
            if reason is None and purpose == "page" and is_fallback_render(sess, resp):
                reason = "edge fallback render (origin throttled; price hidden)"
        except AmazonBlocked as e:
            reason = str(e)
            resp = None
        except Exception as e:
            proxied = sess.proxy is not None
            drop_session(country, purpose, language)
            if proxied and not site["exit_country"] and _on_fallback(site["country"]):
                # the fallback exit itself is failing (e.g. proxy auth 407):
                # leave the fallback and go back to direct egress
                with _fallback_lock:
                    _fallback_until.pop((threading.get_ident(), site["country"]), None)
            attempt += 1
            if attempt > RETRIES:
                raise AmazonUpstreamError(f"{label}: {type(e).__name__}: {e}")
            time.sleep(0.5 * attempt)
            continue
        if reason:
            blocks += 1
            drop_session(country, purpose, language)
            if blocks >= BLOCK_RETRIES:
                raise AmazonBlocked(f"{label}: {reason} (after {blocks} sessions)")
            if blocks >= 2 and not site["exit_country"] and config.amazon_fallback_proxy(site["country"]):
                _enter_fallback(site["country"])
            time.sleep(1.5 * blocks)     # the 503 dogs page is mostly a burst limiter: back off
            continue
        if resp.status_code == 404:
            raise AmazonNotFound(f"{label}: not found")
        if resp.status_code == 400:
            raise AmazonBadRequest(f"{label}: HTTP 400")
        if resp.status_code >= 500:
            attempt += 1
            if attempt > RETRIES:
                raise AmazonUpstreamError(f"HTTP {resp.status_code} on {label}")
            time.sleep(0.5 * attempt)
            continue
        if purpose == "page" and locate and not relocated and resp.status_code == 200:
            wanted = postal_code or site["postal_code"]
            if wanted and sess.located != wanted and _locate_from(sess, resp.text, wanted):
                relocated = True     # the session now shops locally: render this page again
                continue
        return resp


def ensure_location(country, postal_code=None, language=None):
    """Make sure the thread's session for `country` shops from a local
    address before a same-host XHR (offers) that renders location-dependent
    data: harvests the token from a cheap storefront page when needed."""
    site = sites.site(country)
    wanted = postal_code or site["postal_code"]
    if not wanted:
        return False
    sess = session(country, "page", language)
    if sess.located == wanted:
        return True
    _request(country, "GET", "/gp/bestsellers/", locate=True, postal_code=wanted, language=language, label="locate")
    return session(country, "page", language).located == wanted


def page(country, path, params=None, *, referer=None, locate=True, postal_code=None, language=None, label=None):
    """Storefront HTML. Sign-in redirects (login-gated pages) raise
    AmazonBlocked with a clear message."""
    resp = _request(country, "GET", path, params=params, referer=referer, locate=locate, postal_code=postal_code,
                    language=language, label=label)
    if is_signin(resp.url):
        raise AmazonBlocked(f"{label or path}: Amazon requires a signed-in user for this page")
    return resp.text or ""


def page_with_url(country, path, params=None, *, referer=None, locate=True, postal_code=None, language=None,
                  label=None):
    """(final url, html) — for callers that follow canonical redirects."""
    resp = _request(country, "GET", path, params=params, referer=referer, locate=locate, postal_code=postal_code,
                    language=language, label=label)
    if is_signin(resp.url):
        raise AmazonBlocked(f"{label or path}: Amazon requires a signed-in user for this page")
    return resp.url, (resp.text or "")


def ajax(country, path, params=None, *, method="GET", data=None, json_body=None, referer=None, headers=None,
         locate=False, postal_code=None, language=None, label=None):
    """Same-host XHR (offers, seller feedback, deals search) -> response
    text. `locate=True` first makes the session shop locally (offers)."""
    site = sites.site(country)
    if locate:
        ensure_location(country, postal_code, language)
    hdrs = {**AJAX_HEADERS, "referer": referer or site["base"] + "/", **(headers or {})}
    resp = _request(country, method, path, params=params, data=data, json_body=json_body, headers=hdrs,
                    referer=referer, locate=False, language=language, label=label)
    return resp.text or ""


def ajax_json(country, path, params=None, *, method="GET", data=None, json_body=None, referer=None, headers=None,
              locate=False, postal_code=None, language=None, label=None):
    text = ajax(country, path, params, method=method, data=data, json_body=json_body, referer=referer,
                headers=headers, locate=locate, postal_code=postal_code, language=language, label=label)
    try:
        return json.loads(text) if text.strip() else None
    except ValueError:
        dump_debug("non_json", text)
        raise AmazonUpstreamError(f"{label or path}: non-JSON body ({text[:80]!r})")


def page_cookies(country, language=None):
    """The storefront cookies of the calling thread's page session, as
    (name, value, domain, path) tuples — handed to worker threads whose
    own sessions never loaded a page (deal price fan-out)."""
    sess = session(country, "page", language)
    return [(c.name, c.value, c.domain, c.path) for c in sess.curl.cookies.jar]


def aapi(country, path, *, accept, csrf_token, currency=None, referer=None, cookies=None, label=None):
    """data.amazon.<tld> product API (`Accept` carries the type + expand
    list, `x-api-csrf-token` the deals page's token). HTTP/1.1 session.
    The data host authorises the call through the storefront cookies
    (session-id, …) of the session that loaded the deals page: they are
    copied into the HTTP/1.1 session before every call — `cookies` (from
    page_cookies) when the caller runs on another thread."""
    site = sites.site(country)
    url = f"https://{site['data_host']}{path}"
    data_sess = session(country, "aapi")
    for name, value, domain, cpath in (cookies if cookies is not None else page_cookies(country)):
        data_sess.curl.cookies.set(name, value, domain=domain or ("." + site["domain"]), path=cpath or "/")
    headers = {
        "accept": accept,
        "accept-language": site["language"].replace("_", "-"),
        "content-type": "application/json",
        "x-api-csrf-token": csrf_token,
        "x-cc-currency-of-preference": currency or site["currency"],
        "origin": site["base"],
        "referer": referer or site["base"] + "/deals",
    }
    resp = _request(country, "GET", url, headers=headers, purpose="aapi", locate=False, label=label or path)
    text = resp.text or ""
    try:
        body = json.loads(text) if text.strip() else None
    except ValueError:
        raise AmazonUpstreamError(f"{label or path}: non-JSON body ({text[:80]!r})")
    if isinstance(body, dict) and body.get("type") == "error/v1":
        status = ((body.get("entity") or {}).get("status")) or resp.status_code
        if status == 404:
            raise AmazonNotFound(f"{label or path}: not found")
        raise AmazonUpstreamError(f"{label or path}: AAPI error {status}")
    return body


def completion(country, params):
    """completion.amazon.<tld> keyword suggestions -> parsed JSON."""
    site = sites.site(country)
    query = {"mid": site["marketplace_id"], "alias": "aps", "site-variant": "desktop", "version": "3",
             "event": "onKeyPress", "wc": "", "lop": site["language"], "client-info": "amazon-search-ui"}
    query.update(params)
    last = None
    hosts = dict.fromkeys((site["completion_host"], site["completion_region_host"]))
    for host in hosts:
        try:
            resp = _request(country, "GET", f"https://{host}/api/2017/suggestions", params=query,
                            headers={"accept": "application/json", "accept-language": "en-US,en;q=0.9"},
                            purpose="aapi", locate=False, label="suggestions")
            body = json.loads(resp.text or "")
        except (AmazonUpstreamError, ValueError) as e:
            last = e if isinstance(e, AmazonUpstreamError) else AmazonUpstreamError(f"suggestions on {host}: non-JSON body")
            continue
        if body.get("suggestions") or host == site["completion_region_host"]:
            return body
        last = body
    if isinstance(last, dict):
        return last
    raise last or AmazonUpstreamError("suggestions: no host answered")
