"""Amazon transport: plain curl_cffi with browser-impersonated TLS, no
browser (validated 2026-09-22 from direct Indian egress and residential proxy
exits). Four tricks make the storefronts answer reliably, all applied here
so the endpoint modules only ask for pages:

  1. WARM SESSION — a session per marketplace GETs the homepage first
     (session-id cookie); cold requests are a coin flip (503 "dogs" page /
     captcha). Sessions live in a process-wide identity_pool.Pool in
     exclusive mode (since 2026-11-09), keyed (country, purpose, language):
     a request borrows one for ONE storefront call (warm-up, PoW,
     click-through, delivery location and the re-render all on that
     session) and hands it back, so the next request on ANY of the pod's
     64 threads reuses it. Before, each thread kept its own, so the
     warm-up (1-5 requests) repeated per thread and after every block. A
     session is retired after AMAZON_SESSION_USES calls, AMAZON_SESSION_MAX_AGE
     seconds, or on the first block / transport error (`dead`).
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

Egress (config.AMAZON_EGRESS, 2026-10-19): a new session rides a sticky
proxy DATACENTER exit — never the process's own IP in production
(the pod's IP got Amazon's price-less edge fallback render under bursts).
Blocks (image captcha / 503 dogs / AWS WAF 202 / edge fallback render) and
transport errors throw the session away; AMAZON_DATACENTER_FAILURES of them
on a marketplace (process-wide) move that marketplace to residential exits
in its own country (config.amazon_fallback_proxy) for good, until restart
(_note_failure). Marketplaces that never answer foreign exits (sites.py
`exit_country`: JP, AU) always run through a residential exit pinned to
their country.
Since 2026-11-11 AMAZON_EGRESS is "gateway": the marketplaces listed in
config.AMAZON_GATEWAY_EXITS ride the gateway (exit_gateway.py) named
there — the US one or a country's own, JP and AU included — in front of
those tiers. There a session is cookies only: every request leaves from
another gateway address, and Amazon honours the session cookies across them
(warm-up, delivery location, the data host's copied cookies all hold — the
evidence is in config.py's Amazon block). Failures on the gateway send the
marketplace's new sessions back to the tier behind it (the datacenter
exit; JP / AU their pinned residential exit) for config.AMAZON_GATEWAY_PAUSE
seconds (_note_failure); from there the residential switch above works as
before.

FALLBACK HOOK: if Amazon ever starts serving a JavaScript challenge that
no fingerprint passes, escalate the way g2 does — a patchright pool with
flag "amazon" in config.CHROME_POOLS whose Chrome loads the storefront
and replays these same GETs through the in-page fetch
(patchright_driver.FetchResponse). Not built: dead code while plain HTTP
works.

Failure taxonomy (scraper_errors, mapped to HTTP by route_glue):
  AmazonUpstreamError  transport failure / 5xx / unparsable body   — retryable
  AmazonBlocked        captcha / dogs page after the fallback exit — retryable
  AmazonBadRequest     upstream rejected the params                 — never retried
  AmazonNotFound       ASIN / seller / page does not exist          — never retried
"""
import fastjson
import os
import re
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
import identity_pool
from amazon import sites
from scraper_errors import BadRequest, Blocked, NotFound, UpstreamError
from scraper_utils import debug

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

# No Accept-Language here: every request goes through _Session.headers(),
# which sets the session's own (sites.accept_language of the marketplace
# language), so a literal would only ever be overridden.
PAGE_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
}
AJAX_HEADERS = {
    "accept": "text/html,*/*",
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


# ---- pooled sessions ----------------------------------------------------------------------

_local = threading.local()   # `last`: a pointer to the page session this thread's latest call rode (not a cache)
_egress_lock = threading.Lock()
_dc_failures = {}       # country -> failures seen on the datacenter / direct egress (process-wide)
_residential = set()    # countries moved to residential exits for good (until restart)
_gateway_failures = {}      # country -> time.monotonic() of its latest failures on the gateway exit
_gateway_paused = {}        # country -> time.monotonic() until which its new sessions skip the gateway


class _Session(identity_pool.Identity):
    """One curl_cffi session plus the per-session state the tricks need. The
    pool (identity_pool.Identity) keeps the budget / age / refusal
    bookkeeping; `dead` marks a blocked session for retirement on give-back."""

    def __init__(self, site, proxy, http1=False, language=None, egress="direct"):
        from curl_cffi import requests as curl_requests
        super().__init__(exit=proxy)
        self.egress = egress         # "gateway" | "datacenter" | "residential" | "direct" (see _proxy_for)
        # `proxy` "gateway" / "gateway:<cc>" = the gateway exit: `curl` is then an
        # exit_gateway.Session (a look-alike with its own cookie jar, see
        # cookie_list / set_cookie / keep_language)
        self.gateway = bool(config.gateway_exit(proxy))
        # ONE curl handle for the session's life (no thread-local handles):
        # the pool lends it to one request at a time, so threads take turns
        kwargs = {"impersonate": site["fingerprint"], "timeout": TIMEOUT, "use_thread_local_curl": False}
        if self.gateway:
            # no http_version: it would only shape the hop to the gateway, and the
            # data host answers every call through the gateway as it is
            # (33/33, 2026-11-11)
            from egress import new_session      # lazy: only the gateway exit needs it
            self.curl = new_session(proxy, **kwargs)
        else:
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
        self.set_cookie("i18n-prefs", site["currency"])
        self.set_cookie(self.locale_cookie, self.language)
        self.site = site
        self.proxy = proxy
        self.warmed = False
        self.located = None          # the postal code the session shops from
        self.pow_solved = 0
        self.glow_token = None
        self.closed = False
        self.cookie_snapshot = []    # (name, value, domain, path) after the last page call (page_cookies)

    def headers(self, base):
        """`base` with this session's Accept-Language and User-Agent."""
        out = dict(base)
        out.update(USER_AGENTS.get(self.site["fingerprint"], {}))
        out["accept-language"] = self.accept_language
        return out

    def cookie_list(self):
        """(name, value, domain, path) of every cookie the session holds.
        The gateway's jar keeps no path and stores a Domain= cookie without
        its dot: the marketplace's own domain gets the dot back (so a copy
        handed to the data host's session is sent to data.amazon.<tld>);
        any other domain is one host's own cookie."""
        if self.gateway:
            apex = self.domain[1:]
            return [(c.name, c.value, self.domain if c.domain == apex else c.domain, "/") for c in self.curl.cookies]
        return [(c.name, c.value, c.domain, c.path) for c in self.curl.cookies.jar]

    def set_cookie(self, name, value, domain=None, path="/"):
        """Seed a cookie (the marketplace's dot-domain by default)."""
        if self.gateway:
            self.curl.cookies.set(name, value, domain or self.domain)      # exit_gateway.CookieJar: no path
        else:
            self.curl.cookies.set(name, value, domain=domain or self.domain, path=path)

    def keep_language(self):
        """Put the locale cookie back when a response rewrote it (Amazon
        sets lc-acb<cc> from Accept-Language on the first real page and
        after a captcha). Set-Cookie lands on the bare host while ours sits
        on the dot-domain, so every copy is replaced."""
        if self.gateway:
            # exit_gateway.CookieJar cannot drop one cookie: a stale copy is
            # overwritten where it sits
            cookies = self.cookie_list()
            for name, value, domain, _ in cookies:
                if name == self.locale_cookie and value != self.language:
                    self.set_cookie(name, self.language, domain)
            if not any(name == self.locale_cookie for name, *_ in cookies):
                self.set_cookie(self.locale_cookie, self.language)
            return
        jar = self.curl.cookies.jar
        stale = [c for c in jar if c.name == self.locale_cookie and c.value != self.language]
        for c in stale:
            try:
                jar.clear(c.domain, c.path, c.name)
            except KeyError:
                pass
        if stale or not any(c.name == self.locale_cookie for c in jar):
            self.curl.cookies.set(self.locale_cookie, self.language, domain=self.domain)

    def snapshot_cookies(self):
        """Copy the jar (taken by the borrower, so another thread may read
        the copy later without touching the live jar)."""
        self.cookie_snapshot = self.cookie_list()
        return self.cookie_snapshot

    def close(self):
        """The pool is done with it (on_retire): release the curl session."""
        if self.closed:
            return
        self.closed = True
        try:
            self.curl.close()
        except Exception:
            pass


def _key(country, purpose="page", language=None):
    """The pool key of a session: (marketplace, "page" | "aapi", storefront language)."""
    site = sites.site(country)
    return (site["country"], purpose, language or site["language"])


def _mint(key, hint=None):
    """The pool's mint: a cold session for `key` on the egress _proxy_for
    picks (never warmed here: the first call warms it, like before)."""
    country, purpose, language = key
    site = sites.site(country)
    proxy, egress = _proxy_for(site)
    return _Session(site, proxy, http1=(purpose == "aapi"), language=language, egress=egress)


# Exclusive lending: a session owns its curl handle, so one request at a time;
# the pool waits for a lent session to come back before minting another (see
# identity_pool). Retired sessions are closed here.
_sessions = identity_pool.Pool(
    "amazon", _mint, mode="exclusive",
    budget=lambda: config.AMAZON_SESSION_USES,
    max_age=lambda: config.AMAZON_SESSION_MAX_AGE,
    on_retire=lambda ident, reason: ident.close())


def _gateway_for(country):
    """The gateway exit a NEW session of that marketplace rides ("gateway" =
    the US gateway, "gateway:<cc>" = that country's; config.AMAZON_GATEWAY_EXITS), or
    None: AMAZON_EGRESS is not "gateway", the AMAZON_PROXY_COUNTRY override is
    on, the marketplace is not listed, is paused after gateway failures, or
    was moved to residential exits for good."""
    if not config.gateway_exit(config.AMAZON_EGRESS) or config.AMAZON_PROXY_COUNTRY:
        return None
    if country in _residential or time.monotonic() < _gateway_paused.get(country, 0.0):
        return None
    return config.gateway_exit(config.AMAZON_GATEWAY_EXITS.get(country))


def _proxy_for(site):
    """(proxy, egress) for a NEW session of `site`, per config.AMAZON_EGRESS:
    a residential exit pinned to the country for storefronts that refuse
    foreign IPs, the AMAZON_PROXY_COUNTRY override, else a datacenter exit
    until the marketplace has been moved to residential for good
    (_note_failure), then a residential exit in the marketplace's country.
    Direct egress only when AMAZON_EGRESS is "direct" (a moved marketplace
    whose residential switch is disabled stays on the datacenter exit).
    AMAZON_EGRESS "gateway" puts the gateway exit in front of all that for the
    marketplaces in config.AMAZON_GATEWAY_EXITS (_gateway_for): any other
    marketplace, and one whose gateway use is paused (_note_failure), gets
    the "datacenter" behaviour above unchanged — a pinned storefront its
    residential exit."""
    country = site["country"]
    gateway = _gateway_for(country)
    if gateway:
        return gateway, "gateway"
    if site["exit_country"]:
        return config.amazon_country_proxy(site["exit_country"]), "residential"
    if config.AMAZON_PROXY_COUNTRY:
        return config.amazon_country_proxy(config.AMAZON_PROXY_COUNTRY), "residential"
    egress = config.AMAZON_EGRESS
    if config.gateway_exit(egress):
        egress = "datacenter"       # not listed or paused: the tier behind the gateway
    if egress == "datacenter" and country not in _residential:
        return config.amazon_datacenter_proxy(), "datacenter"
    if egress in ("datacenter", "residential") or country in _residential:
        proxy = config.amazon_fallback_proxy(country)
        if proxy:
            return proxy, "residential"
        # AMAZON_FALLBACK_PROXY_COUNTRY = "" only disables the datacenter ->
        # residential SWITCH: a marketplace configured for residential
        # egress still rides its own country's exit, never the process's IP
        if egress == "residential":
            return config.amazon_country_proxy(country), "residential"
        if egress == "datacenter":
            return config.amazon_datacenter_proxy(), "datacenter"
    return None, "direct"


def _note_failure(sess, reason):
    """A block / fallback render / transport error on a session: counted per
    marketplace while it still runs on the datacenter (or direct) egress;
    at config.AMAZON_DATACENTER_FAILURES the marketplace moves to residential
    exits permanently (until restart). Failures on residential exits or on
    a country-pinned storefront change nothing (there is nowhere further to
    go); its session is simply replaced. On the gateway exit the switch is
    temporary: config.AMAZON_GATEWAY_FAILURES failures of a marketplace within
    config.AMAZON_GATEWAY_WINDOW seconds send its NEW sessions to the tier
    behind it (_proxy_for: the datacenter exit, a pinned storefront's
    residential exit) for config.AMAZON_GATEWAY_PAUSE seconds — so the third
    session of a call the gateway failed twice is already on that tier —
    and then the gateway is tried again."""
    country = sess.site["country"]
    if sess.egress == "gateway":
        now = time.monotonic()
        with _egress_lock:
            recent = [t for t in _gateway_failures.get(country, ()) if now - t < config.AMAZON_GATEWAY_WINDOW] + [now]
            _gateway_failures[country] = recent
            if len(recent) >= config.AMAZON_GATEWAY_FAILURES:
                _gateway_failures[country] = []
                _gateway_paused[country] = now + config.AMAZON_GATEWAY_PAUSE
                print(f"amazon {country}: {len(recent)} failures on the gateway exit within "
                      f"{config.AMAZON_GATEWAY_WINDOW} s (last: {reason}); proxy exits for {config.AMAZON_GATEWAY_PAUSE} s")
        return
    if sess.egress == "residential" or sess.site["exit_country"] or country in _residential:
        return
    if not config.amazon_fallback_proxy(country):
        return
    with _egress_lock:
        _dc_failures[country] = _dc_failures.get(country, 0) + 1
        if _dc_failures[country] >= config.AMAZON_DATACENTER_FAILURES and country not in _residential:
            _residential.add(country)
            print(f"amazon {country}: {_dc_failures[country]} failures on the {sess.egress} egress "
                  f"(last: {reason}); moving this marketplace to residential exits for good")


def drop_session(country, purpose="page", language=None):
    """Retire the session that served this thread's latest call of that key
    (products.py: a page that rendered without a title / inline reviews
    wants its next render on another session). Retired at once when idle,
    on give-back when another thread is using it; a no-op when the thread's
    last call rode a session of another key."""
    sess = getattr(_local, "last", None)
    if sess is not None and sess.key == _key(country, purpose, language):
        _sessions.retire(sess, "refused")
        _local.last = None


def dump_debug(name, text):
    """Write a raw response to $AMAZON_DEBUG_DIR/<name>.txt."""
    debug.dump("AMAZON_DEBUG_DIR", name, text)


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
    jar = sess.curl.cookies if getattr(sess, "gateway", False) else sess.curl.cookies.jar
    return not any(c.name == "session-id" for c in jar)


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


def _render(sess, method, url, hdrs, *, purpose, clicks, params=None, data=None, json_body=None,
            allow_redirects=True):
    """One navigation on a borrowed session with the inline tricks: the
    Akamai PoW, the "continue shopping" submits (`clicks`: a one-element
    counter, CLICK_THROUGHS per call) and the locale cookie. Returns (resp,
    block reason or None); transport errors propagate."""
    def send():
        return sess.curl.request(method, url, params=params or None, data=data, json=json_body, headers=hdrs,
                                 allow_redirects=allow_redirects)
    resp = send()
    if purpose == "page" and is_pow(resp.text or ""):
        if _solve_pow(sess, resp.text):
            resp = send()
    while purpose != "aapi" and clicks[0] < CLICK_THROUGHS and is_click_through(resp.text or ""):
        clicks[0] += 1
        if not click_through(sess, resp):
            break
        resp = send()
    if purpose != "aapi":
        sess.keep_language()
    reason = _blocked_reason(resp)
    if reason is None and purpose == "page" and is_pow(resp.text or ""):
        reason = "unsolved bm-verify interstitial"   # never hand the 2 KB stub to a parser
    if reason is None and purpose == "page" and is_fallback_render(sess, resp):
        reason = "edge fallback render (origin throttled; price hidden)"
    return resp, reason


def _request(country, method, path, *, params=None, data=None, json_body=None, headers=None, purpose="page",
             locate=True, locate_first=False, postal_code=None, language=None, referer=None, label=None,
             allow_redirects=True, cookies=None):
    """One storefront call with the block handling: another pooled session
    on a captcha / dogs page (the blocked one is retired; the fallback exit
    once the marketplace has failed enough), transport retries on 5xx /
    network errors, inline PoW solving, and (page purpose) the delivery
    location: `locate` harvests the token from this page's own body and
    renders the page again on the same session so it shows local prices;
    `locate_first` (a same-host XHR such as the offers) makes the session
    shop locally from a cheap storefront page BEFORE the call. `cookies`
    [(name, value, domain, path)] are set on the borrowed session first
    (the data host's HTTP/1.1 session is authorised by the storefront
    session's cookies).

    One session is borrowed per loop iteration — warm-up, PoW, click-through,
    locate and re-render all ride it — and given back before any back-off
    sleep; a block or a transport error marks it dead (retired on give-back)
    and the next iteration borrows another."""
    site = sites.site(country)
    key = _key(country, purpose, language)
    url = path if path.startswith("http") else site["base"] + path
    label = label or url.split("?")[0]
    wanted = (postal_code or site["postal_code"]) if purpose == "page" and (locate or locate_first) else None
    attempt = 0
    blocks = 0
    clicks = [0]
    delay = 0.0
    while True:
        if delay:
            time.sleep(delay)
        sess = _sessions.borrow(key)
        if purpose == "page":
            _local.last = sess
        try:
            try:
                if purpose == "page":
                    _warm(sess)
                for name, value, domain, cpath in cookies or ():
                    sess.set_cookie(name, value, domain, cpath or "/")
                hdrs = sess.headers(headers or PAGE_HEADERS)
                hdrs.setdefault("referer", _referer(site, referer))
                reason = None
                if locate_first and wanted and sess.located != wanted:
                    # the offers XHR renders for the session's address: a cheap
                    # storefront page carries the glow token (best effort: a
                    # non-block failure leaves the session as it was)
                    page_resp, reason = _render(sess, "GET", site["base"] + "/gp/bestsellers/",
                                                {**sess.headers(PAGE_HEADERS), "referer": _referer(site, None)},
                                                purpose="page", clicks=clicks)
                    if reason is None and page_resp.status_code == 200:
                        _locate_from(sess, page_resp.text, wanted)
                if reason is None:
                    resp, reason = _render(sess, method, url, hdrs, purpose=purpose, clicks=clicks, params=params,
                                           data=data, json_body=json_body, allow_redirects=allow_redirects)
                    if (reason is None and locate and wanted and resp.status_code == 200 and sess.located != wanted
                            and _locate_from(sess, resp.text, wanted)):
                        # the session now shops locally: render this page again on it
                        resp, reason = _render(sess, method, url, hdrs, purpose=purpose, clicks=clicks, params=params,
                                               data=data, json_body=json_body, allow_redirects=allow_redirects)
            except AmazonBlocked as e:
                reason = str(e)
                resp = None
            except Exception as e:
                _note_failure(sess, f"{type(e).__name__}: {e}")
                sess.dead = True
                attempt += 1
                if attempt > RETRIES:
                    raise AmazonUpstreamError(f"{label}: {type(e).__name__}: {e}")
                delay = 0.5 * attempt
                continue
            if reason:
                blocks += 1
                _note_failure(sess, reason)
                sess.dead = True
                if blocks >= BLOCK_RETRIES:
                    raise AmazonBlocked(f"{label}: {reason} (after {blocks} sessions)")
                delay = 1.5 * blocks     # the 503 dogs page is mostly a burst limiter: back off
                continue
            if resp.status_code == 404:
                raise AmazonNotFound(f"{label}: not found")
            if resp.status_code == 400:
                raise AmazonBadRequest(f"{label}: HTTP 400")
            if resp.status_code >= 500:
                attempt += 1
                if attempt > RETRIES:
                    raise AmazonUpstreamError(f"HTTP {resp.status_code} on {label}")
                delay = 0.5 * attempt
                continue
            if purpose == "page":
                sess.snapshot_cookies()
            return resp
        finally:
            _sessions.give_back(sess)


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
    text. `locate=True` first makes the borrowed session shop locally
    (offers), on that same session."""
    site = sites.site(country)
    hdrs = {**AJAX_HEADERS, "referer": referer or site["base"] + "/", **(headers or {})}
    resp = _request(country, method, path, params=params, data=data, json_body=json_body, headers=hdrs,
                    referer=referer, locate=False, locate_first=locate, postal_code=postal_code,
                    language=language, label=label)
    return resp.text or ""


def ajax_json(country, path, params=None, *, method="GET", data=None, json_body=None, referer=None, headers=None,
              locate=False, postal_code=None, language=None, label=None):
    text = ajax(country, path, params, method=method, data=data, json_body=json_body, referer=referer,
                headers=headers, locate=locate, postal_code=postal_code, language=language, label=label)
    try:
        return fastjson.loads(text) if text.strip() else None
    except ValueError:
        dump_debug("non_json", text)
        raise AmazonUpstreamError(f"{label or path}: non-JSON body ({text[:80]!r})")


def page_cookies(country, language=None):
    """The storefront cookies of the page session this thread's latest call
    rode (the one that loaded the deals page), as (name, value, domain,
    path) tuples — handed to worker threads for the data host (deal price
    fan-out). When the thread made no such call, a pooled page session's."""
    key = _key(country, "page", language)
    sess = getattr(_local, "last", None)
    if sess is not None and sess.key == key and not sess.dead:
        return list(sess.cookie_snapshot)
    with _sessions.lend(key) as sess:
        return sess.snapshot_cookies()


def aapi(country, path, *, accept, csrf_token, currency=None, referer=None, cookies=None, label=None):
    """data.amazon.<tld> product API (`Accept` carries the type + expand
    list, `x-api-csrf-token` the deals page's token). HTTP/1.1 session.
    The data host authorises the call through the storefront cookies
    (session-id, …) of the session that loaded the deals page: they are
    copied into the borrowed HTTP/1.1 session before every call — `cookies`
    (from page_cookies) when the caller runs on another thread."""
    site = sites.site(country)
    url = f"https://{site['data_host']}{path}"
    cookies = [(name, value, domain or ("." + site["domain"]), cpath or "/")
               for name, value, domain, cpath in (cookies if cookies is not None else page_cookies(country))]
    headers = {
        "accept": accept,
        "accept-language": site["language"].replace("_", "-"),
        "content-type": "application/json",
        "x-api-csrf-token": csrf_token,
        "x-cc-currency-of-preference": currency or site["currency"],
        "origin": site["base"],
        "referer": referer or site["base"] + "/deals",
    }
    resp = _request(country, "GET", url, headers=headers, purpose="aapi", locate=False, label=label or path,
                    cookies=cookies)
    text = resp.text or ""
    try:
        body = fastjson.loads(text) if text.strip() else None
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
                            headers={"accept": "application/json"},
                            purpose="aapi", locate=False, label="suggestions")
            body = fastjson.loads(resp.text or "")
        except (AmazonUpstreamError, ValueError) as e:
            last = e if isinstance(e, AmazonUpstreamError) else AmazonUpstreamError(f"suggestions on {host}: non-JSON body")
            continue
        if body.get("suggestions") or host == site["completion_region_host"]:
            return body
        last = body
    if isinstance(last, dict):
        return last
    raise last or AmazonUpstreamError("suggestions: no host answered")
