"""amazon/fetch.py on the pooled sessions: no network (a fake curl session
inside a real fetch._Session; the pool's mint is faked). The tricks that
touch the session (warm-up, delivery location, drop_session, page_cookies)
are exercised through the public surfaces.

    python -m pytest amazon/test_fetch.py -q
"""
import os
import sys
import threading
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from amazon import fetch, sites  # noqa: E402
from scraper_utils.testing import hosted_only

CAPTCHA = "<html>Enter the characters you see below<form action='/errors/validateCaptcha'></form></html>"
GLOW_PAGE = '<html>... "anti-csrftoken-a2z":"GLOW" ...</html>'
MODAL = 'CSRF_TOKEN : "MODAL"'


class Resp:
    def __init__(self, status=200, text="<html>page</html>", url="https://www.amazon.com/x", payload=None,
                 headers=None):
        self.status_code, self.text, self.url, self._payload = status, text, url, payload
        self.headers = headers or {}

    def json(self):
        if self._payload is None:
            raise ValueError("not json")
        return self._payload


class FakeCookies:
    def __init__(self):
        self.jar = []

    def set(self, name, value, domain=None, path="/"):
        self.jar.append(SimpleNamespace(name=name, value=value, domain=domain, path=path))


class FakeCurl:
    """Scripted responses for get / post / request alike; every call recorded."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []
        self.cookies = FakeCookies()
        self.closed = False

    def _answer(self, method, url):
        self.calls.append((method, url))
        return self.responses.pop(0)

    def request(self, method, url, **kw):
        return self._answer(method, url)

    def get(self, url, **kw):
        return self._answer("GET", url)

    def post(self, url, **kw):
        return self._answer("POST", url)

    def close(self):
        self.closed = True


@pytest.fixture
def pool(monkeypatch):
    monkeypatch.setattr(fetch, "BLOCK_RETRIES", 1)
    monkeypatch.setattr(fetch, "RETRIES", 0)
    monkeypatch.setattr(fetch, "_note_failure", lambda sess, reason: None)   # keep the egress ledger out of it
    monkeypatch.setattr(fetch.time, "sleep", lambda s: None)
    queue, minted = [], []

    def fake_mint(key, hint=None):
        country, purpose, language = key
        sess = fetch._Session(sites.site(country), None, http1=(purpose == "aapi"), language=language)
        sess.curl.close()
        sess.curl = FakeCurl(queue.pop(0) if queue else [])
        minted.append(sess)
        return sess

    p = fetch._sessions
    p.clear()
    p._mint_fn = fake_mint
    p.minted = minted
    p.script = lambda *per_session: queue.extend(list(r) for r in per_session)
    fetch._local.last = None
    yield p
    p.clear()
    p._mint_fn = fetch._mint
    fetch._local.last = None


def test_three_threads_share_one_warmed_session_and_a_block_retires_it(pool):
    """Three sequential page calls on three different threads ride ONE pooled
    session of (US, page, en_US): the homepage warm-up happens once (before:
    once per thread). A captcha then retires it (closed, nothing idle)."""
    pool.script([Resp(text="<html>home</html>")] + [Resp(text="<html>page %d</html>" % i) for i in (1, 2, 3)]
                + [Resp(text=CAPTCHA)])
    results = []
    for _ in range(3):
        t = threading.Thread(target=lambda: results.append(fetch.page("US", "/dp/B000", locate=False)))
        t.start()
        t.join()
    assert results == ["<html>page 1</html>", "<html>page 2</html>", "<html>page 3</html>"]
    assert len(pool.minted) == 1
    sess = pool.minted[0]
    assert sess.key == ("US", "page", "en_US") and sess.warmed
    assert sess.curl.calls[0] == ("GET", "https://www.amazon.com/") and len(sess.curl.calls) == 4
    assert pool.stats(sess.key)["idle"] == 1 and not sess.closed
    with pytest.raises(fetch.AmazonBlocked):
        fetch.page("US", "/dp/B000", locate=False)
    assert sess.dead and sess.closed and pool.stats(sess.key)["idle"] == 0


def test_offers_xhr_locates_the_borrowed_session_first_on_the_same_session(pool):
    """ajax(locate=True): the delivery-location change (bestsellers page for
    the glow token, modal, address POST) and the XHR all ride ONE borrowed
    session; the next XHR skips it (the session is located)."""
    pool.script([Resp(text="<html>home</html>"), Resp(text=GLOW_PAGE), Resp(text=MODAL),
                 Resp(text='{"isAddressUpdated": 1}', payload={"isAddressUpdated": 1}),
                 Resp(text="offers-1"), Resp(text="offers-2")])
    assert fetch.ajax("US", "/gp/aod/ajax", {"asin": "B000"}, locate=True, postal_code="10001") == "offers-1"
    sess = pool.minted[0]
    assert sess.located == "10001" and sess.glow_token == "GLOW"
    assert [u.split("amazon.com", 1)[1].split("?")[0] for _, u in sess.curl.calls] == [
        "/", "/gp/bestsellers/", "/portal-migration/hz/glow/get-rendered-address-selections",
        "/portal-migration/hz/glow/address-change", "/gp/aod/ajax"]
    assert fetch.ajax("US", "/gp/aod/ajax", {"asin": "B000"}, locate=True, postal_code="10001") == "offers-2"
    assert len(pool.minted) == 1 and len(sess.curl.calls) == 6


def test_page_locate_renders_again_on_the_same_session(pool):
    pool.script([Resp(text="<html>home</html>"), Resp(text=GLOW_PAGE), Resp(text=MODAL),
                 Resp(text="{}", payload={"isAddressUpdated": 1}), Resp(text="<html>local page</html>")])
    assert fetch.page("US", "/dp/B000", postal_code="10001") == "<html>local page</html>"
    sess = pool.minted[0]
    assert sess.located == "10001" and len(pool.minted) == 1
    assert [u for _, u in sess.curl.calls].count("https://www.amazon.com/dp/B000") == 2


def test_drop_session_retires_the_session_this_thread_last_rode(pool):
    pool.script([Resp(text="<html>home</html>"), Resp(text="<html>no title</html>")], [Resp(text="<html>home</html>")])
    fetch.page("US", "/dp/B000", locate=False)
    first = pool.minted[0]
    fetch.drop_session("US", "page")
    assert first.closed and pool.stats(first.key)["idle"] == 0
    fetch.drop_session("US", "page")                 # nothing to drop: a no-op
    fetch.drop_session("DE", "page")
    assert len(pool.minted) == 1


def test_page_cookies_are_the_last_page_sessions_and_reach_the_aapi_session(pool):
    pool.script([Resp(text="<html>home</html>"), Resp(text="<html>deals</html>")],
                [Resp(text='{"entity": {}}', payload={"entity": {}})])
    fetch.page("US", "/deals", locate=False)
    page_sess = pool.minted[0]
    page_sess.curl.cookies.set("session-id", "SID", domain=".amazon.com")
    page_sess.snapshot_cookies()
    cookies = fetch.page_cookies("US")
    assert ("session-id", "SID", ".amazon.com", "/") in cookies
    body = fetch.aapi("US", "/api/marketplaces/X/products/B000", accept="a", csrf_token="t", cookies=cookies)
    assert body == {"entity": {}}
    data_sess = pool.minted[1]
    assert data_sess.key == ("US", "aapi", "en_US")
    assert [(c.name, c.value) for c in data_sess.curl.cookies.jar if c.name == "session-id"] == [("session-id", "SID")]


def test_sessions_are_keyed_by_marketplace_purpose_and_language(pool):
    pool.script([Resp(text="<html>home</html>"), Resp(text="us")], [Resp(text="<html>home</html>"), Resp(text="de")],
                [Resp(text="<html>home</html>"), Resp(text="de-en")])
    assert fetch.page("US", "/x", locate=False) == "us"
    assert fetch.page("DE", "/x", locate=False) == "de"
    assert fetch.page("DE", "/x", locate=False, language="en_GB") == "de-en"
    assert [s.key for s in pool.minted] == [("US", "page", "en_US"), ("DE", "page", "de_DE"), ("DE", "page", "en_GB")]
    assert pool.left(pool.minted[0]) == fetch.config.AMAZON_SESSION_USES - 1


# ---- the gateway exit (config.AMAZON_EGRESS = "gateway") --------------------------------------------

_note_failure = fetch._note_failure          # the real one (the `pool` fixture stubs it)


@pytest.fixture
def tiers(monkeypatch):
    """_proxy_for / _note_failure with fake exits and a clean ledger."""
    clock = [1000.0]
    monkeypatch.setattr(fetch.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(fetch.config, "AMAZON_EGRESS", "gateway")
    monkeypatch.setattr(fetch.config, "AMAZON_GATEWAY_EXITS", {"US": "gateway", "DE": "gateway"})
    monkeypatch.setattr(fetch.config, "AMAZON_PROXY_COUNTRY", None)
    monkeypatch.setattr(fetch.config, "amazon_datacenter_proxy", lambda: "http://dc")
    monkeypatch.setattr(fetch.config, "amazon_fallback_proxy", lambda country: f"http://res-{country.lower()}")
    monkeypatch.setattr(fetch.config, "amazon_country_proxy", lambda country: f"http://pin-{country.lower()}")
    for ledger in ("_dc_failures", "_gateway_failures", "_gateway_paused"):
        monkeypatch.setattr(fetch, ledger, {})
    monkeypatch.setattr(fetch, "_residential", set())
    return clock


def _exit_of(country):
    return fetch._proxy_for(sites.site(country))


def _failed(country, egress):
    _note_failure(SimpleNamespace(site=sites.site(country), egress=egress), "captcha")


@hosted_only
def test_gateway_egress_rides_each_listed_marketplaces_own_gateway_exit(tiers):
    assert _exit_of("US") == ("gateway", "gateway") and _exit_of("DE") == ("gateway", "gateway")
    assert _exit_of("FR") == ("http://dc", "datacenter")            # not listed: the datacenter tier as before
    assert _exit_of("JP") == ("http://pin-jp", "residential")       # a pinned storefront that is not listed
    fetch.config.AMAZON_GATEWAY_EXITS = {"US": "gateway", "FR": "gateway:fr", "JP": "gateway:jp", "NL": "gateway:de"}
    assert _exit_of("FR") == ("gateway:fr", "gateway")                      # its own country's gateway
    assert _exit_of("NL") == ("gateway:de", "gateway")                      # a neighbour's
    assert _exit_of("JP") == ("gateway:jp", "gateway")                      # a pinned storefront rides it when listed
    _failed("JP", "gateway")
    _failed("JP", "gateway")
    assert _exit_of("JP") == ("http://pin-jp", "residential")       # ... and falls back to its pinned exit
    fetch.config.AMAZON_PROXY_COUNTRY = "de"                        # the emergency override beats the gateway
    assert _exit_of("US") == ("http://pin-de", "residential")
    fetch.config.AMAZON_PROXY_COUNTRY = None
    fetch.config.AMAZON_EGRESS = "datacenter"                       # the old knob value: nothing changed
    assert _exit_of("US") == ("http://dc", "datacenter") and _exit_of("JP") == ("http://pin-jp", "residential")


@hosted_only
def test_gateway_failures_pause_it_for_that_marketplace_then_it_is_tried_again(tiers):
    clock = tiers
    _failed("DE", "gateway")
    assert _exit_of("DE") == ("gateway", "gateway")                         # one failure is not a pattern
    clock[0] += fetch.config.AMAZON_GATEWAY_WINDOW + 1
    _failed("DE", "gateway")                                            # the first one is out of the window
    assert _exit_of("DE") == ("gateway", "gateway")
    clock[0] += 5
    _failed("DE", "gateway")
    assert _exit_of("DE") == ("http://dc", "datacenter")            # two inside the window: datacenter exits
    assert _exit_of("US") == ("gateway", "gateway")                         # per marketplace
    assert fetch._dc_failures == {} and not fetch._residential     # gateway failures never count towards residential
    _failed("DE", "datacenter")
    _failed("DE", "datacenter")
    assert _exit_of("DE") == ("http://res-de", "residential")       # the datacenter tier escalates as before
    fetch._residential.clear()
    clock[0] += fetch.config.AMAZON_GATEWAY_PAUSE + 1
    assert _exit_of("DE") == ("gateway", "gateway")                         # the pause is over


@hosted_only
def test_gateway_session_keeps_cookies_in_the_gateways_own_jar():
    """A session on "gateway" holds an exit_gateway.Session (no curl jar): the
    seeded cookies, the locale re-assert and the snapshot handed to the data
    host's session all go through cookie_list / set_cookie."""
    sess = fetch._Session(sites.site("DE"), "gateway", egress="gateway")
    try:
        assert sess.gateway and getattr(sess.curl, "via_exit_gateway", False)
        assert ("i18n-prefs", "EUR", ".amazon.de", "/") in sess.cookie_list()
        assert ("lc-acbde", "de_DE", ".amazon.de", "/") in sess.cookie_list()
        # Amazon rewrites the locale on the dot-domain and sets a host-only copy
        sess.curl.cookies.set("lc-acbde", "en_GB", ".amazon.de")
        sess.curl.cookies.set("lc-acbde", "en_GB", "www.amazon.de")
        sess.curl.cookies.set("session-id", "SID", ".amazon.de")
        sess.keep_language()
        assert {value for name, value, _, _ in sess.cookie_list() if name == "lc-acbde"} == {"de_DE"}
        assert sess.curl.cookies.header_for("www.amazon.de").count("lc-acbde=de_DE") == 2
        # the snapshot's dot-domain cookies reach data.amazon.de on another gateway session
        data = fetch._Session(sites.site("DE"), "gateway", http1=True, egress="gateway")
        for name, value, domain, path in sess.snapshot_cookies():
            data.set_cookie(name, value, domain, path)
        assert "session-id=SID" in data.curl.cookies.header_for("data.amazon.de")
        assert "lc-acbde=de_DE" in data.curl.cookies.header_for("data.amazon.de")
        data.close()
        assert not fetch.is_fallback_render(sess, Resp(text="<html>" + "x" * 60_000))    # session-id is in the jar
        sess.curl.cookies.clear()
        assert fetch.is_fallback_render(sess, Resp(text="<html>" + "x" * 60_000))
    finally:
        sess.close()
