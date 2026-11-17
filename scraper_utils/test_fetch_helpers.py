"""The fetch-layer helpers against verbatim copies of the code they replaced.

Each reference below is the function as it stood in the site modules. The
tests drive the reference and the shared helper through the same scripts and
require the same result and the same side effects, in the same order."""
import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest

from scraper_utils import debug, fanout, memo, retry, sessions


class Boom(Exception):
    pass


class Other(Exception):
    pass


# ---- retry ---------------------------------------------------------------------------------

def _plain(fn, config, local, drop):
    last = None
    for attempt in range(1, config.MAX_RETRIES + 1):
        try:
            return fn()
        except Boom as e:
            last = e
            drop()
            if attempt < config.MAX_RETRIES:
                time.sleep(config.RETRY_BACKOFF * attempt)
    raise last


def _sleep_after_handler(fn, config, local, drop):
    last = None
    for attempt in range(1, config.MAX_RETRIES + 1):
        try:
            return fn()
        except Boom as e:
            last = e
            drop()
        if attempt < config.MAX_RETRIES:
            time.sleep(config.RETRY_BACKOFF * attempt)
    raise last


def _ladder(fn, config, local, drop):
    last = None
    local.past_gateway = False
    try:
        for attempt in range(1, config.MAX_RETRIES + 1):
            try:
                return fn()
            except Boom as e:
                last = e
                drop()
            if attempt < config.MAX_RETRIES:
                local.past_gateway = attempt + 1 == config.MAX_RETRIES
                time.sleep(config.RETRY_BACKOFF * attempt)
        raise last
    finally:
        local.past_gateway = False


SCRIPTS = [["ok"], [Boom, "ok"], [Boom, Boom, "ok"], [Boom] * 6, [Other], [Boom, Other], [Boom, Boom, Other],
           [KeyboardInterrupt], [Boom, Boom, Boom, "ok"]]


def _trace(loop, script, retries, monkeypatch):
    """Everything observable about one call: outcome, drops, sleeps, and the
    flag each attempt saw."""
    events = []
    config = SimpleNamespace(MAX_RETRIES=retries, RETRY_BACKOFF=0.5)
    local = threading.local()
    steps = iter(script)
    monkeypatch.setattr(time, "sleep", lambda seconds: events.append(("sleep", seconds)))

    def fn():
        events.append(("attempt", getattr(local, "past_gateway", "unset")))
        step = next(steps)
        if step == "ok":
            return "done"
        raise step("x")

    try:
        outcome = ("returned", loop(fn, config, local, lambda: events.append(("drop",))))
    except BaseException as e:
        outcome = ("raised", type(e).__name__, str(e))
    return outcome, events, getattr(local, "past_gateway", "unset")


@pytest.mark.parametrize("retries", [0, 1, 2, 3, 4])
@pytest.mark.parametrize("script", SCRIPTS)
def test_run_is_the_plain_loop(script, retries, monkeypatch):
    def shared(fn, config, local, drop):
        return retry.run(fn, config, Boom, lambda e: drop())
    expected = _trace(_plain, script, retries, monkeypatch)
    assert _trace(shared, script, retries, monkeypatch) == expected
    assert _trace(_sleep_after_handler, script, retries, monkeypatch) == expected


@pytest.mark.parametrize("retries", [0, 1, 2, 3, 4])
@pytest.mark.parametrize("script", SCRIPTS)
def test_run_flagged_is_the_ladder_loop(script, retries, monkeypatch):
    def shared(fn, config, local, drop):
        return retry.run_flagged(fn, config, Boom, lambda e: drop(), local)
    assert _trace(shared, script, retries, monkeypatch) == _trace(_ladder, script, retries, monkeypatch)


def test_run_reads_the_config_on_every_pass(monkeypatch):
    config = SimpleNamespace(MAX_RETRIES=5, RETRY_BACKOFF=1)
    slept = []
    monkeypatch.setattr(time, "sleep", slept.append)

    def fn():
        config.MAX_RETRIES = 2            # lowered mid-call: no sleep after the second failure
        raise Boom("x")

    with pytest.raises(Boom):
        retry.run(fn, config, Boom, None)
    assert slept == [1]


# ---- sessions ------------------------------------------------------------------------------

class FakeSession:
    def __init__(self, fail=False):
        self.closed, self.fail = 0, fail

    def close(self):
        self.closed += 1
        if self.fail:
            raise RuntimeError("close failed")


def test_drop_closes_and_clears_the_thread_session():
    local = threading.local()
    sessions.drop(local)                                  # nothing to drop
    assert local.session is None
    for fail in (False, True):
        sess = local.session = FakeSession(fail)
        sessions.drop(local)
        assert local.session is None and sess.closed == 1


def test_thread_store_and_keyed_sessions():
    local = threading.local()
    store = sessions.thread_store(local)
    assert isinstance(store, OrderedDict) and sessions.thread_store(local) is store and local.sessions is store
    built = []

    def build():
        built.append(FakeSession())
        return built[-1]

    first = sessions.keyed(store, "a", build, 2)
    assert sessions.keyed(store, "a", build, 2) is first and len(built) == 1
    sessions.keyed(store, "b", build, 2)
    sessions.keyed(store, "a", build, 2)                  # "a" is now the most recent
    sessions.keyed(store, "c", build, 2)                  # evicts "b", the least recent
    assert list(store) == ["a", "c"] and built[1].closed == 1 and first.closed == 0
    sessions.drop_keyed(store, "a")
    sessions.drop_keyed(store, "missing")
    assert list(store) == ["c"] and first.closed == 1
    sessions.close_quietly(None)
    sessions.close_quietly(FakeSession(fail=True))


# ---- memo ----------------------------------------------------------------------------------

def _old_memoized(store, lock, cap, key, fn, ttl):
    now = time.monotonic()
    with lock:
        hit = store.get(key)
        if hit and now - hit[0] < ttl:
            store.move_to_end(key)
            return hit[1]
    value = fn()
    with lock:
        store[key] = (now, value)
        store.move_to_end(key)
        while len(store) > cap:
            store.popitem(last=False)
    return value


def _old_memo(store, lock, key, ttl, build):
    now = time.time()
    with lock:
        hit = store.get(key)
        if hit and hit[0] > now:
            return hit[1]
    value = build()
    with lock:
        store[key] = (now + ttl, value)
    return value


def _memo_script(cached, monkeypatch, clock_name):
    clock = [100.0]
    monkeypatch.setattr(time, clock_name, lambda: clock[0])
    calls, out = [], []

    def build(key):
        def fn():
            calls.append(key)
            return f"{key}@{clock[0]}"
        return fn

    # the last pair lands exactly on the ttl
    for key, advance in [("a", 0), ("a", 5), ("b", 0), ("a", 4.9), ("c", 0), ("a", 0.2), ("b", 0), ("a", 30), ("d", 0),
                         ("e", 0), ("e", 10)]:
        clock[0] += advance
        out.append(cached(key, build(key)))
    return out, calls


def test_memoized_is_the_old_lru(monkeypatch):
    def run(impl):
        store, lock = OrderedDict(), threading.Lock()
        result = _memo_script(lambda key, fn: impl(store, lock, 2, key, fn, 10), monkeypatch, "monotonic")
        return result, list(store.items())
    assert run(memo.memoized) == run(_old_memoized)


def test_expiring_and_remember(monkeypatch):
    def run(impl):
        store, lock = {}, threading.Lock()
        result = _memo_script(lambda key, fn: impl(store, lock, key, 10, fn), monkeypatch, "time")
        return result, sorted(store.items())
    assert run(memo.expiring) == run(_old_memo)
    store, lock = {}, threading.Lock()
    monkeypatch.setattr(time, "time", lambda: 50.0)
    memo.remember(store, lock, "k", 10, "v")
    assert store == {"k": (60.0, "v")} and memo.expiring(store, lock, "k", 10, lambda: "rebuilt") == "v"


# ---- fan-out -------------------------------------------------------------------------------

def _fail():
    raise Boom("x")


def test_per_call_and_guarded_per_call():
    assert fanout.per_call([], 4) == [] and fanout.per_call([lambda: 1], 4) == [1]
    assert fanout.per_call([lambda: 1, lambda: 2, lambda: 3], 2) == [1, 2, 3]
    with pytest.raises(Boom):
        fanout.per_call([lambda: 1, _fail, lambda: 3], 2)
    assert fanout.guarded_per_call([], 4) == []
    out = fanout.guarded_per_call([lambda: 1, _fail, lambda: 3], 0)          # workers below 1 still run
    assert out[0] == ("ok", 1) and out[2] == ("ok", 3) and out[1][0] == "error" and isinstance(out[1][1], Boom)


def test_windowed_uses_the_pool_for_its_nesting_level():
    pools = (ThreadPoolExecutor(2, thread_name_prefix="x-fanout-0"), ThreadPoolExecutor(2, thread_name_prefix="x-fanout-1"))

    def where():
        return threading.current_thread().name[:10]

    def nested():
        inner = fanout.windowed(pools, "x-fanout", [where, where], 2)       # from an outer thread: the inner pool
        innermost = fanout.windowed(pools, "x-fanout", [lambda: pools[1].submit(
            lambda: fanout.windowed(pools, "x-fanout", [where], 2)).result()], 2)
        return where(), inner, innermost

    try:
        assert fanout.windowed(pools, "x-fanout", [], 2) == []
        assert fanout.windowed(pools, "x-fanout", [lambda: 1, lambda: 2, lambda: 3], 2) == [1, 2, 3]
        outer, inner, innermost = fanout.windowed(pools, "x-fanout", [nested], 2)[0]
        assert outer == "x-fanout-0" and inner == ["x-fanout-1", "x-fanout-1"] and innermost == [["x-fanout-1"]]
        ran = []
        with pytest.raises(Boom):                         # the first failure in order; nothing new starts after it
            fanout.windowed(pools, "x-fanout", [_fail] + [lambda i=i: ran.append(i) for i in range(6)], 1)
        assert ran == []
    finally:
        for pool in pools:
            pool.shutdown()


def test_soft_and_gather():
    assert fanout.soft(_fail, "d", (Boom,))() == "d" and fanout.soft(lambda: 1, "d", (Boom,))() == 1
    with pytest.raises(Boom):
        fanout.soft(_fail, "d", (Other,))()

    def serial(fns):
        return [fn() for fn in fns]

    assert fanout.gather(serial, {"a": lambda: 1, "b": _fail}, (Boom,)) == {"a": 1, "b": None}
    assert fanout.gather_optional(serial, {"a": lambda: 1, "b": _fail}, ("b",), (Boom,)) == {"a": 1, "b": None}
    with pytest.raises(Boom):
        fanout.gather_optional(serial, {"a": lambda: 1, "b": _fail}, ("a",), (Boom,))


# ---- debug dump ----------------------------------------------------------------------------

def test_dump(tmp_path, monkeypatch):
    monkeypatch.delenv("X_DEBUG_DIR", raising=False)
    debug.dump("X_DEBUG_DIR", "a", "body")                # unset: nothing happens
    target = tmp_path / "dumps"
    monkeypatch.setenv("X_DEBUG_DIR", str(target))
    debug.dump("X_DEBUG_DIR", "a", "")                    # empty text: nothing written
    assert not target.exists()
    debug.dump("X_DEBUG_DIR", "a", "body")
    debug.dump("X_DEBUG_DIR", "b", "café", encoding="utf-8")
    assert (target / "a.txt").read_text() == "body" and (target / "b.txt").read_text(encoding="utf-8") == "café"
    monkeypatch.setenv("X_DEBUG_DIR", str(tmp_path / "a-file"))
    (tmp_path / "a-file").write_text("in the way")
    debug.dump("X_DEBUG_DIR", "c", "body")                # cannot create the folder: swallowed


# ---- session builders, response classification, the exit ladder, the shared payload cache -------

class _Curl:
    def __init__(self, impersonate=None):
        self.impersonate, self.headers, self.proxies = impersonate, {}, None


def test_curl_session_and_thread_session(monkeypatch):
    import curl_cffi.requests
    monkeypatch.setattr(curl_cffi.requests, "Session", _Curl)
    order = []
    sess = sessions.curl_session("chrome", proxy_fn=lambda: order.append("proxy asked") or "http://p",
                                 accept_language=lambda proxy: f"lang({proxy})")
    assert (sess.impersonate, sess.proxies, sess.headers) == ("chrome", {"http": "http://p", "https": "http://p"},
                                                             {"accept-language": "lang(http://p)"})
    direct = sessions.curl_session("chrome", None)
    assert direct.proxies is None and direct.headers == {}
    assert sessions.speaking(direct, "en-AU").headers == {"accept-language": "en-AU"}
    local = threading.local()
    built = []
    first = sessions.thread_session(local, lambda: built.append(1) or _Curl())
    assert sessions.thread_session(local, lambda: built.append(1) or _Curl()) is first and built == [1] and local.session is first


def test_classify():
    from scraper_utils import responses

    class NotFound(Exception): pass
    class BadRequest(Exception): pass
    class Blocked(Exception): pass
    class Upstream(Exception): pass

    def read(status):
        resp = SimpleNamespace(status_code=status, text="x" * 500)
        return responses.classify(resp, "the page", NotFound, BadRequest, Blocked, Upstream)

    assert read(200) == "x" * 500
    for status, error, message in [(404, NotFound, "the page not found"), (400, BadRequest, "x" * 200),
                                   (401, Blocked, "HTTP 401 for the page"), (403, Blocked, "HTTP 403 for the page"),
                                   (429, Blocked, "HTTP 429 for the page"), (500, Upstream, "HTTP 500 for the page"),
                                   (204, Upstream, "HTTP 204 for the page")]:
        with pytest.raises(error) as caught:
            read(status)
        assert str(caught.value) == message


def test_current_step(monkeypatch):
    from scraper_utils import ladder
    monkeypatch.setattr(time, "time", lambda: 1000.0)
    local = threading.local()
    assert ladder.current_step(2, 2000.0, local, lambda step: True) == 2          # cooling down, flag unset
    assert ladder.current_step(2, 500.0, local, lambda step: True) == 0           # cooldown over
    local.past_gateway = True
    assert ladder.current_step(1, 2000.0, local, lambda step: step == 1) == 2     # last retry passes the gateway
    assert ladder.current_step(2, 2000.0, local, lambda step: step == 1) == 2     # not a gateway step: stays


def test_more_fan_out_shapes():
    with ThreadPoolExecutor(2) as pool:
        for run in (lambda fns: fanout.on_pool(pool, fns), lambda fns: fanout.on_pool_caller_first(pool, fns),
                    lambda fns: fanout.per_call_map(fns, 2)):
            assert run([]) == [] and run([lambda: 1]) == [1] and run([lambda: 1, lambda: 2, lambda: 3]) == [1, 2, 3]
            with pytest.raises(Boom):
                run([lambda: 1, _fail, lambda: 3])
        main = threading.current_thread().name
        names = fanout.on_pool_caller_first(pool, [lambda: threading.current_thread().name] * 2)
        assert names[0] == main and names[1] != main


def test_shared_raw_uses_the_response_cache_when_there_is_one(monkeypatch):
    import sys
    import types
    calls = []

    def memoized(key, build, ttl=None):
        calls.append(("memo", key, ttl))
        return build()

    monkeypatch.setitem(sys.modules, "cache", None)                              # no cache module: memo alone
    assert memo.shared_raw(memoized, "site.raw.", 600, "page", {"b": 2, "a": 1}, lambda: "built", 30) == "built"
    assert calls == [("memo", ("page", '{"a": 1, "b": 2}'), 30)]
    fake = types.ModuleType("cache")
    fake.cached_call = lambda ns, params, impl, ttl: calls.append(("cache", ns, params, ttl)) or impl()
    monkeypatch.setitem(sys.modules, "cache", fake)
    del calls[:]
    assert memo.shared_raw(memoized, "site.raw.", 600, "page", {"a": 1}, lambda: "built", 5000) == "built"
    assert calls == [("memo", ("page", '{"a": 1}'), 600), ("cache", "site.raw.page", {"a": 1}, 5000)]
