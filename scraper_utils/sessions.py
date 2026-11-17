"""Per-thread HTTP sessions: where a site keeps them and how it drops them.

The store itself stays in the site module (`_local = threading.local()` in its
fetch.py) and is passed in on every call, so the site's tests can still read,
replace and reset it."""
from collections import OrderedDict


def close_quietly(sess):
    """Close a session; a session that fails to close is simply let go."""
    if sess is not None:
        try:
            sess.close()
        except Exception:
            pass


def drop(local):
    """Close the thread's session so a poisoned connection (or exit) dies."""
    sess = getattr(local, "session", None)
    local.session = None
    close_quietly(sess)


def thread_store(local):
    """The thread's {key: session} store, created on first use."""
    store = getattr(local, "sessions", None)
    if store is None:
        store = local.sessions = OrderedDict()
    return store


def drop_keyed(store, key):
    """Close and forget the session kept under `key`."""
    close_quietly(store.pop(key, None))


def keyed(store, key, build, cap):
    """The session kept under `key`, built on first use. The store holds the
    `cap` most recently used sessions; older ones are closed."""
    sess = store.get(key)
    if sess is None:
        sess = build()
        store[key] = sess
        while len(store) > cap:
            _, old = store.popitem(last=False)
            close_quietly(old)
    else:
        store.move_to_end(key)
    return sess


def curl_session(impersonate, proxy=None, proxy_fn=None, accept_language=None):
    """A new curl_cffi session. `proxy` is a URL the caller already has;
    `proxy_fn` is asked for one after the session exists (the order some sites
    count on). `accept_language(proxy)` names the language the session sends."""
    from curl_cffi import requests as curl_requests
    sess = curl_requests.Session(impersonate=impersonate)
    if proxy_fn is not None:
        proxy = proxy_fn()
    if proxy:
        sess.proxies = {"http": proxy, "https": proxy}
    if accept_language is not None:
        sess.headers["accept-language"] = accept_language(proxy)
    return sess


def speaking(sess, language):
    """The session, announcing `language` as its Accept-Language."""
    sess.headers["accept-language"] = language
    return sess


def thread_session(local, build):
    """The thread's session, built on first use."""
    sess = getattr(local, "session", None)
    if sess is None:
        sess = build()
        local.session = sess
    return sess
