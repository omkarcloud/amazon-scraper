"""The network helpers the amazon module needs. Requests go out from your own
connection, or through the proxy config.py names; there is no exit policy
here, so every session is a plain curl_cffi session."""

ACCEPT_LANGUAGE = "en-US,en;q=0.9"


def site_accept_language(site=None):
    return ACCEPT_LANGUAGE


def accept_language(proxy=None, country=None):
    return ACCEPT_LANGUAGE


def gateway_country(exit=None):
    return None


def exit_country(proxy=None):
    return None


def exit_proxy(exit=None):
    return exit if exit and "://" in str(exit) else None


def new_session(exit=None, impersonate="chrome", **kw):
    from curl_cffi import requests as curl
    sess = curl.Session(impersonate=impersonate, **kw)
    proxy = exit_proxy(exit)
    if proxy:
        sess.proxies = {"http": proxy, "https": proxy}
    return sess


def exit_session(exit=None, impersonate="chrome", **kw):
    return new_session(exit, impersonate, **kw)
