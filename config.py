"""Configuration for the Amazon Scraper. Everything can be set with an
environment variable; the defaults work out of the box.

    PORT          port the API listens on (default 8000)
    AMAZON_PROXY  proxy URL for every request, e.g. http://user:pass@host:port
                  (default: none — direct). The US, UK, German, French,
                  Indian, Canadian … storefronts answer direct requests, so you
                  very likely don't need this. Two exceptions: amazon.co.jp and
                  amazon.com.au only serve visitors from their own country —
                  for country=JP / country=AU set a proxy that exits in Japan /
                  Australia. A residential proxy also helps if you run very
                  high volumes from one IP.

Everything else below is a plain constant with a working default — edit it
here if you need to.
"""
import os

PORT = int(os.environ.get("PORT", "8000"))

# Retry policy for transport errors and blocks (every request).
MAX_RETRIES = 3
RETRY_BACKOFF = 2          # seconds, multiplied by the attempt number

AMAZON_PROXY = os.environ.get("AMAZON_PROXY") or None

# With a proxy set, every session goes through it (the package asks for a
# per-country exit; one proxy serves them all). Without it: direct.
AMAZON_PROXY_COUNTRY = "any" if AMAZON_PROXY else None
AMAZON_DIRECT_COOLDOWN = 300   # seconds a blocked marketplace stays on the proxy


def amazon_country_proxy(country):
    """The proxy for a session (None = direct)."""
    return AMAZON_PROXY


def amazon_fallback_proxy(country):
    """The proxy a marketplace moves to after repeated blocks (None = none)."""
    return AMAZON_PROXY
