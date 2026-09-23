"""Shared failure taxonomy for every scraper package.

Each scraper's transport (`<site>/fetch.py`) raises the same four kinds of
error; subclassing these bases lets ONE route layer (route_glue.py) map
them to HTTP for every site:

    UpstreamError   transport failure / 5xx            -> 502, retryable
    Blocked         anti-bot challenge / 403 / 429     -> 502, retryable on a new exit/identity
    BadRequest      upstream rejected the params        -> 400, never retried
    NotFound        entity / page does not exist        -> 404, never retried

A site keeps its own names for readability and for `except` clauses inside
its package:

    class AlibabaUpstreamError(UpstreamError): ...
    class AlibabaBlocked(AlibabaUpstreamError, Blocked): ...
    class AlibabaBadRequest(BadRequest): ...
    class AlibabaNotFound(NotFound): ...
"""


class UpstreamError(Exception):
    """Transport failure or 5xx — retryable."""


class Blocked(UpstreamError):
    """Anti-bot challenge / 403 / 429 — retryable after a fresh exit or identity."""


class BadRequest(Exception):
    """Upstream rejected the params — never retried."""


class NotFound(Exception):
    """Entity / page does not exist — never retried."""
