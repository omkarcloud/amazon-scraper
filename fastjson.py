"""JSON helpers: orjson when installed, the stdlib otherwise, with identical
results either way.

    import fastjson
    obj = fastjson.loads(text_or_bytes)
    text = fastjson.dumps(obj)          # non-ASCII kept as-is, compact

Why: the scrapers spend their JSON time on big blobs — API response bodies
(route_glue.json_response), cache payloads (cache.py, 158 KB at p99), page
bodies fetched through the browser boxes and the Next.js/Nuxt data blobs
embedded in HTML. orjson is 1.4-2x faster to parse and 3-4x faster to
serialise those (2026-11-05 bench). The free-threaded contact image has no
orjson (no cp314t wheel), so there it silently runs on the stdlib.

Guarantees: both calls fall back to the stdlib on the inputs orjson rejects
that the stdlib accepts (NaN/Infinity literals, lone surrogates, nesting
past orjson's depth limit, non-UTF-8 bytes, float subclasses), so callers
see the same values and the same exception types (orjson's errors subclass
json.JSONDecodeError / TypeError). str / bytes SUBCLASSES (bs4's
NavigableString from `script.string`, lxml text) are passed to orjson as
their base type — orjson refuses the subclass outright, which would make
every such call parse twice. Known divergence: an integer literal of 2**64
or more parses as a float under orjson (the stdlib keeps the exact int);
no site this parses carries numbers that large.

Not for cache KEYS (cache.canonical uses the stdlib: its bytes must never
change) and not for request bodies a site signs — those must keep the exact
serialisation their signature was built over.
"""
import json as _json

try:
    import orjson
except ImportError:  # the free-threaded contact image
    orjson = None

_DUMP_OPTS = 0 if orjson is None else orjson.OPT_NON_STR_KEYS


def loads(s):
    """Parse JSON text or bytes."""
    if orjson is None:
        return _json.loads(s)
    kind = type(s)
    if kind is not str and kind is not bytes:
        # orjson rejects subclasses ("Input must be bytes, bytearray,
        # memoryview, or str"): bs4's NavigableString is the common one
        if isinstance(s, str):
            s = str(s)
        elif isinstance(s, (bytes, bytearray, memoryview)):
            s = bytes(s)
    try:
        return orjson.loads(s)
    except _json.JSONDecodeError:
        # orjson is stricter than the stdlib (NaN, deep nesting, bad UTF-8);
        # the stdlib decides, and raises its own error if it is truly invalid
        return _json.loads(s)


def dumps(obj):
    """Compact JSON text, non-ASCII unescaped (= json.dumps(ensure_ascii=False)
    without the separator spaces)."""
    if orjson is None:
        return _json.dumps(obj, ensure_ascii=False)
    try:
        return orjson.dumps(obj, option=_DUMP_OPTS).decode("utf-8")
    except TypeError:
        # orjson refuses what the stdlib may still handle (ints beyond 64
        # bits, float subclasses); same TypeError if neither can
        return _json.dumps(obj, ensure_ascii=False)
