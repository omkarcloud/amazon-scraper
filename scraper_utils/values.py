"""Value helpers for parsers: safe nested access, numbers and booleans. Pure
functions, no network; every helper tolerates None and junk.

Numbers come in a few variants, because sites differ on purpose: some round a
float where others truncate, some read the number out of a longer text. Pick
the variant by what the site's data needs; all of them return None for None,
booleans, junk, NaN and infinity."""
import math
import re

# ---- safe access ---------------------------------------------------------------------------

def g(obj, *path, default=None):
    """Safe nested get: g(d, "a", "b", 0) -> d["a"]["b"][0] or default."""
    for step in path:
        if isinstance(step, int):
            if not isinstance(obj, list) or not -len(obj) <= step < len(obj):
                return default
            obj = obj[step]
        else:
            if not isinstance(obj, dict):
                return default
            obj = obj.get(step)
        if obj is None:
            return default
    return obj


def as_list(value):
    """The value when it is a list, else []."""
    return value if isinstance(value, list) else []


def as_dict(value):
    """The value when it is a dict, else {}."""
    return value if isinstance(value, dict) else {}


def bool_or_none(value):
    """A real bool as it is; anything else (0, 1, "true") -> None."""
    return value if isinstance(value, bool) else None


# ---- numbers -------------------------------------------------------------------------------

_NUMBER = re.compile(r"-?\d+(?:\.\d+)?")


def _is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


def num(value):
    """int / float / numeric string ('1,234.5') -> float; bool, junk, NaN and
    infinity -> None."""
    if value is None or isinstance(value, bool):
        return None
    if not isinstance(value, (int, float)):
        value = str(value).strip().replace(",", "")
    try:
        out = float(value)
    except (ValueError, OverflowError):
        return None
    return out if math.isfinite(out) else None


to_float = num


def number(value):
    """Like num(), but a whole number comes back as an int (384.0 -> 384)."""
    if _is_int(value):
        return value
    out = num(value)
    if out is None:
        return None
    return int(out) if out.is_integer() else out


def to_int(value):
    """A whole number; a float or float text is truncated ('3.7' -> 3)."""
    if _is_int(value):
        return value
    out = num(value)
    return int(out) if out is not None else None


def to_int_round(value):
    """A whole number; a float or float text is rounded ('3.7' -> 4)."""
    if _is_int(value):
        return value
    out = num(value)
    return int(round(out)) if out is not None else None


def first_float(value):
    """The first number inside a text ('$1,299.99 each' -> 1299.99); a number
    passes through as a float."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return num(value)
    match = _NUMBER.search(str(value).replace(",", ""))
    return num(match.group(0)) if match else None


def first_int(value):
    """The first number inside a text, truncated ('12 reviews' -> 12)."""
    if _is_int(value):
        return value
    out = first_float(value)
    return int(out) if out is not None else None


# ---- numbers with a meaning ----------------------------------------------------------------

def rounded(value, digits=2):
    """num() rounded to `digits` decimals; None when it is not a number."""
    out = num(value)
    return round(out, digits) if out is not None else None


def amount(value):
    """A money amount: whole numbers as int, others rounded to cents."""
    out = num(value)
    if out is None:
        return None
    return int(out) if float(out).is_integer() else round(out, 2)


def rating(value):
    """A 0-5 star rating rounded to 2 places; 0 / missing -> None."""
    out = num(value)
    return round(out, 2) if out else None


def count(value):
    """A non-negative count; negatives -> 0."""
    out = to_int_round(value)
    if out is None:
        return None
    return out if out >= 0 else 0


def year(value):
    """A founding year: 4-digit int, else None (the site stores 0 for unknown)."""
    out = to_int_round(value)
    return out if out and 1000 <= out <= 2100 else None
