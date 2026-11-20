"""Reading a fetched page: its title, and whether it is a challenge page."""
import re

_TITLE_RE = re.compile(r"<title>([^<]*)</title>", re.I)

# Text that only a Cloudflare challenge page carries (matched in lower case).
CF_MARKERS = ("just a moment", "challenge-error-text", "cf-chl", "challenge-platform", "attention required")


def page_title(text):
    """The page's <title>, trimmed; '' when there is none."""
    match = _TITLE_RE.search(text or "")
    return match.group(1).strip() if match else ""


def is_challenged(status, headers, text):
    """True for a Cloudflare challenge: the cf-mitigated header, or a 403 / 429
    / 503 whose body carries a challenge marker."""
    if (headers or {}).get("cf-mitigated") == "challenge":
        return True
    head = (text or "")[:4000].lower()
    return status in (403, 429, 503) and any(m in head for m in CF_MARKERS)
