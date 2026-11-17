"""Text helpers for parsers: trimming, whitespace, and HTML to plain text.
Pure functions, no network; every helper returns None for None and for
anything that ends up empty.

Several ways to clean a short string, because sites want different things:

    stripped()   strings only, edges trimmed
    trimmed()    the same, numbers accepted too
    squashed()   strings only, every run of whitespace becomes one space
    squashed_any()  the same for any value, through str()
    clean()      squashed_any() plus HTML entities unescaped
    plain()      clean() with tags removed first"""
import html as html_lib
import re
from html.parser import HTMLParser

from scraper_utils.values import as_list

# ---- short strings -------------------------------------------------------------------------

def stripped(value):
    """A string with its edges trimmed; None for '' and for non-strings."""
    if not isinstance(value, str):
        return None
    value = value.strip()
    return value or None


def trimmed(value):
    """Whitespace-trimmed string, or None for empty / non-strings."""
    if value is None or isinstance(value, (dict, list, bool)):
        return None
    text = str(value).strip()
    return text or None


def squashed(value):
    """A string with every run of whitespace collapsed; None for non-strings."""
    if not isinstance(value, str):
        return None
    value = " ".join(value.split())
    return value or None


def squashed_any(value):
    """Any value as a string with every run of whitespace collapsed."""
    if value is None:
        return None
    value = " ".join(str(value).split())
    return value or None


def clean(value):
    """Whitespace-collapsed, HTML-unescaped string; '' -> None. Accepts
    {"text": ...} wrappers."""
    if isinstance(value, dict):
        value = value.get("text")
    if value is None:
        return None
    out = " ".join(html_lib.unescape(str(value)).replace("\xa0", " ").split())
    return out or None


def multiline(value):
    """Like clean() but keeps line breaks (review bodies, descriptions)."""
    if isinstance(value, dict):
        value = value.get("text")
    if value is None:
        return None
    lines = [" ".join(line.split()) for line in html_lib.unescape(str(value)).replace("\xa0", " ").splitlines()]
    out = "\n".join(lines)
    out = re.sub(r"\n{3,}", "\n\n", out).strip()
    return out or None


def lower_snake(value):
    """'Work/Life balance' / 'Benefits & perks' -> 'work_life_balance' / 'benefits_perks'."""
    value = clean(value)
    if not value:
        return None
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_") or None


def text_list(values):
    """A list of strings, trimmed, deduped, empties dropped."""
    out = []
    for value in as_list(values):
        item = clean(value)
        if item and item not in out:
            out.append(item)
    return out


# ---- HTML ----------------------------------------------------------------------------------

_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")


def plain(value):
    """Strip tags, unescape entities and collapse whitespace; '' -> None."""
    if value is None:
        return None
    text = _WS_RE.sub(" ", html_lib.unescape(_TAG_RE.sub("", str(value)))).strip()
    return text or None


_BLOCK_TAGS = {"p", "div", "br", "li", "ul", "ol", "h1", "h2", "h3", "h4", "h5", "h6", "tr", "table",
               "section", "article", "header", "footer", "blockquote", "pre", "hr", "dd", "dt", "dl"}
_SKIP_TAGS = {"script", "style", "noscript", "template", "head"}


class _TextExtractor(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts = []
        self._skip = 0

    def handle_starttag(self, tag, attrs):
        if tag in _SKIP_TAGS:
            self._skip += 1
        elif tag == "li":
            self.parts.append("\n- ")
        elif tag == "br":
            self.parts.append("\n")
        elif tag in _BLOCK_TAGS:
            self.parts.append("\n")

    def handle_endtag(self, tag):
        if tag in _SKIP_TAGS:
            self._skip = max(0, self._skip - 1)
        elif tag in ("li", "br"):
            return
        elif tag in ("ul", "ol"):
            self.parts.append("\n\n")      # a list ends its paragraph
        elif tag in _BLOCK_TAGS:
            self.parts.append("\n")

    def handle_data(self, data):
        if not self._skip:
            self.parts.append(data)


def html_to_text(value):
    """An HTML fragment -> readable plain text: block elements become line
    breaks, list items get a dash, runs of blank lines collapse."""
    value = value if isinstance(value, str) else None
    if not value:
        return None
    if "<" not in value:
        return multiline(value)
    parser = _TextExtractor()
    try:
        parser.feed(value)
        parser.close()
    except Exception:
        return multiline(re.sub(r"<[^>]+>", " ", value))
    raw = "".join(parser.parts).replace("\xa0", " ")
    lines = [" ".join(line.split()) for line in raw.splitlines()]
    out = "\n".join(lines)
    out = re.sub(r"\n{3,}", "\n\n", out)
    out = re.sub(r"\n\n(- )", r"\n\1", out).strip()   # list items stay tight
    return out or None
