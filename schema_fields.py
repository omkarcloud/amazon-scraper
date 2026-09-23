"""Shared marshmallow request fields + schema helpers for scraper routes.

Site packages (alibaba/schemas.py, …) compose their route schemas from these
so every /<site>/* API validates the same way:

  * whitespace-stripped strings, empty == missing
  * booleans as true/false | 1/0 | yes/no flags
  * ONE param per input — a `RefField` subclass accepts a bare id OR a full
    site link and hands the raw string to a site-specific `resolver`
    (tripadvisor QueryOrIdField convention; never a sibling `url`/`id` pair)
  * choice params are case-insensitive and may map to upstream tokens
  * comma lists are trimmed/deduped, optionally validated + mapped
  * ISO codes (country / currency / language) are upper/lower-cased and
    validated against the real code lists
  * unknown query params are rejected (BaseSchema) so typos surface as 400s

Ported from rapidapi/utils/{fields,handlers}.py (the gateway's field
library) where the idea was worth keeping: the ISO-3166 country list,
comma-separated values with `allowed` + `value_map`, mapped choices,
`SiteSlugField` (bare slug or site URL -> slug), https-only URL with a
required domain, YYYY-MM / YYYY-MM-DD dates with today/future/past bounds,
limit/offset pagination fields.

`load_query(SchemaCls, query_dict)` -> (data, None) | (None, 400 body).
"""
import re
from datetime import date, datetime, timedelta
from urllib.parse import urlparse

from marshmallow import RAISE, Schema, ValidationError, fields, validate

__all__ = [
    "BaseSchema", "load_query",
    "StrippedString", "QueryField", "Flag", "TrueOnlyFlag", "RefField",
    "PageField", "PageSizeField", "LimitField", "OffsetField",
    "PositiveInt", "NonNegativeNumber", "Price", "Quantity",
    "ChoiceField", "CommaListField", "CurrencyField", "CountryCodeField",
    "LanguageCodeField", "UrlField", "SiteSlugField", "normalize_site_slug",
    "DateField", "YearMonthField", "COUNTRY_CODES", "LANGUAGE_CODES",
]

_TRUE = {"true", "1", "yes", "y", "on"}
_FALSE = {"false", "0", "no", "n", "off", ""}

# ISO 3166-1 alpha-2 (same list the rapidapi gateway validates against).
COUNTRY_CODES = frozenset("""
AF AX AL DZ AS AD AO AI AQ AG AR AM AW AU AT AZ BS BH BD BB BY BE BZ BJ BM BT BO BQ BA BW BV BR IO VG BN BG BF BI
CV KH CM CA KY CF TD CL CN CX CC CO KM CK CR HR CU CW CY CZ CD DK DJ DM DO EC EG SV GQ ER EE SZ ET FK FO FJ FI FR GF
PF TF GA GM GE DE GH GI GR GL GD GP GU GT GG GN GW GY HT HM HN HK HU IS IN ID IR IQ IE IM IL IT CI JM JP JE JO KZ KE
KI XK KW KG LA LV LB LS LR LY LI LT LU MO MG MW MY MV ML MT MH MQ MR MU YT MX FM MD MC MN ME MS MA MZ MM NA NR NP NL
AN NC NZ NI NE NG NU NF MP KP MK NO OM PK PW PS PA PG PY PE PH PN PL PT PR QA CG RE RO RU RW BL SH KN LC MF PM VC WS
SM ST SA SN RS CS SC SL SG SX SK SI SB SO ZA GS KR SS ES LK SD SR SJ SE CH SY TW TJ TZ TH TL TG TK TO TT TN TR TM TC
TV UG UA AE GB US UM UY VI UZ VU VA VE VN WF EH YE ZM ZW
""".split())

# ISO 639-1 two-letter language codes.
LANGUAGE_CODES = frozenset("""
aa ab ae af ak am an ar as av ay az ba be bg bh bi bm bn bo br bs ca ce ch co cr cs cu cv cy da de dv dz ee el en eo
es et eu fa ff fi fj fo fr fy ga gd gl gn gu gv ha he hi ho hr ht hu hy hz ia id ie ig ii ik io is it iu ja jv ka kg
ki kj kk kl km kn ko kr ks ku kv kw ky la lb lg li ln lo lt lu lv mg mh mi mk ml mn mr ms mt my na nb nd ne ng nl nn
no nr nv ny oc oj om or os pa pi pl ps pt qu rm rn ro ru rw sa sc sd se sg si sk sl sm sn so sq sr ss st su sv sw ta
te tg th ti tk tl tn to tr ts tt tw ty ug uk ur uz ve vi vo wa wo xh yi yo za zh zu
""".split())


# ---- schema + loading --------------------------------------------------------

class BaseSchema(Schema):
    """Unknown query params are rejected (a typo must not silently no-op)."""

    class Meta:
        unknown = RAISE


def load_query(schema_cls, query):
    """Validate a query dict. Returns (data, None) or (None, body) where body
    is {"error": "Invalid parameters: …", "errors": {field: [msgs]}} for a 400."""
    try:
        return schema_cls().load(query), None
    except ValidationError as e:
        messages = e.normalized_messages()
        flat = "; ".join(f"{k}: {' '.join(v) if isinstance(v, list) else v}" for k, v in messages.items())
        return None, {"error": f"Invalid parameters: {flat}", "errors": messages}


# ---- strings -----------------------------------------------------------------

class StrippedString(fields.String):
    """String with whitespace collapsed; empty -> error when required, else None."""

    def _deserialize(self, value, attr, data, **kwargs):
        value = super()._deserialize(value, attr, data, **kwargs)
        value = " ".join(value.split())
        if not value:
            if self.required:
                raise ValidationError("Must not be empty.")
            return None
        return value


class QueryField(StrippedString):
    """Free-text search term, required, 1-200 chars by default."""

    def __init__(self, max_length=200, **kwargs):
        kwargs.setdefault("required", True)
        kwargs.setdefault("validate", validate.Length(min=1, max=max_length))
        super().__init__(**kwargs)


class RefField(StrippedString):
    """ONE param that accepts a bare id OR a full site link. Subclasses set
    `resolver` (a function raising ValueError with a user-facing message) or
    pass `resolver=` at construction:

        class ProductRefField(RefField):
            resolver = staticmethod(refs.resolve_product_ref)
    """
    resolver = None

    def __init__(self, resolver=None, **kwargs):
        kwargs.setdefault("required", True)
        super().__init__(**kwargs)
        if resolver is not None:
            self._resolver = resolver
        else:
            self._resolver = type(self).resolver

    def _deserialize(self, value, attr, data, **kwargs):
        value = super()._deserialize(value, attr, data, **kwargs)
        if value is None:
            return None
        if self._resolver is None:
            return value
        try:
            return self._resolver(value)
        except ValueError as e:
            raise ValidationError(str(e))


# ---- booleans ----------------------------------------------------------------

class Flag(fields.Field):
    """Boolean query flag: true/false, 1/0, yes/no, on/off (case-insensitive).
    Defaults to None (= "not specified") unless load_default is given."""

    def __init__(self, **kwargs):
        kwargs.setdefault("load_default", None)
        super().__init__(**kwargs)

    def _deserialize(self, value, attr, data, **kwargs):
        if isinstance(value, bool):
            return value
        low = str(value).strip().lower()
        if low in _TRUE:
            return True
        if low in _FALSE:
            return False
        raise ValidationError("Must be true or false.")


class TrueOnlyFlag(Flag):
    """Opt-in switch (e.g. refresh=true): only true is accepted."""

    def _deserialize(self, value, attr, data, **kwargs):
        value = super()._deserialize(value, attr, data, **kwargs)
        if value is not True:
            raise ValidationError("Must be true, or not provided.")
        return value


# ---- numbers / pagination ----------------------------------------------------

class PageField(fields.Integer):
    """1-based page, default 1, capped at max_page."""

    def __init__(self, max_page=100, **kwargs):
        kwargs.setdefault("load_default", 1)
        kwargs.setdefault("validate", validate.Range(min=1, max=max_page))
        super().__init__(strict=False, **kwargs)


class PageSizeField(fields.Integer):
    def __init__(self, default=10, max_size=50, **kwargs):
        kwargs.setdefault("load_default", default)
        kwargs.setdefault("validate", validate.Range(min=1, max=max_size))
        super().__init__(strict=False, **kwargs)


class LimitField(PageSizeField):
    """Maximum number of results (default 10, max 50 unless overridden)."""


class OffsetField(fields.Integer):
    """Results to skip (default 0)."""

    def __init__(self, **kwargs):
        kwargs.setdefault("load_default", 0)
        kwargs.setdefault("validate", validate.Range(min=0))
        super().__init__(strict=False, **kwargs)


class PositiveInt(fields.Integer):
    """Optional integer >= 1 (counts, quantities, ids given as numbers)."""

    def __init__(self, max_value=None, **kwargs):
        kwargs.setdefault("load_default", None)
        kwargs.setdefault("validate", validate.Range(min=1, max=max_value))
        super().__init__(strict=False, **kwargs)


class NonNegativeNumber(fields.Float):
    """Optional float >= 0."""

    def __init__(self, **kwargs):
        kwargs.setdefault("load_default", None)
        kwargs.setdefault("validate", validate.Range(min=0))
        super().__init__(**kwargs)


class Price(NonNegativeNumber):
    """Price bound in the request's currency."""


class Quantity(PositiveInt):
    """Order/stock quantity."""


# ---- choices / lists ---------------------------------------------------------

class ChoiceField(StrippedString):
    """Case-insensitive choice. `choices` is a list of public values, OR a
    dict {public value: upstream token} in which case the mapped token is
    returned (rapidapi MappedField). Optional by default."""

    def __init__(self, choices, **kwargs):
        kwargs.setdefault("required", False)
        kwargs.setdefault("load_default", None)
        super().__init__(**kwargs)
        self.value_map = dict(choices) if isinstance(choices, dict) else None
        self.choices = [str(c).lower() for c in (choices.keys() if isinstance(choices, dict) else choices)]
        self._lookup = {c.lower(): c for c in (choices.keys() if isinstance(choices, dict) else choices)}

    def _deserialize(self, value, attr, data, **kwargs):
        value = super()._deserialize(value, attr, data, **kwargs)
        if value is None:
            return None
        key = self._lookup.get(value.lower())
        if key is None:
            raise ValidationError(f"Must be one of: {', '.join(self.choices)}.")
        return self.value_map[key] if self.value_map is not None else str(key).lower()


class CommaListField(fields.Field):
    """'a, b,A' -> ['A', 'B'] (trimmed, deduped; upper-cased by default).
    `allowed` restricts values (case-insensitive) and `value_map` maps each
    accepted value to an upstream token (rapidapi CommaSeparatedField)."""

    def __init__(self, allowed=None, value_map=None, upper=True, max_items=10, **kwargs):
        kwargs.setdefault("load_default", None)
        super().__init__(**kwargs)
        self.upper = upper
        self.max_items = max_items
        source = list(value_map.keys()) if value_map else list(allowed or [])
        self._allowed = {str(a).lower(): a for a in source} if source else None
        self.value_map = value_map

    def _deserialize(self, value, attr, data, **kwargs):
        items = []
        for raw in str(value).split(","):
            item = raw.strip()
            if not item:
                continue
            if self._allowed is not None:
                canonical = self._allowed.get(item.lower())
                if canonical is None:
                    raise ValidationError(f"'{item}' is not one of: {', '.join(str(a) for a in self._allowed.values())}.")
                item = self.value_map[canonical] if self.value_map else canonical
            elif self.upper:
                item = item.upper()
            if item not in items:
                items.append(item)
        if not items:
            return None
        if len(items) > self.max_items:
            raise ValidationError(f"At most {self.max_items} values.")
        return items


# ---- ISO codes ---------------------------------------------------------------

class CurrencyField(StrippedString):
    """3-letter ISO 4217 code, upper-cased (USD, EUR, INR, …). Optional."""

    def __init__(self, **kwargs):
        kwargs.setdefault("required", False)
        kwargs.setdefault("load_default", None)
        super().__init__(**kwargs)

    def _deserialize(self, value, attr, data, **kwargs):
        value = super()._deserialize(value, attr, data, **kwargs)
        if value is None:
            return None
        value = value.upper()
        if not (len(value) == 3 and value.isalpha()):
            raise ValidationError("Must be a 3-letter ISO currency code (USD, EUR, INR, ...).")
        return value


class CountryCodeField(StrippedString):
    """2-letter ISO 3166-1 code, upper-cased and checked against the real
    list (CN, US, IN, …). Optional by default."""

    def __init__(self, **kwargs):
        kwargs.setdefault("required", False)
        kwargs.setdefault("load_default", None)
        super().__init__(**kwargs)

    def _deserialize(self, value, attr, data, **kwargs):
        value = super()._deserialize(value, attr, data, **kwargs)
        if value is None:
            return None
        value = value.upper()
        if value not in COUNTRY_CODES:
            raise ValidationError("Must be a 2-letter ISO country code (CN, US, IN, ...).")
        return value


class LanguageCodeField(StrippedString):
    """ISO 639-1 code ("en"), or a locale tag ("en-US" -> kept as "en-US")
    when allow_locale=True. Lower-cases the language part. Optional."""

    def __init__(self, allow_locale=True, **kwargs):
        kwargs.setdefault("required", False)
        kwargs.setdefault("load_default", None)
        super().__init__(**kwargs)
        self.allow_locale = allow_locale

    def _deserialize(self, value, attr, data, **kwargs):
        value = super()._deserialize(value, attr, data, **kwargs)
        if value is None:
            return None
        lang, sep, region = value.replace("_", "-").partition("-")
        lang = lang.lower()
        if lang not in LANGUAGE_CODES or (region and not self.allow_locale):
            raise ValidationError("Must be an ISO 639-1 language code (en, fr, de, ...).")
        if region:
            if not (len(region) == 2 and region.isalpha()):
                raise ValidationError("Locale must look like en-US.")
            return f"{lang}-{region.upper()}"
        return lang


# ---- URLs / slugs --------------------------------------------------------------

class UrlField(fields.Url):
    """http(s) URL; `domain` requires the host to be that registrable domain
    or a subdomain of it (e.g. domain="alibaba.com")."""

    def __init__(self, domain=None, schemes=("http", "https"), **kwargs):
        kwargs.setdefault("required", False)
        kwargs.setdefault("load_default", None)
        super().__init__(schemes=set(schemes), **kwargs)
        self.domain = domain.lower() if domain else None

    def _deserialize(self, value, attr, data, **kwargs):
        value = super()._deserialize(str(value).strip(), attr, data, **kwargs)
        if self.domain:
            host = (urlparse(value).hostname or "").lower()
            if not (host == self.domain or host.endswith("." + self.domain)):
                raise ValidationError(f"Must be a {self.domain} link.")
        return value


def normalize_site_slug(value, site_domain, marker):
    """Bare slug ('acme') or a full URL of `site_domain` on any TLD
    ('https://uk.trustpilot.com/review/acme?x') -> 'acme' (the path segment
    after `marker`). Raises ValueError for another site's URL or an empty
    slug. (Port of rapidapi/utils/fields.normalize_site_slug.)"""
    value = (value or "").strip()
    if value.startswith(("http://", "https://", "//")):
        url = value if not value.startswith("//") else "https:" + value
        host = (urlparse(url).hostname or "").lower()
        labels = host.split(".")
        if site_domain.lower() not in labels:
            raise ValueError(f"not a {site_domain} URL")
        if marker and marker in url:
            value = url.split(marker, 1)[1]
        else:
            value = urlparse(url).path.rstrip("/").rsplit("/", 1)[-1]
    value = value.split("?")[0].split("#")[0].split("/")[0].strip()
    if not value:
        raise ValueError("empty slug")
    return value


class SiteSlugField(StrippedString):
    """Bare slug OR a full URL of one site -> the slug (or the canonical URL
    when as_url=True). site_domain = registrable name without TLD
    ('trustpilot'); marker = the path segment before the slug ('/review/')."""

    def __init__(self, site_domain, marker, as_url=False, canonical_host=None, **kwargs):
        kwargs.setdefault("required", True)
        super().__init__(**kwargs)
        self.site_domain = site_domain
        self.marker = marker
        self.as_url = as_url
        self.canonical_host = canonical_host or f"www.{site_domain}.com"

    def _deserialize(self, value, attr, data, **kwargs):
        value = super()._deserialize(value, attr, data, **kwargs)
        if value is None:
            return None
        try:
            slug = normalize_site_slug(value, self.site_domain, self.marker)
        except ValueError:
            raise ValidationError(f"Must be a valid {self.site_domain} link or slug.")
        return f"https://{self.canonical_host}{self.marker}{slug}" if self.as_url else slug


# ---- dates -------------------------------------------------------------------

class DateField(fields.String):
    """YYYY-MM-DD -> 'YYYY-MM-DD' string. bound='future' (today or later),
    'past' (today or earlier) or None; default_days_ahead=N makes a missing
    value default to today+N (e.g. 1 = tomorrow)."""

    def __init__(self, bound=None, default_days_ahead=None, **kwargs):
        if default_days_ahead is not None:
            kwargs.setdefault("load_default", lambda: (date.today() + timedelta(days=default_days_ahead)).isoformat())
        else:
            kwargs.setdefault("load_default", None)
        kwargs.setdefault("required", False)
        super().__init__(**kwargs)
        self.bound = bound

    def _deserialize(self, value, attr, data, **kwargs):
        value = super()._deserialize(value, attr, data, **kwargs).strip()
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
            raise ValidationError("Must be a date as YYYY-MM-DD (e.g. 2026-10-15).")
        try:
            parsed = datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError as e:
            raise ValidationError(f"Invalid date: {e}.")
        today = date.today()
        if self.bound == "future" and parsed < today:
            raise ValidationError("Must be today or a future date.")
        if self.bound == "past" and parsed > today:
            raise ValidationError("Must be today or a past date.")
        return parsed.isoformat()


class YearMonthField(fields.String):
    """YYYY-MM -> 'YYYY-MM' string; bound='future' = current month or later."""

    def __init__(self, bound=None, **kwargs):
        kwargs.setdefault("required", False)
        kwargs.setdefault("load_default", None)
        super().__init__(**kwargs)
        self.bound = bound

    def _deserialize(self, value, attr, data, **kwargs):
        value = super()._deserialize(value, attr, data, **kwargs).strip()
        if not re.fullmatch(r"\d{4}-\d{2}", value):
            raise ValidationError("Must be a month as YYYY-MM (e.g. 2026-10).")
        try:
            parsed = datetime.strptime(value, "%Y-%m").date()
        except ValueError as e:
            raise ValidationError(f"Invalid month: {e}.")
        if self.bound == "future" and parsed < date.today().replace(day=1):
            raise ValidationError("Must be the current month or later.")
        if self.bound == "past" and parsed > date.today().replace(day=1):
            raise ValidationError("Must be the current month or earlier.")
        return value
