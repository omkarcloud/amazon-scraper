"""Amazon marketplaces served by /amazon/*: one row per ISO country code.

    domain          the storefront host (www.<domain>)
    marketplace_id  Amazon's obfuscated marketplace id (autocomplete `mid`,
                    the data.amazon.* product API path, seller feedback ajax)
    language        the storefront's default locale (the `lc-<suffix>` cookie,
                    see locale_cookie(); Accept-Language follows it)
    currency        ISO code the storefront prices in
    fingerprint     curl_cffi impersonation that passes the product-page
                    bot check from a non-local exit (validated 2026-09-22:
                    chrome works for .com/.ca WITH a search referer; every
                    other storefront answers a captcha to chrome and passes
                    safari17_0)
    postal_code     a real postal code in the marketplace's own country; set
                    once per session through the "Deliver to" (glow) endpoint
                    so the buybox / offers / delivery blocks render as for a
                    local shopper even from a foreign exit
    exit_country    None = direct egress works; an ISO code = the storefront
                    rejects foreign exits, so its sessions run through a
                    residential exit pinned to that country
    data_host       the AAPI host (data.amazon.<tld>) the deals grid hydrates
                    prices from

amazon.cn is not listed: the Chinese marketplace closed to third-party
sellers in 2023 and only serves Kindle / global-store landing pages.
"""

SITES = {
    "US": {"domain": "amazon.com", "marketplace_id": "ATVPDKIKX0DER", "language": "en_US", "currency": "USD",
           "fingerprint": "chrome", "postal_code": "10001", "exit_country": None},
    "CA": {"domain": "amazon.ca", "marketplace_id": "A2EUQ1WTGCTBG2", "language": "en_CA", "currency": "CAD",
           "fingerprint": "chrome", "postal_code": "M5V 3L9", "exit_country": None},
    "MX": {"domain": "amazon.com.mx", "marketplace_id": "A1AM78C64UM0Y8", "language": "es_MX", "currency": "MXN",
           "fingerprint": "safari17_0", "postal_code": "01000", "exit_country": None},
    "BR": {"domain": "amazon.com.br", "marketplace_id": "A2Q3Y263D00KWC", "language": "pt_BR", "currency": "BRL",
           "fingerprint": "safari17_0", "postal_code": "01310-100", "exit_country": None},
    "GB": {"domain": "amazon.co.uk", "marketplace_id": "A1F83G8C2ARO7P", "language": "en_GB", "currency": "GBP",
           "fingerprint": "safari17_0", "postal_code": "SW1A 1AA", "exit_country": None},
    "IE": {"domain": "amazon.ie", "marketplace_id": "A28R8C7NBKEWEA", "language": "en_IE", "currency": "EUR",
           "fingerprint": "safari17_0", "postal_code": "D02 X285", "exit_country": None},
    "DE": {"domain": "amazon.de", "marketplace_id": "A1PA6795UKMFR9", "language": "de_DE", "currency": "EUR",
           "fingerprint": "safari17_0", "postal_code": "10115", "exit_country": None},
    "FR": {"domain": "amazon.fr", "marketplace_id": "A13V1IB3VIYZZH", "language": "fr_FR", "currency": "EUR",
           "fingerprint": "safari17_0", "postal_code": "75001", "exit_country": None},
    "IT": {"domain": "amazon.it", "marketplace_id": "APJ6JRA9NG5V4", "language": "it_IT", "currency": "EUR",
           "fingerprint": "safari17_0", "postal_code": "00100", "exit_country": None},
    "ES": {"domain": "amazon.es", "marketplace_id": "A1RKKUPIHCS9HS", "language": "es_ES", "currency": "EUR",
           "fingerprint": "safari17_0", "postal_code": "28001", "exit_country": None},
    "NL": {"domain": "amazon.nl", "marketplace_id": "A1805IZSGTT6HS", "language": "nl_NL", "currency": "EUR",
           "fingerprint": "safari17_0", "postal_code": "1012 AB", "exit_country": None},
    "BE": {"domain": "amazon.com.be", "marketplace_id": "AMEN7PMS3EDWL", "language": "fr_BE", "currency": "EUR",
           "fingerprint": "safari17_0", "postal_code": "1000", "exit_country": None},
    "SE": {"domain": "amazon.se", "marketplace_id": "A2NODRKZP88ZB9", "language": "sv_SE", "currency": "SEK",
           "fingerprint": "safari17_0", "postal_code": "111 20", "exit_country": None},
    "PL": {"domain": "amazon.pl", "marketplace_id": "A1C3SOZRARQ6R3", "language": "pl_PL", "currency": "PLN",
           "fingerprint": "safari17_0", "postal_code": "00-001", "exit_country": None},
    "TR": {"domain": "amazon.com.tr", "marketplace_id": "A33AVAJ2PDY3EV", "language": "tr_TR", "currency": "TRY",
           "fingerprint": "safari17_0", "postal_code": "34000", "exit_country": None},
    "AE": {"domain": "amazon.ae", "marketplace_id": "A2VIGQ35RCS4UG", "language": "en_AE", "currency": "AED",
           "fingerprint": "safari17_0", "postal_code": None, "exit_country": None},
    "SA": {"domain": "amazon.sa", "marketplace_id": "A17E79C6D8DWNP", "language": "en_AE", "currency": "SAR",
           "fingerprint": "safari17_0", "postal_code": None, "exit_country": None},
    "EG": {"domain": "amazon.eg", "marketplace_id": "ARBP9OOSHTCHU", "language": "en_AE", "currency": "EGP",
           "fingerprint": "safari17_0", "postal_code": None, "exit_country": None},
    "IN": {"domain": "amazon.in", "marketplace_id": "A21TJRUUN4KGV", "language": "en_IN", "currency": "INR",
           "fingerprint": "safari17_0", "postal_code": "110001", "exit_country": None},
    "JP": {"domain": "amazon.co.jp", "marketplace_id": "A1VC38T7YXB528", "language": "ja_JP", "currency": "JPY",
           "fingerprint": "safari17_0", "postal_code": "100-0001", "exit_country": "jp"},
    "SG": {"domain": "amazon.sg", "marketplace_id": "A19VAU5U5O7RUS", "language": "en_SG", "currency": "SGD",
           "fingerprint": "safari17_0", "postal_code": "018956", "exit_country": None},
    "AU": {"domain": "amazon.com.au", "marketplace_id": "A39IBJ37TRP1C6", "language": "en_AU", "currency": "AUD",
           "fingerprint": "safari17_0", "postal_code": "2000", "exit_country": "au"},
    "ZA": {"domain": "amazon.co.za", "marketplace_id": "AE08WJ6YKNBMC", "language": "en_ZA", "currency": "ZAR",
           "fingerprint": "safari17_0", "postal_code": "2000", "exit_country": None},
}

DEFAULT_COUNTRY = "US"
COUNTRIES = tuple(SITES)

# amazon.co.uk -> GB etc. (every host a link can carry, including the bare
# apex and the smile./m. subdomains)
_DOMAIN_TO_COUNTRY = {row["domain"]: code for code, row in SITES.items()}

# The completion service runs one backend per Amazon region and each only
# answers its own marketplace ids (a foreign `mid` gets 200 + empty list).
# completion.<tld> is a CloudFront alias of these; when it breaks
# (completion.amazon.in 502s since 2026-09) the regional host still serves.
_COMPLETION_REGION_HOST = dict(
    {code: "completion.amazon.com" for code in ("US", "CA", "MX", "BR")},
    **{code: "completion.amazon.co.jp" for code in ("JP", "SG", "AU")})


def locale_cookie(site_row):
    """Name of the cookie that holds the storefront language. Each
    marketplace has its own suffix (seen on its session cookies:
    ubid-acbde, sst-acbde, lc-acbde on amazon.de); only amazon.com uses
    `lc-main`, and the UK is `acbuk` rather than its ISO code. A cookie
    under any other name is ignored and the store picks the language from
    Accept-Language instead (verified for all 23 marketplaces 2026-10-16)."""
    code = site_row["country"]
    if code == "US":
        return "lc-main"
    return "lc-acb" + ("uk" if code == "GB" else code.lower())


def accept_language(locale):
    """'de_DE' -> 'de-DE,de;q=0.9,en;q=0.5' (the storefront honours it when
    the locale cookie is missing or reset, e.g. after a captcha)."""
    lang, _, region = (locale or "en_US").partition("_")
    tag = f"{lang}-{region}" if region else lang
    if lang == "en":
        return f"{tag},en;q=0.9"
    return f"{tag},{lang};q=0.9,en;q=0.5"


def site(country=None):
    """The marketplace row for an ISO country code (US when None)."""
    code = (country or DEFAULT_COUNTRY).upper()
    if code == "UK":
        code = "GB"
    row = SITES.get(code)
    if row is None:
        raise ValueError(f"country must be one of: {', '.join(COUNTRIES)}")
    return dict(row, country=code, host=f"www.{row['domain']}", base=f"https://www.{row['domain']}",
                data_host=f"data.{row['domain']}", completion_host=f"completion.{row['domain']}",
                completion_region_host=_COMPLETION_REGION_HOST.get(code, "completion.amazon.co.uk"))


def country_for_host(host):
    """ISO country of an Amazon hostname (any subdomain), None for other hosts."""
    host = (host or "").lower().split(":")[0]
    labels = host.split(".")
    for i in range(len(labels)):
        candidate = ".".join(labels[i:])
        code = _DOMAIN_TO_COUNTRY.get(candidate)
        if code:
            return code
    return None
