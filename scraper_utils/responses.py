"""Reading an HTTP response into the scraper error taxonomy."""


def classify(resp, label, not_found, bad_request, blocked, upstream):
    """The body text of a 200. Otherwise raises the site's own error: 404 ->
    not_found, 400 -> bad_request (with the start of the body), 401 / 403 /
    429 -> blocked, anything else -> upstream."""
    if resp.status_code == 404:
        raise not_found(f"{label} not found")
    if resp.status_code == 400:
        raise bad_request(resp.text[:200])
    if resp.status_code in (401, 403, 429):
        raise blocked(f"HTTP {resp.status_code} for {label}")
    if resp.status_code != 200:
        raise upstream(f"HTTP {resp.status_code} for {label}")
    return resp.text
