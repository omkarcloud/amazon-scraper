"""The caller's picture for a search by image: downloading the `image` link
safely, then re-encoding it the way the site's own page sends a photo.

Downloading:
  * http(s) links only. Every hop is resolved first and refused when it
    points into a private, loopback, link-local or reserved network (the
    download runs on the API's server, next to internal services), so
    redirects are followed by hand, each hop checked;
  * an image Accept header without AVIF: CDNs that negotiate (alicdn,
    Nike's) answer AVIF when it is allowed, and Pillow cannot decode it;
  * browser-impersonated TLS first, then a plain client with the caller's
    `user_agent`: upload.wikimedia.org answers 403 to a browser TLS and
    serves a named client (google_lens/images.py, 2026-09-26);
  * a size cap, read in chunks, so a huge file is refused before it is in
    memory.

Re-encoding (Pillow, imported in the functions that need it): the EXIF
orientation applied (a browser shows a phone photo upright, Pillow keeps the
sensor frame unless told), transparency flattened onto white, resized, JPEG.

Errors are PictureError (a ValueError) with a message for the API caller; a
site turns it into its own BadRequest."""
import ipaddress
import re
import socket
from urllib.parse import urljoin, urlparse

# No AVIF: a CDN that negotiates answers it when allowed, and Pillow cannot decode it.
IMAGE_ACCEPT = "image/webp,image/png,image/jpeg,image/gif,image/*;q=0.8,*/*;q=0.5"
# What a browser sends for an <img> on another site (the first download try).
BROWSER_HEADERS = {"accept": IMAGE_ACCEPT, "sec-fetch-dest": "image", "sec-fetch-mode": "no-cors",
                   "sec-fetch-site": "cross-site"}
# Redirect hops followed (each one host-checked).
MAX_REDIRECTS = 5
# Largest picture downloaded (google_lens's cap).
MAX_BYTES = 20 * 1024 * 1024
# Seconds per download hop.
TIMEOUT = 25
# Smallest side in px: a tracking pixel is not a picture.
MIN_SIDE = 10
# Longest side a JPEG can be written at (Pillow refuses more: the save raised
# OSError for a narrow strip scaled to 500 px wide, which is a caller's
# mistake, not a server error).
JPEG_MAX_SIDE = 65500
# leading bytes of the formats Pillow decodes here: JPEG, PNG, GIF, BMP (and
# WEBP: RIFF....WEBP, see _is_picture)
_MAGIC = (b"\xff\xd8\xff", b"\x89PNG", b"GIF8", b"BM")
_UNSUPPORTED = {b"ftypavif": "AVIF", b"ftypavis": "AVIF", b"ftypheic": "HEIC", b"ftypheix": "HEIC",
                b"ftypmif1": "HEIF", b"ftypmsf1": "HEIF"}


class PictureError(ValueError):
    """The picture cannot be used: the message says why, for the API caller."""


# Longest `image` link accepted.
MAX_LINK = 2048
# A link that names its own scheme ("https://", "ftp://") at its start.
_SCHEME_RE = re.compile(r"[a-zA-Z][a-zA-Z0-9+.\-]*://")


def resolve_link(value):
    """The `image` request param: an http(s) link with a public-looking host
    name ("example.com/a.jpg" gets https://) -> the link. A RefField
    resolver: PictureError (a ValueError) carries the message. Where the
    host really points is checked when the picture is downloaded."""
    value = (value or "").strip()
    if not value:
        raise PictureError("image is required: a picture link (https://...)")
    if value.lower().startswith("data:"):
        raise PictureError("image must be a picture link (https://...), not a data: URI")
    if len(value) > MAX_LINK:
        raise PictureError(f"image link is longer than {MAX_LINK} characters")
    link = value
    if not _SCHEME_RE.match(value):
        # no scheme of its own ("cdn.example.com/a.jpg", "//cdn.example.com/a.jpg"); a
        # "://" further in (a link inside the query string) is not the link's scheme
        bare = value.lstrip("/")
        link = "https://" + bare if "." in bare.split("/")[0] else ""
    parsed = urlparse(link)
    if parsed.scheme not in ("http", "https") or "." not in (parsed.hostname or ""):
        raise PictureError("image must be an http(s) picture link with a public host name")
    return link


def check_public_host(url):
    """Refuse a link that is not http(s) or whose host resolves into a
    private / loopback / link-local / reserved network."""
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise PictureError(f"image must be an http(s) link, got {parsed.scheme or 'no'} scheme")
    host = (parsed.hostname or "").strip("[]")
    if not host:
        raise PictureError("image link has no host")
    try:
        infos = socket.getaddrinfo(host, parsed.port or (443 if parsed.scheme == "https" else 80),
                                   proto=socket.IPPROTO_TCP)
    except (socket.gaierror, UnicodeError, ValueError):
        raise PictureError(f"image host {host} does not resolve")
    for info in infos:
        addr = ipaddress.ip_address(info[4][0].split("%")[0])
        # not is_global: the shared address space (100.64.0.0/10, cluster and
        # carrier-NAT ranges) is neither "private" nor public to Python
        if (addr.is_private or addr.is_loopback or addr.is_link_local or addr.is_reserved
                or addr.is_multicast or addr.is_unspecified or not addr.is_global):
            raise PictureError(f"image host {host} points into a private network")


def _get(url, headers, impersonate, max_bytes, timeout):
    """GET with redirects followed by hand (each hop host-checked) and the
    size cap -> (status, content type, body)."""
    from curl_cffi import requests as curl_requests
    sess = curl_requests.Session(impersonate=impersonate) if impersonate else curl_requests.Session()
    try:
        for _ in range(MAX_REDIRECTS + 1):
            check_public_host(url)
            resp = sess.get(url, headers=headers, timeout=timeout, allow_redirects=False, stream=True)
            try:
                if resp.status_code in (301, 302, 303, 307, 308) and resp.headers.get("location"):
                    url = urljoin(url, resp.headers["location"])
                    continue
                chunks, size = [], 0
                for chunk in resp.iter_content():
                    size += len(chunk)
                    if size > max_bytes:
                        raise PictureError(f"image is larger than {max_bytes // (1024 * 1024)} MB")
                    chunks.append(chunk)
                return resp.status_code, (resp.headers.get("content-type") or "").lower(), b"".join(chunks)
            finally:
                try:
                    resp.close()
                except Exception:
                    pass
        raise PictureError("image link redirects too many times")
    finally:
        try:
            sess.close()
        except Exception:
            pass


def _is_picture(body):
    if body[:4] == b"RIFF":
        return body[8:12] == b"WEBP"
    return body.startswith(_MAGIC)


def download(url, user_agent=None, max_bytes=MAX_BYTES, timeout=TIMEOUT):
    """The picture's bytes. Browser TLS first; a refusal (4xx / 5xx / not a
    picture / a transport error) is tried once more with a plain client
    sending `user_agent` (skipped when None)."""
    attempts = [("chrome", BROWSER_HEADERS)]
    if user_agent:
        attempts.append((None, {"accept": IMAGE_ACCEPT, "user-agent": user_agent}))
    last = None
    for impersonate, headers in attempts:
        try:
            status, ctype, body = _get(url, headers, impersonate, max_bytes, timeout)
        except PictureError:
            raise
        except Exception as e:
            last = f"could not download it ({type(e).__name__})"
            continue
        if status != 200:
            last = f"its server answered HTTP {status}"
            continue
        kind = _UNSUPPORTED.get(body[4:12])
        if kind:
            raise PictureError(f"image must be a JPEG, PNG, WEBP or GIF picture (the link serves {kind})")
        if _is_picture(body):
            return body
        last = ("the link is a web page, not a picture: use the picture's own link"
                if "html" in ctype else f"the link did not serve a picture ({ctype or 'no content type'})")
    raise PictureError(f"image could not be used: {last}")


def decode(body):
    """Bytes -> an upright RGB Pillow image on white (EXIF orientation
    applied, transparency flattened)."""
    import io

    from PIL import Image, ImageOps
    try:
        image = Image.open(io.BytesIO(body))
        image.load()
        image = ImageOps.exif_transpose(image) or image
    except Exception:
        raise PictureError("image could not be decoded as a JPEG, PNG, WEBP or GIF picture")
    if min(image.size) < MIN_SIDE:
        raise PictureError(f"image is too small ({image.width}x{image.height} px)")
    if image.mode in ("RGBA", "LA", "PA") or (image.mode == "P" and "transparency" in image.info):
        rgba = image.convert("RGBA")
        canvas = Image.new("RGB", rgba.size, "white")
        canvas.paste(rgba, mask=rgba.split()[-1])
        return canvas
    return image.convert("RGB")


def to_jpeg(image, width=None, max_side=None, quality=85):
    """A decoded image as JPEG bytes: `width` scales it to exactly that width
    (up or down, aspect kept); `max_side` only shrinks the longer side to it."""
    import io

    from PIL import Image
    if width and image.width != width:
        height = max(1, round(image.height * width / image.width))
        if height > JPEG_MAX_SIDE:      # checked before the resize: a strip this tall is not worth the memory
            raise PictureError("image is too tall for its width: crop it to the product")
        image = image.resize((width, height), Image.LANCZOS)
    elif max_side and max(image.size) > max_side:
        image = image.copy()
        image.thumbnail((max_side, max_side), Image.LANCZOS)
    if max(image.size) > JPEG_MAX_SIDE:
        raise PictureError(f"image is too large ({image.width}x{image.height} px): crop it to the product")
    buf = io.BytesIO()
    image.save(buf, "JPEG", quality=quality)
    return buf.getvalue()


def fetch_jpeg(url, user_agent=None, width=None, max_side=None, quality=85, max_bytes=MAX_BYTES):
    """download + decode + to_jpeg in one call -> JPEG bytes."""
    return to_jpeg(decode(download(url, user_agent=user_agent, max_bytes=max_bytes)),
                   width=width, max_side=max_side, quality=quality)
