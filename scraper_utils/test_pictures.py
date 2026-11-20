"""scraper_utils/pictures.py: the image link, the guarded download, the re-encode.
No network (the HTTP client is faked; host checks use IP literals and localhost).

    python -m pytest scraper_utils/test_pictures.py -q
"""
import io
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scraper_utils import pictures  # noqa: E402
from scraper_utils.pictures import PictureError  # noqa: E402


def png(size=(40, 20), color=(0, 128, 255, 255)):
    from PIL import Image
    buf = io.BytesIO()
    Image.new("RGBA", size, color).save(buf, "PNG")
    return buf.getvalue()


# ---- the link -------------------------------------------------------------------------------

def test_resolve_link():
    assert pictures.resolve_link(" https://cdn.example.com/a.jpg?x=1 ") == "https://cdn.example.com/a.jpg?x=1"
    assert pictures.resolve_link("cdn.example.com/a.jpg") == "https://cdn.example.com/a.jpg"
    assert pictures.resolve_link("http://93.184.216.34/a.jpg") == "http://93.184.216.34/a.jpg"
    # no scheme of its own: a protocol-relative link, and a link whose query holds another link
    assert pictures.resolve_link("//cdn.example.com/a.jpg") == "https://cdn.example.com/a.jpg"
    assert pictures.resolve_link("cdn.example.com/r?u=https://x.example/a.jpg") == "https://cdn.example.com/r?u=https://x.example/a.jpg"


@pytest.mark.parametrize("value,message", [
    ("", "required"), ("data:image/png;base64,AAAA", "data: URI"), ("ftp://example.com/a.jpg", "http(s)"),
    ("localhost/a.jpg", "public host"), ("https://intranet/a.jpg", "public host"),
    ("https://example.com/" + "a" * 2100, "longer than"),
])
def test_resolve_link_rejects(value, message):
    with pytest.raises(PictureError, match=message.replace("(", r"\(").replace(")", r"\)")):
        pictures.resolve_link(value)


# ---- the network guard --------------------------------------------------------------------------

@pytest.mark.parametrize("link", ["http://127.0.0.1/a.jpg", "http://10.1.2.3/a.jpg", "http://192.168.0.9/a.jpg",
                                  "http://169.254.169.254/computeMetadata/v1/", "http://[::1]/a.jpg",
                                  "http://localhost:8080/a.jpg", "http://0.0.0.0/a.jpg",
                                  "http://100.64.0.10/a.jpg"])      # shared address space: cluster / carrier-NAT ranges
def test_private_hosts_are_refused(link):
    with pytest.raises(PictureError, match="private network"):
        pictures.check_public_host(link)


def test_public_host_passes():
    pictures.check_public_host("https://93.184.216.34/a.jpg")


class FakeResponse:
    def __init__(self, status, body=b"", headers=None):
        self.status_code, self.body, self.headers = status, body, headers or {}

    def iter_content(self):
        for i in range(0, len(self.body), 1000):
            yield self.body[i:i + 1000]

    def close(self):
        pass


class FakeSession:
    script = []
    seen = []

    def __init__(self, impersonate=None):
        self.impersonate = impersonate

    def get(self, url, headers=None, timeout=None, allow_redirects=True, stream=False):
        FakeSession.seen.append((url, self.impersonate, (headers or {}).get("user-agent")))
        return FakeSession.script.pop(0)

    def close(self):
        pass


@pytest.fixture
def http(monkeypatch):
    from curl_cffi import requests as curl_requests
    monkeypatch.setattr(curl_requests, "Session", FakeSession)
    FakeSession.script, FakeSession.seen = [], []
    return FakeSession


def test_a_redirect_into_a_private_network_is_refused(http):
    http.script = [FakeResponse(302, headers={"location": "http://169.254.169.254/computeMetadata/v1/"})]
    with pytest.raises(PictureError, match="private network"):
        pictures.download("https://93.184.216.34/a.jpg")
    assert len(http.seen) == 1                              # the private hop was never requested


def test_redirects_are_followed_hop_by_hop(http):
    http.script = [FakeResponse(301, headers={"location": "/b.png"}), FakeResponse(200, png(), {"content-type": "image/png"})]
    assert pictures.download("https://93.184.216.34/a.png") == png()
    assert [u for u, _, _ in http.seen] == ["https://93.184.216.34/a.png", "https://93.184.216.34/b.png"]


def test_size_cap(http):
    http.script = [FakeResponse(200, b"\xff\xd8\xff" + b"0" * 5000)]
    with pytest.raises(PictureError, match="larger than"):
        pictures.download("https://93.184.216.34/a.jpg", max_bytes=4000)


def test_a_refused_browser_download_is_tried_with_the_named_client(http):
    http.script = [FakeResponse(403, b"denied"), FakeResponse(200, png())]
    assert pictures.download("https://93.184.216.34/a.png", user_agent="Named/1.0") == png()
    assert [(imp, ua) for _, imp, ua in http.seen] == [("chrome", None), (None, "Named/1.0")]


@pytest.mark.parametrize("answer,message", [
    (FakeResponse(200, b"<!doctype html><html>", {"content-type": "text/html"}), "web page"),
    (FakeResponse(200, b"\x00\x00\x00\x1cftypavif" + b"0" * 50, {"content-type": "image/avif"}), "AVIF"),
    (FakeResponse(404, b"gone"), "HTTP 404"),
])
def test_what_is_not_a_picture(http, answer, message):
    http.script = [answer]
    with pytest.raises(PictureError, match=message):
        pictures.download("https://93.184.216.34/a")


# ---- the re-encode ------------------------------------------------------------------------------

def test_decode_flattens_transparency_and_applies_exif_orientation():
    from PIL import Image
    im = pictures.decode(png(color=(255, 0, 0, 0)))
    assert im.mode == "RGB" and im.getpixel((1, 1)) == (255, 255, 255)
    buf = io.BytesIO()
    exif = Image.Exif()
    exif[0x0112] = 6                                       # rotate 90 degrees to show upright
    Image.new("RGB", (60, 30), "red").save(buf, "JPEG", exif=exif)
    assert pictures.decode(buf.getvalue()).size == (30, 60)


def test_decode_rejects():
    with pytest.raises(PictureError, match="could not be decoded"):
        pictures.decode(b"\xff\xd8\xff not really a jpeg")
    with pytest.raises(PictureError, match="too small"):
        pictures.decode(png(size=(1, 1)))


def test_to_jpeg_sizes():
    from PIL import Image
    im = pictures.decode(png(size=(1000, 500)))
    assert Image.open(io.BytesIO(pictures.to_jpeg(im, width=500))).size == (500, 250)
    assert Image.open(io.BytesIO(pictures.to_jpeg(pictures.decode(png((200, 100))), width=500))).size == (500, 250)
    assert Image.open(io.BytesIO(pictures.to_jpeg(im, max_side=400))).size == (400, 200)
    assert Image.open(io.BytesIO(pictures.to_jpeg(pictures.decode(png((300, 100))), max_side=400))).size == (300, 100)


def test_to_jpeg_refuses_a_strip_no_jpeg_can_hold():
    """A narrow strip scaled to the wanted width passes JPEG's largest side:
    the caller's mistake (PictureError), where the save raised OSError."""
    strip = pictures.decode(png(size=(10, 2000)))
    with pytest.raises(PictureError, match="too tall"):
        pictures.to_jpeg(strip, width=500)
    assert pictures.to_jpeg(strip, max_side=400)            # shrinking it is fine
