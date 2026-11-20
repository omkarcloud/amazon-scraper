"""amazon/image_search.py: the token is kept per marketplace and a 403 fetches
a fresh one once. No network (the page, its cookies and the upload are faked).

    python -m pytest amazon/test_image_search.py -q
"""
import gzip
import os
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from amazon import image_search as I  # noqa: E402

with gzip.open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures", "stylesnap_us.json.gz"), "rt",
               encoding="utf-8") as f:
    ANSWER = f.read()


@pytest.fixture
def amazon(monkeypatch):
    monkeypatch.setattr(I, "_tokens", {})
    monkeypatch.setattr(I, "_picture", lambda image: b"\xff\xd8\xff jpeg")
    state = SimpleNamespace(pages=[], tokens=[], answers=[])

    def page(country, path, language=None, label=None):
        state.pages.append((country, path))
        return f"<form><input type='hidden' name='stylesnap' value='T{len(state.pages)}'/></form>"

    def upload(country, path, files, params=None, headers=None, cookies=None, language=None, label=None):
        state.tokens.append(params["stylesnapToken"])
        assert files[0][:3] == ("explore-looks.jpg", "explore-looks.jpg", "image/jpeg")
        assert cookies == [("session-id", "S", ".amazon.com", "/")] and headers["origin"] == "https://www.amazon.com"
        return state.answers.pop(0)

    monkeypatch.setattr(I.fetch, "page", page)
    monkeypatch.setattr(I.fetch, "page_cookies", lambda country, language=None: [("session-id", "S", ".amazon.com", "/")])
    monkeypatch.setattr(I.fetch, "upload", upload)
    return state


def ok():
    return SimpleNamespace(status_code=200, text=ANSWER)


def test_a_stale_token_is_replaced_once_and_the_new_one_kept(amazon):
    amazon.answers = [SimpleNamespace(status_code=403, text="<html>refused</html>"), ok(), ok()]
    out = I.search_by_image("https://example.com/a.jpg", "US")
    assert amazon.tokens == ["T1", "T2"] and amazon.pages == [("US", "/stylesnap"), ("US", "/stylesnap")]
    assert out["matches_count"] == 57 and out["country"] == "US" and out["image"] == "https://example.com/a.jpg"
    I.search_by_image("https://example.com/b.jpg", "US")
    assert amazon.tokens[-1] == "T2" and len(amazon.pages) == 2           # kept: no page for the next upload


def test_refused_with_a_fresh_token_is_blocked(amazon):
    amazon.answers = [SimpleNamespace(status_code=403, text="x"), SimpleNamespace(status_code=403, text="x")]
    with pytest.raises(I.fetch.AmazonBlocked):
        I.search_by_image("https://example.com/a.jpg", "US")


def test_nothing_matched_is_not_cached_and_other_marketplaces_are_refused(amazon):
    amazon.answers = [SimpleNamespace(status_code=200, text='{"queryId": "q", "searchResults": []}')]
    out = I.search_by_image("https://example.com/a.jpg", "US")
    assert isinstance(out, I.DontCache) and out.data["results"] == [] and out.data["matches_count"] == 0
    with pytest.raises(I.fetch.AmazonBadRequest, match="marketplaces"):
        I.search_by_image("https://example.com/a.jpg", "CA")
