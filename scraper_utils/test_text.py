"""The text helpers: what each variant promises."""
from scraper_utils import text as T


def test_short_string_variants():
    assert T.stripped("  a  b ") == "a  b" and T.stripped("") is None and T.stripped(5) is None
    assert T.trimmed("  a  b ") == "a  b" and T.trimmed(5) == "5" and T.trimmed(True) is None and T.trimmed({}) is None
    assert T.squashed(" a \n b ") == "a b" and T.squashed(5) is None and T.squashed("  ") is None
    assert T.squashed_any(" a \n b ") == "a b" and T.squashed_any(5) == "5" and T.squashed_any(None) is None
    assert T.clean(" a\xa0&amp; \n b ") == "a & b" and T.clean({"text": " x "}) == "x" and T.clean("") is None
    assert T.plain("<b>a</b> &amp; <i>b</i>") == "a & b" and T.plain(None) is None


def test_multiline_keeps_line_breaks():
    assert T.multiline(" a  b \n\n\n\n c ") == "a b\n\nc"
    assert T.multiline({"text": "x\ny"}) == "x\ny" and T.multiline(None) is None and T.multiline(" \n ") is None


def test_lower_snake():
    assert T.lower_snake("Work/Life balance") == "work_life_balance"
    assert T.lower_snake("Benefits &amp; perks") == "benefits_perks" and T.lower_snake("---") is None


def test_html_to_text():
    assert T.html_to_text("<p>a</p><p>b</p>") == "a\n\nb"
    assert T.html_to_text("a<br>b") == "a\nb"
    assert T.html_to_text("<ul><li>a</li><li>b</li></ul>") == "- a\n- b"
    assert T.html_to_text("<script>x</script>y") == "y"
    assert T.html_to_text("plain &amp; simple") == "plain & simple"
    assert T.html_to_text("") is None and T.html_to_text(5) is None and T.html_to_text(None) is None
