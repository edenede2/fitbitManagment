from html.parser import HTMLParser
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SITE = ROOT / "public_site"


class _Parser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            href = dict(attrs).get("href")
            if href:
                self.links.append(href)


def _read(name: str) -> str:
    text = (SITE / name).read_text(encoding="utf-8")
    parser = _Parser()
    parser.feed(text)
    return text


def test_public_homepage_is_crawler_readable_and_explains_purpose():
    homepage = _read("index.html")
    assert "AdmonTracker" in homepage
    assert "Purpose and functionality" in homepage
    assert "Why AdmonTracker requests Google access" in homepage
    assert 'href="privacy.html"' in homepage
    assert "Login required" not in homepage


def test_static_privacy_policy_contains_google_required_topics():
    privacy = _read("privacy.html")
    required = (
        "Google user data and other information accessed",
        "How information is used",
        "Storage and protection",
        "Sharing and disclosure",
        "Retention, withdrawal, and deletion",
        "Google API Services User Data Policy",
        "Limited Use requirements",
        "Google Secret Manager",
        "radmon@psy.haifa.ac.il",
    )
    for phrase in required:
        assert phrase in privacy


def test_public_site_internal_links_have_targets():
    for name in ("index.html", "privacy.html", "terms.html", "research-ethics.html"):
        source = (SITE / name).read_text(encoding="utf-8")
        parser = _Parser()
        parser.feed(source)
        for href in parser.links:
            if href.startswith(("http://", "https://", "mailto:", "#")):
                continue
            target = href.split("#", 1)[0]
            if target.startswith("assets/"):
                continue  # Copied into the Pages artifact by the deployment workflow.
            assert (SITE / target).is_file(), f"Missing {href} linked from {name}"
