from html.parser import HTMLParser
from pathlib import Path

from utils.compliance import (
    PUBLIC_ETHICS_URL,
    PUBLIC_HOME_URL,
    PUBLIC_PRIVACY_URL,
    PUBLIC_TERMS_URL,
)


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
    for scope_description in (
        "activity and fitness",
        "health metrics and measurements",
        "sleep",
        "settings",
    ):
        assert scope_description in homepage
    assert "battery level and status" in homepage
    for scope in (
        "googlehealth.activity_and_fitness.readonly",
        "googlehealth.health_metrics_and_measurements.readonly",
        "googlehealth.sleep.readonly",
        "googlehealth.settings.readonly",
    ):
        assert scope in homepage
    assert "Google Health write" not in homepage


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
    assert "battery level and status" in privacy
    assert "heart rate, steps, sleep data, physical activity, and respiratory rate" in privacy
    assert "Google Cloud Firestore" in privacy
    assert "Google Sheets recovery" in privacy


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


def test_public_ethics_page_links_both_approved_google_health_addenda():
    ethics = _read("research-ethics.html")
    for language in ("en", "he"):
        filename = f"google-health-addendum-385-23-{language}-approved-v1.0.pdf"
        assert f'assets/{filename}' in ethics
        asset = ROOT / "assets" / "compliance" / filename
        assert asset.read_bytes().startswith(b"%PDF")


def test_canonical_production_urls_point_to_public_static_site():
    assert PUBLIC_HOME_URL == "https://admontracker.online/"
    assert PUBLIC_PRIVACY_URL == "https://admontracker.online/privacy.html"
    assert PUBLIC_TERMS_URL == "https://admontracker.online/terms.html"
    assert PUBLIC_ETHICS_URL == "https://admontracker.online/research-ethics.html"
