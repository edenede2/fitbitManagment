"""Validate actual grants without logging credential payloads."""


def require_granted_scopes(granted: object, required: list[str], *, provider: str) -> None:
    actual = set(str(granted or "").split())
    expected = set(required)
    if not expected or not expected <= actual:
        raise ValueError("Required provider permissions were not granted")
    if provider == "google_health" and {s for s in actual if "/auth/googlehealth." in s} != expected:
        raise ValueError("Unexpected Google Health permissions were granted")
