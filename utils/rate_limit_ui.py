import re
import time
from typing import Any

import streamlit as st


DEFAULT_RETRY_SECONDS = {
    "google_sheets": 90,
    "google_api": 120,
    "fitbit": 900,
    "api": 120,
}


def _exception_text(error: Any) -> str:
    return str(error or "")


def is_rate_limit_error(error: Any) -> bool:
    text = _exception_text(error).lower()
    return any(
        marker in text
        for marker in (
            "429",
            "quota exceeded",
            "rate limit",
            "ratelimit",
            "too many requests",
            "resource_exhausted",
            "user-rate limit exceeded",
        )
    )


def infer_provider(error: Any, default: str = "api") -> str:
    text = _exception_text(error).lower()
    if "fitbit" in text or "api.fitbit.com" in text:
        return "fitbit"
    if "sheets" in text or "gspread" in text or "quota exceeded" in text:
        return "google_sheets"
    if "google" in text or "resource_exhausted" in text:
        return "google_api"
    return default


def estimate_retry_after_seconds(error: Any, provider: str = "api") -> int:
    retry_after = getattr(error, "retry_after", None)
    if retry_after is None:
        response = getattr(error, "response", None)
        headers = getattr(response, "headers", {}) or {}
        retry_after = headers.get("Retry-After") or headers.get("retry-after")

    try:
        if retry_after:
            return max(1, int(float(retry_after)))
    except (TypeError, ValueError):
        pass

    text = _exception_text(error)
    match = re.search(r"retry(?: again)? after ([0-9]+) ?s", text, flags=re.IGNORECASE)
    if match:
        return max(1, int(match.group(1)))

    return DEFAULT_RETRY_SECONDS.get(provider, DEFAULT_RETRY_SECONDS["api"])


def _format_remaining(seconds: int) -> str:
    seconds = max(0, int(seconds))
    minutes, secs = divmod(seconds, 60)
    if minutes:
        return f"{minutes}m {secs:02d}s"
    return f"{secs}s"


def show_rate_limit_notice(
    error: Any,
    *,
    provider: str | None = None,
    key: str = "default",
    context: str = "",
) -> bool:
    if not is_rate_limit_error(error):
        return False

    provider = provider or infer_provider(error)
    retry_seconds = estimate_retry_after_seconds(error, provider)
    state_key = f"rate_limit_until_{provider}_{key}"
    now = time.time()
    retry_until = max(float(st.session_state.get(state_key, 0) or 0), now + retry_seconds)
    st.session_state[state_key] = retry_until

    remaining = max(0, int(retry_until - now))
    provider_label = {
        "google_sheets": "Google Sheets",
        "google_api": "Google API",
        "fitbit": "Fitbit API",
    }.get(provider, "API")
    context_text = f" while {context}" if context else ""

    st.error(
        f"{provider_label} rate limit reached{context_text}. "
        f"Estimated time before trying again: {_format_remaining(remaining)}."
    )

    total = max(retry_seconds, remaining, 1)
    elapsed = max(0, total - remaining)
    progress = min(1.0, elapsed / total)
    st.progress(progress, text=f"Try again in about {_format_remaining(remaining)}")
    st.caption(
        "This timer is an estimate. If the provider sends a Retry-After value, the app uses it; "
        "otherwise it uses a conservative default."
    )
    if st.button("Refresh now", key=f"rate_limit_refresh_{provider}_{key}"):
        st.rerun()
    return True
