"""Central authentication, authorization, and session-isolation helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Literal, Optional

import streamlit as st


AccessMode = Literal["anonymous", "guest", "authenticated"]
AUTHORIZED_ROLES = {"Admin", "Manager", "Student"}
DEVICE_MANAGER_ROLES = {"Admin", "Manager"}

DEMO_DISCLOSURE = (
    "Guest demo — all people, watches, health metrics, and responses are "
    "fictional examples. No production data is loaded. This session is read-only."
)


class AccessDenied(PermissionError):
    """Raised when the current session lacks a required capability."""


@dataclass(frozen=True)
class AccessContext:
    mode: AccessMode
    email: Optional[str] = None
    role: str = "Anonymous"
    project: str = "None"

    @property
    def is_anonymous(self) -> bool:
        return self.mode == "anonymous"

    @property
    def is_guest(self) -> bool:
        return self.mode == "guest"

    @property
    def is_authenticated(self) -> bool:
        return self.mode == "authenticated"

    @property
    def can_read_real_data(self) -> bool:
        return self.is_authenticated and self.role in AUTHORIZED_ROLES

    @property
    def can_write(self) -> bool:
        return self.can_read_real_data

    @property
    def can_manage_devices(self) -> bool:
        return self.is_authenticated and self.role in DEVICE_MANAGER_ROLES

    @property
    def can_call_external_services(self) -> bool:
        return self.is_authenticated and self.role in DEVICE_MANAGER_ROLES

    @property
    def fingerprint(self) -> str:
        return f"{self.mode}:{self.email or ''}:{self.role}:{self.project}"

    @classmethod
    def anonymous(cls) -> "AccessContext":
        return cls(mode="anonymous")

    @classmethod
    def guest(cls, email: Optional[str] = "guest@example.invalid") -> "AccessContext":
        return cls(mode="guest", email=email, role="Guest", project="Demo")


def normalize_role(value: object) -> str:
    role = str(value or "Guest").strip().title()
    return role if role in AUTHORIZED_ROLES else "Guest"


def build_access_context(
    *,
    streamlit_logged_in: bool,
    user_email: Optional[str] = None,
    role: object = "Guest",
    project: object = "None",
    demo_mode: bool = False,
) -> AccessContext:
    """Pure access-resolution function used by the Streamlit adapter and tests."""
    if streamlit_logged_in:
        normalized_role = normalize_role(role)
        if normalized_role in AUTHORIZED_ROLES:
            return AccessContext(
                mode="authenticated",
                email=user_email,
                role=normalized_role,
                project=str(project or "None").strip() or "None",
            )
        return AccessContext.guest(email=user_email)

    if demo_mode:
        return AccessContext.guest()

    return AccessContext.anonymous()


def _streamlit_identity() -> tuple[bool, Optional[str]]:
    try:
        user = st.user
        logged_in = bool(
            user is not None
            and hasattr(user, "is_logged_in")
            and user.is_logged_in
        )
        return logged_in, getattr(user, "email", None) if logged_in else None
    except Exception:
        return False, None


def clear_data_session_state() -> None:
    """Clear all app/widget state while preserving only the demo entry flag."""
    demo_mode = bool(st.session_state.get("demo_mode", False))
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    st.session_state.demo_mode = demo_mode


def synchronize_access_context(context: AccessContext) -> AccessContext:
    """Apply a context and clear cached data whenever identity or mode changes."""
    previous = st.session_state.get("access_fingerprint")
    if previous != context.fingerprint:
        clear_data_session_state()

    if context.is_authenticated:
        # Prevent a prior demo flag from reappearing after a native Google logout.
        st.session_state.demo_mode = False

    st.session_state.access_fingerprint = context.fingerprint
    st.session_state.access_mode = context.mode
    st.session_state.user_email = context.email
    st.session_state.user_role = context.role if not context.is_anonymous else None
    st.session_state.user_project = context.project if not context.is_anonymous else None
    return context


def resolve_access_context(
    access_lookup: Optional[Callable[[str], tuple[str, str]]] = None,
) -> AccessContext:
    """Resolve the current Streamlit session without trusting role session keys."""
    logged_in, email = _streamlit_identity()
    if logged_in:
        role, project = ("Guest", "None")
        if email and access_lookup is not None:
            role, project = access_lookup(email)
        context = build_access_context(
            streamlit_logged_in=True,
            user_email=email,
            role=role,
            project=project,
        )
    else:
        context = build_access_context(
            streamlit_logged_in=False,
            demo_mode=bool(st.session_state.get("demo_mode", False)),
        )
    return synchronize_access_context(context)


def current_access_context() -> AccessContext:
    mode = st.session_state.get("access_mode", "anonymous")
    if mode not in {"anonymous", "guest", "authenticated"}:
        mode = "anonymous"
    return AccessContext(
        mode=mode,
        email=st.session_state.get("user_email"),
        role=st.session_state.get("user_role") or "Anonymous",
        project=st.session_state.get("user_project") or "None",
    )


def require_real_data_access(context: Optional[AccessContext] = None) -> AccessContext:
    context = context or current_access_context()
    if not context.can_read_real_data:
        raise AccessDenied("This session is not permitted to access production data.")
    return context


def require_write_access(context: Optional[AccessContext] = None) -> AccessContext:
    context = context or current_access_context()
    if not context.can_write:
        raise AccessDenied("Guest and anonymous sessions are read-only.")
    return context


def require_device_management(context: Optional[AccessContext] = None) -> AccessContext:
    context = context or current_access_context()
    if not context.can_manage_devices:
        raise AccessDenied("Device management requires an Admin or Manager account.")
    return context


def require_external_service_access(context: Optional[AccessContext] = None) -> AccessContext:
    context = context or current_access_context()
    if not context.can_call_external_services:
        raise AccessDenied("Guest sessions cannot call production services or device APIs.")
    return context


def render_demo_banner(*, include_sidebar: bool = False) -> None:
    st.warning(DEMO_DISCLOSURE, icon="🧪")
    if include_sidebar:
        with st.sidebar:
            st.info(DEMO_DISCLOSURE, icon="🧪")


def render_legal_links() -> None:
    """Render public policy links in the current container."""
    st.page_link("pages/08_Privacy_Policy.py", label="Privacy Policy", icon="🔒")
    st.page_link("pages/09_Terms_of_Service.py", label="Terms of Service", icon="📄")
