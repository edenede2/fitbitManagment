"""Central authentication, authorization, and session-isolation helpers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Callable, Literal, Optional
import time
import math

import streamlit as st

from utils.compliance import (
    PUBLIC_ETHICS_URL,
    PUBLIC_PRIVACY_URL,
    PUBLIC_TERMS_URL,
)


AccessMode = Literal["anonymous", "guest", "authenticated"]
AUTHORIZED_ROLES = {"Admin", "Manager"}
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
    principal_id: str = ""

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
        return f"{self.mode}:{self.email or ''}:{self.role}:{self.project}:{self.principal_id}"

    def allows_project(self, project: str) -> bool:
        return self.can_read_real_data and (
            (self.role == "Admin" and self.project == "*")
            or project in {p.strip() for p in self.project.split(";") if p.strip()}
        )

    @classmethod
    def anonymous(cls) -> "AccessContext":
        return cls(mode="anonymous")

    @classmethod
    def guest(cls, email: Optional[str] = "guest@example.invalid") -> "AccessContext":
        return cls(mode="guest", email=email, role="Guest", project="Demo")


def normalize_role(value: object) -> str:
    role = str(value or "Guest").strip().title()
    return role if role in AUTHORIZED_ROLES else "Guest"


def lookup_access_assignment(secrets: object, email_prefix: str) -> str:
    """Find an explicit user assignment in flat or grouped TOML secrets.

    Streamlit secrets are dict-like and deployments commonly keep access
    entries either at the root or below a ``users``/``access`` table. TOML
    also turns unquoted usernames containing dots into nested tables. Only
    scalar values whose first field is a supported role are accepted.
    """
    target = str(email_prefix or "").strip().casefold()
    # This compatibility helper accepts full emails only. A root/table prefix
    # entry is never an authorization fallback, including dotted TOML keys.
    if "@" not in target:
        return "Guest"

    def valid_assignment(value: object) -> bool:
        if not isinstance(value, str):
            return False
        role = value.split(",", 1)[0].strip().title()
        return role in {*AUTHORIZED_ROLES, "Guest"}

    def find_in_table(table: object, path: tuple[str, ...] = ()):
        if not isinstance(table, Mapping) and not hasattr(table, "items"):
            return None
        try:
            entries = table.items()
        except Exception:
            return None

        for raw_key, value in entries:
            key = str(raw_key)
            current_path = (*path, key)
            full_key = ".".join(current_path).casefold()
            leaf_key = key.casefold()
            if (leaf_key == target or full_key == target) and valid_assignment(value):
                return value

            nested = find_in_table(value, current_path)
            if nested is not None:
                return nested
        return None

    try:
        assignment = find_in_table(secrets)
        if assignment is not None:
            return assignment
    except Exception:
        pass
    return "Guest"


def user_access_from_secrets(secrets: object, user_email: str) -> tuple[str, str]:
    """Resolve a Streamlit OIDC email to an authorized role and project."""
    username = str(user_email or "").strip().casefold()
    if "@" not in username:
        return "Guest", "None"

    access_value = lookup_access_assignment(secrets, username)
    parts = [part.strip() for part in str(access_value).split(",")]
    role = normalize_role(parts[0] if parts else "Guest")
    if role == "Guest":
        return "Guest", "None"
    project = parts[1] if len(parts) > 1 and parts[1] else "None"
    return role, project


def staff_identity_assignment(secrets: object, claims: Mapping, *, now: float | None = None) -> AccessContext:
    """Resolve an explicitly approved OIDC binding, never auto-link by email.

    Streamlit validates the OIDC signature/nonce. Here we additionally validate
    required claims and expiry because its identity cookie can outlive the ID
    token. Email is a checked attribute, not the account's unique identity.
    """
    now = time.time() if now is None else now
    email = str(claims.get("email") or "").strip()
    issuer = claims.get("iss")
    subject = claims.get("sub")
    if (
        claims.get("is_logged_in") is not True
        or issuer not in ("https://accounts.google.com", "accounts.google.com")
        or not isinstance(subject, str) or not subject.strip()
        or claims.get("email_verified") is not True or "@" not in email
    ):
        return AccessContext.guest(email=email or None)
    try:
        expires = float(claims["exp"])
        if not math.isfinite(expires) or expires <= now:
            return AccessContext.guest(email=email)
        expected = secrets.get("auth", {}).get("google", {}).get("client_id")
        audience = claims.get("aud")
        if expected and expected not in (audience if isinstance(audience, list) else [audience]):
            return AccessContext.guest(email=email)
        bindings = secrets.get("staff_identities", [])
    except (KeyError, ValueError, TypeError, AttributeError):
        return AccessContext.guest(email=email)
    if not isinstance(bindings, (list, tuple)):
        return AccessContext.guest(email=email)
    matches = [b for b in bindings if isinstance(b, Mapping) and b.get("enabled") is True
               and b.get("issuer") == "https://accounts.google.com" and b.get("subject") == subject]
    if len(matches) != 1:
        return AccessContext.guest(email=email)
    binding = matches[0]
    role = normalize_role(binding.get("role"))
    project = str(binding.get("project") or "None").strip()
    if (str(binding.get("email") or "").casefold() != email.casefold()
            or role not in AUTHORIZED_ROLES or project in ("", "None")
            or (role == "Admin" and project != "*")
            or (role != "Admin" and "*" in project)):
        return AccessContext.guest(email=email)
    from utils.security_store import security_key
    return AccessContext("authenticated", email, role, project,
                         security_key("staff", "https://accounts.google.com", subject))


def native_logout_principal(claims: Mapping) -> str:
    """Self-revocation only, even after expiry/role removal; NEVER authorizes data.

    Input is the signature-validated native Streamlit identity cookie, not an
    uploaded JWT or participant-controlled request body.
    """
    subject = claims.get("sub")
    if (claims.get("is_logged_in") is not True or claims.get("email_verified") is not True
            or claims.get("iss") not in ("accounts.google.com", "https://accounts.google.com")
            or not isinstance(subject, str) or not subject.strip()):
        return ""
    from utils.security_store import security_key
    return security_key("staff", "https://accounts.google.com", subject)


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


def _user_claim(user: object, name: str) -> object:
    """Read an OIDC claim from Streamlit's dict-like user proxy safely."""
    if isinstance(user, Mapping):
        value = user.get(name)
        if value is not None:
            return value

    try:
        return getattr(user, name)
    except (AttributeError, KeyError, TypeError):
        return None


def _streamlit_identity() -> tuple[bool, Optional[str]]:
    try:
        user = st.user
        if user is None:
            return False, None

        logged_in_claim = _user_claim(user, "is_logged_in")
        email_claim = _user_claim(user, "email")
        email = str(email_claim).strip() if email_claim else None

        # An email claim by itself is not sufficient: Streamlit's explicit
        # login flag must be true before any role assignment can be used.
        logged_in = bool(logged_in_claim)
        return logged_in, email if logged_in else None
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
        security_state = {key: st.session_state[key] for key in ("staff_session_id",)
                          if key in st.session_state}
        clear_data_session_state()
        st.session_state.update(security_state)

    if context.is_authenticated:
        # Prevent a prior demo flag from reappearing after a native Google logout.
        st.session_state.demo_mode = False

    st.session_state.access_fingerprint = context.fingerprint
    st.session_state.access_mode = context.mode
    st.session_state.user_email = context.email
    st.session_state.user_role = context.role if not context.is_anonymous else None
    st.session_state.user_project = context.project if not context.is_anonymous else None
    st.session_state.principal_id = context.principal_id
    return context


def resolve_access_context(
    access_lookup: Optional[Callable[[str], tuple[str, str]]] = None,
) -> AccessContext:
    """Resolve the current Streamlit session without trusting role session keys."""
    logged_in, email = _streamlit_identity()
    if logged_in:
        # access_lookup is retained for API compatibility but cannot override
        # validated subject bindings (including the controller's old lookup).
        names = ("is_logged_in", "email", "iss", "sub", "email_verified", "exp", "aud")
        claims = {name: _user_claim(st.user, name) for name in names}
        try:
            context = staff_identity_assignment(st.secrets, claims)
            if context.is_authenticated:
                from utils.staff_sessions import validate_staff_session
                session_id = st.session_state.get("staff_session_id")
                if not session_id:
                    context = AccessContext.guest(email=email)
                else:
                    validate_staff_session(context, session_id)
        except Exception:
            context = AccessContext.guest(email=email)
    else:
        context = build_access_context(
            streamlit_logged_in=False,
            demo_mode=bool(st.session_state.get("demo_mode", False)),
        )
    return synchronize_access_context(context)


def current_access_context() -> AccessContext:
    return resolve_access_context()


def _checked_context(context: Optional[AccessContext]) -> AccessContext:
    if context is None:
        return current_access_context()
    from utils.security_store import collection_controls_enabled
    if collection_controls_enabled():
        live = current_access_context()
        if live.fingerprint != context.fingerprint:
            raise AccessDenied("This account's authorization changed; sign in again.")
        return live
    return context


def require_project_access(project: str, context: Optional[AccessContext] = None) -> AccessContext:
    context = require_real_data_access(context)
    if not context.allows_project(project):
        raise AccessDenied("This account is not assigned to this project.")
    return context


def require_real_data_access(context: Optional[AccessContext] = None) -> AccessContext:
    context = _checked_context(context)
    if not context.can_read_real_data:
        raise AccessDenied("This session is not permitted to access production data.")
    return context


def require_write_access(context: Optional[AccessContext] = None) -> AccessContext:
    context = _checked_context(context)
    if not context.can_write:
        raise AccessDenied("Guest and anonymous sessions are read-only.")
    return context


def require_device_management(context: Optional[AccessContext] = None) -> AccessContext:
    context = _checked_context(context)
    if not context.can_manage_devices:
        raise AccessDenied("Device management requires an Admin or Manager account.")
    return context


def require_external_service_access(context: Optional[AccessContext] = None) -> AccessContext:
    context = _checked_context(context)
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
    st.page_link(PUBLIC_PRIVACY_URL, label="Privacy Policy", icon="🔒")
    st.page_link(PUBLIC_TERMS_URL, label="Terms of Use", icon="📄")
    st.page_link(PUBLIC_ETHICS_URL, label="Research ethics & documents", icon="🏛️")
