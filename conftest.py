"""Unit tests cannot access production networks or inherit deployment credentials."""
import socket
import pytest


@pytest.fixture(autouse=True)
def isolated_unit_test(monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError("External network access is forbidden in unit tests")

    monkeypatch.setattr(socket.socket, "connect", denied)
    monkeypatch.setattr(socket, "create_connection", denied)
    for name in (
        "DATA_BACKEND", "COLLECTION_CONTROL_PROJECT", "GOOGLE_SERVICE_ACCOUNT_JSON_B64",
        "GOOGLE_APPLICATION_CREDENTIALS", "STREAMLIT_SECRETS_SECRET_REF",
        "SECRET_MANAGER_ENABLED", "ALLOW_PLAINTEXT_SECRET_FALLBACK", "GOOGLE_CLOUD_PROJECT",
        "FIRESTORE_PROJECT_ID", "FIRESTORE_DATABASE_ID",
        "ENROLLMENT_BINDING_REQUIRED",
        "GOOGLE_MFA_CLAIMS_ENABLED",
    ):
        monkeypatch.delenv(name, raising=False)


def pytest_collection_modifyitems(items):
    # Old standalone smoke scripts read the lab .env/archive/tracking files and
    # can write real state. Replaced by isolated unit coverage, not silently run.
    manual = {"test_fitbit_organizer.py", "test_complete_tracking.py", "test_datetime_tracking.py",
              "test_temp_fix.py", "test_status_fix.py"}
    for item in items:
        if item.path.name in manual:
            item.add_marker(pytest.mark.skip(reason="Legacy lab smoke script requires explicit isolated live-test approval"))
