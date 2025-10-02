import os
import warnings

import pytest
import urllib3

from pi_web_sdk.client import PIWebAPIClient
from pi_web_sdk.config import AuthMethod, PIWebAPIConfig
from pi_web_sdk.exceptions import PIWebAPIError


def _bool_from_env(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@pytest.fixture(scope="session")
def pi_web_api_client():
    """Provide a configured PI Web API client backed by the live controller."""
    base_url = os.getenv("PI_WEB_API_BASE_URL", "https://172.30.136.15/piwebapi")
    timeout = int(os.getenv("PI_WEB_API_TIMEOUT", "10"))
    verify_ssl = _bool_from_env("PI_WEB_API_VERIFY_SSL", default=False)
    auth_method_name = os.getenv("PI_WEB_API_AUTH_METHOD", "anonymous").strip().lower()

    try:
        auth_method = AuthMethod(auth_method_name)
    except ValueError as exc:
        pytest.skip(f"Unsupported auth method '{auth_method_name}': {exc}")

    config = PIWebAPIConfig(
        base_url=base_url,
        auth_method=auth_method,
        username=os.getenv("PI_WEB_API_USERNAME"),
        password=os.getenv("PI_WEB_API_PASSWORD"),
        token=os.getenv("PI_WEB_API_TOKEN"),
        verify_ssl=verify_ssl,
        timeout=timeout,
    )

    if not verify_ssl:
        warnings.filterwarnings(
            "ignore", category=urllib3.exceptions.InsecureRequestWarning
        )

    client = PIWebAPIClient(config)

    try:
        client.system.versions()
    except PIWebAPIError as exc:
        pytest.skip(f"PI Web API not reachable at {base_url}: {exc}")

    return client


@pytest.fixture(scope="session")
def test_af_database(pi_web_api_client):
    """
    Provide the AF database WebId for tests.

    Can be configured via environment variable PI_WEB_API_TEST_DATABASE.
    If not set, uses the first database from the first asset server.

    Example:
        export PI_WEB_API_TEST_DATABASE="TestDatabase"
        export PI_WEB_API_TEST_DATABASE="F1RDa..."  # Or use WebId directly
    """
    # Check if specific database is configured
    test_db_name = os.getenv("PI_WEB_API_TEST_DATABASE", "Default")

    # Get first asset server
    servers = pi_web_api_client.asset_server.list()
    if not servers.get("Items"):
        pytest.skip("No asset servers available")

    asset_server = servers["Items"][0]
    asset_server_web_id = asset_server["WebId"]

    # If test database name/WebId is specified, try to find it
    if test_db_name:
        # Check if it's a WebId (starts with F1)
        if test_db_name.startswith("F1"):
            try:
                db = pi_web_api_client.asset_database.get(test_db_name)
                return {
                    "web_id": db["WebId"],
                    "name": db["Name"],
                    "path": db["Path"],
                    "asset_server_web_id": asset_server_web_id,
                }
            except Exception:
                pytest.skip(f"Test database WebId '{test_db_name}' not found")

        # Otherwise treat it as a database name
        databases = pi_web_api_client.asset_server.get_databases(asset_server_web_id)
        for db in databases.get("Items", []):
            if db["Name"] == test_db_name:
                return {
                    "web_id": db["WebId"],
                    "name": db["Name"],
                    "path": db["Path"],
                    "asset_server_web_id": asset_server_web_id,
                }

        pytest.skip(f"Test database '{test_db_name}' not found")

    # Use first available database
    databases = pi_web_api_client.asset_server.get_databases(asset_server_web_id)
    if not databases.get("Items"):
        pytest.skip("No databases available")

    db = databases["Items"][0]
    return {
        "web_id": db["WebId"],
        "name": db["Name"],
        "path": db["Path"],
        "asset_server_web_id": asset_server_web_id,
    }
