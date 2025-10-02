# Test Configuration Guide

## Test Database Configuration

The `test_af_database` fixture allows you to specify which AF database should be used for integration tests.

### Usage in Tests

```python
def test_my_feature(pi_web_api_client, test_af_database):
    """Test that uses the configured test database."""
    # Get database WebId
    db_web_id = test_af_database["web_id"]

    # Access other database info
    db_name = test_af_database["name"]
    db_path = test_af_database["path"]
    asset_server_web_id = test_af_database["asset_server_web_id"]

    # Use in your tests
    result = pi_web_api_client.asset_database.get_elements(db_web_id)
```

### Configuration Options

#### Option 1: Use Default Database (No Configuration)
By default, tests will use the first database from the first asset server:

```bash
pytest tests/
```

#### Option 2: Specify Database by Name
Set the `PI_WEB_API_TEST_DATABASE` environment variable to your database name:

**Windows:**
```cmd
set PI_WEB_API_TEST_DATABASE=MyTestDatabase
pytest tests/
```

**Linux/Mac:**
```bash
export PI_WEB_API_TEST_DATABASE=MyTestDatabase
pytest tests/
```

#### Option 3: Specify Database by WebId
You can also specify the database directly by WebId:

**Windows:**
```cmd
set PI_WEB_API_TEST_DATABASE=F1RDhYFXrzSwkU2e2UpUQU6XrA...
pytest tests/
```

**Linux/Mac:**
```bash
export PI_WEB_API_TEST_DATABASE=F1RDhYFXrzSwkU2e2UpUQU6XrA...
pytest tests/
```

### Fixture Return Value

The `test_af_database` fixture returns a dictionary with:

```python
{
    "web_id": str,                    # Database WebId
    "name": str,                      # Database name
    "path": str,                      # Full AF path (e.g., \\SERVER\DatabaseName)
    "asset_server_web_id": str        # Parent asset server WebId
}
```

### Example Test File

See [test_database_fixture_example.py](test_database_fixture_example.py) for complete examples.

## Full Environment Variables Reference

| Variable | Description | Default |
|----------|-------------|---------|
| `PI_WEB_API_BASE_URL` | PI Web API base URL | `https://172.27.217.94/piwebapi` |
| `PI_WEB_API_AUTH_METHOD` | Authentication method (`anonymous`, `basic`, `kerberos`) | `anonymous` |
| `PI_WEB_API_USERNAME` | Username for basic auth | None |
| `PI_WEB_API_PASSWORD` | Password for basic auth | None |
| `PI_WEB_API_TOKEN` | API token | None |
| `PI_WEB_API_VERIFY_SSL` | Verify SSL certificates (`1`, `true`, `yes`) | `false` |
| `PI_WEB_API_TIMEOUT` | Request timeout in seconds | `10` |
| `PI_WEB_API_TEST_DATABASE` | **NEW:** Test database name or WebId | First database |

## Using a Dedicated Test Database

For production environments, it's recommended to create a dedicated test database:

1. **Create a test database in PI System Explorer:**
   - Right-click on your Asset Server
   - Select "New Database"
   - Name it something like "TestAutomation" or "SDK_Tests"

2. **Configure tests to use it:**
   ```bash
   export PI_WEB_API_TEST_DATABASE=TestAutomation
   ```

3. **Run tests:**
   ```bash
   pytest tests/ -v
   ```

This ensures your tests don't interfere with production data.

## Best Practices

1. **Cleanup after tests**: Always delete test objects you create
2. **Use unique names**: Use timestamps in test object names to avoid conflicts
3. **Isolate test data**: Use a dedicated test database
4. **Handle errors**: Wrap cleanup in try/finally blocks

Example:
```python
def test_my_feature(pi_web_api_client, test_af_database):
    db_web_id = test_af_database["web_id"]
    element_data = {"Name": f"test_{int(time.time())}"}

    try:
        result = pi_web_api_client.asset_database.create_element(
            db_web_id, element_data
        )
        # ... test logic ...
    finally:
        # Cleanup
        if result and "WebId" in result:
            pi_web_api_client.element.delete(result["WebId"])
```
