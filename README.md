# PI Web API Python SDK

A modular Python SDK for interacting with the OSIsoft PI Web API. The codebase has been reorganised from a single monolithic module into a structured package that groups related controllers, configuration primitives, and the HTTP client.

## Project Description
pi_web_sdk delivers a consistently structured Python interface for AVEVA PI Web API deployments. It wraps the REST endpoints with typed controllers, rich client helpers, and practical defaults so you can query PI data, manage assets, and orchestrate analytics without hand-crafting HTTP calls. The package is organised for extensibility: add new controllers or override behaviours while keeping a cohesive developer experience.

## Features
- **Typed configuration** via `PIWebAPIConfig` and enums for authentication and WebID formats
- **Reusable HTTP client** `PIWebAPIClient` wrapper around `requests.Session` with centralised error handling
- **Domain-organized controllers** split by functionality (system, assets, data, streams, OMF, etc.) for easier navigation
- **Stream Updates** for incremental data retrieval without websockets (marker-based polling)
- **OMF support** with ORM-style API for creating types, containers, assets, and hierarchies
- **Comprehensive CRUD operations** for all major PI Web API endpoints
- **Backwards-compatible** `aveva_web_api.py` re-export for existing imports

## Installation
This project depends on `requests`. Install it with:

```bash
pip install requests
```

## Quick Start

### Basic Usage
```python
from pi_web_sdk import AuthMethod, PIWebAPIClient, PIWebAPIConfig

config = PIWebAPIConfig(
    base_url="https://your-pi-server/piwebapi",
    auth_method=AuthMethod.ANONYMOUS,
    verify_ssl=False,  # enable in production
)

client = PIWebAPIClient(config)
print(client.home.get())
```

### Working with Assets
```python
# List asset servers
servers = client.asset_server.list()
server_web_id = servers["Items"][0]["WebId"]

# Get databases
databases = client.asset_server.get_databases(server_web_id)
db_web_id = databases["Items"][0]["WebId"]

# Create an element
element = {
    "Name": "MyElement",
    "Description": "Test element",
    "TemplateName": "MyTemplate"
}
client.asset_database.create_element(db_web_id, element)
```

### Working with Streams
```python
# Get stream value
value = client.stream.get_value(stream_web_id)

# Get recorded data
recorded = client.stream.get_recorded(
    web_id=stream_web_id,
    start_time="*-7d",
    end_time="*",
    max_count=1000
)

# Update stream value
client.stream.update_value(
    web_id=stream_web_id,
    value={"Timestamp": "2024-01-01T00:00:00Z", "Value": 42.5}
)
```

### Stream Updates (Incremental Data Retrieval)
```python
import time

# Register for stream updates
registration = client.stream.register_update(stream_web_id)
marker = registration["LatestMarker"]

# Poll for incremental updates
while True:
    time.sleep(5)  # Wait between polls

    # Retrieve only new data since last marker
    updates = client.stream.retrieve_update(marker)

    for item in updates.get("Items", []):
        print(f"{item['Timestamp']}: {item['Value']}")

    # Update marker for next poll
    marker = updates["LatestMarker"]

# For multiple streams, use streamset
registration = client.streamset.register_updates([stream_id1, stream_id2, stream_id3])
marker = registration["LatestMarker"]

updates = client.streamset.retrieve_updates(marker)
for stream_update in updates.get("Items", []):
    stream_id = stream_update["WebId"]
    for item in stream_update.get("Items", []):
        print(f"Stream {stream_id}: {item['Timestamp']} = {item['Value']}")
```

See [examples/README_STREAM_UPDATES.md](examples/README_STREAM_UPDATES.md) for comprehensive Stream Updates documentation.

### OMF (OSIsoft Message Format) Support
```python
from pi_web_sdk.omf import OMFManager, create_temperature_sensor_type, create_single_asset

# Initialize OMF manager
omf_manager = OMFManager(client, data_server_web_id)

# Create a type definition
sensor_type = create_temperature_sensor_type()
omf_manager.create_type(sensor_type)

# Create a container
container = {"id": "sensor1", "typeId": "TempSensorType"}
omf_manager.create_container(container)

# Send data
data_point = {
    "timestamp": "2024-01-01T00:00:00Z",
    "temperature": 25.5
}
omf_manager.send_data("sensor1", data_point)
```

### OMF Hierarchies
```python
from pi_web_sdk.omf.hierarchy import create_industrial_hierarchy

# Create hierarchy from paths
hierarchy = create_industrial_hierarchy([
    "Plant/Area1/Line1",
    "Plant/Area1/Line2",
    "Plant/Area2/Line3"
])

# Deploy hierarchy
omf_manager.create_hierarchy_from_paths(hierarchy.get_all_paths())
```

## Available Controllers
All controller instances are available as attributes on `PIWebAPIClient`:

### System & Configuration
- `client.home` - Home endpoint
- `client.system` - System information and status
- `client.configuration` - System configuration

### Asset Model
- `client.asset_server` - Asset servers
- `client.asset_database` - Asset databases
- `client.element` - Elements
- `client.element_category` - Element categories
- `client.element_template` - Element templates
- `client.attribute` - Attributes
- `client.attribute_category` - Attribute categories
- `client.attribute_template` - Attribute templates

### Data & Streams
- `client.data_server` - Data servers
- `client.point` - PI Points
- `client.stream` - Stream data operations (including Stream Updates)
- `client.streamset` - Batch stream operations (including Stream Set Updates)

### Analysis & Events
- `client.analysis` - PI Analyses
- `client.event_frame` - Event frames
- `client.table` - PI Tables

### OMF
- `client.omf` - OSIsoft Message Format endpoint

### Batch & Advanced
- `client.batch` - Batch operations
- `client.calculation` - Calculations
- `client.channel` - Channels

### Supporting Resources
- `client.enumeration_set` - Enumeration sets
- `client.enumeration_value` - Enumeration values
- `client.unit` - Units of measure
- `client.time_rule` - Time rules
- `client.security` - Security operations
- `client.notification` - Notification rules
- `client.metrics` - System metrics

## Package Layout
- `pi_web_sdk/config.py` - Enums and configuration dataclass
- `pi_web_sdk/exceptions.py` - Custom exception types
- `pi_web_sdk/client.py` - Session management and HTTP helpers
- `pi_web_sdk/controllers/` - Individual controller modules grouped by domain
- `pi_web_sdk/omf/` - OMF support with ORM-style API
  - `omf/orm.py` - Core OMF classes (Type, Container, Asset, Data)
  - `omf/hierarchy.py` - Hierarchy builder utilities
- `aveva_web_api.py` - Compatibility shim for existing imports

## Extending the SDK
Each controller inherits from `BaseController`, which exposes helper methods and the configured client session. Add new endpoint support by:

1. Create a new controller module under `pi_web_sdk/controllers/`
2. Register it in `pi_web_sdk/controllers/__init__.py`
3. Add it to `pi_web_sdk/client.py` in the `PIWebAPIClient.__init__` method

Example:
```python
from .base import BaseController

class MyController(BaseController):
    def get(self, web_id: str) -> Dict:
        return self.client.get(f"myresource/{web_id}")
```

## Testing
Run the test suite:

```bash
# Run all tests
pytest tests/

# Run specific test files
pytest tests/test_omf_endpoint.py -v

# Run with integration marker
pytest -m integration
```

## Documentation & Examples
- [PI Web API Reference](https://docs.aveva.com/bundle/pi-web-api-reference/page/help/getting-started.html)
- [OMF Documentation](https://docs.aveva.com/)
- [Stream Updates Guide](examples/README_STREAM_UPDATES.md) - Comprehensive guide for incremental data retrieval
- [Stream Updates Examples](examples/stream_updates_example.py) - Working code examples
- See `examples/` directory for more usage examples

## Recent Additions

### Stream Updates (v2025.01)
Stream Updates provides an efficient way to retrieve incremental data updates without websockets. Key features:
- **Marker-based tracking** - Maintains position in data stream
- **Single or multiple streams** - Support for individual streams and stream sets
- **Metadata change detection** - Notifies when data is invalidated
- **Unit conversion** - Convert values during retrieval
- **Selected fields** - Filter response data

```python
# Register once
registration = client.stream.register_update(stream_web_id)
marker = registration["LatestMarker"]

# Poll repeatedly for new data only
while True:
    time.sleep(5)
    updates = client.stream.retrieve_update(marker)
    # Process updates["Items"]
    marker = updates["LatestMarker"]
```

**Requirements**: PI Web API 2019+ with Stream Updates feature enabled

See [examples/README_STREAM_UPDATES.md](examples/README_STREAM_UPDATES.md) for complete documentation.

## License
See LICENSE file for details.

