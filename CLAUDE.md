# PI Web API SDK - Development Guide

## Project Overview
This is a modular Python SDK for interacting with the OSIsoft/AVEVA PI Web API. The SDK provides a clean, typed interface to PI Web API REST endpoints with domain-organized controllers.

## Architecture

### Core Components
- **PIWebAPIClient** (`pi_web_sdk/client.py`) - Main HTTP client wrapper around requests.Session
- **PIWebAPIConfig** (`pi_web_sdk/config.py`) - Configuration dataclass with authentication and connection settings
- **BaseController** (`pi_web_sdk/controllers/base.py`) - Base class for all controllers with shared utilities

### Controller Organization
Controllers are organized by domain in `pi_web_sdk/controllers/`:

**System & Configuration**
- `system.py` - HomeController, SystemController, ConfigurationController

**Asset Model**
- `asset.py` - AssetServerController, AssetDatabaseController, ElementController, ElementCategoryController, ElementTemplateController
- `attribute.py` - AttributeController, AttributeCategoryController, AttributeTemplateController

**Data & Streams**
- `data.py` - DataServerController, PointController
- `stream.py` - StreamController, StreamSetController

**Analysis & Events**
- `analysis.py` - AnalysisController, AnalysisTemplateController, AnalysisCategoryController
- `event.py` - EventFrameController

**OMF Support**
- `omf.py` - OmfController for OSIsoft Message Format endpoints, OMFManager for high-level OMF operations
- `pi_web_sdk/models/omf.py` - OMF data models (OMFType, OMFContainer, OMFAsset, OMFTimeSeriesData, OMFBatch, OMFHierarchy, OMFHierarchyNode)

**Other Resources**
- `batch.py` - BatchController, CalculationController, ChannelController
- `table.py` - TableController, TableCategoryController
- `enumeration.py` - EnumerationSetController, EnumerationValueController
- `unit.py` - UnitController, UnitClassController
- `time_rule.py` - TimeRuleController, TimeRulePlugInController
- `security.py` - SecurityIdentityController, SecurityMappingController
- `notification.py` - NotificationRuleController, NotificationContactTemplateController
- `metrics.py` - MetricsController

## Development Guidelines

### Adding a New Controller
1. Create a new file in `pi_web_sdk/controllers/` (e.g., `myresource.py`)
2. Define controller class(es) inheriting from `BaseController`
3. Add to `__all__` export list in the controller file
4. Import and export in `pi_web_sdk/controllers/__init__.py`
5. Instantiate in `PIWebAPIClient.__init__` in `pi_web_sdk/client.py`
6. Write tests in `tests/`

Example controller structure:
```python
from __future__ import annotations
from typing import Dict, Optional
from .base import BaseController

__all__ = ['MyResourceController']

class MyResourceController(BaseController):
    """Controller for MyResource operations."""

    def get(self, web_id: str, selected_fields: Optional[str] = None) -> Dict:
        """Get resource by WebID."""
        params = {}
        if selected_fields:
            params["selectedFields"] = selected_fields
        return self.client.get(f"myresources/{web_id}", params=params)

    def get_by_path(self, path: str, selected_fields: Optional[str] = None) -> Dict:
        """Get resource by path."""
        params = {}
        if selected_fields:
            params["selectedFields"] = selected_fields
        return self.client.get(
            f"myresources/path/{self._encode_path(path)}", params=params
        )

    def update(self, web_id: str, resource: Dict) -> Dict:
        """Update a resource."""
        return self.client.patch(f"myresources/{web_id}", data=resource)

    def delete(self, web_id: str) -> Dict:
        """Delete a resource."""
        return self.client.delete(f"myresources/{web_id}")
```

### Common Patterns

**Path Encoding**
Always use `self._encode_path(path)` when dealing with path parameters to properly URL-encode them:
```python
# Correct
f"elements/path/{self._encode_path(path)}"

# Incorrect
f"elements?path={path}"
f"elements/path/{path}"
```

**Query Parameters**
Build params dict conditionally and pass to HTTP methods:
```python
params = {}
if name_filter:
    params["nameFilter"] = name_filter
if selected_fields:
    params["selectedFields"] = selected_fields
return self.client.get(f"elements/{web_id}/attributes", params=params)
```

**Pagination**
Include standard pagination parameters:
```python
def get_items(
    self,
    web_id: str,
    start_index: int = 0,
    max_count: int = 1000,
    name_filter: Optional[str] = None,
) -> Dict:
    params = {
        "startIndex": start_index,
        "maxCount": max_count,
    }
    if name_filter:
        params["nameFilter"] = name_filter
    return self.client.get(f"resources/{web_id}/items", params=params)
```

**CRUD Methods**
Standard naming for CRUD operations:
- `get(web_id)` - Get by WebID
- `get_by_path(path)` - Get by path
- `get_by_name(name)` - Get by name (for top-level resources)
- `update(web_id, data)` - Update resource
- `delete(web_id)` - Delete resource
- `create_*(parent_web_id, data)` - Create child resource

### OMF Development

**OMF ORM Usage**
```python
from pi_web_sdk.models.omf import OMFType, OMFContainer, OMFAsset
from pi_web_sdk.controllers.omf import OMFManager

# Define type
type_def = OMFType(
    id="TempSensorType",
    classification="dynamic",
    type="object",
    properties=[
        OMFProperty("timestamp", "string", is_index=True, format="date-time"),
        OMFProperty("temperature", "number", name="Temperature")
    ]
)

# Create container
container = OMFContainer(id="sensor1", type_id="TempSensorType")

# Create asset
asset = OMFAsset(id="asset1", name="Sensor 1", asset_type_id="AssetType")

# Use manager to send
manager = OMFManager(client, data_server_web_id)
manager.create_type(type_def)
manager.create_container(container)
manager.create_asset(asset)
```

**OMF Hierarchy Usage**
```python
from pi_web_sdk.models.omf import create_industrial_hierarchy
from pi_web_sdk.controllers.omf import OMFManager

# Build hierarchy from paths
hierarchy = create_industrial_hierarchy([
    "Plant/Area1/Line1",
    "Plant/Area1/Line2",
    "Plant/Area2/Line3"
])

# Get all nodes for asset creation
nodes = hierarchy.get_all_nodes()

# Deploy via OMF
manager.create_hierarchy_from_paths(hierarchy.get_all_paths())
```

## Testing

### Test Organization
- `tests/test_*_controller.py` - Unit tests with mocked responses
- `tests/test_*_live.py` - Integration tests (marked with `@pytest.mark.integration`)
- `tests/test_omf_*.py` - OMF-specific tests
- `tests/conftest.py` - Shared fixtures

### Running Tests
```bash
# All tests
pytest tests/

# Specific test file
pytest tests/test_omf_endpoint.py -v

# Integration tests only (requires live PI server)
pytest -m integration

# Skip integration tests
pytest -m "not integration"
```

### Test Patterns
```python
import pytest
from unittest.mock import MagicMock, patch

def test_controller_method(mock_client):
    """Test description."""
    # Arrange
    mock_client.get.return_value = {"WebId": "123", "Name": "Test"}
    controller = MyResourceController(mock_client)

    # Act
    result = controller.get("123")

    # Assert
    assert result["WebId"] == "123"
    mock_client.get.assert_called_once_with("myresources/123", params={})
```

## Code Style

### Imports
```python
from __future__ import annotations  # Always first

from typing import Dict, List, Optional  # Standard library

from .base import BaseController  # Local imports
```

### Type Hints
Use type hints for all method signatures:
```python
def get(self, web_id: str, selected_fields: Optional[str] = None) -> Dict:
    """Get resource by WebID."""
    pass
```

### Docstrings
Use simple docstrings that describe what the method does:
```python
def get_elements(self, web_id: str, name_filter: Optional[str] = None) -> Dict:
    """Get elements from asset database.

    Args:
        web_id: WebID of the asset database
        name_filter: Optional name filter pattern

    Returns:
        Dictionary containing Items array with element data
    """
```

## Known Issues & Gotchas

1. **Path encoding** - Always use `_encode_path()` for path parameters
2. **Element.get_by_path** - Fixed to use `elements/path/{encoded}` instead of `elements?path={path}`
3. **WebID format** - PI Web API returns WebID field, not webid or WebId
4. **Pagination** - Default maxCount is 1000, adjust as needed for large datasets
5. **OMF headers** - OMF requires specific headers (messagetype, omfversion, action)
6. **Integration tests** - Some tests are skipped by default, require live PI server

## Future Improvements

- [ ] Add retry logic with exponential backoff
- [ ] Implement async/await support
- [ ] Add response model classes for type safety
- [ ] Generate API documentation from docstrings
- [ ] Add more OMF convenience methods
- [ ] Support for PI Web API 2021+ features
- [ ] Add logging throughout the SDK
- [ ] Implement connection pooling configuration
- [ ] Add examples for common use cases

## Resources

- [PI Web API Reference](https://docs.aveva.com/bundle/pi-web-api-reference/page/help/getting-started.html)
- [OMF Documentation](https://docs.aveva.com/)
- [PI Web API REST Endpoints](https://docs.aveva.com/bundle/pi-web-api-reference/page/help/topics/home.html)
