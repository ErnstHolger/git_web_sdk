# OMF ORM System

A comprehensive Object-Relational Mapping (ORM) system for OMF (OCS Message Format) operations with PI Web API, built using Python dataclasses for type safety and developer productivity.

## Features

- **Type-Safe Operations**: Using Python dataclasses with automatic validation
- **Intuitive API**: Object-oriented approach to OMF operations
- **Automatic Validation**: Built-in validation for OMF message structures
- **Convenience Methods**: Helper functions for common scenarios
- **Batch Operations**: Efficient batch processing of multiple OMF messages
- **Manager Pattern**: High-level OMF manager for streamlined operations

## Quick Start

```python
from pi_web_sdk import PIWebAPIClient, AuthenticationMethod
from pi_web_sdk.omf import OMFManager, create_temperature_sensor_type

# Initialize client and OMF manager
client = PIWebAPIClient(
    base_url="https://your-server/piwebapi",
    auth_method=AuthenticationMethod.BASIC,
    username="username",
    password="password"
)

omf_manager = OMFManager(client)

# Create a temperature sensor type
sensor_type = create_temperature_sensor_type("TempSensor_001")
omf_manager.create_type(sensor_type)

# Send a data point
omf_manager.send_single_data_point(
    "sensor_container_id",
    temperature=25.5,
    humidity=60.0,
    quality="Good"
)
```

## Core Components

### 1. OMF Models (`models.py`)

#### OMFType
Represents OMF type definitions with automatic validation:

```python
from pi_web_sdk.omf import OMFType, OMFProperty, PropertyType, Classification

# Create dynamic type for sensors
sensor_type = OMFType.create_dynamic_type(
    id="MySensorType",
    additional_properties={
        "temperature": OMFProperty(
            type=PropertyType.NUMBER,
            description="Temperature in Celsius"
        ),
        "pressure": OMFProperty(
            type=PropertyType.NUMBER,
            description="Pressure in Pa"
        )
    },
    description="Custom sensor type"
)

# Create static type for assets
asset_type = OMFType.create_static_type(
    id="MyAssetType",
    additional_properties={
        "location": OMFProperty(
            type=PropertyType.STRING,
            description="Asset location"
        ),
        "model": OMFProperty(
            type=PropertyType.STRING,
            description="Equipment model"
        )
    }
)
```

#### OMFContainer
Represents data streams/containers:

```python
from pi_web_sdk.omf import OMFContainer

container = OMFContainer(
    id="sensor_stream_001",
    type_id="MySensorType",
    name="Temperature Sensor Stream",
    description="Primary temperature monitoring",
    tags={"location": "building_a", "floor": "2"},
    metadata={"sensor_model": "TH-3000"}
)
```

#### OMFAsset
Represents static assets:

```python
from pi_web_sdk.omf import OMFAsset

# Single asset
asset = OMFAsset.create_single_asset(
    type_id="MyAssetType",
    name="Pump_001",
    location="Mechanical Room",
    model="XYZ-5000",
    serialNumber="SN123456"
)

# Multiple assets
multi_asset = OMFAsset(
    type_id="MyAssetType",
    values=[
        {"name": "Pump_001", "location": "Room A", "model": "XYZ-5000"},
        {"name": "Pump_002", "location": "Room B", "model": "XYZ-5000"}
    ]
)
```

#### OMFTimeSeriesData
Represents time series data:

```python
from pi_web_sdk.omf import OMFTimeSeriesData

ts_data = OMFTimeSeriesData(
    container_id="sensor_stream_001",
    values=[]
)

# Add data points
ts_data.add_data_point(temperature=25.5, pressure=101325.0, quality="Good")
ts_data.add_data_points([
    {"temperature": 26.0, "pressure": 101300.0, "quality": "Good"},
    {"temperature": 26.5, "pressure": 101280.0, "quality": "Good"}
])
```

### 2. OMF Manager (`manager.py`)

High-level manager for OMF operations:

```python
from pi_web_sdk.omf import OMFManager

# Create manager (auto-detects data server)
omf_manager = OMFManager(client)

# Or specify data server
omf_manager = OMFManager(client, data_server_web_id="specific_server_id")

# Complete sensor setup in one call
results = omf_manager.create_complete_sensor_setup(
    sensor_id="complete_sensor_001",
    sensor_name="Complete Temperature Sensor",
    sensor_type=sensor_type,
    initial_data=[
        {"temperature": 25.0, "humidity": 50.0, "quality": "Good"}
    ]
)

# Convenience methods
omf_manager.send_single_data_point(
    "sensor_id",
    temperature=25.5,
    humidity=60.0
)

omf_manager.send_sensor_data("sensor_id", [
    {"temperature": 26.0, "humidity": 59.0},
    {"temperature": 26.5, "humidity": 58.0}
])
```

### 3. Batch Operations

Efficient batch processing:

```python
from pi_web_sdk.omf import OMFBatch

batch = OMFBatch()

# Add multiple types, containers, assets, and data
batch.add_type(sensor_type)
batch.add_type(asset_type)
batch.add_container(container)
batch.add_asset(asset)
batch.add_time_series(ts_data)

# Send entire batch
results = omf_manager.send_batch(batch)
```

## Property Types

Supported OMF property types via `PropertyType` enum:

- `PropertyType.STRING` - Text values
- `PropertyType.NUMBER` - Floating point numbers
- `PropertyType.INTEGER` - Integer values  
- `PropertyType.BOOLEAN` - True/false values
- `PropertyType.ARRAY` - Array values
- `PropertyType.OBJECT` - Object values

## Property Attributes

OMF properties support these attributes:

- `type`: Property data type (required)
- `description`: Human-readable description
- `format`: Format specification (e.g., "date-time")
- `is_index`: Mark as index property for type
- `is_name`: Mark as name property for type

## Validation Features

### Type Validation
- Dynamic types must have at least one index property
- Static types must have at least one index or name property
- Automatic timestamp property for dynamic types
- Automatic name property for static types

### Data Validation
- Automatic timestamp generation for time series data
- Property type checking
- Required field validation

## Convenience Functions

Pre-built type creators for common scenarios:

```python
from pi_web_sdk.omf import (
    create_temperature_sensor_type,
    create_equipment_asset_type,
    create_sensor_type,
    create_equipment_type
)

# Standard temperature sensor
temp_sensor = create_temperature_sensor_type("TempSensor_001")

# Standard equipment asset
equipment = create_equipment_asset_type("Equipment_001")

# Custom sensor type
custom_sensor = create_sensor_type(
    "CustomSensor_001",
    sensor_properties={
        "vibration": OMFProperty(
            type=PropertyType.NUMBER,
            description="Vibration level"
        )
    }
)
```

## Error Handling

The ORM system provides clear error messages:

```python
try:
    # This will raise ValueError
    invalid_type = OMFType(
        id="InvalidType",
        classification=Classification.DYNAMIC,
        properties={}  # No index property
    )
except ValueError as e:
    print(f"Validation error: {e}")
```

## Advanced Usage

### Custom Property Types

```python
# Create complex property definitions
complex_properties = {
    "measurements": OMFProperty(
        type=PropertyType.OBJECT,
        description="Complex measurement object"
    ),
    "tags": OMFProperty(
        type=PropertyType.ARRAY,
        description="Array of tags"
    ),
    "calibration_date": OMFProperty(
        type=PropertyType.STRING,
        format="date-time",
        description="Last calibration date"
    )
}
```

### Metadata and Tags

```python
# Rich container metadata
container = OMFContainer(
    id="advanced_sensor",
    type_id="AdvancedSensorType",
    name="Advanced Monitoring Sensor",
    tags={
        "department": "operations",
        "criticality": "high",
        "location": "building_a_floor_2"
    },
    metadata={
        "installation_date": "2023-01-15",
        "warranty_expires": "2026-01-15",
        "maintenance_schedule": "quarterly",
        "vendor": "SensorTech Corp"
    }
)
```

### Performance Optimization

```python
# Use batches for better performance
batch = OMFBatch()

# Add multiple operations
for i in range(100):
    ts_data = OMFTimeSeriesData(f"sensor_{i}", data_points)
    batch.add_time_series(ts_data)

# Send all at once
results = omf_manager.send_batch(batch)
```

## Best Practices

1. **Use Type Factories**: Leverage convenience functions for standard types
2. **Batch Operations**: Use `OMFBatch` for multiple operations
3. **Validation First**: Let the ORM validate your data structures
4. **Meaningful Names**: Use descriptive IDs and names
5. **Rich Metadata**: Include comprehensive tags and metadata
6. **Error Handling**: Wrap operations in try-catch blocks

## Testing

The OMF ORM system includes comprehensive tests:

- **Unit Tests**: `tests/test_omf_orm.py` (25 tests)
- **Integration Tests**: `tests/test_omf_orm_integration.py` (10 tests)
- **Example Usage**: `examples/omf_orm_example.py`

Run tests:
```bash
# Unit tests
pytest tests/test_omf_orm.py -v

# Integration tests (requires PI Web API)
pytest tests/test_omf_orm_integration.py -v
```

## Migration from Raw OMF

### Before (Raw OMF)
```python
# Raw OMF type definition
type_def = {
    "id": "SensorType",
    "type": "object", 
    "classification": "dynamic",
    "properties": {
        "timestamp": {
            "type": "string",
            "format": "date-time",
            "isindex": True
        },
        "temperature": {
            "type": "number",
            "description": "Temperature value"
        }
    }
}

client.omf.post_async(
    data=[type_def],
    message_type="Type",
    omf_version="1.2",
    action="create",
    data_server_web_id=server_id
)
```

### After (OMF ORM)
```python
# Type-safe ORM approach
sensor_type = OMFType.create_dynamic_type(
    id="SensorType",
    additional_properties={
        "temperature": OMFProperty(
            type=PropertyType.NUMBER,
            description="Temperature value"
        )
    }
)

omf_manager.create_type(sensor_type)
```

## Benefits

- **Type Safety**: Compile-time checking with dataclasses
- **Validation**: Automatic validation of OMF message structure
- **Productivity**: Reduced boilerplate code
- **Maintainability**: Clear, readable code structure
- **Testing**: Easy to unit test with mock objects
- **Documentation**: Self-documenting with type hints