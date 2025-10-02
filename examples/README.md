# PI Web API SDK Examples

This directory contains practical examples demonstrating how to use the PI Web API SDK.

## OMF (OCS Message Format) Example

The `omf_example.py` demonstrates a complete OMF workflow:

### Features Demonstrated

1. **Type Creation**
   - Dynamic types for time series data
   - Static types for asset metadata

2. **Asset Creation**
   - Equipment assets with metadata
   - Location and installation information

3. **Stream Creation**
   - Data containers for time series
   - Sensor data streams

4. **Data Ingestion**
   - Sending time series data
   - Batch data operations

### Running the Example

1. Update connection details in `omf_example.py`:
   ```python
   client = PIWebAPIClient(
       base_url="https://your-pi-web-api-server/piwebapi",
       username="your_username",
       password="your_password"
   )
   ```

2. Run the example:
   ```bash
   python examples/omf_example.py
   ```

### Expected Output

```
Using data server: Your-PI-Server
Creating OMF dynamic type...
Dynamic type created successfully
Creating OMF static type...
Static type created successfully
Creating OMF container...
Container created successfully
Creating OMF asset...
Asset created successfully
Sending time series data...
Successfully sent 5 data points
Sending additional data batch...
Additional data sent successfully

OMF workflow completed!
Created assets and streams with identifier: 1759261234

Summary:
- Dynamic type: TemperatureSensor_1759261234
- Static type: EquipmentAsset_1759261234
- Container: RoomSensor_1759261234
- Asset: ConferenceRoomSensor_1759261234
- Data points sent: 6
```

### Prerequisites

- PI Web API server with OMF endpoint enabled
- Valid authentication credentials
- PI Data Archive server available

### OMF Message Types

The example demonstrates all three OMF message types:

- **Type**: Define data structure schemas
- **Container**: Create data streams/containers
- **Data**: Send actual time series data and asset information

### Notes

- Each run creates unique assets using timestamps
- The example includes error handling for common issues
- All data is sent to the first available data server
- OMF version 1.2 is used for compatibility