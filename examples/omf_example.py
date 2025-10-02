"""
Example demonstrating OMF (OCS Message Format) usage with PI Web API.

This example shows how to:
1. Create OMF types for dynamic data and static assets
2. Create containers (streams) for time series data
3. Create assets with metadata
4. Send time series data to streams
"""

import time
from datetime import datetime, timezone
from pi_web_sdk import PIWebAPIClient, AuthenticationMethod

# Configure client
client = PIWebAPIClient(
    base_url="https://your-pi-web-api-server/piwebapi",
    auth_method=AuthenticationMethod.BASIC,
    username="your_username",
    password="your_password",
    verify_ssl=False
)

def main():
    """Demonstrate complete OMF workflow."""
    
    # Get first available data server
    servers = client.data_server.list().get("Items", [])
    if not servers:
        print("No data servers available")
        return
    
    data_server_web_id = servers[0]["WebId"]
    print(f"Using data server: {servers[0]['Name']}")
    
    timestamp = int(time.time())
    
    # Step 1: Create dynamic type for sensor data
    print("Creating OMF dynamic type...")
    sensor_type = {
        "id": f"TemperatureSensor_{timestamp}",
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
                "description": "Temperature in Celsius"
            },
            "humidity": {
                "type": "number", 
                "description": "Relative humidity percentage"
            },
            "quality": {
                "type": "string",
                "description": "Data quality indicator"
            }
        }
    }
    
    response = client.omf.post_async(
        data=[sensor_type],
        message_type="Type",
        omf_version="1.2",
        action="create",
        data_server_web_id=data_server_web_id
    )
    print("Dynamic type created successfully")
    
    # Step 2: Create static type for equipment assets
    print("Creating OMF static type...")
    equipment_type = {
        "id": f"EquipmentAsset_{timestamp}",
        "type": "object",
        "classification": "static",
        "properties": {
            "name": {
                "type": "string",
                "isindex": True
            },
            "location": {
                "type": "string",
                "description": "Physical location"
            },
            "manufacturer": {
                "type": "string",
                "description": "Equipment manufacturer"
            },
            "model": {
                "type": "string",
                "description": "Equipment model"
            },
            "serialNumber": {
                "type": "string",
                "description": "Serial number"
            },
            "installDate": {
                "type": "string",
                "format": "date-time",
                "description": "Installation date"
            }
        }
    }
    
    response = client.omf.post_async(
        data=[equipment_type],
        message_type="Type",
        omf_version="1.2",
        action="create",
        data_server_web_id=data_server_web_id
    )
    print("Static type created successfully")
    
    # Step 3: Create container (stream) for sensor data
    print("Creating OMF container...")
    container = {
        "id": f"RoomSensor_{timestamp}",
        "typeid": f"TemperatureSensor_{timestamp}",
        "name": f"Room Temperature Sensor {timestamp}",
        "description": "Temperature and humidity sensor in conference room"
    }
    
    response = client.omf.post_async(
        data=[container],
        message_type="Container",
        omf_version="1.2",
        action="create",
        data_server_web_id=data_server_web_id
    )
    print("Container created successfully")
    
    # Step 4: Create equipment asset
    print("Creating OMF asset...")
    asset = {
        "typeid": f"EquipmentAsset_{timestamp}",
        "values": [
            {
                "name": f"ConferenceRoomSensor_{timestamp}",
                "location": "Building A - Conference Room 1",
                "manufacturer": "SensorTech Corp",
                "model": "TH-3000",
                "serialNumber": f"SN{timestamp}",
                "installDate": datetime.now(timezone.utc).isoformat()
            }
        ]
    }
    
    response = client.omf.post_async(
        data=[asset],
        message_type="Data",
        omf_version="1.2",
        action="create",
        data_server_web_id=data_server_web_id
    )
    print("Asset created successfully")
    
    # Step 5: Send time series data
    print("Sending time series data...")
    
    # Generate some sample data points
    current_time = datetime.now(timezone.utc)
    data_points = []
    
    for i in range(5):
        # Simulate sensor readings over time
        timestamp_str = current_time.replace(
            second=current_time.second + i
        ).isoformat()
        
        data_points.append({
            "timestamp": timestamp_str,
            "temperature": 22.0 + (i * 0.5),  # Gradually increasing temperature
            "humidity": 45.0 + (i * 2.0),     # Gradually increasing humidity
            "quality": "Good"
        })
    
    ts_data = {
        "containerid": f"RoomSensor_{timestamp}",
        "values": data_points
    }
    
    response = client.omf.post_async(
        data=[ts_data],
        message_type="Data",
        omf_version="1.2",
        action="create",
        data_server_web_id=data_server_web_id
    )
    print(f"Successfully sent {len(data_points)} data points")
    
    # Step 6: Send additional batch of data
    print("Sending additional data batch...")
    
    # Simulate real-time updates
    time.sleep(1)
    
    batch_data = {
        "containerid": f"RoomSensor_{timestamp}",
        "values": [
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "temperature": 24.5,
                "humidity": 52.0,
                "quality": "Good"
            }
        ]
    }
    
    response = client.omf.post_async(
        data=[batch_data],
        message_type="Data",
        omf_version="1.2",
        action="create",
        data_server_web_id=data_server_web_id
    )
    print("Additional data sent successfully")
    
    print("\\nOMF workflow completed!")
    print(f"Created assets and streams with identifier: {timestamp}")
    print("\\nSummary:")
    print(f"- Dynamic type: TemperatureSensor_{timestamp}")
    print(f"- Static type: EquipmentAsset_{timestamp}")
    print(f"- Container: RoomSensor_{timestamp}")
    print(f"- Asset: ConferenceRoomSensor_{timestamp}")
    print(f"- Data points sent: {len(data_points) + 1}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error: {e}")
        print("\\nMake sure to update the connection details in the script:")
        print("- base_url: Your PI Web API server URL")
        print("- username/password: Valid credentials")
        print("- Verify the OMF endpoint is enabled on your PI Web API server")