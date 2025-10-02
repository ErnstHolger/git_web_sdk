"""
Example demonstrating OMF ORM system usage with PI Web API.

This example shows how to use the dataclass-based ORM system for OMF operations:
1. Type-safe OMF type definitions
2. Easy container and asset creation
3. Streamlined data ingestion
4. Batch operations
5. Convenience methods for common scenarios
"""

import time
from datetime import datetime, timezone
from pi_web_sdk import PIWebAPIClient, AuthenticationMethod
from pi_web_sdk.omf import (
    OMFManager, OMFType, OMFProperty, OMFContainer, OMFAsset,
    OMFTimeSeriesData, OMFBatch, Classification, PropertyType,
    create_temperature_sensor_type, create_equipment_asset_type
)

# Configure client
client = PIWebAPIClient(
    base_url="https://your-pi-web-api-server/piwebapi",
    auth_method=AuthenticationMethod.BASIC,
    username="your_username",
    password="your_password",
    verify_ssl=False
)

def demonstrate_basic_orm_usage():
    """Demonstrate basic OMF ORM usage."""
    print("=== Basic OMF ORM Usage ===")
    
    # Create OMF manager (auto-detects data server)
    omf_manager = OMFManager(client)
    print(f"Using data server: {omf_manager.get_data_server_info()['Name']}")
    
    timestamp = int(time.time())
    
    # 1. Create sensor type using convenience function
    print("\\n1. Creating sensor type...")
    sensor_type = create_temperature_sensor_type(f"ORMSensor_{timestamp}")
    omf_manager.create_type(sensor_type)
    print(f"Created sensor type: {sensor_type.id}")
    
    # 2. Create container using ORM
    print("\\n2. Creating container...")
    container = OMFContainer(
        id=f"ORMContainer_{timestamp}",
        type_id=sensor_type.id,
        name=f"ORM Demo Sensor {timestamp}",
        description="Sensor created using OMF ORM system",
        tags={"department": "demo", "location": "building_a"},
        metadata={"sensor_model": "ORM-3000", "firmware": "1.2.3"}
    )
    omf_manager.create_container(container)
    print(f"Created container: {container.id}")
    
    # 3. Send data using convenience method
    print("\\n3. Sending sensor data...")
    omf_manager.send_single_data_point(
        container.id,
        temperature=22.5,
        humidity=55.0,
        quality="Good"
    )
    print("Sent single data point")
    
    # 4. Send multiple data points
    data_points = [
        {"temperature": 23.0, "humidity": 54.0, "quality": "Good"},
        {"temperature": 23.5, "humidity": 53.0, "quality": "Good"},
        {"temperature": 24.0, "humidity": 52.0, "quality": "Good"}
    ]
    omf_manager.send_sensor_data(container.id, data_points)
    print(f"Sent {len(data_points)} additional data points")


def demonstrate_custom_types():
    """Demonstrate creating custom OMF types."""
    print("\\n=== Custom OMF Types ===")
    
    omf_manager = OMFManager(client)
    timestamp = int(time.time())
    
    # Create custom dynamic type for industrial equipment
    print("\\n1. Creating custom dynamic type...")
    equipment_properties = {
        "pressure": OMFProperty(
            type=PropertyType.NUMBER,
            description="Operating pressure in PSI"
        ),
        "flow_rate": OMFProperty(
            type=PropertyType.NUMBER,
            description="Flow rate in GPM"
        ),
        "status": OMFProperty(
            type=PropertyType.STRING,
            description="Equipment operational status"
        ),
        "alarm_count": OMFProperty(
            type=PropertyType.INTEGER,
            description="Active alarm count"
        ),
        "is_running": OMFProperty(
            type=PropertyType.BOOLEAN,
            description="Whether equipment is running"
        )
    }
    
    pump_type = OMFType.create_dynamic_type(
        id=f"IndustrialPump_{timestamp}",
        additional_properties=equipment_properties,
        description="Industrial pump with pressure and flow monitoring"
    )
    
    omf_manager.create_type(pump_type)
    print(f"Created custom pump type: {pump_type.id}")
    
    # Create custom static type for maintenance records
    print("\\n2. Creating custom static type...")
    maintenance_properties = {
        "last_service_date": OMFProperty(
            type=PropertyType.STRING,
            format="date-time",
            description="Last maintenance service date"
        ),
        "service_hours": OMFProperty(
            type=PropertyType.NUMBER,
            description="Total service hours"
        ),
        "maintenance_level": OMFProperty(
            type=PropertyType.STRING,
            description="Required maintenance level"
        ),
        "technician_id": OMFProperty(
            type=PropertyType.STRING,
            description="Last service technician ID"
        )
    }
    
    maintenance_type = OMFType.create_static_type(
        id=f"MaintenanceRecord_{timestamp}",
        additional_properties=maintenance_properties,
        description="Equipment maintenance tracking"
    )
    
    omf_manager.create_type(maintenance_type)
    print(f"Created maintenance type: {maintenance_type.id}")
    
    return pump_type, maintenance_type


def demonstrate_asset_creation():
    """Demonstrate asset creation with ORM."""
    print("\\n=== Asset Creation ===")
    
    omf_manager = OMFManager(client)
    timestamp = int(time.time())
    
    # Create equipment asset type
    print("\\n1. Creating equipment asset type...")
    asset_type = create_equipment_asset_type(f"ORMEquipment_{timestamp}")
    omf_manager.create_type(asset_type)
    
    # Create multiple assets using different methods
    print("\\n2. Creating assets...")
    
    # Method 1: Single asset with helper
    asset1 = OMFAsset.create_single_asset(
        type_id=asset_type.id,
        name=f"Pump_001_{timestamp}",
        location="Building A - Mechanical Room",
        manufacturer="Industrial Pumps Inc",
        model="IP-5000",
        serialNumber=f"SN{timestamp}001",
        installDate=datetime.now(timezone.utc).isoformat()
    )
    omf_manager.create_asset(asset1)
    print("Created asset: Pump_001")
    
    # Method 2: Multiple assets in batch
    asset2 = OMFAsset(
        type_id=asset_type.id,
        values=[
            {
                "name": f"Pump_002_{timestamp}",
                "location": "Building B - Utility Room",
                "manufacturer": "Industrial Pumps Inc",
                "model": "IP-5000",
                "serialNumber": f"SN{timestamp}002",
                "installDate": datetime.now(timezone.utc).isoformat()
            },
            {
                "name": f"Pump_003_{timestamp}",
                "location": "Building C - Basement",
                "manufacturer": "Industrial Pumps Inc", 
                "model": "IP-7000",
                "serialNumber": f"SN{timestamp}003",
                "installDate": datetime.now(timezone.utc).isoformat()
            }
        ]
    )
    omf_manager.create_asset(asset2)
    print("Created assets: Pump_002, Pump_003")


def demonstrate_time_series_operations():
    """Demonstrate time series data operations."""
    print("\\n=== Time Series Operations ===")
    
    omf_manager = OMFManager(client)
    timestamp = int(time.time())
    
    # Setup sensor
    sensor_type = create_temperature_sensor_type(f"TSDemo_{timestamp}")
    omf_manager.create_type(sensor_type)
    
    container = OMFContainer(
        id=f"TSContainer_{timestamp}",
        type_id=sensor_type.id,
        name=f"Time Series Demo {timestamp}"
    )
    omf_manager.create_container(container)
    
    # Method 1: Create and add data points manually
    print("\\n1. Manual time series creation...")
    ts_data = OMFTimeSeriesData(
        container_id=container.id,
        values=[]
    )
    
    # Add individual data points
    ts_data.add_data_point(temperature=20.0, humidity=60.0, quality="Good")
    ts_data.add_data_point(temperature=20.5, humidity=59.5, quality="Good")
    ts_data.add_data_point(temperature=21.0, humidity=59.0, quality="Good")
    
    # Add batch of data points
    additional_points = [
        {"temperature": 21.5, "humidity": 58.5, "quality": "Good"},
        {"temperature": 22.0, "humidity": 58.0, "quality": "Good"}
    ]
    ts_data.add_data_points(additional_points)
    
    omf_manager.send_time_series_data(ts_data)
    print(f"Sent {len(ts_data.values)} data points manually")
    
    # Method 2: Use convenience methods
    print("\\n2. Using convenience methods...")
    
    # Send single point
    omf_manager.send_single_data_point(
        container.id,
        temperature=22.5,
        humidity=57.5,
        quality="Good"
    )
    
    # Send multiple points
    more_points = [
        {"temperature": 23.0, "humidity": 57.0, "quality": "Good"},
        {"temperature": 23.5, "humidity": 56.5, "quality": "Good"}
    ]
    omf_manager.send_sensor_data(container.id, more_points)
    print("Sent additional points using convenience methods")


def demonstrate_batch_operations():
    """Demonstrate batch operations."""
    print("\\n=== Batch Operations ===")
    
    omf_manager = OMFManager(client)
    timestamp = int(time.time())
    
    # Create batch
    batch = OMFBatch()
    
    print("\\n1. Creating batch with multiple types...")
    
    # Add multiple sensor types
    sensor_type1 = create_temperature_sensor_type(f"BatchSensor1_{timestamp}")
    sensor_type2 = create_temperature_sensor_type(f"BatchSensor2_{timestamp}")
    asset_type = create_equipment_asset_type(f"BatchAsset_{timestamp}")
    
    batch.add_type(sensor_type1)
    batch.add_type(sensor_type2)
    batch.add_type(asset_type)
    
    # Add containers
    container1 = OMFContainer(
        id=f"BatchContainer1_{timestamp}",
        type_id=sensor_type1.id,
        name=f"Batch Sensor 1 {timestamp}"
    )
    container2 = OMFContainer(
        id=f"BatchContainer2_{timestamp}",
        type_id=sensor_type2.id,
        name=f"Batch Sensor 2 {timestamp}"
    )
    
    batch.add_container(container1)
    batch.add_container(container2)
    
    # Add asset
    asset = OMFAsset.create_single_asset(
        type_id=asset_type.id,
        name=f"BatchEquipment_{timestamp}",
        location="Batch Processing Area",
        manufacturer="Batch Corp",
        model="BC-1000",
        serialNumber=f"BC{timestamp}"
    )
    batch.add_asset(asset)
    
    # Add time series data
    ts_data1 = OMFTimeSeriesData(
        container_id=container1.id,
        values=[{"temperature": 25.0, "humidity": 50.0, "quality": "Good"}]
    )
    ts_data2 = OMFTimeSeriesData(
        container_id=container2.id,
        values=[{"temperature": 24.5, "humidity": 51.0, "quality": "Good"}]
    )
    
    batch.add_time_series(ts_data1)
    batch.add_time_series(ts_data2)
    
    print(f"Batch contains: {len(batch.types)} types, {len(batch.containers)} containers, "
          f"{len(batch.assets)} assets, {len(batch.time_series)} time series")
    
    # Send batch
    print("\\n2. Sending batch...")
    results = omf_manager.send_batch(batch)
    
    print("Batch sent successfully!")
    print(f"Results: {list(results.keys())}")


def demonstrate_complete_workflow():
    """Demonstrate complete sensor setup workflow."""
    print("\\n=== Complete Workflow ===")
    
    omf_manager = OMFManager(client)
    timestamp = int(time.time())
    
    print("\\n1. Complete sensor setup in one call...")
    
    # Create sensor type
    sensor_type = create_temperature_sensor_type(f"WorkflowSensor_{timestamp}")
    
    # Use complete setup method
    results = omf_manager.create_complete_sensor_setup(
        sensor_id=f"CompleteWorkflow_{timestamp}",
        sensor_name=f"Complete Workflow Sensor {timestamp}",
        sensor_type=sensor_type,
        initial_data=[
            {"temperature": 21.0, "humidity": 45.0, "quality": "Good"},
            {"temperature": 21.5, "humidity": 44.5, "quality": "Good"},
            {"temperature": 22.0, "humidity": 44.0, "quality": "Good"}
        ]
    )
    
    print("Complete setup finished!")
    print(f"Operations completed: {list(results.keys())}")
    
    # Continue sending data
    print("\\n2. Continuing to send data...")
    for i in range(3):
        omf_manager.send_single_data_point(
            f"CompleteWorkflow_{timestamp}",
            temperature=22.5 + i * 0.5,
            humidity=43.5 - i * 0.5,
            quality="Good"
        )
        time.sleep(0.1)  # Small delay between points
    
    print("Workflow complete!")


def main():
    """Run all OMF ORM demonstrations."""
    print("OMF ORM System Demonstration")
    print("=" * 50)
    
    try:
        demonstrate_basic_orm_usage()
        demonstrate_custom_types()
        demonstrate_asset_creation()
        demonstrate_time_series_operations()
        demonstrate_batch_operations()
        demonstrate_complete_workflow()
        
        print("\\n" + "=" * 50)
        print("All demonstrations completed successfully!")
        print("\\nKey benefits of OMF ORM:")
        print("- Type safety with dataclasses")
        print("- Automatic validation")
        print("- Convenient helper methods")
        print("- Batch operations support")
        print("- Clear, readable code")
        
    except Exception as e:
        print(f"\\nError: {e}")
        print("\\nMake sure to update the connection details:")
        print("- base_url: Your PI Web API server URL")
        print("- username/password: Valid credentials")
        print("- Ensure OMF endpoint is enabled")


if __name__ == "__main__":
    main()