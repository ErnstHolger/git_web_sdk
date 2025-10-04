"""Example: Creating element hierarchies in PI using OMF."""

from pi_web_sdk import PIWebAPIClient, PIWebAPIConfig
from pi_web_sdk.models.omf import create_hierarchy_from_paths, create_industrial_hierarchy
from pi_web_sdk.controllers.omf import OMFManager
import time

# Configure client
config = PIWebAPIConfig(
    base_url="https://your-pi-server/piwebapi",
    username="your-username",
    password="your-password",
    verify_ssl=False
)

client = PIWebAPIClient(config)

# Get data server
servers = client.data_server.list()
data_server_web_id = servers["Items"][0]["WebId"]

# Create OMF manager
manager = OMFManager(client, data_server_web_id)

# Example 1: Create simple hierarchy from paths
print("Creating simple hierarchy from paths...")
paths = [
    "MyPlant/Area1/Line1/Sensor1",
    "MyPlant/Area1/Line1/Sensor2",
    "MyPlant/Area1/Line2/Sensor3",
    "MyPlant/Area2/Line3/Sensor4",
]

results = manager.create_hierarchy_from_paths(
    paths=paths,
    root_type_id="ContainerType",
    leaf_type_id="SensorType",
    create_types=True  # This will create types AND assets
)

print(f"Types created: {len(results['types_created'])}")
print(f"Assets created: {sum(a['count'] for a in results['assets_created'])}")

# Give PI time to process
time.sleep(2)

# Example 2: Create industrial hierarchy
print("\nCreating industrial hierarchy...")
results = manager.create_industrial_hierarchy(
    plants=["PlantA", "PlantB"],
    units_per_plant={
        "PlantA": ["Boiler", "Turbine"],
        "PlantB": ["Reactor", "Cooler"]
    },
    sensors_per_unit={
        "Boiler": ["TempSensor", "PressureSensor"],
        "Turbine": ["SpeedSensor", "VibrationSensor"],
        "Reactor": ["TempSensor", "LevelSensor"],
        "Cooler": ["TempSensor", "FlowSensor"]
    },
    create_types=True
)

print(f"Types created: {len(results['types_created'])}")
print(f"Assets created: {sum(a['count'] for a in results['assets_created'])}")

# Verify elements in PI Asset Framework
print("\nVerifying elements in PI...")
asset_servers = client.asset_server.list()
if asset_servers["Items"]:
    asset_server = asset_servers["Items"][0]
    dbs = client.asset_server.get_databases(asset_server["WebId"])

    for db in dbs.get("Items", []):
        elements = client.asset_database.get_elements(
            db["WebId"],
            name_filter="MyPlant"  # Or "PlantA" for example 2
        )
        if elements.get("Items"):
            print(f"Found elements in database '{db['Name']}':")
            for elem in elements["Items"]:
                print(f"  - {elem['Name']} (WebId: {elem['WebId']})")
