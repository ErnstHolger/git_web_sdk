"""Example: Creating element hierarchies using direct PI Web API element creation.

This example shows how to create hierarchies using the AssetDatabaseController
and ElementController methods directly (without OMF).
"""

from pi_web_sdk import PIWebAPIClient, PIWebAPIConfig
import time

# Configure client
config = PIWebAPIConfig(
    base_url="https://your-pi-server/piwebapi",
    username="your-username",
    password="your-password",
    verify_ssl=False
)

client = PIWebAPIClient(config)

# Get asset server and database
servers = client.asset_server.list()
if not servers.get("Items"):
    raise Exception("No asset servers found")

asset_server = servers["Items"][0]
print(f"Using Asset Server: {asset_server['Name']}")

# Get databases
dbs = client.asset_server.get_databases(asset_server["WebId"])
if not dbs.get("Items"):
    raise Exception("No databases found")

# Use first database (typically "Configuration" for test elements)
database = dbs["Items"][0]
db_web_id = database["WebId"]
print(f"Using Database: {database['Name']}")

# =============================================================================
# Example 1: Simple Parent-Child Hierarchy
# =============================================================================
print("\n" + "="*80)
print("Example 1: Creating Simple Parent-Child Hierarchy")
print("="*80)

# Create root element in database
plant_name = f"Plant_{int(time.time())}"
plant_element = client.asset_database.create_element(db_web_id, {
    "Name": plant_name,
    "Description": "Main manufacturing plant"
})
plant_web_id = plant_element["WebId"]
print(f"✓ Created: {plant_name} (WebId: {plant_web_id})")

# Create child element under plant
area_element = client.element.create_element(plant_web_id, {
    "Name": "ProductionArea",
    "Description": "Main production area"
})
area_web_id = area_element["WebId"]
print(f"  ✓ Created child: ProductionArea (WebId: {area_web_id})")

# Verify by reading back
retrieved_plant = client.element.get(plant_web_id)
print(f"\nVerified: {retrieved_plant['Name']}")
print(f"  Path: {retrieved_plant.get('Path', 'N/A')}")

# =============================================================================
# Example 2: Multi-Level Industrial Hierarchy
# =============================================================================
print("\n" + "="*80)
print("Example 2: Creating Multi-Level Industrial Hierarchy")
print("="*80)

# Plant -> Area -> Line -> Equipment -> Sensor
test_id = int(time.time())

# Level 1: Plant
plant2 = client.asset_database.create_element(db_web_id, {
    "Name": f"IndustrialPlant_{test_id}",
    "Description": "Industrial manufacturing facility"
})
print(f"Level 1: Plant - {plant2['WebId']}")

# Level 2: Areas
area1 = client.element.create_element(plant2["WebId"], {
    "Name": "Assembly_Area",
    "Description": "Assembly operations"
})
area2 = client.element.create_element(plant2["WebId"], {
    "Name": "Packaging_Area",
    "Description": "Packaging operations"
})
print(f"  Level 2: Areas - Created 2 areas")

# Level 3: Lines under Assembly Area
line1 = client.element.create_element(area1["WebId"], {
    "Name": "AssemblyLine_1",
    "Description": "Primary assembly line"
})
line2 = client.element.create_element(area1["WebId"], {
    "Name": "AssemblyLine_2",
    "Description": "Secondary assembly line"
})
print(f"    Level 3: Lines - Created 2 lines under Assembly_Area")

# Level 4: Equipment under Line 1
robot = client.element.create_element(line1["WebId"], {
    "Name": "Robot_A01",
    "Description": "Welding robot"
})
conveyor = client.element.create_element(line1["WebId"], {
    "Name": "Conveyor_B01",
    "Description": "Main conveyor belt"
})
print(f"      Level 4: Equipment - Created 2 pieces of equipment")

# Level 5: Sensors under Robot
temp_sensor = client.element.create_element(robot["WebId"], {
    "Name": "TempSensor_001",
    "Description": "Motor temperature sensor"
})
vibration_sensor = client.element.create_element(robot["WebId"], {
    "Name": "VibrationSensor_001",
    "Description": "Vibration monitor"
})
print(f"        Level 5: Sensors - Created 2 sensors under Robot_A01")

# =============================================================================
# Example 3: Creating Elements with Attributes
# =============================================================================
print("\n" + "="*80)
print("Example 3: Creating Element with Attributes")
print("="*80)

# Create equipment element
equipment = client.asset_database.create_element(db_web_id, {
    "Name": f"Pump_{test_id}",
    "Description": "Centrifugal pump",
})
equipment_web_id = equipment["WebId"]
print(f"✓ Created equipment: Pump_{test_id}")

# Add attributes to the equipment
# Note: For production, you'd typically use templates which pre-define attributes
attributes = [
    {
        "Name": "FlowRate",
        "Description": "Current flow rate",
        "Type": "Double"
    },
    {
        "Name": "Pressure",
        "Description": "Discharge pressure",
        "Type": "Double"
    },
    {
        "Name": "Status",
        "Description": "Pump operational status",
        "Type": "String"
    }
]

for attr_data in attributes:
    attr = client.element.create_attribute(equipment_web_id, attr_data)
    print(f"  ✓ Added attribute: {attr_data['Name']}")

# Verify attributes
attrs = client.element.get_attributes(equipment_web_id)
print(f"\nTotal attributes on {equipment['WebId']}: {len(attrs.get('Items', []))}")

# =============================================================================
# Example 4: Batch Creation - Complete Factory Hierarchy
# =============================================================================
print("\n" + "="*80)
print("Example 4: Batch Creating Complete Factory Hierarchy")
print("="*80)

factory_structure = {
    f"Factory_{test_id}": {
        "Warehouse": {
            "Zone_A": ["Shelf_1", "Shelf_2", "Shelf_3"],
            "Zone_B": ["Shelf_4", "Shelf_5"]
        },
        "Production": {
            "Line_1": ["Station_1", "Station_2", "Station_3"],
            "Line_2": ["Station_4", "Station_5"]
        },
        "QualityControl": {
            "Lab_1": ["Analyzer_1", "Analyzer_2"],
            "Lab_2": ["Microscope_1"]
        }
    }
}

def create_hierarchy(parent_web_id, structure, level=0):
    """Recursively create element hierarchy."""
    indent = "  " * level

    for name, children in structure.items():
        # Create element
        if level == 0:
            # Root level - create in database
            elem = client.asset_database.create_element(parent_web_id, {"Name": name})
        else:
            # Child level - create under parent element
            elem = client.element.create_element(parent_web_id, {"Name": name})

        elem_web_id = elem["WebId"]
        print(f"{indent}✓ {name}")

        # Process children
        if isinstance(children, dict):
            # Nested dictionary - recurse
            create_hierarchy(elem_web_id, children, level + 1)
        elif isinstance(children, list):
            # List of leaf nodes
            for child_name in children:
                child = client.element.create_element(elem_web_id, {"Name": child_name})
                print(f"{indent}  ✓ {child_name}")

create_hierarchy(db_web_id, factory_structure)

# =============================================================================
# Example 5: Querying Created Hierarchy
# =============================================================================
print("\n" + "="*80)
print("Example 5: Querying and Navigating Created Hierarchy")
print("="*80)

# Get all root elements we created
elements = client.asset_database.get_elements(
    db_web_id,
    name_filter=f"*_{test_id}*"
)

print(f"Found {len(elements.get('Items', []))} root elements matching *_{test_id}*:")
for elem in elements.get("Items", []):
    print(f"\n  {elem['Name']} ({elem['WebId']})")

    # Get children of this element
    children = client.element.get_elements(elem["WebId"])
    if children.get("Items"):
        print(f"    Children:")
        for child in children["Items"]:
            print(f"      - {child['Name']}")

            # Get grandchildren
            grandchildren = client.element.get_elements(child["WebId"])
            if grandchildren.get("Items"):
                for gc in grandchildren["Items"]:
                    print(f"          - {gc['Name']}")

# =============================================================================
# Cleanup (Optional)
# =============================================================================
print("\n" + "="*80)
print("Cleanup")
print("="*80)
print("Note: Created elements remain in PI. To clean up:")
print("  - Delete elements manually through PI System Explorer")
print("  - Or use client.element.delete(web_id) for each element")

# Example cleanup (uncomment to actually delete):
# client.element.delete(plant_web_id)
# client.element.delete(plant2["WebId"])
# client.element.delete(equipment_web_id)
# # ... etc for other root elements

print("\n✓ Examples completed successfully!")
