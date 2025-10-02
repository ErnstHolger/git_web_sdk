"""
PI Web SDK Use Case Examples

This module demonstrates three practical use cases:
1. Create element hierarchy in AF database Configuration
2. Create attributes and populate with historical data
3. Get all numeric attributes from an element
4. Get interpolated values at different sampling rates
"""

import math
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List

from pi_web_sdk import AuthMethod, PIWebAPIClient, PIWebAPIConfig, WebIDType
from pi_web_sdk.exceptions import PIWebAPIError

# Configuration
BASE_URL = "https://172.30.136.15/piwebapi"
USERNAME = None
PASSWORD = None
DATABASE_NAME = "Default"  # Target AF database name (Default or Configuration)


def utc_iso(dt: datetime) -> str:
    """Convert datetime to ISO 8601 UTC format."""
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def create_client() -> PIWebAPIClient:
    """Create and configure PI Web API client."""
    config = PIWebAPIConfig(
        base_url=BASE_URL,
        auth_method=AuthMethod.ANONYMOUS,
        username=USERNAME,
        password=PASSWORD,
        verify_ssl=False,
        timeout=30,
        webid_type=WebIDType.ID_ONLY,
    )
    return PIWebAPIClient(config)


def use_case_1_create_hierarchy(client: PIWebAPIClient) -> Dict[str, str]:
    """
    Use Case 1: Create element hierarchy in AF database Configuration

    Creates the following structure:
    - IndyIQ/Model/Model1
    - IndyIQ/Model/Model2
    - IndyIQ/Model/Model3

    Returns:
        Dictionary mapping element paths to their WebIDs
    """
    print("\n" + "=" * 80)
    print("USE CASE 1: Creating Element Hierarchy")
    print("=" * 80)

    # Get the target database by name
    print(f"Looking for database: {DATABASE_NAME}...")
    try:
        # Get asset server
        servers = client.asset_server.list()
        if not servers.get("Items"):
            raise SystemExit("No asset servers found")

        server_webid = servers["Items"][0]["WebId"]
        server_name = servers["Items"][0]["Name"]
        print(f"  Using server: {server_name}")

        # Get all databases
        databases = client.asset_server.get_databases(server_webid)

        # Find target database
        database = None
        for db in databases.get("Items", []):
            if db["Name"] == DATABASE_NAME:
                database = db
                break

        if not database:
            available_dbs = [db['Name'] for db in databases.get('Items', [])]
            raise SystemExit(
                f"Could not find '{DATABASE_NAME}' database. Available: {available_dbs}"
            )

        db_web_id = database["WebId"]
        db_path = database.get("Path", f"\\\\{server_name}\\{DATABASE_NAME}")
        print(f"[OK] Found database: {database['Name']} (WebID: {db_web_id})")
        print(f"     Database path: {db_path}")

    except PIWebAPIError as exc:
        raise SystemExit(f"Could not find database: {exc.message}") from exc

    element_webids = {}

    # Create IndyIQ root element
    print("\nCreating element hierarchy...")
    try:
        indyiq_def = {"Name": "IndyIQ", "Description": "Root element for IndyIQ models"}
        result = client.asset_database.create_element(db_web_id, indyiq_def)
        indyiq_webid = result["WebId"]
        element_webids["IndyIQ"] = indyiq_webid
        print("[OK] Created IndyIQ")
    except PIWebAPIError as exc:
        if exc.status_code == 409:
            print("  IndyIQ already exists, retrieving...")
            # Try to find element with retry (might be indexing delay)
            indyiq_webid = None
            for attempt in range(3):
                if attempt > 0:
                    print(f"    Retry {attempt}/2 after delay...")
                    time.sleep(2)  # Wait for indexing

                elements = client.asset_database.get_elements(
                    db_web_id,
                    max_count=1000,
                    search_full_hierarchy=True
                )

                for elem in elements.get("Items", []):
                    if elem.get("Name") == "IndyIQ":
                        indyiq_webid = elem["WebId"]
                        element_webids["IndyIQ"] = indyiq_webid
                        print(f"  Found at: {elem.get('Path', 'N/A')}")
                        break

                if indyiq_webid:
                    break

            if not indyiq_webid:
                print(f"  [X] Could not find 'IndyIQ' after 3 attempts. Found {len(elements.get('Items', []))} total elements.")
                print("  This element may have been created previously. Skipping hierarchy creation.")
                # Return empty dict to skip hierarchy creation
                return {}
        else:
            raise

    # Create Model element under IndyIQ
    try:
        model_def = {"Name": "Model", "Description": "Container for model instances"}
        result = client.element.create_element(indyiq_webid, model_def)
        model_webid = result["WebId"]
        element_webids["IndyIQ\\Model"] = model_webid
        print("[OK] Created IndyIQ\\Model")
    except PIWebAPIError as exc:
        if exc.status_code == 409:
            print("  IndyIQ\\Model already exists, retrieving...")
            # List child elements of IndyIQ
            elements = client.element.get_elements(indyiq_webid, max_count=100)
            model_webid = None
            for elem in elements.get("Items", []):
                if elem.get("Name") == "Model":
                    model_webid = elem["WebId"]
                    element_webids["IndyIQ\\Model"] = model_webid
                    break

            if not model_webid:
                raise SystemExit("Element 'Model' reported as existing but cannot be found") from exc
        else:
            raise

    # Create Model1, Model2, Model3 under Model
    for i in range(1, 4):
        model_name = f"Model{i}"
        try:
            model_instance_def = {
                "Name": model_name,
                "Description": f"Model instance {i}",
            }
            result = client.element.create_element(model_webid, model_instance_def)
            model_instance_webid = result["WebId"]
            element_webids[f"IndyIQ\\Model\\{model_name}"] = model_instance_webid
            print(f"[OK] Created IndyIQ\\Model\\{model_name}")
        except PIWebAPIError as exc:
            if exc.status_code == 409:
                print(f"  IndyIQ\\Model\\{model_name} already exists, retrieving...")
                # List child elements of Model
                elements = client.element.get_elements(model_webid, max_count=100)
                found = False
                for elem in elements.get("Items", []):
                    if elem.get("Name") == model_name:
                        element_webids[f"IndyIQ\\Model\\{model_name}"] = elem["WebId"]
                        found = True
                        break

                if not found:
                    raise SystemExit(f"Element '{model_name}' reported as existing but cannot be found") from exc
            else:
                raise

    print(
        f"\n[OK] Hierarchy creation complete! Created {len(element_webids)} elements."
    )
    return element_webids


def use_case_2_create_attributes_and_data(
    client: PIWebAPIClient, element_webids: Dict[str, str]
) -> tuple[Dict[str, str], Dict[str, str]]:
    """
    Use Case 2: Create attributes and populate with historical data

    Creates 6 attributes in IndyIQ/Model/Model1:
    - sine1, sine2, sine3 (sine waves with different frequencies)
    - square1, square2, square3 (square waves with different periods)

    Populates with 2 days of data at 10-second intervals.

    Returns:
        Tuple of (attribute_webids dict, point_webids dict)
    """
    print("\n" + "=" * 80)
    print("USE CASE 2: Creating Attributes and Populating Data")
    print("=" * 80)

    model1_webid = element_webids["IndyIQ\\Model\\Model1"]

    # Define attributes to create
    attributes = {
        "sine1": "Sine wave with 60s period",
        "sine2": "Sine wave with 120s period",
        "sine3": "Sine wave with 180s period",
        "square1": "Square wave with 100s period",
        "square2": "Square wave with 200s period",
        "square3": "Square wave with 300s period",
    }

    attribute_webids = {}
    point_webids = {}

    # Get Data Archive server
    print("\nGetting Data Archive server...")
    try:
        servers = client.data_server.list()
        if not servers.get("Items"):
            raise SystemExit("No Data Archive servers found")
        data_server_webid = servers["Items"][0]["WebId"]
        data_server_name = servers["Items"][0]["Name"]
        print(f"[OK] Using Data Archive server: {data_server_name}")
    except PIWebAPIError as exc:
        raise SystemExit(f"Could not get Data Archive server: {exc.message}") from exc

    print("\nCreating PI Points and attributes...")

    # Create PI Points and link them to attributes
    for attr_name, description in attributes.items():
        point_name = f"IndyIQ_Model1_{attr_name}"

        # Create or get PI Point
        try:
            # Try to find existing point first
            existing_point = client.data_server.find_point_by_name(data_server_webid, point_name)

            if existing_point:
                point_webid = existing_point["WebId"]
                print(f"  PI Point {point_name} already exists")
            else:
                # Create new PI Point
                point_def = {
                    "Name": point_name,
                    "PointClass": "classic",
                    "PointType": "Float32",
                    "Descriptor": description,
                }

                point_result = client.data_server.create_point(data_server_webid, point_def)
                point_webid = point_result["WebId"]
                print(f"[OK] Created PI Point: {point_name}")

            point_webids[attr_name] = point_webid

        except PIWebAPIError as exc:
            print(f"  [X] Error with PI Point {point_name}: {exc.message}")
            continue

        # Create attribute linked to PI Point
        try:
            # For PI Point data references, don't specify Type - let it be inferred
            attr_def = {
                "Name": attr_name,
                "Description": description,
                "DataReferencePlugIn": "PI Point",
                "ConfigString": f"\\\\{data_server_name}\\{point_name}",
            }

            result = client.element.create_attribute(model1_webid, attr_def)
            attr_webid = result["WebId"]
            attribute_webids[attr_name] = attr_webid
            print(f"[OK] Created attribute: {attr_name}")

        except PIWebAPIError as exc:
            if exc.status_code == 409:
                print(f"  Attribute {attr_name} already exists, retrieving...")
                # Get existing attribute
                attrs = client.element.get_attributes(model1_webid)
                for attr in attrs.get("Items", []):
                    if attr["Name"] == attr_name:
                        attribute_webids[attr_name] = attr["WebId"]
                        break
            else:
                print(f"  Warning: Could not create {attr_name}: {exc.message}")

    # Generate and write historical data (last 2 days, 10-second intervals)
    print("\nGenerating historical time-series data (2 days at 10-second intervals)...")

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=2)
    interval_seconds = 10

    # Calculate number of data points
    total_seconds = int((end_time - start_time).total_seconds())
    num_points = total_seconds // interval_seconds

    print(f"  Generating {num_points} data points per attribute...")

    # Generate data for each attribute
    successful_writes = 0
    failed_writes = 0

    for attr_name, point_webid in point_webids.items():
        print(f"\n  Processing {attr_name}")
        print(f"    WebID: {point_webid}")
        values = []

        for i in range(num_points):
            timestamp = start_time + timedelta(seconds=i * interval_seconds)

            # Generate value based on attribute type
            if attr_name.startswith("sine"):
                # Sine waves with different periods
                period_map = {"sine1": 60, "sine2": 120, "sine3": 180}
                period = period_map[attr_name]
                value = 50 + 25 * math.sin(2 * math.pi * i * interval_seconds / period)
            else:  # square waves
                period_map = {"square1": 100, "square2": 200, "square3": 300}
                period = period_map[attr_name]
                cycle_position = (i * interval_seconds) % period
                value = 75 if cycle_position < period / 2 else 25

            values.append({
                "Timestamp": utc_iso(timestamp),
                "Value": round(value, 2),
            })

        # Write values in batches to avoid timeout
        batch_size = 1000
        total_batches = (len(values) + batch_size - 1) // batch_size

        print(f"  Writing data for {attr_name} ({len(values)} points in {total_batches} batches)...")

        write_error = False
        for batch_idx in range(0, len(values), batch_size):
            batch = values[batch_idx:batch_idx + batch_size]
            batch_num = batch_idx // batch_size + 1

            try:
                client.stream.update_values(point_webid, batch, buffer_option="BufferIfPossible")

                if batch_num % 5 == 0 or batch_num == total_batches:
                    print(f"    Batch {batch_num}/{total_batches} complete")

            except PIWebAPIError as exc:
                print(f"  [X] Error writing batch {batch_num} for {attr_name}: {exc.message}")
                write_error = True
                break

        if write_error:
            failed_writes += 1
            print(f"[X] FAILED data write for {attr_name}")
        else:
            successful_writes += 1
            print(f"[OK] Completed data write for {attr_name}")

    print(f"\nData write summary: {successful_writes} successful, {failed_writes} failed")

    if successful_writes > 0:
        print("\nWaiting 10 seconds for PI buffer to flush data to archive...")
        time.sleep(10)
        print("[OK] Buffer flush wait complete")

    print("\n[OK] Attribute creation and data population complete!")
    print(f"    Created {len(attribute_webids)} attributes and {len(point_webids)} PI Points")
    return attribute_webids, point_webids


def use_case_3_get_numeric_attributes(
    client: PIWebAPIClient, element_webid_or_path: str
) -> List[Dict]:
    """
    Use Case 3: Get all numeric attributes from an element

    Retrieves all attributes from the specified element and filters
    to only numeric types (Int, Float, PIPoint with numeric type).

    Args:
        client: PI Web API client
        element_webid_or_path: WebID or full path to the element

    Returns:
        List of numeric attribute dictionaries
    """
    print("\n" + "=" * 80)
    print("USE CASE 3: Get All Numeric Attributes")
    print("=" * 80)

    # Determine if input is WebID or path (paths contain backslash)
    if "\\" in element_webid_or_path:
        print(f"\nRetrieving element by path: {element_webid_or_path}")
        try:
            element = client.element.get_by_path(element_webid_or_path)
            element_webid = element["WebId"]
            print(f"[OK] Found element: {element['Name']} (WebID: {element_webid})")
        except PIWebAPIError as exc:
            raise SystemExit(f"Could not find element: {exc.message}") from exc
    else:
        print(f"\nUsing element WebID: {element_webid_or_path}")
        element_webid = element_webid_or_path
        try:
            element = client.element.get(element_webid)
            print(f"[OK] Found element: {element['Name']} (WebID: {element_webid})")
        except PIWebAPIError as exc:
            raise SystemExit(f"Could not find element: {exc.message}") from exc

    # Get all attributes
    try:
        attributes_response = client.element.get_attributes(element_webid)
        all_attributes = attributes_response.get("Items", [])
        print(f"[OK] Retrieved {len(all_attributes)} total attributes")
    except PIWebAPIError as exc:
        raise SystemExit(f"Could not get attributes: {exc.message}") from exc

    # Filter to numeric attributes
    numeric_types = [
        "Int16",
        "Int32",
        "Int64",
        "Float16",
        "Float32",
        "Float64",
        "Double",
        "Single",
    ]
    numeric_attributes = []

    print("\nAnalyzing attributes for numeric types...")

    for attr in all_attributes:
        attr_name = attr.get("Name", "Unknown")
        attr_type = attr.get("Type", "")

        is_numeric = False

        # Check if it's a direct numeric type
        if attr_type in numeric_types:
            is_numeric = True
            print(f"  [OK] {attr_name}: {attr_type} (direct numeric)")

        # Check if it's a PI Point (need to check the point's type)
        elif attr_type == "PIPoint":
            try:
                # Try to get point info from attribute
                if "Links" in attr and "Point" in attr["Links"]:
                    point_webid = attr["Links"]["Point"].split("?")[0].split("/")[-1]
                    point_info = client.point.get(point_webid)
                    point_type = point_info.get("PointType", "")

                    if any(
                        num_type.lower() in point_type.lower()
                        for num_type in numeric_types
                    ):
                        is_numeric = True
                        print(f"  [OK] {attr_name}: PIPoint ({point_type})")
            except Exception:
                print(f"  ? {attr_name}: PIPoint (could not verify type)")

        if is_numeric:
            numeric_attributes.append(attr)

    print(f"\n[OK] Found {len(numeric_attributes)} numeric attributes")

    # Display summary
    if numeric_attributes:
        print("\nNumeric Attributes Summary:")
        print("-" * 80)
        for attr in numeric_attributes:
            name = attr.get("Name", "Unknown")
            attr_type = attr.get("Type", "Unknown")
            description = attr.get("Description", "No description")
            print(f"  • {name:15s} | Type: {attr_type:15s} | {description}")

    return numeric_attributes


def use_case_4_get_interpolated_values(
    client: PIWebAPIClient,
    point_webids: Dict[str, str],
    days: int = 1,
    interval_seconds: int = 30,
) -> Dict[str, List[Dict]]:
    """
    Use Case 4: Get interpolated values at specified sampling rate

    Retrieves interpolated values for all attributes over the specified
    time range at the given sampling interval.

    Args:
        client: PI Web API client
        point_webids: Dictionary mapping attribute names to PI Point WebIDs
        days: Number of days to retrieve (default: 1)
        interval_seconds: Sampling interval in seconds (default: 30)

    Returns:
        Dictionary mapping attribute names to their interpolated values
    """
    print("\n" + "=" * 80)
    print("USE CASE 4: Get Interpolated Values")
    print("=" * 80)

    # Query the same time range where we wrote data (last 2 days ending now)
    # But only get the most recent 'days' worth
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=days)

    print("Note: Querying data from the historical dataset (last 2 days)")

    print("\nRetrieving interpolated data:")
    print(
        f"  Time range: {start_time.strftime('%Y-%m-%d %H:%M:%S')} to {end_time.strftime('%Y-%m-%d %H:%M:%S')}"
    )
    print(f"  Duration: {days} day(s)")
    print(f"  Sampling interval: {interval_seconds} seconds")

    # Calculate expected number of points
    total_seconds = int((end_time - start_time).total_seconds())
    expected_points = total_seconds // interval_seconds
    print(f"  Expected data points: ~{expected_points} per attribute\n")

    interpolated_data = {}

    for attr_name, point_webid in point_webids.items():
        try:
            # Get interpolated values
            result = client.stream.get_interpolated(
                web_id=point_webid,
                start_time=utc_iso(start_time),
                end_time=utc_iso(end_time),
                interval=f"{interval_seconds}s",
            )

            values = result.get("Items", [])
            interpolated_data[attr_name] = values

            # Calculate statistics
            if values:
                # Try to extract numeric values (might be in Value or other fields)
                numeric_values = []
                for v in values:
                    val = v.get("Value")
                    if val is not None:
                        try:
                            # Handle different value formats
                            if isinstance(val, dict):
                                # PI Web API returns system values as dicts like {'Name': 'No Data', 'Value': 248, 'IsSystem': True}
                                # Skip system values
                                if not val.get("IsSystem", False):
                                    # Non-system dict value - shouldn't happen but try anyway
                                    numeric_values.append(float(val.get("Value", val)))
                            elif isinstance(val, str):
                                numeric_values.append(float(val))
                            elif isinstance(val, (int, float)):
                                numeric_values.append(float(val))
                        except (ValueError, TypeError, KeyError):
                            pass  # Skip non-numeric values

                if numeric_values:
                    avg_value = sum(numeric_values) / len(numeric_values)
                    min_value = min(numeric_values)
                    max_value = max(numeric_values)

                    print(
                        f"  [OK] {attr_name:10s}: {len(values):5d} points | "
                        f"Avg: {avg_value:6.2f} | Min: {min_value:6.2f} | Max: {max_value:6.2f}"
                    )
                else:
                    print(
                        f"  [OK] {attr_name:10s}: {len(values):5d} points (no numeric data)"
                    )
            else:
                print(f"  [X] {attr_name:10s}: No data returned")

        except PIWebAPIError as exc:
            print(f"  [X] {attr_name}: Error retrieving data - {exc.message}")
            interpolated_data[attr_name] = []

    print("\n[OK] Interpolated data retrieval complete!")
    return interpolated_data


def main() -> None:
    """Main execution function demonstrating all use cases."""

    print("\n" + "=" * 80)
    print("PI Web SDK - Comprehensive Use Case Examples")
    print("=" * 80)

    # Initialize client
    print("\nInitializing PI Web API client...")
    client = create_client()
    print("[OK] Client initialized successfully")

    try:
        # Use Case 1: Create hierarchy
        element_webids = use_case_1_create_hierarchy(client)

        # Check if hierarchy creation was successful
        if not element_webids:
            print("\n[X] Hierarchy creation was skipped. Cannot proceed with remaining use cases.")
            print("    Please delete the 'IndyIQ' element from PI System Explorer and run again.")
            return

        # Use Case 2: Create attributes and populate data
        attribute_webids, point_webids = use_case_2_create_attributes_and_data(client, element_webids)

        # Use Case 3: Get all numeric attributes
        # Get Model1's WebID from element_webids
        model1_webid = element_webids.get("IndyIQ\\Model\\Model1")
        if model1_webid:
            numeric_attributes = use_case_3_get_numeric_attributes(client, model1_webid)
        else:
            print("[X] Warning: Model1 not found, skipping use case 3")
            numeric_attributes = []

        # Use Case 4: Get interpolated values (1 day at 30-second intervals)
        if point_webids:
            interpolated_data = use_case_4_get_interpolated_values(
                client, point_webids, days=1, interval_seconds=30
            )

        print("\n" + "=" * 80)
        print("All use cases completed successfully!")
        print("=" * 80)

    except PIWebAPIError as exc:
        print(f"\n[X] Error: {exc.message}")
        if exc.status_code:
            print(f"  Status code: {exc.status_code}")
        raise SystemExit(1) from exc
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user.")
        raise SystemExit(0)
    except Exception as exc:
        print(f"\n[X] Unexpected error: {str(exc)}")
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
