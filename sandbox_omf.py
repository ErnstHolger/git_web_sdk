"""
PI Web SDK OMF Use Case Examples

This module demonstrates the same use cases as sandbox.py but using OMF (OSIsoft Message Format) endpoints:
1. Create element hierarchy using OMF assets
2. Create containers and populate with historical time-series data via OMF
3. Get all numeric attributes from an element (using traditional API)
4. Get interpolated values at different sampling rates (using traditional API)

Key differences from traditional API:
- OMF uses message-based approach (Type, Container, Data)
- More efficient for bulk data ingestion
- Write-only; queries still use traditional API
"""

import math
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List

from pi_web_sdk import AuthMethod, PIWebAPIClient, PIWebAPIConfig, WebIDType
from pi_web_sdk.exceptions import PIWebAPIError
from pi_web_sdk.omf import (
    OMFManager,
    OMFType,
    OMFProperty,
    OMFContainer,
    OMFAsset,
    OMFTimeSeriesData,
    OMFBatch,
    Classification,
    PropertyType,
)

# Configuration
BASE_URL = "https://172.30.136.15/piwebapi"
USERNAME = None
PASSWORD = None
DATABASE_NAME = "Default"  # Target AF database name


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


def use_case_1_create_hierarchy_omf(
    client: PIWebAPIClient, omf_manager: OMFManager
) -> Dict[str, str]:
    """
    Use Case 1: Create element hierarchy using traditional API + OMF metadata

    Creates ACTUAL AF element hierarchy with proper parent-child relationships:
    - IndyIQ_OMF (root in AF database)
      └─ Model (child of IndyIQ_OMF)
         ├─ Model1 (child of Model)
         ├─ Model2 (child of Model)
         └─ Model3 (child of Model)

    Note: OMF cannot create AF element hierarchies directly. This hybrid approach:
    1. Uses traditional API to create actual AF elements with parent-child relationships
    2. Uses OMF to add metadata assets that reference these elements

    Returns:
        Dictionary mapping element paths to their WebIDs
    """
    print("\n" + "=" * 80)
    print("USE CASE 1: Creating Element Hierarchy (Hybrid Approach)")
    print("=" * 80)
    print("\nIMPORTANT: OMF alone cannot create AF hierarchies.")
    print("Using traditional API for AF elements, OMF for metadata.")

    element_webids = {}

    try:
        # Get the database
        print(f"\nLooking for database: {DATABASE_NAME}...")
        servers = client.asset_server.list()
        if not servers.get("Items"):
            raise SystemExit("No asset servers found")

        server_webid = servers["Items"][0]["WebId"]
        server_name = servers["Items"][0]["Name"]
        databases = client.asset_server.get_databases(server_webid)

        database = None
        for db in databases.get("Items", []):
            if db["Name"] == DATABASE_NAME:
                database = db
                break

        if not database:
            raise SystemExit(f"Database '{DATABASE_NAME}' not found")

        db_web_id = database["WebId"]
        db_path = database.get("Path", f"\\\\{server_name}\\{DATABASE_NAME}")
        print(f"[OK] Using database: {db_path}")

        # Create AF hierarchy using traditional API
        print("\n1. Creating AF element hierarchy (Traditional API)...")

        def create_or_get_element(parent_webid, name, description, parent_path="", is_root=False):
            """Create or retrieve element"""
            full_path = f"{parent_path}\\{name}" if parent_path else name

            # Try to find existing
            try:
                if is_root:
                    existing = client.asset_database.get_elements(parent_webid, name_filter=name)
                else:
                    existing = client.element.get_elements(parent_webid, name_filter=name)

                for elem in existing.get("Items", []):
                    if elem["Name"] == name:
                        print(f"  {full_path} already exists")
                        return elem
            except:
                pass

            # Create new
            element_def = {"Name": name, "Description": description}

            if is_root:
                client.asset_database.create_element(parent_webid, element_def)
            else:
                client.element.create_element(parent_webid, element_def)

            time.sleep(0.5)

            # Retrieve
            if is_root:
                result = client.asset_database.get_elements(parent_webid, name_filter=name)
            else:
                result = client.element.get_elements(parent_webid, name_filter=name)

            for elem in result.get("Items", []):
                if elem["Name"] == name:
                    print(f"  [OK] Created {full_path}")
                    return elem

            raise Exception(f"Failed to create {name}")

        # Create hierarchy
        indyiq = create_or_get_element(db_web_id, "IndyIQ_OMF", "Root for OMF demo", db_path, is_root=True)
        element_webids["IndyIQ"] = indyiq["WebId"]

        model = create_or_get_element(indyiq["WebId"], "Model", "Model container", f"{db_path}\\IndyIQ_OMF", is_root=False)
        element_webids["IndyIQ\\Model"] = model["WebId"]

        for i in range(1, 4):
            m = create_or_get_element(model["WebId"], f"Model{i}", f"Model instance {i}", f"{db_path}\\IndyIQ_OMF\\Model", is_root=False)
            element_webids[f"IndyIQ\\Model\\Model{i}"] = m["WebId"]

        print("\n[OK] Created AF hierarchy:")
        print(f"     {db_path}\\IndyIQ_OMF")
        print(f"       +- Model")
        print(f"          +- Model1")
        print(f"          +- Model2")
        print(f"          +- Model3")

        # Create OMF metadata
        print("\n2. Creating OMF metadata assets...")
        timestamp = int(time.time())

        meta_type = OMFType.create_static_type(
            id=f"AFMetadata_{timestamp}",
            additional_properties={
                "af_webid": OMFProperty(PropertyType.STRING, "AF element WebID"),
                "af_path": OMFProperty(PropertyType.STRING, "AF element path"),
            }
        )
        omf_manager.create_type(meta_type)

        meta_values = []
        for path, webid in element_webids.items():
            path_suffix = ""
            if "\\" in path:
                parts = path.split("\\", 1)
                if len(parts) > 1:
                    path_suffix = "\\" + parts[1]

            meta_values.append({
                "name": f"meta_{path.replace(chr(92), '_')}",
                "af_webid": webid,
                "af_path": f"{db_path}\\IndyIQ_OMF{path_suffix}"
            })

        omf_manager.create_asset(OMFAsset(meta_type.id, meta_values))
        print(f"[OK] Created OMF metadata for {len(meta_values)} elements")

        print(f"\n[OK] Hierarchy complete! {len(element_webids)} AF elements created.")
        return element_webids

    except PIWebAPIError as exc:
        raise SystemExit(f"Hierarchy creation failed: {exc.message}") from exc


def use_case_2_create_attributes_and_data_omf(
    client: PIWebAPIClient,
    omf_manager: OMFManager,
    element_ids: Dict[str, str]
) -> tuple[Dict[str, str], Dict[str, str]]:
    """
    Use Case 2: Create containers and populate with time-series data using OMF

    Creates 6 OMF containers (dynamic streams) for:
    - sine1, sine2, sine3 (sine waves with different frequencies)
    - square1, square2, square3 (square waves with different periods)

    Populates with 2 days of data at 10-second intervals using OMF data messages.

    Returns:
        Tuple of (container_ids dict, container_ids dict)
    """
    print("\n" + "=" * 80)
    print("USE CASE 2: Creating Containers and Populating Data with OMF")
    print("=" * 80)

    timestamp = int(time.time())
    model1_id = element_ids.get("IndyIQ\\Model\\Model1", f"IndyIQ_{timestamp}_Model1")

    # Define container names and descriptions
    containers_def = {
        "sine1": "Sine wave with 60s period",
        "sine2": "Sine wave with 120s period",
        "sine3": "Sine wave with 180s period",
        "square1": "Square wave with 100s period",
        "square2": "Square wave with 200s period",
        "square3": "Square wave with 300s period",
    }

    container_ids = {}

    try:
        # Create OMF dynamic type for time-series data
        print("\n1. Creating OMF dynamic type for time-series data...")

        sensor_type = OMFType.create_dynamic_type(
            id=f"IndyIQ_SensorDataType_{timestamp}",
            timestamp_property="timestamp",
            additional_properties={
                "value": OMFProperty(
                    type=PropertyType.NUMBER,
                    description="Sensor value"
                ),
            },
            description="Time-series sensor data type"
        )

        omf_manager.create_type(sensor_type)
        print("[OK] Created OMF dynamic type for sensor data")

        # Create OMF containers for each sensor
        print("\n2. Creating OMF containers (streams)...")

        for container_name, description in containers_def.items():
            container_id = f"{model1_id}_{container_name}"

            container = OMFContainer(
                id=container_id,
                type_id=sensor_type.id,
                name=container_name,
                description=description,
                # Note: tags omitted due to OMF model limitations
                metadata={"created_by": "OMF sandbox script", "element_id": model1_id}
            )

            omf_manager.create_container(container)
            container_ids[container_name] = container_id
            print(f"[OK] Created container: {container_name}")

        # Generate and send time-series data
        print("\n3. Generating historical time-series data (2 days at 10-second intervals)...")

        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=2)
        interval_seconds = 10

        # Calculate number of data points
        total_seconds = int((end_time - start_time).total_seconds())
        num_points = total_seconds // interval_seconds

        print(f"  Generating {num_points} data points per container...")

        successful_writes = 0
        failed_writes = 0

        for container_name, container_id in container_ids.items():
            print(f"\n  Processing {container_name}")
            print(f"    Container ID: {container_id}")

            values = []

            for i in range(num_points):
                timestamp_value = start_time + timedelta(seconds=i * interval_seconds)

                # Generate value based on container type
                if container_name.startswith("sine"):
                    # Sine waves with different periods
                    period_map = {"sine1": 60, "sine2": 120, "sine3": 180}
                    period = period_map[container_name]
                    value = 50 + 25 * math.sin(2 * math.pi * i * interval_seconds / period)
                else:  # square waves
                    period_map = {"square1": 100, "square2": 200, "square3": 300}
                    period = period_map[container_name]
                    cycle_position = (i * interval_seconds) % period
                    value = 75 if cycle_position < period / 2 else 25

                values.append({
                    "timestamp": utc_iso(timestamp_value),
                    "value": round(value, 2),
                })

            # Create OMF time-series data message
            ts_data = OMFTimeSeriesData(
                container_id=container_id,
                values=values
            )

            # Send data in batches to avoid timeout
            batch_size = 5000  # OMF can handle larger batches than traditional API
            total_batches = (len(values) + batch_size - 1) // batch_size

            print(f"  Writing data for {container_name} ({len(values)} points in {total_batches} batches)...")

            write_error = False
            for batch_idx in range(0, len(values), batch_size):
                batch_values = values[batch_idx:batch_idx + batch_size]
                batch_num = batch_idx // batch_size + 1

                try:
                    # Create batch-specific time-series data
                    batch_ts_data = OMFTimeSeriesData(
                        container_id=container_id,
                        values=batch_values
                    )

                    omf_manager.send_time_series_data(batch_ts_data)

                    if batch_num % 2 == 0 or batch_num == total_batches:
                        print(f"    Batch {batch_num}/{total_batches} complete")

                except PIWebAPIError as exc:
                    print(f"  [X] Error writing batch {batch_num} for {container_name}: {exc.message}")
                    write_error = True
                    break

            if write_error:
                failed_writes += 1
                print(f"[X] FAILED data write for {container_name}")
            else:
                successful_writes += 1
                print(f"[OK] Completed data write for {container_name}")

        print(f"\nData write summary: {successful_writes} successful, {failed_writes} failed")

        if successful_writes > 0:
            print("\nWaiting 10 seconds for OMF buffer to flush data to archive...")
            time.sleep(10)
            print("[OK] Buffer flush wait complete")

        print("\n[OK] Container creation and data population complete!")
        print(f"    Created {len(container_ids)} OMF containers")

        return container_ids, container_ids

    except PIWebAPIError as exc:
        raise SystemExit(f"OMF data ingestion failed: {exc.message}") from exc


def use_case_3_get_numeric_attributes(
    client: PIWebAPIClient, element_webid_or_path: str
) -> List[Dict]:
    """
    Use Case 3: Get all numeric attributes from an element

    Note: This uses traditional PI Web API since OMF is write-only.
    Queries the AF database to find attributes created by OMF containers.

    Args:
        client: PI Web API client
        element_webid_or_path: WebID or full path to the element

    Returns:
        List of numeric attribute dictionaries
    """
    print("\n" + "=" * 80)
    print("USE CASE 3: Get All Numeric Attributes (Traditional API)")
    print("=" * 80)
    print("\nNote: OMF is write-only, using traditional API for queries")

    # Determine if input is WebID or path
    if "\\" in element_webid_or_path:
        print(f"\nRetrieving element by path: {element_webid_or_path}")
        try:
            element = client.element.get_by_path(element_webid_or_path)
            element_webid = element["WebId"]
            print(f"[OK] Found element: {element['Name']} (WebID: {element_webid})")
        except PIWebAPIError as exc:
            print(f"  [X] Could not find element: {exc.message}")
            print(f"  Note: OMF-created assets may not immediately appear in AF hierarchy")
            return []
    else:
        print(f"\nUsing element WebID: {element_webid_or_path}")
        element_webid = element_webid_or_path
        try:
            element = client.element.get(element_webid)
            print(f"[OK] Found element: {element['Name']} (WebID: {element_webid})")
        except PIWebAPIError as exc:
            print(f"  [X] Could not find element: {exc.message}")
            return []

    # Get all attributes
    try:
        attributes_response = client.element.get_attributes(element_webid)
        all_attributes = attributes_response.get("Items", [])
        print(f"[OK] Retrieved {len(all_attributes)} total attributes")
    except PIWebAPIError as exc:
        print(f"  [X] Could not get attributes: {exc.message}")
        return []

    # Filter to numeric attributes
    numeric_types = [
        "Int16", "Int32", "Int64",
        "Float16", "Float32", "Float64",
        "Double", "Single",
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
        # Check if it's a PI Point
        elif attr_type == "PIPoint":
            try:
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
            print(f"  - {name:15s} | Type: {attr_type:15s} | {description}")

    return numeric_attributes


def use_case_4_get_interpolated_values(
    client: PIWebAPIClient,
    container_ids: Dict[str, str],
    days: int = 1,
    interval_seconds: int = 30,
) -> Dict[str, List[Dict]]:
    """
    Use Case 4: Get interpolated values at specified sampling rate

    Note: This uses traditional PI Web API since OMF is write-only.
    Queries the streams created by OMF containers.

    Args:
        client: PI Web API client
        container_ids: Dictionary mapping container names to container IDs
        days: Number of days to retrieve (default: 1)
        interval_seconds: Sampling interval in seconds (default: 30)

    Returns:
        Dictionary mapping container names to their interpolated values
    """
    print("\n" + "=" * 80)
    print("USE CASE 4: Get Interpolated Values (Traditional API)")
    print("=" * 80)
    print("\nNote: OMF is write-only, using traditional API for queries")

    # Query the same time range where we wrote data
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=days)

    print("\nNote: Querying data from the OMF-created streams")

    print("\nRetrieving interpolated data:")
    print(
        f"  Time range: {start_time.strftime('%Y-%m-%d %H:%M:%S')} to {end_time.strftime('%Y-%m-%d %H:%M:%S')}"
    )
    print(f"  Duration: {days} day(s)")
    print(f"  Sampling interval: {interval_seconds} seconds")

    # Calculate expected number of points
    total_seconds = int((end_time - start_time).total_seconds())
    expected_points = total_seconds // interval_seconds
    print(f"  Expected data points: ~{expected_points} per container\n")

    interpolated_data = {}

    # Need to find the PI Point WebIDs for the OMF containers
    # OMF containers map to PI Points that we can query
    print("Resolving OMF container IDs to PI Point WebIDs...")

    # Get data server to search for points
    try:
        servers = client.data_server.list()
        if not servers.get("Items"):
            print("[X] No data servers found")
            return {}

        data_server_webid = servers["Items"][0]["WebId"]
        data_server_name = servers["Items"][0]["Name"]
        print(f"[OK] Using data server: {data_server_name}")

    except PIWebAPIError as exc:
        print(f"[X] Could not get data server: {exc.message}")
        return {}

    for container_name, container_id in container_ids.items():
        # Try to find the PI Point created by OMF container
        # OMF typically creates points with the container ID as the name
        try:
            # Search for point by name (container ID)
            point = client.data_server.find_point_by_name(data_server_webid, container_id)

            if not point:
                print(f"  [X] {container_name:10s}: Point not found (container ID: {container_id})")
                continue

            point_webid = point["WebId"]

            # Get interpolated values
            result = client.stream.get_interpolated(
                web_id=point_webid,
                start_time=utc_iso(start_time),
                end_time=utc_iso(end_time),
                interval=f"{interval_seconds}s",
            )

            values = result.get("Items", [])
            interpolated_data[container_name] = values

            # Calculate statistics
            if values:
                numeric_values = []
                for v in values:
                    val = v.get("Value")
                    if val is not None:
                        try:
                            # Handle different value formats
                            if isinstance(val, dict):
                                # Skip system values
                                if not val.get("IsSystem", False):
                                    numeric_values.append(float(val.get("Value", val)))
                            elif isinstance(val, str):
                                numeric_values.append(float(val))
                            elif isinstance(val, (int, float)):
                                numeric_values.append(float(val))
                        except (ValueError, TypeError, KeyError):
                            pass

                if numeric_values:
                    avg_value = sum(numeric_values) / len(numeric_values)
                    min_value = min(numeric_values)
                    max_value = max(numeric_values)

                    print(
                        f"  [OK] {container_name:10s}: {len(values):5d} points | "
                        f"Avg: {avg_value:6.2f} | Min: {min_value:6.2f} | Max: {max_value:6.2f}"
                    )
                else:
                    print(
                        f"  [OK] {container_name:10s}: {len(values):5d} points (no numeric data)"
                    )
            else:
                print(f"  [X] {container_name:10s}: No data returned")

        except PIWebAPIError as exc:
            print(f"  [X] {container_name}: Error retrieving data - {exc.message}")
            interpolated_data[container_name] = []

    print("\n[OK] Interpolated data retrieval complete!")
    return interpolated_data


def main() -> None:
    """Main execution function demonstrating all use cases with OMF."""

    print("\n" + "=" * 80)
    print("PI Web SDK - OMF Use Case Examples")
    print("=" * 80)
    print("\nThis script demonstrates using OMF (OSIsoft Message Format) endpoints")
    print("instead of traditional PI Web API REST endpoints.")

    # Initialize client
    print("\nInitializing PI Web API client...")
    client = create_client()
    print("[OK] Client initialized successfully")

    # Initialize OMF manager
    print("\nInitializing OMF manager...")
    try:
        servers = client.data_server.list()
        if not servers.get("Items"):
            raise SystemExit("No data servers found")

        data_server_webid = servers["Items"][0]["WebId"]
        data_server_name = servers["Items"][0]["Name"]

        omf_manager = OMFManager(client, data_server_webid)
        print(f"[OK] OMF manager initialized with data server: {data_server_name}")

    except PIWebAPIError as exc:
        raise SystemExit(f"Could not initialize OMF manager: {exc.message}") from exc

    try:
        # Use Case 1: Create hierarchy with OMF
        element_ids = use_case_1_create_hierarchy_omf(client, omf_manager)

        # Use Case 2: Create containers and populate data with OMF
        container_ids, _ = use_case_2_create_attributes_and_data_omf(
            client, omf_manager, element_ids
        )

        # Use Case 3: Get all numeric attributes (traditional API)
        # Note: OMF-created elements may not be queryable via traditional API immediately
        model1_id = element_ids.get("IndyIQ\\Model\\Model1")
        if model1_id:
            print(f"\nAttempting to query OMF-created element: {model1_id}")
            numeric_attributes = use_case_3_get_numeric_attributes(client, model1_id)
        else:
            print("[X] Warning: Model1 ID not found, skipping use case 3")
            numeric_attributes = []

        # Use Case 4: Get interpolated values (traditional API)
        if container_ids:
            interpolated_data = use_case_4_get_interpolated_values(
                client, container_ids, days=1, interval_seconds=30
            )

        print("\n" + "=" * 80)
        print("All use cases completed successfully!")
        print("=" * 80)
        print("\nKey takeaways:")
        print("- OMF provides efficient message-based data ingestion")
        print("- OMF is write-only; queries use traditional API")
        print("- OMF containers map to PI Points for data storage")
        print("- OMF assets may not immediately appear in AF hierarchy")

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
