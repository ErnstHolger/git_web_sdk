"""Example: Using Stream Updates for incremental data retrieval.

Stream Updates provides an efficient way to receive incremental data updates
without opening websockets. It uses markers to track position in the stream.
"""

from pi_web_sdk import PIWebAPIClient
from pi_web_sdk.config import PIWebAPIConfig
import time


def single_stream_updates_example():
    """Example of using Stream Updates for a single stream."""
    # Configure client
    config = PIWebAPIConfig(
        base_url="https://your-pi-server/piwebapi",
        username="your-username",
        password="your-password",
        verify_ssl=True
    )
    client = PIWebAPIClient(config)
    
    # Get a stream WebID (example: from an attribute)
    element = client.element.get_by_path(r"\\ServerName\DatabaseName\ElementName")
    attribute = client.attribute.get_by_path(
        r"\\ServerName\DatabaseName\ElementName|AttributeName"
    )
    stream_web_id = attribute["WebId"]
    
    # Step 1: Register for updates
    print("Registering stream for updates...")
    registration = client.stream.register_update(
        stream_web_id,
        selected_fields="Items.Timestamp;Items.Value"
    )
    
    if registration.get("Status") == "Succeeded":
        marker = registration["LatestMarker"]
        print(f"Registration successful. Initial marker: {marker}")
    else:
        print(f"Registration failed: {registration}")
        return
    
    # Step 2: Poll for updates in a loop
    try:
        for i in range(10):  # Poll 10 times
            print(f"\nPoll iteration {i + 1}...")
            
            # Wait before polling (adjust based on your data update frequency)
            time.sleep(5)
            
            # Retrieve updates
            updates = client.stream.retrieve_update(marker)
            
            # Process updates
            items = updates.get("Items", [])
            if items:
                print(f"Received {len(items)} new values:")
                for item in items:
                    timestamp = item.get("Timestamp")
                    value = item.get("Value")
                    print(f"  {timestamp}: {value}")
            else:
                print("No new updates")
            
            # Update marker for next iteration
            marker = updates["LatestMarker"]
            print(f"New marker: {marker}")
            
    except KeyboardInterrupt:
        print("\nStopped by user")
    except Exception as e:
        print(f"Error: {e}")
        # On error, you should re-register and fetch fresh data


def multiple_streams_updates_example():
    """Example of using Stream Updates for multiple streams."""
    # Configure client
    config = PIWebAPIConfig(
        base_url="https://your-pi-server/piwebapi",
        username="your-username",
        password="your-password",
        verify_ssl=True
    )
    client = PIWebAPIClient(config)
    
    # Get multiple stream WebIDs
    stream_web_ids = [
        # Get from attributes
        client.attribute.get_by_path(
            r"\\ServerName\DatabaseName\Element1|Temperature"
        )["WebId"],
        client.attribute.get_by_path(
            r"\\ServerName\DatabaseName\Element1|Pressure"
        )["WebId"],
        client.attribute.get_by_path(
            r"\\ServerName\DatabaseName\Element2|Flow"
        )["WebId"],
    ]
    
    # Step 1: Register all streams for updates
    print("Registering multiple streams for updates...")
    registration = client.streamset.register_updates(
        stream_web_ids,
        selected_fields="Items.Timestamp;Items.Value;Items.WebId"
    )
    
    # Check registration status for each stream
    registration_items = registration.get("Items", [])
    for item in registration_items:
        web_id = item.get("WebId")
        status = item.get("Status")
        print(f"  {web_id}: {status}")
    
    marker = registration["LatestMarker"]
    print(f"Initial marker: {marker}\n")
    
    # Step 2: Poll for updates
    try:
        for i in range(10):  # Poll 10 times
            print(f"Poll iteration {i + 1}...")
            
            # Wait before polling
            time.sleep(5)
            
            # Retrieve updates for all streams
            updates = client.streamset.retrieve_updates(marker)
            
            # Process updates per stream
            stream_updates = updates.get("Items", [])
            total_values = 0
            
            for stream_update in stream_updates:
                stream_web_id = stream_update.get("WebId")
                items = stream_update.get("Items", [])
                
                if items:
                    print(f"\n  Stream {stream_web_id}:")
                    for item in items:
                        timestamp = item.get("Timestamp")
                        value = item.get("Value")
                        print(f"    {timestamp}: {value}")
                    total_values += len(items)
            
            if total_values == 0:
                print("  No new updates for any stream")
            
            # Update marker
            marker = updates["LatestMarker"]
            print(f"\nNew marker: {marker}")
            
    except KeyboardInterrupt:
        print("\nStopped by user")
    except Exception as e:
        print(f"Error: {e}")


def updates_with_unit_conversion_example():
    """Example of retrieving updates with unit conversion."""
    config = PIWebAPIConfig(
        base_url="https://your-pi-server/piwebapi",
        username="your-username",
        password="your-password",
        verify_ssl=True
    )
    client = PIWebAPIClient(config)
    
    # Get temperature attribute WebID
    temp_attr = client.attribute.get_by_path(
        r"\\ServerName\DatabaseName\ElementName|Temperature"
    )
    stream_web_id = temp_attr["WebId"]
    
    # Register for updates
    registration = client.stream.register_update(stream_web_id)
    marker = registration["LatestMarker"]
    
    # Retrieve updates with unit conversion (e.g., Celsius to Fahrenheit)
    updates = client.stream.retrieve_update(
        marker,
        desired_units="degF"  # Convert to Fahrenheit
    )
    
    print("Temperature values in Fahrenheit:")
    for item in updates.get("Items", []):
        timestamp = item.get("Timestamp")
        value = item.get("Value")
        print(f"  {timestamp}: {value}°F")


def error_handling_example():
    """Example of handling Stream Updates errors."""
    config = PIWebAPIConfig(
        base_url="https://your-pi-server/piwebapi",
        username="your-username",
        password="your-password",
        verify_ssl=True
    )
    client = PIWebAPIClient(config)
    
    stream_web_id = "P1AbcDEFg..."  # Your stream WebID
    marker = None
    
    while True:
        try:
            # Register if we don't have a marker
            if marker is None:
                print("Registering for updates...")
                registration = client.stream.register_update(stream_web_id)
                
                if registration.get("Status") == "AlreadyRegistered":
                    print("Stream already registered")
                    marker = registration["LatestMarker"]
                elif registration.get("Status") == "Succeeded":
                    marker = registration["LatestMarker"]
                    print(f"Registration successful: {marker}")
                else:
                    # Check for registration errors
                    exception = registration.get("Exception")
                    print(f"Registration failed: {exception}")
                    time.sleep(10)
                    continue
            
            # Retrieve updates
            time.sleep(5)
            updates = client.stream.retrieve_update(marker)
            
            # Check for errors in the response
            if "Errors" in updates:
                print(f"Errors in update response: {updates['Errors']}")
                # Re-register and fetch fresh data
                marker = None
                continue
            
            # Process updates
            items = updates.get("Items", [])
            if items:
                print(f"Received {len(items)} updates")
            
            # Update marker
            marker = updates["LatestMarker"]
            
        except Exception as e:
            print(f"Exception occurred: {e}")
            # On error, clear marker to force re-registration
            marker = None
            time.sleep(10)


if __name__ == "__main__":
    print("Stream Updates Examples\n")
    print("1. Single stream updates")
    print("2. Multiple streams updates")
    print("3. Updates with unit conversion")
    print("4. Error handling")
    print("\nUncomment the example you want to run in the code.")
    
    # Uncomment the example you want to run:
    # single_stream_updates_example()
    # multiple_streams_updates_example()
    # updates_with_unit_conversion_example()
    # error_handling_example()
