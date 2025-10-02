"""Controllers for stream and stream set endpoints."""

from __future__ import annotations

from typing import Dict, List, Optional

from .base import BaseController

__all__ = [
    'StreamController',
    'StreamSetController',
]

class StreamController(BaseController):
    """Controller for Stream operations."""

    def get_value(
        self,
        web_id: str,
        selected_fields: Optional[str] = None,
        time: Optional[str] = None,
        desired_units: Optional[str] = None,
    ) -> Dict:
        """Get current stream value."""
        params = {}
        if selected_fields:
            params["selectedFields"] = selected_fields
        if time:
            params["time"] = time
        if desired_units:
            params["desiredUnits"] = desired_units
        return self.client.get(f"streams/{web_id}/value", params=params)

    def get_recorded(
        self,
        web_id: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        boundary_type: Optional[str] = None,
        max_count: Optional[int] = None,
        include_filtered_values: bool = False,
        filter_expression: Optional[str] = None,
        selected_fields: Optional[str] = None,
        time_zone: Optional[str] = None,
        desired_units: Optional[str] = None,
    ) -> Dict:
        """Get recorded values."""
        params = {"includeFilteredValues": include_filtered_values}
        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time
        if boundary_type:
            params["boundaryType"] = boundary_type
        if max_count:
            params["maxCount"] = max_count
        if filter_expression:
            params["filterExpression"] = filter_expression
        if selected_fields:
            params["selectedFields"] = selected_fields
        if time_zone:
            params["timeZone"] = time_zone
        if desired_units:
            params["desiredUnits"] = desired_units

        return self.client.get(f"streams/{web_id}/recorded", params=params)

    def get_interpolated(
        self,
        web_id: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        interval: Optional[str] = None,
        filter_expression: Optional[str] = None,
        include_filtered_values: bool = False,
        selected_fields: Optional[str] = None,
        time_zone: Optional[str] = None,
        desired_units: Optional[str] = None,
        sync_time: Optional[str] = None,
        sync_time_boundary_type: Optional[str] = None,
    ) -> Dict:
        """Get interpolated values."""
        params = {"includeFilteredValues": include_filtered_values}
        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time
        if interval:
            params["interval"] = interval
        if filter_expression:
            params["filterExpression"] = filter_expression
        if selected_fields:
            params["selectedFields"] = selected_fields
        if time_zone:
            params["timeZone"] = time_zone
        if desired_units:
            params["desiredUnits"] = desired_units
        if sync_time:
            params["syncTime"] = sync_time
        if sync_time_boundary_type:
            params["syncTimeBoundaryType"] = sync_time_boundary_type

        return self.client.get(f"streams/{web_id}/interpolated", params=params)

    def get_plot(
        self,
        web_id: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        intervals: Optional[int] = None,
        selected_fields: Optional[str] = None,
        time_zone: Optional[str] = None,
        desired_units: Optional[str] = None,
    ) -> Dict:
        """Get plot values."""
        params = {}
        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time
        if intervals:
            params["intervals"] = intervals
        if selected_fields:
            params["selectedFields"] = selected_fields
        if time_zone:
            params["timeZone"] = time_zone
        if desired_units:
            params["desiredUnits"] = desired_units

        return self.client.get(f"streams/{web_id}/plot", params=params)

    def get_summary(
        self,
        web_id: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        summary_type: Optional[List[str]] = None,
        summary_duration: Optional[str] = None,
        calculation_basis: Optional[str] = None,
        time_type: Optional[str] = None,
        selected_fields: Optional[str] = None,
        time_zone: Optional[str] = None,
        filter_expression: Optional[str] = None,
    ) -> Dict:
        """Get summary values."""
        params = {}
        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time
        if summary_type:
            params["summaryType"] = summary_type
        if summary_duration:
            params["summaryDuration"] = summary_duration
        if calculation_basis:
            params["calculationBasis"] = calculation_basis
        if time_type:
            params["timeType"] = time_type
        if selected_fields:
            params["selectedFields"] = selected_fields
        if time_zone:
            params["timeZone"] = time_zone
        if filter_expression:
            params["filterExpression"] = filter_expression

        return self.client.get(f"streams/{web_id}/summary", params=params)

    def update_value(
        self,
        web_id: str,
        value: Dict,
        buffer_option: Optional[str] = None,
        update_option: Optional[str] = None,
    ) -> Dict:
        """Update stream value."""
        params = {}
        if buffer_option:
            params["bufferOption"] = buffer_option
        if update_option:
            params["updateOption"] = update_option
        return self.client.put(f"streams/{web_id}/value", data=value, params=params)

    def update_values(
        self,
        web_id: str,
        values: List[Dict],
        buffer_option: Optional[str] = None,
        update_option: Optional[str] = None,
    ) -> Dict:
        """Update multiple stream values."""
        params = {}
        if buffer_option:
            params["bufferOption"] = buffer_option
        if update_option:
            params["updateOption"] = update_option
        return self.client.post(f"streams/{web_id}/recorded", data=values, params=params)

    def register_update(
        self,
        web_id: str,
        selected_fields: Optional[str] = None,
    ) -> Dict:
        """Register for stream updates.

        Registers a stream for incremental updates. Returns a marker that can be used
        to retrieve updates via retrieve_update().

        Args:
            web_id: WebID of the stream
            selected_fields: Optional comma-separated list of fields to include

        Returns:
            Dictionary with LatestMarker and registration status
        """
        params = {}
        if selected_fields:
            params["selectedFields"] = selected_fields
        return self.client.post(f"streams/{web_id}/updates", params=params)

    def retrieve_update(
        self,
        marker: str,
        selected_fields: Optional[str] = None,
        desired_units: Optional[str] = None,
    ) -> Dict:
        """Retrieve stream updates using a marker.

        Gets incremental updates since the last marker position. Response includes
        a new LatestMarker for subsequent queries.

        Args:
            marker: Marker from previous register_update or retrieve_update call
            selected_fields: Optional comma-separated list of fields to include
            desired_units: Optional unit of measure for returned values

        Returns:
            Dictionary with Items (updates) and LatestMarker
        """
        params = {}
        if selected_fields:
            params["selectedFields"] = selected_fields
        if desired_units:
            params["desiredUnits"] = desired_units
        return self.client.get(f"streams/updates/{marker}", params=params)


class StreamSetController(BaseController):
    """Controller for Stream Set operations."""

    def get_values(
        self,
        web_ids: List[str],
        selected_fields: Optional[str] = None,
        time: Optional[str] = None,
        desired_units: Optional[str] = None,
    ) -> Dict:
        """Get current values for multiple streams."""
        params = {"webId": web_ids}
        if selected_fields:
            params["selectedFields"] = selected_fields
        if time:
            params["time"] = time
        if desired_units:
            params["desiredUnits"] = desired_units
        return self.client.get("streamsets/value", params=params)

    def get_recorded(
        self,
        web_ids: List[str],
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        boundary_type: Optional[str] = None,
        max_count: Optional[int] = None,
        include_filtered_values: bool = False,
        filter_expression: Optional[str] = None,
        selected_fields: Optional[str] = None,
        time_zone: Optional[str] = None,
        desired_units: Optional[str] = None,
    ) -> Dict:
        """Get recorded values for multiple streams."""
        params = {"webId": web_ids, "includeFilteredValues": include_filtered_values}
        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time
        if boundary_type:
            params["boundaryType"] = boundary_type
        if max_count:
            params["maxCount"] = max_count
        if filter_expression:
            params["filterExpression"] = filter_expression
        if selected_fields:
            params["selectedFields"] = selected_fields
        if time_zone:
            params["timeZone"] = time_zone
        if desired_units:
            params["desiredUnits"] = desired_units

        return self.client.get("streamsets/recorded", params=params)

    def get_interpolated(
        self,
        web_ids: List[str],
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        interval: Optional[str] = None,
        filter_expression: Optional[str] = None,
        include_filtered_values: bool = False,
        selected_fields: Optional[str] = None,
        time_zone: Optional[str] = None,
        desired_units: Optional[str] = None,
        sync_time: Optional[str] = None,
        sync_time_boundary_type: Optional[str] = None,
    ) -> Dict:
        """Get interpolated values for multiple streams."""
        params = {"webId": web_ids, "includeFilteredValues": include_filtered_values}
        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time
        if interval:
            params["interval"] = interval
        if filter_expression:
            params["filterExpression"] = filter_expression
        if selected_fields:
            params["selectedFields"] = selected_fields
        if time_zone:
            params["timeZone"] = time_zone
        if desired_units:
            params["desiredUnits"] = desired_units
        if sync_time:
            params["syncTime"] = sync_time
        if sync_time_boundary_type:
            params["syncTimeBoundaryType"] = sync_time_boundary_type

        return self.client.get("streamsets/interpolated", params=params)

    def get_plot(
        self,
        web_ids: List[str],
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        intervals: Optional[int] = None,
        selected_fields: Optional[str] = None,
        time_zone: Optional[str] = None,
        desired_units: Optional[str] = None,
    ) -> Dict:
        """Get plot values for multiple streams."""
        params = {"webId": web_ids}
        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time
        if intervals:
            params["intervals"] = intervals
        if selected_fields:
            params["selectedFields"] = selected_fields
        if time_zone:
            params["timeZone"] = time_zone
        if desired_units:
            params["desiredUnits"] = desired_units

        return self.client.get("streamsets/plot", params=params)

    def get_summaries(
        self,
        web_ids: List[str],
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        summary_type: Optional[List[str]] = None,
        summary_duration: Optional[str] = None,
        calculation_basis: Optional[str] = None,
        time_type: Optional[str] = None,
        selected_fields: Optional[str] = None,
        time_zone: Optional[str] = None,
        filter_expression: Optional[str] = None,
    ) -> Dict:
        """Get summary values for multiple streams."""
        params = {"webId": web_ids}
        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time
        if summary_type:
            params["summaryType"] = summary_type
        if summary_duration:
            params["summaryDuration"] = summary_duration
        if calculation_basis:
            params["calculationBasis"] = calculation_basis
        if time_type:
            params["timeType"] = time_type
        if selected_fields:
            params["selectedFields"] = selected_fields
        if time_zone:
            params["timeZone"] = time_zone
        if filter_expression:
            params["filterExpression"] = filter_expression

        return self.client.get("streamsets/summaries", params=params)

    def update_values(self, updates: List[Dict]) -> Dict:
        """Update values for multiple streams.

        Args:
            updates: List of dicts with 'WebId' and 'Value' keys
        """
        return self.client.put("streamsets/value", data=updates)

    def register_updates(
        self,
        web_ids: List[str],
        selected_fields: Optional[str] = None,
    ) -> Dict:
        """Register multiple streams for updates.

        Registers multiple streams for incremental updates. Returns markers that can be used
        to retrieve updates via retrieve_updates().

        Args:
            web_ids: List of stream WebIDs to register
            selected_fields: Optional comma-separated list of fields to include

        Returns:
            Dictionary with Items containing registration status for each stream and LatestMarker
        """
        params = {"webId": web_ids}
        if selected_fields:
            params["selectedFields"] = selected_fields
        return self.client.post("streamsets/updates", params=params)

    def retrieve_updates(
        self,
        marker: str,
        selected_fields: Optional[str] = None,
        desired_units: Optional[str] = None,
    ) -> Dict:
        """Retrieve updates for multiple streams using a marker.

        Gets incremental updates for all registered streams since the last marker position.
        Response includes a new LatestMarker for subsequent queries.

        Args:
            marker: Marker from previous register_updates or retrieve_updates call
            selected_fields: Optional comma-separated list of fields to include
            desired_units: Optional unit of measure for returned values

        Returns:
            Dictionary with Items (updates per stream) and LatestMarker
        """
        params = {"marker": marker}
        if selected_fields:
            params["selectedFields"] = selected_fields
        if desired_units:
            params["desiredUnits"] = desired_units
        return self.client.get("streamsets/updates", params=params)
