"""Controllers for metrics endpoints."""

from __future__ import annotations

from typing import Dict, Optional

from .base import BaseController

__all__ = [
    'MetricsController',
]


class MetricsController(BaseController):
    """Controller for Metrics operations."""

    def environment(self) -> Dict:
        """Get environment metrics."""
        return self.client.get("metrics/environment")

    def landing(self) -> Dict:
        """Get landing page metrics."""
        return self.client.get("metrics/landing")

    def requests(
        self,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        interval: Optional[str] = None,
    ) -> Dict:
        """Get request metrics."""
        params = {}
        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time
        if interval:
            params["interval"] = interval
        return self.client.get("metrics/requests", params=params)