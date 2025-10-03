"""Shared helpers for PI Web API controllers."""

from __future__ import annotations

import urllib.parse
from datetime import datetime
from typing import Optional, Union

__all__ = ['BaseController']

class BaseController:
    """Base controller class."""

    def __init__(self, client: "PIWebAPIClient"):
        self.client = client

    def _encode_path(self, path: str) -> str:
        """URL encode a path parameter."""
        return urllib.parse.quote(path, safe="")

    def _format_time(self, time_value: Union[str, datetime, None]) -> Optional[str]:
        """Convert time value to PI Web API compatible string format.

        Args:
            time_value: Time as string, datetime object, or None

        Returns:
            Formatted time string or None if input is None

        Notes:
            - String values are returned as-is (allows special values like "*", "Today", etc.)
            - datetime objects are converted to ISO 8601 format
            - Timezone-aware datetimes include timezone info
            - Timezone-naive datetimes are treated as local time
        """
        if time_value is None:
            return None
        if isinstance(time_value, str):
            return time_value
        if isinstance(time_value, datetime):
            # Use isoformat() which produces ISO 8601 compliant strings
            # Format: YYYY-MM-DDTHH:MM:SS[.ffffff][+HH:MM]
            return time_value.isoformat()
        raise TypeError(f"Time value must be str, datetime, or None, got {type(time_value)}")
