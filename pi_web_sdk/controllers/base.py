"""Shared helpers for PI Web API controllers."""

from __future__ import annotations

import urllib.parse

__all__ = ['BaseController']

class BaseController:
    """Base controller class."""

    def __init__(self, client: "PIWebAPIClient"):
        self.client = client

    def _encode_path(self, path: str) -> str:
        """URL encode a path parameter."""
        return urllib.parse.quote(path, safe="")
