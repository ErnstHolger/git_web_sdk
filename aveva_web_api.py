"""Legacy entry point for the PI Web API SDK.

This module re-exports the public interfaces from the reorganised
`pi_web_sdk` package."""

from __future__ import annotations

from pi_web_sdk import (
    AuthMethod,
    PIWebAPIClient,
    PIWebAPIConfig,
    PIWebAPIError,
    WebIDType,
)

__all__ = ['PIWebAPIClient', 'PIWebAPIConfig', 'PIWebAPIError', 'AuthMethod', 'WebIDType']

if __name__ == '__main__':
    print('PI Web API SDK refactored into the pi_web_sdk package. See README.md for usage guidance.')
