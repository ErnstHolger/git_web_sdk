"""Controllers for OMF (OCS Message Format) endpoints."""

from __future__ import annotations

from typing import Dict, Optional

from .base import BaseController

__all__ = [
    'OmfController',
]


class OmfController(BaseController):
    """Controller for OMF operations."""

    def post_async(
        self,
        data: Dict,
        message_type: Optional[str] = None,
        omf_version: Optional[str] = None,
        action: Optional[str] = None,
        data_server_web_id: Optional[str] = None,
    ) -> Dict:
        """Send OMF data asynchronously.
        
        Args:
            data: The OMF message data
            message_type: Type of OMF message (Type, Container, Data)
            omf_version: OMF version
            action: Action to perform (create, update, delete)
            data_server_web_id: WebID of the target data server
        """
        headers = {}
        if message_type:
            headers["messagetype"] = message_type
        if omf_version:
            headers["omfversion"] = omf_version
        if action:
            headers["action"] = action
        
        params = {}
        if data_server_web_id:
            params["dataServerWebId"] = data_server_web_id
            
        return self.client.post("omf", data=data, headers=headers, params=params)