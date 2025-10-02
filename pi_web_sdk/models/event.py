"""Data models for event frame-related PI Web API objects."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Any, Optional, List
from .base import PIWebAPIObject


__all__ = [
    "EventFrame",
    "EventFrameCategory",
]


@dataclass
class EventFrame(PIWebAPIObject):
    """PI AF Event Frame object."""
    
    acknowledge_date: Optional[str] = None
    acknowledged_by: Optional[str] = None
    are_values_captured: Optional[bool] = None
    can_be_acknowledged: Optional[bool] = None
    category_names: Optional[List[str]] = None
    end_time: Optional[str] = None
    has_children: Optional[bool] = None
    is_acknowledged: Optional[bool] = None
    is_annotation: Optional[bool] = None
    is_locked: Optional[bool] = None
    referenced_element_web_id: Optional[str] = None
    security_descriptor: Optional[str] = None
    severity: Optional[str] = None
    start_time: Optional[str] = None
    template_name: Optional[str] = None
    
    def to_dict(self, exclude_none: bool = True) -> Dict[str, Any]:
        """Convert to PI Web API format."""
        result = super().to_dict(exclude_none)
        
        if self.acknowledge_date is not None or not exclude_none:
            result['AcknowledgeDate'] = self.acknowledge_date
        if self.acknowledged_by is not None or not exclude_none:
            result['AcknowledgedBy'] = self.acknowledged_by
        if self.are_values_captured is not None or not exclude_none:
            result['AreValuesCaptured'] = self.are_values_captured
        if self.can_be_acknowledged is not None or not exclude_none:
            result['CanBeAcknowledged'] = self.can_be_acknowledged
        if self.category_names is not None or not exclude_none:
            result['CategoryNames'] = self.category_names
        if self.end_time is not None or not exclude_none:
            result['EndTime'] = self.end_time
        if self.has_children is not None or not exclude_none:
            result['HasChildren'] = self.has_children
        if self.is_acknowledged is not None or not exclude_none:
            result['IsAcknowledged'] = self.is_acknowledged
        if self.is_annotation is not None or not exclude_none:
            result['IsAnnotation'] = self.is_annotation
        if self.is_locked is not None or not exclude_none:
            result['IsLocked'] = self.is_locked
        if self.referenced_element_web_id is not None or not exclude_none:
            result['ReferencedElementWebId'] = self.referenced_element_web_id
        if self.security_descriptor is not None or not exclude_none:
            result['SecurityDescriptor'] = self.security_descriptor
        if self.severity is not None or not exclude_none:
            result['Severity'] = self.severity
        if self.start_time is not None or not exclude_none:
            result['StartTime'] = self.start_time
        if self.template_name is not None or not exclude_none:
            result['TemplateName'] = self.template_name
            
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> EventFrame:
        """Create from PI Web API response."""
        base = PIWebAPIObject.from_dict(data)
        return cls(
            web_id=base.web_id,
            id=base.id,
            name=base.name,
            description=base.description,
            path=base.path,
            links=base.links,
            acknowledge_date=data.get('AcknowledgeDate'),
            acknowledged_by=data.get('AcknowledgedBy'),
            are_values_captured=data.get('AreValuesCaptured'),
            can_be_acknowledged=data.get('CanBeAcknowledged'),
            category_names=data.get('CategoryNames'),
            end_time=data.get('EndTime'),
            has_children=data.get('HasChildren'),
            is_acknowledged=data.get('IsAcknowledged'),
            is_annotation=data.get('IsAnnotation'),
            is_locked=data.get('IsLocked'),
            referenced_element_web_id=data.get('ReferencedElementWebId'),
            security_descriptor=data.get('SecurityDescriptor'),
            severity=data.get('Severity'),
            start_time=data.get('StartTime'),
            template_name=data.get('TemplateName'),
        )


@dataclass
class EventFrameCategory(PIWebAPIObject):
    """PI AF Event Frame Category object."""
    
    security_descriptor: Optional[str] = None
    
    def to_dict(self, exclude_none: bool = True) -> Dict[str, Any]:
        """Convert to PI Web API format."""
        result = super().to_dict(exclude_none)
        
        if self.security_descriptor is not None or not exclude_none:
            result['SecurityDescriptor'] = self.security_descriptor
            
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> EventFrameCategory:
        """Create from PI Web API response."""
        base = PIWebAPIObject.from_dict(data)
        return cls(
            web_id=base.web_id,
            id=base.id,
            name=base.name,
            description=base.description,
            path=base.path,
            links=base.links,
            security_descriptor=data.get('SecurityDescriptor'),
        )
