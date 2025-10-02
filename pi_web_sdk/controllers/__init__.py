"""Convenience imports for controller classes."""

from __future__ import annotations

from .system import (
    HomeController,
    SystemController,
    ConfigurationController,
)

from .asset import (
    AssetServerController,
    AssetDatabaseController,
    ElementController,
    ElementCategoryController,
    ElementTemplateController,
)

from .attribute import (
    AttributeController,
    AttributeCategoryController,
    AttributeTemplateController,
)

from .attribute_trait import AttributeTraitController

from .data import (
    DataServerController,
    PointController,
)

from .analysis import (
    AnalysisController,
    AnalysisCategoryController,
    AnalysisRuleController,
    AnalysisTemplateController,
)

from .batch import (
    BatchController,
    CalculationController,
    ChannelController,
)

from .enumeration import (
    EnumerationSetController,
    EnumerationValueController,
)

from .event import EventFrameController

from .stream import (
    StreamController,
    StreamSetController,
)

from .table import TableController, TableCategoryController

from .omf import OmfController

from .security import (
    SecurityIdentityController,
    SecurityMappingController,
)

from .notification import (
    NotificationContactTemplateController,
    NotificationPlugInController,
    NotificationRuleController,
    NotificationRuleSubscriberController,
    NotificationRuleTemplateController,
)

from .time_rule import (
    TimeRuleController,
    TimeRulePlugInController,
)

from .unit import (
    UnitController,
    UnitClassController,
)

from .metrics import MetricsController

__all__ = [
    'HomeController',
    'SystemController',
    'ConfigurationController',
    'AssetServerController',
    'AssetDatabaseController',
    'ElementController',
    'ElementCategoryController',
    'ElementTemplateController',
    'AttributeController',
    'AttributeCategoryController',
    'AttributeTemplateController',
    'AttributeTraitController',
    'DataServerController',
    'PointController',
    'AnalysisController',
    'AnalysisCategoryController',
    'AnalysisRuleController',
    'AnalysisTemplateController',
    'BatchController',
    'CalculationController',
    'ChannelController',
    'EnumerationSetController',
    'EnumerationValueController',
    'EventFrameController',
    'StreamController',
    'StreamSetController',
    'TableController',
    'TableCategoryController',
    'OmfController',
    'SecurityIdentityController',
    'SecurityMappingController',
    'NotificationContactTemplateController',
    'NotificationPlugInController',
    'NotificationRuleController',
    'NotificationRuleSubscriberController',
    'NotificationRuleTemplateController',
    'TimeRuleController',
    'TimeRulePlugInController',
    'UnitController',
    'UnitClassController',
    'MetricsController',
]
