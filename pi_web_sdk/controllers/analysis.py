"""Controllers for analysis-related endpoints."""

from __future__ import annotations

from .base import BaseController

__all__ = [
    'AnalysisController',
    'AnalysisCategoryController',
    'AnalysisRuleController',
    'AnalysisTemplateController',
]

class AnalysisController(BaseController):
    """Controller for Analysis operations."""

    pass


class AnalysisCategoryController(BaseController):
    """Controller for Analysis Category operations."""

    pass


class AnalysisRuleController(BaseController):
    """Controller for Analysis Rule operations."""

    pass


class AnalysisTemplateController(BaseController):
    """Controller for Analysis Template operations."""

    pass
