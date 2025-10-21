"""Enums for pipeline monitoring system."""

from enum import Enum


class PipelineStatus(Enum):
    """Enumeration of possible pipeline execution statuses."""
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    TIMEOUT = "TIMEOUT"
    UNKNOWN = "UNKNOWN"


class AlertSeverity(Enum):
    """Enumeration of alert severity levels."""
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"