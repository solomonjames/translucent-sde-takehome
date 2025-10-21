"""Data models for pipeline monitoring system."""

from datetime import datetime
from typing import Optional

from src.enums import PipelineStatus, AlertSeverity


class Alert:
    """Represents an alert for pipeline issues."""

    def __init__(self, pipeline_id: str, severity: AlertSeverity, message: str,
                 team: str, timestamp: datetime):
        self.pipeline_id = pipeline_id
        self.severity = severity
        self.message = message
        self.team = team
        self.timestamp = timestamp

    def to_dict(self) -> dict:
        """Convert alert to dictionary for serialization."""
        return {
            'pipeline_id': self.pipeline_id,
            'severity': self.severity.value,
            'message': self.message,
            'team': self.team,
            'timestamp': self.timestamp.isoformat()
        }


class PipelineExecution:
    """Represents a single pipeline execution."""

    def __init__(self, execution_id: str, pipeline_id: str, status: str,
                 start_time: str, end_time: str, duration: Optional[int],
                 records_processed: Optional[int], team: str):
        self.execution_id = execution_id
        self.pipeline_id = pipeline_id
        # Handle unknown statuses gracefully
        try:
            self.status = PipelineStatus(status)
        except ValueError:
            self.status = PipelineStatus.UNKNOWN
        self.start_time = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
        self.end_time = datetime.fromisoformat(end_time.replace('Z', '+00:00')) if end_time else None
        self.duration = duration if duration is not None else 0  # seconds
        self.records_processed = records_processed if records_processed is not None else 0
        self.team = team

    @classmethod
    def from_json(cls, data: dict) -> 'PipelineExecution':
        """Factory method to create a PipelineExecution from JSON data.

        Args:
            data: Dictionary containing execution data

        Returns:
            PipelineExecution instance
        """
        return cls(
            execution_id=data['execution_id'],
            pipeline_id=data['pipeline_id'],
            status=data['status'],
            start_time=data['start_time'],
            end_time=data['end_time'],
            duration=data['duration'],
            records_processed=data['records_processed'],
            team=data['team']
        )
