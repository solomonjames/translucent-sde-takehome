from datetime import datetime

from src.enums import AlertSeverity


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
