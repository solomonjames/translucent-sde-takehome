"""Pipeline monitoring logic."""

import json
import statistics
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

from ..enums import PipelineStatus, AlertSeverity
from ..models import Alert, PipelineExecution
from .anomaly_detector import AnomalyDetector


class PipelineMonitor:
    """Basic pipeline monitoring system."""

    def __init__(self):
        self.executions: List[PipelineExecution] = []
        self.pipeline_metrics: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
            'total_executions': 0,
            'successful_executions': 0,
            'failed_executions': 0,
            'avg_duration': 0.0,
            'avg_records_processed': 0.0,
            'last_execution': None,
            'team': None
        })
        self.alerts: List[Alert] = []
        self.anomaly_detector = AnomalyDetector()

    def load_executions(self, data_file: str) -> None:
        """Load pipeline execution data from file."""
        with open(data_file, 'r') as f:
            for line in f:
                if line.strip():
                    data = json.loads(line.strip())
                    execution = PipelineExecution.from_json(data)
                    self.executions.append(execution)
                    self._update_pipeline_metrics(execution)

    def _update_pipeline_metrics(self, execution: PipelineExecution) -> None:
        """Update metrics for a pipeline based on execution data."""
        metrics = self.pipeline_metrics[execution.pipeline_id]
        metrics['total_executions'] += 1
        metrics['team'] = execution.team

        if execution.status == PipelineStatus.SUCCESS:
            metrics['successful_executions'] += 1
        elif execution.status == PipelineStatus.FAILED:
            metrics['failed_executions'] += 1

        # Update averages (simplified - candidates should improve this)
        if execution.duration and execution.duration > 0:
            current_avg = metrics['avg_duration']
            total_execs = metrics['total_executions']
            metrics['avg_duration'] = ((current_avg * (total_execs - 1)) + execution.duration) / total_execs

        if execution.records_processed and execution.records_processed > 0:
            current_avg = metrics['avg_records_processed']
            total_execs = metrics['total_executions']
            metrics['avg_records_processed'] = ((current_avg * (total_execs - 1)) + execution.records_processed) / total_execs

        metrics['last_execution'] = execution.start_time

    def get_pipeline_health(self, pipeline_id: Optional[str] = None) -> Dict[str, Any]:
        """Get health status for a specific pipeline or all pipelines."""
        if pipeline_id:
            if pipeline_id not in self.pipeline_metrics:
                return {'error': f'Pipeline {pipeline_id} not found'}

            metrics = self.pipeline_metrics[pipeline_id]
            success_rate = (metrics['successful_executions'] / metrics['total_executions'] * 100) if metrics['total_executions'] > 0 else 0

            return {
                'pipeline_id': pipeline_id,
                'success_rate': success_rate,
                'total_executions': metrics['total_executions'],
                'failed_executions': metrics['failed_executions'],
                'avg_duration': metrics['avg_duration'],
                'avg_records_processed': metrics['avg_records_processed'],
                'last_execution': metrics['last_execution'].isoformat() if metrics['last_execution'] else None,
                'team': metrics['team']
            }
        else:
            # Return health for all pipelines
            health_summary = {}
            for pid, metrics in self.pipeline_metrics.items():
                success_rate = (metrics['successful_executions'] / metrics['total_executions'] * 100) if metrics['total_executions'] > 0 else 0
                health_summary[pid] = {
                    'success_rate': success_rate,
                    'total_executions': metrics['total_executions'],
                    'team': metrics['team']
                }
            return health_summary

    def detect_anomalies(self) -> List[Alert]:
        """Detect anomalies in pipeline executions using statistical thresholds."""
        return self.anomaly_detector.detect_all_anomalies(self.pipeline_metrics)

    def get_team_metrics(self) -> Dict[str, Dict[str, Any]]:
        """Get metrics aggregated by team."""
        team_metrics: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
            'total_pipelines': 0,
            'total_executions': 0,
            'successful_executions': 0,
            'failed_executions': 0,
            'avg_success_rate': 0.0
        })

        for pipeline_id, metrics in self.pipeline_metrics.items():
            team = metrics['team'] or 'unknown'
            team_metrics[team]['total_pipelines'] += 1
            team_metrics[team]['total_executions'] += metrics['total_executions']
            team_metrics[team]['successful_executions'] += metrics['successful_executions']
            team_metrics[team]['failed_executions'] += metrics['failed_executions']

        # Calculate average success rates
        for team, metrics in team_metrics.items():
            if metrics['total_executions'] > 0:
                metrics['avg_success_rate'] = (metrics['successful_executions'] / metrics['total_executions']) * 100

        return dict(team_metrics)

    def get_performance_trends(self, pipeline_id: str, days: int = 7) -> Dict[str, Any]:
        """Get performance trends for a pipeline over the last N days."""
        cutoff_date = datetime.now() - timedelta(days=days)
        # Make cutoff_date timezone-aware to match execution times
        cutoff_date = cutoff_date.replace(tzinfo=self.executions[0].start_time.tzinfo) if self.executions else cutoff_date

        recent_executions = [
            exec for exec in self.executions
            if exec.pipeline_id == pipeline_id and exec.start_time >= cutoff_date
        ]

        if not recent_executions:
            return {'error': f'No executions found for {pipeline_id} in the last {days} days'}

        durations = [exec.duration for exec in recent_executions if exec.duration > 0]
        success_count = sum(1 for exec in recent_executions if exec.status == PipelineStatus.SUCCESS)

        return {
            'pipeline_id': pipeline_id,
            'total_executions': len(recent_executions),
            'success_rate': (success_count / len(recent_executions)) * 100,
            'avg_duration': statistics.mean(durations) if durations else 0,
            'min_duration': min(durations) if durations else 0,
            'max_duration': max(durations) if durations else 0,
            'days_analyzed': days
        }

    def query(self, query_type: str, **kwargs) -> Any:
        """Execute a query on the pipeline data."""
        if query_type == "pipeline_health":
            return self.get_pipeline_health(kwargs.get('pipeline_id'))
        elif query_type == "anomalies":
            return [alert.to_dict() for alert in self.detect_anomalies()]
        elif query_type == "team_metrics":
            return self.get_team_metrics()
        elif query_type == "performance_trends":
            return self.get_performance_trends(kwargs.get('pipeline_id', ''), kwargs.get('days', 7))
        elif query_type == "total_executions":
            return len(self.executions)
        else:
            return f"Unknown query: {query_type}"
