#!/usr/bin/env python3
"""
Baseline Pipeline Monitor - A simple pipeline monitoring system

This is a basic implementation that candidates should enhance with:
- Better pipeline health tracking
- Intelligent alerting with severity levels
- Anomaly detection for performance issues
- Efficient data structures for high-volume processing
- Dashboard capabilities for visualization
"""

import argparse
import json
import re
from collections import defaultdict, Counter
from datetime import datetime, timedelta
from typing import Dict, List, Any, Tuple, Optional
from enum import Enum
import statistics


class PipelineStatus(Enum):
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    TIMEOUT = "TIMEOUT"
    UNKNOWN = "UNKNOWN"


class AlertSeverity(Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class Alert:
    def __init__(self, pipeline_id: str, severity: AlertSeverity, message: str, 
                 team: str, timestamp: datetime):
        self.pipeline_id = pipeline_id
        self.severity = severity
        self.message = message
        self.team = team
        self.timestamp = timestamp
    
    def to_dict(self):
        return {
            'pipeline_id': self.pipeline_id,
            'severity': self.severity.value,
            'message': self.message,
            'team': self.team,
            'timestamp': self.timestamp.isoformat()
        }


class PipelineExecution:
    def __init__(self, execution_id: str, pipeline_id: str, status: str, 
                 start_time: str, end_time: str, duration: int, 
                 records_processed: int, team: str):
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


class PipelineMonitor:
    """Basic pipeline monitoring system with room for improvement."""
    
    def __init__(self):
        self.executions = []
        self.pipeline_metrics = defaultdict(lambda: {
            'total_executions': 0,
            'successful_executions': 0,
            'failed_executions': 0,
            'avg_duration': 0,
            'avg_records_processed': 0,
            'last_execution': None,
            'team': None
        })
        self.alerts = []
        
    def load_executions(self, data_file: str) -> None:
        """Load pipeline execution data from file."""
        with open(data_file, 'r') as f:
            for line in f:
                if line.strip():
                    data = json.loads(line.strip())
                    execution = PipelineExecution(
                        execution_id=data['execution_id'],
                        pipeline_id=data['pipeline_id'],
                        status=data['status'],
                        start_time=data['start_time'],
                        end_time=data['end_time'],
                        duration=data['duration'],
                        records_processed=data['records_processed'],
                        team=data['team']
                    )
                    self.execution.append(execution)
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
        """Detect anomalies in pipeline executions.
        
        This is a basic implementation - candidates should improve this significantly.
        """
        anomalies = []
        
        for pipeline_id, metrics in self.pipeline_metrics.items():
            # Simple anomaly: pipelines with <80% success rate
            if metrics['total_executions'] >= 5:  # Only check pipelines with enough data
                success_rate = (metrics['successful_executions'] / metrics['total_executions']) * 100
                
                if success_rate < 80:
                    severity = AlertSeverity.HIGH if success_rate < 50 else AlertSeverity.MEDIUM
                    alert = Alert(
                        pipeline_id=pipeline_id,
                        severity=severity,
                        message=f"Low success rate: {success_rate:.1f}%",
                        team=metrics['team'],
                        timestamp=datetime.now()
                    )
                    anomalies.append(alert)
        
        return anomalies
    
    def get_team_metrics(self) -> Dict[str, Dict[str, Any]]:
        """Get metrics aggregated by team."""
        team_metrics = defaultdict(lambda: {
            'total_pipelines': 0,
            'total_executions': 0,
            'successful_executions': 0,
            'failed_executions': 0,
            'avg_success_rate': 0
        })
        
        for pipeline_id, metrics in self.pipeline_metrics.items():
            team = metrics['team']
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


def main():
    parser = argparse.ArgumentParser(description='Monitor data pipeline executions')
    parser.add_argument('--data-file', required=True, help='Path to pipeline execution data file')
    parser.add_argument('--query', required=True, 
                       choices=['pipeline_health', 'anomalies', 'team_metrics', 'performance_trends', 'total_executions'],
                       help='Type of analysis to perform')
    parser.add_argument('--pipeline-id', help='Specific pipeline ID for targeted queries')
    parser.add_argument('--days', type=int, default=7, help='Number of days for trend analysis')
    
    args = parser.parse_args()
    
    monitor = PipelineMonitor()
    monitor.load_executions(args.data_file)
    
    kwargs = {}
    if args.pipeline_id:
        kwargs['pipeline_id'] = args.pipeline_id
    if args.days:
        kwargs['days'] = args.days
    
    result = monitor.query(args.query, **kwargs)
    
    if isinstance(result, (dict, list)):
        print(json.dumps(result, indent=2, default=str))
    else:
        print(result)


if __name__ == "__main__":
    main()
