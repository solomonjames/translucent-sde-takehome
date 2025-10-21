"""Anomaly detection logic for pipeline metrics."""

import statistics
from datetime import datetime
from typing import Dict, List, Any, Callable, Optional

from ..models import Alert
from ..enums import AlertSeverity


class AnomalyDetector:
    """Detects anomalies in pipeline metrics using statistical methods."""

    def __init__(self, min_executions: int = 5, min_data_points: int = 3):
        self.min_executions = min_executions
        self.min_data_points = min_data_points

    def _collect_metric_values(self, pipeline_metrics: Dict[str, Dict[str, Any]], metric_key: str, condition: Optional[Callable[[float], bool]] = None) -> List[float]:
        """Collect metric values for pipelines meeting criteria."""
        values = []
        for metrics in pipeline_metrics.values():
            if metrics['total_executions'] >= self.min_executions:
                value = metrics.get(metric_key)
                if value is not None and (condition is None or condition(value)):
                    values.append(value)
        return values

    def _detect_anomalies(self, pipeline_metrics: Dict[str, Dict[str, Any]], values: List[float], metric_key: str,
                          threshold_func: Callable[[float, float], float], message_func: Callable[[float, float, float], str],
                          severity_func: Callable[[float], AlertSeverity], check_func: Callable[[float, float, float], bool]) -> List[Alert]:
        """Generic anomaly detection for a metric."""
        anomalies = []
        if len(values) >= self.min_data_points:
            mean_val = statistics.mean(values)
            stdev_val = statistics.stdev(values)
            threshold = threshold_func(mean_val, stdev_val)

            for pipeline_id, metrics in pipeline_metrics.items():
                if metrics['total_executions'] >= self.min_executions:
                    if metric_key == 'success_rate':
                        value = (metrics['successful_executions'] / metrics['total_executions']) * 100
                    else:
                        value = metrics.get(metric_key)
                    if value is not None and check_func(mean_val, stdev_val, value):
                        deviation = abs(mean_val - value)
                        severity = severity_func(deviation)
                        message = message_func(value, threshold, mean_val)
                        alert = Alert(
                            pipeline_id=pipeline_id,
                            severity=severity,
                            message=message,
                            team=metrics['team'] or 'unknown',
                            timestamp=datetime.now()
                        )
                        anomalies.append(alert)
        return anomalies

    def detect_success_rate_anomalies(self, pipeline_metrics: Dict[str, Dict[str, Any]]) -> List[Alert]:
        """Detect anomalies in success rates."""
        # Build comparison population from computed success rates per pipeline
        values: List[float] = []
        for m in pipeline_metrics.values():
            total = m.get('total_executions', 0)
            if total >= self.min_executions and total > 0:
                sr = (m.get('successful_executions', 0) / total) * 100.0
                values.append(sr)

        def threshold_func(mean: float, stdev: float) -> float:
            # If stdev is zero (all equal), allow a small margin to still flag clearly low performers
            adj_stdev = stdev if stdev > 0 else 5.0
            return mean - 1 * adj_stdev

        def check_func(mean: float, stdev: float, value: float) -> bool:
            return value < threshold_func(mean, stdev)

        def message_func(value: float, threshold: float, mean: float) -> str:
            return f"Anomalous success rate: {value:.1f}% (threshold: {threshold:.1f}%, peer mean: {mean:.1f}%)"

        def severity_func(deviation: float) -> AlertSeverity:
            return AlertSeverity.HIGH if deviation > 30 else AlertSeverity.MEDIUM

        return self._detect_anomalies(
            pipeline_metrics,
            values,
            'success_rate',
            threshold_func,
            message_func,
            severity_func,
            check_func,
        )

    def detect_duration_anomalies(self, pipeline_metrics: Dict[str, Dict[str, Any]]) -> List[Alert]:
        """Detect anomalies in average durations."""
        def condition(value): return value > 0
        def threshold_func(mean, stdev): return mean + 2 * stdev
        def check_func(mean, stdev, value): return value > threshold_func(mean, stdev)
        def message_func(value, threshold, mean): return f"Anomalous high duration: {value:.1f}s (threshold: {threshold:.1f}s)"
        def severity_func(deviation): return AlertSeverity.HIGH if deviation > 100 else AlertSeverity.MEDIUM

        values = self._collect_metric_values(pipeline_metrics, 'avg_duration', condition)
        return self._detect_anomalies(pipeline_metrics, values, 'avg_duration', threshold_func, message_func, severity_func, check_func)

    def detect_records_anomalies(self, pipeline_metrics: Dict[str, Dict[str, Any]]) -> List[Alert]:
        """Detect anomalies in average records processed."""
        def condition(value): return value > 0
        def threshold_func(mean, stdev): return mean - 2 * stdev
        def check_func(mean, stdev, value): return value < threshold_func(mean, stdev)
        def message_func(value, threshold, mean): return f"Anomalous low records processed: {value:.1f} (threshold: {threshold:.1f})"
        def severity_func(deviation): return AlertSeverity.HIGH if deviation > 1000 else AlertSeverity.MEDIUM

        values = self._collect_metric_values(pipeline_metrics, 'avg_records_processed', condition)
        return self._detect_anomalies(pipeline_metrics, values, 'avg_records_processed', threshold_func, message_func, severity_func, check_func)

    def detect_all_anomalies(self, pipeline_metrics: Dict[str, Dict[str, Any]]) -> List[Alert]:
        """Detect all types of anomalies."""
        anomalies = []
        anomalies.extend(self.detect_success_rate_anomalies(pipeline_metrics))
        anomalies.extend(self.detect_duration_anomalies(pipeline_metrics))
        anomalies.extend(self.detect_records_anomalies(pipeline_metrics))
        return anomalies
