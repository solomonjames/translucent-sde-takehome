"""Anomaly detection logic for pipeline metrics.

This module provides the AnomalyDetector which can flag unusual metrics
for data pipelines (success rate, duration, and records processed).
The focus is on simple statistical thresholds over peer pipelines.
"""

import statistics
from datetime import datetime
from typing import Dict, List, Any, Callable, Optional

from ..models import Alert
from ..enums import AlertSeverity


class AnomalyDetector:
    """Detects anomalies in pipeline metrics using statistical methods.

    Minimal state is kept; detector is configured with thresholds like the
    minimum number of executions and minimum size of the peer population.
    """

    # Centralize magic numbers to improve readability/maintainability
    STDEV_FALLBACK_SR: float = 5.0  # Used when stdev is 0 for success-rate
    SR_SIGMA: float = 1.0
    SR_SEVERITY_HIGH_DELTA: float = 30.0  # percentage points away from mean

    DURATION_SIGMA: float = 2.0
    DURATION_SEVERITY_HIGH_DELTA: float = 100.0  # seconds away from mean

    RECORDS_SIGMA: float = 2.0
    RECORDS_SEVERITY_HIGH_DELTA: float = 1000.0  # records away from mean

    def __init__(self, min_executions: int = 5, min_data_points: int = 3):
        self.min_executions = min_executions
        self.min_data_points = min_data_points

    def _collect_metric_values(
        self,
        pipeline_metrics: Dict[str, Dict[str, Any]],
        metric_key: str,
        condition: Optional[Callable[[float], bool]] = None,
    ) -> List[float]:
        """Collect metric values for pipelines meeting criteria.

        Only includes pipelines with at least `min_executions` and where the
        optional condition holds for the metric value.
        """
        values: List[float] = []
        for metrics in pipeline_metrics.values():
            if metrics.get('total_executions', 0) >= self.min_executions:
                value = metrics.get(metric_key)
                if value is not None and (condition is None or condition(value)):
                    values.append(value)
        return values

    def _detect_anomalies(
        self,
        pipeline_metrics: Dict[str, Dict[str, Any]],
        values: List[float],
        metric_key: str,
        threshold_func: Callable[[float, float], float],
        message_func: Callable[[float, float, float], str],
        severity_func: Callable[[float], AlertSeverity],
        check_func: Callable[[float, float, float], bool],
    ) -> List[Alert]:
        """Generic anomaly detection for a metric.

        - Builds peer mean/stdev from the provided `values`.
        - For each pipeline that meets execution minimums, computes the
          metric value (with special handling for success rate), checks it
          against the threshold, and emits an Alert if anomalous.
        """
        anomalies: List[Alert] = []
        if len(values) < self.min_data_points:
            return anomalies

        mean_val = statistics.mean(values)
        stdev_val = statistics.stdev(values)
        threshold = threshold_func(mean_val, stdev_val)

        for pipeline_id, metrics in pipeline_metrics.items():
            if metrics.get('total_executions', 0) < self.min_executions:
                continue

            if metric_key == 'success_rate':
                total = metrics.get('total_executions', 0)
                succ = metrics.get('successful_executions', 0)
                value = (succ / total) * 100 if total > 0 else None
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
                    team=(metrics.get('team') or 'unknown'),
                    timestamp=datetime.now(),
                )
                anomalies.append(alert)

        return anomalies

    def detect_success_rate_anomalies(self, pipeline_metrics: Dict[str, Dict[str, Any]]) -> List[Alert]:
        """Detect anomalies in success rates.

        The peer population is the set of pipelines meeting min_executions,
        using their success-rate percentage as the sample.
        """
        # Build comparison population from computed success rates per pipeline
        values: List[float] = [
            (m.get('successful_executions', 0) / total) * 100.0
            for m in pipeline_metrics.values()
            if (total := m.get('total_executions', 0)) >= self.min_executions and total > 0
        ]

        def threshold_func(mean: float, stdev: float) -> float:
            # If stdev is zero (all equal), allow a small margin to still flag clearly low performers
            adj_stdev = stdev if stdev > 0 else self.STDEV_FALLBACK_SR
            return mean - self.SR_SIGMA * adj_stdev

        def check_func(mean: float, stdev: float, value: float) -> bool:
            return value < threshold_func(mean, stdev)

        def message_func(value: float, threshold: float, mean: float) -> str:
            return (
                f"Anomalous success rate: {value:.1f}% (threshold: {threshold:.1f}%, "
                f"peer mean: {mean:.1f}%)"
            )

        def severity_func(deviation: float) -> AlertSeverity:
            return (
                AlertSeverity.HIGH
                if deviation > self.SR_SEVERITY_HIGH_DELTA
                else AlertSeverity.MEDIUM
            )

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

        def condition(value: float) -> bool:
            return value > 0

        def threshold_func(mean: float, stdev: float) -> float:
            return mean + self.DURATION_SIGMA * stdev

        def check_func(mean: float, stdev: float, value: float) -> bool:
            return value > threshold_func(mean, stdev)

        def message_func(value: float, threshold: float, mean: float) -> str:
            return f"Anomalous high duration: {value:.1f}s (threshold: {threshold:.1f}s)"

        def severity_func(deviation: float) -> AlertSeverity:
            return (
                AlertSeverity.HIGH
                if deviation > self.DURATION_SEVERITY_HIGH_DELTA
                else AlertSeverity.MEDIUM
            )

        values = self._collect_metric_values(pipeline_metrics, 'avg_duration', condition)
        return self._detect_anomalies(
            pipeline_metrics,
            values,
            'avg_duration',
            threshold_func,
            message_func,
            severity_func,
            check_func,
        )

    def detect_records_anomalies(self, pipeline_metrics: Dict[str, Dict[str, Any]]) -> List[Alert]:
        """Detect anomalies in average records processed."""

        def condition(value: float) -> bool:
            return value > 0

        def threshold_func(mean: float, stdev: float) -> float:
            return mean - self.RECORDS_SIGMA * stdev

        def check_func(mean: float, stdev: float, value: float) -> bool:
            return value < threshold_func(mean, stdev)

        def message_func(value: float, threshold: float, mean: float) -> str:
            return f"Anomalous low records processed: {value:.1f} (threshold: {threshold:.1f})"

        def severity_func(deviation: float) -> AlertSeverity:
            return (
                AlertSeverity.HIGH
                if deviation > self.RECORDS_SEVERITY_HIGH_DELTA
                else AlertSeverity.MEDIUM
            )

        values = self._collect_metric_values(pipeline_metrics, 'avg_records_processed', condition)
        return self._detect_anomalies(
            pipeline_metrics,
            values,
            'avg_records_processed',
            threshold_func,
            message_func,
            severity_func,
            check_func,
        )

    def detect_all_anomalies(self, pipeline_metrics: Dict[str, Dict[str, Any]]) -> List[Alert]:
        """Detect all types of anomalies."""
        anomalies: List[Alert] = []
        anomalies.extend(self.detect_success_rate_anomalies(pipeline_metrics))
        anomalies.extend(self.detect_duration_anomalies(pipeline_metrics))
        anomalies.extend(self.detect_records_anomalies(pipeline_metrics))
        return anomalies
