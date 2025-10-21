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

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import argparse
import json

from collections import defaultdict
from enum import Enum

from src.monitors import PipelineMonitor


def main():
    parser = argparse.ArgumentParser(description='Monitor data pipeline executions')
    parser.add_argument('--data-file', required=True, help='Path to pipeline execution data file')
    parser.add_argument('--query', required=True,
                       choices=['pipeline_health', 'anomalies', 'team_metrics', 'performance_trends', 'total_executions'],
                       help='Type of analysis to perform')
    parser.add_argument('--pipeline-id', help='Specific pipeline ID for targeted queries')
    parser.add_argument('--days', type=int, default=7, help='Number of days for trend analysis')

    args = parser.parse_args()

    # Basic input validation
    if args.data_file is None or not os.path.isfile(args.data_file):
        print(json.dumps({"error": f"Data file not found: {args.data_file}"}))
        sys.exit(1)
    if args.days is None or args.days <= 0:
        # Validate and correct invalid 'days' input
        print(json.dumps({"error": "Invalid 'days' value provided; defaulting to 7"}))
        args.days = 7

    monitor = PipelineMonitor()
    try:
        monitor.load_executions(args.data_file)
    except Exception as e:
        # Error handling for unexpected exceptions during load
        print(json.dumps({"error": f"Exception while loading data: {str(e)}"}))
        sys.exit(1)

    kwargs = {}
    if args.pipeline_id:
        kwargs['pipeline_id'] = args.pipeline_id
    if args.days:
        kwargs['days'] = args.days

    try:
        result = monitor.query(args.query, **kwargs)
    except Exception as e:
        print(json.dumps({"error": f"Exception while executing query: {str(e)}"}))
        sys.exit(1)

    if isinstance(result, (dict, list)):
        print(json.dumps(result, indent=2, default=str))
    else:
        print(result)


if __name__ == "__main__":
    main()
