#!/usr/bin/env python3
"""
Evaluation script for the pipeline monitoring system.

Tests various aspects:
1. Basic functionality (pipeline health tracking)
2. Anomaly detection accuracy
3. Team metrics aggregation
4. Performance with large datasets
5. Data structure efficiency
6. Code quality and robustness
"""

import subprocess
import json
import time
import sys
from typing import Dict, Any, List


def run_monitor(query: str, **kwargs) -> Any:
    """Run the monitor with a specific query."""
    try:
        cmd = [
            'python3', 'baseline_pipeline_monitor.py', 
            '--data-file', 'data/pipeline_executions.jsonl',
            '--query', query
        ]
        
        # Add optional arguments
        if 'pipeline_id' in kwargs:
            cmd.extend(['--pipeline-id', kwargs['pipeline_id']])
        if 'days' in kwargs:
            cmd.extend(['--days', str(kwargs['days'])])
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode != 0:
            print(f"Error running monitor for query '{query}': {result.stderr}")
            return None
        
        # Try to parse as JSON, fallback to string
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError:
            return result.stdout.strip()
    except subprocess.TimeoutExpired:
        print(f"Timeout running monitor for query '{query}'")
        return None
    except Exception as e:
        print(f"Exception running monitor for query '{query}': {e}")
        return None


def test_pipeline_health():
    """Test 1: Pipeline health tracking accuracy."""
    print("Test 1: Pipeline health tracking...")
    
    # Test overall health
    health_summary = run_monitor('pipeline_health')
    if not isinstance(health_summary, dict):
        print("✗ Pipeline health should return a dictionary")
        return False
    
    # Should have health data for multiple pipelines
    if len(health_summary) >= 5:
        print(f"✓ Found health data for {len(health_summary)} pipelines")
        
        # Check if we can get health for a specific pipeline
        specific_health = run_monitor('pipeline_health', pipeline_id='user-data-etl')
        if isinstance(specific_health, dict) and 'success_rate' in specific_health:
            print(f"✓ Specific pipeline health: {specific_health['success_rate']:.1f}% success rate")
            return True
        else:
            print("✗ Specific pipeline health query failed")
            return False
    else:
        print(f"✗ Expected health data for multiple pipelines, found: {len(health_summary)}")
        return False


def test_anomaly_detection():
    """Test 2: Anomaly detection accuracy."""
    print("\nTest 2: Anomaly detection...")
    
    anomalies = run_monitor('anomalies')
    if not isinstance(anomalies, list):
        print("✗ Anomalies should return a list")
        return False
    
    # Should detect report-generation as having issues (we created anomalies for it)
    report_anomalies = [a for a in anomalies if a.get('pipeline_id') == 'report-generation']
    
    if report_anomalies:
        print(f"✓ Detected {len(anomalies)} anomalies, including report-generation issues")
        return True
    else:
        print(f"✗ Expected to detect report-generation anomalies, found: {len(anomalies)} total")
        return False


def test_team_metrics():
    """Test 3: Team metrics aggregation."""
    print("\nTest 3: Team metrics aggregation...")
    
    team_metrics = run_monitor('team_metrics')
    if not isinstance(team_metrics, dict):
        print("✗ Team metrics should return a dictionary")
        return False
    
    # Should have metrics for multiple teams
    expected_teams = ['data-engineering', 'backend', 'ml-engineering', 'platform']
    found_teams = set(team_metrics.keys())
    
    if len(found_teams.intersection(set(expected_teams))) >= 3:
        print(f"✓ Found metrics for teams: {list(found_teams)}")
        
        # Check if data-engineering team has reasonable metrics
        if 'data-engineering' in team_metrics:
            de_metrics = team_metrics['data-engineering']
            if 'total_pipelines' in de_metrics and 'avg_success_rate' in de_metrics:
                print(f"✓ Data Engineering: {de_metrics['total_pipelines']} pipelines, {de_metrics['avg_success_rate']:.1f}% avg success rate")
                return True
        
        return True
    else:
        print(f"✗ Expected teams like {expected_teams}, found: {list(found_teams)}")
        return False


def test_performance():
    """Test 4: Performance with large dataset."""
    print("\nTest 4: Performance test...")
    
    start_time = time.time()
    result = run_monitor('total_executions')
    end_time = time.time()
    
    if result is None:
        return False
    
    execution_time = end_time - start_time
    
    # Should process 1000+ executions in under 5 seconds
    if execution_time < 5.0 and isinstance(result, (int, float)) and result >= 1000:
        print(f"✓ Processed {result} executions in {execution_time:.2f}s")
        return True
    else:
        print(f"✗ Processed {result} executions in {execution_time:.2f}s (expected <5s)")
        return False


def test_data_structures():
    """Test 5: Efficient data structure usage."""
    print("\nTest 5: Data structure efficiency...")
    
    # This test checks if the monitor uses appropriate data structures
    try:
        with open('baseline_pipeline_monitor.py', 'r') as f:
            code = f.read()
        
        # Check for use of appropriate data structures
        has_collections = 'from collections import' in code
        has_defaultdict = 'defaultdict' in code
        has_enum = 'from enum import' in code or 'class.*Enum' in code
        
        if has_collections and (has_defaultdict or has_enum):
            print("✓ Uses appropriate data structures (collections, enums)")
            return True
        else:
            print("✗ Should use efficient data structures like defaultdict and enums")
            return False
    except FileNotFoundError:
        print("✗ Could not find baseline_pipeline_monitor.py")
        return False


def test_robustness():
    """Test 6: Code robustness and error handling."""
    print("\nTest 6: Code robustness...")
    
    try:
        with open('baseline_pipeline_monitor.py', 'r') as f:
            code = f.read()
        
        # Check for basic error handling and validation
        has_try_except = 'try:' in code and 'except' in code
        has_input_validation = 'if' in code and ('not' in code or 'is None' in code)
        has_error_handling = 'error' in code.lower() or 'exception' in code.lower()
        
        if (has_try_except or has_input_validation) and has_error_handling:
            print("✓ Shows evidence of error handling and input validation")
            return True
        else:
            print("✗ Should include error handling and input validation")
            return False
    except FileNotFoundError:
        print("✗ Could not find baseline_pipeline_monitor.py")
        return False


def test_performance_trends():
    """Test 7: Performance trends analysis."""
    print("\nTest 7: Performance trends analysis...")
    
    trends = run_monitor('performance_trends', pipeline_id='user-data-etl', days=7)
    if not isinstance(trends, dict):
        print("✗ Performance trends should return a dictionary")
        return False
    
    # Should have trend data
    if 'pipeline_id' in trends and 'success_rate' in trends and 'avg_duration' in trends:
        print(f"✓ Performance trends: {trends['success_rate']:.1f}% success rate, {trends['avg_duration']:.1f}s avg duration")
        return True
    else:
        print(f"✗ Expected trend data with success_rate and avg_duration, found: {list(trends.keys())}")
        return False


def main():
    """Run all evaluation tests."""
    print("=" * 60)
    print("PIPELINE MONITORING SYSTEM EVALUATION")
    print("=" * 60)
    
    tests = [
        test_pipeline_health,
        test_anomaly_detection,
        test_team_metrics,
        test_performance,
        test_data_structures,
        test_robustness,
        test_performance_trends
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as e:
            print(f"✗ Test failed with exception: {e}")
    
    print("\n" + "=" * 60)
    print(f"FINAL SCORE: {passed}/{total}")
    print("=" * 60)
    
    if passed >= 5:
        print("🎉 Excellent! Your monitoring system meets the requirements.")
        print("Consider improving:")
        if passed < total:
            print("- Review failed tests for areas of improvement")
        print("- Add more sophisticated anomaly detection")
        print("- Implement real-time alerting capabilities")
    elif passed >= 4:
        print("✅ Good job! Your monitoring system is mostly functional.")
        print("Focus on improving:")
        print("- Anomaly detection accuracy")
        print("- Performance optimization")
        print("- Error handling and robustness")
    else:
        print("❌ Your monitoring system needs significant improvement.")
        print("Focus on:")
        print("- Basic pipeline health tracking")
        print("- Proper anomaly detection")
        print("- Team metrics aggregation")
        print("- Performance optimization")
    
    return passed >= 4


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)