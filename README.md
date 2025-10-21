# Take‑Home (2‑3 hrs) – Software Engineer

**Objective:** Build a data pipeline monitoring system that tracks execution status, detects failures, and sends alerts to appropriate teams.

## Provided

- `baseline_pipeline_monitor.py` – basic pipeline monitoring with simple alerting (may have a small bug to fix)
- `data/pipeline_executions.jsonl` – synthetic pipeline execution data (no real data)
- `eval.py` – runs tests that validate your monitoring system's accuracy and performance
- `requirements.txt` – only standard Python libraries required

## Your Tasks (**aim for 2 hrs of coding time**)

1. **Fix any bugs** in the baseline implementation to get it running
2. **Enhance the monitoring system** to track pipeline health metrics and execution patterns
3. **Implement intelligent alerting** with different severity levels and team routing
4. **Add anomaly detection** for performance degradation and failure patterns
5. **Optimize for high-volume** event processing using efficient data structures
6. Write a **short `ARCHITECTURE.md`** (≤1 page) explaining your design choices and how you'd scale this system

## Running locally

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
python baseline_pipeline_monitor.py --data-file data/pipeline_executions.jsonl --query "pipeline_health"
python eval.py         # should output score; target ≥4/6
```

## Evaluation Criteria

The evaluation tests your system on:
- **Accuracy**: Correctly identifying pipeline failures and performance issues
- **Performance**: Efficient processing of high-volume execution events
- **Alerting**: Proper severity classification and team routing
- **Data Structures**: Efficient storage and retrieval of pipeline metrics
- **Code Quality**: Clean, maintainable, and well-documented code

## Submission

- Push to a Git repo or share a zip
- Make sure `eval.py` runs without errors
- Include your `ARCHITECTURE.md` with scaling considerations
- Be prepared to discuss trade-offs and design decisions
