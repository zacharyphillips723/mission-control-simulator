"""
Mission Control — Inference Logger

Decorator and utility that logs every Model Serving call to both:
  - Delta Lake (models.inference_log) — permanent audit trail
  - Lakebase (ops.throughput_metrics) — real-time throughput tracking

Wraps model serving endpoint calls to capture input, output, latency,
and caller identity for every invocation.
"""

import functools
import json
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Optional


@dataclass
class InferenceRecord:
    """A single model inference call record."""
    inference_id: str
    endpoint_name: str
    caller: str
    input_features: dict
    output_prediction: dict
    latency_ms: float
    timestamp: datetime
    simulation_time_s: float
    tick_id: Optional[str] = None
    metadata: Optional[dict] = None


class InferenceLogger:
    """
    Captures and batches model inference logs for writing to Delta and Lakebase.

    Usage:
        logger = InferenceLogger(catalog="mission_control_dev")

        # Wrap a model call
        result = logger.log_call(
            endpoint_name="mission-control-trajectory-dev",
            caller="flight_dynamics",
            input_features={"pos_x": 1e8, ...},
            call_fn=lambda: _call_endpoint("mission-control-trajectory-dev", payload),
            simulation_time_s=12345.0,
            tick_id="tick-abc",
        )

        # Flush all records to Delta at end of tick
        logger.flush_to_delta(spark)
    """

    def __init__(self, catalog: str = "mission_control_dev"):
        self.catalog = catalog
        self.records: list[InferenceRecord] = []
        self._call_count = 0
        self._total_latency_ms = 0.0

    def log_call(
        self,
        endpoint_name: str,
        caller: str,
        input_features: dict,
        call_fn: Callable[[], Any],
        simulation_time_s: float = 0.0,
        tick_id: Optional[str] = None,
        metadata: Optional[dict] = None,
    ) -> Any:
        """
        Execute a model serving call and log the input/output/latency.

        Args:
            endpoint_name: Model serving endpoint name
            caller: Who's calling (agent name, 'autopilot', 'ground')
            input_features: Input payload sent to the endpoint
            call_fn: Callable that actually makes the endpoint call
            simulation_time_s: Current simulation time
            tick_id: Optional tick correlation ID
            metadata: Optional extra metadata

        Returns:
            The raw result from call_fn
        """
        start = time.perf_counter()
        try:
            result = call_fn()
            latency_ms = (time.perf_counter() - start) * 1000

            record = InferenceRecord(
                inference_id=str(uuid.uuid4()),
                endpoint_name=endpoint_name,
                caller=caller,
                input_features=input_features,
                output_prediction=result if isinstance(result, dict) else {"raw": str(result)},
                latency_ms=latency_ms,
                timestamp=datetime.now(timezone.utc),
                simulation_time_s=simulation_time_s,
                tick_id=tick_id,
                metadata=metadata,
            )
            self.records.append(record)
            self._call_count += 1
            self._total_latency_ms += latency_ms

            return result

        except Exception as e:
            latency_ms = (time.perf_counter() - start) * 1000
            record = InferenceRecord(
                inference_id=str(uuid.uuid4()),
                endpoint_name=endpoint_name,
                caller=caller,
                input_features=input_features,
                output_prediction={"error": str(e)},
                latency_ms=latency_ms,
                timestamp=datetime.now(timezone.utc),
                simulation_time_s=simulation_time_s,
                tick_id=tick_id,
                metadata={"error": True, **(metadata or {})},
            )
            self.records.append(record)
            self._call_count += 1
            raise

    def flush_to_delta(self, spark) -> int:
        """
        Write all buffered inference records to Delta Lake.

        Args:
            spark: Active SparkSession

        Returns:
            Number of records written
        """
        if not self.records:
            return 0

        from pyspark.sql import functions as F

        rows = [
            {
                "inference_id": r.inference_id,
                "endpoint_name": r.endpoint_name,
                "caller": r.caller,
                "input_features": json.dumps(r.input_features),
                "output_prediction": json.dumps(r.output_prediction),
                "latency_ms": r.latency_ms,
                "timestamp": r.timestamp.isoformat(),
                "simulation_time_s": r.simulation_time_s,
                "tick_id": r.tick_id,
                "metadata": json.dumps(r.metadata) if r.metadata else None,
            }
            for r in self.records
        ]

        df = spark.createDataFrame(rows)
        df = df.withColumn("timestamp", F.to_timestamp("timestamp"))
        df.write.mode("append").saveAsTable(
            f"`{self.catalog}`.models.inference_log"
        )

        count = len(self.records)
        self.records.clear()
        return count

    @property
    def call_count(self) -> int:
        return self._call_count

    @property
    def avg_latency_ms(self) -> float:
        if self._call_count == 0:
            return 0.0
        return self._total_latency_ms / self._call_count

    def summary(self) -> dict:
        return {
            "total_calls": self._call_count,
            "buffered_records": len(self.records),
            "avg_latency_ms": self.avg_latency_ms,
        }


def logged_endpoint_call(
    logger: InferenceLogger,
    endpoint_name: str,
    caller: str,
    payload: dict,
    call_fn: Callable[[str, dict], dict],
    simulation_time_s: float = 0.0,
    tick_id: Optional[str] = None,
) -> dict:
    """
    Convenience function to call a model serving endpoint with logging.

    Args:
        logger: InferenceLogger instance
        endpoint_name: Endpoint to call
        caller: Who's calling
        payload: Request payload
        call_fn: Function(endpoint_name, payload) -> dict
        simulation_time_s: Current sim time
        tick_id: Optional tick ID

    Returns:
        Endpoint response dict
    """
    return logger.log_call(
        endpoint_name=endpoint_name,
        caller=caller,
        input_features=payload,
        call_fn=lambda: call_fn(endpoint_name, payload),
        simulation_time_s=simulation_time_s,
        tick_id=tick_id,
    )
