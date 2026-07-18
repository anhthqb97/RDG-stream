"""RDG Stream read API — query metrics from Cassandra."""

from __future__ import annotations

import os
from datetime import UTC, date, datetime
from typing import Any

from cassandra.cluster import Cluster
from cassandra.util import Date as CassandraDate
from fastapi import FastAPI, HTTPException, Query

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
KEYSPACE = "rdg"

app = FastAPI(title="RDG Stream Read API", version="1.0.0")
_cluster: Cluster | None = None
_session = None


def get_session():
    global _cluster, _session
    if _session is None:
        _cluster = Cluster([CASSANDRA_HOST])
        _session = _cluster.connect(KEYSPACE)
    return _session


def row_to_dict(row: Any) -> dict[str, Any]:
    data = {}
    for name in row._fields:
        value = getattr(row, name)
        if isinstance(value, datetime):
            data[name] = value.astimezone(UTC).isoformat()
        elif isinstance(value, (date, CassandraDate)):
            data[name] = str(value)
        else:
            data[name] = value
    return data


@app.get("/health")
def health() -> dict[str, str]:
    get_session().execute("SELECT keyspace_name FROM system_schema.keyspaces LIMIT 1")
    return {"status": "ok"}


@app.get("/metrics/current")
def metrics_current(
    plant_id: str = Query(..., min_length=1),
) -> list[dict[str, Any]]:
    rows = get_session().execute(
        """
        SELECT plant_id, metric, value, unit, updated_at
        FROM metrics_current
        WHERE plant_id = %s
        """,
        (plant_id,),
    )
    return [row_to_dict(row) for row in rows]


@app.get("/metrics/history")
def metrics_history(
    plant_id: str = Query(..., min_length=1),
    metric: str = Query(..., min_length=1),
    bucket: date | None = None,
    limit: int = Query(default=50, ge=1, le=500),
) -> list[dict[str, Any]]:
    bucket_value = bucket or datetime.now(tz=UTC).date()
    rows = get_session().execute(
        """
        SELECT plant_id, metric, bucket, ts, value
        FROM metrics_ts
        WHERE plant_id = %s AND metric = %s AND bucket = %s
        LIMIT %s
        """,
        (plant_id, metric, bucket_value, limit),
    )
    results = [row_to_dict(row) for row in rows]
    if not results:
        raise HTTPException(
            status_code=404,
            detail=f"No history for plant_id={plant_id}, metric={metric}, bucket={bucket_value}",
        )
    return results
