# Use Cases — RDG Stream Platform

> Business scenarios for the real-time metrics pipeline. Each use case maps to requirements (REQ/NFR) and test cases (TC) in [test-plan.md](./test-plan.md).

---

## 1. Actors

| Actor | Description |
|-------|-------------|
| **Plant telemetry source** | Mock producer, webhook, or future SCADA adapter publishing JSON to Kafka |
| **Stream processor** | PyFlink job validating, windowing, and aggregating events |
| **Data store** | Cassandra keyspace `rdg` (`metrics_current`, `metrics_ts`) |
| **Operator** | Engineer running Docker stack, submitting Flink job, inspecting UIs |
| **Downstream consumer** | Dashboard or read API client querying latest or historical metrics |

---

## 2. Use case catalog

| ID | Title | Priority | Requirements |
|----|-------|----------|--------------|
| UC-01 | Ingest plant telemetry continuously | P0 | REQ-01, REQ-02, NFR-04 |
| UC-02 | Validate and quarantine bad events | P0 | REQ-03 |
| UC-03 | Compute minute-level aggregates | P0 | REQ-04, REQ-05 |
| UC-04 | Persist snapshot and time-series | P0 | REQ-06, REQ-07 |
| UC-05 | Recover from TaskManager failure | P1 | REQ-08, NFR-05 |
| UC-06 | Query metrics for operations | P1 | REQ-10 |
| UC-07 | Reset local environment | P1 | NFR-07 |

---

## 3. Use case specifications

### UC-01 — Ingest plant telemetry continuously

| Field | Description |
|-------|-------------|
| **Goal** | Plant equipment metrics flow into Kafka without manual intervention |
| **Primary actor** | Plant telemetry source |
| **Preconditions** | Stack healthy; topic `metrics.raw` exists; mock producer running (dev profile) |
| **Trigger** | Producer timer fires every 2 seconds |
| **Main flow** | 1. Source builds JSON with required fields → 2. Publishes to `metrics.raw` → 3. Kafka retains message → 4. Flink consumer group `rdg-flink` advances offsets |
| **Postconditions** | Topic receives new messages; consumer lag remains bounded |
| **Test coverage** | TC-007, TC-009, TC-016 |

**Sample event:**

```json
{
  "plant_id": "plant-a",
  "equipment_id": "pump-01",
  "metric": "temperature",
  "value": 42.5,
  "unit": "C",
  "ts": "2026-07-18T10:00:00Z"
}
```

---

### UC-02 — Validate and quarantine bad events

| Field | Description |
|-------|-------------|
| **Goal** | Invalid payloads never corrupt Cassandra; they are isolated for inspection |
| **Primary actor** | Stream processor |
| **Preconditions** | Flink job RUNNING; topic `metrics.dlq` exists |
| **Trigger** | Malformed or incomplete JSON arrives on `metrics.raw` |
| **Main flow** | 1. Job validates required fields and types → 2. Invalid record wrapped with `error`, `reason`, `original` → 3. Routed to `metrics.dlq` → 4. Valid pipeline continues |
| **Postconditions** | DLQ contains quarantined record; job stays RUNNING; no bad row in Cassandra |
| **Test coverage** | TC-010 |

---

### UC-03 — Compute minute-level aggregates

| Field | Description |
|-------|-------------|
| **Goal** | Operators see rolled-up values (avg/min/max) per plant and metric each minute |
| **Primary actor** | Stream processor |
| **Preconditions** | Valid events keyed by `(plant_id, metric)` |
| **Trigger** | 1-minute processing-time window closes |
| **Main flow** | 1. KeyBy plant + metric → 2. Tumbling 1-min window → 3. Aggregate sum/count/min/max → 4. Emit average to sink |
| **Postconditions** | One aggregate per key per closed window |
| **Test coverage** | TC-011, TC-016 |

---

### UC-04 — Persist snapshot and time-series

| Field | Description |
|-------|-------------|
| **Goal** | Dashboards read latest value and historical buckets from Cassandra |
| **Primary actor** | Stream processor |
| **Preconditions** | Keyspace `rdg` and tables created by init |
| **Trigger** | Window aggregate emitted |
| **Main flow** | 1. Upsert `metrics_current` (latest avg per plant/metric) → 2. Insert `metrics_ts` row (partition: plant, metric, date bucket) |
| **Postconditions** | Both tables contain consistent non-null data |
| **Test coverage** | TC-013, TC-014, TC-016, TC-017 |

---

### UC-05 — Recover from TaskManager failure

| Field | Description |
|-------|-------------|
| **Goal** | Pipeline resumes after infrastructure blip without manual redeploy |
| **Primary actor** | Operator |
| **Preconditions** | Checkpointing enabled (60s); MinIO bucket `flink-checkpoints` exists |
| **Trigger** | TaskManager container restarted |
| **Main flow** | 1. Operator restarts `flink-taskmanager` → 2. Flink restores from checkpoint → 3. Job returns RUNNING → 4. Processing and Cassandra writes resume |
| **Postconditions** | Same job ID or restored job RUNNING; new data within ~90s |
| **Test coverage** | TC-012, TC-018, TC-023 |

---

### UC-06 — Query metrics for operations

| Field | Description |
|-------|-------------|
| **Goal** | External apps read stored metrics without direct CQL |
| **Primary actor** | Downstream consumer |
| **Preconditions** | Data in Cassandra; read API running (dev profile) |
| **Trigger** | HTTP GET with `plant_id` (and optional `metric`) |
| **Main flow** | 1. Client calls read API → 2. Service queries Cassandra → 3. JSON response returned |
| **Postconditions** | Current snapshot or history rows returned |
| **Test coverage** | TC-013, TC-015 (Phase 7) |

---

### UC-07 — Reset local environment

| Field | Description |
|-------|-------------|
| **Goal** | Developer wipes all local pipeline data for a clean test run |
| **Primary actor** | Operator |
| **Preconditions** | Stack was running with persisted volumes |
| **Trigger** | `docker compose --profile dev down -v` |
| **Main flow** | 1. Tear down containers and volumes → 2. `up -d` fresh stack → 3. Init recreates topics/schema → 4. Cassandra empty until job resubmitted |
| **Postconditions** | `metrics_current` count = 0 before new ingestion |
| **Test coverage** | TC-024 |

---

## 4. Traceability matrix

| Use case | Requirements | Test cases | Phase 9 task |
|----------|--------------|------------|--------------|
| UC-01 | REQ-01, REQ-02, NFR-04 | TC-001, TC-007, TC-009, TC-016 | TASK-053, 054 |
| UC-02 | REQ-03 | TC-010 | TASK-055 |
| UC-03 | REQ-04, REQ-05 | TC-011, TC-016 | TASK-054 |
| UC-04 | REQ-06, REQ-07 | TC-013, TC-014, TC-017 | TASK-054, 057 |
| UC-05 | REQ-08, NFR-05 | TC-012, TC-018, TC-023 | TASK-056 |
| UC-06 | REQ-10 | TC-013, TC-015 | TASK-057 |
| UC-07 | NFR-07 | TC-024 | TASK-058 |

---

## 5. Phase 9 validation entry / exit criteria

**Entry criteria**

- Docker Desktop running with ≥ 8 GB RAM
- Ports 8081, 8090, 9042, 9092, 9000, 9001 free
- Repository at `main` with PyFlink job and compose stack

**Exit criteria (MVP)**

- All P0 test cases pass (TC-001 – TC-010, TC-013, TC-014, TC-016, TC-017)
- P1 recovery and cleanup pass (TC-018, TC-024)
- Results logged in [test-plan.md](./test-plan.md) §6
- No open P0 defects

**Automated runner:** `scripts/run-phase9-validation.sh`
