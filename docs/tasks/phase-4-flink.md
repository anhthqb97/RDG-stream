# Phase 4 — Flink Processing

| | |
|---|---|
| **Tasks** | TASK-027 – TASK-036 |
| **Priority** | P0 |
| **Previous** | [Phase 3 — Kafka](./phase-3-kafka.md) · [Phase 5 — Cassandra](./phase-5-cassandra.md) |
| **Next** | [Phase 6 — Sources](./phase-6-sources.md) |

---

## Goal

Build and deploy Flink job: read Kafka → validate → window → aggregate → sink to Cassandra + MinIO checkpoints.

---

## Processing logic

| Step | Action |
|------|--------|
| 1 | Read `metrics.raw` |
| 2 | Validate schema → invalid to `metrics.dlq` |
| 3 | KeyBy `plant_id` + `metric` |
| 4 | Tumbling window 1 min |
| 5 | Compute avg, min, max |
| 6 | Sink to `metrics_current` + `metrics_ts` |
| 7 | Checkpoint to MinIO |

---

## Tasks

### TASK-027 — Scaffold Flink job

| | |
|---|---|
| **Requirement** | REQ-04 |
| **Priority** | P0 |
| **Status** | [x] |

Create **PyFlink** project in `flink/jobs/`:
- `rdg_job.py` — main DataStream job
- `requirements.txt` — `apache-flink==1.19.*`
- Submit: `flink run -py /opt/flink/jobs/rdg_job.py`

---

### TASK-028 — Kafka source connector

| | |
|---|---|
| **Requirement** | REQ-02 |
| **Priority** | P0 |
| **Status** | [x] |

Consume from `metrics.raw`, bootstrap `kafka:9092`.

---

### TASK-029 — JSON schema validation

| | |
|---|---|
| **Requirement** | REQ-03 |
| **Priority** | P0 |
| **Status** | [x] |

Validate required fields: `plant_id`, `equipment_id`, `metric`, `value`, `unit`, `ts`.

---

### TASK-030 — Route to DLQ

| | |
|---|---|
| **Requirement** | REQ-03 |
| **Priority** | P0 |
| **Status** | [x] |

Invalid records → `metrics.dlq` as **wrapped JSON** with `error`, `reason`, `original` fields.

---

### TASK-031 — KeyBy plant_id + metric

| | |
|---|---|
| **Requirement** | REQ-04 |
| **Priority** | P0 |
| **Status** | [x] |

Partition stream by composite key for correct windowing.

---

### TASK-032 — 1-min tumbling window

| | |
|---|---|
| **Requirement** | REQ-05, NFR-04 |
| **Priority** | P0 |
| **Status** | [x] |

Window size: 60 seconds, tumbling (no overlap).

---

### TASK-033 — Compute aggregates

| | |
|---|---|
| **Requirement** | REQ-05 |
| **Priority** | P0 |
| **Status** | [x] |

Per window: **avg**, **min**, **max** of `value`.

---

### TASK-034 — Cassandra sink

| | |
|---|---|
| **Requirement** | REQ-06, REQ-07 |
| **Priority** | P0 |
| **Status** | [ ] |

Write to:
- `rdg.metrics_current` — latest snapshot
- `rdg.metrics_ts` — time-series rows

---

### TASK-035 — MinIO checkpoints

| | |
|---|---|
| **Requirement** | REQ-08, NFR-05 |
| **Priority** | P0 |
| **Status** | [ ] |

Configure checkpoint storage: `s3://flink-checkpoints/` on MinIO (`http://minio:9000`).

---

### TASK-036 — Submit PyFlink job

| | |
|---|---|
| **Requirement** | REQ-04 |
| **Priority** | P0 |
| **Status** | [ ] |

```bash
# Install deps in Flink container or custom image
pip install -r flink/jobs/requirements.txt

# Submit
docker compose exec flink-jobmanager flink run -py /opt/flink/jobs/rdg_job.py

# Or via Dashboard: upload rdg_job.py + dependencies
```

---

## Verify

**Test cases:** TC-008, TC-009, TC-010, TC-011, TC-012

- Job status = RUNNING in Flink Dashboard
- Invalid JSON in `metrics.dlq`
- Aggregates in Cassandra after 70s

---

## Phase complete when

- [ ] Flink job RUNNING
- [ ] Consumes from `metrics.raw`
- [ ] Writes to Cassandra tables
- [ ] Checkpoints visible in MinIO console
