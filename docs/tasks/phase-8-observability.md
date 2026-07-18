# Phase 8 — Observability

| | |
|---|---|
| **Tasks** | TASK-049 – TASK-052 |
| **Priority** | P1 |
| **Previous** | [Phase 6 — Sources](./phase-6-sources.md) |
| **Next** | [Phase 9 — Validation](./phase-9-validation.md) |

---

## Goal

Monitor pipeline health during local development.

---

## Tools

| Tool | URL | Check |
|------|-----|-------|
| Kafka UI | http://localhost:8090 | Topics, lag, messages |
| Flink Dashboard | http://localhost:8081 | Job status, backpressure |
| MinIO Console | http://localhost:9001 | Checkpoint files |
| Docker logs | `docker compose logs -f <service>` | Errors |

---

## Tasks

### TASK-049 — Add Kafka UI

| | |
|---|---|
| **Requirement** | NFR-06 |
| **Priority** | P1 |
| **Status** | [ ] |

- Image: `kafbat/kafka-ui:latest`
- Port: `8090`
- Profile: `dev`

---

### TASK-050 — Confirm Flink Dashboard

| | |
|---|---|
| **Requirement** | NFR-06 |
| **Priority** | P1 |
| **Status** | [ ] |

Verify http://localhost:8081 loads and shows JobManager + TaskManagers.

---

### TASK-051 — Monitor Kafka lag and DLQ

| | |
|---|---|
| **Requirement** | REQ-03 |
| **Priority** | P1 |
| **Status** | [ ] |

In Kafka UI:
- Check consumer group lag for Flink job
- Inspect `metrics.dlq` for schema errors

---

### TASK-052 — Monitor Flink metrics

| | |
|---|---|
| **Requirement** | REQ-04 |
| **Priority** | P1 |
| **Status** | [ ] |

In Flink Dashboard → job → Metrics:
- `numRecordsIn` / `numRecordsOut` > 0
- No sustained backpressure under mock load

---

## Verify

**Test cases:** TC-021, TC-022, TC-023

---

## Phase complete when

- [ ] Kafka UI accessible and shows live messages
- [ ] Flink Dashboard shows RUNNING job with metrics
- [ ] MinIO shows checkpoint files updating
