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
| **Status** | [x] |

- Image: `kafbat/kafka-ui:latest`
- Port: `8090`
- Profile: `dev`

**Result (2026-07-18):** Pass — service configured in `docker-compose.yml` (Phase 2). UI loads at http://localhost:8090; cluster `local` connected; topics `metrics.raw`, `metrics.dlq`, `events.lifecycle` listed; `metrics.raw` shows live JSON messages (~317 total).

---

### TASK-050 — Confirm Flink Dashboard

| | |
|---|---|
| **Requirement** | NFR-06 |
| **Priority** | P1 |
| **Status** | [x] |

Verify http://localhost:8081 loads and shows JobManager + TaskManagers.

**Result (2026-07-18):** Pass — overview shows 1 TaskManager, 2 slots; job `rdg-stream-job` (`11691954…`) state **RUNNING**.

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
