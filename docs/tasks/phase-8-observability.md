# Phase 8 — Observability

| | |
|---|---|
| **Tasks** | TASK-049 – TASK-052 |
| **Priority** | P1 |
| **Previous** | [Phase 7 — Downstream](./phase-7-downstream.md) |
| **Next** | [Phase 10 — Production](./phase-10-production.md) |

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
| **Status** | [x] |

In Kafka UI:
- Check consumer group lag for Flink job
- Inspect `metrics.dlq` for schema errors

**CLI verification:**

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group rdg-flink
```

**Result (2026-07-18):** Pass — consumer group `rdg-flink` lag 6–8 per partition (stable under mock load); `metrics.dlq` topic present with 0 messages after clean restart (TC-010 invalid routing verified separately in Phase 9).

---

### TASK-052 — Monitor Flink metrics

| | |
|---|---|
| **Requirement** | REQ-04 |
| **Priority** | P1 |
| **Status** | [x] |

In Flink Dashboard → job → Metrics:
- `numRecordsIn` / `numRecordsOut` > 0
- No sustained backpressure under mock load

**Result (2026-07-18):** Pass — `0.Source__kafka-metrics-raw.numRecordsIn` = 316; `0.isBackPressured` = false. MinIO bucket `flink-checkpoints/rdg/checkpoints/` contains checkpoint metadata for job `11691954…` (TC-012/TC-023).

---

## Verify

**Test cases:** TC-021, TC-022, TC-023 — all pass (2026-07-18).

---

## Phase complete when

- [x] Kafka UI accessible and shows live messages
- [x] Flink Dashboard shows RUNNING job with metrics
- [x] MinIO shows checkpoint files updating
