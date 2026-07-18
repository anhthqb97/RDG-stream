# Requirements — RDG Stream Platform

> Source of truth for functional, non-functional, and technical constraints.
> Task index: [README.md](./README.md)

---

## Functional requirements

| ID | Requirement | Source |
|----|-------------|--------|
| **REQ-01** | Ingest JSON metric events from multiple sources (SCADA, webhook, mock) | Pipeline S1/S2/S3 |
| **REQ-02** | Buffer events in Kafka topic `metrics.raw` | Architecture §4 |
| **REQ-03** | Validate event schema; route invalid records to `metrics.dlq` | Architecture §5 step 2 |
| **REQ-04** | Process events with Flink: KeyBy `plant_id` + `metric` | Architecture §5 step 3 |
| **REQ-05** | Aggregate with 1-min tumbling window: avg, min, max | Architecture §5 step 4 |
| **REQ-06** | Persist latest snapshot to `rdg.metrics_current` | Architecture §6 |
| **REQ-07** | Persist time-series to `rdg.metrics_ts` | Architecture §6 |
| **REQ-08** | Save Flink checkpoints to MinIO (`s3://flink-checkpoints/`) | Architecture §5 step 6 |
| **REQ-09** | Optional publish to `events.lifecycle` topic | Pipeline |
| **REQ-10** | Read stored metrics via cqlsh or read API | Implementation §7 |

### Event schema (REQ-01)

```json
{
  "plant_id": "NM01",
  "equipment_id": "GEN-01",
  "metric": "power_output_mw",
  "value": 45.2,
  "unit": "MW",
  "ts": "2026-07-18T10:00:00Z"
}
```

**Required fields:** `plant_id`, `equipment_id`, `metric`, `value`, `unit`, `ts`

---

## Non-functional requirements

| ID | Requirement | Target |
|----|-------------|--------|
| **NFR-01** | 100% free & open-source stack | No paid cloud services |
| **NFR-02** | Run locally in Docker | Single laptop |
| **NFR-03** | Minimum RAM | 8 GB Docker allocation |
| **NFR-04** | Local latency | Event → Cassandra within ~90s (1-min window) |
| **NFR-05** | Fault recovery | Flink job recovers after TaskManager restart |
| **NFR-06** | Dev observability | Kafka UI + Flink Dashboard |
| **NFR-07** | Data reset | `docker compose down -v` wipes all data |
| **NFR-08** | English-only documentation | All docs in English |

> Full requirements: [tasks/requirements.md](./tasks/requirements.md) · Open questions: [open-questions.md](./open-questions.md)

---

## Technical constraints

| ID | Constraint |
|----|------------|
| **CON-01** | Kafka 3.7 KRaft — `apache/kafka:3.7.0` |
| **CON-02** | Flink 1.19 — `flink:1.19-scala_2.12-java11` |
| **CON-03** | Cassandra 4.1 — `cassandra:4.1` |
| **CON-04** | MinIO for checkpoints — `minio/minio:latest` |
| **CON-05** | Local: single Kafka broker, single Cassandra node, 1 TaskManager |
| **CON-06** | Networks: `stream` + `storage` |
| **CON-07** | Compose profiles: `default`, `dev`, `prod` (later) |

---

## Requirement → phase mapping

| Requirement | Phase |
|-------------|-------|
| REQ-01 | [Phase 6 — Sources](./phase-6-sources.md) |
| REQ-02, REQ-03 | [Phase 3 — Kafka](./phase-3-kafka.md) |
| REQ-04, REQ-05, REQ-08 | [Phase 4 — Flink](./phase-4-flink.md) |
| REQ-06, REQ-07 | [Phase 5 — Cassandra](./phase-5-cassandra.md) |
| REQ-10 | [Phase 7 — Downstream](./phase-7-downstream.md) |
| NFR-02 | [Phase 0](./phase-0-setup.md), [Phase 2](./phase-2-infrastructure.md) |
| NFR-06 | [Phase 8 — Observability](./phase-8-observability.md) |
| All | [Phase 9 — Validation](./phase-9-validation.md) |
