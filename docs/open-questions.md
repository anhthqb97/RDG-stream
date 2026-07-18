# Open Questions — RDG Stream Platform

> All decisions resolved for documentation phase. Update [architecture.md](./architecture.md) when implementation starts.
>
> **Last updated:** 2026-07-18

---

## Language strategy

| Component | Language | Why |
|-----------|----------|-----|
| **Flink streaming job** | **Python (PyFlink)** | User choice; Flink 1.19 supports PyFlink DataStream API |
| **Mock producer (S3)** | **Python** | Same stack as PyFlink; `confluent-kafka` or `kafka-python` |
| **Webhook adapter (S2)** | **Python** | FastAPI or Flask → Kafka |
| **Read API (optional)** | **Python** | FastAPI + `cassandra-driver` |
| **DLQ replay script** | **Python** | CLI script to replay `metrics.dlq` → `metrics.raw` |

**Summary:** **Python for the entire application layer.** Flink still runs on the JVM runtime, but job logic is written in PyFlink.

**Not supported:** Go for Flink jobs — Flink has no native Go API.

---

## Before Phase 2 (Docker)

| # | Question | Options | Decision |
|---|----------|---------|----------|
| OQ-01 | Flink job language | Java vs Scala vs Python vs Go | **Python (PyFlink)** — DataStream API in `flink/jobs/` |
| OQ-02 | Flink build / deps | Maven vs Gradle vs pip | **pip + requirements.txt** — `apache-flink==1.19.*` pinned to cluster version |
| OQ-03 | Init container pattern | Single init vs per-service entrypoint | **Single init container** — creates Kafka topics + applies Cassandra schema after dependencies are healthy |

---

## Before Phase 4 (Flink)

| # | Question | Options | Decision |
|---|----------|---------|----------|
| OQ-04 | Window sizes | 1 min only vs 1 min + 5 min | **1 min only for MVP/local** · add **5 min** window in prod (second operator branch) |
| OQ-05 | DLQ message format | Raw JSON vs wrapped with error reason | **Wrapped** — `{ "error": "...", "reason": "...", "original": { ... } }` |
| OQ-06 | Checkpoint interval | 60s vs 120s | **60s** — faster recovery on local restarts |
| OQ-07 | Delivery semantics | Exactly-once vs at-least-once | **At-least-once for MVP** with **idempotent Cassandra upserts** (same PK overwrites). EXACTLY_ONCE in prod if needed |

**Submit command:**

```bash
docker compose exec flink-jobmanager flink run -py /opt/flink/jobs/rdg_job.py
```

**DLQ wrapper example:**

```json
{
  "error": "validation_failed",
  "reason": "missing required field: ts",
  "timestamp": "2026-07-18T10:00:00Z",
  "original": { "plant_id": "BAD", "metric": "x" }
}
```

---

## Before Phase 6 (Sources)

| # | Question | Options | Decision |
|---|----------|---------|----------|
| OQ-08 | Mock producer language | Python vs Node vs Go | **Python** — `confluent-kafka` or `kafka-python`, same language as PyFlink |
| OQ-09 | Webhook auth | None (local) vs API key | **None for local dev** · **API key header** (`X-API-Key`) for prod webhook |

---

## Before Phase 10 (Production)

| # | Question | Options | Decision |
|---|----------|---------|----------|
| OQ-10 | Kafka deployment | KRaft bare metal vs Strimzi on k3s | **Docker Compose KRaft for local** · **Strimzi on k3s for prod** |
| OQ-11 | Cassandra replication | RF=3, which DCs | **RF=3, single DC** to start · multi-DC when geo redundancy is required |
| OQ-12 | TLS scope | Kafka only vs all services | **No TLS local** · **TLS on all external edges in prod** |
| OQ-13 | SCADA protocol | OPC-UA vs MQTT vs REST poll | **REST poll adapter first** · OPC-UA or MQTT when hardware is known |
| OQ-14 | DLQ replay tool | Manual script vs admin UI | **Python CLI for MVP** (`rdg_dlq_replay.py`) · admin UI later if needed |

---

## Resolved (from design docs)

| # | Question | Decision | Documented in |
|---|----------|----------|---------------|
| RQ-01 | Message format | JSON with 6 required fields | pipeline.md |
| RQ-02 | Ingestion broker | Apache Kafka 3.7 KRaft | architecture.md |
| RQ-03 | Stream processor | Apache Flink 1.19 + PyFlink | architecture.md |
| RQ-04 | Storage | Apache Cassandra 4.1 | architecture.md |
| RQ-05 | Checkpoint store | MinIO (S3-compatible) | architecture.md |
| RQ-06 | Local topology | Single broker, single Cassandra node | architecture.md |
| RQ-07 | Cost model | 100% free OSS, Docker local | requirements.md |
| RQ-08 | Application language | Python (producer, webhook, API, CLI) | this doc |
| RQ-09 | Flink job language | Python (PyFlink) | this doc |

---

## Decision rationale (short)

### Why Python (PyFlink) for Flink?

- Official Flink Python API (PyFlink) supports DataStream operations: source, KeyBy, window, sink
- One language across Flink job, producers, and APIs
- Good fit for JSON validation and aggregation logic
- Runs on existing `flink:1.19-scala_2.12-java11` image with Python installed in job container

### PyFlink trade-offs (know before building)

| Topic | Note |
|-------|------|
| Connectors | Kafka source is well supported; Cassandra may use `cassandra-driver` in a custom sink or JDBC |
| Packaging | Mount `flink/jobs/` into container; pin `apache-flink` version to 1.19.x |
| Performance | Java is faster at very high throughput; fine for local / plant metrics scale |

### Why not Go for Flink?

Flink has no Go DataStream API. Go is not an option for the streaming job.

---

## How to update

When a decision changes during implementation:

1. Update the **Decision** column above
2. Update [architecture.md](./architecture.md)
3. Update the relevant [tasks/phase-N.md](./tasks/phase-N.md)
4. Add a row to the changelog below

### Changelog

| Date | Change |
|------|--------|
| 2026-07-18 | All OQ-01 – OQ-14 resolved |
| 2026-07-18 | Flink job language set to **Python (PyFlink)**; Python for all app services |
