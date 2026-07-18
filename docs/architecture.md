# Architecture — RDG Stream Platform

> Draft v0.2 · **100% free & open source** · **Docker Compose** · **Local test on laptop**  
> **Status:** Documentation phase — design complete, implementation not started.
>
> Index: [README.md](./README.md) · Pipeline: [pipeline.md](./pipeline.md) · Charts: [workflow.md](./workflow.md) · Tasks: [tasks/](./tasks/) · Checklist: [implementation-checklist.md](./implementation-checklist.md) · Tests: [test-plan.md](./test-plan.md)

---

## 1. Pipeline overview

Full diagram, component IDs, and connections: **[pipeline.md](./pipeline.md)** · Charts: **[workflow.md](./workflow.md)**

```
S1 ──┐
S2 ──┼──► KF ──► FL ──► CS
S3 ──┘      │      ├──► MO
            └──► KUI   └──► FUI
```

---

## 2. Free stack — component roles

| Layer | Tech | Docker image | Role |
|-------|------|--------------|---------|
| **Ingestion** | Apache Kafka 3.7 (KRaft) | `apache/kafka:3.7.0` | Buffer events, no Zookeeper needed |
| **Processing** | Apache Flink 1.19 (PyFlink) | `flink:1.19-scala_2.12-java11` | Window aggregate, route to storage |
| **Storage** | Apache Cassandra 4.1 | `cassandra:4.1` | Time-series + snapshot wide rows |
| **Checkpoints** | MinIO | `minio/minio:latest` | Free S3-compatible checkpoint store |
| **Sources / API** | Python services | custom Docker build | Mock producer, webhook, read API, DLQ CLI |
| **Ops (dev)** | Kafbat Kafka UI | `kafbat/kafka-ui:latest` | Inspect topics & messages |
| **Ops (dev)** | Flink built-in UI | port 8081 | Job monitor |

**Not used:** Confluent Cloud, AWS MSK, DynamoDB, or paid managed Flink — everything runs in local Docker.

---

## 3. Local quick start

> **Requires implementation (Phase 2+).** Not runnable during documentation phase.

```bash
# Prerequisites: Docker Desktop, 8 GB+ RAM for Docker

docker compose --profile dev up -d   # start stack
docker compose ps                    # wait until healthy

# UIs
open http://localhost:8090           # Kafka UI
open http://localhost:8081           # Flink Dashboard
open http://localhost:9001           # MinIO Console (minioadmin/minioadmin)

# Verify data path
docker compose logs mock-producer --tail 10
docker compose exec cassandra cqlsh -e "SELECT * FROM rdg.metrics_current LIMIT 5;"

docker compose --profile dev down    # stop
docker compose --profile dev down -v # stop + wipe data
```

---

## 4. Kafka topics

| Topic | Partitions (local) | Producer | Consumer |
|-------|-------------------|----------|----------|
| `metrics.raw` | 3 | mock producer, webhook | Flink job |
| `metrics.dlq` | 1 | Flink (errors) | Manual replay |
| `events.lifecycle` | 1 | Flink | Optional downstream |

**Message schema (`metrics.raw`):**

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

---

## 5. Flink processing (draft)

| Step | Logic |
|------|-------|
| 1 | Read `metrics.raw` |
| 2 | Validate schema, invalid → `metrics.dlq` |
| 3 | KeyBy `plant_id` + `metric` |
| 4 | Tumbling window 1 min → avg, min, max |
| 5 | Sink to Cassandra `metrics_current` + `metrics_ts` |
| 6 | Checkpoint to MinIO (`s3://flink-checkpoints/`) |

Job deploy: PyFlink script in `./flink/jobs/rdg_job.py` → submit via:

```bash
flink run -py /opt/flink/jobs/rdg_job.py
```

Dependencies: `pip install -r flink/jobs/requirements.txt` (`apache-flink==1.19.*`).

**Language note:** Flink job + producers + API = **Python**. See [open-questions.md](./open-questions.md).

---

## 6. Cassandra model

Keyspace: `rdg`

```cql
CREATE KEYSPACE IF NOT EXISTS rdg
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Current snapshot (dashboard read)
CREATE TABLE rdg.metrics_current (
  plant_id text,
  metric text,
  value double,
  unit text,
  updated_at timestamp,
  PRIMARY KEY ((plant_id), metric)
);

-- Time-series (historical)
CREATE TABLE rdg.metrics_ts (
  plant_id text,
  metric text,
  bucket date,
  ts timestamp,
  value double,
  PRIMARY KEY ((plant_id, metric, bucket), ts)
) WITH CLUSTERING ORDER BY (ts DESC);
```

`SimpleStrategy, replication_factor: 1` — sufficient for local Docker.

---

## 7. Docker services (local)

| Service | Image | Port (host) | Network |
|---------|-------|-------------|---------|
| kafka | `apache/kafka:3.7.0` | 9092 | stream |
| kafka-ui | `kafbat/kafka-ui:latest` | 8090 | stream |
| flink-jobmanager | `flink:1.19-scala_2.12-java11` | 8081 | stream |
| flink-taskmanager | `flink:1.19-scala_2.12-java11` | — | stream |
| cassandra | `cassandra:4.1` | 9042 | storage |
| minio | `minio/minio:latest` | 9000, 9001 | stream |
| init | one-shot script | — | stream |
| mock-producer | custom | — | stream |

**Networks:**

- `stream` — Kafka, Flink, MinIO, producers
- `storage` — Cassandra (Flink connects cross-network)

**Connect from host machine** (e.g. Laravel on macOS):

```
Kafka:     localhost:9092
Cassandra: localhost:9042
Flink:     localhost:8081
```

---

## 8. Profiles

| Profile | Services | Use case |
|---------|----------|----------|
| `default` | kafka, cassandra, flink, minio, init | Core pipeline |
| `dev` | default + kafka-ui + mock-producer | **Local learning & test** |
| `prod` | multi-node TBD, no UI | Self-hosted later (still free OSS) |

---

## 9. Local test checklist

- [ ] `docker compose --profile dev up -d` — all containers healthy
- [ ] Kafka UI shows `metrics.raw` messages
- [ ] Flink job status = RUNNING
- [ ] `cqlsh` returns rows in `rdg.metrics_current`
- [ ] Restart `flink-taskmanager` — job recovers from checkpoint
- [ ] `docker compose down -v` — clean reset works

---

## 10. Production (later)

Local = single node, no auth. Prod self-hosted (still free software):

- [ ] Kafka: 3 brokers KRaft or Strimzi on k3s
- [ ] Cassandra: 3-node cluster, `NetworkTopologyStrategy`
- [ ] MinIO: distributed mode
- [ ] TLS + SASL (optional)
- [ ] No public ports except edge API
