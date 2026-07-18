# Phase 2 — Docker Infrastructure

| | |
|---|---|
| **Tasks** | TASK-011 – TASK-020 |
| **Priority** | P0 |
| **Status** | [ ] |
| **Previous** | [Phase 1 — Design](./phase-1-design.md) |
| **Next** | [Phase 3 — Kafka](./phase-3-kafka.md) · [Phase 5 — Cassandra](./phase-5-cassandra.md) |

---

## Goal

Create `docker-compose.yml` with all core services running locally.

---

## Tasks

### TASK-011 — Create docker-compose.yml

| | |
|---|---|
| **Requirement** | NFR-02, CON-06 |
| **Priority** | P0 |
| **Status** | [x] |

Define all services in root `docker-compose.yml`.

**Verified (2026-07-18):** `docker-compose.yml` at repo root

---

### TASK-012 — Configure networks

| | |
|---|---|
| **Requirement** | CON-06 |
| **Priority** | P0 |
| **Status** | [x] |

| Network | Services |
|---------|----------|
| `stream` | Kafka, Flink, MinIO, producers |
| `storage` | Cassandra |

Flink connects to both networks.

---

### TASK-013 — Add volumes

| | |
|---|---|
| **Requirement** | NFR-07 |
| **Priority** | P0 |
| **Status** | [x] |

Persist data for Kafka and Cassandra between restarts. Use `docker compose down -v` to wipe.

**Volumes:** `kafka-data`, `cassandra-data`, `minio-data`

---

### TASK-014 — Add healthchecks

| | |
|---|---|
| **Requirement** | NFR-02 |
| **Priority** | P0 |
| **Status** | [x] |

Use `depends_on: condition: service_healthy` so init runs after Kafka/Cassandra are ready.

---

### TASK-015 — Configure profiles

| | |
|---|---|
| **Requirement** | CON-07 |
| **Priority** | P0 |
| **Status** | [x] |

| Profile | Services |
|---------|----------|
| `default` | kafka, cassandra, flink, minio, init |
| `dev` | default + kafka-ui + mock-producer |

---

### TASK-016 — Create repo layout

| | |
|---|---|
| **Priority** | P0 |
| **Status** | [x] |

```
rdg-stream/
├── .cursor/rules/
├── docker-compose.yml
├── init/
├── kafka/init-topics.sh
├── cassandra/schema.cql
├── flink/jobs/
├── producers/mock/
└── docs/
```

---

### TASK-017 — Add Kafka service

| | |
|---|---|
| **Requirement** | CON-01, REQ-02 |
| **Priority** | P0 |
| **Status** | [ ] |

- Image: `apache/kafka:3.7.0`
- Port: `9092`
- Network: `stream`
- Mode: KRaft (no Zookeeper)

---

### TASK-018 — Add Flink services

| | |
|---|---|
| **Requirement** | CON-02, REQ-04 |
| **Priority** | P0 |
| **Status** | [ ] |

- Image: `flink:1.19-scala_2.12-java11`
- Services: `flink-jobmanager` (port 8081), `flink-taskmanager`
- Network: `stream` + `storage`

---

### TASK-019 — Add Cassandra service

| | |
|---|---|
| **Requirement** | CON-03, REQ-06 |
| **Priority** | P0 |
| **Status** | [ ] |

- Image: `cassandra:4.1`
- Port: `9042`
- Network: `storage`
- Env: `MAX_HEAP_SIZE=512M`, `HEAP_NEWSIZE=128M`

---

### TASK-020 — Add MinIO service

| | |
|---|---|
| **Requirement** | CON-04, REQ-08 |
| **Priority** | P0 |
| **Status** | [ ] |

- Image: `minio/minio:latest`
- Ports: `9000` (API), `9001` (console)
- Network: `stream`
- Credentials: `minioadmin` / `minioadmin`

---

## Verify

```bash
docker compose --profile dev up -d
docker compose ps
```

**Test case:** TC-001 — all containers healthy

**Verified (2026-07-18):** All core containers healthy; init created topics and applied schema

---

## Phase complete when

- [x] `docker compose --profile dev up -d` succeeds
- [x] All core containers running (kafka, flink, cassandra, minio)
- [x] No restart loops in `docker compose ps`