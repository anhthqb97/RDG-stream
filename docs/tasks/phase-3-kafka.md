# Phase 3 — Kafka Ingestion

| | |
|---|---|
| **Tasks** | TASK-021 – TASK-026 |
| **Priority** | P0 |
| **Status** | [ ] |
| **Previous** | [Phase 2 — Infrastructure](./phase-2-infrastructure.md) |
| **Next** | [Phase 4 — Flink](./phase-4-flink.md) |

---

## Goal

Create Kafka topics and verify produce/consume on `metrics.raw`.

---

## Tasks

### TASK-021 — Write init-topics.sh

| | |
|---|---|
| **Requirement** | REQ-02, REQ-03 |
| **Priority** | P0 |
| **Status** | [x] |

Create `kafka/init-topics.sh` to create all topics on first boot.

**Deliverable:** [kafka/init-topics.sh](../../kafka/init-topics.sh)

---

### TASK-022 — Create metrics.raw topic

| | |
|---|---|
| **Requirement** | REQ-02 |
| **Priority** | P0 |
| **Status** | [x] |

- Topic: `metrics.raw`
- Partitions: 3 (local)

**Verified (2026-07-18):** PartitionCount: 3

---

### TASK-023 — Create metrics.dlq topic

| | |
|---|---|
| **Requirement** | REQ-03 |
| **Priority** | P0 |
| **Status** | [ ] |

- Topic: `metrics.dlq`
- Partitions: 1 (local)

**Verified (2026-07-18):** PartitionCount: 1

---

### TASK-024 — Create events.lifecycle topic

| | |
|---|---|
| **Requirement** | REQ-09 |
| **Priority** | P1 |
| **Status** | [ ] |

- Topic: `events.lifecycle`
- Partitions: 1 (local)

**Verified (2026-07-18):** PartitionCount: 1

---

### TASK-025 — Wire init container

| | |
|---|---|
| **Requirement** | REQ-02 |
| **Priority** | P0 |
| **Status** | [ ] |

One-shot init service in compose runs `init-topics.sh` after Kafka is healthy.

**Deliverable:** [init/init.sh](../../init/init.sh) + `docker-compose.yml` `init` service

---

### TASK-026 — Test manual produce

| | |
|---|---|
| **Requirement** | REQ-01 |
| **Priority** | P0 |
| **Status** | [ ] |

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic metrics.raw << 'EOF'
{"plant_id":"TEST","equipment_id":"GEN-01","metric":"power_output_mw","value":10.0,"unit":"MW","ts":"2026-07-18T10:00:00Z"}
EOF
```

**Verified (2026-07-18):** TC-004 produce OK · TC-005 consume OK

---

## Verify

**Test cases:** TC-002, TC-004, TC-005

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

---

## Phase complete when

- [x] Topics `metrics.raw`, `metrics.dlq`, `events.lifecycle` exist
- [x] Manual produce appears in Kafka UI (http://localhost:8090)
- [x] Console consumer reads message back