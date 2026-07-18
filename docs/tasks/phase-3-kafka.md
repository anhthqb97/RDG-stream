# Phase 3 — Kafka Ingestion

| | |
|---|---|
| **Tasks** | TASK-021 – TASK-026 |
| **Priority** | P0 |
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
| **Status** | [ ] |

Create `kafka/init-topics.sh` to create all topics on first boot.

---

### TASK-022 — Create metrics.raw topic

| | |
|---|---|
| **Requirement** | REQ-02 |
| **Priority** | P0 |
| **Status** | [ ] |

- Topic: `metrics.raw`
- Partitions: 3 (local)

---

### TASK-023 — Create metrics.dlq topic

| | |
|---|---|
| **Requirement** | REQ-03 |
| **Priority** | P0 |
| **Status** | [ ] |

- Topic: `metrics.dlq`
- Partitions: 1 (local)

---

### TASK-024 — Create events.lifecycle topic

| | |
|---|---|
| **Requirement** | REQ-09 |
| **Priority** | P1 |
| **Status** | [ ] |

- Topic: `events.lifecycle`
- Partitions: 1 (local)

---

### TASK-025 — Wire init container

| | |
|---|---|
| **Requirement** | REQ-02 |
| **Priority** | P0 |
| **Status** | [ ] |

One-shot init service in compose runs `init-topics.sh` after Kafka is healthy.

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

---

## Verify

**Test cases:** TC-002, TC-004, TC-005

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

---

## Phase complete when

- [ ] Topics `metrics.raw`, `metrics.dlq`, `events.lifecycle` exist
- [ ] Manual produce appears in Kafka UI (http://localhost:8090)
- [ ] Console consumer reads message back
