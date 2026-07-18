# Phase 1 — Design & Planning

| | |
|---|---|
| **Tasks** | TASK-005 – TASK-010 |
| **Priority** | P0 |
| **Status** | **Complete** (documentation phase) |
| **Previous** | [Phase 0 — Setup](./phase-0-setup.md) |
| **Next** | [Phase 2 — Infrastructure](./phase-2-infrastructure.md) |

---

## Goal

Define pipeline architecture, data contracts, and topic schema before writing code.

---

## Tasks

### TASK-005 — Map pipeline layers

| | |
|---|---|
| **Requirement** | REQ-01 – REQ-07 |
| **Priority** | P0 |
| **Status** | [x] |

Document layers: **Sources → Kafka → Flink → Cassandra**

**Deliverable:** [pipeline.md](../pipeline.md)

---

### TASK-006 — Document data flows

| | |
|---|---|
| **Requirement** | REQ-02, REQ-06, REQ-07 |
| **Priority** | P0 |
| **Status** | [x] |

Map each flow: producer → topic → Flink job → Cassandra table.

**Deliverable:** [pipeline.md](../pipeline.md) connections table

---

### TASK-007 — Confirm free stack

| | |
|---|---|
| **Requirement** | NFR-01, CON-01 – CON-04 |
| **Priority** | P0 |
| **Status** | [x] |

Confirm no paid services: Confluent Cloud, AWS MSK, DynamoDB, managed Flink.

**Deliverable:** [architecture.md](../architecture.md) §2

---

### TASK-008 — Finalize JSON schema

| | |
|---|---|
| **Requirement** | REQ-01 |
| **Priority** | P0 |
| **Status** | [x] |

**Deliverable:** [pipeline.md](../pipeline.md) message schema

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

### TASK-009 — Define Kafka topics

| | |
|---|---|
| **Requirement** | REQ-02, REQ-03 |
| **Priority** | P0 |
| **Status** | [x] |

**Deliverable:** [architecture.md](../architecture.md) §4

| Topic | Partitions (local) | Purpose |
|-------|-------------------|---------|
| `metrics.raw` | 3 | Incoming events |
| `metrics.dlq` | 1 | Invalid records |
| `events.lifecycle` | 1 | Optional downstream |

---

### TASK-010 — Document open questions

| | |
|---|---|
| **Requirement** | — |
| **Priority** | P1 |
| **Status** | [x] |

**Deliverable:** [open-questions.md](../open-questions.md)

---

## Phase complete when

- [x] Pipeline diagram and component IDs documented
- [x] JSON schema finalized
- [x] Kafka topics defined
- [x] [architecture.md](../architecture.md) and [pipeline.md](../pipeline.md) reviewed
- [x] Open questions logged in [open-questions.md](../open-questions.md)
