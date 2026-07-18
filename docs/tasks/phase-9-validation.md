# Phase 9 — Validation & Testing

| | |
|---|---|
| **Tasks** | TASK-053 – TASK-059 |
| **Priority** | P0 (053–055, 057) · P1 (056, 058–059) |
| **Previous** | [Phase 6 — Sources](./phase-6-sources.md) |
| **Next** | [Phase 7 — Downstream](./phase-7-downstream.md) · [Phase 10 — Production](./phase-10-production.md) |

---

## Goal

Validate full pipeline end-to-end using [test-plan.md](../test-plan.md).

---

## Smoke test sequence

```
TC-001 → TC-007 → TC-008 → TC-016 → TC-013
```

---

## Tasks

### TASK-053 — Stack starts healthy

| | |
|---|---|
| **Requirement** | NFR-02 |
| **Priority** | P0 |
| **Status** | [ ] |

**TC-001:** `docker compose --profile dev up -d` → all containers healthy.

---

### TASK-054 — End-to-end pipeline

| | |
|---|---|
| **Requirement** | REQ-01 – REQ-07 |
| **Priority** | P0 |
| **Status** | [ ] |

**TC-016:** Mock producer → Kafka → Flink → Cassandra. Wait 90s, query both tables.

---

### TASK-055 — DLQ routing

| | |
|---|---|
| **Requirement** | REQ-03 |
| **Priority** | P0 |
| **Status** | [ ] |

**TC-010:** Send invalid JSON → appears in `metrics.dlq`, not in Cassandra.

---

### TASK-056 — TaskManager recovery

| | |
|---|---|
| **Requirement** | NFR-05 |
| **Priority** | P1 |
| **Status** | [ ] |

**TC-018:** `docker compose restart flink-taskmanager` → job recovers, data keeps flowing.

---

### TASK-057 — Rows in metrics_current

| | |
|---|---|
| **Requirement** | REQ-06 |
| **Priority** | P0 |
| **Status** | [ ] |

**TC-013:** `SELECT * FROM rdg.metrics_current` returns rows with non-null values.

---

### TASK-058 — Clean teardown

| | |
|---|---|
| **Requirement** | NFR-07 |
| **Priority** | P1 |
| **Status** | [ ] |

**TC-024:** `docker compose --profile dev down -v` → fresh restart has zero rows.

---

### TASK-059 — Full test plan

| | |
|---|---|
| **Requirement** | All REQ |
| **Priority** | P1 |
| **Status** | [ ] |

Run all test cases TC-001 – TC-024. Log results in test-plan run log.

---

## MVP definition of done

- [ ] TASK-053, 054, 055, 057 pass
- [ ] Mock producer → Kafka → Flink → Cassandra works
- [ ] Invalid data goes to DLQ only
- [ ] Smoke tests pass

---

## Phase complete when

- [ ] All P0 test cases pass
- [ ] Test run logged in [test-plan.md](../test-plan.md)
- [ ] No open P0 defects
