# Phase 9 — Validation & Testing

| | |
|---|---|
| **Tasks** | TASK-053 – TASK-059 |
| **Priority** | P0 (053–055, 057) · P1 (056, 058–059) |
| **Previous** | [Phase 6 — Sources](./phase-6-sources.md) |
| **Next** | [Phase 7 — Downstream](./phase-7-downstream.md) · [Phase 10 — Production](./phase-10-production.md) |
| **Use cases** | [use-cases.md](../use-cases.md) |
| **Runner** | `scripts/run-phase9-validation.sh` |

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
| **Status** | [x] |

**TC-001:** `docker compose --profile dev up -d` → all containers healthy.

**Result (2026-07-18):** Pass — kafka, cassandra, flink (custom image), minio healthy; kafka-ui, mock-producer running.

---

### TASK-054 — End-to-end pipeline

| | |
|---|---|
| **Requirement** | REQ-01 – REQ-07 |
| **Priority** | P0 |
| **Status** | [x] |

**TC-016:** Mock producer → Kafka → Flink → Cassandra. Wait 90s, query both tables.

**Result (2026-07-18):** Pass — `metrics_current` 13 rows, `metrics_ts` 37 rows; Flink job RUNNING.

**Re-run (2026-07-18T08:37Z):** Pass — E2E raw=796, current=12, ts=282, job RUNNING (TC-016).

---

### TASK-055 — DLQ routing

| | |
|---|---|
| **Requirement** | REQ-03 |
| **Priority** | P0 |
| **Status** | [x] |

**TC-010:** Send invalid JSON → appears in `metrics.dlq`, not in Cassandra.

**Result (2026-07-18):** Pass — invalid `{"plant_id":"BAD","metric":"x"}` routed to DLQ with wrapped JSON.

**Re-run (2026-07-18T08:37Z):** Pass — DLQ contains `validation_error` wrapper; job stays RUNNING (TC-010).

---

### TASK-056 — TaskManager recovery

| | |
|---|---|
| **Requirement** | NFR-05 |
| **Priority** | P1 |
| **Status** | [x] |

**TC-018:** `docker compose restart flink-taskmanager` → job recovers, data keeps flowing.

**Result (2026-07-18):** Pass — after enabling checkpointing (60s) and fixed-delay restart strategy, job `dc6214b3887605d12f526cedd5d1a035` returned to RUNNING within 60s; Cassandra `updated_at` continued updating after TaskManager restart.

**Re-run (2026-07-18T08:37Z):** Pass — job `11691954…` RUNNING within 60s after TM restart (TC-018).

---

### TASK-057 — Rows in metrics_current

| | |
|---|---|
| **Requirement** | REQ-06 |
| **Priority** | P0 |
| **Status** | [x] |

**TC-013:** `SELECT * FROM rdg.metrics_current` returns rows with non-null values.

**Result (2026-07-18):** Pass — plant-a metrics with non-null value and updated_at.

---

### TASK-058 — Clean teardown

| | |
|---|---|
| **Requirement** | NFR-07 |
| **Priority** | P1 |
| **Status** | [x] |

**TC-024:** `docker compose --profile dev down -v` → fresh restart has zero rows.

**Result (2026-07-18):** Pass — after `down -v` and `up -d`, `metrics_current` count = 0 before Flink job resubmit; stack restarted clean with `minio-init` creating `flink-checkpoints` bucket.

**Re-run (2026-07-18T08:54Z):** Pass — `metrics_current` count = 0 after `down -v` + fresh `up -d` (TC-024).

---

### TASK-059 — Full test plan

| | |
|---|---|
| **Requirement** | All REQ |
| **Priority** | P1 |
| **Status** | [x] |

Run all test cases TC-001 – TC-024. Log results in test-plan run log.

**Result (2026-07-18):** Pass — all P0/P1 cases pass; P2 recovery cases (TC-019, TC-020) not run this pass. See [test-plan.md](../test-plan.md) §6.

**Re-run (2026-07-18T08:37Z):** Pass — 21/21 automated P0/P1 cases (TC-001–018, TC-021–023); TC-019/020 N/A; TC-024 pass on separate run. Evidence: `/tmp/rdg-phase9-2026-07-18T083758Z.log`.

---

## MVP definition of done

- [x] TASK-053, 054, 055, 057 pass
- [x] Mock producer → Kafka → Flink → Cassandra works
- [x] Invalid data goes to DLQ only
- [x] Smoke tests pass

---

## Phase complete when

- [x] All P0 test cases pass
- [x] Test run logged in [test-plan.md](../test-plan.md)
- [x] No open P0 defects
