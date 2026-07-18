# Phase 5 — Cassandra Storage

| | |
|---|---|
| **Tasks** | TASK-037 – TASK-041 |
| **Priority** | P0 |
| **Status** | [ ] |
| **Previous** | [Phase 2 — Infrastructure](./phase-2-infrastructure.md) |
| **Next** | [Phase 4 — Flink](./phase-4-flink.md) |

---

## Goal

Define and apply Cassandra schema for metric snapshots and time-series.

---

## Tasks

### TASK-037 — Write schema.cql

| | |
|---|---|
| **Requirement** | REQ-06, REQ-07 |
| **Priority** | P0 |
| **Status** | [x] |

Create `cassandra/schema.cql` with keyspace and tables.

**Deliverable:** [cassandra/schema.cql](../../cassandra/schema.cql)

---

### TASK-038 — Create keyspace rdg

| | |
|---|---|
| **Requirement** | CON-05 |
| **Priority** | P0 |
| **Status** | [x] |

```cql
CREATE KEYSPACE IF NOT EXISTS rdg
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
```

**Verified (2026-07-18):** TC-003 · `DESCRIBE KEYSPACE rdg`

---

### TASK-039 — Create metrics_current table

| | |
|---|---|
| **Requirement** | REQ-06 |
| **Priority** | P0 |
| **Status** | [x] |

```cql
CREATE TABLE rdg.metrics_current (
  plant_id text,
  metric text,
  value double,
  unit text,
  updated_at timestamp,
  PRIMARY KEY ((plant_id), metric)
);
```

**Verified (2026-07-18):** table exists · empty SELECT OK

---

### TASK-040 — Create metrics_ts table

| | |
|---|---|
| **Requirement** | REQ-07 |
| **Priority** | P0 |
| **Status** | [ ] |

```cql
CREATE TABLE rdg.metrics_ts (
  plant_id text,
  metric text,
  bucket date,
  ts timestamp,
  value double,
  PRIMARY KEY ((plant_id, metric, bucket), ts)
) WITH CLUSTERING ORDER BY (ts DESC);
```

**Verified (2026-07-18):** table exists · empty SELECT OK

---

### TASK-041 — Wire init script

| | |
|---|---|
| **Requirement** | REQ-06 |
| **Priority** | P0 |
| **Status** | [ ] |

Init container or entrypoint applies `schema.cql` after Cassandra is healthy.

**Deliverable:** [init/init.sh](../../init/init.sh) applies schema via `cqlsh`

---

## Verify

**Test cases:** TC-003, TC-013, TC-014, TC-015

```bash
docker compose exec cassandra cqlsh -e "DESCRIBE KEYSPACE rdg;"
docker compose exec cassandra cqlsh -e "SELECT * FROM rdg.metrics_current LIMIT 5;"
docker compose exec cassandra cqlsh -e "SELECT * FROM rdg.metrics_ts LIMIT 5;"
```

---

## Phase complete when

- [x] Keyspace `rdg` exists
- [x] Tables `metrics_current` and `metrics_ts` exist
- [ ] Flink sink writes rows (after Phase 4)