# Phase 7 — Downstream (Optional)

| | |
|---|---|
| **Tasks** | TASK-046 – TASK-048 |
| **Priority** | P0 (046) · P2–P3 (047–048) |
| **Previous** | [Phase 9 — Validation](./phase-9-validation.md) |
| **Next** | [Phase 8 — Observability](./phase-8-observability.md) |

---

## Goal

Expose stored metrics for dashboards and external applications.

---

## Tasks

### TASK-046 — Verify cqlsh read path

| | |
|---|---|
| **Requirement** | REQ-10 |
| **Priority** | P0 |
| **Status** | [x] |

```bash
docker compose exec cassandra cqlsh -e \
  "SELECT plant_id, metric, value, updated_at FROM rdg.metrics_current LIMIT 10;"

docker compose exec cassandra cqlsh -e \
  "SELECT plant_id, metric, bucket, ts, value FROM rdg.metrics_ts \
   WHERE plant_id='plant-a' AND metric='temperature' AND bucket='2026-07-18' LIMIT 5;"
```

**Note:** `metrics_ts` partition key is `(plant_id, metric, bucket)` — query all three columns (use today's date for `bucket`).

**Result (2026-07-18):** Pass — 10 rows from `metrics_current`; time-series rows returned for `plant-a` / `temperature`.

---

### TASK-047 — Simple read API (optional)

| | |
|---|---|
| **Requirement** | REQ-10 |
| **Priority** | P2 |
| **Status** | [ ] |

Lightweight **FastAPI** service in Docker:
- `GET /metrics/current?plant_id=NM01`
- `GET /metrics/history?plant_id=NM01&metric=power_output_mw`

Connects to Cassandra at `cassandra:9042`.

---

### TASK-048 — External app connection (optional)

| | |
|---|---|
| **Requirement** | REQ-10 |
| **Priority** | P3 |
| **Status** | [ ] |

Document host access for apps outside Docker:

```
Cassandra: localhost:9042
Kafka:     localhost:9092
```

---

## Phase complete when

- [ ] Metrics readable via cqlsh
- [ ] (Optional) Read API returns JSON from Cassandra
