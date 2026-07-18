# Phase 6 — Data Sources

| | |
|---|---|
| **Tasks** | TASK-042 – TASK-045 |
| **Priority** | P0 (042–043) · P2 (044–045) |
| **Previous** | [Phase 4 — Flink](./phase-4-flink.md) |
| **Next** | [Phase 9 — Validation](./phase-9-validation.md) |

---

## Goal

Generate test data locally via mock producer (substitute for SCADA/webhook).

---

## Source mapping

| Real source | Local substitute |
|-------------|------------------|
| SCADA / Historian (S1) | Mock producer |
| REST webhook (S2) | Webhook adapter (optional) |
| File / IoT | kafka-console-producer replay |

---

## Tasks

### TASK-042 — Build mock producer

| | |
|---|---|
| **Requirement** | REQ-01, NFR-02 |
| **Priority** | P0 |
| **Status** | [x] |

- **Python** script in `producers/mock/` (see [open-questions.md](../open-questions.md))
- Publishes valid JSON to `metrics.raw` every **2 seconds**
- Random `plant_id`, `metric`, `value`
- Kafka client: `confluent-kafka` or `kafka-python`

---

### TASK-043 — Add to dev profile

| | |
|---|---|
| **Requirement** | REQ-01 |
| **Priority** | P0 |
| **Status** | [ ] |

Add `mock-producer` service to compose `dev` profile. Depends on Kafka healthy.

---

### TASK-044 — REST webhook adapter (optional)

| | |
|---|---|
| **Requirement** | REQ-01 |
| **Priority** | P2 |
| **Status** | [ ] |

Small **Python** HTTP server (FastAPI) on port **8080**:
- `POST /metrics` → publish to Kafka
- Local: no auth · Prod: `X-API-Key` header

---

### TASK-045 — JSON file replay (optional)

| | |
|---|---|
| **Requirement** | REQ-01 |
| **Priority** | P2 |
| **Status** | [ ] |

Replay events from file via `kafka-console-producer`.

---

## Verify

**Test case:** TC-007

```bash
docker compose logs mock-producer --tail 20
# Kafka UI → metrics.raw → new messages every ~2s
```

---

## Phase complete when

- [ ] Mock producer running in dev profile
- [ ] Messages visible in Kafka UI
- [ ] No repeated errors in producer logs
