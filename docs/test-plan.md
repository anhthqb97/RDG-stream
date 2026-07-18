# Test Plan — RDG Stream Platform

> Local Docker test plan with test cases for the full pipeline.  
> **Status:** Ready for use after Phase 2–6 implementation.
>
> Index: [README.md](./README.md) · Reference: [pipeline.md](./pipeline.md) · [workflow.md](./workflow.md) · [architecture.md](./architecture.md) · Validation tasks: [tasks/phase-9-validation.md](./tasks/phase-9-validation.md)

---

## 1. Scope

| In scope | Out of scope (later) |
|----------|----------------------|
| Docker stack startup / health | Multi-node Kafka / Cassandra cluster |
| Kafka topics & produce/consume | TLS / SASL auth |
| Flink job processing | Load test at prod scale |
| Cassandra read/write | SCADA real integration |
| MinIO checkpoints | CI automation (Phase 2) |
| End-to-end data flow | |

**Environment:** local · `docker compose --profile dev`

**Test data:** mock producer (S3) + manual JSON via `kafka-console-producer`

---

## 2. Test setup

```bash
# Fresh start before test run
docker compose --profile dev down -v
docker compose --profile dev up -d

# Wait until healthy (~60s)
docker compose ps

# Submit PyFlink job (when rdg_job.py exists)
# docker compose exec flink-jobmanager flink run -py /opt/flink/jobs/rdg_job.py
```

| Item | Value |
|------|-------|
| Kafka bootstrap | `localhost:9092` |
| Flink UI | http://localhost:8081 |
| Kafka UI | http://localhost:8090 |
| Cassandra | `localhost:9042` |
| MinIO console | http://localhost:9001 |

**Valid sample message:**

```json
{"plant_id":"NM01","equipment_id":"GEN-01","metric":"power_output_mw","value":45.2,"unit":"MW","ts":"2026-07-18T10:00:00Z"}
```

---

## 3. Test case summary

| ID | Category | Title | Priority |
|----|----------|-------|----------|
| TC-001 | Infra | Stack starts with all services healthy | P0 |
| TC-002 | Infra | Init creates Kafka topics | P0 |
| TC-003 | Infra | Init creates Cassandra keyspace & tables | P0 |
| TC-004 | KF | Produce valid message to `metrics.raw` | P0 |
| TC-005 | KF | Consume message from `metrics.raw` | P1 |
| TC-006 | KF | Topic `metrics.dlq` exists | P1 |
| TC-007 | S3 | Mock producer sends events continuously | P0 |
| TC-008 | FL | Flink job submits and reaches RUNNING | P0 |
| TC-009 | FL | Flink consumes from `metrics.raw` | P0 |
| TC-010 | FL | Invalid message routed to `metrics.dlq` | P0 |
| TC-011 | FL | Window aggregate computes avg/min/max | P1 |
| TC-012 | FL | Checkpoint saved to MinIO | P1 |
| TC-013 | CS | Row written to `metrics_current` | P0 |
| TC-014 | CS | Row written to `metrics_ts` | P0 |
| TC-015 | CS | Query by plant_id returns expected data | P1 |
| TC-016 | E2E | Full pipeline: S3 → KF → FL → CS | P0 |
| TC-017 | E2E | Manual produce → appears in Cassandra | P0 |
| TC-018 | Recovery | Restart TaskManager — job recovers | P1 |
| TC-019 | Recovery | Restart Kafka — pipeline resumes | P2 |
| TC-020 | Recovery | Restart Cassandra — Flink retries sink | P2 |
| TC-021 | Ops | Kafka UI shows topics and messages | P1 |
| TC-022 | Ops | Flink Dashboard shows job metrics | P1 |
| TC-023 | Ops | MinIO bucket contains checkpoint files | P2 |
| TC-024 | Cleanup | `docker compose down -v` removes data | P1 |

---

## 4. Test cases (detail)

### 4.1 Infrastructure

---

#### TC-001 — Stack starts with all services healthy

| Field | Value |
|-------|-------|
| **Priority** | P0 |
| **Components** | KF, FL, CS, MO, KUI, S3 |

**Preconditions:** Docker Desktop running, ports 8090/8081/9042/9092/9000/9001 free

**Steps:**
1. Run `docker compose --profile dev up -d`
2. Wait 60 seconds
3. Run `docker compose ps`

**Expected result:**
- [ ] `kafka`, `cassandra`, `flink-jobmanager`, `flink-taskmanager`, `minio` → `healthy` or `running`
- [ ] `kafka-ui`, `mock-producer` → `running`
- [ ] No container in `Restarting` or `Exit` state

---

#### TC-002 — Init creates Kafka topics

| Field | Value |
|-------|-------|
| **Priority** | P0 |
| **Components** | KF, init |

**Preconditions:** TC-001 passed

**Steps:**
1. Run:
   ```bash
   docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
     --bootstrap-server localhost:9092 --list
   ```

**Expected result:**
- [ ] Topics exist: `metrics.raw`, `metrics.dlq`, `events.lifecycle`

---

#### TC-003 — Init creates Cassandra keyspace & tables

| Field | Value |
|-------|-------|
| **Priority** | P0 |
| **Components** | CS, init |

**Preconditions:** TC-001 passed

**Steps:**
1. Run:
   ```bash
   docker compose exec cassandra cqlsh -e "DESCRIBE KEYSPACE rdg;"
   ```

**Expected result:**
- [ ] Keyspace `rdg` exists
- [ ] Tables `metrics_current` and `metrics_ts` exist

---

### 4.2 Kafka (KF)

---

#### TC-004 — Produce valid message to `metrics.raw`

| Field | Value |
|-------|-------|
| **Priority** | P0 |
| **Components** | KF |

**Preconditions:** TC-002 passed

**Steps:**
1. Run console producer:
   ```bash
   docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
     --bootstrap-server localhost:9092 \
     --topic metrics.raw << 'EOF'
   {"plant_id":"TC04","equipment_id":"GEN-01","metric":"power_output_mw","value":10.0,"unit":"MW","ts":"2026-07-18T10:00:00Z"}
   EOF
   ```
2. Open Kafka UI → Topics → `metrics.raw` → Messages

**Expected result:**
- [ ] Message appears in topic with correct JSON fields

---

#### TC-005 — Consume message from `metrics.raw`

| Field | Value |
|-------|-------|
| **Priority** | P1 |
| **Components** | KF |

**Preconditions:** TC-004 passed

**Steps:**
1. Run:
   ```bash
   docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
     --bootstrap-server localhost:9092 \
     --topic metrics.raw \
     --from-beginning \
     --max-messages 1 \
     --timeout-ms 10000
   ```

**Expected result:**
- [ ] Consumer prints JSON with `plant_id":"TC04"`

---

#### TC-006 — Topic `metrics.dlq` exists

| Field | Value |
|-------|-------|
| **Priority** | P1 |
| **Components** | KF |

**Preconditions:** TC-002 passed

**Steps:**
1. List topics (same as TC-002)
2. Describe DLQ topic:
   ```bash
   docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
     --bootstrap-server localhost:9092 \
     --describe --topic metrics.dlq
   ```

**Expected result:**
- [ ] `metrics.dlq` listed
- [ ] Partition count ≥ 1

---

### 4.3 Source — Mock producer (S3)

---

#### TC-007 — Mock producer sends events continuously

| Field | Value |
|-------|-------|
| **Priority** | P0 |
| **Components** | S3, KF |

**Preconditions:** TC-001 passed, mock-producer running

**Steps:**
1. Run `docker compose logs mock-producer --tail 20`
2. Open Kafka UI → `metrics.raw` → latest messages

**Expected result:**
- [ ] Logs show periodic publish (no repeated errors)
- [ ] New messages appear in Kafka UI within 10 seconds

---

### 4.4 Flink (FL)

---

#### TC-008 — Flink job submits and reaches RUNNING

| Field | Value |
|-------|-------|
| **Priority** | P0 |
| **Components** | FL, FUI |

**Preconditions:** TC-001 passed, PyFlink job deployed

**Steps:**
1. Open http://localhost:8081
2. Submit job (UI or CLI)
3. Check Jobs → Running Jobs

**Expected result:**
- [ ] Job status = **RUNNING**
- [ ] No exception in TaskManager logs:
  ```bash
  docker compose logs flink-taskmanager --tail 50
  ```

---

#### TC-009 — Flink consumes from `metrics.raw`

| Field | Value |
|-------|-------|
| **Priority** | P0 |
| **Components** | FL, KF |

**Preconditions:** TC-007 and TC-008 passed

**Steps:**
1. Open Flink Dashboard → job → Metrics
2. Check records consumed counter increases
3. Optional — consumer lag:
   ```bash
   docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
     --bootstrap-server localhost:9092 \
     --describe --group flink-metrics-job
   ```

**Expected result:**
- [ ] Input records/sec > 0
- [ ] Consumer lag stable (not growing unbounded)

---

#### TC-010 — Invalid message routed to `metrics.dlq`

| Field | Value |
|-------|-------|
| **Priority** | P0 |
| **Components** | FL, KF |

**Preconditions:** TC-008 passed

**Steps:**
1. Produce invalid JSON:
   ```bash
   docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
     --bootstrap-server localhost:9092 \
     --topic metrics.raw << 'EOF'
   {"plant_id":"BAD","metric":"x"}
   EOF
   ```
2. Wait 30 seconds
3. Consume from DLQ:
   ```bash
   docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
     --bootstrap-server localhost:9092 \
     --topic metrics.dlq \
     --from-beginning \
     --timeout-ms 15000
   ```

**Expected result:**
- [ ] Invalid record appears in `metrics.dlq`
- [ ] Flink job remains RUNNING

---

#### TC-011 — Window aggregate computes avg/min/max

| Field | Value |
|-------|-------|
| **Priority** | P1 |
| **Components** | FL, CS |

**Preconditions:** TC-008 passed

**Steps:**
1. Produce 3 messages same key, different values:
   ```bash
   for v in 10 20 30; do
     docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
       --bootstrap-server localhost:9092 --topic metrics.raw << EOF
   {"plant_id":"TC11","equipment_id":"GEN-01","metric":"power_output_mw","value":$v,"unit":"MW","ts":"2026-07-18T10:00:00Z"}
   EOF
   done
   ```
2. Wait 70 seconds (1-min window + buffer)
3. Query Cassandra:
   ```bash
   docker compose exec cassandra cqlsh -e \
     "SELECT * FROM rdg.metrics_current WHERE plant_id='TC11';"
   ```

**Expected result:**
- [ ] Row exists for `plant_id=TC11`, `metric=power_output_mw`
- [ ] Aggregated value ≈ avg(10,20,30) = **20.0** (± tolerance if partial window)

---

#### TC-012 — Checkpoint saved to MinIO

| Field | Value |
|-------|-------|
| **Priority** | P1 |
| **Components** | FL, MO |

**Preconditions:** TC-008 passed, job running ≥ 2 min

**Steps:**
1. Open http://localhost:9001 (login: `minioadmin` / `minioadmin`)
2. Browse bucket `flink-checkpoints`

**Expected result:**
- [ ] Bucket exists
- [ ] Contains checkpoint metadata/files updated recently

---

### 4.5 Cassandra (CS)

---

#### TC-013 — Row written to `metrics_current`

| Field | Value |
|-------|-------|
| **Priority** | P0 |
| **Components** | CS, FL |

**Preconditions:** TC-016 or TC-007 + TC-008 passed, wait ≥ 70s

**Steps:**
```bash
docker compose exec cassandra cqlsh -e \
  "SELECT plant_id, metric, value, updated_at FROM rdg.metrics_current LIMIT 10;"
```

**Expected result:**
- [ ] At least 1 row returned
- [ ] `plant_id`, `metric`, `value`, `updated_at` are non-null

---

#### TC-014 — Row written to `metrics_ts`

| Field | Value |
|-------|-------|
| **Priority** | P0 |
| **Components** | CS, FL |

**Preconditions:** TC-013 passed

**Steps:**
```bash
docker compose exec cassandra cqlsh -e \
  "SELECT plant_id, metric, bucket, ts, value FROM rdg.metrics_ts LIMIT 10;"
```

**Expected result:**
- [ ] At least 1 time-series row
- [ ] `ts` ordered descending per partition

---

#### TC-015 — Query by plant_id returns expected data

| Field | Value |
|-------|-------|
| **Priority** | P1 |
| **Components** | CS |

**Preconditions:** TC-004 produced `plant_id=TC04`, pipeline processed it

**Steps:**
```bash
docker compose exec cassandra cqlsh -e \
  "SELECT * FROM rdg.metrics_current WHERE plant_id='TC04';"
```

**Expected result:**
- [ ] Row found for plant `TC04`
- [ ] `metric` = `power_output_mw`

---

### 4.6 End-to-end (E2E)

---

#### TC-016 — Full pipeline: S3 → KF → FL → CS

| Field | Value |
|-------|-------|
| **Priority** | P0 |
| **Components** | S3, KF, FL, CS |

**Preconditions:** Fresh stack, Flink job RUNNING

**Steps:**
1. Confirm mock-producer logs active (TC-007)
2. Confirm Flink consuming (TC-009)
3. Wait 90 seconds
4. Query `metrics_current` and `metrics_ts`

**Expected result:**
- [ ] Kafka has messages in `metrics.raw`
- [ ] Flink job RUNNING, no errors
- [ ] Cassandra has rows from mock plant IDs
- [ ] `metrics.dlq` empty or only from intentional bad tests

**Pass criteria:** all three layers show data for the same time window

---

#### TC-017 — Manual produce → appears in Cassandra

| Field | Value |
|-------|-------|
| **Priority** | P0 |
| **Components** | KF, FL, CS |

**Preconditions:** TC-008 passed

**Steps:**
1. Produce unique plant:
   ```bash
   docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
     --bootstrap-server localhost:9092 --topic metrics.raw << 'EOF'
   {"plant_id":"TC17","equipment_id":"GEN-99","metric":"voltage_kv","value":220.5,"unit":"kV","ts":"2026-07-18T11:00:00Z"}
   EOF
   ```
2. Wait 70 seconds
3. Query:
   ```bash
   docker compose exec cassandra cqlsh -e \
     "SELECT * FROM rdg.metrics_current WHERE plant_id='TC17';"
   ```

**Expected result:**
- [ ] Row with `metric=voltage_kv`, `value=220.5`

---

### 4.7 Recovery & resilience

---

#### TC-018 — Restart TaskManager — job recovers

| Field | Value |
|-------|-------|
| **Priority** | P1 |
| **Components** | FL, MO |

**Preconditions:** TC-008 passed, checkpoints enabled

**Steps:**
1. Note current job ID in Flink UI
2. Run `docker compose restart flink-taskmanager`
3. Wait 60 seconds
4. Check Flink UI and Cassandra for new data

**Expected result:**
- [ ] Job returns to RUNNING (same or restored job)
- [ ] New rows still appear in Cassandra after restart

---

#### TC-019 — Restart Kafka — pipeline resumes

| Field | Value |
|-------|-------|
| **Priority** | P2 |
| **Components** | KF, FL |

**Preconditions:** TC-016 passed

**Steps:**
1. Run `docker compose restart kafka`
2. Wait 60 seconds
3. Produce test message (TC-017 format, `plant_id=TC19`)
4. Wait 70 seconds, query Cassandra

**Expected result:**
- [ ] Kafka healthy after restart
- [ ] Message for `TC19` eventually in Cassandra

---

#### TC-020 — Restart Cassandra — Flink retries sink

| Field | Value |
|-------|-------|
| **Priority** | P2 |
| **Components** | CS, FL |

**Preconditions:** TC-008 passed

**Steps:**
1. Run `docker compose restart cassandra`
2. Wait 90 seconds for Cassandra ready
3. Produce message `plant_id=TC20`
4. Wait 70 seconds, query Cassandra

**Expected result:**
- [ ] Cassandra healthy
- [ ] Row for `TC20` appears (may delay during restart)
- [ ] Flink job stays RUNNING or recovers without manual redeploy

---

### 4.8 Observability (Ops)

---

#### TC-021 — Kafka UI shows topics and messages

| Field | Value |
|-------|-------|
| **Priority** | P1 |
| **Components** | KUI, KF |

**Preconditions:** TC-001 passed

**Steps:**
1. Open http://localhost:8090
2. Navigate to Topics → `metrics.raw`

**Expected result:**
- [ ] UI loads without error
- [ ] Topics listed match TC-002
- [ ] Message preview shows valid JSON

---

#### TC-022 — Flink Dashboard shows job metrics

| Field | Value |
|-------|-------|
| **Priority** | P1 |
| **Components** | FUI, FL |

**Preconditions:** TC-008 passed

**Steps:**
1. Open http://localhost:8081
2. Open running job → Metrics tab

**Expected result:**
- [ ] `numRecordsIn`, `numRecordsOut` visible
- [ ] No sustained backpressure (red) under normal mock load

---

#### TC-023 — MinIO bucket contains checkpoint files

| Field | Value |
|-------|-------|
| **Priority** | P2 |
| **Components** | MO, FL |

**Preconditions:** TC-012 passed

**Steps:**
1. MinIO console → `flink-checkpoints` bucket
2. Note file count and last modified time
3. Wait 2 min, refresh

**Expected result:**
- [ ] File timestamps update after checkpoint interval

---

### 4.9 Cleanup

---

#### TC-024 — `docker compose down -v` removes data

| Field | Value |
|-------|-------|
| **Priority** | P1 |
| **Components** | All |

**Preconditions:** Stack was running with data

**Steps:**
1. Run `docker compose --profile dev down -v`
2. Run `docker compose --profile dev up -d`
3. Wait 60s, query Cassandra:
   ```bash
   docker compose exec cassandra cqlsh -e \
     "SELECT COUNT(*) FROM rdg.metrics_current;"
   ```

**Expected result:**
- [ ] Stack starts clean
- [ ] Row count = 0 until new data produced

---

## 5. Test execution order

Run in this order for first full pass:

```
TC-001 → TC-002 → TC-003 → TC-007 → TC-008
    → TC-009 → TC-016 → TC-013 → TC-014
    → TC-004 → TC-010 → TC-017
    → TC-021 → TC-022 → TC-012
    → TC-018 → TC-024
```

**Smoke test (minimum):** TC-001, TC-007, TC-008, TC-016, TC-013

---

## 6. Test run log

| Run date | Tester | Environment | Pass | Fail | Blocked | Notes |
|----------|--------|-------------|------|------|---------|-------|
| 2026-07-18 | local | local Docker | TC-001,007,008,010,013,016 | — | 056,058,059 | MVP smoke pass |
| | | | | | | |

**Result key:** Pass · Fail · Blocked · N/A

---

## 7. Defect template

When a test fails, log:

| Field | Example |
|-------|---------|
| Defect ID | BUG-001 |
| Linked TC | TC-010 |
| Severity | High |
| Steps to reproduce | … |
| Actual | Message not in DLQ |
| Expected | Invalid JSON in `metrics.dlq` |
| Logs | `docker compose logs flink-taskmanager` |
