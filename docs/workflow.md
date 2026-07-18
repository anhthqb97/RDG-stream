# Workflow Charts — RDG Stream Platform

> Visual guides for data flow, build steps, and local testing.  
> Index: [README.md](./README.md) · Components: [pipeline.md](./pipeline.md) · Tasks: [tasks/](./tasks/)

---

## 1. System overview

How all parts connect:

```mermaid
flowchart LR
    subgraph Sources["① Data sources"]
        S1[SCADA]
        S2[Webhook]
        S3[Mock producer]
    end

    subgraph Ingestion["② Ingestion"]
        KF[(Kafka<br/>metrics.raw)]
    end

    subgraph Processing["③ Processing"]
        FL[Flink job]
    end

    subgraph Storage["④ Storage"]
        CS[(Cassandra<br/>rdg.*)]
    end

    subgraph Side["Support"]
        MO[(MinIO)]
        KUI[Kafka UI]
        FUI[Flink UI]
    end

    S1 & S2 & S3 --> KF
    KF --> FL
    FL --> CS
    FL --> MO
    KF -.-> KUI
    FL -.-> FUI
```

| # | Stage | What happens |
|---|-------|--------------|
| ① | Sources | Equipment sends JSON metric events |
| ② | Kafka | Events buffered in `metrics.raw` topic |
| ③ | Flink | Validate, aggregate, route |
| ④ | Cassandra | Store current values + time-series |

---

## 2. Event lifecycle (single message)

What happens to one metric event from start to finish:

```mermaid
flowchart TD
    A[📡 Source emits JSON event] --> B{Valid schema?}
    B -->|Yes| C[Kafka: metrics.raw]
    B -->|No| D[Kafka: metrics.dlq]
    C --> E[Flink consumes event]
    E --> F[KeyBy plant_id + metric]
    F --> G[1-min tumbling window]
    G --> H[Compute avg / min / max]
    H --> I[Write metrics_current]
    H --> J[Write metrics_ts]
    I --> K[✅ Dashboard / API reads data]
    J --> K
    E --> L[Save checkpoint to MinIO]
    D --> M[⚠️ Manual replay or fix]
```

**Timeline example:**

```
T+0s    Mock producer sends { plant_id: NM01, value: 45.2 }
T+1s    Message lands in Kafka metrics.raw
T+2s    Flink picks up message
T+60s   Window closes → aggregate written to Cassandra
T+61s   SELECT from metrics_current returns result
```

---

## 3. Flink processing workflow

Inside the Flink job:

```mermaid
flowchart TD
    START([Job starts]) --> READ[Read from metrics.raw]
    READ --> PARSE[Parse JSON]
    PARSE --> VALID{All fields<br/>present?}

    VALID -->|No| DLQ[Send to metrics.dlq]
    VALID -->|Yes| KEY[KeyBy plant_id + metric]

    KEY --> WIN[Tumbling window 1 min]
    WIN --> AGG[Aggregate: avg, min, max]

    AGG --> SINK1[Sink → metrics_current]
    AGG --> SINK2[Sink → metrics_ts]

    SINK1 --> CP[Checkpoint to MinIO]
    SINK2 --> CP
    CP --> READ

    DLQ --> READ
```

---

## 4. Build workflow (design → running system)

Steps to go from architecture design to a working local stack:

```mermaid
flowchart TD
    A[📋 Receive design] --> B[Define schema & topics]
    B --> C[Create docker-compose.yml]
    C --> D[Start Docker stack]
    D --> E[Init Kafka topics]
    E --> F[Apply Cassandra schema]
    F --> G[Start mock producer]
    G --> H[Build & submit Flink job]
    H --> I{Data in Cassandra?}

    I -->|No| J[Debug: Kafka UI + Flink UI + logs]
    J --> H

    I -->|Yes| K[✅ Run test plan]
    K --> L[Add real sources]
    L --> M[Production hardening]
```

| Step | Command / action | Verify |
|------|------------------|--------|
| 1 | Read [architecture.md](./architecture.md) | Schema defined |
| 2 | `docker compose --profile dev up -d` | All containers healthy |
| 3 | Init scripts run automatically | Topics + tables exist |
| 4 | Submit PyFlink job (`flink run -py`) | Job = RUNNING |
| 5 | Wait 90s | `SELECT * FROM rdg.metrics_current` |

Full checklist: [implementation-checklist.md](./implementation-checklist.md)

---

## 5. Local dev workflow

Day-to-day flow for developers:

```mermaid
flowchart LR
    A[Clone repo] --> B[docker compose<br/>--profile dev up -d]
    B --> C[Open UIs]
    C --> D[Kafka UI :8090]
    C --> E[Flink UI :8081]
    B --> F[Submit Flink job]
    F --> G[Mock producer<br/>sends events]
    G --> H[Query Cassandra<br/>via cqlsh]
    H --> I{Pass?}
    I -->|Yes| J[Develop / test]
    I -->|No| K[Check logs & DLQ]
    K --> F
    J --> L[docker compose down]
```

**Quick commands:**

```bash
docker compose --profile dev up -d          # start
docker compose ps                           # health check
docker compose logs mock-producer --tail 20 # producer
docker compose exec cassandra cqlsh -e "SELECT * FROM rdg.metrics_current LIMIT 5;"
docker compose --profile dev down -v        # stop + reset
```

---

## 6. Test workflow

Smoke test sequence (minimum validation):

```mermaid
flowchart TD
    START([Fresh stack]) --> TC001[TC-001<br/>Stack healthy]
    TC001 --> TC007[TC-007<br/>Mock producer active]
    TC007 --> TC008[TC-008<br/>Flink job RUNNING]
    TC008 --> TC016[TC-016<br/>E2E pipeline]
    TC016 --> TC013[TC-013<br/>Rows in Cassandra]

    TC013 --> PASS{All pass?}
    PASS -->|Yes| DONE([✅ Ready for dev])
    PASS -->|No| FAIL[Log defect<br/>Check DLQ + logs]
    FAIL --> TC008

    DONE --> OPT[Optional:<br/>TC-010 DLQ test<br/>TC-018 recovery test]
```

Full test cases: [test-plan.md](./test-plan.md)

---

## 7. Failure & recovery workflow

What happens when a service restarts:

```mermaid
flowchart TD
    subgraph Normal["Normal operation"]
        P[Producer] --> K[Kafka] --> F[Flink] --> C[Cassandra]
        F --> M[MinIO checkpoint]
    end

    subgraph Recovery["After TaskManager restart"]
        R1[TaskManager down] --> R2[Flink reads checkpoint<br/>from MinIO]
        R2 --> R3[Job resumes from<br/>last offset]
        R3 --> R4[Processing continues]
    end

    subgraph DLQ["Bad data path"]
        B[Invalid JSON] --> D[metrics.dlq]
        D --> FIX[Fix schema or replay]
        FIX --> K
    end
```

---

## 8. Topic routing

Where messages go:

```mermaid
flowchart LR
    subgraph Producers
        S1[S1 SCADA]
        S2[S2 Webhook]
        S3[S3 Mock]
    end

    subgraph Topics
        RAW[metrics.raw]
        DLQ[metrics.dlq]
        LIFE[events.lifecycle]
    end

    subgraph Consumers
        FL[Flink job]
        MAN[Manual replay]
        DOWN[Downstream optional]
    end

    S1 & S2 & S3 --> RAW
    RAW --> FL
    FL -->|invalid| DLQ
    FL -->|optional| LIFE
    DLQ --> MAN
    LIFE --> DOWN
```

---

## Legend

| Symbol | Meaning |
|--------|---------|
| Solid arrow `-->` | Data flow |
| Dotted arrow `-.->` | Monitor / debug only |
| Diamond `{}` | Decision point |
| Cylinder `[( )]` | Database / queue |
