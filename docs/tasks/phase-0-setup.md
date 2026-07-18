# Phase 0 — Environment Setup

| | |
|---|---|
| **Tasks** | TASK-001 – TASK-004 |
| **Priority** | P0 |
| **Status** | [ ] |
| **Next** | [Phase 1 — Design](./phase-1-design.md) |

---

## Goal

Prepare local machine to run the RDG Stream Docker stack.

---

## Tasks

### TASK-001 — Install Docker

| | |
|---|---|
| **Requirement** | NFR-02 |
| **Priority** | P0 |
| **Status** | [x] |

Install Docker Desktop (macOS/Windows) or Docker Engine + Compose v2 (Linux).

**Verify:**
```bash
docker --version
docker compose version
```

**Verified (2026-07-18):** Docker 28.0.1 · Compose v2.33.1

---

### TASK-002 — Allocate RAM to Docker

| | |
|---|---|
| **Requirement** | NFR-03 |
| **Priority** | P0 |
| **Status** | [ ] |

Docker Desktop → Settings → Resources → Memory: **8 GB minimum**, 12 GB recommended.

**Verified (2026-07-18):** 8 GB allocated (Docker reports ~7.65 GiB available)

---

### TASK-003 — Free required ports

| | |
|---|---|
| **Requirement** | NFR-02 |
| **Priority** | P0 |
| **Status** | [ ] |

Ensure these ports are not in use:

| Port | Service |
|------|---------|
| 9092 | Kafka |
| 8090 | Kafka UI |
| 8081 | Flink |
| 9042 | Cassandra |
| 9000 | MinIO API |
| 9001 | MinIO Console |

**Verify:**
```bash
lsof -i :9092 -i :8081 -i :9042
```

**Verified (2026-07-18):** All ports free

---

### TASK-004 — Clone repo

| | |
|---|---|
| **Requirement** | — |
| **Priority** | P0 |
| **Status** | [ ] |

```bash
git clone git@github.com:anhthqb97/RDG-stream.git
cd rdg-stream
```

**Verified (2026-07-18):** `/Users/ASUS/Documents/Projects/rdg-stream`

---

## Phase complete when

- [x] Docker and Compose installed
- [x] 8 GB+ RAM allocated
- [x] All ports free
- [x] Repo cloned locally