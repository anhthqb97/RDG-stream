# Phase 10 — Production (Later)

| | |
|---|---|
| **Tasks** | TASK-060 – TASK-065 |
| **Priority** | P3 |
| **Previous** | [Phase 9 — Validation](./phase-9-validation.md) |

---

## Goal

Scale and harden the stack for self-hosted production (still free OSS).

---

## Tasks

### TASK-060 — Multi-broker Kafka

| | |
|---|---|
| **Requirement** | CON-07 |
| **Priority** | P3 |
| **Status** | [ ] |

Deploy 3-broker KRaft cluster or Strimzi on k3s.

---

### TASK-061 — Cassandra cluster

| | |
|---|---|
| **Requirement** | CON-05 |
| **Priority** | P3 |
| **Status** | [ ] |

3-node cluster with `NetworkTopologyStrategy`, replication factor 3.

---

### TASK-062 — MinIO distributed mode

| | |
|---|---|
| **Requirement** | REQ-08 |
| **Priority** | P3 |
| **Status** | [ ] |

Distributed MinIO for durable checkpoint storage.

---

### TASK-063 — Security hardening

| | |
|---|---|
| **Priority** | P3 |
| **Status** | [ ] |

- Kafka SASL/SSL
- Cassandra authentication
- Network isolation
- No public ports except edge API gateway

---

### TASK-064 — CI pipeline

| | |
|---|---|
| **Priority** | P3 |
| **Status** | [ ] |

GitHub Actions (free tier):
- Build and test PyFlink job on push
- Run lint / unit tests
- Optional: deploy to staging

---

### TASK-065 — Real SCADA adapter

| | |
|---|---|
| **Requirement** | REQ-01 |
| **Priority** | P3 |
| **Status** | [ ] |

Replace mock producer with S1 SCADA / Historian adapter → Kafka.

---

## Phase complete when

- [ ] Multi-node stack deployed
- [ ] TLS/auth configured
- [ ] CI builds and tests Flink job
- [ ] Real data source connected
