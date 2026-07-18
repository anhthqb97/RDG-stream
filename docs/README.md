# Documentation — RDG Stream Platform

> **Start here.** This repo is currently in the **documentation phase** — design, requirements, tasks, and test plans are complete; implementation (Docker, code) has not started yet.

---

## Project status

| Area | Status | Notes |
|------|--------|-------|
| **Documentation** | In progress | Phase 1 (design) complete |
| **Implementation** | Not started | Phases 2–10 pending |
| **Runnable stack** | No | `docker-compose.yml` does not exist yet |

**Documentation complete:** Phase 1 — Design & planning  
**Next step (implementation):** [Phase 2 — Infrastructure](./tasks/phase-2-infrastructure.md)

---

## How to read these docs

| If you want to… | Read this |
|-----------------|-----------|
| Understand the big picture | [../README.md](../README.md) |
| See components and connections | [pipeline.md](./pipeline.md) |
| See workflow charts | [workflow.md](./workflow.md) |
| Read architecture details | [architecture.md](./architecture.md) |
| Track requirements | [tasks/requirements.md](./tasks/requirements.md) |
| Work through build tasks | [tasks/README.md](./tasks/README.md) → phase files |
| Quick checklist while building | [implementation-checklist.md](./implementation-checklist.md) |
| Run tests (after implementation) | [test-plan.md](./test-plan.md) |
| See unresolved decisions | [open-questions.md](./open-questions.md) |

---

## Document roles (avoid duplication)

| Document | Purpose | Audience |
|----------|---------|----------|
| **pipeline.md** | Single reference: diagram, IDs, schema, URLs | Everyone |
| **architecture.md** | Technical design: stack, CQL, Docker services | Developers |
| **workflow.md** | Visual flows: data path, build, test, recovery | New team members |
| **tasks/** | Detailed tasks per phase (TASK-001+) | Implementers |
| **implementation-checklist.md** | Condensed checklist with commands | During build/debug |
| **test-plan.md** | Test cases TC-001 – TC-024 | QA / validation |
| **open-questions.md** | Decisions deferred to implementation | Tech lead |

**Rule:** Design details live in `pipeline.md` and `architecture.md`. Tasks live in `tasks/`. The checklist links to both — it does not duplicate full specs.

---

## Recommended reading order

### 1. Learn the system (documentation only)

```
README.md → pipeline.md → workflow.md → architecture.md
```

### 2. Plan implementation

```
tasks/requirements.md → tasks/README.md → tasks/phase-2-infrastructure.md
```

### 3. Validate (after code exists)

```
test-plan.md → tasks/phase-9-validation.md
```

---

## Documentation map

```mermaid
flowchart TD
    START[docs/README.md] --> README[../README.md]
    START --> PIPE[pipeline.md]
    START --> ARCH[architecture.md]
    START --> WF[workflow.md]
    START --> TASKS[tasks/README.md]
    START --> OQ[open-questions.md]

    TASKS --> REQ[requirements.md]
    TASKS --> P1[phase-1-design ✅]
    TASKS --> P2[phase-2 … phase-10]

    PIPE --> ARCH
    WF --> PIPE
    IC[implementation-checklist.md] --> TASKS
    TP[test-plan.md] --> PIPE
```

---

## File index

### Root

| File | Description |
|------|-------------|
| [README.md](../README.md) | Project overview |

### docs/

| File | Description |
|------|-------------|
| [pipeline.md](./pipeline.md) | Pipeline reference |
| [architecture.md](./architecture.md) | Architecture & schema |
| [workflow.md](./workflow.md) | Workflow charts |
| [implementation-checklist.md](./implementation-checklist.md) | Build checklist |
| [test-plan.md](./test-plan.md) | Test cases |
| [tasks.md](./tasks.md) | Task index |
| [open-questions.md](./open-questions.md) | Open decisions |

### docs/tasks/

| File | Phase | Status |
|------|-------|--------|
| [requirements.md](./tasks/requirements.md) | Requirements | Complete |
| [phase-0-setup.md](./tasks/phase-0-setup.md) | Environment | Not started |
| [phase-1-design.md](./tasks/phase-1-design.md) | Design | **Complete** |
| [phase-2-infrastructure.md](./tasks/phase-2-infrastructure.md) | Docker | Not started |
| [phase-3-kafka.md](./tasks/phase-3-kafka.md) | Kafka | Not started |
| [phase-4-flink.md](./tasks/phase-4-flink.md) | Flink | Not started |
| [phase-5-cassandra.md](./tasks/phase-5-cassandra.md) | Cassandra | Not started |
| [phase-6-sources.md](./tasks/phase-6-sources.md) | Sources | Not started |
| [phase-7-downstream.md](./tasks/phase-7-downstream.md) | Downstream | Not started |
| [phase-8-observability.md](./tasks/phase-8-observability.md) | Observability | Not started |
| [phase-9-validation.md](./tasks/phase-9-validation.md) | Validation | Not started |
| [phase-10-production.md](./tasks/phase-10-production.md) | Production | Not started |

---

## Planned repo layout (implementation phase)

These folders are **documented but not created yet**:

```
rdg-stream/
├── docker-compose.yml      ← Phase 2
├── kafka/init-topics.sh    ← Phase 3
├── cassandra/schema.cql    ← Phase 5
├── flink/jobs/             ← Phase 4
└── producers/mock/         ← Phase 6
```

---

## Documentation changelog

| Date | Change |
|------|--------|
| 2026-07-18 | Initial documentation set: pipeline, architecture, workflow, tasks, test plan |
| 2026-07-18 | All open questions resolved; **Python (PyFlink)** for Flink job and all app services |
