# Git Branching Strategies Tailored for Data Engineering Teams (DataOps)

## 1. Why Data Engineering Needs a Tailored Git Strategy
In traditional software engineering, code changes are stateless: deploying an update replaces binary or server-side logic in memory.
In **Data Engineering**, pipelines have deep side-effects on persistent external state:
- Pipelines mutate distributed databases, data warehouses, and streaming topics.
- A bug in transformation logic can permanently corrupt historical data or trigger cascading downstream failures in analytics and machine learning models.
- Upstream schemas change frequently, demanding disciplined versioning for DDL migrations.

A **DataOps-tailored Git branching model** guarantees that code, schemas, and configurations are thoroughly tested against sanitized and synthetic datasets before ever interacting with production data stores.

---

## 2. The DataOps Branching Model

```
       [hotfix/data-anomaly-patch]
            \               /
             \             /
              ▼           ▼
main   ────────●──────────●───────────── (Production: Live Warehouse / Prod Airflow)
               ▲          ▲
               │          │
staging ───────●──────────●───────────── (Pre-Prod: Synthetic Data Verification)
               ▲
               │
develop ───────●──────────────────────── (Integration: Continuous Testing)
             /   \
            /     \
feature/etl-cleaning  schema/add-brand-col
```

### Branch Roles & Semantics:

| Branch Name | Lifecycle | Connected Environment | Data Scope | Allowed Merges |
| :--- | :--- | :--- | :--- | :--- |
| `main` | Permanent | **Production** | Live production database (`earbuds_staging`), production Airflow cluster | Merges strictly from `staging` (or `hotfix/*`) via protected PRs |
| `staging` | Permanent | **Pre-Production** | Staging database / sanitized replica batches | Merges from `develop` after automated CI test suite passes |
| `develop` | Permanent | **Integration** | Local / dev database (`earbuds_rough`) | Merges from feature, schema, and dag branches |
| `feature/etl-*` | Ephemeral | **Local Dev** | Unit test fixtures & mocked database calls | Merges into `develop` |
| `schema/*` | Ephemeral | **Dev/Staging** | Schema migrations (DDL scripts, staging table alterations) | Merges into `develop` with migration verification |
| `dag/*` | Ephemeral | **Dev Orchestration** | Airflow DAG structure and scheduling parameters | Merges into `develop` after DAG integrity tests pass |
| `hotfix/*` | Ephemeral | **Direct to Prod** | Emergency pipeline patches (e.g. unexpected null handling) | Merged into `main` and immediately back-ported to `develop` |

---

## 3. Branch Naming Conventions
Data engineering teams must follow standardized prefixes for traceability:
- `feature/etl-<description>`: Changes to extraction, transformation, or loading code (e.g., `feature/etl-numeric-sanitization`).
- `feature/connector-<source>`: New ingestion connectors (e.g., `feature/connector-kafka-stream`).
- `schema/<entity>-<change>`: Database table schema migrations (e.g., `schema/earbuds-add-brand-column`).
- `dag/<pipeline-name>`: Orchestration workflow adjustments (e.g., `dag/earbuds-hourly-schedule`).
- `hotfix/<ticket-id>`: Critical production pipeline fixes (e.g., `hotfix/null-price-bypass`).

---

## 4. Pull Request (PR) & Merge Verification Checklist
Before any PR can be merged into `develop`, `staging`, or `main`, it must satisfy the **DataOps Quality Gates**:

- [ ] **Automated CI Pass**: All unit tests and mock integration tests pass with 0 failures.
- [ ] **Test Coverage**: Test coverage remains $\ge 85\%$ across all ETL modules.
- [ ] **Idempotency Guarantee**: Running the pipeline multiple times with the same `batch_id` produces identical warehouse state without duplicate records.
- [ ] **Schema Backward Compatibility**: Schema additions must not break existing downstream views, dashboards, or consumers.
- [ ] **Zero Credential Leaks**: Automated secret scanning ensures no `.env`, passwords, or API keys are committed.
- [ ] **Quarantine Strategy**: Unparseable or corrupt data must be routed to rejected/quarantine tables, never silently dropped or allowed to crash the pipeline.

---

## 5. Developer Workflow Cheat Sheet

### Step 1: Start a new feature branch from `develop`
```bash
git checkout develop
git pull origin develop
git checkout -b feature/etl-numeric-sanitization
```

### Step 2: Implement changes & run automated tests locally
```bash
# Run unit and mock tests locally
make test
# Run local CI pipeline runner
make ci-local
```

### Step 3: Commit with semantic messages
```bash
git add etl/transform.py tests/test_transform.py
git commit -m "feat(transform): enhance clean_numeric to handle non-breaking spaces and currency symbols"
```

### Step 4: Rebase and create Pull Request
```bash
git checkout develop
git pull origin develop
git checkout feature/etl-numeric-sanitization
git rebase develop
git push -u origin feature/etl-numeric-sanitization
```

