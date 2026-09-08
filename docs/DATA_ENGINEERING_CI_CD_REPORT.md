# Academic Project Report: CI/CD & Version Control for Data Engineering Pipelines

**Course**: Data Engineering (Semester 9)  
**Project**: Earbuds Resilient ETL Pipeline  
**Topics Covered**:
1. Code Versioning & DataOps Git Branching Strategies
2. Pipeline as Code: Infrastructure & Configuration Programmatic Management
3. Continuous Integration / Continuous Deployment (CI/CD) & Automated Test Pyramid
4. Automated Runners: GitHub Actions & Local CI Runner

---

## 1. Executive Summary
Data engineering systems operate under fundamentally different constraints than standard web applications. While web services are largely stateless, data pipelines interact with persistent state in data lakes, event brokers, and relational warehouses. Code changes or unvalidated schema shifts have irreversible side effects on analytical truth.

This project implements a production-grade **DataOps CI/CD and Version Control Framework** for the Earbuds Data Pipeline, ensuring that every data transformation is verified through unit tests, mocked database interactions, and automated CI quality gates before deployment.

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                                   DATALAKE / SOURCE                                    │
│                              Raw Batches (earbuds_rough)                               │
└───────────────────────────────────────────┬────────────────────────────────────────────┘
                                            │
                                  [STAGE 1: EXTRACT]
                                            │
                                            ▼
                                 [STAGE 2: TRANSFORM]
                         (clean_numeric, text normalization)
                                            │
                                            ▼
                                  [STAGE 3: VALIDATE]
                                (Data Quality Firewall)
                                            │
                     ┌──────────────────────┴──────────────────────┐
                     ▼                                             ▼
             [VALID RECORDS]                               [REJECTED RECORDS]
                     │                                     (Quarantine / DLQ)
                     ▼
             [STAGE 4: LOAD]
       (Intra- & Inter-Batch Dedup,
        ACID Transaction Commit)
                     │
                     ▼
┌───────────────────────────────────────────┴────────────────────────────────────────────┐
│                               ANALYTICAL STORAGE WAREHOUSE                             │
│                                    (earbuds_staging)                                   │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Topic 1: Code Versioning & Git Branching Strategies for Data Teams

### 2.1 The Need for DataOps Branching
In traditional Git workflows, developers branch, push code, and merge into production. In Data Engineering, doing so without environment isolation causes:
- **Data Poisoning**: Unverified transformation bugs altering production tables.
- **Downtime**: Unchecked schema migrations breaking downstream analytical dashboards.
- **Race Conditions**: Parallel batch ingestions colliding during uncoordinated schema changes.

### 2.2 Implemented Branching Hierarchy
To solve this, we designed and implemented a **DataOps Branching Hierarchy** documented in [`docs/GIT_BRANCHING_STRATEGY.md`](./GIT_BRANCHING_STRATEGY.md):

1. **`main` (Production)**:
   - Contains strictly production-ready, release-tagged pipeline code.
   - Connected directly to the live PostgreSQL warehouse (`earbuds_staging`) and production Airflow instances.
   - Direct pushes are blocked; merges require signed PR approvals and automated CI pass.

2. **`staging` (Pre-Production)**:
   - Connects to an isolated staging database replica.
   - Runs against sanitized production-like data batches to verify ingestion latency, throughput, and schema parity.

3. **`develop` (Integration)**:
   - Primary branch for integrating feature and pipeline modifications.
   - Receives continuous integration builds on every commit.

4. **Dedicated Topic Branches**:
   - `feature/etl-*`: New data cleaning, transformation, or connector algorithms.
   - `schema/*`: Schema definition changes (DDL migrations) with mandatory backward-compatibility checks.
   - `dag/*`: Airflow DAG DAGBag orchestration definitions.
   - `hotfix/*`: Emergency patches for upstream data anomalies, merged to `main` and back-ported to `develop`.

---

## 3. Topic 2: Pipeline as Code (PaC)

### 3.1 Managing Infrastructure Programmatically
Rather than manually creating databases, message queues, and orchestrators, our infrastructure is declared as code:
- **Containerized Pipeline (`Dockerfile`)**: Builds an isolated Python execution environment containing all required system libraries (`libpq-dev`), Python dependencies (`requirements.txt`), and developer testing tooling (`requirements-dev.txt`).
- **Service Orchestration (`docker-compose.yml`, `airflow-compose.yml`)**:
  - Apache Kafka 4.0 running in modern, ZooKeeper-less KRaft mode on port `9092`.
  - Apache Airflow 3.0 workflow orchestrator on port `8080`, configured with volume mounts (`./dags`, `./etl`, `./logs`) and the shared `etl-network` bridge.
- **Unified Task Runner (`Makefile`)**:
  - Provides declarative targets (`make setup-env`, `make test`, `make lint`, `make ci-local`, `make up`, `make down`).

### 3.2 Managing Configuration Programmatically (12-Factor App)
All connection strings, ports, credentials, and tuning parameters are strictly decoupled from source code:
- **`etl/config.py`**: Centralized configuration module using typed dataclasses (`DatabaseConfig`, `KafkaConfig`, `AirflowConfig`, `PipelineSettings`).
- **`.env.example`**: Version-controlled template documenting all environment variables with default local fallbacks.
- **`.gitignore`**: Guarantees zero credentials, secrets, or local environment overrides (`.env`, `simple_auth_manager_passwords.json`) leak into version control.

---

## 4. Topic 3: Continuous Integration (CI/CD) & Automated Test Pyramid

Data pipeline testing requires a multi-layered verification strategy:

```
                  ┌──────────────────────┐
                  │ Airflow DAG Integrity│
                  │   & Syntax Tests     │
                  └──────────┬───────────┘
                             │
                  ┌──────────▼───────────┐
                  │   Database Mock &    │
                  │  Transaction Tests   │
                  └──────────┬───────────┘
                             │
                  ┌──────────▼───────────┐
                  │    Unit Tests for    │
                  │ Transforms & Quality │
                  └──────────────────────┘
```

### 4.1 Unit Testing Transformation Logic (`tests/test_transform.py`)
Transformation algorithms must be pure, deterministic functions. Our test suite validates:
- **Currency & Symbol Stripping**: `"₹1,499"`, `"$49.99"` $\rightarrow$ `1499.0`, `49.99`.
- **Formatting Noise**: Comma removal (`"1,234,567.89"` $\rightarrow$ `1234567.89`), percentage removal (`"50%"` $\rightarrow$ `50.0`).
- **Null Entropy**: Textual representations (`"NaN"`, `"none"`, `"null"`, `"N/A"`, `"-"`, `""`, `None`) reliably map to `0.0`.
- **Negative & Extreme Values**: Parsing negative prices, while discarding mathematical infinities and garbage strings.
- **Immutability Check**: Validates that `transform_data(df)` never mutates the input DataFrame in place.

### 4.2 Unit Testing Quality Gates (`tests/test_validator.py`)
Enforces the **Quarantine / DLQ pattern**:
- Evaluates missing/whitespace-only titles, negative prices, negative MRPs, rating bounds $[0.0, 5.0]$, and discount bounds $[0.0, 100.0]$.
- **Conservation of Records Test**: Asserts that $\text{len}(\text{valid}) + \text{len}(\text{rejected}) = \text{len}(\text{input})$, ensuring no records vanish silently.

### 4.3 Mock Testing for Data Pipelines (`tests/test_extract_mock.py`, `tests/test_loader_mock.py`, `tests/test_pipeline_mock.py`)
Why Mocks are Mandatory in Data Engineering:
1. **Zero Database Dependency**: Automated CI runners in GitHub Actions can execute tests without running a multi-gigabyte live PostgreSQL cluster.
2. **Deterministic Edge Case Simulation**: Mocks simulate database connection failures, disk-full conditions, and duplicate collision states on demand.
3. **Verified Idempotency**:
   - In-batch deduplication: Drops duplicate product URLs within the same batch.
   - Inter-batch deduplication: Filters out records already cataloged in `earbuds_staging`.
   - Transaction Rollback: Verifies `conn.rollback()` is invoked when an insert error occurs, ensuring atomicity.

### 4.4 Airflow DAG Integrity Testing (`tests/test_dag_integrity.py`)
Airflow DAGs are written in Python; an unhandled exception or missing import crashes the entire scheduler.
Our DAG integrity tests verify:
- Python Abstract Syntax Tree (AST) parsing without syntax errors.
- Mandatory task definitions (`extract`, `transform`, `validate`, `load`).
- Correct stage sequencing (`extract >> transform >> validate >> load`).

---

## 5. Topic 4: Automated CI Runners (GitHub Actions & Local CLI Runner)

### 5.1 GitHub Actions Workflow (`.github/workflows/ci.yml`)
Triggers automatically on every `push` and `pull_request` targeting `main`, `staging`, or `develop`:
- **Job 1: Linting (`flake8`)**: Enforces syntax standards, detects unused imports and PEP 8 compliance.
- **Job 2: Unit Tests**: Executes transformation and validation unit tests.
- **Job 3: Mock & Integration Tests**: Runs database mocks and DAG integrity tests.
- **Job 4: Test Coverage**: Generates a unified coverage report and verifies the threshold $\ge 80\%$.

### 5.2 Local CI Runner (`scripts/run_local_ci.sh`)
Provides data engineers with instant local feedback before committing code:
```bash
bash scripts/run_local_ci.sh
# or via Makefile
make ci-local
```
Outputs colored, step-by-step progress reports and returns standard Unix exit codes (`0` for success, `1` for failures) for integration into pre-commit git hooks.

---

## 6. Summary of Deliverables

| Area | Component | Path | Description |
| :--- | :--- | :--- | :--- |
| **Version Control** | Git Repository | `.git/` | Initialized repo with `main`, `develop`, and `staging` branches |
| **Version Control** | DataOps .gitignore | [`.gitignore`](../.gitignore) | Excludes secrets, logs, pkl, virtualenvs, and temporary data |
| **Version Control** | Strategy Guide | [`docs/GIT_BRANCHING_STRATEGY.md`](./GIT_BRANCHING_STRATEGY.md) | Branching guidelines tailored for data engineering |
| **Pipeline as Code**| Configuration | [`etl/config.py`](../etl/config.py) | Centralized, typed programmatic configuration management |
| **Pipeline as Code**| Config Template| [`.env.example`](../.env.example) | Environment variable specification template |
| **Pipeline as Code**| Container Spec | [`Dockerfile`](../Dockerfile) | Containerized ETL execution and test environment |
| **Pipeline as Code**| Compose Mounts | [`airflow-compose.yml`](../airflow-compose.yml) | Relative, portable volume mounts and parameterized secrets |
| **Pipeline as Code**| Task Runner | [`Makefile`](../Makefile) | Declarative automation interface |
| **Pipeline as Code**| Architecture Doc| [`docs/PIPELINE_AS_CODE.md`](./PIPELINE_AS_CODE.md) | Architectural documentation of PaC principles |
| **Unit Testing**    | Transform Tests| [`tests/test_transform.py`](../tests/test_transform.py) | 13 test cases for numeric/text cleaning |
| **Unit Testing**    | Validator Tests| [`tests/test_validator.py`](../tests/test_validator.py) | 8 test cases for quality gate business rules |
| **Mock Testing**    | Extract Mocks  | [`tests/test_extract_mock.py`](../tests/test_extract_mock.py) | SQL parameter binding & connection safety |
| **Mock Testing**    | Loader Mocks   | [`tests/test_loader_mock.py`](../tests/test_loader_mock.py) | In-batch & cross-batch dedup, ACID rollback |
| **Mock Testing**    | Pipeline Mocks | [`tests/test_pipeline_mock.py`](../tests/test_pipeline_mock.py) | End-to-end stage orchestration flow |
| **DAG Integrity**   | Airflow DAG    | [`tests/test_dag_integrity.py`](../tests/test_dag_integrity.py) | AST parsing & task dependency graph checks |
| **CI Automation**   | GitHub Actions | [`.github/workflows/ci.yml`](../.github/workflows/ci.yml) | Multi-stage cloud CI pipeline |
| **CI Automation**   | Local CI Runner| [`scripts/run_local_ci.sh`](../scripts/run_local_ci.sh) | Executable bash runner with colored status output |

