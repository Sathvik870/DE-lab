# Pipeline as Code (PaC): Managing Infrastructure & Configuration Programmatically

## 1. Introduction & Theoretical Foundations
In modern Data Engineering and DataOps, **Pipeline as Code (PaC)** is the architectural paradigm where data processing workflows, runtime environments, external integrations, and configuration settings are treated with the same rigor as production software code:
- Defined declaratively in version-controlled text files.
- Provisioned automatically without manual GUI-based interventions.
- Replicable across development, testing, staging, and production environments.
- Continuously tested, verified, and audited.

---

## 2. Infrastructure as Code Architecture
The infrastructure supporting the Earbuds Data Pipeline consists of three synchronized microservices managed declaratively:

```
┌─────────────────────────────────────────────────────────────┐
│                 Docker Network: etl-network                 │
│                                                             │
│   ┌────────────────┐   ┌────────────────┐   ┌───────────┐   │
│   │  Apache Kafka  │   │ Apache Airflow │   │PostgreSQL │   │
│   │  (Port: 9092)  │──▶│  (Port: 8080)  │──▶│(Port:5432)│   │
│   │  kraft-broker  │   │Local/Sequential│   │earbuds_db │   │
│   └────────────────┘   └────────────────┘   └───────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### Key Components:
1. **Containerized Execution (`Dockerfile`)**:
   - Encapsulates Python runtime, OS-level database dependencies (`libpq-dev`), and dependencies (`requirements.txt`, `requirements-dev.txt`).
   - Ensures deterministic execution across local developer workstations, staging runners, and CI/CD agents.
2. **Container Orchestration (`docker-compose.yml` & `airflow-compose.yml`)**:
   - `docker-compose.yml`: Defines the event streaming broker (Kafka 4.0 in modern KRaft mode, eliminating ZooKeeper complexity).
   - `airflow-compose.yml`: Defines the Airflow 3.0 workflow engine with declarative health-checks, volume mounts (`./dags`, `./etl`, `./logs`), and isolated network configuration.
3. **Reproducible Command Interface (`Makefile`)**:
   - Abstracts multi-step docker and testing commands into intuitive programmatic recipes (`make up`, `make down`, `make test`, `make ci-local`).

---

## 3. Configuration Management as Code
Hardcoding database connection strings, passwords, or broker addresses inside ETL scripts introduces severe security vulnerabilities and prevents environment portability.

We enforce programmatic configuration separation:
1. **The 12-Factor Config Principle**:
   - All environment-dependent parameters are externalized into environment variables.
   - `.env.example`: Provides a comprehensive, version-controlled template documenting all required environment variables and sensible local defaults.
   - `.env`: Machine-local secrets file, strictly ignored by `.gitignore`.
2. **Centralized Programmatic Module (`etl/config.py`)**:
   - Uses typed Python dataclasses (`DatabaseConfig`, `KafkaConfig`, `AirflowConfig`, `PipelineSettings`).
   - Provides safe fallback defaults for local execution while allowing container or cloud overrides via environment variables.

---

## 4. Orchestration as Code (Airflow DAG)
Data workflows are declared as directed acyclic graphs (DAGs) in pure Python (`airflow/dags/earbuds_etl_dag.py`):
- **Dynamic Execution**: Tasks accept runtime execution parameters (e.g. `batch_id`) passed via DAG run configuration.
- **Stage Decoupling**: Tasks (`extract >> transform >> validate >> load`) maintain single-responsibility isolation.
- **State Handoff**: Inter-task data exchange is managed via intermediate serialized staging artifacts (`/opt/airflow/logs/etl_tmp/`) with automated cleanup in the final load stage.

---

## 5. Security & Secret Protection
To comply with enterprise security and university grading benchmarks:
- Plaintext secrets (`.env`, `airflow/secrets/*.json`) are barred from repository commits.
- Passwords in Compose manifests are parameterized with `${VAR:-default}` pattern.
- Database credentials in connection pools are injected at runtime via environment variables.

