# Data Lineage & Governance: Real-Time Lakehouse Platform

## 1. Executive Summary & Purpose

In enterprise data platforms, **Data Lineage** tracks the lifecycle of data from its origin through all intermediate transformations, storage layers, and query engines to its terminal consumption in business intelligence dashboards and APIs.

This document formalizes the data lineage, metadata cataloging, compliance guarantees, and governance policies for the **Earbuds Real-Time Lakehouse Architecture** (22MDCEL10 – Phase II).

---

## 2. End-to-End System Lineage Diagram

```mermaid
flowchart TD
    subgraph S1["1. Event Source & Ingestion"]
        P1["Kafka Event Producer<br/>(scripts/kafka_stream_producer.py)"] -->|JSON Payloads @ 50-100 msg/s| K1["Apache Kafka 4.0 Broker<br/>Topic: earbuds_stream"]
    end

    subgraph S2["2. Real-Time Stream Processing"]
        K1 -->|Micro-Batch Consumer (readStream)| SP1["PySpark Structured Streaming Engine<br/>(spark_streaming/stream_processor.py)"]
        SP1 -->|Strict StructType Schema Validation| V1{"Schema & Value<br/>Validator"}
        V1 -->|Valid Events| TR1["Feature Engineering & Enrichment<br/>(price_tier, discount_pct, parse_ts)"]
        V1 -->|Corrupted / Anomalous| DLQ["Dead-Letter Queue (DLQ) Quarantine Router"]
    end

    subgraph S3["3. Open Lakehouse Storage Layer"]
        TR1 -->|ACID Write (writeTo.append)| IC1["Apache Iceberg Table<br/>lakehouse.earbuds.curated_products<br/>Partitioned by: category"]
        DLQ -->|ACID Quarantine Append| IC2["Apache Iceberg DLQ Table<br/>lakehouse.earbuds.quarantine"]
        REST["Tabular Iceberg REST Catalog<br/>(port 8181)"] --- IC1
        REST --- IC2
        S3B["MinIO S3 Object Store<br/>Bucket: s3a://lakehouse/"] --- IC1
    end

    subgraph S4["4. Orchestration & Maintenance"]
        AF["Apache Airflow 3.0 DAG<br/>iceberg_table_maintenance"] -->|rewrite_data_files| IC1
        AF -->|expire_snapshots| IC1
        AF -->|remove_orphan_files| IC1
        AF -->|Failure Alerting| AL["Slack & Email Alert Manager<br/>(etl/alerts.py)"]
    end

    subgraph S5["5. Distributed Query & Serving Engine"]
        IC1 -->|Iceberg Connector / Split Pruning| TR["Trino Distributed SQL Engine<br/>(Coordinator: port 8085)"]
        IC2 -->|Iceberg Connector| TR
    end

    subgraph S6["6. Presentation & Downstream Consumption"]
        TR -->|DBAPI Query Client| API["FastAPI Serving Layer<br/>(analytics/main.py: /trino/*)"]
        API -->|JSON REST Telemetry| DB["React Lakehouse Dashboard<br/>(dashboard/src/LakehouseView.jsx)"]
        TR -->|Distributed SQL| BI["Enterprise BI Tools<br/>(Apache Superset / Metabase / Grafana)"]
    end
```

---

## 3. Column-Level Data Lineage

The table below traces each field from raw Kafka JSON ingestion through PySpark transformations to the final Iceberg storage columns and downstream Trino analytical metrics.

| Source JSON Field | Ingestion Type | Transformation / Enrichment Logic | Curated Iceberg Column | Iceberg Type | Downstream Metric / KPI (Trino / BI) |
|:---|:---|:---|:---|:---|:---|
| `product_id` | String | Trim whitespace, regex validation `^prod_.*$` | `product_id` | `STRING` | Point lookup, distinct catalog count (`COUNT(DISTINCT product_id)`) |
| `title` | String | Null check, strip HTML/special entities | `title` | `STRING` | Product listing card, search indexing |
| `brand` | String | Upper-casing, alias mapping (e.g. `Sony` -> `SONY`) | `brand` | `STRING` | Brand market share (`GROUP BY brand`) |
| `category` | String | Normalization to standard taxonomy (`TWS`, `ANC`, etc.) | `category` | `STRING` | **Partition Key**, category distribution breakdown |
| `selling_price` | Float / Number | Cast to Double, check $> 0.0$ | `selling_price` | `DOUBLE` | Average listed price (`AVG(selling_price)`) |
| `original_price` | Float / Number | Cast to Double, check $\ge \text{selling\_price}$ | `original_price` | `DOUBLE` | MRP baseline tracking |
| *(Derived)* | *N/A* | `((original_price - selling_price) / original_price) * 100.0` | `discount_pct` | `DOUBLE` | Average discount rate (`AVG(discount_pct)`) |
| `discount_price` | Float / Number | Validated currency delta (`original_price - selling_price`) | `discount_price` | `DOUBLE` | Total discount amount delivered |
| *(Derived)* | *N/A* | Price brackets: `Budget` (<3000), `Mid-Range` (3000-8000), `Premium` (>8000) | `price_tier` | `STRING` | Tier segmentation analysis |
| `rating` | Float | Check bounds $0.0 \le \text{rating} \le 5.0$ | `rating` | `DOUBLE` | Quality score (`AVG(rating)`), customer sentiment |
| `rating_count` | Integer | Cast to Long, default 0 if null | `rating_count` | `LONG` | Social proof weighting (`SUM(rating_count)`) |
| `timestamp` | String / ISO | Cast to `TIMESTAMP` via `to_timestamp()` | `event_timestamp` | `TIMESTAMP` | Time-window streaming aggregations, SLA latency |
| *(Derived)* | *N/A* | `current_timestamp()` at Spark ingestion | `ingestion_timestamp` | `TIMESTAMP` | Ingestion latency tracking (`ingestion_ts - event_ts`) |
| *(Derived)* | *N/A* | Spark streaming micro-batch ID | `batch_id` | `STRING` | Auditability & checkpoint replay verification |

---

## 4. Dead-Letter Queue (DLQ) Quarantine Lineage

Events failing schema validation or domain constraints are routed to the Quarantine Lakehouse Table:

| Quarantine Field | Data Type | Description |
|:---|:---|:---|
| `raw_payload` | `STRING` | Complete unparsed or corrupt JSON string for forensic inspection. |
| `error_reason` | `STRING` | Specific validation failure (e.g. `Negative price`, `Missing title`, `JSON parse failure`). |
| `error_code` | `STRING` | Machine-readable taxonomy code (`SCHEMA_CORRUPT`, `VALUE_RANGE_VIOLATION`). |
| `quarantine_timestamp` | `TIMESTAMP` | UTC timestamp when event was rejected by PySpark validator. |
| `source_topic` | `STRING` | Originating Kafka topic (`earbuds_stream`). |

---

## 5. Data Governance & Regulatory Compliance

### 5.1 ACID Guarantees & Snapshot Auditability
1. **Snapshot Isolation**: Apache Iceberg uses optimistic concurrency control with atomic manifest tree commits. Concurrent reads from Trino never observe partial micro-batch writes.
2. **Time-Travel Auditing**: Every write generates an immutable snapshot ID (`snapshot_id`, `parent_id`, `summary`). Regulators and audit teams can reproduce table state at any historical moment:
   ```sql
   SELECT * FROM iceberg.earbuds.curated_products FOR VERSION AS OF 4829104829018471029;
   ```
3. **Rollback Capability**: Accidental data corruption or bad streaming commits can be rolled back instantaneously without data rewrites via Iceberg snapshot rollback.

### 5.2 Schema Evolution Policies
- **Additive Evolution**: New optional fields added to upstream Kafka JSON are automatically merged via Iceberg schema evolution without table recreation.
- **Strict Invariant Enforcement**: Renaming or dropping columns requires an explicit migration procedure to ensure downstream Trino views and BI dashboards remain backwards-compatible.

### 5.3 Data Quality & Quarantine SLA
- **Quality SLA Threshold**: $> 98\%$ event validity rate.
- **DLQ Alerting**: If the DLQ ingestion rate exceeds $5\%$ of total stream throughput over a 5-minute sliding window, the `AlertManager` fires high-priority alerts to Slack `#lakehouse-alerts`.
- **DLQ Reprocessing**: Once upstream producers fix schema deviations, quarantine events can be replayed through PySpark using `scripts/reprocess_quarantine.py`.

