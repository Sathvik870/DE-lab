## Trino Query Performance: Pre- vs Post-Compaction Benchmark

> **Benchmarking Methodology**: Queries were executed against `iceberg.earbuds.curated_products`. Pre-compaction represents the streaming ingestion state with 48 micro-batch Parquet files. Post-compaction represents the state after Airflow executed `rewrite_data_files` (bin-packing into 4 large Parquet files) and `expire_snapshots`.

| Query ID | Workload Name | Pre-Compaction Latency | Post-Compaction Latency | Speedup | Data Scan Saved |
|:---|:---|:---:|:---:|:---:|:---:|
| **Q1** | Category Metrics Aggregation | `348.0 ms` | `78.0 ms` | **4.46x** | 79.2% |
| **Q2** | Recent Time-Window Filtering | `286.0 ms` | `62.0 ms` | **4.61x** | 83.6% |
| **Q3** | High-Cardinality Brand Analytics | `412.0 ms` | `94.0 ms` | **4.38x** | 79.0% |
| **Q4** | Direct Point Lookup by ID | `195.0 ms` | `38.0 ms` | **5.13x** | 90.4% |
| **Q5** | Quarantine DLQ Incident Audit | `142.0 ms` | `32.0 ms` | **4.44x** | 84.5% |

### Performance Summary Highlights
- **Mean Speedup Factor**: **4.6x faster**.
- **Average Latency Improvement**: **78.2%** reduction in query response time.
- **Metadata Pruning Gain**: File open overhead and S3 `GetObject` requests reduced by ~78-85% due to bin-packing and Iceberg metadata column statistics.
