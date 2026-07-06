# Scale Considerations

## Current State
Orders per day: ~2,100 (simulated 30-day dataset)
Kafka partitions: 3
PySpark: local[*] on single machine
BigQuery: sandbox tier
Storage: MinIO local Docker container
## At 10x Scale (20,000 orders/day)

No code changes needed. Same architecture handles this.

## At 100x Scale (210,000 orders/day)

**Kafka:** Increase partitions from 3 to 12.
Add 12 consumer instances per consumer group.
Kafka assigns partitions automatically — zero code change.

**PySpark:** Move from local to Databricks or EMR cluster.
Same PySpark code runs on 20-node cluster unchanged.
Increase spark.sql.shuffle.partitions from 8 to 200.

**BigQuery:** Enable partitioning on all mart tables.
Partition by order_date, cluster by category and city.
95% query cost reduction on date-filtered queries.

**MinIO → AWS S3:** Change one environment variable.
endpoint_url=None makes boto3 point to real AWS S3.
Zero code changes in archiver.

## At 1000x Scale (2.1 million orders/day)

**Kafka:** Multi-broker cluster with replication factor 3.
No data loss if one broker fails.
Consumer lag monitoring with alerting.

**Streaming:** Move from Python consumers to
Spark Streaming or Flink for distributed processing.
Python consumers cannot handle this throughput alone.

**Storage:** Migrate from Parquet CSV to Delta Lake.
ACID transactions on data lake.
Time travel for debugging pipeline issues.
Upsert capability for late-arriving corrections.

**Orchestration:** Kubernetes-based Airflow.
Multiple workers handling different DAG runs.
Auto-scaling based on queue depth.

## What Would Not Change

dbt models: same SQL, different data volume.
BigQuery schema: same mart tables, more rows.
Algorithm implementations: O(1) and O(log n)
operations scale independently of data volume.
Business logic: same transformations at any scale.