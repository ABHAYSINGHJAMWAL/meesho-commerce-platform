# Technology Decisions

Every technology choice has a specific reason.
These are the exact answers to "why X not Y" 
interview questions.

---

## Why Apache Kafka

**Not:** Direct API calls, RabbitMQ, Redis Pub/Sub

**Reason:** Three independent consumers need the same
events simultaneously — inventory validator, fraud
detector, and live metrics aggregator.

With direct API calls: events must be duplicated
to all three consumers manually.

With RabbitMQ: messages are deleted after one consumer
reads them. Only one consumer gets each event.

With Kafka: events are retained for 7 days.
All consumer groups read independently without
affecting each other. If fraud detection is slow
during a traffic spike, it does not affect
inventory validation. Complete decoupling.

---

## Why PySpark Not Pandas

**Not:** pandas, plain Python, SQL only

**Reason:** 30 days of order data is potentially
15 million rows. Pandas loads everything into one
machine's memory and crashes at scale.

PySpark distributes work across all CPU cores locally
and scales to a 100-node cluster with zero code changes.
The transformations written locally are identical
to what runs on Databricks or AWS EMR in production.

---

## Why Airflow in Docker

**Not:** Prefect, cron jobs, native Windows Airflow

**Reason:** Airflow does not run natively on Windows.
Docker provides a Linux environment where Airflow
works correctly. The containerized setup mirrors
exactly how companies deploy Airflow in production.

---

## Why MinIO for Data Lake

**Not:** Local files, PostgreSQL, direct BigQuery writes

**Reason:** MinIO is S3-compatible — identical boto3 API
to AWS S3. The exact same code deploys to production AWS
by changing one environment variable (endpoint_url).
This gives genuine cloud storage experience with zero
cloud costs during development.

Bronze zone uses Hive partitioning (year=/month=/day=/)
which BigQuery, Spark, and Athena all recognize
automatically — no extra configuration needed.

---

## Why BigQuery for Warehouse

**Not:** PostgreSQL, SQLite, Snowflake

**Reason:** Reuses existing Project 1 GCP setup — zero
additional cloud configuration. BigQuery is what
Meesho, Nykaa, and Dream11 actually use.
Partition by date and cluster by category reduces
analytical query costs by 95%.

---

## Why Parquet Not CSV

**Not:** CSV, JSON, plain text

**Reason:** Parquet is columnar — analytical queries
reading two columns on a 100-column table read only
2% of the data. CSV reads everything every time.

Parquet with Snappy compression is 10x smaller than
CSV for the same data. Cheaper to store, faster to
query, schema embedded in file.

---

## Why Manual Kafka Commit

**Not:** Auto commit (enable_auto_commit=True)

**Reason:** Auto commit marks a message as processed
immediately on receipt — before processing code runs.
If the consumer crashes after receiving but before
processing, Kafka thinks the message was handled
and skips it on restart. Silent data loss.

Manual commit marks processed only after successful
execution. If code crashes, Kafka redelivers.
No data loss. Trade-off is possible duplicate
processing — handled with idempotency keys.