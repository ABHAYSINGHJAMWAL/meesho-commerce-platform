# System Architecture

## Overview

A modern data engineering platform implementing
real-time streaming, batch processing, cloud storage,
and analytical serving in a single integrated system.

## Architecture Diagram
EVENT GENERATION
MeeshoEventGenerator
(realistic Indian e-commerce data)
↓
STREAMING INGESTION
Apache Kafka (3 topics, Docker)
meesho-orders
meesho-inventory
meesho-fraud-signals
↓ (4 independent consumer groups)
┌────┼────┬────────────┐
↓    ↓    ↓            ↓
Inventory Fraud  Live    S3 Archiver
Validator Detector Metrics (boto3)
(HashMap)(Sliding (Heap +     ↓
Window) HashMap) MinIO S3
                Bronze Layer
                     (Parquet, Hive
partitioned)
          ↓
BATCH PROCESSING (PySpark)
daily_order_processing.py
→ Daily revenue summary
→ Cohort retention analysis
→ Seller performance scoring
→ Fraud pattern detection
→ Category trend analysis
↓
STORAGE
BigQuery (meesho_raw dataset)
mart_daily_revenue
mart_cohort_retention
mart_seller_performance
mart_fraud_patterns
mart_category_trends
↓
SERVING
Metabase dashboardsORCHESTRATION (Airflow via Docker/Astro CLI)
meesho_commerce_pipeline DAG
check_kafka_health
check_data_freshness
run_pyspark_batch
validate_batch_output
check_fraud_threshold
send_pipeline_summary

## Data Flow
Raw Events → Kafka → 4 Consumers → MinIO S3 (Bronze)
                              ↓
 PySpark reads Bronze
                           ↓
                             Transforms to Gold
                          ↓
                  BigQuery Writer loads
                                        ↓
       Metabase visualizes

## Technology Stack

| Layer | Technology | Version |
|---|---|---|
| Event Generation | Python | 3.12 |
| Message Bus | Apache Kafka | 7.4.0 |
| Stream Processing | Python consumers | 3.12 |
| Batch Processing | PySpark | 3.5.0 |
| Orchestration | Apache Airflow | via Astro CLI |
| Data Lake | MinIO (S3-compatible) | latest |
| Data Warehouse | Google BigQuery | cloud |
| Containerization | Docker + Compose | latest |
| Version Control | Git + GitHub | - |

## DSA Implementations

| Component | Algorithm | Complexity | LeetCode |
|---|---|---|---|
| Inventory Validator | HashMap | O(1) lookup | Two Sum |
| Fraud Detector | Sliding Window | O(1) amortized | Sliding Window Max |
| Live Metrics | Min-Heap | O(log k) | Top K Elements |
| Airflow DAG | Topological Sort | O(V+E) | Course Schedule |
| Spark Merge | K-Way Merge | O(n log k) | Merge K Sorted Lists |
| SLA Monitor | Binary Search | O(log n) | First Bad Version 