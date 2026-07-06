# 🛍️ Meesho-Scale Real-Time Commerce Platform

> A production-grade data engineering platform solving inventory overselling, stale business intelligence, and invisible seller fraud in real time.

![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=for-the-badge\&logo=apachekafka\&logoColor=white)
![Apache Spark](https://img.shields.io/badge/PySpark%203.5-E25A1C?style=for-the-badge\&logo=apachespark\&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge\&logo=apacheairflow\&logoColor=white)
![Google BigQuery](https://img.shields.io/badge/BigQuery-669DF6?style=for-the-badge\&logo=googlebigquery\&logoColor=white)
![MinIO](https://img.shields.io/badge/MinIO%20S3-C72E49?style=for-the-badge\&logo=minio\&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge\&logo=docker\&logoColor=white)
![Python](https://img.shields.io/badge/Python%203.12-3776AB?style=for-the-badge\&logo=python\&logoColor=white)
![Tests](https://img.shields.io/badge/Unit%20Tests-40-success?style=for-the-badge)

---

## 🎯 The Problem

### 🔴 Inventory Overselling

500 customers buy the last 100 units simultaneously.

All 500 orders get confirmed.

400 cancellations destroy customer trust.

### 🟠 Stale Business Intelligence

The CEO asks which categories are trending at 2 PM.

The available answer is 14 hours old from the previous night's batch pipeline.

Business decisions are made using stale data.

### 🔵 Invisible Seller Fraud

A seller creates 500 fake orders in 60 seconds.

The seller reaches the #1 ranking and remains there for 12 hours before traditional batch fraud detection catches the activity.

---

## 🏗️ Architecture

```text
                         REAL-TIME STREAMING LAYER

┌─────────────────┐
│ Event Generator │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Apache Kafka   │
│    3 Topics     │
└────────┬────────┘
         │
         ├──────────────► Inventory Validator
         │                 HashMap O(1)
         │
         ├──────────────► Fraud Detector
         │                 Sliding Window
         │
         ├──────────────► Live Metrics
         │                 Min-Heap
         │
         └──────────────► S3 Archiver
                           │
                           ▼
                       MinIO S3
                       Bronze Zone


                           BATCH LAYER

┌──────────────┐
│ MinIO Bronze │
└──────┬───────┘
       │
       ▼
┌─────────────────┐
│  PySpark Batch  │
│  5 Transforms   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    BigQuery     │
│  5 Mart Tables  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    Metabase     │
└─────────────────┘
```

### 🔄 Orchestration

Apache Airflow orchestrates every pipeline layer:

```text
Health Checks
      ↓
Batch Processing
      ↓
Data Validation
      ↓
Fraud Threshold Check
      ↓
Pipeline Summary Alert
```

---

## 📊 Key Metrics

| Metric                  |       Value |
| ----------------------- | ----------: |
| Events processed        | 500K / hour |
| Kafka consumer groups   |           4 |
| PySpark transformations |           5 |
| Airflow tasks           |           8 |
| BigQuery mart tables    |           5 |
| Unit tests              |          40 |

---

## 🛠️ Tech Stack

| Layer            | Technology      | Why This Technology                                                                       |
| ---------------- | --------------- | ----------------------------------------------------------------------------------------- |
| Streaming        | Apache Kafka    | Multi-consumer decoupling allows 4 independent consumer groups to process the same events |
| Batch Processing | PySpark 3.5     | Distributed processing that scales from a laptop to a multi-node cluster                  |
| Orchestration    | Apache Airflow  | DAG-based workflow orchestration using Docker and Astro CLI                               |
| Data Lake        | MinIO S3        | S3-compatible object storage using boto3                                                  |
| Data Warehouse   | Google BigQuery | Partitioned and clustered analytical warehouse                                            |
| Containers       | Docker Compose  | Reproducible Kafka and MinIO infrastructure                                               |
| Language         | Python 3.12     | Streaming consumers, data processing, testing and utilities                               |

---

## ⚡ DSA Algorithms Used in Production

This project connects common DSA concepts with real data engineering problems.

| Algorithm        | Production Use                                | Related LeetCode Pattern |
| ---------------- | --------------------------------------------- | ------------------------ |
| HashMap          | O(1) inventory stock lookup                   | Two Sum                  |
| Sliding Window   | Seller order velocity fraud detection         | Sliding Window Maximum   |
| Min-Heap         | Real-time top-10 seller tracking              | Top K Elements           |
| Topological Sort | Airflow DAG task ordering and cycle detection | Course Schedule          |
| K-Way Merge      | Merge sorted Spark partition outputs          | Merge K Sorted Lists     |
| Binary Search    | SLA breach detection on sorted metrics        | First Bad Version        |

---

## 🔍 Real-Time Streaming Consumers

Kafka events are processed by four independent consumer groups.

### 1. Inventory Validator

Uses a HashMap for O(1) stock lookup.

```text
Order Event
     ↓
Lookup Product Stock
     ↓
stock[product_id]
     ↓
Validate Quantity
     ↓
CONFIRMED / REJECTED
```

This prevents inventory overselling during concurrent purchase spikes.

---

### 2. Fraud Detector

Uses a sliding window to track seller order velocity.

```text
Seller Orders
     ↓
Maintain 60-Second Window
     ↓
Remove Expired Events
     ↓
Count Active Orders
     ↓
Orders > 30 / minute?
     ↓
Flag Seller
```

The detector identifies suspicious sellers in real time instead of waiting for a nightly batch pipeline.

---

### 3. Live Metrics

Uses a Min-Heap to maintain the top sellers.

```text
Incoming Order
      ↓
Update Seller Revenue
      ↓
Push Into Min-Heap
      ↓
Heap Size > 10?
      ↓
Remove Minimum
      ↓
Top 10 Sellers
```

Each update runs in:

```text
O(log k)
```

where:

```text
k = 10
```

---

### 4. S3 Archiver

Kafka events are archived into the Bronze data lake.

```text
Kafka
   ↓
S3 Archiver
   ↓
Parquet
   ↓
Hive Partitioning
   ↓
MinIO Bronze Zone
```

Example partition structure:

```text
bronze/
└── orders/
    └── year=2026/
        └── month=06/
            └── day=27/
                └── orders.parquet
```

Hive-style partition paths allow compatible query engines to identify partition columns and prune irrelevant partitions.

---

## 🔥 PySpark Batch Processing

The PySpark batch layer performs five transformations.

```text
Bronze Data
     ↓
Read Parquet
     ↓
Clean Records
     ↓
Transform Orders
     ↓
Aggregate Metrics
     ↓
Generate Mart Tables
     ↓
BigQuery
```

PySpark is used instead of pandas because processing can be distributed across multiple executors when data exceeds single-machine memory.

The same transformation logic can move from local development to a Spark cluster with infrastructure and configuration changes.

---

## 🗄️ BigQuery Data Marts

The pipeline generates five analytical mart tables.

```text
Processed Data
      ↓
BigQuery
      │
      ├── Order Metrics
      ├── Seller Metrics
      ├── Product Metrics
      ├── Category Metrics
      └── Fraud Metrics
```

Partitioning and clustering reduce unnecessary data scans for analytical queries.

---

## 📂 Project Structure

```text
meesho-commerce-platform/
│
├── ingestion/
│   ├── event_generator.py
│   ├── kafka_producer.py
│   └── s3_archiver.py
│
├── streaming/
│   ├── inventory_validator.py
│   ├── fraud_detector.py
│   └── live_metrics.py
│
├── batch/
│   ├── spark_jobs/
│   │   └── daily_order_processing.py
│   │
│   ├── utils/
│   │   ├── algorithms.py
│   │   └── data_quality.py
│   │
│   └── bigquery_writer.py
│
├── dags/
│   └── meesho_commerce_pipeline.py
│
├── docs/
│
├── tests/
│   └── test_algorithms.py
│
├── docker-compose.yml
│
└── README.md
```

---

## 🚀 Running the Project

### 1. Start Infrastructure

```bash
docker-compose up -d
```

This starts:

```text
Apache Kafka
MinIO S3
```

---

### 2. Run the Event Producer

```bash
python ingestion/kafka_producer.py
```

The producer generates realistic Indian e-commerce events and publishes them to three Kafka topics.

---

### 3. Run Streaming Consumers

Open four terminals.

#### Terminal 1 — Inventory Validator

```bash
python streaming/inventory_validator.py
```

#### Terminal 2 — Fraud Detector

```bash
python streaming/fraud_detector.py
```

#### Terminal 3 — Live Metrics

```bash
python streaming/live_metrics.py
```

#### Terminal 4 — S3 Archiver

```bash
python ingestion/s3_archiver.py
```

---

### 4. Run the PySpark Batch Job

```bash
python batch/spark_jobs/daily_order_processing.py
```

---

### 5. Load Data Into BigQuery

```bash
python batch/bigquery_writer.py
```

This loads five mart tables into BigQuery.

---

### 6. Run Unit Tests

```bash
python -m pytest tests/test_algorithms.py -v
```

Expected test suite:

```text
40 passed
```

---

## 🧠 Key Engineering Decisions

### 🟠 Kafka Over RabbitMQ or Direct API Calls

Four independent consumer groups read the same events without interfering with each other.

```text
                    Kafka Event
                         │
          ┌──────────────┼──────────────┐
          │              │              │
          ▼              ▼              ▼
     Inventory         Fraud        Live Metrics
```

If fraud detection becomes slow during a traffic spike, inventory validation continues independently.

Kafka retention also enables event replay.

---

### 🔥 PySpark Over pandas

pandas processes data on a single machine.

```text
pandas
   ↓
Single Machine Memory
```

PySpark distributes processing.

```text
PySpark
   ↓
Executor 1
Executor 2
Executor 3
Executor N
```

When data grows beyond one machine's memory, Spark can distribute partitions across executors.

---

### 🪣 MinIO Over Local Files

MinIO provides an S3-compatible API.

The project uses boto3 to interact with object storage.

```text
Local Development
        ↓
      MinIO

Production
        ↓
      AWS S3
```

The storage endpoint can be changed through configuration without rewriting the core object-storage logic.

---

### ✅ Manual Kafka Commit Over Auto Commit

Auto commit can mark an event as consumed before business processing successfully finishes.

```text
Receive Event
      ↓
Auto Commit
      ↓
Process Event
      ↓
CRASH ❌
```

The offset may already be committed.

Manual commit changes the flow:

```text
Receive Event
      ↓
Process Event
      ↓
Success
      ↓
Commit Offset
```

Offsets are committed only after successful processing.

---

## 📈 Scaling Strategy

### 10× Scale

No major architecture change.

The same architecture can handle increased event volume by scaling the existing services and resources.

---

### 100× Scale

```text
Kafka Partitions
3 → 12

Local PySpark
      ↓
Databricks Cluster

BigQuery
      ↓
Partitioning + Clustering
```

The processing logic remains similar while compute and infrastructure scale horizontally.

---

### 1000× Scale

```text
Single Kafka Broker
        ↓
Multi-Broker Kafka
Replication Factor = 3

Python Consumers
        ↓
Spark Structured Streaming

Parquet Data Lake
        ↓
Delta Lake

Docker Airflow
        ↓
Kubernetes-Based Airflow
```

At this scale, the platform would require stronger distributed processing, fault tolerance and ACID guarantees.

---

## 🧪 Testing

The project contains 40 unit tests covering the core algorithm implementations.

```bash
python -m pytest tests/test_algorithms.py -v
```

Algorithms tested include:

* HashMap-based inventory lookup
* Sliding window fraud detection
* Min-Heap top-K tracking
* Topological sort
* K-Way merge
* Binary search

---

## 🎓 What This Project Demonstrates

* Real-time event-driven data architecture
* Apache Kafka producer and consumer design
* Independent Kafka consumer groups
* Manual Kafka offset management
* PySpark distributed transformations
* Data lake architecture using MinIO
* Hive-style partitioning
* Apache Airflow DAG orchestration
* Google BigQuery analytical marts
* Data quality validation
* Production use cases for DSA algorithms
* Docker-based local infrastructure
* Unit testing for data engineering utilities
* Data platform scaling strategies

---

## 👨‍💻 Author

**Abhay Singh Jamwal**

Data Engineering Portfolio Project
