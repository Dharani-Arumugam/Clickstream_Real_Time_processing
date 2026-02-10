# Clickstream_Real_Time_processing
Designed and implemented a real-time ad clickstream pipeline processing 50K+ events/sec using Kafka and Spark Structured Streaming, with production-style Bronze ingestion, late events, and duplicate handling.Overview

This project implements an end-to-end real-time clickstream data platform using Kafka, Spark Structured Streaming, and Delta Lake.
It simulates high-volume user interaction events (impressions, clicks, conversions), processes them in near-real time, and stores them in a Bronze → Silver → Gold medallion architecture on Amazon S3.

The goal is to demonstrate production-grade streaming data engineering patterns: schema enforcement, deduplication, watermarking, fault tolerance, and incremental analytics.

### Project Structure
~~~
.
├── jobs/
│   ├── 01_bronze_kafka_to_parquet.py
│   ├── 02_silver_parquet_to_delta.py
│   └── 03_gold_attribution.py
|   └── config.py
├── docker-compose.yml
|
├── Kafka_Producer/
│   ├── clickstream_producer.py
|   ├── producer_config.py
│   └── event_tracker.py
└── README.md
~~~
## Architecture Diagram

<img width="1536" height="1024" alt="ArchitectureDiagram" src="https://github.com/user-attachments/assets/bddb6161-7872-44cf-952d-36321f256893" />

### 🐳 Local Development Setup

#### Dockerized stack:
  - Kafka (KRaft mode)
  - Spark Master + Worker
  - Schema Registry
  - Redpanda Console

### 🚀 What This Project Demonstrates

  - Real-time streaming pipelines
  - Production-ready Spark + Kafka patterns
  - Delta Lake on S3
  - Medallion architecture best practices
  - Exactly-once, fault-tolerant streaming design

## 🗂️ Data Layers (Medallion Architecture)
#### 🟤 Bronze Layer (Raw)
  Source: Kafka
  Format: Parquet
  Storage: s3a://clickstream-event-data/bronze/
  Characteristics:
        - Raw, append-only events
        - Minimal transformation
        - Partitioned by kafka_topic and ingest_date
#### ⚪ Silver Layer (Refined – Delta Lake)
  Source: Bronze
  Format: Delta
  Storage: delta/silver/impressions
           delta/silver/clicks
           delta/silver/conversions
  Transformations:
      - Schema enforcement
      - Deduplication using event_id
      - Event-time extraction & normalization
      - Watermarking (event_ts)
      - Partitioning by event_date
  Guarantees:
      Exactly-once processing
      Fault tolerance via checkpoints
      
#### 🟡 Gold Layer (Analytics-Ready)
  Source: Silver
  Format: Delta
  Examples:
      - Campaign-level attribution
      - Click-through rates
      - Conversion funnels
      - Revenue aggregation
      - Optimized for BI and ML workloads



