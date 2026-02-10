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





