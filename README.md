# Clickstream_Real_Time_processing
Designed and implemented a real-time ad clickstream pipeline processing 50K+ events/sec using Kafka and Spark Structured Streaming, with production-style Bronze ingestion, late events, and duplicate handling.Overview

This project implements an end-to-end real-time clickstream data platform using Kafka, Spark Structured Streaming, and Delta Lake.
It simulates high-volume user interaction events (impressions, clicks, conversions), processes them in near-real time, and stores them in a Bronze → Silver → Gold medallion architecture on Amazon S3.

The goal is to demonstrate production-grade streaming data engineering patterns: schema enforcement, deduplication, watermarking, fault tolerance, and incremental analytics.

### High Level Flow 
~~~
Event Producers
   ↓
Kafka Topics (impressions, clicks, conversions)
   ↓
Spark Structured Streaming
   ↓
Bronze (raw events, schema-on-read)
   ↓
Silver (cleaned, deduplicated, enriched)
   ↓
Gold (aggregations & attribution)
   ↓
Analytics / BI / ML
~~~

### 🐳 Local Development Setup

#### Dockerized stack:
  - Kafka (KRaft mode)
  - Spark Master + Worker
  - Schema Registry
  - Redpanda Console





