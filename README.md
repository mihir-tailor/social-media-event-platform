# social-media-event-platform
A production-grade, scalable backend system for ingesting, processing, and analyzing high-volume social media events in real-time.


🏗️ Architecture
┌─────────────┐     ┌──────────┐     ┌─────────────┐     ┌──────────┐
│  FastAPI    │────▶│  Kafka   │────▶│   Spark     │────▶│   S3     │
│  Ingestion  │     │  Queue   │     │  Streaming  │     │  (Raw)   │
└─────────────┘     └──────────┘     └─────────────┘     └──────────┘
                                            │
                                            ▼
                                     ┌──────────┐     ┌─────────────┐
                                     │  MySQL   │◀────│   FastAPI   │
                                     │  (Agg)   │     │  Analytics  │
                                     └──────────┘     └─────────────┘

# ✨ Features

# Event Ingestion

High-throughput REST API for event ingestion
Batch and single event support
Kafka-based asynchronous processing
Automatic retry and error handling
Request validation with Pydantic

# Real-Time Processing

Spark Structured Streaming for event processing
Multiple time-window aggregations (5min, 1hr, 1day)
Late data handling with watermarking
Exactly-once semantics with checkpointing

# Data Storage

S3 Data Lake: Partitioned raw events (Parquet)
MySQL: Fast querying of aggregated metrics
Dual-write pattern for different access patterns

# Analytics API

RESTful endpoints for analytics queries
Event metrics by time windows
User activity tracking
Top users leaderboard
Event distribution analysis
Platform-wise metrics

# 🛠️ Tech Stack
ComponentTechnologyPurposeAPI FrameworkFastAPIHigh-performance async APIMessage QueueApache KafkaEvent streaming & bufferingStream ProcessingPySparkReal-time data processingData LakeAWS S3Raw event storageDatabaseMySQLAggregated metrics storageContainerizationDockerService packagingOrchestrationKubernetesProduction deploymentLanguagePython 3.11Primary development language

# 📦 Prerequisites

Docker 20.10+
Docker Compose 2.0+
Python 3.11+
AWS Account (for S3, optional for local dev)
Kubernetes cluster (for production deployment)
8GB RAM minimum
20GB disk space