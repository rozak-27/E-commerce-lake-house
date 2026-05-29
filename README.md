<<<<<<< HEAD
# E-Commerce Lakehouse Data Platform

Full-stack data lakehouse pipeline:
**Simulator → Kafka → Spark → MinIO (Iceberg) → Trino → FastAPI → React Dashboard**

## Architecture

```
1. Data Source     → Python fake event simulator
2. Ingestion       → Kafka + Schema Registry
3. Processing      → Apache Spark (Structured Streaming + Batch)
4. Storage         → MinIO S3 + Apache Iceberg (Medallion: Bronze/Silver/Gold)
5. Query Layer     → Trino (distributed SQL)
6. API             → FastAPI
7. Dashboard       → React + Next.js + TypeScript
8. Orchestration   → Apache Airflow
9. Monitoring      → Prometheus 
```

## Quick Start

```bash
# 1. Start all services
docker-compose up -d

# 2. Run simulator
cd simulator && python simulator.py

# 3. Start dashboard
cd dashboard && npm install && npm run dev
```

## Tech Stack
- Python 3.11, Kafka, Apache Spark 3.5, MinIO, Apache Iceberg, Trino, FastAPI, React/Next.js, Airflow, Prometheus, Grafana, Docker
=======
# E-commerce-lake-house
>>>>>>> 670ac08b5f8b2d81e173fc4a10fa4299548d87fd
