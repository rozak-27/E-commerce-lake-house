# Setup Guide

## Prerequisites
- Docker & Docker Compose
- Python 3.11+
- Node.js 18+
- Java 11+ (for Spark locally)

## 1. Start infrastructure

```bash
cp .env.example .env
docker-compose up -d
# Services: Kafka :9092, MinIO :9000/:9001, Trino :8082, Airflow :8083
# Grafana :3001, Prometheus :9090, Kafka UI :8080, API :8000
```

## 2. Run event simulator

```bash
cd simulator
pip install -r requirements.txt
python simulator.py --rate 5
# Options: --rate N (events/sec), --duration N (seconds)
```

## 3. Start Spark streaming (Bronze ingest)

```bash
cd processing/spark/jobs
spark-submit --packages \
  org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
  org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3 \
  bronze_ingest.py
```

## 4. Run batch jobs (Silver & Gold)

```bash
spark-submit silver_clean.py
spark-submit gold_aggregate.py
```

## 5. Query with Trino

```bash
# Via CLI
trino --server localhost:8082 --catalog iceberg --schema gold
> SELECT * FROM order_agg LIMIT 10;

# Or use sample queries in query/trino/sql/sample_queries.sql
```

## 6. Start the dashboard

```bash
cd dashboard
npm install
cp .env.local.example .env.local   # set NEXT_PUBLIC_API_URL
npm run dev   # http://localhost:3000
```

## URLs

| Service      | URL                        | Credentials         |
|-------------|----------------------------|---------------------|
| Kafka UI    | http://localhost:8080      | —                   |
| MinIO       | http://localhost:9001      | minioadmin/minioadmin123 |
| Trino       | http://localhost:8082      | admin               |
| Airflow     | http://localhost:8083      | admin/admin         |
| API Docs    | http://localhost:8000/docs | —                   |
| Grafana     | http://localhost:3001      | admin/admin         |
| Dashboard   | http://localhost:3000      | —                   |
