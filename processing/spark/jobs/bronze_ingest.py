"""
Bronze Ingest Job — Iceberg Version
=====================================
Kafka → Bronze Iceberg tables di MinIO

Jalankan:
    docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/work-dir/bronze_ingest.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType
)
import os

# ── Config ───────────────────────────────────────────────
KAFKA_BOOTSTRAP     = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
MINIO_ENDPOINT      = os.getenv("MINIO_ENDPOINT",   "http://minio:9000")
MINIO_ACCESS_KEY    = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY    = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
HIVE_METASTORE_URI  = os.getenv("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")

WAREHOUSE_PATH  = "s3a://warehouse/"
CHECKPOINT_PATH = "s3a://bronze/_checkpoints"

# ── Schema per topic ──────────────────────────────────────
USER_SCHEMA = StructType([
    StructField("event_id",     StringType()),
    StructField("event_type",   StringType()),
    StructField("user_id",      StringType()),
    StructField("session_id",   StringType()),
    StructField("timestamp",    StringType()),
    StructField("platform",     StringType()),
    StructField("ip_address",   StringType()),
    StructField("user_agent",   StringType()),
    StructField("product_id",   StringType()),
    StructField("product_name", StringType()),
    StructField("category",     StringType()),
    StructField("price",        DoubleType()),
])

SEARCH_SCHEMA = StructType([
    StructField("event_id",     StringType()),
    StructField("event_type",   StringType()),
    StructField("user_id",      StringType()),
    StructField("session_id",   StringType()),
    StructField("timestamp",    StringType()),
    StructField("platform",     StringType()),
    StructField("ip_address",   StringType()),
    StructField("user_agent",   StringType()),
    StructField("query",        StringType()),
    StructField("result_count", IntegerType()),
])

ORDER_SCHEMA = StructType([
    StructField("event_id",       StringType()),
    StructField("event_type",     StringType()),
    StructField("user_id",        StringType()),
    StructField("session_id",     StringType()),
    StructField("timestamp",      StringType()),
    StructField("platform",       StringType()),
    StructField("product_id",     StringType()),
    StructField("product_name",   StringType()),
    StructField("category",       StringType()),
    StructField("price",          DoubleType()),
    StructField("order_id",       StringType()),
    StructField("quantity",       IntegerType()),
    StructField("total_amount",   DoubleType()),
    StructField("payment_method", StringType()),
    StructField("shipping_city",  StringType()),
])

PAYMENT_SCHEMA = StructType([
    StructField("event_id",       StringType()),
    StructField("order_id",       StringType()),
    StructField("user_id",        StringType()),
    StructField("payment_method", StringType()),
    StructField("amount",         DoubleType()),
    StructField("status",         StringType()),
    StructField("timestamp",      StringType()),
])

PRODUCT_SCHEMA = StructType([
    StructField("event_id",     StringType()),
    StructField("event_type",   StringType()),
    StructField("user_id",      StringType()),
    StructField("session_id",   StringType()),
    StructField("timestamp",    StringType()),
    StructField("platform",     StringType()),
    StructField("product_id",   StringType()),
    StructField("product_name", StringType()),
    StructField("category",     StringType()),
    StructField("price",        DoubleType()),
    StructField("quantity",     IntegerType()),
])

TOPIC_SCHEMAS = {
    "user-events":    USER_SCHEMA,
    "product-events": PRODUCT_SCHEMA,
    "order-events":   ORDER_SCHEMA,
    "payment-events": PAYMENT_SCHEMA,
    "search-events":  SEARCH_SCHEMA,
}

# ── Spark Session ─────────────────────────────────────────
def create_spark():
    return (
        SparkSession.builder
        .appName("bronze-ingest")
        # Iceberg extensions
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # Iceberg catalog → pakai Hive Metastore
        .config("spark.sql.catalog.iceberg",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hive")
        .config("spark.sql.catalog.iceberg.uri",  HIVE_METASTORE_URI)
        .config("spark.sql.catalog.iceberg.warehouse", WAREHOUSE_PATH)
        # MinIO
        .config("spark.hadoop.fs.s3a.endpoint",              MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",            MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",            MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",     "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

# ── Create schema & table kalau belum ada ─────────────────
def ensure_table(spark: SparkSession, table_name: str, schema: StructType):
    spark.sql("CREATE DATABASE IF NOT EXISTS iceberg.bronze")
    
    # buat kolom dari schema
    cols = ", ".join([
        f"{f.name} {f.dataType.simpleString().upper()}"
        for f in schema.fields
    ])
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS iceberg.bronze.{table_name} (
            {cols},
            kafka_timestamp TIMESTAMP,
            ingested_at TIMESTAMP
        )
        USING iceberg
        LOCATION '{WAREHOUSE_PATH}bronze/{table_name}'
    """)

# ── Ingest per topic ──────────────────────────────────────
def ingest_topic(spark: SparkSession, topic: str, schema: StructType):
    table_name = topic.replace("-", "_")

    # pastikan tabel Iceberg sudah ada
    ensure_table(spark, table_name, schema)

    # baca dari Kafka
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # parse JSON + tambah metadata
    parsed_df = (
        raw_df
        .select(
            from_json(col("value").cast("string"), schema).alias("data"),
            col("timestamp").alias("kafka_timestamp"),
        )
        .select(
            "data.*",
            "kafka_timestamp",
            current_timestamp().alias("ingested_at"),
        )
    )

    # tulis ke Iceberg table
    query = (
        parsed_df.writeStream
        .format("iceberg")
        .outputMode("append")
        .option("path", f"iceberg.bronze.{table_name}")
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/{table_name}")
        .trigger(processingTime="30 seconds")
        .start()
    )

    print(f"  ✅ Streaming: {topic} → iceberg.bronze.{table_name}")
    return query

# ── Main ─────────────────────────────────────────────────
def main():
    print("🔥 Bronze Ingest Job (Iceberg) starting...")
    print(f"   Kafka     : {KAFKA_BOOTSTRAP}")
    print(f"   Metastore : {HIVE_METASTORE_URI}")
    print(f"   Warehouse : {WAREHOUSE_PATH}")
    print("─" * 50)

    spark   = create_spark()
    queries = []

    for topic, schema in TOPIC_SCHEMAS.items():
        query = ingest_topic(spark, topic, schema)
        queries.append(query)

    print(f"\n✅ {len(queries)} streams running!")
    print("   Data mengalir: Kafka → Iceberg Bronze")
    print("   Tekan Ctrl+C untuk stop\n")

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()