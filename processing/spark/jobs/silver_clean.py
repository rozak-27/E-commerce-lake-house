"""
Silver Clean Job — Iceberg Version
=====================================
Bronze Iceberg → Silver Iceberg

Jalankan:
    docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/work-dir/silver_clean.py
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp, when, lower, trim, abs as spark_abs
import os

MINIO_ENDPOINT     = os.getenv("MINIO_ENDPOINT",   "http://minio:9000")
MINIO_ACCESS_KEY   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY   = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
HIVE_METASTORE_URI = os.getenv("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")
WAREHOUSE_PATH     = "s3a://warehouse/"

TABLES = [
    "user_events", "product_events", "order_events",
    "payment_events", "search_events",
]

def create_spark():
    return (
        SparkSession.builder
        .appName("silver-clean")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg",           "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type",      "hive")
        .config("spark.sql.catalog.iceberg.uri",       HIVE_METASTORE_URI)
        .config("spark.sql.catalog.iceberg.warehouse", WAREHOUSE_PATH)
        .config("spark.hadoop.fs.s3a.endpoint",              MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",            MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",            MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",     "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

# ── Cleaners ──────────────────────────────────────────────
def clean_user_events(df: DataFrame) -> DataFrame:
    return (
        df.dropDuplicates(["event_id"])
        .filter(col("user_id").isNotNull())
        .filter(col("event_type").isNotNull())
        .withColumn("event_type", lower(trim(col("event_type"))))
        .withColumn("platform",   lower(trim(col("platform"))))
        .withColumn("price", when(col("price") < 0, None).otherwise(col("price")))
        .withColumn("event_timestamp", to_timestamp(col("timestamp")))
        .drop("timestamp")
    )

def clean_order_events(df: DataFrame) -> DataFrame:
    return (
        df.dropDuplicates(["event_id"])
        .filter(col("order_id").isNotNull())
        .filter(col("total_amount") > 0)
        .withColumn("payment_method",  lower(trim(col("payment_method"))))
        .withColumn("event_timestamp", to_timestamp(col("timestamp")))
        .drop("timestamp")
    )

def clean_payment_events(df: DataFrame) -> DataFrame:
    return (
        df.dropDuplicates(["event_id"])
        .filter(col("order_id").isNotNull())
        .filter(col("amount") > 0)
        .withColumn("status",          lower(trim(col("status"))))
        .withColumn("payment_method",  lower(trim(col("payment_method"))))
        .withColumn("event_timestamp", to_timestamp(col("timestamp")))
        .drop("timestamp")
    )

def clean_search_events(df: DataFrame) -> DataFrame:
    return (
        df.dropDuplicates(["event_id"])
        .filter(col("user_id").isNotNull())
        .withColumn("query",           lower(trim(col("query"))))
        .withColumn("result_count",    when(col("result_count") < 0, 0).otherwise(col("result_count")))
        .withColumn("event_timestamp", to_timestamp(col("timestamp")))
        .drop("timestamp")
    )

def clean_product_events(df: DataFrame) -> DataFrame:
    return (
        df.dropDuplicates(["event_id"])
        .filter(col("product_id").isNotNull())
        .withColumn("price",           when(col("price") < 0, None).otherwise(col("price")))
        .withColumn("event_timestamp", to_timestamp(col("timestamp")))
        .drop("timestamp")
    )

CLEANERS = {
    "user_events":    clean_user_events,
    "order_events":   clean_order_events,
    "payment_events": clean_payment_events,
    "search_events":  clean_search_events,
    "product_events": clean_product_events,
}

def process_table(spark: SparkSession, table: str):
    print(f"\n  📥 Reading iceberg.bronze.{table}...")
    df     = spark.table(f"iceberg.bronze.{table}")
    before = df.count()
    print(f"     Rows before : {before:,}")

    cleaner = CLEANERS.get(table)
    if cleaner:
        df = cleaner(df)

    after = df.count()
    print(f"     Rows after  : {after:,}  (dropped: {before - after:,})")

    # buat schema silver kalau belum ada
    spark.sql("CREATE DATABASE IF NOT EXISTS iceberg.silver")

    # tulis ke Silver Iceberg
    (
        df.writeTo(f"iceberg.silver.{table}")
        .tableProperty("write.format.default", "parquet")
        .createOrReplace()
    )
    print(f"  ✅ Saved → iceberg.silver.{table}")

def main():
    print("🥈 Silver Clean Job (Iceberg) starting...")
    print(f"   Metastore : {HIVE_METASTORE_URI}")
    print("─" * 50)

    spark = create_spark()

    for table in TABLES:
        try:
            process_table(spark, table)
        except Exception as e:
            print(f"  ⚠️  Skipped {table}: {e}")

    spark.stop()
    print("\n✅ Silver Clean Job done!")

if __name__ == "__main__":
    main()