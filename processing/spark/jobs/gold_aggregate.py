"""
Gold Aggregate Job — Iceberg Version
=====================================
Silver Iceberg → Gold Iceberg

Jalankan:
    docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/work-dir/gold_aggregate.py
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum,
    date_trunc, round as spark_round, desc
)
import os

MINIO_ENDPOINT     = os.getenv("MINIO_ENDPOINT",   "http://minio:9000")
MINIO_ACCESS_KEY   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY   = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
HIVE_METASTORE_URI = os.getenv("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")
WAREHOUSE_PATH     = "s3a://warehouse/"

def create_spark():
    return (
        SparkSession.builder
        .appName("gold-aggregate")
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

def save_gold(spark: SparkSession, df: DataFrame, name: str):
    spark.sql("CREATE DATABASE IF NOT EXISTS iceberg.gold")
    (
        df.writeTo(f"iceberg.gold.{name}")
        .tableProperty("write.format.default", "parquet")
        .createOrReplace()
    )
    print(f"  ✅ Saved → iceberg.gold.{name}  ({df.count():,} rows)")

def agg_revenue_daily(spark):
    print("\n  💰 Aggregating revenue_daily...")
    df = spark.table("iceberg.silver.order_events")
    result = (
        df.withColumn("date", date_trunc("day", col("event_timestamp")))
        .groupBy("date")
        .agg(
            spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
            count("order_id").alias("total_orders"),
            countDistinct("user_id").alias("unique_buyers"),
        )
        .orderBy("date")
    )
    save_gold(spark, result, "revenue_daily")

def agg_dau_daily(spark):
    print("\n  👥 Aggregating dau_daily...")
    df = spark.table("iceberg.silver.user_events")
    result = (
        df.withColumn("date", date_trunc("day", col("event_timestamp")))
        .groupBy("date")
        .agg(
            countDistinct("user_id").alias("active_users"),
            count("event_id").alias("total_events"),
        )
        .orderBy("date")
    )
    save_gold(spark, result, "dau_daily")

def agg_top_products(spark):
    print("\n  🏆 Aggregating top_products...")
    df = spark.table("iceberg.silver.order_events")
    result = (
        df.groupBy("product_id", "product_name", "category")
        .agg(
            spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
            spark_sum("quantity").alias("total_quantity"),
            count("order_id").alias("total_orders"),
        )
        .orderBy(desc("total_revenue"))
        .limit(50)
    )
    save_gold(spark, result, "top_products")

def agg_funnel(spark):
    print("\n  🔻 Aggregating funnel_events...")
    df = spark.table("iceberg.silver.user_events")
    result = (
        df.withColumn("date", date_trunc("day", col("event_timestamp")))
        .groupBy("date", "event_type")
        .agg(
            count("event_id").alias("event_count"),
            countDistinct("user_id").alias("unique_users"),
        )
        .orderBy("date", "event_type")
    )
    save_gold(spark, result, "funnel_events")

def agg_payment_summary(spark):
    print("\n  💳 Aggregating payment_summary...")
    df = spark.table("iceberg.silver.payment_events")
    result = (
        df.groupBy("payment_method", "status")
        .agg(
            count("event_id").alias("transaction_count"),
            spark_round(spark_sum("amount"), 2).alias("total_amount"),
        )
        .orderBy("payment_method", "status")
    )
    save_gold(spark, result, "payment_summary")

def main():
    print("🥇 Gold Aggregate Job (Iceberg) starting...")
    print(f"   Metastore : {HIVE_METASTORE_URI}")
    print("─" * 50)

    spark = create_spark()

    jobs = [
        ("revenue_daily",   agg_revenue_daily),
        ("dau_daily",       agg_dau_daily),
        ("top_products",    agg_top_products),
        ("funnel_events",   agg_funnel),
        ("payment_summary", agg_payment_summary),
    ]

    for name, fn in jobs:
        try:
            fn(spark)
        except Exception as e:
            print(f"  ⚠️  Skipped {name}: {e}")

    spark.stop()
    print("\n✅ Gold Aggregate Job done!")
    print("   Iceberg tables siap di-query Trino!")

if __name__ == "__main__":
    main()