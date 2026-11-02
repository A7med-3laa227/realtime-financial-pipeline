"""
Spark Structured Streaming job: Kafka → Snowflake
====================================================
Consumes enriched transactions from two Kafka topics
(written by transactions_stream.py) and writes them
into the Snowflake data warehouse using the Snowflake
Spark connector (net.snowflake:spark-snowflake).

Topics consumed:
  - transactions.flagged      → FINANCIAL_DB.PUBLIC.FLAGGED_TRANSACTIONS
  - transactions.metrics.1m   → FINANCIAL_DB.PUBLIC.TRANSACTION_METRICS_1M

Run with:
  spark-submit \
    --packages net.snowflake:spark-snowflake_2.12:2.16.0,\
               net.snowflake:snowflake-jdbc:3.16.1,\
               org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    /opt/spark-apps/jobs/spark_to_snowflake.py
"""

import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    DoubleType, LongType, StringType, StructField, StructType,
)

# ---------------------------------------------------------------------------
# Configuration (passed via env vars from docker-compose)
# ---------------------------------------------------------------------------
BOOTSTRAP       = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
FLAGGED_TOPIC   = os.getenv("FLAGGED_TOPIC", "transactions.flagged")
METRICS_TOPIC   = os.getenv("METRICS_TOPIC", "transactions.metrics.1m")
CHECKPOINT_ROOT = os.getenv("CHECKPOINT_ROOT", "/tmp/checkpoints")

# Snowflake connection
SF_ACCOUNT      = os.getenv("SNOWFLAKE_ACCOUNT", "")
SF_USER         = os.getenv("SNOWFLAKE_USER", "")
SF_PASSWORD     = os.getenv("SNOWFLAKE_PASSWORD", "")
SF_PRIVATE_KEY  = os.getenv("SNOWFLAKE_PRIVATE_KEY", "")
SF_ROLE         = os.getenv("SNOWFLAKE_ROLE", "PIPELINE_ROLE")
SF_WAREHOUSE    = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SF_DATABASE     = os.getenv("SNOWFLAKE_DATABASE", "FINANCIAL_DB")
SF_SCHEMA       = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")

# Build the Snowflake connection options dict
sf_options = {
    "sfURL": f"{SF_ACCOUNT}.snowflakecomputing.com",
    "sfUser": SF_USER,
    "sfRole": SF_ROLE,
    "sfWarehouse": SF_WAREHOUSE,
    "sfDatabase": SF_DATABASE,
    "sfSchema": SF_SCHEMA,
}

# Prefer key-pair auth; fall back to password
if SF_PRIVATE_KEY:
    sf_options["pem_private_key"] = SF_PRIVATE_KEY
elif SF_PASSWORD:
    sf_options["sfPassword"] = SF_PASSWORD
else:
    raise ValueError(
        "Snowflake auth not configured. "
        "Set SNOWFLAKE_PASSWORD or SNOWFLAKE_PRIVATE_KEY."
    )

# ---------------------------------------------------------------------------
# Spark Session
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("kafka-to-snowflake")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# Snowflake writer helper
# ---------------------------------------------------------------------------
def write_to_snowflake(df, batch_id, table: str):
    """Write a micro-batch DataFrame to Snowflake via the Spark connector."""
    if df.rdd.isEmpty():
        print(f"[batch {batch_id}] Empty batch — skipping write to {table}")
        return

    # Count *before* writing so we don't trigger a second scan afterwards
    row_count = df.count()

    (
        df.write
        .format("snowflake")
        .options(**sf_options)
        .option("dbtable", table)
        .mode("append")
        .save()
    )
    print(f"[batch {batch_id}] Written {row_count} rows to {SF_DATABASE}.{SF_SCHEMA}.{table}")


# ---------------------------------------------------------------------------
# 1. FLAGGED TRANSACTIONS — Kafka → Snowflake
# ---------------------------------------------------------------------------
flagged_schema = StructType([
    StructField("transaction_id",  StringType()),
    StructField("user_id",         StringType()),
    StructField("event_time",      StringType()),   # ISO string from Spark
    StructField("amount",          DoubleType()),
    StructField("currency",        StringType()),
    StructField("amount_usd",      DoubleType()),
    StructField("merchant_name",   StringType()),
    StructField("payment_method",  StringType()),
    StructField("city",            StringType()),
    StructField("country",         StringType()),
    StructField("ip_address",      StringType()),
    StructField("risk_reason",     StringType()),
])

flagged_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", FLAGGED_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

flagged_df = (
    flagged_raw
    .select(F.from_json(F.col("value").cast("string"), flagged_schema).alias("d"))
    .select("d.*")
    .withColumn("event_time", F.to_timestamp("event_time"))
)

flagged_query = (
    flagged_df.writeStream
    .foreachBatch(lambda df, bid: write_to_snowflake(df, bid, "FLAGGED_TRANSACTIONS"))
    .option("checkpointLocation", f"{CHECKPOINT_ROOT}/sf_flagged")
    .trigger(processingTime="15 seconds")
    .queryName("flagged_to_snowflake")
    .start()
)

# ---------------------------------------------------------------------------
# 2. METRICS — Kafka → Snowflake
# ---------------------------------------------------------------------------
metrics_schema = StructType([
    StructField("window_start",     StringType()),
    StructField("window_end",       StringType()),
    StructField("currency",         StringType()),
    StructField("payment_method",   StringType()),
    StructField("txn_count",        LongType()),
    StructField("total_usd",        DoubleType()),
    StructField("avg_usd",          DoubleType()),
    StructField("max_usd",          DoubleType()),
    StructField("unique_users",     LongType()),
    StructField("high_value_count", LongType()),
    StructField("voucher_count",    LongType()),
])

metrics_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", METRICS_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

metrics_df = (
    metrics_raw
    .select(F.from_json(F.col("value").cast("string"), metrics_schema).alias("d"))
    .select("d.*")
    .withColumn("window_start", F.to_timestamp("window_start"))
    .withColumn("window_end",   F.to_timestamp("window_end"))
)

metrics_query = (
    metrics_df.writeStream
    .foreachBatch(lambda df, bid: write_to_snowflake(df, bid, "TRANSACTION_METRICS_1M"))
    .option("checkpointLocation", f"{CHECKPOINT_ROOT}/sf_metrics")
    .trigger(processingTime="15 seconds")
    .queryName("metrics_to_snowflake")
    .start()
)

# ---------------------------------------------------------------------------
# Wait for either stream to terminate
# ---------------------------------------------------------------------------
spark.streams.awaitAnyTermination()
