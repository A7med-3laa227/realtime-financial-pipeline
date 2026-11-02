"""
Kafka (Debezium) -> Spark Structured Streaming -> ClickHouse (financial_db.transactions)

Run (the clickhouse-loader compose service does this for you):

  docker compose --profile jobs up clickhouse-loader

or by hand from the Spark master container:

  docker exec -it spark-master /opt/spark/bin/spark-submit \
    --master spark://spark:7077 --conf spark.driver.host=spark \
    --conf spark.driver.extraJavaOptions=-Duser.timezone=UTC \
    --conf spark.executor.extraJavaOptions=-Duser.timezone=UTC \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.clickhouse:clickhouse-jdbc:0.6.3,org.apache.httpcomponents.client5:httpclient5:5.3.1 \
    /opt/spark-apps/jobs/spark_to_clickhouse.py
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, expr, from_json, when
from pyspark.sql.types import LongType, StringType, StructField, StructType

KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "pg.public.transactions")   # <topic.prefix>.<schema>.<table>
CHECKPOINT = os.getenv("CHECKPOINT_DIR", "/opt/spark-apps/jobs/_checkpoints/transactions_clickhouse")

CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CH_PORT = os.getenv("CLICKHOUSE_PORT", "8123")
CH_DB = os.getenv("CLICKHOUSE_DB", "financial_db")
CH_USER = os.getenv("CLICKHOUSE_USER", "clickhouse")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "2452005")
CH_TABLE = f"{CH_DB}.transactions"
CH_URL = f"jdbc:clickhouse://{CH_HOST}:{CH_PORT}/{CH_DB}"

if not CH_PASSWORD:
    raise RuntimeError("CLICKHOUSE_PASSWORD is not set (check your .env file)")

spark = (SparkSession.builder.appName("transactions-to-clickhouse")
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())

row = StructType([
    StructField("transaction_id", StringType()),
    StructField("user_id", StringType()),
    StructField("timestamp", LongType()),     # epoch micros from Debezium
    StructField("amount", StringType()),      # connector: decimal.handling.mode=string
    StructField("currency", StringType()),
    StructField("city", StringType()),
    StructField("country", StringType()),
    StructField("merchant_name", StringType()),
    StructField("payment_method", StringType()),
    StructField("ip_address", StringType()),
    StructField("voucher_code", StringType()),
    StructField("affiliate_id", StringType()),
])
envelope = StructType([
    StructField("before", row),
    StructField("after", row),
    StructField("op", StringType()),
    StructField("ts_ms", LongType()),
])

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", KAFKA)
       .option("subscribe", TOPIC)
       .option("startingOffsets", "earliest")
       .option("maxOffsetsPerTrigger", 50000)
       .load())

parsed = (raw
          .select(from_json(col("value").cast("string"), envelope).alias("v"))
          .where(col("v.op").isNotNull())            # skips tombstones / malformed messages
          .select(
              coalesce(col("v.after"), col("v.before")).alias("r"),   # deletes carry "before"
              col("v.op").alias("op"),
              col("v.ts_ms").alias("version"))
          .select("r.*", "version", when(col("op") == "d", 1).otherwise(0).alias("is_deleted"))
          .withColumn("timestamp", expr("timestamp_micros(timestamp)"))
          .withColumn("amount", col("amount").cast("decimal(18,2)")))


def write_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    (batch_df.write
     .format("jdbc")
     .option("url", CH_URL)
     .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
     .option("dbtable", CH_TABLE)
     .option("user", CH_USER)
     .option("password", CH_PASSWORD)
     .option("batchsize", 50000)           # large inserts = fewer parts in ClickHouse
     .option("isolationLevel", "NONE")     # ClickHouse has no transactions
     .mode("append")
     .save())


# availableNow: process everything new since the last checkpoint, then exit.
# A retried batch can insert duplicate rows; ReplacingMergeTree + FINAL
# (the transactions_current view) collapses them.
query = (parsed.writeStream
         .foreachBatch(write_batch)
         .option("checkpointLocation", CHECKPOINT)
         .trigger(availableNow=True)
         .start())

query.awaitTermination()