import sys
from pyspark.sql import SparkSession, functions as F, types as T

KAFKA_BOOTSTRAP = "kafka:9092"  # inside Docker network
TOPIC = "events.raw"

DATA_ROOT = "/opt/data"          # bind-mount for data
CKPT_ROOT = "/tmp/checkpoints"   # container-local for Spark state

BRONZE_PATH = f"{DATA_ROOT}/bronze/events"
CKPT_BRONZE = f"{CKPT_ROOT}/bronze_events"

SILVER_ROOT = f"{DATA_ROOT}/silver"
CKPT_PV   = f"{CKPT_ROOT}/silver_page_view"
CKPT_CART = f"{CKPT_ROOT}/silver_add_to_cart"
CKPT_ORD  = f"{CKPT_ROOT}/silver_order_placed"
CKPT_SESS = f"{CKPT_ROOT}/silver_sessions"
CKPT_LATE = f"{CKPT_ROOT}/silver_late"



def spark():
    return (SparkSession.builder
            .appName("weekend-de")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate())

# Generic superset schema so we can parse all payloads
LINE_ITEM = T.StructType([
    T.StructField("sku", T.StringType()),
    T.StructField("qty", T.IntegerType()),
    T.StructField("price", T.DoubleType()),
])

EVENT_SCHEMA = T.StructType([
    T.StructField("event_type", T.StringType()),
    T.StructField("event_id", T.StringType()),
    T.StructField("user_id", T.StringType()),
    T.StructField("order_id", T.StringType()),
    T.StructField("ts", T.StringType()),
    # page_view
    T.StructField("url", T.StringType()),
    T.StructField("referrer", T.StringType()),
    T.StructField("ua", T.StringType()),
    T.StructField("session_hint", T.StringType()),
    # add_to_cart
    T.StructField("sku", T.StringType()),
    T.StructField("qty", T.IntegerType()),
    T.StructField("price", T.DoubleType()),
    # order_placed
    T.StructField("total", T.DoubleType()),
    T.StructField("line_items", T.ArrayType(LINE_ITEM)),
])

def main():
    sp = spark()

    raw = (sp.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load())

    parsed = (raw
        .select(
            F.col("key").cast("string").alias("k"),
            F.col("value").cast("string").alias("v"))
        .select(F.from_json(F.col("v"), EVENT_SCHEMA).alias("e"))
        .select("e.*")
        .withColumn("ts", F.to_timestamp("ts"))
        .withColumn("event_date", F.to_date("ts"))
        .withColumn("ingest_ts", F.current_timestamp())
    )

    # Deduplicate within a 10-minute watermark window
    deduped = (parsed
        .withWatermark("ts", "10 minutes")
        .dropDuplicates(["event_id"])
    )

    # === Bronze (append, partitioned by event_date)
    bronze_q = (deduped
        .writeStream
        .partitionBy("event_date")
        .format("parquet")
        .option("path", BRONZE_PATH)
        .option("checkpointLocation", CKPT_BRONZE)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start())

    # Late audit (anything older than now()-10m when we see it)
    late_flagged = deduped.withColumn(
        "is_late", F.col("ts") < (F.current_timestamp() - F.expr("INTERVAL 10 minutes"))
    )
    late_rows = late_flagged.filter("is_late = true")

    late_q = (late_rows
        .writeStream
        .format("parquet")
        .option("path", f"{SILVER_ROOT}/late/events")
        .option("checkpointLocation", CKPT_LATE)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start())

    # === Silver normalized: page_view, add_to_cart, order_placed
    page_view = (deduped.filter(F.col("event_type") == F.lit("page_view"))
        .select("event_id","user_id","ts","event_date","url","referrer","ua","session_hint","ingest_ts"))
    cart = (deduped.filter(F.col("event_type") == F.lit("add_to_cart"))
        .select("event_id","user_id","ts","event_date","sku","qty","price","ingest_ts"))
    orders = (deduped.filter(F.col("event_type") == F.lit("order_placed"))
        .select("event_id","order_id","user_id","ts","event_date","total","line_items","ingest_ts"))

    pv_q = (page_view.writeStream
        .partitionBy("event_date")
        .format("parquet")
        .option("path", f"{SILVER_ROOT}/page_view")
        .option("checkpointLocation", CKPT_PV)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start())

    cart_q = (cart.writeStream
        .partitionBy("event_date")
        .format("parquet")
        .option("path", f"{SILVER_ROOT}/add_to_cart")
        .option("checkpointLocation", CKPT_CART)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start())

    ord_q = (orders.writeStream
        .partitionBy("event_date")
        .format("parquet")
        .option("path", f"{SILVER_ROOT}/order_placed")
        .option("checkpointLocation", CKPT_ORD)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start())

    # === Sessionization (30m inactivity) on event-time with watermark
    events_for_sessions = (deduped
        .select("user_id", "ts", "event_type")
        .withWatermark("ts", "10 minutes"))

    sess_agg = (events_for_sessions
        .groupBy(
            "user_id",
            F.session_window(F.col("ts"), "30 minutes").alias("sw")
        )
        .agg(
            F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("page_count"),
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("cart_events"),
            F.max(F.when(F.col("event_type") == "order_placed", 1).otherwise(0)).alias("conversion_flag")
        )
        .select(
            F.sha2(F.concat_ws("|", F.col("user_id"), F.col("sw.start").cast("string"), F.col("sw.end").cast("string")), 256).alias("session_id"),
            F.col("user_id"),
            F.col("sw.start").alias("session_start"),
            F.col("sw.end").alias("session_end"),
            F.col("page_count"),
            F.col("cart_events"),
            F.col("conversion_flag").cast("int"),
            F.to_date(F.col("sw.start")).alias("session_date"),
            F.current_timestamp().alias("ingest_ts")
        )
    )

    sess_q = (sess_agg.writeStream
        .partitionBy("session_date")
        .format("parquet")
        .option("path", f"{SILVER_ROOT}/sessions")
        .option("checkpointLocation", CKPT_SESS)
        .outputMode("append")
        .trigger(processingTime="20 seconds")
        .start())

    sp.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
