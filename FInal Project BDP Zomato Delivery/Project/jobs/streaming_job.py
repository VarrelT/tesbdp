from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, count, round as spark_round
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType
import json, os

# ── Schema sesuai kolom dataset Zomato ─────────────────────────────────────
schema = StructType() \
    .add("ID", StringType()) \
    .add("Delivery_person_ID", StringType()) \
    .add("Delivery_person_Age", StringType()) \
    .add("Delivery_person_Ratings", DoubleType()) \
    .add("Restaurant_latitude", DoubleType()) \
    .add("Restaurant_longitude", DoubleType()) \
    .add("Delivery_location_latitude", DoubleType()) \
    .add("Delivery_location_longitude", DoubleType()) \
    .add("Order_Date", StringType()) \
    .add("Time_Ordered", StringType()) \
    .add("Time_Order_picked", StringType()) \
    .add("Weather_conditions", StringType()) \
    .add("Road_traffic_density", StringType()) \
    .add("Vehicle_condition", IntegerType()) \
    .add("Type_of_order", StringType()) \
    .add("Type_of_vehicle", StringType()) \
    .add("multiple_deliveries", DoubleType()) \
    .add("Festival", StringType()) \
    .add("City", StringType()) \
    .add("Time_taken (min)", DoubleType())

spark = SparkSession.builder \
    .appName("ZomatoStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ── Baca stream dari Kafka ──────────────────────────────────────────────────
raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "zomato-delivery") \
    .option("startingOffsets", "latest") \
    .load()

parsed = raw.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# ── Output dir ─────────────────────────────────────────────────────────────
DASHBOARD_DIR = "/opt/dashboard_data"
os.makedirs(DASHBOARD_DIR, exist_ok=True)


def write_snapshot(batch_df, batch_id):
    """Tulis hasil agregasi ke file JSON untuk dibaca Streamlit."""

    # 1. Jumlah order realtime (total keseluruhan batch ini)
    total_orders = batch_df.count()

    # 2. Rata-rata delivery time realtime
    avg_row = batch_df.agg(
        spark_round(avg("`Time_taken (min)`"), 2).alias("avg_time")
    ).collect()
    avg_time = avg_row[0]["avg_time"] if avg_row else 0.0

    # 3. Distribusi order per kota
    by_city = batch_df.groupBy("City") \
        .agg(count("*").alias("order_count")) \
        .orderBy(col("order_count").desc()) \
        .collect()

    # 4. Traffic condition live
    by_traffic = batch_df.groupBy("Road_traffic_density") \
        .agg(
            count("*").alias("order_count"),
            spark_round(avg("`Time_taken (min)`"), 2).alias("avg_time")
        ) \
        .orderBy(col("order_count").desc()) \
        .collect()

    snapshot = {
        "batch_id": batch_id,
        "total_orders": total_orders,
        "avg_delivery_time": avg_time,
        "by_city": [r.asDict() for r in by_city],
        "by_traffic": [r.asDict() for r in by_traffic],
    }

    # Tulis snapshot terbaru
    with open(f"{DASHBOARD_DIR}/latest_snapshot.json", "w") as f:
        json.dump(snapshot, f, indent=2)

    # Append ke history untuk grafik tren
    with open(f"{DASHBOARD_DIR}/history.jsonl", "a") as f:
        f.write(json.dumps({
            "batch_id": batch_id,
            "total_orders": total_orders,
            "avg_delivery_time": avg_time
        }) + "\n")

    print(f"[Batch {batch_id}] orders={total_orders} | avg_time={avg_time} min")


# ── Jalankan streaming query ────────────────────────────────────────────────
query = parsed.writeStream \
    .outputMode("append") \
    .foreachBatch(write_snapshot) \
    .trigger(processingTime="10 seconds") \
    .option("checkpointLocation", "/opt/checkpoints/streaming") \
    .start()

query.awaitTermination()