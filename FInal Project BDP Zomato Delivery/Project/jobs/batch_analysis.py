from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, col, round as spark_round
import json, os

spark = SparkSession.builder \
    .appName("ZomatoBatchAnalysis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ── Baca dataset dari HDFS ──────────────────────────────────────────────────
df = spark.read.csv(
    "hdfs://namenode:9000/data/zomato_clean.csv",
    header=True,
    inferSchema=True
)

total = df.count()
print(f"\n{'='*55}")
print(f"  ZOMATO DELIVERY — BATCH ANALYSIS")
print(f"  Total orders: {total}")
print(f"{'='*55}\n")

# ── 1. Traffic vs waktu delivery ────────────────────────────────────────────
print(">>> [1] Avg Delivery Time per Traffic Density")
df_traffic = df.groupBy("Road_traffic_density") \
    .agg(
        spark_round(avg("`Time_taken (min)`"), 2).alias("avg_delivery_time_min"),
        count("*").alias("total_orders")
    ) \
    .orderBy(col("avg_delivery_time_min").desc())
df_traffic.show(truncate=False)

# ── 2. Cuaca vs waktu delivery ──────────────────────────────────────────────
print(">>> [2] Avg Delivery Time per Weather Condition")
df_weather = df.groupBy("Weather_conditions") \
    .agg(
        spark_round(avg("`Time_taken (min)`"), 2).alias("avg_delivery_time_min"),
        count("*").alias("total_orders")
    ) \
    .orderBy(col("avg_delivery_time_min").desc())
df_weather.show(truncate=False)

# ── 3. Kota vs waktu delivery ───────────────────────────────────────────────
print(">>> [3] Avg Delivery Time per City")
df_city = df.groupBy("City") \
    .agg(
        spark_round(avg("`Time_taken (min)`"), 2).alias("avg_delivery_time_min"),
        count("*").alias("total_orders")
    ) \
    .orderBy(col("avg_delivery_time_min").desc())
df_city.show(truncate=False)

# ── 4. Heatmap: Traffic x Cuaca ────────────────────────────────────────────
print(">>> [4] Heatmap: Traffic x Weather vs Avg Delivery Time")
df_heatmap = df.groupBy("Road_traffic_density", "Weather_conditions") \
    .agg(
        spark_round(avg("`Time_taken (min)`"), 2).alias("avg_delivery_time_min")
    ) \
    .orderBy("Road_traffic_density", "Weather_conditions")
df_heatmap.show(50, truncate=False)

# ── 5. Jumlah delivery per person (top 20) ─────────────────────────────────
print(">>> [5] Top 20 Delivery Person by Jumlah Delivery")
df_person = df.groupBy("Delivery_person_ID") \
    .agg(
        count("*").alias("total_deliveries"),
        spark_round(avg("`Time_taken (min)`"), 2).alias("avg_delivery_time_min"),
        spark_round(avg("Delivery_person_Ratings"), 2).alias("avg_rating")
    ) \
    .orderBy(col("total_deliveries").desc())
df_person.show(20, truncate=False)

# ── Export JSON untuk Streamlit dashboard ───────────────────────────────────
DASHBOARD_DIR = "/opt/dashboard_data"
os.makedirs(DASHBOARD_DIR, exist_ok=True)

batch_results = {
    "total_orders": total,
    "by_traffic":  [r.asDict() for r in df_traffic.collect()],
    "by_weather":  [r.asDict() for r in df_weather.collect()],
    "by_city":     [r.asDict() for r in df_city.collect()],
    "heatmap":     [r.asDict() for r in df_heatmap.collect()],
    "by_person":   [r.asDict() for r in df_person.limit(20).collect()],
}

with open(f"{DASHBOARD_DIR}/batch_results.json", "w") as f:
    json.dump(batch_results, f, indent=2)

print(f"\n✅ Batch selesai. Hasil disimpan ke {DASHBOARD_DIR}/batch_results.json")
spark.stop()