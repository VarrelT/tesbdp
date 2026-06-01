"""
producer.py — Kafka Producer untuk Zomato Delivery Streaming
Membaca zomato_clean.csv (hasil cleaning) dan mengirim ke Kafka
row per row setiap 0.5 detik, diurutkan berdasarkan Order_Date.
"""

import pandas as pd
import json
import time
from kafka import KafkaProducer

KAFKA_SERVER = "localhost:29092"
KAFKA_TOPIC  = "zomato-delivery"
CSV_PATH     = "../data/zomato_clean.csv"   # pakai data yang sudah bersih
INTERVAL     = 0.5                          # detik per baris

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Baca dataset bersih dan urutkan berdasarkan tanggal order
df = pd.read_csv(CSV_PATH)
df = df.sort_values("Order_Date").reset_index(drop=True)

print(f"Dataset dimuat: {len(df)} baris (sudah bersih)")
print(f"Mengirim ke Kafka topic '{KAFKA_TOPIC}' setiap {INTERVAL} detik...")
print("-" * 60)

for idx, row in df.iterrows():
    # Konversi row ke dict, handle NaN
    message = {}
    for col, val in row.items():
        if pd.isna(val):
            message[col] = None
        elif hasattr(val, "item"):          # numpy type → python native
            message[col] = val.item()
        elif hasattr(val, "isoformat"):     # datetime → string
            message[col] = val.isoformat()
        else:
            message[col] = val

    producer.send(KAFKA_TOPIC, value=message)
    print(
        f"[{idx+1}/{len(df)}] "
        f"ID: {message.get('ID', '-')} | "
        f"City: {message.get('City', '-')} | "
        f"Traffic: {message.get('Road_traffic_density', '-')} | "
        f"Time: {message.get('Time_taken (min)', '-')} min"
    )
    time.sleep(INTERVAL)

producer.flush()
print("\n✅ Semua data sudah dikirim ke Kafka.")