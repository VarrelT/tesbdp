# Zomato Delivery — Big Data Pipeline

**Final Project Big Data Processing**

---

## Architecture Diagram

```
  zomato.csv (raw, dari Kaggle)
       │
       ▼
  [cleaning.py]           ← jalankan SEKALI di lokal (Python)
       │
       ▼
  zomato_clean.csv
       │
       ├─────────────────────────────────────────┐
       │  BATCH PATH                             │  STREAMING PATH
       ▼                                         ▼
  Upload ke HDFS                          [producer.py]
       │                                  0.5 dtk/baris, urut Order_Date
       ▼                                         │
  [Spark Batch]                                  ▼
  batch_analysis.py                         [Kafka]
       │                                  topic: zomato-delivery
       │                                         │
       │                                         ▼
       │                                  [Spark Streaming]
       │                                  streaming_job.py
       │                                  trigger: 10 detik
       │                                         │
       └──────────────┬──────────────────────────┘
                      ▼
              [Streamlit Dashboard]
              Tab Batch | Tab Real-time
              localhost:8501
```

**Alur ringkas:**
1. `cleaning.py` → bersihkan raw CSV, output `zomato_clean.csv`
2. **Batch**: upload `zomato_clean.csv` ke HDFS → Spark batch baca & analisis → simpan `batch_results.json`
3. **Streaming**: producer baca `zomato_clean.csv` → kirim ke Kafka → Spark Streaming agregasi tiap 10 dtk → simpan `latest_snapshot.json`
4. Streamlit membaca kedua JSON dan menampilkan dashboard

---

## Project Description

Domain: **Food Delivery (Zomato)**

Proyek ini membangun pipeline big data end-to-end untuk menganalisis operasi pengiriman makanan dari platform Zomato. Pipeline mencakup dua jalur:

- **Batch processing** — membaca seluruh dataset historis dari HDFS menggunakan Apache Spark untuk menghasilkan insight tentang faktor-faktor yang mempengaruhi waktu pengiriman.
- **Stream processing** — mensimulasikan data pengiriman yang masuk secara real-time melalui Kafka, diproses oleh Spark Structured Streaming, dan divisualisasikan live di Streamlit.

Sebelum masuk ke pipeline, dataset diproses melalui tahap **data cleaning** yang mencakup penanganan missing values, konversi tipe data, penghapusan outliers (IQR), duplikat, dan validasi range nilai.

Seluruh sistem berjalan di atas Docker Compose dan dapat dijalankan di laptop manapun.

---

## Problem Statement

### Batch Insight
> **Faktor apa saja yang paling mempengaruhi waktu pengiriman makanan?**

- Kondisi lalu lintas mana yang menyebabkan waktu pengiriman terlama?
- Cuaca seperti apa yang paling menghambat pengiriman?
- Kota mana yang rata-rata waktu deliverynya paling tinggi?
- Kombinasi traffic × cuaca mana yang paling buruk? *(heatmap)*
- Siapa delivery person dengan jumlah pengiriman terbanyak?

### Real-time Metric
> **Bagaimana kondisi pengiriman yang sedang terjadi saat ini?**

- Jumlah order yang masuk per batch (setiap 10 detik)
- Rata-rata waktu delivery dari order yang sedang berjalan
- Distribusi order per kota secara live
- Kondisi traffic yang sedang mendominasi order saat ini

---

## Dataset Description

| Field | Keterangan |
|---|---|
| **Sumber** | [Kaggle — Zomato Delivery Operations Analytics](https://www.kaggle.com/datasets/saurabhbadole/zomato-delivery-operations-analytics-dataset) |
| **Ukuran Raw** | ~15 MB, 45.584 rows |
| **Ukuran Cleaned** | ~13 MB, ±42.493 rows |
| **Format** | CSV |

**Proses cleaning yang dilakukan (`cleaning.py`):**

| Tahap | Tindakan | Keterangan |
|---|---|---|
| 1. Missing Values | Drop baris `multiple_deliveries` NULL | Fill `Vehicle_condition` dengan mode |
| 2. Konversi Tipe | Numeric → float/int | `Order_Date` → datetime (`%d-%m-%Y`) |
| 3. Outliers (IQR) | Hapus outlier `Delivery_person_Age` & `Time_taken (min)` | Metode IQR × 1.5 |
| 4. Duplikat | `drop_duplicates()` | — |
| 5. Validasi Range | Age > 0 & ≤ 100, Ratings > 0, Time > 0 | Hapus nilai tidak valid |

**Kolom utama yang digunakan pipeline:**

| Kolom | Digunakan untuk |
|---|---|
| `ID` | Identifier unik tiap order |
| `Delivery_person_ID` | Analisis per kurir (batch) |
| `Delivery_person_Ratings` | Performa kurir |
| `Weather_conditions` | Batch insight — pengaruh cuaca |
| `Road_traffic_density` | Batch + streaming — kondisi traffic |
| `City` | Batch + streaming — distribusi kota |
| `Time_taken (min)` | **Target utama analisis** |
| `Order_Date` | Sorting data untuk simulasi streaming |
| `Festival` | Faktor tambahan batch |

---

## How to Run

### Prasyarat
Pastikan sudah terinstall:
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (aktif dan berjalan)
- Python 3.8+

```bash
pip install pandas numpy kafka-python
```

---

### Langkah 1 — Clone Repository

```bash
git clone https://github.com/VarrelT/BDP-Final-Project-Kelompok-4
cd FInal Project BDP Zomato Delivery\Project
```

Struktur folder yang diharapkan:

```
Project/
├── docker-compose.yml
├── README.md
├── data/
│   ├── cleaning.py          ← script cleaning (dari Cekdata.ipynb)
│   ├── zomato.csv           ← download dari Kaggle (raw) — letakkan di sini
│   └── zomato_clean.csv     ← dibuat otomatis setelah menjalankan cleaning.py
├── producer/
│   ├── producer.py
│   └── requirement.txt
├── jobs/
│   ├── batch_analysis.py
│   └── streaming_job.py
└── dashboard/
    ├── app.py
    ├── Dockerfile
    └── requirements.txt
```

> **Download dataset**: https://www.kaggle.com/datasets/saurabhbadole/zomato-delivery-operations-analytics-dataset
> Rename file menjadi `zomato.csv` dan taruh di folder `data/`.

---

### Langkah 2 — Jalankan Data Cleaning

```bash
python data/cleaning.py
```

Output yang diharapkan:
```
Dataset berhasil dimuat!
Ukuran dataset: (45584, 20)
...
================================================================================
RINGKASAN CLEANING
================================================================================
Data Original : (45584, 20)
Data Cleaned  : (42493, 20)
Total dihapus : 3091 baris
Data tersisa  : 93.22%
...
✓ Data bersih disimpan ke: data/zomato_clean.csv
```

File `data/zomato_clean.csv` akan terbentuk otomatis. File ini yang digunakan oleh **batch (HDFS) maupun streaming (producer)**.

---

### Langkah 3 — Buat Folder yang Dibutuhkan

**PowerShell (Windows):**
```powershell
New-Item -ItemType Directory -Path checkpoints, dashboard_data, hadoop_config, hadoop_namenode, hadoop_datanode1, hadoop_tmp -Force | Out-Null
```

**Bash (Linux/Mac):**
```bash
mkdir -p checkpoints dashboard_data hadoop_config hadoop_namenode hadoop_datanode1 hadoop_tmp
chmod 777 checkpoints dashboard_data
```

---

### Langkah 4 — Jalankan Semua Service Docker

```bash
docker compose up --build -d
```

Tunggu 30–60 detik, lalu verifikasi:

```bash
docker compose ps
```

Semua container berikut harus berstatus `Up`:

| Container | Port | Fungsi |
|---|---|---|
| `namenode` | 9870 | HDFS NameNode |
| `datanode1` | 9864 | HDFS DataNode |
| `zookeeper` | 2181 | Koordinator Kafka |
| `kafka` | 9092, 29092 | Message broker |
| `kafka-ui` | 8080 | Monitor Kafka |
| `spark-master` | 7077, 8082 | Spark Master |
| `spark-worker` | — | Spark Worker |
| `streamlit` | 8501 | Dashboard |

---

### Langkah 5 — Upload Dataset Bersih ke HDFS

**PowerShell (Windows):**
```powershell
# Buat direktori di HDFS
docker exec -it namenode hdfs dfs -mkdir -p /data

# Copy file cleaned ke container
docker cp data/zomato_clean.csv namenode:/tmp/zomato_clean.csv

# Upload ke HDFS
docker exec -it namenode hdfs dfs -put /tmp/zomato_clean.csv /data/zomato_clean.csv

# Verifikasi
docker exec -it namenode hdfs dfs -ls /data/
```

**Bash (Linux/Mac):** *(sama seperti di atas)*

Output yang diharapkan:
```
Found 1 items
-rw-r--r--   1 root supergroup   13xxxxxx  /data/zomato_clean.csv
```

---

### Langkah 6 — Submit Spark Batch Job

**PowerShell (Windows):**
```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  /opt/jobs/batch_analysis.py
```

**Bash (Linux/Mac):**
```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/jobs/batch_analysis.py
```

Tunggu ~1–2 menit hingga selesai. File `dashboard_data/batch_results.json` akan terbentuk otomatis, dan tab **Batch Insights** di dashboard akan aktif.

---

### Langkah 7 — Jalankan Producer (Terminal Baru)

```bash
cd producer
pip install -r requirement.txt
python producer.py
```

Output:
```
Dataset dimuat: 42493 baris (sudah bersih)
Mengirim ke Kafka topic 'zomato-delivery' setiap 0.5 detik...
[1/42493] ID: DEL001 | City: Metropoliton | Traffic: High | Time: 24.0 min
[2/42493] ID: DEL002 | City: Urban        | Traffic: Jam  | Time: 31.0 min
...
```

Biarkan producer tetap berjalan di terminal ini.

---

### Langkah 8 — Submit Spark Streaming Job (Terminal Baru)

**PowerShell (Windows):**
```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 `
  /opt/jobs/streaming_job.py
```

**Bash (Linux/Mac):**
```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
  /opt/jobs/streaming_job.py
```

Output setiap 10 detik:
```
[Batch 0] orders=20 | avg_time=28.5 min
[Batch 1] orders=20 | avg_time=26.3 min
...
```

---

### Langkah 9 — Buka Dashboard

| URL | Konten |
|---|---|
| http://localhost:8501 | **Streamlit Dashboard** (real-time + batch) |
| http://localhost:8080 | Kafka UI — monitor topic & consumer |
| http://localhost:8082 | Spark Master UI |
| http://localhost:9870 | HDFS NameNode UI |

---

## Expected Output

### Spark Batch Console
```
================================================================================
  ZOMATO DELIVERY — BATCH ANALYSIS
  Total orders: 42493
================================================================================

>>> [1] Avg Delivery Time per Traffic Density
+--------------------+---------------------+------------+
|Road_traffic_density|avg_delivery_time_min|total_orders|
+--------------------+---------------------+------------+
|Jam                 |53.83                |10500       |
|High                |48.50                |13200       |
|Medium              |35.17                |11000       |
|Low                 |22.31                |7793        |
+--------------------+---------------------+------------+

✅ Batch selesai. Hasil disimpan ke /opt/dashboard_data/batch_results.json
```

### Streamlit Dashboard

**Tab Real-time Metrics:**
- 3 KPI card: total orders, avg delivery time, batch ID (auto-refresh 5 detik)
- Bar chart distribusi order per kota
- Bar chart traffic condition (hijau=Low, kuning=Medium, merah=High, ungu=Jam)
- Dual-axis line chart tren order + avg time per batch

**Tab Batch Insights:**
- Horizontal bar chart avg delivery time per traffic density
- Horizontal bar chart avg delivery time per cuaca
- Bar chart avg delivery time per kota
- Heatmap Traffic × Cuaca (merah=lama, hijau=cepat)
- Tabel top 20 delivery person

---

## Findings & Conclusion

*(Isi setelah batch job dijalankan)*

Berdasarkan hasil batch analysis:
- **Traffic density** adalah faktor dominan. Kondisi `Jam` menghasilkan rata-rata waktu delivery ~2× lebih lama dibanding `Low`.
- **Cuaca** berkontribusi namun tidak sebesar traffic. Kondisi `Stormy` dan `Sandstorms` menambah waktu delivery.
- **Kota Metropolitan** memiliki rata-rata waktu tertinggi karena kepadatan jalanan.
- Heatmap menunjukkan kombinasi **Jam + Stormy** adalah skenario terburuk.

Dari streaming real-time:
- Distribusi order per kota relatif merata di setiap batch.
- Traffic `High` dan `Jam` mendominasi, mencerminkan jam sibuk pengiriman.

---

## Known Limitations

- **Dataset statis**: Producer membaca ulang `zomato_clean.csv` yang sama — bukan data live sungguhan, hanya simulasi.
- **Single Kafka broker**: Tanpa replikasi, tidak cocok untuk produksi.
- **HDFS single DataNode**: Tidak ada replikasi data.
- **Streaming tidak berbasis event time**: Menggunakan processing time, bukan `Order_Date` asli.
- **Batch tidak terjadwal otomatis**: Harus dijalankan manual via spark-submit.
- **Improvement**: Dengan lebih banyak waktu, pipeline dapat ditambahkan scheduling (Airflow), multi-node HDFS, dan windowing berbasis event time yang akurat.