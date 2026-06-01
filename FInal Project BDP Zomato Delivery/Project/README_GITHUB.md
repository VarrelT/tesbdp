# 🛵 Zomato Delivery - Big Data Processing Pipeline

Final Project: Batch + Real-time Analytics on Zomato Delivery Dataset

---

## 📋 Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Prerequisites](#prerequisites)
3. [Project Structure](#project-structure)
4. [Setup Instructions](#setup-instructions)
5. [Running the Pipeline](#running-the-pipeline)
6. [Dashboard Access](#dashboard-access)
7. [Troubleshooting](#troubleshooting)

---

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    ZOMATO DELIVERY BDP PIPELINE                 │
└─────────────────────────────────────────────────────────────────┘

┌──────────────┐
│   Raw Data   │  → Zomato Dataset.csv (45,584 rows)
└──────────────┘
       │
       ↓
┌──────────────────────┐
│  Data Cleaning       │  → pandas: outlier removal, type conversion
│  (cleaning.py)       │  → Output: zomato_clean.csv (42,493 rows)
└──────────────────────┘
       │
       ├─────────────────────┬──────────────────────┐
       ↓                     ↓                      ↓
  ┌─────────────┐   ┌───────────────┐   ┌────────────────┐
  │   HDFS      │   │  Kafka Topic  │   │   Local CSV    │
  │ /data/      │   │ zomato-       │   │ (for Producer) │
  │ zomato_     │   │ delivery      │   │                │
  │ clean.csv   │   │               │   │                │
  └─────────────┘   └───────────────┘   └────────────────┘
       │                    │                     │
       ├────────────────────┤                     │
       ↓                    ↓                     ↓
  ┌────────────────────────────────────────────────────┐
  │         PRODUCER (HOST)                            │
  │  - Simulates real-time delivery stream            │
  │  - Reads zomato_clean.csv                         │
  │  - Sends 1 row per 0.5 seconds to Kafka          │
  │  - Bootstrap: localhost:29092                     │
  └────────────────────────────────────────────────────┘
       │
       ├─────────────────────────────────────────────┬──────────┐
       ↓                                             ↓          ↓
  ┌─────────────────────┐            ┌──────────────────────────────┐
  │ BATCH JOB           │            │ STREAMING JOB (Every 10s)    │
  │ (batch_analysis.py) │            │ (streaming_job.py)           │
  │                     │            │                              │
  │ - Read from HDFS    │            │ - Subscribe to Kafka topic   │
  │ - Full dataset      │            │ - Mini-batch aggregation     │
  │   analysis          │            │ - Compute KPIs               │
  │ - Generate JSON     │            │ - Write snapshots            │
  │   report            │            │                              │
  └─────────────────────┘            └──────────────────────────────┘
       │                                  │              │
       ↓                                  ↓              ↓
  ┌─────────────────────────────────────────────────────────────────┐
  │           DOCKER VOLUME: dashboard_data/                        │
  │                                                                  │
  │  ├─ batch_results.json (Traffic, Weather, City, Heatmap, Top20)│
  │  ├─ latest_snapshot.json (Current batch KPIs)                 │
  │  └─ history.jsonl (Streaming history, append-only)            │
  └─────────────────────────────────────────────────────────────────┘
                              │
                              ↓
                    ┌──────────────────┐
                    │  STREAMLIT DASHBOARD
                    │  http://localhost:8501
                    │
                    │  TAB 1: Real-time Metrics
                    │  - Live KPI cards
                    │  - City distribution
                    │  - Traffic condition status
                    │  - Historical trend chart
                    │
                    │  TAB 2: Batch Insights
                    │  - Traffic impact analysis
                    │  - Weather impact analysis
                    │  - City-wise performance
                    │  - Traffic×Weather heatmap
                    │  - Top 20 delivery persons
                    └──────────────────┘
```

---

## 📦 Prerequisites

### System Requirements
- **Docker & Docker Compose** (v20.10+)
- **Windows PowerShell 5.1+** or Bash/Linux
- **Disk space**: ~5GB (for Docker images + data)
- **RAM**: 4GB minimum

### Installed Tools
- Docker Desktop with compose plugin
- PowerShell (Windows) or Bash (Linux/Mac)

---

## 📁 Project Structure

```
Project/
├── docker-compose.yml              # Orchestrates all 8 containers
├── README.md                        # This file
├── data/
│   ├── cleaning.py                 # Data preprocessing script
│   └── Zomato Dataset.csv          # Raw input data (45,584 rows)
├── dashboard/
│   ├── Dockerfile                  # Streamlit container image
│   ├── app.py                       # Streamlit dashboard application
│   └── requirements.txt             # Python dependencies
├── jobs/
│   ├── batch_analysis.py           # Spark batch job (full dataset analysis)
│   └── streaming_job.py            # Spark streaming job (Kafka consumer)
├── Producer/
│   ├── producer.py                 # Kafka producer (simulates real-time data)
│   └── requirement.txt              # Producer dependencies
├── hadoop_config/
│   ├── core-site.xml               # Hadoop core configuration
│   └── hdfs-site.xml               # HDFS-specific configuration
├── start-hdfs.sh                    # NameNode initialization script
├── init-datanode.sh                 # DataNode initialization script
└── .gitignore                       # Git ignore file

Generated at runtime (do NOT commit):
├── hadoop_namenode/                # HDFS NameNode persistent storage
├── hadoop_datanode1/               # HDFS DataNode data blocks
└── dashboard_data/                 # Spark output files (JSON)
    ├── batch_results.json
    ├── latest_snapshot.json
    └── history.jsonl
```

---

## 🚀 Setup Instructions

### Step 1: Clone Repository

```bash
git clone <your-repo-url>
cd "Project"
```

### Step 2: Prepare Environment

**Windows PowerShell:**
```powershell
# Create required directories
New-Item -ItemType Directory -Force -Path "hadoop_namenode"
New-Item -ItemType Directory -Force -Path "hadoop_datanode1"
New-Item -ItemType Directory -Force -Path "dashboard_data"

# Set permissions (if needed)
icacls "hadoop_namenode" /grant:r "$env:USERNAME:(OI)(CI)F"
icacls "hadoop_datanode1" /grant:r "$env:USERNAME:(OI)(CI)F"
icacls "dashboard_data" /grant:r "$env:USERNAME:(OI)(CI)F"
```

**Linux/Mac:**
```bash
mkdir -p hadoop_namenode hadoop_datanode1 dashboard_data
chmod 777 hadoop_namenode hadoop_datanode1 dashboard_data
```

### Step 3: Start Docker Services

```bash
docker compose up -d
```

This starts 8 containers:
- **namenode** (HDFS NameNode)
- **datanode1** (HDFS DataNode)
- **zookeeper** (Kafka coordinator)
- **kafka** (Message broker)
- **kafka-ui** (Kafka admin dashboard)
- **spark-master** (Spark master)
- **spark-worker** (Spark worker)
- **streamlit** (Dashboard)

Verify all containers running:
```bash
docker compose ps
```

### Step 4: Prepare Data in HDFS

```bash
# Clean the data
docker exec spark-master python3 /opt/data/cleaning.py

# Upload to HDFS
docker exec -it namenode hdfs dfs -put /opt/data/zomato_clean.csv /data/
```

Verify upload:
```bash
docker exec -it namenode hdfs dfs -ls /data/
```

---

## ▶️ Running the Pipeline

### Step 1: Start Batch Analysis Job

```bash
# Windows PowerShell
docker exec -it spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  /opt/jobs/batch_analysis.py

# Linux/Mac
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/jobs/batch_analysis.py
```

**Expected output**: `Batch analysis completed! Output: /opt/dashboard_data/batch_results.json`

Wait ~2-3 minutes for completion.

### Step 2: Start Producer (Real-time Data Stream)

**From HOST machine (NOT in Docker)**:

```bash
# First, navigate to project directory
cd Producer

# Windows PowerShell
python producer.py

# Linux/Mac
python3 producer.py
```

**Expected output**: 
```
Kafka server: localhost:29092
Connected! Sending data...
Message 1 sent...
Message 2 sent...
```

### Step 3: Start Streaming Job

**In a new terminal**:

```bash
# Windows PowerShell
docker exec -it spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 `
  /opt/jobs/streaming_job.py

# Linux/Mac
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
  /opt/jobs/streaming_job.py
```

**Expected output**:
```
Waiting for trigger...
Streaming job is running. Press Ctrl+C to stop.
Batch 1 processed: 58 records...
Batch 2 processed: 61 records...
```

---

## 📊 Dashboard Access

### Real-time Dashboard

**URL**: `http://localhost:8501`

**Tab 1: Real-time Metrics** (updates every 10 seconds)
- 📈 **KPI Cards**: Total Orders, Avg Delivery Time, Batch ID
- 📍 **City Distribution**: Bar chart of orders by city
- 🚦 **Traffic Status**: Current traffic condition breakdown
- 📉 **Historical Trend**: Line chart of total orders & avg time over time

**Tab 2: Batch Insights** (static analysis of full dataset)
- 📦 **Dataset Overview**: Total orders = 42,493
- 🚦 **Traffic Impact**: Bar chart showing avg delivery time per traffic density
- ☁️ **Weather Impact**: Bar chart showing avg delivery time per weather condition
- 🏙️ **City Performance**: Bar chart of avg delivery time per city
- 🔥 **Traffic × Weather Heatmap**: 2D matrix showing combined impact
- 👤 **Top 20 Delivery Persons**: Table with delivery count, avg time, rating

### Other Dashboards

- **Kafka UI**: `http://localhost:8080`
- **Spark Master UI**: `http://localhost:8080` (port may vary)

---

## 🔧 Troubleshooting

### Issue 1: Batch Insights Tab is Blank

**Symptom**: Real-time tab shows data, but Batch Insights tab is empty

**Solution**:
```bash
# Check if batch_results.json was created
docker exec streamlit ls -la /app/dashboard_data/

# If missing, run batch job again
docker exec -it spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  /opt/jobs/batch_analysis.py

# Clear Streamlit cache and refresh browser
docker exec streamlit rm -rf /root/.streamlit/cache

# Hard refresh browser (Ctrl+Shift+R) and revisit http://localhost:8501
```

### Issue 2: Producer Not Sending Data

**Symptom**: Producer script runs but says "Connection refused"

**Solution**:
```bash
# Verify Kafka is running
docker compose ps kafka

# Check Kafka logs
docker logs kafka

# Verify bootstrap server
docker exec kafka kafka-broker-api-versions.sh --bootstrap-server kafka:9092

# If needed, restart Kafka
docker compose restart kafka
```

### Issue 3: HDFS Permission Denied

**Symptom**: `Permission denied` when uploading files to HDFS

**Solution**:
```bash
# Reset HDFS permissions
docker exec -it namenode hdfs dfs -chmod 777 /data

# Or recreate directory
docker exec -it namenode hdfs dfs -rm -r /data
docker exec -it namenode hdfs dfs -mkdir -p /data
docker exec -it namenode hdfs dfs -chmod 777 /data
```

### Issue 4: Streaming Job Not Consuming Messages

**Symptom**: Streaming job runs but shows 0 records processed

**Solution**:
```bash
# Producer must run BEFORE streaming job (to create topic)
# Check if topic exists
docker exec -it kafka kafka-topics.sh --list --bootstrap-server kafka:9092

# If missing, ensure producer ran first
# Restart both: producer → streaming job

# Check message count
docker exec -it kafka kafka-run-class.sh kafka.tools.JmxTool --object-name kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec
```

### Issue 5: Docker Containers Not Starting

**Symptom**: `docker compose up -d` returns error

**Solution**:
```bash
# Check Docker daemon
docker ps

# If Docker not running: start Docker Desktop

# View detailed logs
docker compose logs

# Clean start (warning: removes all container data)
docker compose down -v
docker compose up -d
```

---

## 📊 Expected Data Volumes

| Component | Records | Size |
|-----------|---------|------|
| Raw CSV | 45,584 | ~8 MB |
| Cleaned CSV | 42,493 | ~7.5 MB |
| Batch Results JSON | 1 | ~8 KB |
| Latest Snapshot JSON | 1 (updates every 10s) | ~1 KB |
| History JSONL | Grows over time | ~1-5 KB per batch |

---

## 🎯 Key Technologies

| Component | Technology | Version |
|-----------|-----------|---------|
| **Data Processing** | Apache Spark | 4.0.0 |
| **Distributed Storage** | Hadoop HDFS | 3.3.5 |
| **Message Queue** | Apache Kafka | 7.5.0 |
| **Coordination** | Apache Zookeeper | 7.5.0 |
| **Dashboard** | Streamlit | Latest |
| **Data Wrangling** | pandas, numpy | Latest |
| **Visualization** | Plotly | Latest |

---

## 📝 Data Schema

### Cleaned Dataset Columns

```
Order_ID, Customer_ID, Delivery_person_ID, Delivery_person_Age,
Delivery_person_Ratings, Restaurant_latitude, Restaurant_longitude,
Delivery_location_latitude, Delivery_location_longitude,
Order_Date (parsed), Order_time, Type_of_vehicle, Road_traffic_density,
Weather_conditions, Vehicle_condition, multiple_deliveries,
delivery_person_home_location_latitude, delivery_person_home_location_longitude,
City, Time_taken (min) [TARGET]
```

### Batch Results JSON Schema

```json
{
  "total_orders": 42493,
  "by_traffic": [
    {
      "Road_traffic_density": "Low|Medium|High|Jam|null",
      "avg_delivery_time_min": float,
      "total_orders": int
    }
  ],
  "by_weather": [...],
  "by_city": [...],
  "heatmap": [...],
  "by_person": [
    {
      "Delivery_person_ID": "string",
      "total_deliveries": int,
      "avg_delivery_time_min": float,
      "avg_rating": float
    }
  ]
}
```

---

## 🤝 Contributing

For improvements or bug fixes:
1. Fork the repository
2. Create a feature branch
3. Make changes
4. Submit a pull request

---

## 📄 License

This project is part of a Big Data Processing final project.

---

## 👥 Authors

- Semester 6 BDP Final Project Team

---

## ❓ FAQ

**Q: How long does batch analysis take?**  
A: ~2-3 minutes for 42,493 records on Spark local mode

**Q: Can I run this without Docker?**  
A: Not recommended; requires Hadoop, Kafka, Spark, Zookeeper setup locally

**Q: How much disk space do I need?**  
A: ~5GB for Docker images + data storage

**Q: Can I modify the pipeline to use different data?**  
A: Yes; update `data/Zomato Dataset.csv` and re-run cleaning.py

---

**Last Updated**: June 2026
