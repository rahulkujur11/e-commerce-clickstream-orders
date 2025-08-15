# E-Commerce Clickstream Orders – Real-Time Streaming Pipeline
You’re the lone data engineer for a tiny shop. Product wants near-real-time “What’s happening right now?” plus reliable daily aggregates.

A fully containerized **real-time data pipeline** that processes simulated e-commerce clickstream and order events using:

- **Apache Kafka** – event ingestion
- **Apache Spark Structured Streaming** – real-time ETL
- **Delta Lake** – Bronze & Silver storage layers
- **Docker Compose** – local orchestration

---

## 📐 Architecture

```mermaid
flowchart LR
    subgraph Generator["Event Generator"]
        E1[JSON Clickstream Event Producer]
    end

    subgraph Kafka["Kafka Cluster"]
        T1[(Topic: events.raw)]
    end

    subgraph Spark["Spark Structured Streaming"]
        S1[Ingest from Kafka]
        S2[Bronze Writer<br>(Raw Delta)]
        S3[Silver Writer<br>(Clean & Enriched Delta)]
    end

    subgraph DeltaLake["Delta Lake Storage"]
        B[Bronze Tables]
        Si[Silver Tables]
    end

    E1 --> T1
    T1 --> S1
    S1 --> S2
    S1 --> S3
    S2 --> B
    S3 --> Si
```

---

## 🗂 Data Layers

| Layer  | Description |
|--------|-------------|
| **Bronze** | Raw, unmodified Kafka events stored as-is in Delta format |
| **Silver** | Cleaned, parsed, and enriched datasets ready for analytics |

---

## ⚙️ Prerequisites

- **Docker** & **Docker Compose**
- **Python 3.10+** (for the event generator)
- **Git**

---

## 🚀 Setup & Run

### 1️⃣ Clone Repository
```bash
git clone https://github.com/your-user/e-commerce-clickstream-orders.git
cd e-commerce-clickstream-orders
```

### 2️⃣ Start Kafka & Spark Cluster
```bash
docker compose up -d
```
Starts:
- **Zookeeper**
- **Kafka**
- **Spark Master**
- **Spark Worker(s)**

---

### 3️⃣ Verify Kafka Topic
```bash
docker compose exec kafka kafka-topics.sh   --bootstrap-server kafka:9092 --list
```
Expected:
```
events.raw
```

---

### 4️⃣ Start Event Generator
```bash
cd generators
python -m venv .venv
source .venv/bin/activate   # Linux/macOS
# .\.venv\Scripts\activate  # Windows PowerShell
python event_gen.py --eps 100
```
**`--eps`** controls **events per second**.

---

### 5️⃣ Run Spark Structured Streaming Job
```bash
docker compose exec spark-master /opt/bitnami/spark/bin/spark-submit   --master spark://spark-master:7077   --conf spark.executor.cores=1   --conf spark.cores.max=2   --executor-memory 1g   --driver-memory 1g   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1   /opt/streaming/main.py
```

---

## 📁 Data Output

| Layer  | Path |
|--------|------|
| Bronze | `./data/bronze/events/event_date=YYYY-MM-DD/` |
| Silver | `./data/silver/{page_view,add_to_cart,order_placed}/` |

---

## 🔍 Checking Data
```bash
ls data/bronze/events/event_date=$(date +%Y-%m-%d)
```

---

## 📊 Monitoring

| Component | URL |
|-----------|-----|
| Spark Master UI | [http://localhost:8080](http://localhost:8080) |
| Spark Worker UI | [http://localhost:8081](http://localhost:8081) |

---

## 🛠 Troubleshooting

| Issue | Cause | Fix |
|-------|-------|-----|
| `Initial job has not accepted any resources` | No Spark workers registered | Check Spark UI → Restart workers |
| `Permission denied` writing to `/opt/data` | Docker volume permissions on Windows | Run container with `user: "0:0"` in `docker-compose.yml` |
| Bronze/Silver empty | No events being produced | Start `event_gen.py` **before** Spark job |

---

## 📈 Next Steps

- Add **Gold Layer** (aggregated metrics: daily active users, orders by category)
- Integrate **BI Tools** (Tableau, Power BI) with Silver tables
- Deploy to **cloud** with managed Kafka/Spark services

---

## 📜 License
MIT – see [LICENSE](LICENSE)
