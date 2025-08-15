# E-Commerce Clickstream Orders – Real-Time Streaming Pipeline
You’re the lone data engineer for a tiny shop. Product wants near-real-time “What’s happening right now?” plus reliable daily aggregates.

This project simulates an **end-to-end streaming data pipeline** for e-commerce clickstream events (page views, cart actions, orders) using:

- **Apache Kafka** – Event ingestion
- **Apache Spark Structured Streaming** – Real-time processing
- **Delta Lake** – Storage in Bronze/Silver layers
- **Docker Compose** – Local orchestration

---

## 📊 Architecture

```mermaid
flowchart LR
    subgraph Generator["Event Generator"]
        E1[JSON Event Producer]
    end

    subgraph KafkaCluster["Kafka Cluster"]
        K1[(Topic: events.raw)]
    end

    subgraph SparkCluster["Spark Structured Streaming"]
        S1[Ingest Kafka Stream]
        S2[Bronze Layer<br/>(Raw Data)]
        S3[Silver Layer<br/>(Clean/Enriched)]
    end

    subgraph Storage["Delta Lake Storage"]
        B[Bronze Delta Tables]
        Si[Silver Delta Tables]
    end

    E1 --> K1
    K1 --> S1
    S1 --> S2
    S1 --> S3
    S2 --> B
    S3 --> Si
