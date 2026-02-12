# Architecture Diagram

![Architecture Diagram](../assets/architecture_diagram.png)

## 1. Overview

The data product ingests transport and delivery data from multiple sources, processes it through a medallion architecture (Bronze => Silver => Gold), and produces KPIs for route performance analysis.

| Component | Technology |
|-----------|------------|
| Storage | AWS S3 (Delta Lake format) |
| Compute & Analytics | Databricks (Spark, Delta) |
| Governance | Unity Catalog + AWS Lake Formation |
| Data Quality | Great Expectations |
| Orchestration | Databricks Workflows (primary) / Airflow MWAA (alternative) |

### General Architecture Diagram

```mermaid
flowchart TB
    subgraph Sources["Data Sources"]
        CSV[("CSV\nShipments, Vehicles")]
        JSON[("JSON\nRoutes, Weather")]
    end

    subgraph AWS["AWS Cloud"]
        subgraph S3["S3 Data Lake"]
            LANDING[("Landing Zone")]
            BRONZE[("Bronze\nRaw + Metadata")]
            SILVER[("Silver\nCleaned")]
            GOLD[("Gold\nRoute Performance KPIs")]
        end

        subgraph Databricks["Databricks"]
            SPARK[("Spark / Delta")]
        end

        subgraph Gov["Governance"]
            UC[("Unity Catalog")]
            LF[("Lake Formation")]
        end

        subgraph Ops["Orchestration & Quality"]
            ORCH[("Workflows / MWAA")]
            GX[("Great Expectations")]
        end
    end

    subgraph Consumers["Consumers"]
        BI[("BI / Dashboards")]
        ML[("ML / Analytics")]
    end

    CSV --> LANDING
    JSON --> LANDING
    LANDING --> BRONZE
    BRONZE --> SILVER
    SILVER --> GOLD
    GOLD --> BI
    GOLD --> ML

    SPARK -.-> BRONZE
    SPARK -.-> SILVER
    SPARK -.-> GOLD
    ORCH -.-> SPARK
    GX -.-> BRONZE
    GX -.-> SILVER
    GX -.-> GOLD
    UC -.-> S3
    LF -.-> S3
```

#### How to Read the Diagram

| Element | Meaning |
|---------|---------|
| **Solid arrows (→)** | Primary data flow; data moves from source to landing, through bronze/silver/gold, and to consumers |
| **Dotted arrows (-·→)** | Supporting or control flows; compute, orchestration, validation, and governance apply to the data without being part of the main path |

#### Data Flow

1. **Sources** — Structured (CSV) and semi-structured (JSON) data from ERP systems, telematics, and external APIs land in S3.
2. **Landing Zone** — Raw files are dropped here; no transformation. Acts as a staging area before ingestion.
3. **Bronze** — Raw data with metadata (ingestion timestamp, source file, run ID). Corrupt records go to quarantine.
4. **Silver** — Deduplicated, cleaned, and normalized. Invalid rows (e.g. missing required columns) are quarantined.
5. **Gold** — Curated KPIs (delay_minutes, emission_kg, efficiency_score, etc.) joined from shipments, routes, vehicles, and weather.
6. **Consumers** — BI tools, dashboards, and ML models consume the gold layer.

#### Supporting Components

| Component | Role |
|-----------|------|
| **Databricks (Spark / Delta)** | Compute engine for all transformations; reads, writes, and processes Delta tables in S3 |
| **Orchestration (Workflows / MWAA)** | Schedules and triggers jobs; Bronze → Silver → Gold; handles retries and failures |
| **Great Expectations** | Validates data at each layer; checks schema, nulls, ranges; fails jobs on validation errors |
| **Unity Catalog** | Central metastore; table-level access control; lineage and audit |
| **Lake Formation** | S3-level permissions; resource tagging; secure sharing across AWS accounts |
