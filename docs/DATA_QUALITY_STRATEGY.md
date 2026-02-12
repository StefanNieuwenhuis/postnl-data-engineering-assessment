# Data Quality Strategy

This document describes the data quality design decisions and approach across the pipeline.

## Design Principles

1. **Config-driven**: Quality rules are defined in `config/config.yaml` to avoid hardcoding.
2. **Layer-appropriate**: Bronze preserves raw data; Silver enforces quality; Gold assumes cleaned data.
3. **Traceability**: Bronze metadata (`ingestion_timestamp`, `run_id`, `source_file`) supports audit and replay.

## Current Implementation

### Bronze

| Mechanism | Description |
|-----------|-------------|
| **Schema enforcement** | All datasets use schemas from `schema_registry`; no `inferSchema` for raw data. |
| **Format handling** | `header=True` only for CSV; JSON uses schema without header. |
| **Metadata** | Ingestion metadata added for traceability; raw columns preserved. |

### Silver

| Mechanism | Description | Config |
|-----------|-------------|--------|
| **Deduplication** | One row per business key; watermark on `ingestion_timestamp` (10 min). | `dedupe_keys` |
| **Missing values** | Rows with NULL in required columns → quarantine when `quarantine: true`; else dropped. | `required_columns`, `quarantine` |
| **Timestamp normalization** | Multiple formats supported; `norm_*` columns added. | `date_columns`, `datetime_columns` |

### Gold

| Mechanism | Description |
|-----------|-------------|
| **Division guards** | `F.when` guards avoid division by zero in `efficiency_score`, `delay_pct`. |
| **KPI documentation** | Units and edge cases documented in `docs/KPI_DEFINITIONS.md`. |


## Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Drop invalid rows** | Silver drops rows with NULL in `required_columns` instead of quarantining. This keeps silver simple and avoids a separate quarantine path. |
| **Merge keys over append** | Delta MERGE with business keys ensures idempotency; re-runs do not create duplicates. |
| **Schema registry** | Centralized schemas enable consistency and early validation at ingest. |
| **No external validation framework** | No Great Expectations or similar; schema + config-driven rules suffice for current scope. |
