# Creditstar Data Engineer Test – Streaming Feature Pipeline

A near-real-time pipeline that detects changed rows in a PostgreSQL CRM
replica, computes client-level features, and writes raw data + features to
MinIO (S3-compatible object storage) as Parquet files.

---

## Quick start

```bash
# 1. Clone and enter the repo
git clone <this-repo> && cd creditstar-test

# 2. Copy env template (defaults work out of the box)
cp .env.example .env

# 3. Start all services
docker compose up --build

# 4. Watch pipeline logs
docker compose logs -f pipeline

# 5. (Optional) inject live changes and watch the pipeline react
docker compose exec pipeline python scripts/simulate_changes.py
```

**MinIO console** → http://localhost:9001 (minioadmin / minioadmin)  
**PostgreSQL** → localhost:5432 / creditstar / postgres

---

## Running the unit tests

```bash
cd pipeline
pip install -r requirements.txt
python -m pytest tests/ -v
```

All 13 tests pass without a live database (mocked cursors).

---

## Repository layout

```
.
├── docker-compose.yml          # Service orchestration
├── .env.example                # Environment variable template
├── database/
│   ├── 01_schema.sql           # Tables, indexes, triggers, watermark table
│   └── 02_seed.sql             # Seed data covering all feature edge cases
└── pipeline/
    ├── Dockerfile
    ├── requirements.txt
    ├── config.py               # Config from env vars
    ├── main.py                 # Entry point
    ├── db/connector.py         # Postgres connection helpers
    ├── storage/minio_client.py # MinIO / S3 write helpers
    ├── features/
    │   └── client_features.py  # Feature computation (Sub Task 2)
    ├── streaming/
    │   └── pipeline.py         # CDC polling loop (Sub Task 1 + 2)
    ├── scripts/
    │   └── simulate_changes.py # Live demo helper
    └── tests/
        └── test_features.py    # 13 unit tests (no DB required)
```

---

## Sub Task 1 – Infrastructure

### What I chose

| Component | Choice | Reason |
|-----------|--------|--------|
| Source DB | PostgreSQL 15 | Requirement; `wal_level=logical` enabled for future true CDC |
| Object storage | MinIO | S3-compatible, runs locally in Docker, zero cost |
| Orchestration | Docker Compose | Reproducible one-command setup; no cloud account needed |
| CDC mechanism | Timestamp polling | Simple, portable, survives container restarts via JSON watermarks |
| Data format | Parquet (pyarrow) | Columnar, compressed, natively readable by Spark/Athena/DuckDB |

### Trade-offs considered

**Polling vs Debezium**  
Timestamp polling is operationally simple and has no schema dependencies, but
it introduces latency equal to the poll interval (10 s by default) and cannot
capture hard DELETEs. Debezium reading the WAL would be sub-second and
capture all change types, but requires Kafka + Zookeeper + Kafka Connect,
making local setup significantly heavier. Given that loan/payment records are
never deleted in the CRM, missing DELETEs is acceptable here.

**MinIO vs cloud S3**  
MinIO is API-compatible with AWS S3, so `storage/minio_client.py` works
unchanged against real S3 by swapping the endpoint env var. This avoids
cloud costs for local development while keeping production the same code
path.

**JSON watermarks vs DB-backed watermarks**  
A JSON file on a Docker volume is simple and survives restarts. The
`pipeline_watermarks` table in the schema was an alternative; I kept it in
the schema for reference (useful if multiple pipeline replicas need
coordination).

### With more time / budget

1. **True CDC via Debezium** – Wire up a Debezium PostgreSQL connector into
   Kafka. The Python consumer subscribes to the topic, which gives sub-second
   latency, reliable DELETE capture, and a replayable event log.

2. **Kafka as the transport layer** – Decouples ingestion from feature
   computation. Multiple consumers (ML training jobs, real-time decision
   engine, monitoring) can read the same topic independently.

3. **Schema Registry + Avro/Protobuf** – Enforces schema evolution contracts
   between producer and consumers.

4. **Dedicated feature store** – Write computed features to Feast or Hopsworks
   instead of raw Parquet so the decision engine can do point-in-time correct
   lookups.

5. **Airflow / Prefect for orchestration** – Replace the `while True` loop
   with a proper DAG that has retry logic, SLA alerting, and backfill support.

6. **Partitioning strategy** – Partition by `client_id % N` buckets to make
   downstream joins efficient at scale.

---

## Sub Task 2 – Features

All feature logic lives in `pipeline/features/client_features.py`.

### Feature definitions

#### `client.paid_loans.count`

```python
SELECT COUNT(*) FROM loans
WHERE client_id = %s AND status = 'paid'
```

**Assumptions:**
- Only loans with `status = 'paid'` count. `'defaulted'` and `'written_off'`
  loans are not considered paid.
- Returns `0` for clients with no history (brand-new clients).

---

#### `client.days_since_last_late_payment.count`

```python
SELECT MAX(payment_date)
FROM   payments
WHERE  client_id = %s
  AND  is_late = TRUE
  AND  payment_date IS NOT NULL
```

Days = `today (UTC) − last_late_payment_date`.

**Assumptions:**
- `is_late = TRUE AND payment_date IS NOT NULL` means the payment was
  eventually made but after its due date. Scheduled future payments that are
  not yet past due are excluded.
- Returns `None` (null) when the client has no late payment on record.
  Downstream consumers should treat `None` as "no late payment ever", not as
  zero – a client with `days = None` is better than one with `days = 5000`.
- Days are integer calendar days, not fractional.

---

#### `client.profit_in_last_90_days.rate`

```sql
SELECT
    COALESCE(SUM(p.interest_amount), 0.0)  AS total_interest_received,
    SUM(l.amount)                           AS total_loan_amount
FROM   loans l
LEFT JOIN payments p
       ON p.loan_id = l.id AND p.payment_date IS NOT NULL
WHERE  l.client_id = %s
  AND  l.issued_at >= NOW() - INTERVAL '90 days'
```

Rate = `total_interest_received / total_loan_amount`.

**Assumptions:**
- "Issued in the last 90 days" means `loan.issued_at >= NOW() − 90 days`.
- "Interest payment received" means a payment row where `payment_date IS NOT
  NULL` (cash has actually arrived). Scheduled but unpaid instalments are
  excluded.
- Returns `None` when the client has no loans issued in the last 90 days
  (feature is undefined, not zero – important for model training).
- Returns `0.0` when loans exist in the window but no payments have been
  received yet (denominator > 0, numerator = 0 – perfectly valid ratio).
- Returns `None` if `sum(loan.amount) = 0` to guard against division by zero
  (data quality edge case).

### Seed data edge cases covered

| Client | paid_loans | days_since_late | profit_rate_90d |
|--------|-----------|-----------------|-----------------|
| Alice  | 2         | ~30             | `15.34 / 800` ≈ 0.019 |
| Bob    | 0         | None            | None (loan outside window) |
| Carol  | 1         | ~200            | None (no loans in window) |
| David  | 0         | None            | None (no loans at all) |
| Eve    | 0         | None            | 0.0 (loan in window, no payment yet) |

---

## Object storage layout

```
creditstar-features/
├── raw/
│   ├── clients/year=…/month=…/day=…/<time>_<n>.parquet
│   ├── loan_applications/…
│   ├── loans/…
│   └── payments/…
└── features/
    └── client_features/year=…/month=…/day=…/<time>_<n>.parquet
```

Each Parquet file for `client_features` contains:

| Column | Type | Notes |
|--------|------|-------|
| `client_id` | int | |
| `paid_loans_count` | int | |
| `days_since_last_late_payment` | float / null | |
| `profit_in_last_90_days_rate` | float / null | |
| `computed_at` | ISO-8601 string | UTC timestamp of computation |

---

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_HOST` | `localhost` | |
| `POSTGRES_PORT` | `5432` | |
| `POSTGRES_DB` | `creditstar` | |
| `POSTGRES_USER` | `postgres` | |
| `POSTGRES_PASSWORD` | `postgres` | |
| `MINIO_ENDPOINT` | `localhost:9000` | |
| `MINIO_ACCESS_KEY` | `minioadmin` | |
| `MINIO_SECRET_KEY` | `minioadmin` | |
| `MINIO_BUCKET` | `creditstar-features` | |
| `MINIO_SECURE` | `false` | Set `true` for TLS |
| `POLL_INTERVAL_SECONDS` | `10` | CDC poll frequency |
| `LOG_LEVEL` | `INFO` | `DEBUG` for verbose output |
