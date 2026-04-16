# Creditstar Data Engineer Test – Streaming Feature Pipeline

A near-real-time pipeline that detects changed rows in a PostgreSQL CRM
replica (`de_test_materials`), computes client-level features, and writes raw
data + features to MinIO (S3-compatible object storage) as Parquet files.

---

## Quick start

```bash
# 1. Clone and enter the repo
git clone <this-repo> && cd creditstar-test

# 2. Copy env template (defaults work out of the box)
cp .env.example .env

# 3. Start all services  (first run restores the dump – takes ~30 s)
docker compose up --build

# 4. Watch pipeline logs
docker compose logs -f pipeline

# 5. (Optional) inject live changes and watch the pipeline react
docker compose exec pipeline python scripts/simulate_changes.py
```

**MinIO console** → http://localhost:9001 (minioadmin / minioadmin)  
**PostgreSQL**    → localhost:5432 / de_test_materials / postgres

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
├── docker-compose.yml
├── .env.example
├── database/
│   ├── de_test_task_db          # PostgreSQL custom-format dump (binary, ~6 MB)
│   ├── 00_restore.sh            # Restores dump on first container start
│   └── 01_pipeline_setup.sql    # Adds watermark table + indexes post-restore
└── pipeline/
    ├── Dockerfile
    ├── requirements.txt
    ├── config.py
    ├── main.py
    ├── db/connector.py
    ├── storage/minio_client.py
    ├── features/
    │   └── client_features.py   # Feature computation (Sub Task 2)
    ├── streaming/
    │   └── pipeline.py          # CDC polling loop (Sub Task 1 + 2)
    ├── scripts/
    │   └── simulate_changes.py  # Live demo helper
    └── tests/
        └── test_features.py     # 13 unit tests (no DB required)
```

---

## Sub Task 1 – Infrastructure

### What I chose

| Component | Choice | Reason |
|-----------|--------|--------|
| Source DB | PostgreSQL 15 + dump restore | The provided dump targets PostgreSQL |
| Object storage | MinIO | S3-compatible, zero cost, runs locally in Docker |
| Orchestration | Docker Compose | One-command reproducible setup |
| CDC mechanism | Date-watermark polling | Simple, no extra infrastructure |
| Output format | Parquet (pyarrow) | Columnar, compressed, works with Spark/Athena/DuckDB |

### Trade-offs considered

**Polling vs Debezium**  
The CRM schema uses DATE columns (`updated_on`, `created_on`) for tracking
changes — day-level granularity.  Polling on these columns is simple and
requires no schema migration.  A sub-second CDC solution (Debezium reading
PostgreSQL WAL) would require adding TIMESTAMPTZ `updated_at` columns and
setting up Kafka + Kafka Connect, which adds significant operational
complexity for a local setup.

**Date granularity**  
Because `loan.updated_on` is a DATE (not TIMESTAMPTZ), the polling window
is one calendar day.  If two updates happen on the same day and the second
one falls after our watermark was advanced, it will be caught in the next
cycle.  In practice this means ~day-level latency rather than the configured
poll interval.  A proper migration adding `TIMESTAMPTZ updated_at` with a
trigger would fix this.

**MinIO vs cloud S3**  
`storage/minio_client.py` is S3 API-compatible.  Switching to AWS S3 in
production is a one-line env-var change (`MINIO_ENDPOINT=s3.amazonaws.com`).

### With more time / budget

1. **Add TIMESTAMPTZ `updated_at` columns** – migrate `loan`, `payment`, `user`
   to use proper timestamps so sub-minute CDC becomes possible.

2. **Debezium + Kafka** – true WAL-based CDC; captures every row change
   including hard deletes, sub-second latency, replayable event log.

3. **Schema Registry (Avro/Protobuf)** – enforce schema evolution contracts
   between producer and consumers.

4. **Feature store** (Feast / Hopsworks) – replace raw Parquet with a
   point-in-time correct feature store so the decision engine can do
   low-latency lookups during scoring.

5. **Airflow / Prefect** – replace the `while True` loop with a DAG that
   provides retry logic, SLA alerting, and backfill.

---

## Sub Task 2 – Features

All logic is in `pipeline/features/client_features.py`.

### Real database schema (de_test_materials)

| Table | Key columns |
|-------|-------------|
| `user` | `id`, `created_on`, `first_name`, `last_name`, `birth_date`, `personal_code` |
| `loan` | `id`, `client_id`, `amount`, `status` ('paid'\|'overdue'\|'application'), `created_on`, `duration`, `matured_on`, `updated_on` |
| `payment` | `id`, `loan_id`, `amount`, `principle`, `interest`, `status`, `created_on` |

---

### `client.paid_loans.count`

```sql
SELECT COUNT(*) FROM loan
WHERE client_id = %s AND status = 'paid'
```

**Assumptions:**
- Only `status = 'paid'` is counted.  `'overdue'` and `'application'` are excluded.
- Returns `0` for clients with no paid loans.

---

### `client.days_since_last_late_payment.count`

```sql
SELECT MAX(updated_on) AS last_late_date
FROM   loan
WHERE  client_id = %s
  AND  (
        status = 'overdue'
        OR (status = 'paid' AND updated_on > matured_on)
       )
```

Days = `TODAY (UTC) − last_late_date`.

**Assumptions:**
- The `payment` table is empty in this dump; late-payment information is
  inferred from the `loan` table.
- "Late" = loan went overdue (`status = 'overdue'`) OR loan was paid after
  its maturity date (`status = 'paid' AND updated_on > matured_on`).
- `loan.updated_on` is used as the event date (last status change).
- Returns `None` when the client has no overdue or late-paid loans on record.
  Downstream consumers should treat `None` as "no late payment ever", not zero.

---

### `client.profit_in_last_90_days.rate`

```sql
SELECT
    COALESCE(SUM(p.interest), 0.0) AS total_interest_received,
    SUM(l.amount)                  AS total_loan_amount
FROM   loan l
LEFT JOIN payment p ON p.loan_id = l.id
WHERE  l.client_id = %s
  AND  l.created_on >= CURRENT_DATE - INTERVAL '90 days'
```

Rate = `total_interest_received / total_loan_amount`.

**Assumptions:**
- "Issued in last 90 days" = `loan.created_on >= TODAY − 90 days`.
- "Interest received" = `payment.interest` for payments linked to those loans.
- Returns `None` when no loans were issued in the last 90 days (feature is
  undefined, not zero — important for model training).
- Returns `0.0` when loans exist in the window but no payments yet.
- Returns `None` if `SUM(loan.amount) = 0` (division-by-zero guard).
- **Note:** the test dataset covers 2019–2020; running today (2026+) means this
  feature returns `None` for all clients.  This is correct — the feature
  is genuinely unavailable for historical data outside the rolling window.

---

## CDC watermark columns

| Table | Change-detection column | Granularity |
|-------|------------------------|-------------|
| `user` | `created_on` | Day (inserts only) |
| `loan` | `updated_on` | Day (inserts + updates) |
| `payment` | `created_on` | Day (inserts only) |

Watermarks are stored in `/app/state/watermarks.json` (Docker volume) and
survive container restarts.

---

## Object storage layout

```
creditstar-features/
├── raw/
│   ├── user/year=…/month=…/day=…/<time>_<n>.parquet
│   ├── loan/…
│   └── payment/…
└── features/
    └── client_features/year=…/month=…/day=…/<time>_<n>.parquet
```

`client_features` Parquet schema:

| Column | Type | Notes |
|--------|------|-------|
| `client_id` | int | |
| `paid_loans_count` | int | 0 if no paid loans |
| `days_since_last_late_payment` | float / null | null = no late payment on record |
| `profit_in_last_90_days_rate` | float / null | null = no loans in window |
| `computed_at` | ISO-8601 string | UTC timestamp |

---

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_HOST` | `localhost` | |
| `POSTGRES_PORT` | `5432` | |
| `POSTGRES_DB` | `de_test_materials` | |
| `POSTGRES_USER` | `postgres` | |
| `POSTGRES_PASSWORD` | `postgres` | |
| `MINIO_ENDPOINT` | `localhost:9000` | |
| `MINIO_ACCESS_KEY` | `minioadmin` | |
| `MINIO_SECRET_KEY` | `minioadmin` | |
| `MINIO_BUCKET` | `creditstar-features` | |
| `MINIO_SECURE` | `false` | Set `true` for TLS |
| `POLL_INTERVAL_SECONDS` | `10` | CDC poll frequency |
| `LOG_LEVEL` | `INFO` | `DEBUG` for verbose output |
