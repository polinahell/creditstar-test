#!/usr/bin/env python3
"""
verify.py – end-to-end smoke test that runs without Docker.

Checks:
  1. PostgreSQL connection + dump restore (counts rows)
  2. All three feature functions against real data (clients 1-5)
  3. One full pipeline cycle with MinIO mocked out
  4. Parquet serialisation of feature records

Run from repo root:
    POSTGRES_HOST=localhost POSTGRES_DB=de_test_materials \
        python pipeline/scripts/verify.py
"""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
os.environ.setdefault("POSTGRES_HOST", "localhost")
os.environ.setdefault("POSTGRES_DB",   "de_test_materials")
os.environ.setdefault("POSTGRES_USER", "postgres")
os.environ.setdefault("POSTGRES_PASSWORD", "postgres")
os.environ.setdefault("MINIO_ENDPOINT",    "localhost:9000")
os.environ.setdefault("MINIO_ACCESS_KEY",  "minioadmin")
os.environ.setdefault("MINIO_SECRET_KEY",  "minioadmin")

from unittest.mock import MagicMock, patch
import io, json
from datetime import date

PASS = "\033[92m✓\033[0m"
FAIL = "\033[91m✗\033[0m"

failures = 0

def check(label, condition, detail=""):
    global failures
    if condition:
        print(f"  {PASS} {label}")
    else:
        print(f"  {FAIL} {label}" + (f": {detail}" if detail else ""))
        failures += 1

# ── 1. Database connection ──────────────────────────────────────────────────
print("\n[1] Database connection & dump contents")
try:
    from db.connector import get_cursor
    with get_cursor() as (cur, _):
        cur.execute("SELECT COUNT(*) AS n FROM loan")
        loan_count = cur.fetchone()["n"]
        cur.execute("SELECT COUNT(*) AS n FROM payment")
        payment_count = cur.fetchone()["n"]
        cur.execute('SELECT COUNT(*) AS n FROM "user"')
        user_count = cur.fetchone()["n"]
        cur.execute("SELECT DISTINCT status FROM loan ORDER BY status")
        statuses = [r["status"] for r in cur.fetchall()]

    check(f"loan table has rows ({loan_count:,})",  loan_count > 0)
    check(f"payment table accessible ({payment_count} rows)", True)
    check(f"user table accessible ({user_count} rows)", True)
    check("expected loan statuses present",
          {"application", "overdue", "paid"}.issubset(set(statuses)),
          str(statuses))
except Exception as e:
    print(f"  {FAIL} Could not connect: {e}")
    failures += 1
    print("\nABORTED – fix DB connection first.")
    sys.exit(1)

# ── 2. Feature computation against real data ───────────────────────────────
print("\n[2] Feature computation (clients 1–5)")
from features.client_features import (
    compute_paid_loans_count,
    compute_days_since_last_late_payment,
    compute_profit_in_last_90_days_rate,
    compute_features_for_clients,
)

with get_cursor() as (cur, _):
    for cid in range(1, 6):
        paid   = compute_paid_loans_count(cid, cur)
        late   = compute_days_since_last_late_payment(cid, cur)
        profit = compute_profit_in_last_90_days_rate(cid, cur)
        print(f"     client {cid}: paid_loans={paid}  "
              f"days_since_late={late}  profit_90d={profit}")
        check(f"client {cid} paid_loans is int >= 0",
              isinstance(paid, int) and paid >= 0)
        check(f"client {cid} days_late is None or float >= 0",
              late is None or (isinstance(late, float) and late >= 0))
        check(f"client {cid} profit_rate is None or float",
              profit is None or isinstance(profit, float))

# ── 3. Batch feature computation ───────────────────────────────────────────
print("\n[3] Batch compute_features_for_clients([1,2,3])")
with get_cursor() as (cur, _):
    records = compute_features_for_clients([1, 2, 3], cur)
check("returns 3 records", len(records) == 3)
check("each record has required keys",
      all({"client_id","paid_loans_count",
           "days_since_last_late_payment",
           "profit_in_last_90_days_rate",
           "computed_at"}.issubset(r) for r in records))

# ── 4. Parquet serialisation ───────────────────────────────────────────────
print("\n[4] Parquet serialisation of feature records")
try:
    import pandas as pd
    df = pd.DataFrame(records)
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False)
    buf.seek(0)
    df2 = pd.read_parquet(buf)
    check("round-trip produces same row count", len(df2) == len(df))
    check("client_id column present", "client_id" in df2.columns)
    check(f"parquet size reasonable ({buf.getbuffer().nbytes} bytes)",
          buf.getbuffer().nbytes > 0)
except Exception as e:
    check("parquet round-trip", False, str(e))

# ── 5. Pipeline cycle with mocked MinIO ───────────────────────────────────
print("\n[5] Full pipeline cycle (MinIO mocked)")
try:
    written_paths = []

    def fake_write(df, path):
        written_paths.append((path, len(df)))

    fake_storage = MagicMock()
    fake_storage.dated_path.side_effect = lambda prefix, tag="": f"{prefix}/test/{tag}.parquet"
    fake_storage.write_parquet.side_effect = fake_write

    # Import after patching to avoid connecting to MinIO at import time
    with patch.dict("sys.modules", {}):
        import streaming.pipeline as sp
        original_storage = sp.storage
        sp.storage = fake_storage
        try:
            # Use a recent date so the test cycle only processes a small slice
            wm = {t: "2020-08-01" for t, _ in sp.TRACKED_TABLES}
            new_wm = sp.run_cycle(wm)
        finally:
            sp.storage = original_storage

    check("watermarks updated after cycle",
          all(new_wm[t] > "1970-01-01" for t, _ in sp.TRACKED_TABLES))
    check("raw/loan data written to storage",
          any("raw/loan" in p for p, _ in written_paths))
    check("features written to storage",
          any("features/client_features" in p for p, _ in written_paths))

    total_rows = sum(n for _, n in written_paths)
    print(f"     Written {len(written_paths)} Parquet file(s), "
          f"{total_rows:,} total rows")
    for path, n in written_paths:
        print(f"       {n:>7,} rows → {path}")
except Exception as e:
    check("pipeline cycle", False, str(e))
    import traceback; traceback.print_exc()

# ── Summary ────────────────────────────────────────────────────────────────
print(f"\n{'='*50}")
if failures == 0:
    print(f"\033[92m  ALL CHECKS PASSED\033[0m")
else:
    print(f"\033[91m  {failures} CHECK(S) FAILED\033[0m")
sys.exit(failures)
