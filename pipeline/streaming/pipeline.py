"""
Polling-based CDC pipeline.

Design
------
Rather than setting up Debezium + Kafka (which requires significant
infrastructure), we implement a watermark-polling CDC loop:

1. Each cycle, for every tracked table, we SELECT rows WHERE updated_at
   is strictly greater than the stored watermark.
2. Changed rows are written to MinIO as Parquet files under raw/<table>/…
3. We collect the set of client_ids that were affected (directly or via
   foreign key) and recompute their features.
4. Computed features are written to MinIO under features/client_features/…
5. Watermarks are advanced to the wall-clock time captured at the start of
   the cycle (not the max updated_at of the batch) so we never miss rows
   that land while the cycle is running.

Trade-offs vs true CDC (Debezium/logical replication)
------------------------------------------------------
+ Simple to run locally; no Kafka/Zookeeper/Kafka Connect overhead.
+ Works with any PostgreSQL version; no special replication roles needed.
- Misses hard DELETEs (no tombstones). Acceptable for this use case
  because loan/payment records are never deleted in the CRM.
- Minimum latency is the poll interval (configurable, default 10 s).
  True CDC would be sub-second.
- Requires updated_at to be maintained reliably (enforced by DB triggers).
"""

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import psycopg2.extras

from config import config
from db.connector import get_cursor
from features.client_features import compute_features_for_clients
from storage.minio_client import storage

logger = logging.getLogger(__name__)

TRACKED_TABLES = ["clients", "loan_applications", "loans", "payments"]

# Watermarks are persisted to a JSON file on a Docker volume so the pipeline
# survives container restarts without reprocessing all historical data.
_STATE_PATH = Path("/app/state/watermarks.json")


# ---------------------------------------------------------------------------
# Watermark helpers
# ---------------------------------------------------------------------------

def _load_watermarks() -> dict:
    if _STATE_PATH.exists():
        with _STATE_PATH.open() as f:
            return json.load(f)
    # Epoch zero → first run will perform a full initial load of all tables.
    return {t: "1970-01-01T00:00:00+00:00" for t in TRACKED_TABLES}


def _save_watermarks(wm: dict) -> None:
    _STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _STATE_PATH.open("w") as f:
        json.dump(wm, f, indent=2)


# ---------------------------------------------------------------------------
# Per-table helpers
# ---------------------------------------------------------------------------

def _fetch_changed_rows(table: str, since: str, cur) -> Optional[pd.DataFrame]:
    """Return a DataFrame of rows updated after *since*, or None if none."""
    cur.execute(
        f"SELECT * FROM {table} WHERE updated_at > %s ORDER BY updated_at",  # noqa: S608
        (since,),
    )
    rows = cur.fetchall()
    if not rows:
        return None
    return pd.DataFrame([dict(r) for r in rows])


def _collect_affected_client_ids(table: str, since: str, cur) -> list:
    """Return distinct client IDs touched in *table* since *since*."""
    if table == "clients":
        cur.execute(
            "SELECT id AS client_id FROM clients WHERE updated_at > %s",
            (since,),
        )
    else:
        # loan_applications, loans, payments all have a client_id column
        cur.execute(
            f"SELECT DISTINCT client_id FROM {table} WHERE updated_at > %s",  # noqa: S608
            (since,),
        )
    return [r["client_id"] for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Main cycle
# ---------------------------------------------------------------------------

def run_cycle(watermarks: dict) -> dict:
    """
    Execute one polling cycle.  Returns updated watermarks.
    """
    cycle_start = datetime.now(timezone.utc).isoformat()
    affected: set = set()

    with get_cursor() as (cur, _conn):
        for table in TRACKED_TABLES:
            since = watermarks[table]

            df = _fetch_changed_rows(table, since, cur)
            if df is not None:
                path = storage.dated_path(f"raw/{table}", f"n{len(df)}")
                storage.write_parquet(df, path)
                logger.info("raw/%s: streamed %d rows", table, len(df))

            changed_ids = _collect_affected_client_ids(table, since, cur)
            affected.update(changed_ids)

        # Advance all watermarks to the moment the cycle started.
        # Using cycle_start (not max(updated_at)) means rows that arrive
        # concurrently with our SELECT are captured in the next cycle.
        new_watermarks = {t: cycle_start for t in TRACKED_TABLES}

        if affected:
            logger.info("Computing features for %d client(s)", len(affected))
            feature_records = compute_features_for_clients(sorted(affected), cur)
            if feature_records:
                df_feat = pd.DataFrame(feature_records)
                path = storage.dated_path(
                    "features/client_features", f"n{len(feature_records)}"
                )
                storage.write_parquet(df_feat, path)

    return new_watermarks


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run() -> None:
    logging.basicConfig(
        level=getattr(logging, config.log_level, logging.INFO),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )
    logger.info(
        "Pipeline started – poll interval %ds", config.poll_interval_seconds
    )

    watermarks = _load_watermarks()

    while True:
        try:
            watermarks = run_cycle(watermarks)
            _save_watermarks(watermarks)
        except Exception:
            logger.exception("Cycle failed – will retry after poll interval")

        time.sleep(config.poll_interval_seconds)
