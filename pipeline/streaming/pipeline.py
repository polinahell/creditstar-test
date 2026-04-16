"""
Polling-based CDC pipeline for de_test_materials schema.

Tables tracked
--------------
user    – change detection via created_on (inserts only; no updated_on)
loan    – change detection via updated_on  (inserts + status updates)
payment – change detection via created_on  (inserts only; no updated_on)

All date columns are DATE (not TIMESTAMPTZ), so the polling granularity is
one day.  With more time we would add a proper TIMESTAMPTZ updated_at column
via a migration, and wire up Debezium for sub-second CDC.

IMPORTANT: 'user' is a reserved word in PostgreSQL – always double-quote it.
"""

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

from config import config
from db.connector import get_cursor
from features.client_features import compute_features_for_clients
from storage.minio_client import storage

logger = logging.getLogger(__name__)

# (table, change-detection column)
TRACKED_TABLES = [
    ("user",    "created_on"),
    ("loan",    "updated_on"),
    ("payment", "created_on"),
]

_STATE_PATH = Path("/app/state/watermarks.json")


# ---------------------------------------------------------------------------
# Watermark helpers
# ---------------------------------------------------------------------------

def _load_watermarks() -> dict:
    if _STATE_PATH.exists():
        with _STATE_PATH.open() as f:
            return json.load(f)
    # Epoch date → first run performs a full initial load.
    return {t: "1970-01-01" for t, _ in TRACKED_TABLES}


def _save_watermarks(wm: dict) -> None:
    _STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _STATE_PATH.open("w") as f:
        json.dump(wm, f, indent=2)


# ---------------------------------------------------------------------------
# Per-table helpers
# ---------------------------------------------------------------------------

# 'user' is a reserved word – needs quoting in every SQL statement.
_QUOTED = {"user": '"user"'}

def _tbl(name: str) -> str:
    return _QUOTED.get(name, name)


def _fetch_changed_rows(table: str, date_col: str, since: str, cur) -> Optional[pd.DataFrame]:
    cur.execute(
        f"SELECT * FROM {_tbl(table)} WHERE {date_col} > %s::date ORDER BY {date_col}",
        (since,),
    )
    rows = cur.fetchall()
    if not rows:
        return None
    return pd.DataFrame([dict(r) for r in rows])


def _collect_affected_client_ids(table: str, date_col: str, since: str, cur) -> list:
    if table == "user":
        cur.execute(
            'SELECT id AS client_id FROM "user" WHERE created_on > %s::date',
            (since,),
        )
    elif table == "loan":
        cur.execute(
            "SELECT DISTINCT client_id FROM loan WHERE updated_on > %s::date",
            (since,),
        )
    elif table == "payment":
        # payments don't have client_id; join through loan
        cur.execute(
            """
            SELECT DISTINCT l.client_id
            FROM   payment p
            JOIN   loan l ON l.id = p.loan_id
            WHERE  p.created_on > %s::date
            """,
            (since,),
        )
    return [r["client_id"] for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Main cycle
# ---------------------------------------------------------------------------

def run_cycle(watermarks: dict) -> dict:
    cycle_date = datetime.now(timezone.utc).date().isoformat()
    affected: set = set()

    with get_cursor() as (cur, _conn):
        for table, date_col in TRACKED_TABLES:
            since = watermarks[table]

            df = _fetch_changed_rows(table, date_col, since, cur)
            if df is not None:
                path = storage.dated_path(f"raw/{table}", f"n{len(df)}")
                storage.write_parquet(df, path)
                logger.info("raw/%s: streamed %d rows (since %s)", table, len(df), since)

            changed_ids = _collect_affected_client_ids(table, date_col, since, cur)
            affected.update(changed_ids)

        new_watermarks = {t: cycle_date for t, _ in TRACKED_TABLES}

        if affected:
            logger.info("Computing features for %d client(s)", len(affected))
            records = compute_features_for_clients(sorted(affected), cur)
            if records:
                df_feat = pd.DataFrame(records)
                path = storage.dated_path("features/client_features", f"n{len(records)}")
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
    logger.info("Pipeline started – poll interval %ds", config.poll_interval_seconds)

    watermarks = _load_watermarks()

    while True:
        try:
            watermarks = run_cycle(watermarks)
            _save_watermarks(watermarks)
        except Exception:
            logger.exception("Cycle failed – will retry after poll interval")

        time.sleep(config.poll_interval_seconds)
