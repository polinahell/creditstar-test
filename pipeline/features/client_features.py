"""
Client-level feature computations based on the de_test_materials schema.

Real tables
-----------
user    – CRM client record  (id, created_on, first_name, last_name, …)
loan    – Loan record         (id, client_id, amount, status, created_on,
                               duration, matured_on, updated_on)
          status values: 'paid' | 'overdue' | 'active' | 'application'
payment – Payment instalment  (id, loan_id, amount, principle, interest,
                               status, created_on)

Design: all features are computed in a single batch pass (3 SQL queries for
any number of clients) to avoid N×3 round-trips to the database.

Assumptions & edge-case documentation
--------------------------------------
paid_loans_count
    • Counts loans WHERE status = 'paid'.
    • 'overdue', 'active', and 'application' are not counted.
    • Returns 0 for clients absent from the result set (no paid loans).

days_since_last_late_payment
    • "Late" is inferred from loan table (payment table may be empty):
        – status = 'overdue'                           (missed / unpaid)
        – status = 'paid' AND updated_on > matured_on  (paid after due date)
    • updated_on is used as the event date.
    • Returns None for clients with no overdue/late-paid loans.
    • Days = integer calendar days (TODAY UTC − updated_on).

profit_in_last_90_days_rate
    • Numerator  : SUM(payment.interest) for payments on loans issued in
                   the last 90 days (LEFT JOIN, so 0 if no payments yet).
    • Denominator: SUM(loan.amount) for loans issued in the last 90 days.
    • Returns None when no loans were issued in the last 90 days.
    • Returns 0.0  when loans exist but no payments have been received yet.
    • Note: test data covers 2019–2020; any query from 2026+ returns None
      for all clients – this is correct, expected behaviour.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

import psycopg2.extras

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Batch computation (efficient: 3 queries for N clients)
# ---------------------------------------------------------------------------

def compute_features_for_clients(client_ids: list, cur) -> list:
    """
    Compute all three features for every id in *client_ids* using three
    set-based SQL queries instead of N×3 individual lookups.

    Returns a list of dicts ready to be converted to a DataFrame.
    """
    if not client_ids:
        return []

    id_tuple = tuple(set(client_ids))  # deduplicate
    computed_at = datetime.now(timezone.utc).isoformat()

    # -- Feature 1: paid loans count ----------------------------------------
    cur.execute(
        """
        SELECT client_id, COUNT(*) AS paid_loans_count
        FROM   loan
        WHERE  client_id = ANY(%s)
          AND  status = 'paid'
        GROUP  BY client_id
        """,
        (list(id_tuple),),
    )
    paid_map = {r["client_id"]: int(r["paid_loans_count"]) for r in cur.fetchall()}

    # -- Feature 2: days since last late payment ----------------------------
    cur.execute(
        """
        SELECT client_id, MAX(updated_on) AS last_late_date
        FROM   loan
        WHERE  client_id = ANY(%s)
          AND  (
                status = 'overdue'
                OR (status = 'paid' AND updated_on > matured_on)
               )
        GROUP  BY client_id
        """,
        (list(id_tuple),),
    )
    today = datetime.now(timezone.utc).date()
    late_map: dict = {}
    for r in cur.fetchall():
        if r["last_late_date"] is not None:
            late_map[r["client_id"]] = float((today - r["last_late_date"]).days)

    # -- Feature 3: profit rate in last 90 days ----------------------------
    cur.execute(
        """
        SELECT
            l.client_id,
            COALESCE(SUM(p.interest), 0.0) AS total_interest_received,
            SUM(l.amount)                  AS total_loan_amount
        FROM   loan l
        LEFT JOIN payment p ON p.loan_id = l.id
        WHERE  l.client_id = ANY(%s)
          AND  l.created_on >= CURRENT_DATE - INTERVAL '90 days'
        GROUP  BY l.client_id
        """,
        (list(id_tuple),),
    )
    profit_map: dict = {}
    for r in cur.fetchall():
        amt = float(r["total_loan_amount"]) if r["total_loan_amount"] is not None else None
        if amt is None or amt == 0:
            profit_map[r["client_id"]] = None
        else:
            profit_map[r["client_id"]] = float(r["total_interest_received"]) / amt

    # -- Assemble records ---------------------------------------------------
    records = []
    for cid in sorted(set(client_ids)):
        records.append(
            {
                "client_id": cid,
                "paid_loans_count": paid_map.get(cid, 0),
                "days_since_last_late_payment": late_map.get(cid),
                "profit_in_last_90_days_rate": profit_map.get(cid),
                "computed_at": computed_at,
            }
        )
    logger.debug("Computed features for %d clients", len(records))
    return records


# ---------------------------------------------------------------------------
# Single-client helpers (used by unit tests; delegate to batch internally)
# ---------------------------------------------------------------------------

def compute_paid_loans_count(client_id: int, cur) -> int:
    cur.execute(
        "SELECT COUNT(*) AS cnt FROM loan WHERE client_id = %s AND status = 'paid'",
        (client_id,),
    )
    return int(cur.fetchone()["cnt"])


def compute_days_since_last_late_payment(client_id: int, cur) -> Optional[float]:
    cur.execute(
        """
        SELECT MAX(updated_on) AS last_late_date
        FROM   loan
        WHERE  client_id = %s
          AND  (
                status = 'overdue'
                OR (status = 'paid' AND updated_on > matured_on)
               )
        """,
        (client_id,),
    )
    row = cur.fetchone()
    if row["last_late_date"] is None:
        return None
    today = datetime.now(timezone.utc).date()
    return float((today - row["last_late_date"]).days)


def compute_profit_in_last_90_days_rate(client_id: int, cur) -> Optional[float]:
    cur.execute(
        """
        SELECT
            COALESCE(SUM(p.interest), 0.0) AS total_interest_received,
            SUM(l.amount)                  AS total_loan_amount
        FROM   loan l
        LEFT JOIN payment p ON p.loan_id = l.id
        WHERE  l.client_id = %s
          AND  l.created_on >= CURRENT_DATE - INTERVAL '90 days'
        """,
        (client_id,),
    )
    row = cur.fetchone()
    total = row["total_loan_amount"]
    if total is None:
        return None
    total = float(total)
    if total == 0:
        return None
    return float(row["total_interest_received"]) / total
