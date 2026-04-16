"""
Client-level feature computations requested by the data science team.

Each function accepts a client_id and an open psycopg2 cursor (RealDictCursor).
All SQL uses parameterised queries; no string interpolation of user data.

Assumptions & edge-case documentation
--------------------------------------
paid_loans_count
    • Counts loans WHERE status = 'paid'.
    • Returns 0 for clients with no paid loans (including brand-new clients).
    • 'defaulted' / 'written_off' loans are NOT counted as paid.

days_since_last_late_payment
    • A late payment is a row in `payments` WHERE is_late = TRUE
      AND payment_date IS NOT NULL (the payment was actually made, just late).
    • Returns None when the client has never made a late payment or has no
      payment history at all – downstream consumers should treat None as
      "no late payment on record", not as zero.
    • Days are calculated as integer days from today (UTC) to the latest
      late payment_date.

profit_in_last_90_days_rate
    • Numerator  : sum of interest_amount from payments WHERE payment_date
                   IS NOT NULL on loans issued in the last 90 days.
    • Denominator: sum of loan.amount for loans issued in the last 90 days.
    • Returns None  when the client has no loans issued in the last 90 days
      (undefined / missing, not zero).
    • Returns 0.0  when loans exist in the window but no payments have been
      received yet (valid ratio of 0/positive_number).
    • Division-by-zero against a zero-sum loan amount is guarded and returns
      None (edge case: loan amount recorded as 0, which shouldn't happen in
      normal operation).
"""

import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Individual feature functions
# ---------------------------------------------------------------------------

def compute_paid_loans_count(client_id: int, cur) -> int:
    cur.execute(
        "SELECT COUNT(*) AS cnt FROM loans WHERE client_id = %s AND status = 'paid'",
        (client_id,),
    )
    return int(cur.fetchone()["cnt"])


def compute_days_since_last_late_payment(client_id: int, cur) -> Optional[float]:
    cur.execute(
        """
        SELECT MAX(payment_date) AS last_late_date
        FROM   payments
        WHERE  client_id = %s
          AND  is_late = TRUE
          AND  payment_date IS NOT NULL
        """,
        (client_id,),
    )
    row = cur.fetchone()
    if row["last_late_date"] is None:
        return None  # no late payment on record

    today = datetime.now(timezone.utc).date()
    return float((today - row["last_late_date"]).days)


def compute_profit_in_last_90_days_rate(client_id: int, cur) -> Optional[float]:
    cur.execute(
        """
        SELECT
            COALESCE(SUM(p.interest_amount), 0.0)  AS total_interest_received,
            SUM(l.amount)                           AS total_loan_amount
        FROM   loans l
        LEFT JOIN payments p
               ON p.loan_id = l.id
              AND p.payment_date IS NOT NULL
        WHERE  l.client_id = %s
          AND  l.issued_at >= NOW() - INTERVAL '90 days'
        """,
        (client_id,),
    )
    row = cur.fetchone()

    total_loan_amount = row["total_loan_amount"]
    if total_loan_amount is None:
        return None  # no loans issued in the last 90 days

    total_loan_amount = float(total_loan_amount)
    if total_loan_amount == 0:
        return None  # guard: should not occur with valid data

    return float(row["total_interest_received"]) / total_loan_amount


# ---------------------------------------------------------------------------
# Batch entry point – single DB connection for all clients in one cycle
# ---------------------------------------------------------------------------

def compute_features_for_clients(client_ids: list, cur) -> list:
    """
    Compute all three features for every client_id in *client_ids*.
    Returns a list of dicts ready to be turned into a DataFrame.

    A single open cursor is passed in so the caller controls the
    transaction/connection lifecycle.
    """
    if not client_ids:
        return []

    computed_at = datetime.now(timezone.utc).isoformat()
    records = []

    for cid in client_ids:
        paid_loans = compute_paid_loans_count(cid, cur)
        days_late = compute_days_since_last_late_payment(cid, cur)
        profit_rate = compute_profit_in_last_90_days_rate(cid, cur)

        records.append(
            {
                "client_id": cid,
                "paid_loans_count": paid_loans,
                "days_since_last_late_payment": days_late,
                "profit_in_last_90_days_rate": profit_rate,
                "computed_at": computed_at,
            }
        )
        logger.debug(
            "client=%d paid_loans=%d days_late=%s profit_rate=%s",
            cid,
            paid_loans,
            days_late,
            profit_rate,
        )

    return records
