"""
Client-level feature computations based on the de_test_materials schema.

Real tables
-----------
user    – CRM client record  (id, created_on, first_name, last_name, …)
loan    – Loan record         (id, client_id, amount, status, created_on,
                               duration, matured_on, updated_on)
payment – Payment instalment  (id, loan_id, amount, principle, interest,
                               status, created_on)

Loan status values observed in the dump: 'paid' | 'overdue' | 'application'

Assumptions & edge-case documentation
--------------------------------------
paid_loans_count
    • Counts loans WHERE status = 'paid'.
    • Returns 0 for clients with no paid loans.
    • 'overdue' and 'application' are not counted.

days_since_last_late_payment
    • "Late payment" is inferred from the loan table (the payment table is
      empty in this dump):
        – status = 'overdue'                           (missed payment)
        – status = 'paid' AND updated_on > matured_on  (paid after due date)
    • updated_on is used as the event date (when the status last changed).
    • Returns None when the client has never had an overdue or late-paid loan.
    • Days are integer calendar days (TODAY UTC − updated_on).

profit_in_last_90_days_rate
    • Numerator  : SUM(payment.interest) for payments made on loans issued
                   in the last 90 days (payment.created_on IS NOT NULL acts
                   as proof of receipt, though the current dump has no rows).
    • Denominator: SUM(loan.amount) for loans issued in the last 90 days.
    • Returns None  when no loans were issued in the last 90 days.
    • Returns 0.0  when loans exist in the window but no payments yet.
    • Note: the test dataset covers 2019–2020; running today (2026+) means
      this feature will always return None – this is correct behaviour and
      demonstrates the NULL-safety of the implementation.
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
        "SELECT COUNT(*) AS cnt FROM loan WHERE client_id = %s AND status = 'paid'",
        (client_id,),
    )
    return int(cur.fetchone()["cnt"])


def compute_days_since_last_late_payment(client_id: int, cur) -> Optional[float]:
    """
    Uses loan.updated_on as a proxy for payment date.
    Late = overdue loan OR paid loan where updated_on > matured_on.
    """
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
        return None  # no late/overdue loans on record

    today = datetime.now(timezone.utc).date()
    return float((today - row["last_late_date"]).days)


def compute_profit_in_last_90_days_rate(client_id: int, cur) -> Optional[float]:
    """
    SUM(payment.interest) / SUM(loan.amount) for loans issued in last 90 days.
    Falls back to None when no loans exist in the window.
    """
    cur.execute(
        """
        SELECT
            COALESCE(SUM(p.interest), 0.0) AS total_interest_received,
            SUM(l.amount)                  AS total_loan_amount
        FROM   loan l
        LEFT JOIN payment p
               ON p.loan_id = l.id
        WHERE  l.client_id = %s
          AND  l.created_on >= CURRENT_DATE - INTERVAL '90 days'
        """,
        (client_id,),
    )
    row = cur.fetchone()

    total_loan_amount = row["total_loan_amount"]
    if total_loan_amount is None:
        return None  # no loans issued in the last 90 days

    total_loan_amount = float(total_loan_amount)
    if total_loan_amount == 0:
        return None  # guard against zero-amount loans (data quality)

    return float(row["total_interest_received"]) / total_loan_amount


# ---------------------------------------------------------------------------
# Batch entry point
# ---------------------------------------------------------------------------

def compute_features_for_clients(client_ids: list, cur) -> list:
    """
    Compute all three features for every client_id.
    A single open cursor is passed in; the caller owns the connection.
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
            cid, paid_loans, days_late, profit_rate,
        )

    return records
