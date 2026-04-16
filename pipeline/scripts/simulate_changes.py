"""
simulate_changes.py – injects live changes into the database so you can
watch the pipeline detect and stream them in real time.

Usage (from repo root, while docker-compose is running):
    docker compose exec pipeline python scripts/simulate_changes.py

Or directly against a local Postgres:
    POSTGRES_HOST=localhost python pipeline/scripts/simulate_changes.py
"""

import sys
import os
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from db.connector import get_cursor


def main() -> None:
    print("=== Creditstar pipeline change simulator ===\n")

    with get_cursor() as (cur, conn):
        # -- 1. New client applies for a loan --------------------------------
        cur.execute(
            """
            INSERT INTO clients (first_name, last_name, email, date_of_birth, country_code)
            VALUES ('Frank', 'Demo', 'frank.demo@example.com', '1992-06-01', 'EE')
            RETURNING id
            """
        )
        client_id = cur.fetchone()["id"]
        print(f"[+] Inserted new client id={client_id} (Frank Demo)")

        # -- 2. Loan application ----------------------------------------------
        cur.execute(
            """
            INSERT INTO loan_applications
                (client_id, requested_amount, requested_term_months, status, decided_at)
            VALUES (%s, 750.00, 12, 'approved', NOW())
            RETURNING id
            """,
            (client_id,),
        )
        app_id = cur.fetchone()["id"]
        print(f"[+] Loan application id={app_id} approved")

        # -- 3. Loan disbursed ------------------------------------------------
        cur.execute(
            """
            INSERT INTO loans
                (client_id, application_id, amount, interest_rate, term_months, status, issued_at)
            VALUES (%s, %s, 750.00, 0.2200, 12, 'active', NOW())
            RETURNING id
            """,
            (client_id, app_id),
        )
        loan_id = cur.fetchone()["id"]
        print(f"[+] Loan id={loan_id} issued for €750 @ 22 % APR")

        # -- 4. First payment received on time --------------------------------
        cur.execute(
            """
            INSERT INTO payments
                (loan_id, client_id, due_date, payment_date,
                 amount, principal_amount, interest_amount, is_late)
            VALUES (%s, %s,
                    (NOW() + INTERVAL '30 days')::date,
                    NOW()::date,
                    68.75, 54.00, 14.75, FALSE)
            """,
            (loan_id, client_id),
        )
        print("[+] First payment recorded (on-time)")

        conn.commit()

    print("\n[~] Waiting 15 s for the pipeline to pick up changes …")
    time.sleep(15)

    # -- 5. Simulate a late payment on an existing client --------------------
    with get_cursor() as (cur, conn):
        cur.execute(
            """
            UPDATE payments
            SET    payment_date = NOW()::date,
                   is_late      = TRUE
            WHERE  client_id = 1
              AND  payment_date IS NULL
            LIMIT  1
            """
        )
        if cur.rowcount:
            print("\n[~] Marked an overdue payment for Alice (client_id=1) as late")
        else:
            print("\n[~] No unpaid payments found for Alice; skipping late-payment update")

        # -- 6. Mark one of Alice's loans as fully paid -----------------------
        cur.execute(
            """
            UPDATE loans
            SET    status  = 'paid',
                   paid_at = NOW()
            WHERE  client_id = 1
              AND  status   = 'active'
            LIMIT  1
            """
        )
        if cur.rowcount:
            print("[~] Marked one of Alice's active loans as paid")

        conn.commit()

    print("\n[✓] Changes committed – watch the pipeline logs for streaming activity.")


if __name__ == "__main__":
    main()
