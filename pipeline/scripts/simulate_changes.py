"""
simulate_changes.py – injects live changes into de_test_materials so you
can watch the pipeline stream them in real time.

Usage (while docker-compose is running):
    docker compose exec pipeline python scripts/simulate_changes.py

Or locally:
    POSTGRES_HOST=localhost POSTGRES_DB=de_test_materials \\
        python pipeline/scripts/simulate_changes.py
"""

import sys, os, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from db.connector import get_cursor


def main() -> None:
    print("=== Creditstar pipeline change simulator ===\n")

    with get_cursor() as (cur, conn):
        # -- 1. New user -------------------------------------------------------
        cur.execute(
            """
            INSERT INTO "user" (created_on, first_name, last_name,
                                birth_date, personal_code)
            VALUES (CURRENT_DATE, 'Demo', 'User', '1992-06-01', 'DEMO99999')
            RETURNING id
            """
        )
        uid = cur.fetchone()["id"]
        print(f"[+] New user id={uid} (Demo User)")

        # -- 2. Loan application (will be picked up as new loan row) ----------
        cur.execute(
            """
            INSERT INTO loan (client_id, amount, created_on, duration,
                              matured_on, status, updated_on)
            VALUES (%s, 750.00, CURRENT_DATE, 30,
                    CURRENT_DATE + INTERVAL '30 days',
                    'application', CURRENT_DATE)
            RETURNING id
            """,
            (uid,),
        )
        loan_id = cur.fetchone()["id"]
        print(f"[+] Loan id={loan_id} inserted (status=application)")

        conn.commit()

    print("\n[~] Waiting 15 s for pipeline to pick up new rows …")
    time.sleep(15)

    with get_cursor() as (cur, conn):
        # -- 3. Approve and disburse loan -------------------------------------
        cur.execute(
            """
            UPDATE loan
            SET status = 'paid',
                matured_on = CURRENT_DATE + INTERVAL '30 days',
                updated_on = CURRENT_DATE
            WHERE id = %s
            """,
            (loan_id,),
        )
        print(f"\n[~] Loan id={loan_id} marked as paid")

        # -- 4. Mark an existing overdue loan as paid (late payment) ----------
        cur.execute(
            """
            UPDATE loan
            SET status     = 'paid',
                updated_on = CURRENT_DATE
            WHERE status = 'overdue'
            LIMIT 1
            """
        )
        if cur.rowcount:
            print("[~] One overdue loan marked as paid (late payment event)")

        conn.commit()

    print("\n[✓] Changes committed – check pipeline logs and MinIO console.")


if __name__ == "__main__":
    main()
