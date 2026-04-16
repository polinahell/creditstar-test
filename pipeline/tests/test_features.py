"""
Unit tests for client feature computations.

All DB interaction is mocked so tests run without a live Postgres instance.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from features.client_features import (
    compute_days_since_last_late_payment,
    compute_paid_loans_count,
    compute_profit_in_last_90_days_rate,
)


def _cursor(fetchone_result):
    cur = MagicMock()
    cur.fetchone.return_value = fetchone_result
    return cur


# ---------------------------------------------------------------------------
# paid_loans_count
# ---------------------------------------------------------------------------

class TestPaidLoansCount:
    def test_returns_count_when_loans_exist(self):
        assert compute_paid_loans_count(1, _cursor({"cnt": 3})) == 3

    def test_returns_zero_when_no_paid_loans(self):
        assert compute_paid_loans_count(2, _cursor({"cnt": 0})) == 0

    def test_result_is_int(self):
        # DB may return Decimal; ensure we always get int
        assert isinstance(compute_paid_loans_count(1, _cursor({"cnt": 1})), int)


# ---------------------------------------------------------------------------
# days_since_last_late_payment
# ---------------------------------------------------------------------------

class TestDaysSinceLastLatePayment:
    def test_returns_none_when_no_late_payments(self):
        cur = _cursor({"last_late_date": None})
        assert compute_days_since_last_late_payment(1, cur) is None

    def test_returns_correct_days(self):
        late_date = (datetime.now(timezone.utc) - timedelta(days=30)).date()
        cur = _cursor({"last_late_date": late_date})
        result = compute_days_since_last_late_payment(1, cur)
        assert result == pytest.approx(30.0)

    def test_returns_zero_for_today(self):
        today = datetime.now(timezone.utc).date()
        cur = _cursor({"last_late_date": today})
        assert compute_days_since_last_late_payment(1, cur) == 0.0

    def test_result_is_float(self):
        late_date = (datetime.now(timezone.utc) - timedelta(days=5)).date()
        result = compute_days_since_last_late_payment(1, _cursor({"last_late_date": late_date}))
        assert isinstance(result, float)


# ---------------------------------------------------------------------------
# profit_in_last_90_days_rate
# ---------------------------------------------------------------------------

class TestProfitInLast90DaysRate:
    def test_returns_none_when_no_loans_in_window(self):
        cur = _cursor({"total_interest_received": 0, "total_loan_amount": None})
        assert compute_profit_in_last_90_days_rate(1, cur) is None

    def test_returns_zero_when_loans_but_no_payments_yet(self):
        cur = _cursor({"total_interest_received": 0.0, "total_loan_amount": 1000.0})
        assert compute_profit_in_last_90_days_rate(1, cur) == 0.0

    def test_correct_ratio(self):
        # 150 interest on 1000 principal → 15 %
        cur = _cursor({"total_interest_received": 150.0, "total_loan_amount": 1000.0})
        assert compute_profit_in_last_90_days_rate(1, cur) == pytest.approx(0.15)

    def test_multiple_loans_aggregated(self):
        # 200 interest on 2000 total → 10 %
        cur = _cursor({"total_interest_received": 200.0, "total_loan_amount": 2000.0})
        assert compute_profit_in_last_90_days_rate(1, cur) == pytest.approx(0.10)

    def test_returns_none_for_zero_loan_amount(self):
        # Defensive: loan with amount=0 recorded (data quality issue)
        cur = _cursor({"total_interest_received": 0.0, "total_loan_amount": 0.0})
        assert compute_profit_in_last_90_days_rate(1, cur) is None

    def test_result_is_float_when_computable(self):
        cur = _cursor({"total_interest_received": 50.0, "total_loan_amount": 500.0})
        result = compute_profit_in_last_90_days_rate(1, cur)
        assert isinstance(result, float)
