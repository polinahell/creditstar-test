"""
Unit tests for client feature computations.

Schema under test (de_test_materials):
  loan    – amount, client_id, created_on, duration, id, matured_on,
            status ('paid'|'overdue'|'application'), updated_on
  payment – id, loan_id, amount, principle, interest, status, created_on

All DB interaction is mocked so tests run without a live database.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from features.client_features import (
    compute_days_since_last_late_payment,
    compute_paid_loans_count,
    compute_profit_in_last_90_days_rate,
)


def _cur(fetchone_result):
    m = MagicMock()
    m.fetchone.return_value = fetchone_result
    return m


# ---------------------------------------------------------------------------
# paid_loans_count
# ---------------------------------------------------------------------------

class TestPaidLoansCount:
    def test_multiple_paid_loans(self):
        assert compute_paid_loans_count(1, _cur({"cnt": 5})) == 5

    def test_zero_paid_loans(self):
        assert compute_paid_loans_count(2, _cur({"cnt": 0})) == 0

    def test_result_is_int(self):
        assert isinstance(compute_paid_loans_count(1, _cur({"cnt": 3})), int)


# ---------------------------------------------------------------------------
# days_since_last_late_payment
# (late = overdue loan OR paid loan where updated_on > matured_on)
# ---------------------------------------------------------------------------

class TestDaysSinceLastLatePayment:
    def test_no_late_loans_returns_none(self):
        assert compute_days_since_last_late_payment(1, _cur({"last_late_date": None})) is None

    def test_correct_days_for_past_date(self):
        late = (datetime.now(timezone.utc) - timedelta(days=45)).date()
        result = compute_days_since_last_late_payment(1, _cur({"last_late_date": late}))
        assert result == pytest.approx(45.0)

    def test_zero_days_for_today(self):
        today = datetime.now(timezone.utc).date()
        result = compute_days_since_last_late_payment(1, _cur({"last_late_date": today}))
        assert result == 0.0

    def test_result_is_float(self):
        d = (datetime.now(timezone.utc) - timedelta(days=10)).date()
        result = compute_days_since_last_late_payment(1, _cur({"last_late_date": d}))
        assert isinstance(result, float)


# ---------------------------------------------------------------------------
# profit_in_last_90_days_rate
# ---------------------------------------------------------------------------

class TestProfitInLast90DaysRate:
    def test_no_loans_in_window_returns_none(self):
        cur = _cur({"total_interest_received": 0, "total_loan_amount": None})
        assert compute_profit_in_last_90_days_rate(1, cur) is None

    def test_loans_but_no_payments_returns_zero(self):
        cur = _cur({"total_interest_received": 0.0, "total_loan_amount": 1000.0})
        assert compute_profit_in_last_90_days_rate(1, cur) == 0.0

    def test_correct_ratio(self):
        # 120 interest on 800 amount → 15 %
        cur = _cur({"total_interest_received": 120.0, "total_loan_amount": 800.0})
        assert compute_profit_in_last_90_days_rate(1, cur) == pytest.approx(0.15)

    def test_multiple_loans_aggregated(self):
        cur = _cur({"total_interest_received": 300.0, "total_loan_amount": 3000.0})
        assert compute_profit_in_last_90_days_rate(1, cur) == pytest.approx(0.10)

    def test_zero_loan_amount_returns_none(self):
        cur = _cur({"total_interest_received": 0.0, "total_loan_amount": 0.0})
        assert compute_profit_in_last_90_days_rate(1, cur) is None

    def test_result_is_float(self):
        cur = _cur({"total_interest_received": 50.0, "total_loan_amount": 500.0})
        assert isinstance(compute_profit_in_last_90_days_rate(1, cur), float)
