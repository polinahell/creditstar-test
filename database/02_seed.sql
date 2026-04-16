-- ============================================================
-- Seed data – uses interval arithmetic so dates stay relative
-- to the current wall-clock time regardless of when the dump
-- is restored.
--
-- Client scenarios:
--   1  Alice  – 2 paid loans + 1 active; late payment 30 days ago;
--               loan issued 20 days ago → all 3 features have values.
--   2  Bob    – 1 active loan issued 120 days ago (outside 90-day
--               window); no late payments → profit rate = NULL.
--   3  Carol  – 1 paid loan; late payment 200 days ago; no loans
--               in last 90 days → profit rate = NULL.
--   4  David  – brand-new client, no loans → all features = 0 / NULL.
--   5  Eve    – loan issued 10 days ago, no payments received yet
--               → profit rate = 0 (denominator > 0, numerator = 0).
-- ============================================================

-- clients
INSERT INTO clients (first_name, last_name, email, date_of_birth, country_code) VALUES
    ('Alice',  'Smith',  'alice@example.com',  '1985-03-15', 'EE'),
    ('Bob',    'Jones',  'bob@example.com',    '1990-07-22', 'LV'),
    ('Carol',  'Brown',  'carol@example.com',  '1978-11-08', 'LT'),
    ('David',  'White',  'david@example.com',  '1995-02-14', 'EE'),
    ('Eve',    'Green',  'eve@example.com',    '1988-09-30', 'FI');

-- ============================================================
-- Alice (client_id = 1)
-- Loan A1: paid, issued 400 days ago
-- Loan A2: paid, issued 200 days ago
-- Loan A3: active, issued 20 days ago  ← inside 90-day window
-- ============================================================
INSERT INTO loans (client_id, amount, interest_rate, term_months, status, issued_at, paid_at) VALUES
    (1, 1000.00, 0.2400, 12, 'paid',   NOW() - INTERVAL '400 days', NOW() - INTERVAL '50 days'),
    (1, 1500.00, 0.2000, 12, 'paid',   NOW() - INTERVAL '200 days', NOW() - INTERVAL '20 days'),
    (1,  800.00, 0.2200, 6,  'active', NOW() - INTERVAL '20 days',  NULL);

-- Payments for Loan A1 (id=1) – all paid on time
INSERT INTO payments (loan_id, client_id, due_date, payment_date, amount, principal_amount, interest_amount, is_late) VALUES
    (1, 1, (NOW() - INTERVAL '370 days')::date, (NOW() - INTERVAL '370 days')::date, 93.00, 73.00, 20.00, FALSE),
    (1, 1, (NOW() - INTERVAL '340 days')::date, (NOW() - INTERVAL '340 days')::date, 93.00, 74.46, 18.54, FALSE);

-- Payments for Loan A2 (id=2) – one late payment 30 days ago
INSERT INTO payments (loan_id, client_id, due_date, payment_date, amount, principal_amount, interest_amount, is_late) VALUES
    (2, 1, (NOW() - INTERVAL '170 days')::date, (NOW() - INTERVAL '170 days')::date, 137.50, 112.50, 25.00, FALSE),
    (2, 1, (NOW() - INTERVAL '140 days')::date, (NOW() - INTERVAL '35 days')::date,  137.50, 114.38, 23.12, TRUE);

-- Payments for Loan A3 (id=3) – one payment received (interest counted in 90-day window)
INSERT INTO payments (loan_id, client_id, due_date, payment_date, amount, principal_amount, interest_amount, is_late) VALUES
    (3, 1, (NOW() - INTERVAL '5 days')::date, (NOW() - INTERVAL '5 days')::date, 143.67, 128.33, 15.34, FALSE);

-- ============================================================
-- Bob (client_id = 2)
-- Loan B1: active, issued 120 days ago (outside 90-day window)
-- ============================================================
INSERT INTO loans (client_id, amount, interest_rate, term_months, status, issued_at, paid_at) VALUES
    (2, 2000.00, 0.1800, 24, 'active', NOW() - INTERVAL '120 days', NULL);

-- All Bob's payments are on time
INSERT INTO payments (loan_id, client_id, due_date, payment_date, amount, principal_amount, interest_amount, is_late) VALUES
    (4, 2, (NOW() - INTERVAL '90 days')::date,  (NOW() - INTERVAL '90 days')::date,  97.33, 67.33, 30.00, FALSE),
    (4, 2, (NOW() - INTERVAL '60 days')::date,  (NOW() - INTERVAL '60 days')::date,  97.33, 68.34, 28.99, FALSE),
    (4, 2, (NOW() - INTERVAL '30 days')::date,  (NOW() - INTERVAL '30 days')::date,  97.33, 69.36, 27.97, FALSE);

-- ============================================================
-- Carol (client_id = 3)
-- Loan C1: paid, issued 365 days ago
-- ============================================================
INSERT INTO loans (client_id, amount, interest_rate, term_months, status, issued_at, paid_at) VALUES
    (3, 500.00, 0.2800, 6, 'paid', NOW() - INTERVAL '365 days', NOW() - INTERVAL '180 days');

INSERT INTO payments (loan_id, client_id, due_date, payment_date, amount, principal_amount, interest_amount, is_late) VALUES
    (5, 3, (NOW() - INTERVAL '335 days')::date, (NOW() - INTERVAL '335 days')::date, 92.17, 80.50, 11.67, FALSE),
    (5, 3, (NOW() - INTERVAL '305 days')::date, (NOW() - INTERVAL '200 days')::date, 92.17, 82.37,  9.80, TRUE);

-- ============================================================
-- David (client_id = 4) – no loans, no payments (new client)
-- ============================================================

-- ============================================================
-- Eve (client_id = 5)
-- Loan E1: active, issued 10 days ago – no payments received yet
-- ============================================================
INSERT INTO loans (client_id, amount, interest_rate, term_months, status, issued_at, paid_at) VALUES
    (5, 1200.00, 0.2100, 12, 'active', NOW() - INTERVAL '10 days', NULL);

-- One scheduled instalment not yet paid (payment_date IS NULL)
INSERT INTO payments (loan_id, client_id, due_date, payment_date, amount, principal_amount, interest_amount, is_late) VALUES
    (6, 5, (NOW() + INTERVAL '20 days')::date, NULL, 110.50, 89.50, 21.00, FALSE);
