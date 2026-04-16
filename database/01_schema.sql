-- ============================================================
-- Creditstar CRM schema
-- WAL-level logical replication is enabled at the server level
-- via docker-compose (wal_level=logical).
-- ============================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ----------------------------------------------------------
-- clients
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS clients (
    id              SERIAL PRIMARY KEY,
    first_name      VARCHAR(100) NOT NULL,
    last_name       VARCHAR(100) NOT NULL,
    email           VARCHAR(255) UNIQUE NOT NULL,
    date_of_birth   DATE,
    country_code    CHAR(2),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ----------------------------------------------------------
-- loan_applications
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS loan_applications (
    id                    SERIAL PRIMARY KEY,
    client_id             INTEGER NOT NULL REFERENCES clients(id),
    requested_amount      NUMERIC(12, 2) NOT NULL,
    requested_term_months INTEGER NOT NULL,
    -- pending | approved | rejected
    status                VARCHAR(50) NOT NULL DEFAULT 'pending',
    submitted_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    decided_at            TIMESTAMPTZ,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ----------------------------------------------------------
-- loans
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS loans (
    id            SERIAL PRIMARY KEY,
    client_id     INTEGER NOT NULL REFERENCES clients(id),
    application_id INTEGER REFERENCES loan_applications(id),
    amount        NUMERIC(12, 2) NOT NULL,
    -- annual interest rate, e.g. 0.2450 = 24.50 %
    interest_rate NUMERIC(6, 4) NOT NULL,
    term_months   INTEGER NOT NULL,
    -- active | paid | defaulted | written_off
    status        VARCHAR(50) NOT NULL DEFAULT 'active',
    issued_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    paid_at       TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ----------------------------------------------------------
-- payments
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS payments (
    id               SERIAL PRIMARY KEY,
    loan_id          INTEGER NOT NULL REFERENCES loans(id),
    client_id        INTEGER NOT NULL REFERENCES clients(id),
    due_date         DATE NOT NULL,
    -- NULL means the instalment has not been paid yet
    payment_date     DATE,
    amount           NUMERIC(12, 2) NOT NULL,
    principal_amount NUMERIC(12, 2) NOT NULL,
    interest_amount  NUMERIC(12, 2) NOT NULL,
    is_late          BOOLEAN NOT NULL DEFAULT FALSE,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ----------------------------------------------------------
-- Indexes
-- ----------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_loans_client_id     ON loans(client_id);
CREATE INDEX IF NOT EXISTS idx_loans_status        ON loans(status);
CREATE INDEX IF NOT EXISTS idx_loans_issued_at     ON loans(issued_at);
CREATE INDEX IF NOT EXISTS idx_loans_updated_at    ON loans(updated_at);

CREATE INDEX IF NOT EXISTS idx_payments_client_id  ON payments(client_id);
CREATE INDEX IF NOT EXISTS idx_payments_loan_id    ON payments(loan_id);
CREATE INDEX IF NOT EXISTS idx_payments_is_late    ON payments(is_late);
CREATE INDEX IF NOT EXISTS idx_payments_updated_at ON payments(updated_at);

CREATE INDEX IF NOT EXISTS idx_clients_updated_at  ON clients(updated_at);

CREATE INDEX IF NOT EXISTS idx_loan_apps_updated_at ON loan_applications(updated_at);

-- ----------------------------------------------------------
-- auto-update updated_at on every row change
-- ----------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_clients_updated_at
    BEFORE UPDATE ON clients
    FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();

CREATE TRIGGER trg_loan_applications_updated_at
    BEFORE UPDATE ON loan_applications
    FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();

CREATE TRIGGER trg_loans_updated_at
    BEFORE UPDATE ON loans
    FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();

CREATE TRIGGER trg_payments_updated_at
    BEFORE UPDATE ON payments
    FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();

-- ----------------------------------------------------------
-- Pipeline watermark table – tracks last-processed timestamp
-- per source table so the polling CDC can resume correctly.
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_watermarks (
    table_name          VARCHAR(100) PRIMARY KEY,
    last_processed_at   TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01 00:00:00+00'
);

INSERT INTO pipeline_watermarks (table_name)
VALUES ('clients'), ('loans'), ('payments'), ('loan_applications')
ON CONFLICT DO NOTHING;
