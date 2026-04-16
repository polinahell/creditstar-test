-- Post-restore setup: adds CDC pipeline infrastructure on top of the
-- restored CRM schema without touching existing tables or data.

-- Indexes used by the polling CDC loop to efficiently find changed rows.
CREATE INDEX IF NOT EXISTS idx_loan_updated_on   ON loan(updated_on);
CREATE INDEX IF NOT EXISTS idx_loan_client_id    ON loan(client_id);
CREATE INDEX IF NOT EXISTS idx_payment_created_on ON payment(created_on);
CREATE INDEX IF NOT EXISTS idx_payment_loan_id   ON payment(loan_id);

-- Watermark table – stores the last-processed date per source table.
-- Initialized to epoch so the first pipeline run performs a full load.
CREATE TABLE IF NOT EXISTS pipeline_watermarks (
    table_name          VARCHAR(100) PRIMARY KEY,
    last_processed_date DATE NOT NULL DEFAULT '1970-01-01'
);

INSERT INTO pipeline_watermarks (table_name)
VALUES ('user'), ('loan'), ('payment')
ON CONFLICT DO NOTHING;
