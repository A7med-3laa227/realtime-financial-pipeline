-- ============================================================
-- Postgres Init SQL
-- Runs automatically on first container start via initdb.d
-- ============================================================

-- POSTGRES_DB is set to financial_db in docker-compose,
-- so this init script runs against it automatically.

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id  VARCHAR(255) PRIMARY KEY,
    user_id         VARCHAR(255),
    timestamp       TIMESTAMP,
    amount          DECIMAL(12, 2),
    currency        VARCHAR(10),
    city            VARCHAR(255),
    country         VARCHAR(255),
    merchant_name   VARCHAR(255),
    payment_method  VARCHAR(50),
    ip_address      VARCHAR(50),
    voucher_code    VARCHAR(50),
    affiliate_id    VARCHAR(255)
);

-- Index for efficient time-based queries (needed for Debezium CDC)
CREATE INDEX IF NOT EXISTS idx_transactions_timestamp
    ON transactions (timestamp);

-- Index for currency-based analysis
CREATE INDEX IF NOT EXISTS idx_transactions_currency
    ON transactions (currency);
