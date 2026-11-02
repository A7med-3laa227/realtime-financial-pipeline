-- Runs automatically the first time the ClickHouse container starts with an empty data volume
-- (mounted into /docker-entrypoint-initdb.d). To re-run it by hand:
--   docker exec -i clickhouse clickhouse-client --user <user> --password <password> --multiquery < sql/clickhouse_init.sql

CREATE DATABASE IF NOT EXISTS financial_db;

-- Landing table written by Spark (one row per CDC event).
-- ReplacingMergeTree keeps the row with the highest `version` per sorting key, and
-- `is_deleted` marks rows whose latest event was a Debezium delete (op = 'd').
CREATE TABLE IF NOT EXISTS financial_db.transactions
(
    transaction_id  String,
    user_id         String,
    timestamp       DateTime64(6, 'UTC'),
    amount          Decimal(18, 2),
    currency        LowCardinality(String),
    city            String,
    country         LowCardinality(String),
    merchant_name   String,
    payment_method  LowCardinality(String),
    ip_address      String,
    voucher_code    String,
    affiliate_id    String,
    version         UInt64,   -- Debezium ts_ms: newest event wins
    is_deleted      UInt8     -- 1 when Debezium op = 'd'
)
ENGINE = ReplacingMergeTree(version, is_deleted)
PARTITION BY toYYYYMM(timestamp)
-- transaction_id must be in the sorting key: it is what ReplacingMergeTree de-duplicates on
ORDER BY (toDate(timestamp), transaction_id);

-- Current state: latest event per transaction, deletes removed.
-- FINAL applies the de-duplication at read time (background merges are not instant),
-- and also makes Spark retries harmless (duplicate rows collapse).
CREATE OR REPLACE VIEW financial_db.transactions_current AS
SELECT * EXCEPT (version, is_deleted)
FROM financial_db.transactions FINAL
WHERE is_deleted = 0;

-- Ready-made source for dashboards
CREATE OR REPLACE VIEW financial_db.daily_spend AS
SELECT
    toDate(timestamp)  AS day,
    country,
    currency,
    payment_method,
    count()            AS transactions,
    sum(amount)        AS total_amount,
    avg(amount)        AS avg_amount
FROM financial_db.transactions_current
GROUP BY day, country, currency, payment_method;