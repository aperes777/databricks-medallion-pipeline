-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW bronze_transactions_incremental AS

SELECT
    card_id,
    customer_id,
    price,
    product_id,
    transaction_timestamp,
    created_at
FROM bronze.card_transactions
WHERE created_at >= (
    SELECT COALESCE(MAX(created_at), '1900-01-01') - INTERVAL 2 DAYS
    FROM silver.card_transactions
);


-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dedup_transactions AS

SELECT
    card_id,
    customer_id,
    price,
    product_id,
    transaction_timestamp,
    created_at
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY card_id
               ORDER BY created_at DESC
           ) rn
    FROM bronze_transactions_incremental
)
WHERE rn = 1;

-- COMMAND ----------

MERGE INTO silver.card_transactions AS target
USING dedup_transactions AS source
ON target.card_id = source.card_id

WHEN NOT MATCHED THEN
INSERT (
    card_id,
    customer_id,
    price,
    product_id,
    transaction_timestamp,
    created_at
)
VALUES (
    source.card_id,
    source.customer_id,
    source.price,
    source.product_id,
    source.transaction_timestamp,
    source.created_at
);

-- COMMAND ----------

OPTIMIZE silver.card_transactions
ZORDER BY (card_id);