-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW bronze_incremental AS
SELECT 
        card_id,
        customer_id,
        lastname,
        firstname,
        email,
        address,
        birthday,
        country,
        created_at,
        updated_at
FROM bronze.customers
WHERE created_at >= (
    SELECT COALESCE(MAX(created_at), '1900-01-01') - INTERVAL 2 DAYS
    FROM silver.customers
);

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dedup_customers AS

SELECT
    card_id,
    customer_id,
    firstname,
    lastname,
    email,
    address,
    birthday,
    country,
    created_at
FROM (
    SELECT
        card_id,
        customer_id,
        firstname,
        lastname,
        email,
        address,
        birthday,
        country,
        created_at,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY created_at DESC
        ) AS rn
    FROM bronze_incremental
)
WHERE rn = 1;

-- COMMAND ----------

MERGE INTO silver.customers AS target
USING dedup_customers AS source
ON target.customer_id = source.customer_id

WHEN MATCHED AND target.created_at < source.created_at THEN
UPDATE SET
    target.firstname = source.firstname,
    target.lastname = source.lastname,
    target.email = source.email,
    target.address = source.address,
    target.birthday = source.birthday,
    target.country = source.country,
    target.created_at = source.created_at

WHEN NOT MATCHED THEN
INSERT (
    card_id,
    customer_id,
    firstname,
    lastname,
    email,
    address,
    birthday,
    country,
    created_at
)
VALUES (
    source.card_id,
    source.customer_id,
    source.firstname,
    source.lastname,
    source.email,
    source.address,
    source.birthday,
    source.country,
    source.created_at
);

-- COMMAND ----------

OPTIMIZE silver.customers
ZORDER BY (customer_id);