-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW bronze_incremental_products AS

SELECT 
        product_id,
        product_name,
        created_at
FROM bronze.products
WHERE created_at >= (
    SELECT COALESCE(MAX(created_at), '1900-01-01') - INTERVAL 2 DAYS
    FROM silver.products
);

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dedup_transactions_products AS

SELECT
      product_id,
      product_name,
      created_at
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY product_id
               ORDER BY created_at DESC
           ) rn
    FROM bronze_incremental_products
)
WHERE rn = 1;

-- COMMAND ----------

MERGE INTO silver.products AS target
USING dedup_transactions_products AS source
ON target.product_id = source.product_id

WHEN MATCHED 
AND target.product_name <> source.product_name THEN
UPDATE SET
    target.product_name = source.product_name,
    target.created_at = source.created_at

WHEN NOT MATCHED THEN
INSERT (
    product_id,
    product_name,
    created_at
)
VALUES (
    source.product_id,
    source.product_name,
    source.created_at
);