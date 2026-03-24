-- Databricks notebook source
-- CARGA INICIAL DA FATO
INSERT INTO gold.fact_transactions (
    customer_sk,
    date_sk,
    product_sk,
    price,
    transaction_hash
)
SELECT
    COALESCE(dc.customer_sk, -1) AS customer_sk,
    dd.date_sk,
    COALESCE(dp.product_sk, -1) AS product_sk,
    t.price,
    md5(concat_ws('|',
        t.customer_id,
        t.product_id,
        t.transaction_date,
        t.price
    )) AS transaction_hash
FROM silver.card_transactions t
LEFT JOIN gold.dim_customer dc
    ON t.customer_id = dc.customer_id
    AND t.transaction_date >= DATE(dc.start_date)
    AND t.transaction_date < DATE(dc.end_date)
JOIN gold.dim_date dd
    ON t.transaction_date = dd.date
LEFT JOIN gold.dim_product dp
    ON CAST(t.product_id AS STRING) = dp.product_id;

-- COMMAND ----------

MERGE INTO gold.fact_transactions AS target
USING (
    SELECT
        COALESCE(dc.customer_sk, -1) AS customer_sk,
        dd.date_sk,
        dp.product_sk,
        t.price,
        md5(concat_ws('|',
            t.customer_id,
            t.product_id,
            t.transaction_date,
            t.price
        )) AS transaction_hash
    FROM silver.card_transactions t
    LEFT JOIN gold.dim_customer dc
        ON t.customer_id = dc.customer_id
        AND t.transaction_date >= DATE(dc.start_date)
        AND t.transaction_date < DATE(dc.end_date)
    JOIN gold.dim_date dd
        ON t.transaction_date = dd.date
    JOIN gold.dim_product dp
        ON CAST(t.product_id AS STRING) = dp.product_id
) AS source
ON target.transaction_hash = source.transaction_hash

WHEN NOT MATCHED THEN
INSERT (
    customer_sk,
    date_sk,
    product_sk,
    price,
    transaction_hash
)
VALUES (
    source.customer_sk,
    source.date_sk,
    source.product_sk,
    source.price,
    source.transaction_hash
);