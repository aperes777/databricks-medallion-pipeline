-- Databricks notebook source
INSERT INTO gold.dim_product (product_id)

SELECT
    s.product_id
FROM silver.products s
LEFT JOIN gold.dim_product d
ON s.product_id = d.product_id
WHERE d.product_id IS NULL;

-- COMMAND ----------

select * from gold.dim_product;