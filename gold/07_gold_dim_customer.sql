# Databricks notebook source
# MAGIC %sql
# MAGIC --CARGA INICIAL NA GOLD.DIM_CUSTOMER 
# MAGIC /*INSERT INTO gold.dim_customer (
# MAGIC     customer_id,
# MAGIC     card_id,
# MAGIC     firstname,
# MAGIC     lastname,
# MAGIC     email,
# MAGIC     address,
# MAGIC     birthday,
# MAGIC     country,
# MAGIC     customer_hash,
# MAGIC     created_at,
# MAGIC     start_date,
# MAGIC     end_date,
# MAGIC     is_current
# MAGIC )
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     card_id,
# MAGIC     firstname,
# MAGIC     lastname,
# MAGIC     email,
# MAGIC     address,
# MAGIC     birthday,
# MAGIC     country,
# MAGIC     md5(concat_ws('|',
# MAGIC         firstname,
# MAGIC         lastname,
# MAGIC         email,
# MAGIC         address,
# MAGIC         birthday,
# MAGIC         country
# MAGIC     )) AS customer_hash,
# MAGIC     created_at,
# MAGIC     created_at AS start_date,
# MAGIC     TIMESTAMP('9999-12-31 23:59:59') AS end_date,
# MAGIC     'Y' AS is_current
# MAGIC FROM silver.customers;*/

# COMMAND ----------

# MAGIC %sql
# MAGIC --CRIA HASH NA DIMENSAO
# MAGIC CREATE OR REPLACE TEMP VIEW vw_customer_source AS
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     card_id,
# MAGIC     firstname,
# MAGIC     lastname,
# MAGIC     email,
# MAGIC     address,
# MAGIC     birthday,
# MAGIC     country,
# MAGIC     created_at,
# MAGIC     md5(concat_ws('|',
# MAGIC         firstname,
# MAGIC         lastname,
# MAGIC         email,
# MAGIC         address,
# MAGIC         birthday,
# MAGIC         country
# MAGIC     )) AS customer_hash
# MAGIC FROM silver.customers;
# MAGIC --where customer_id = 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC --DETECTA MUDANÇAS EM ATRIBUTOS
# MAGIC CREATE OR REPLACE TEMP VIEW vw_customer_changes AS
# MAGIC SELECT
# MAGIC     source.customer_id,
# MAGIC     source.card_id,
# MAGIC     source.firstname,
# MAGIC     source.lastname,
# MAGIC     source.email,
# MAGIC     source.address,
# MAGIC     source.birthday,
# MAGIC     source.country,
# MAGIC     source.created_at,
# MAGIC     source.customer_hash
# MAGIC FROM vw_customer_source source
# MAGIC LEFT JOIN gold.dim_customer target
# MAGIC     ON source.customer_id = target.customer_id
# MAGIC     AND target.is_current = 'Y'
# MAGIC WHERE
# MAGIC     target.customer_id IS NULL
# MAGIC     OR COALESCE(target.customer_hash,'') <> COALESCE(source.customer_hash,'');
# MAGIC --AND source.customer_id = 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC --FECHA REGISTROS ANTIGOS
# MAGIC MERGE INTO gold.dim_customer AS target
# MAGIC USING vw_customer_changes AS source
# MAGIC ON target.customer_id = source.customer_id
# MAGIC AND target.is_current = 'Y'
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC     target.end_date = source.created_at,
# MAGIC     target.is_current = 'N';

# COMMAND ----------

# MAGIC %sql
# MAGIC --INSERE REGISTROS NOVOS COM NOVA DATA DE VIGÊNCIA
# MAGIC WITH customer_change AS (
# MAGIC     SELECT
# MAGIC         customer_id,
# MAGIC         card_id,
# MAGIC         firstname,
# MAGIC         lastname,
# MAGIC         email,
# MAGIC         address,
# MAGIC         birthday,
# MAGIC         country,
# MAGIC         customer_hash,
# MAGIC         created_at
# MAGIC     FROM (
# MAGIC         SELECT  customer_id,
# MAGIC                 card_id,
# MAGIC                 firstname,
# MAGIC                 lastname,
# MAGIC                 email,
# MAGIC                 address,
# MAGIC                 birthday,
# MAGIC                 country,
# MAGIC                 created_at,
# MAGIC                 customer_hash,
# MAGIC                 ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at DESC
# MAGIC             ) AS rn
# MAGIC         FROM vw_customer_changes
# MAGIC     ) t
# MAGIC     WHERE rn = 1
# MAGIC )
# MAGIC INSERT INTO gold.dim_customer (
# MAGIC     customer_id,
# MAGIC     card_id,
# MAGIC     firstname,
# MAGIC     lastname,
# MAGIC     email,
# MAGIC     address,
# MAGIC     birthday,
# MAGIC     country,
# MAGIC     customer_hash,
# MAGIC     created_at,
# MAGIC     start_date,
# MAGIC     end_date,
# MAGIC     is_current
# MAGIC )
# MAGIC SELECT
# MAGIC     source.customer_id,
# MAGIC     source.card_id,
# MAGIC     source.firstname,
# MAGIC     source.lastname,
# MAGIC     source.email,
# MAGIC     source.address,
# MAGIC     source.birthday,
# MAGIC     source.country,
# MAGIC     source.customer_hash,
# MAGIC     source.created_at,
# MAGIC     source.created_at AS start_date,
# MAGIC     TIMESTAMP('9999-12-31 23:59:59') AS end_date,
# MAGIC     'Y' AS is_current
# MAGIC FROM customer_change source
# MAGIC LEFT JOIN gold.dim_customer target
# MAGIC     ON source.customer_id = target.customer_id
# MAGIC     AND target.is_current = 'Y'
# MAGIC WHERE target.customer_id IS NULL;