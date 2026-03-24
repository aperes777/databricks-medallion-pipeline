%python

from datetime import timedelta

dbutils.widgets.text("postgres_password", "")
password = dbutils.widgets.get("postgres_password")

jdbc_url = "jdbc:postgresql://ep-rough-sun-ai8hkkuu-pooler.c-4.us-east-1.aws.neon.tech/neondb?sslmode=require"

last_timestamp = spark.sql("""
SELECT COALESCE(MAX(created_at), '1900-01-01')
FROM bronze.products
""").first()[0]

lookback_timestamp = last_timestamp - timedelta(days=2)
lookback_timestamp_str = lookback_timestamp.strftime("%Y-%m-%d %H:%M:%S")

print(f"Last timestamp: {last_timestamp}")
print(f"Lookback timestamp: {lookback_timestamp}")

query = f"""
(
SELECT  product_id,
        product_name,
        created_at
FROM oltp.products
WHERE created_at >= '{lookback_timestamp_str}'
) AS new_products
"""

df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", query)
    .option("user", "neondb_owner")
    .option("password", "npg_VzTCyIa8rc0U")
   ## .option("password", 'password')
    .option("driver", "org.postgresql.Driver")
    .load()
)

##display(df)

df.write \
.format("delta") \
.mode("append") \
.saveAsTable("bronze.products") 