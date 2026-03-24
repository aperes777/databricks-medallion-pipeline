%python

from datetime import timedelta

dbutils.widgets.text("postgres_password", "")
password = dbutils.widgets.get("postgres_password")

jdbc_url = "jdbc:postgresql://ep-rough-sun-ai8hkkuu-pooler.c-4.us-east-1.aws.neon.tech/neondb?sslmode=require"

last_timestamp = spark.sql("""
SELECT COALESCE(MAX(created_at), '1900-01-01')
FROM bronze.customers
""").first()[0]

lookback_timestamp = last_timestamp - timedelta(days=2)
lookback_timestamp_str = lookback_timestamp.strftime("%Y-%m-%d %H:%M:%S")

print(f"Last timestamp: {last_timestamp}")
print(f"Lookback timestamp: {lookback_timestamp}")

query = f"""
(
SELECT card_id,
        customer_id,
        lastname,
        firstname,
        email,
        address,
        birthday,
        country,
        created_at,
        updated_at
FROM oltp.customers
WHERE created_at >= '{lookback_timestamp_str}'
) AS new_customers
"""

df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", query)
    .option("user", "neondb_owner")
    .option("password", "xxxxxxxxxxxxxxxxxxxx")
    .option("driver", "org.postgresql.Driver")
    .load()
)

df.write \
.format("delta") \
.mode("append") \
.saveAsTable("bronze.customers")