from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Load Gold Tables to PostgreSQL") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

gold_path = "/app/data/gold"

jdbc_url = "jdbc:postgresql://postgres:5432/fashion_dw"

db_properties = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

gold_tables = [
    "dim_customer",
    "dim_product",
    "dim_store",
    "dim_date",
    "fact_sales"
]

for table in gold_tables:
    print(f"Loading {table} to PostgreSQL...")

    df = spark.read.parquet(f"{gold_path}/{table}")

    df.write \
        .mode("overwrite") \
        .jdbc(
            url=jdbc_url,
            table=table,
            properties=db_properties
        )

    print(f"Loaded {table}")

spark.stop()
