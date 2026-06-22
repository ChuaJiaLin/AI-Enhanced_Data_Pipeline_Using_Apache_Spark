from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, quarter, dayofmonth

spark = SparkSession.builder \
    .appName("Fashion Gold ETL") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

silver_path = "/app/data/silver"
gold_path = "/app/data/gold"

customers = spark.read.parquet(f"{silver_path}/customers")
products = spark.read.parquet(f"{silver_path}/products")
stores = spark.read.parquet(f"{silver_path}/stores")
transactions = spark.read.parquet(f"{silver_path}/transactions")

# Dimension tables
dim_customer = customers.dropDuplicates(["customer_id"])
dim_product = products.dropDuplicates(["product_id"])
dim_store = stores.dropDuplicates(["store_id"])

# Convert transaction date
date_col = "date"

if date_col in transactions.columns:
    transactions = transactions.withColumn("date", to_date(col("date")))

    dim_date = transactions.select("date") \
        .dropna() \
        .dropDuplicates(["date"]) \
        .withColumn("year", year(col("date"))) \
        .withColumn("month", month(col("date"))) \
        .withColumn("quarter", quarter(col("date"))) \
        .withColumn("day", dayofmonth(col("date")))
else:
    dim_date = None
    print("No date column found in transactions.")

# Validate fact table using inner joins
before_fact_count = transactions.count()

fact_sales = transactions \
    .join(dim_customer.select("customer_id"), "customer_id", "inner") \
    .join(dim_product.select("product_id"), "product_id", "inner") \
    .join(dim_store.select("store_id"), "store_id", "inner")

if dim_date is not None:
    fact_sales = fact_sales.join(
        dim_date.select("date"),
        "date",
        "inner"
    )

after_fact_count = fact_sales.count()

print(f"Fact rows before validation: {before_fact_count}")
print(f"Fact rows after validation: {after_fact_count}")
print(f"Invalid fact rows removed: {before_fact_count - after_fact_count}")

# Save Gold tables
dim_customer.coalesce(2).write.mode("overwrite").parquet(f"{gold_path}/dim_customer")
dim_product.coalesce(2).write.mode("overwrite").parquet(f"{gold_path}/dim_product")
dim_store.coalesce(2).write.mode("overwrite").parquet(f"{gold_path}/dim_store")
fact_sales.coalesce(2).write.mode("overwrite").parquet(f"{gold_path}/fact_sales")

if dim_date is not None:
    dim_date.coalesce(1).write.mode("overwrite").parquet(f"{gold_path}/dim_date")

print("Gold layer created successfully.")

spark.stop()