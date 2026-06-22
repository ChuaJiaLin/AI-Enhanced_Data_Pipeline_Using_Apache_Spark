from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Fashion CSV to Bronze Parquet") \
    .getOrCreate()

raw_path = "/app/data/raw"
bronze_path = "/app/data/bronze"

csv_files = [
    "transactions.csv",
    "customers.csv",
    "products.csv",
    "stores.csv"
]

for file in csv_files:
    name = file.replace(".csv", "")
    print(f"Reading {file}...")

    df = spark.read.csv(
        f"{raw_path}/{file}",
        header=True,
        inferSchema=True
    )

    df.printSchema()
    df.show(5)

    output_path = f"{bronze_path}/{name}"
    df.write.mode("overwrite").parquet(output_path)

    print(f"Saved to {output_path}")

spark.stop()