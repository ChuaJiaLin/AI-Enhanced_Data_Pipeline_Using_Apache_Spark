'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, regexp_replace, udf
from pyspark.sql.types import StringType, BooleanType
from deep_translator import GoogleTranslator
import re

spark = SparkSession.builder \
    .appName("Fashion Silver ETL") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.default.parallelism", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

bronze_path = "/app/data/bronze"
silver_path = "/app/data/silver"

tables = [
    "customers",
    "stores",
    "products",
    "transactions"
]

translation_tables = [
    "stores",
    "customers"
]

translation_columns = [
    "store_name",
    "city",
    "country"
]

def has_chinese(text):
    if text is None:
        return False
    return bool(re.search(r"[\u4e00-\u9fff]", str(text)))

def translate_text(text):
    if text is None:
        return None

    text = str(text).strip()

    if not has_chinese(text):
        return text

    try:
        return GoogleTranslator(
            source="zh-CN",
            target="en"
        ).translate(text).strip()
    except Exception as e:
        print(f"Translation failed for {text}: {e}")
        return text

def standardize_column_names(df):
    for old_col in df.columns:
        new_col = old_col.strip().lower().replace(" ", "_")
        df = df.withColumnRenamed(old_col, new_col)
    return df

def clean_string_columns(df):
    string_cols = [
        field.name
        for field in df.schema.fields
        if field.dataType.simpleString() == "string"
    ]

    for c in string_cols:
        df = df.withColumn(c, trim(col(c)))
        df = df.withColumn(c, regexp_replace(col(c), r"\s+", " "))

    return df

def translate_distinct_values(df, column_name):
    if column_name not in df.columns:
        return df

    print(f"Checking Chinese values in column: {column_name}")

    before_chinese_count = df.filter(
        has_chinese_udf(col(column_name))
    ).count()

    print(f"Chinese records before translation in {column_name}: {before_chinese_count}")

    unique_values = [
        row[column_name]
        for row in df.select(column_name).distinct().collect()
        if row[column_name] is not None and has_chinese(row[column_name])
    ]

    print(f"Unique Chinese values found in {column_name}: {len(unique_values)}")

    if len(unique_values) == 0:
        return df

    translation_map = {}

    for value in unique_values:
        translated = translate_text(value)
        translation_map[value] = translated
        print(f"{value} -> {translated}")
    
    def map_translate(x):
        if x is None:
            return None
        return translation_map.get(x, x)

    map_udf = udf(map_translate, StringType())

    df = df.withColumn(column_name, map_udf(col(column_name)))

    after_chinese_count = df.filter(
        has_chinese_udf(col(column_name))
    ).count()

    print(f"Chinese records after translation in {column_name}: {after_chinese_count}")

    if before_chinese_count > 0:
        success_rate = ((before_chinese_count - after_chinese_count) / before_chinese_count) * 100
        print(f"Translation success rate for {column_name}: {success_rate:.2f}%")

    return df

def validate_translation(df, table_name):
    print(f"\nTranslation validation for {table_name}")

    for c in translation_columns:
        if c in df.columns:
            remaining_chinese = df.filter(
                has_chinese_udf(col(c))
            ).count()

            print(f"Remaining Chinese records in {c}: {remaining_chinese}")

    return df

has_chinese_udf = udf(has_chinese, BooleanType())

for table in tables:
    print(f"\n==============================")
    print(f"Processing table: {table}")
    print(f"==============================")

    df = spark.read.parquet(f"{bronze_path}/{table}")

    df = standardize_column_names(df)
    df = clean_string_columns(df)

    original_count = df.count()

    df = df.dropDuplicates()
    after_duplicate_count = df.count()

    df = df.dropna()
    after_null_count = df.count()

    if table in translation_tables:
        print(f"Translation enabled for {table}")

        for c in translation_columns:
            df = translate_distinct_values(df, c)
    else:
        print(f"Translation skipped for {table}")

    print(f"Original records: {original_count}")
    print(f"After duplicate removal: {after_duplicate_count}")
    print(f"Duplicates removed: {original_count - after_duplicate_count}")
    print(f"After null removal: {after_null_count}")
    print(f"Null records removed: {after_duplicate_count - after_null_count}")

    df.coalesce(2) \
        .write \
        .mode("overwrite") \
        .parquet(f"{silver_path}/{table}")

    print(f"Saved Silver table: {silver_path}/{table}")

spark.stop()
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, regexp_replace, udf
from pyspark.sql.types import StringType, BooleanType
import deepl
import os
import re

spark = SparkSession.builder \
    .appName("Fashion Silver ETL with DeepL AI Translation") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.default.parallelism", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

bronze_path = "/app/data/bronze"
silver_path = "/app/data/silver"

# DeepL API key
DEEPL_API_KEY = os.getenv("DEEPL_API_KEY")

if not DEEPL_API_KEY:
    raise ValueError("DEEPL_API_KEY is not set. Please set it in Docker environment.")

translator = deepl.Translator(DEEPL_API_KEY)

tables = [
    "customers",
    "stores",
    "products",
    "transactions"
]

translation_tables = [
    "customers",
    "stores"
]

translation_columns = [
    "store_name",
    "city",
    "country"
]

def has_non_english_characters(text):
    if text is None:
        return False

    text = str(text)

    # Detect Chinese characters and common accented European characters
    return bool(re.search(r"[\u4e00-\u9fffÀ-ÿ]", text))

def translate_text(text):
    if text is None:
        return None

    text = str(text).strip()

    if text == "":
        return text

    if not has_non_english_characters(text):
        return text

    try:
        result = translator.translate_text(
            text,
            target_lang="EN-US"
        )
        return result.text.strip()

    except Exception as e:
        print(f"Translation failed for {text}: {e}")
        return text

def standardize_column_names(df):
    for old_col in df.columns:
        new_col = old_col.strip().lower().replace(" ", "_")
        df = df.withColumnRenamed(old_col, new_col)
    return df

def clean_string_columns(df):
    string_cols = [
        field.name
        for field in df.schema.fields
        if field.dataType.simpleString() == "string"
    ]

    for c in string_cols:
        df = df.withColumn(c, trim(col(c)))
        df = df.withColumn(c, regexp_replace(col(c), r"\s+", " "))

    return df

def translate_distinct_values(df, column_name):
    if column_name not in df.columns:
        return df

    print(f"Checking multilingual values in column: {column_name}")

    before_count = df.filter(
        multilingual_udf(col(column_name))
    ).count()

    print(f"Multilingual records before translation in {column_name}: {before_count}")

    unique_values = [
        row[column_name]
        for row in df.select(column_name).distinct().collect()
        if row[column_name] is not None and has_non_english_characters(row[column_name])
    ]

    print(f"Unique multilingual values found in {column_name}: {len(unique_values)}")

    if len(unique_values) == 0:
        return df

    translation_map = {}

    for value in unique_values:
        translated = translate_text(value)
        translation_map[value] = translated
        print(f"{value} -> {translated}")

    def map_translate(x):
        if x is None:
            return None
        return translation_map.get(x, x)

    map_udf = udf(map_translate, StringType())

    df = df.withColumn(column_name, map_udf(col(column_name)))

    after_count = df.filter(
        multilingual_udf(col(column_name))
    ).count()

    print(f"Multilingual records after translation in {column_name}: {after_count}")

    if before_count > 0:
        success_rate = ((before_count - after_count) / before_count) * 100
        print(f"Translation success rate for {column_name}: {success_rate:.2f}%")

    return df

multilingual_udf = udf(has_non_english_characters, BooleanType())

for table in tables:
    print("\n==============================")
    print(f"Processing table: {table}")
    print("==============================")

    df = spark.read.parquet(f"{bronze_path}/{table}")

    # Data cleaning
    df = standardize_column_names(df)
    df = clean_string_columns(df)

    original_count = df.count()

    df = df.dropDuplicates()
    after_duplicate_count = df.count()

    df = df.dropna()
    after_null_count = df.count()

    # AI translation only for customers and stores
    if table in translation_tables:
        print(f"DeepL AI translation enabled for {table}")

        for c in translation_columns:
            df = translate_distinct_values(df, c)
    else:
        print(f"Translation skipped for {table}")

    print(f"Original records: {original_count}")
    print(f"After duplicate removal: {after_duplicate_count}")
    print(f"Duplicates removed: {original_count - after_duplicate_count}")
    print(f"After null removal: {after_null_count}")
    print(f"Null records removed: {after_duplicate_count - after_null_count}")

    df.coalesce(2) \
        .write \
        .mode("overwrite") \
        .parquet(f"{silver_path}/{table}")

    print(f"Saved Silver table: {silver_path}/{table}")

spark.stop()