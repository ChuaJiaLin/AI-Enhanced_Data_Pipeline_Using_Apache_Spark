import psycopg2

conn = psycopg2.connect(
    host="fashion_postgres",
    database="fashion_dw",
    user="admin",
    password="admin"
)

cur = conn.cursor()

# Remove existing foreign keys
cur.execute("""
ALTER TABLE fact_sales DROP CONSTRAINT IF EXISTS fk_customer;
ALTER TABLE fact_sales DROP CONSTRAINT IF EXISTS fk_product;
ALTER TABLE fact_sales DROP CONSTRAINT IF EXISTS fk_store;
ALTER TABLE fact_sales DROP CONSTRAINT IF EXISTS fk_date;
""")

# Remove existing primary keys
cur.execute("""
ALTER TABLE dim_customer DROP CONSTRAINT IF EXISTS dim_customer_pkey;
ALTER TABLE dim_product DROP CONSTRAINT IF EXISTS dim_product_pkey;
ALTER TABLE dim_store DROP CONSTRAINT IF EXISTS dim_store_pkey;
ALTER TABLE dim_date DROP CONSTRAINT IF EXISTS dim_date_pkey;
""")

# Add primary keys
cur.execute("""
ALTER TABLE dim_customer
ADD PRIMARY KEY (customer_id);
""")

cur.execute("""
ALTER TABLE dim_product
ADD PRIMARY KEY (product_id);
""")

cur.execute("""
ALTER TABLE dim_store
ADD PRIMARY KEY (store_id);
""")

cur.execute("""
ALTER TABLE dim_date
ADD PRIMARY KEY (date);
""")

# Add foreign keys
cur.execute("""
ALTER TABLE fact_sales
ADD CONSTRAINT fk_customer
FOREIGN KEY (customer_id)
REFERENCES dim_customer(customer_id);
""")

cur.execute("""
ALTER TABLE fact_sales
ADD CONSTRAINT fk_product
FOREIGN KEY (product_id)
REFERENCES dim_product(product_id);
""")

cur.execute("""
ALTER TABLE fact_sales
ADD CONSTRAINT fk_store
FOREIGN KEY (store_id)
REFERENCES dim_store(store_id);
""")

cur.execute("""
ALTER TABLE fact_sales
ADD CONSTRAINT fk_date
FOREIGN KEY (date)
REFERENCES dim_date(date);
""")

conn.commit()

cur.close()
conn.close()

print("Star schema relationships created successfully.")