%pip install faker

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType
from faker import Faker

fake = Faker()

# --- Faker UDFs ---------------------------------------------------------
# Note: .cast(IntegerType()) / .cast(DoubleType()) is important. Faker
# returns Python ints/floats, but being explicit avoids type surprises
# when Spark assembles the final DataFrame.
fake_order_id         = F.udf(lambda: f"ORD-{fake.random_int(min=100000, max=999999)}", StringType())
fake_customer_id      = F.udf(lambda: f"CUST-{fake.random_int(min=1000, max=9999)}", StringType())
fake_product_category = F.udf(lambda: fake.random_element(elements=('Electronics','Books','Home','Sports','Clothing')), StringType())
fake_region           = F.udf(lambda: fake.random_element(elements=('North','South','East','West')), StringType())
fake_payment_method   = F.udf(lambda: fake.random_element(elements=('Credit Card','PayPal','Gift Card','Boleto')), StringType())
fake_quantity         = F.udf(lambda: fake.random_int(min=1, max=5), IntegerType())
fake_unit_price       = F.udf(lambda: round(fake.random_number(digits=3) + fake.random.random(), 2), DoubleType())
fake_discount         = F.udf(lambda: round(fake.random.uniform(0, 0.25), 2), DoubleType())
fake_delivery_days    = F.udf(lambda: fake.random_int(min=1, max=15), IntegerType())
fake_customer_rating  = F.udf(lambda: fake.random_int(min=1, max=5), IntegerType())

# --- Build the DataFrame ------------------------------------------------
num_new_rows = 50000

new_data_df = spark.range(0, num_new_rows).select(
    fake_order_id().alias("order_id"),
    # FIX: cast the random day offset to INT before passing to date_add
    F.date_add(F.lit('2026-01-01'), (F.rand() * 365).cast("int")).cast("date").alias("order_date"),
    fake_customer_id().alias("customer_id"),
    fake_product_category().alias("product_category"),
    fake_region().alias("region"),
    fake_quantity().alias("quantity"),
    fake_unit_price().alias("unit_price"),
    fake_discount().alias("discount"),
    fake_payment_method().alias("payment_method"),
    fake_delivery_days().alias("delivery_days"),
    fake_customer_rating().alias("customer_rating")
)

# --- Derive revenue -----------------------------------------------------
new_data_df = new_data_df.withColumn(
    "revenue",
    F.round(
        (F.col("quantity") * F.col("unit_price")) * (1 - F.col("discount")),
        2
    )
)

# --- Sanity check before writing ---------------------------------------
print(f"Rows to insert: {new_data_df.count()}")
new_data_df.printSchema()
new_data_df.show(5, truncate=False)

# --- Append to the Delta table -----------------------------------------
new_data_df.write.mode("append").saveAsTable("default.silver_ecommerce_sales")

print(f"Successfully appended {num_new_rows} new records to default.silver_ecommerce_sales")
