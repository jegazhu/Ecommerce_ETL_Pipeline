from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import coalesce, try_to_date

delta_bronze_path = "/Volumes/workspace/default/ecommerce_data/delta/bronze"
print("✅ Configuración lista")

df_silver = spark.read.format("delta").load(delta_bronze_path)
print(f"Registros leídos de Bronze: {df_silver.count()}")
df_silver.printSchema()

for c in df_silver.columns:
    df_silver = df_silver.withColumn(c, trim(col(c)))

print("✅ Trim aplicado a todas las columnas")

numeric_cols = [c for c in df_silver.columns if any(
    keyword in c.lower() for keyword in ['amount', 'price', 'quantity', 'total', 'discount', 'sales', 'revenue', 'rating', 'days']
)]

for c in numeric_cols:
    df_silver = df_silver.withColumn(c, regexp_replace(col(c), r"[\$,]", "").cast(DoubleType()))
    print(f"✅ Convertida a Double: {c}")

date_cols = [c for c in df_silver.columns if 'date' in c.lower()]

date_formats = [
    "M/d/yyyy",
    "MM/dd/yyyy",
    "yyyy-MM-dd",
    "dd-MM-yyyy",
    "yyyy/MM/dd",
]

for c in date_cols:
    parsed = coalesce(*[try_to_date(col(c), fmt) for fmt in date_formats])
    df_silver = df_silver.withColumn(c, parsed)
    print(f"✅ Convertida a Date: {c}")

# Diagnóstico de nulos tras el parseo
for c in date_cols:
    null_count = df_silver.filter(col(c).isNull()).count()
    total = df_silver.count()
    print(f"   {c}: {null_count}/{total} valores NULL tras parseo")

before = df_silver.count()
df_silver = df_silver.dropDuplicates()
after = df_silver.count()
print(f"🧹 Duplicados eliminados: {before - after}")

for c in date_cols + numeric_cols:
    df_silver = df_silver.filter(col(c).isNotNull())

print(f"✅ Registros finales en Silver: {df_silver.count()}")

df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.silver_ecommerce_sales")

print("✅ Tabla Silver registrada en Unity Catalog")
