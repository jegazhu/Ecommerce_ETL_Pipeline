# Capa Bronze (Ingesta Cruda)
# Leer CSV como Bronze (todo como string para preservar la data cruda)
# Ruta de tu archivo subido al Volume
csv_path = "/Volumes/workspace/default/ecommerce_data/ecommerce_sales_analytics_5000.csv"

df_bronze = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .option("multiLine", "true") \
    .option("escape", '"') \
    .load(csv_path)

print(f"Registros en Bronze: {df_bronze.count()}")
df_bronze.printSchema()
df_bronze.show(5, truncate=False)

# Guardar la tabla en formato Delta (sobrescribiendo si ya existe)
# Rutas de destino para las tablas Delta en el Volume
delta_bronze_path = "/Volumes/workspace/default/ecommerce_data/delta/bronze"
df_bronze.write.format("delta").mode("overwrite").save(delta_bronze_path)
print(f"✅ Capa Bronze guardada en: {delta_bronze_path}")
