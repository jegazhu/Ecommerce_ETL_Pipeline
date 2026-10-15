# Crear la Estructura del Proyecto en Databricks

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime

# Ruta de tu archivo subido al Volume
csv_path = "/Volumes/workspace/default/ecommerce_data/ecommerce_sales_analytics_5000.csv"

# Rutas de destino para las tablas Delta en el Volume
delta_bronze_path = "/Volumes/workspace/default/ecommerce_data/delta/bronze"
delta_silver_path = "/Volumes/workspace/default/ecommerce_data/delta/silver"
delta_gold_path = "/Volumes/workspace/default/ecommerce_data/delta/gold"

print("✅ Rutas configuradas correctamente.")

# Capa Bronze (Ingesta Cruda)
# Leer CSV como Bronze (todo como string para preservar la data cruda)
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
df_bronze.write.format("delta").mode("overwrite").save(delta_bronze_path)
print(f"✅ Capa Bronze guardada en: {delta_bronze_path}")

# Capa Silver (Limpieza y Normalización)

# Leer desde Bronze
df_silver = spark.read.format("delta").load(delta_bronze_path)
from pyspark.sql.functions import coalesce, try_to_date

# --- 1. Limpieza general: trim a todas las columnas de texto ---
for c in df_silver.columns:
    df_silver = df_silver.withColumn(c, trim(col(c)))

# --- 2. Identificar y transformar columnas monetarias y numéricas ---
# Buscamos columnas que probablemente sean montos o cantidades
numeric_cols = [c for c in df_silver.columns if any(keyword in c.lower() for keyword in ['amount', 'price', 'quantity', 'total', 'discount', 'sales'])]

for c in numeric_cols:
    # Quitar símbolos de moneda y comas, luego castear a Double
    df_silver = df_silver.withColumn(c, regexp_replace(col(c), r"[\$,]", "").cast(DoubleType()))
    print(f"✅ Convertida a Double: {c}")

# --- 3. Parsear columnas de fecha con tolerancia a múltiples formatos ---
date_cols = [c for c in df_silver.columns if 'date' in c.lower()]

# Formatos comunes que puede traer el dataset
date_formats = [
    "M/d/yyyy",      # 1/1/2022  (el que está fallando)
    "MM/dd/yyyy",    # 01/01/2022
    "yyyy-MM-dd",    # 2022-01-01
    "dd-MM-yyyy",    # 01-01-2022
    "yyyy/MM/dd",    # 2022/01/01
]

for c in date_cols:
    # Intentar cada formato y quedarnos con el primero que no sea NULL
    parsed = coalesce(*[try_to_date(col(c), fmt) for fmt in date_formats])
    df_silver = df_silver.withColumn(c, parsed)
    print(f"✅ Convertida a Date: {c}")

# Diagnóstico: cuántos valores quedaron NULL después del parseo
for c in date_cols:
    null_count = df_silver.filter(col(c).isNull()).count()
    total = df_silver.count()
    print(f"   {c}: {null_count}/{total} valores NULL tras parseo")

# --- 4. Eliminar duplicados ---
before = df_silver.count()
df_silver = df_silver.dropDuplicates()
after = df_silver.count()
print(f"🧹 Duplicados eliminados: {before - after}")

# --- 5. Filtrar registros con datos faltantes en columnas clave ---
# Ejemplo: eliminar filas sin fecha de orden o sin monto total
for c in date_cols + numeric_cols:
    df_silver = df_silver.filter(col(c).isNotNull())

print(f"\n✅ Capa Silver guardada. Registros finales: {df_silver.count()}")
df_silver.printSchema()

# Guardar la tabla Silver
df_silver.write.format("delta").mode("overwrite").save(delta_silver_path)

# Capa Gold (Agregaciones para Análisis)

# Leer desde Silver
df_gold_base = spark.read.format("delta").load(delta_silver_path)

# --- Gold 1: Ventas y Pedidos por Mes ---
# Usamos la primera columna de fecha que encontramos
date_col_name = date_cols[0] 
total_col_name = next((c for c in numeric_cols if 'total' in c.lower() or 'amount' in c.lower()), numeric_cols[0])

df_ventas_mes = df_gold_base \
    .withColumn("anio", year(col(date_col_name))) \
    .withColumn("mes", month(col(date_col_name))) \
    .groupBy("anio", "mes") \
    .agg(
        sum(total_col_name).alias("ventas_totales"),
        count("*").alias("numero_pedidos"),
        round(avg(total_col_name), 2).alias("ticket_promedio")
    ) \
    .orderBy("anio", "mes")

print("=== Gold: Ventas por Mes ===")
df_ventas_mes.show(20, truncate=False)
df_ventas_mes.write.format("delta").mode("overwrite").save(f"{delta_gold_path}/ventas_por_mes")

# --- Gold 2: Top 10 Productos/Categorías por Ingresos ---
# Buscamos una columna categórica para agrupar
cat_col_name = next((c for c in df_gold_base.columns if any(keyword in c.lower() for keyword in ['product', 'category', 'segment'])), None)

if cat_col_name:
    df_top = df_gold_base \
        .groupBy(cat_col_name) \
        .agg(sum(total_col_name).alias("ingresos_totales")) \
        .orderBy(desc("ingresos_totales")) \
        .limit(10)
    
    print(f"\n=== Gold: Top 10 por {cat_col_name} ===")
    df_top.show(10, truncate=False)
    df_top.write.format("delta").mode("overwrite").save(f"{delta_gold_path}/top_productos")

print(f"\n✅ Capa Gold guardada en: {delta_gold_path}")

# Listar el contenido del Volume
dbutils.fs.ls("/Volumes/workspace/default/ecommerce_data/delta/")

# Consultar la tabla Gold de ventas por mes
df_ventas = spark.read.format("delta").load("/Volumes/workspace/default/ecommerce_data/delta/gold/ventas_por_mes")
df_ventas.show(20, truncate=False)

# Consultar la tabla Gold de top productos
df_top = spark.read.format("delta").load("/Volumes/workspace/default/ecommerce_data/delta/gold/top_productos")
df_top.show(10, truncate=False)

# Listar dentro de cada capa
print("=== BRONZE ===")
dbutils.fs.ls("/Volumes/workspace/default/ecommerce_data/delta/bronze/")

print("\n=== SILVER ===")
dbutils.fs.ls("/Volumes/workspace/default/ecommerce_data/delta/silver/")

print("\n=== GOLD ===")
dbutils.fs.ls("/Volumes/workspace/default/ecommerce_data/delta/gold/")

# Borrar solo la capa Gold (mantiene Bronze y Silver intactos)
dbutils.fs.rm("/Volumes/workspace/default/ecommerce_data/delta/gold/", recurse=True)

# Verificar que se borró
dbutils.fs.ls("/Volumes/workspace/default/ecommerce_data/delta/")

# Gold mejorado con las columnas reales
from pyspark.sql.functions import *
from pyspark.sql.window import Window

delta_silver_path = "/Volumes/workspace/default/ecommerce_data/delta/silver"
delta_gold_path = "/Volumes/workspace/default/ecommerce_data/delta/gold"

df = spark.read.format("delta").load(delta_silver_path)

# ============================================================
# GOLD 1: Ventas por mes (tendencia temporal)
# ============================================================
df_ventas_mes = df \
    .withColumn("anio", year("order_date")) \
    .withColumn("mes", month("order_date")) \
    .groupBy("anio", "mes") \
    .agg(
        round(sum("revenue"), 2).alias("ventas_totales"),
        count("order_id").alias("numero_pedidos"),
        round(avg("revenue"), 2).alias("ticket_promedio"),
        sum("quantity").alias("unidades_vendidas")
    ) \
    .orderBy("anio", "mes")

df_ventas_mes.write.format("delta").mode("overwrite").save(f"{delta_gold_path}/ventas_por_mes")
print("=== Gold 1: Ventas por mes ===")
df_ventas_mes.show(20, truncate=False)


# ============================================================
# GOLD 2: Top 10 categorías por ingresos
# ============================================================
df_top_categorias = df \
    .groupBy("product_category") \
    .agg(
        round(sum("revenue"), 2).alias("ingresos_totales"),
        count("order_id").alias("numero_pedidos"),
        round(avg("customer_rating"), 2).alias("rating_promedio"),
        sum("quantity").alias("unidades_vendidas")
    ) \
    .orderBy(desc("ingresos_totales")) \
    .limit(10)

df_top_categorias.write.format("delta").mode("overwrite").save(f"{delta_gold_path}/top_categorias")
print("\n=== Gold 2: Top 10 categorías ===")
df_top_categorias.show(10, truncate=False)


# ============================================================
# GOLD 3: Ventas por región
# ============================================================
df_ventas_region = df \
    .groupBy("region") \
    .agg(
        round(sum("revenue"), 2).alias("ingresos_totales"),
        count("order_id").alias("numero_pedidos"),
        round(avg("revenue"), 2).alias("ticket_promedio"),
        round(avg("delivery_days"), 1).alias("dias_entrega_promedio")
    ) \
    .orderBy(desc("ingresos_totales"))

df_ventas_region.write.format("delta").mode("overwrite").save(f"{delta_gold_path}/ventas_por_region")
print("\n=== Gold 3: Ventas por región ===")
df_ventas_region.show(truncate=False)


# ============================================================
# GOLD 4: Métodos de pago más usados
# ============================================================
df_payment = df \
    .groupBy("payment_method") \
    .agg(
        count("order_id").alias("numero_transacciones"),
        round(sum("revenue"), 2).alias("ingresos_totales"),
        round(avg("revenue"), 2).alias("ticket_promedio")
    ) \
    .orderBy(desc("numero_transacciones"))

df_payment.write.format("delta").mode("overwrite").save(f"{delta_gold_path}/metodos_pago")
print("\n=== Gold 4: Métodos de pago ===")
df_payment.show(truncate=False)


# ============================================================
# GOLD 5: Análisis de descuentos vs rating
# ============================================================
df_descuentos = df \
    .withColumn("rango_descuento",
        when(col("discount") == 0, "Sin descuento")
        .when(col("discount") <= 0.1, "1-10%")
        .when(col("discount") <= 0.2, "11-20%")
        .when(col("discount") <= 0.3, "21-30%")
        .otherwise(">30%")
    ) \
    .groupBy("rango_descuento") \
    .agg(
        count("order_id").alias("numero_pedidos"),
        round(avg("customer_rating"), 2).alias("rating_promedio"),
        round(avg("revenue"), 2).alias("revenue_promedio"),
        round(sum("revenue"), 2).alias("ingresos_totales")
    ) \
    .orderBy("rango_descuento")

df_descuentos.write.format("delta").mode("overwrite").save(f"{delta_gold_path}/analisis_descuentos")
print("\n=== Gold 5: Descuentos vs Rating ===")
df_descuentos.show(truncate=False)

print("\n✅ Las 5 tablas Gold fueron creadas exitosamente.")

# Ventas por mes
df_ventas = spark.read.format("delta").load("/Volumes/workspace/default/ecommerce_data/delta/gold/ventas_por_mes")
print(f"Filas en ventas_por_mes: {df_ventas.count()}")
df_ventas.orderBy("anio", "mes").show(20, truncate=False)

# Top productos
df_top = spark.read.format("delta").load("/Volumes/workspace/default/ecommerce_data/delta/gold/top_productos")
print(f"Filas en top_productos: {df_top.count()}")
df_top.show(10, truncate=False)

# Resumen de Silver
df_silver = spark.read.format("delta").load("/Volumes/workspace/default/ecommerce_data/delta/silver")
print(f"Filas en Silver: {df_silver.count()}")
print(f"Columnas: {df_silver.columns}")
df_silver.show(5, truncate=False)

from pyspark.sql.functions import col, count, when

# Contar nulos por columna en Silver
null_counts = df_silver.select([
    count(when(col(c).isNull(), c)).alias(c) for c in df_silver.columns
])

print("=== Valores nulos por columna en Silver ===")
null_counts.show(truncate=False, vertical=True)

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# ============================================================
# Leer desde Silver (tabla registrada en Unity Catalog)
# ============================================================
df = spark.table("workspace.default.silver_ecommerce_sales")
print(f"✅ Leídos {df.count()} registros desde Silver\n")


# ============================================================
# GOLD 1: Ventas por mes (tendencia temporal)
# ============================================================
df_ventas_mes = df \
    .withColumn("anio", year("order_date")) \
    .withColumn("mes", month("order_date")) \
    .groupBy("anio", "mes") \
    .agg(
        round(sum("revenue"), 2).alias("ventas_totales"),
        count("order_id").alias("numero_pedidos"),
        round(avg("revenue"), 2).alias("ticket_promedio"),
        sum("quantity").alias("unidades_vendidas")
    ) \
    .orderBy("anio", "mes")

df_ventas_mes.write.format("delta") \
    .mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.gold_ventas_por_mes")
print(f"✅ Gold 1: ventas_por_mes ({df_ventas_mes.count()} filas)")


# ============================================================
# GOLD 2: Top 10 categorías por ingresos
# ============================================================
df_top_categorias = df \
    .groupBy("product_category") \
    .agg(
        round(sum("revenue"), 2).alias("ingresos_totales"),
        count("order_id").alias("numero_pedidos"),
        round(avg("customer_rating"), 2).alias("rating_promedio"),
        sum("quantity").alias("unidades_vendidas")
    ) \
    .orderBy(desc("ingresos_totales")) \
    .limit(10)

df_top_categorias.write.format("delta") \
    .mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.gold_top_categorias")
print(f"✅ Gold 2: top_categorias ({df_top_categorias.count()} filas)")


# ============================================================
# GOLD 3: Ventas por región
# ============================================================
df_ventas_region = df \
    .groupBy("region") \
    .agg(
        round(sum("revenue"), 2).alias("ingresos_totales"),
        count("order_id").alias("numero_pedidos"),
        round(avg("revenue"), 2).alias("ticket_promedio"),
        round(avg("delivery_days"), 1).alias("dias_entrega_promedio")
    ) \
    .orderBy(desc("ingresos_totales"))

df_ventas_region.write.format("delta") \
    .mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.gold_ventas_por_region")
print(f"✅ Gold 3: ventas_por_region ({df_ventas_region.count()} filas)")


# ============================================================
# GOLD 4: Métodos de pago
# ============================================================
df_metodos_pago = df \
    .groupBy("payment_method") \
    .agg(
        count("order_id").alias("numero_transacciones"),
        round(sum("revenue"), 2).alias("ingresos_totales"),
        round(avg("revenue"), 2).alias("ticket_promedio")
    ) \
    .orderBy(desc("numero_transacciones"))

df_metodos_pago.write.format("delta") \
    .mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.gold_metodos_pago")
print(f"✅ Gold 4: metodos_pago ({df_metodos_pago.count()} filas)")


# ============================================================
# GOLD 5: Análisis de descuentos vs rating y revenue
# ============================================================
df_descuentos = df \
    .withColumn("rango_descuento",
        when(col("discount") == 0, "Sin descuento")
        .when(col("discount") <= 0.1, "1-10%")
        .when(col("discount") <= 0.2, "11-20%")
        .when(col("discount") <= 0.3, "21-30%")
        .otherwise(">30%")
    ) \
    .groupBy("rango_descuento") \
    .agg(
        count("order_id").alias("numero_pedidos"),
        round(avg("customer_rating"), 2).alias("rating_promedio"),
        round(avg("revenue"), 2).alias("revenue_promedio"),
        round(sum("revenue"), 2).alias("ingresos_totales")
    ) \
    .orderBy("rango_descuento")

df_descuentos.write.format("delta") \
    .mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.gold_analisis_descuentos")
print(f"✅ Gold 5: analisis_descuentos ({df_descuentos.count()} filas)")


# ============================================================
# Resumen
# ============================================================
print("\n" + "=" * 60)
print("✅ Las 5 tablas Gold fueron creadas y registradas")
print("=" * 60)
print("\nMuestra de resultados:\n")
print("--- Ventas por mes ---")
df_ventas_mes.show(5, truncate=False)
print("--- Top categorías ---")
df_top_categorias.show(5, truncate=False)
print("--- Ventas por región ---")
df_ventas_region.show(truncate=False)
