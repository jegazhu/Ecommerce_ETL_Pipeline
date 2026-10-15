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
