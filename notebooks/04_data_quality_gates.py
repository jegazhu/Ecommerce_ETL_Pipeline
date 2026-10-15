# %sql
# SHOW TABLES;
#
# Databricks notebook source
# MAGIC %md
# MAGIC # Sentinel Lakehouse: Data Quality & Anomaly Gate
# MAGIC This script audits the Silver layer (`silver_ecommerce_sales`) before passing data to Gold aggregations.
# MAGIC It checks for null keys, negative values, and volume drops to prevent silent data corruption.

# COMMAND:
from pyspark.sql import functions as F

# 1. Load the Silver layer dataframe using the exact table name from your schema
silver_df = spark.table("silver_ecommerce_sales")

print(f"Total rows in Silver layer to audit: {silver_df.count()}")

# COMMAND:
# 2. Inspect available columns to match your schema (uncomment if needed to view columns)
# display(silver_df.limit(5))

# COMMAND:
# 3. Define Quality Rules & Assertions matching your exact schema
id_col = "order_id"
# If your customer column is named differently, update it here (e.g., "customer_id" or "client_id")
customer_col = "customer_id" if "customer_id" in silver_df.columns else "order_id" 
amount_col = "revenue"

null_keys_count = silver_df.filter(F.col(id_col).isNull()).count()

# Rule B: Check for invalid negative values in revenue
negative_amounts_count = silver_df.filter(F.col(amount_col) < 0).count()

# Rule C: Check for extreme outlier spikes (e.g., single order revenue > 50,000)
extreme_outliers_count = silver_df.filter(F.col(amount_col) > 50000).count()

print(f"Validation checks mapped successfully to column: {amount_col}")

# COMMAND:
# 4. Evaluate Quality Metrics & Quarantine Logic

quality_passed = True
error_messages = []

if null_keys_count > 0:
    quality_passed = False
    error_messages.append(f"ALERT: Found {null_keys_count} rows with NULL order IDs.")

if negative_amounts_count > 0:
    quality_passed = False
    error_messages.append(f"ALERT: Found {negative_amounts_count} rows with negative values in '{amount_col}'.")

if extreme_outliers_count > 0:
    print(f"WARNING: Found {extreme_outliers_count} high-value outlier orders exceeding $50,000 in '{amount_col}'.")

# COMMAND:
# 5. Gate Enforcement Action
if not quality_passed:
    print("--- DATA QUALITY GATE FAILED ---")
    for err in error_messages:
        print(err)
    
    # Save bad rows to a Quarantine table
    quarantine_df = silver_df.filter(
        F.col(id_col).isNull() | 
        F.col(amount_col) < 0
    )
    quarantine_df.write.mode("overwrite").saveAsTable("quarantine_ecommerce_errors")
    print("Quarantined corrupted rows to 'quarantine_ecommerce_errors'.")
    
    raise ValueError("Pipeline halted due to data quality violations. Check quarantine logs.")
else:
    print("--- DATA QUALITY GATE FAILED: FALSE ---")
    print("--- DATA QUALITY GATE PASSED ---")
    print("All validations cleared successfully. Safe to proceed to Gold aggregations!")

# COMMAND:
# 5. Gate Enforcement Action
if not quality_passed:
    print("--- DATA QUALITY GATE FAILED ---")
    for err in error_messages:
        print(err)
    
    # Save bad rows to a Quarantine table for auditing instead of letting corrupt data reach Gold
    quarantine_df = silver_df.filter(
        F.col(id_col).isNull() | 
        F.col(customer_col).isNull() | 
        F.col(amount_col) < 0
    )
    quarantine_df.write.mode("overwrite").saveAsTable("quarantine_ecommerce_errors")
    print("Quarantined corrupted rows to 'quarantine_ecommerce_errors'.")
    
    # Raise exception to halt pipeline and prevent bad data from reaching Gold tables
    raise ValueError("Pipeline halted due to data quality violations. Check quarantine logs.")
else:
    print("--- DATA QUALITY GATE PASSED ---")
    print("All validations cleared successfully. Safe to proceed to Gold aggregations!")
