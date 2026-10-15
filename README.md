# 🚀 Olist E-Commerce Medallion Lakehouse & Hybrid Enterprise Bridge

[![Databricks](https://img.shields.io/badge/Databricks-Serverless-FF3621?style=flat&logo=databricks&logoColor=white)](https://databricks.com/)
[![PySpark](https://img.shields.io/badge/Apache-Spark-E25A1C?style=flat&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta-Lake-00ADD8?style=flat&logo=deltalake&logoColor=white)](https://delta.io/)
[![SQL Server](https://img.shields.io/badge/SQL%25Server-SSMS-CC2927?style=flat&logo=microsoftsqlserver&logoColor=white)](https://www.microsoft.com/)

An end-to-end, production-grade cloud data engineering portfolio project demonstrating a **Medallion Architecture (Bronze $\rightarrow$ Silver $\rightarrow$ Gold)** implemented on Databricks. This project features automated orchestration, rigorous data quality governance, and an enterprise hybrid bridge that syncs cloud lakehouse models directly into local SQL Server Management Studio (SSMS) via native ODBC.

---

## 🏗️ Architectural Overview

```text
[ Raw Data / Faker Engine ] 
       │
       ▼
 [ Bronze Layer ] ──(Ingestion & Schema Capture)
       │
       ▼
 [ Silver Layer ] ──(Cleansing, Null Handling, 02_silver_limpieza.py)
       │
       ▼
 [ Gold Layer ]   ──(Aggregations & Monthly Rollups, 03_gold_agregaciones.py)
       │
       ├────────────────────────────────────────┐
       ▼                                        ▼
 [ Databricks SQL Warehouse ]          [ ODBC System DSN Bridge ]
 (Dashboard - E-Commerce Sales)                 │
                                                ▼
                                       [ Local SSMS Enterprise DB ]
                                       (sp_Sync_Databricks_Ventas)
