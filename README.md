# 🚀 Olist E-Commerce Medallion Lakehouse & Hybrid Enterprise Bridge

[![Databricks](https://img.shields.io/badge/Databricks-Serverless-FF3621?style=flat&logo=databricks&logoColor=white)](https://databricks.com/)
[![PySpark](https://img.shields.io/badge/Apache-Spark-E25A1C?style=flat&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta-Lake-00ADD8?style=flat&logo=deltalake&logoColor=white)](https://delta.io/)
[![SQL Server](https://img.shields.io/badge/SQL%25Server-SSMS-CC2927?style=flat&logo=microsoftsqlserver&logoColor=white)](https://www.microsoft.com/)

An end-to-end, production-grade cloud data engineering portfolio project demonstrating a **Medallion Architecture (Bronze $\rightarrow$ Silver $\rightarrow$ Gold)** implemented on Databricks. This project features automated orchestration, rigorous data quality governance, and an enterprise hybrid bridge that syncs cloud lakehouse models directly into local SQL Server Management Studio (SSMS) via native ODBC.

---
📂 Repository Structure
```text
ecommerce-databricks-lakehouse/
├── notebooks/
│   ├── Ecommerce_ETL_Pipeline.py         # Main orchestration pipeline
│   ├── 02_silver_limpieza.py             # Silver-tier cleansing & wrangling
│   ├── 03_gold_agregaciones.py           # Gold-tier business metric rollups
│   ├── 04_data_quality_gates.py          # Automated data quality assertions
│   ├── 05_agentic_bi_validator.py        # Automated analytical readiness checks
│   └── 06_streaming_iot_telemetry.py     # Real-time telemetry simulation
├── sql/
│   └── sp_Sync_Databricks_Ventas.sql     # Local SSMS staging & sync procedure
└── README.md
```

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
```
⚡ Databricks Features & Technical Implementation

# Serverless Compute & Databricks Jobs Orchestration:
* Utilizes Databricks Serverless compute infrastructure for rapid, zero-config spark execution.
* Multi-stage pipeline orchestrated via automated workflow jobs executing raw ingestion, cleaning, and aggregation layers in under 90 seconds.

# Medallion Lakehouse Paradigm (Delta Lake):
* Bronze Layer: Immutable capture of raw e-commerce transaction logs and synthetic telemetry generated via Python Faker routines.
* Silver Layer (02_silver_limpieza.py): Automated schema enforcement, data type casting, and null-value sanitization.
* Gold Layer (03_gold_agregaciones.py): High-performance business metric rollups generating gold_ventas_por_mes (tracking monthly total sales, order counts, average ticket size, and active selling days).

# Data Quality & Governance (`04_data_quality_gates.py` & `05_agentic_bi_validator.py`):
* Programmatic assertion checks and threshold guardrails blocking corrupt or malformed records from hitting downstream consumption layers.

# Hybrid Enterprise Bridge (Databricks $\rightarrow$ Local SSMS):
* Solved cloud-to-on-premise data friction by configuring the native Databricks ODBC driver via a Windows System DSN with Token authentication (`UID=token`).
* Engineered a robust T-SQL staging procedure utilizing temporary table interception to safely pull cloud Gold tables directly into local SSMS databases for localized enterprise reporting.

📊 # Sample Gold Dataset Schema (gold_ventas_por_mes)

| Column Name | Data Type | Description |
| --- | --- | --- |
| anio |INT | Calendar Year |
| mes | INT | Calendar Month | 
| ventas_totales | DECIMAL(18,2) | Total gross revenue aggregated per month
| numero_pedidos | INT | Total order volume | 
| ticket_promedio | DECIMAL(18,2) | Average order value (AOV) | 
| dias_activos | INT | Total active trading days in the month | 
| synced_at | DATETIME | Timestamp of hybrid cloud-to-local synchronization |

## 🌐 Hybrid Cloud-to-Local Integration Architecture

To bridge cloud lakehouse storage with on-premise enterprise reporting, this project implements a secure hybrid pipeline between Databricks and SQL Server Management Studio (SSMS):

* **Linked Server Configuration:** Configured a dedicated Linked Server (`DATABRICKSLAKEHOUSE2`) using the native ODBC driver.
* **Optimized Pass-Through Queries:** Utilized `OPENROWSET` with `PrepareMethod=0` to bypass OLE DB provider schema preparation limits, enabling direct remote table inspection and high-speed row-count audits.
* **Synergy Impact:** Combines Databricks serverless compute (for heavy Spark ETL and Medallion transformations) with local SSMS reporting capability—allowing a seamless back-and-forth workflow across cloud and on-premise data stacks.

<img width="1324" height="570" alt="Screenshot 2026-10-15 185859" src="https://github.com/user-attachments/assets/d29fb7b7-3c28-4c6a-8111-ba694588e97e" />

🛠️ #Tech Stack
* Cloud & Lakehouse: Databricks, Delta Lake, Apache Spark, Serverless SQL Warehouses
* Languages & Libraries: Python, PySpark, Pandas, SQL
* Data Integration: ODBC Driver, Windows System DSN, T-SQL, Stored Procedures
* Version Control: Git, GitHub
