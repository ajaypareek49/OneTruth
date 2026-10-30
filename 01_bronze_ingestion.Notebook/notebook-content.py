# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e5f8de90-fc40-42e3-bfd9-4c487dd433a1",
# META       "default_lakehouse_name": "Bronse_OneTruth_Logistics",
# META       "default_lakehouse_workspace_id": "fd2b1bb0-e015-4db1-b2c4-5768a0cba224",
# META       "known_lakehouses": [
# META         {
# META           "id": "e5f8de90-fc40-42e3-bfd9-4c487dd433a1"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# CELL ********************

# Fabric Notebook — Bronze Ingestion
# Upload this file into Microsoft Fabric as a new Notebook
# Runtime: PySpark (Spark 3.4+)
# Purpose: Ingest CSV source files from OneLake Files into Bronze Delta tables


# ─── Cell 1: Configuration ───────────────────────────────────────────────────
LAKEHOUSE_NAME  = "Bronse_OneTruth_Logistics"      # change to your Lakehouse name
SOURCE_PATH     =  f"abfss://fd2b1bb0-e015-4db1-b2c4-5768a0cba224@onelake.dfs.fabric.microsoft.com/e5f8de90-fc40-42e3-bfd9-4c487dd433a1/Files/"
BRONZE_SCHEMA   = "dbo"

SOURCE_FILES = {
    "tms_carriers":               "tms_carriers.csv",
    "tms_routes":                 "tms_routes.csv",
    "tms_shipments":              "tms_shipments.csv",
    "erp_customers":              "erp_customers.csv",
    "erp_suppliers":              "erp_suppliers.csv",
    "erp_products":               "erp_products.csv",
    "oms_orders":                 "oms_orders.csv",
    "oms_order_lines":            "oms_order_lines.csv",
    "wms_warehouse_tasks":        "wms_warehouse_tasks.csv",
    "wms_inventory_snapshot":     "wms_inventory_snapshot.csv",
    "procurement_inbound_receipts": "procurement_inbound_receipts.csv",
    "finance_freight_invoices":   "finance_freight_invoices.csv",
    "operations_exceptions":      "operations_exceptions.csv",
}

# ─── Cell 2: Ingest all files ─────────────────────────────────────────────────
from pyspark.sql import functions as F
from datetime import datetime

ingestion_log = []

for table_name, filename in SOURCE_FILES.items():
    file_path = SOURCE_PATH + filename
    print(f"Ingesting {filename} → {BRONZE_SCHEMA}.{table_name}")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "false")
        .csv(file_path)
    )

    df = (
        df
        .withColumn("_ingested_at",  F.current_timestamp())
        .withColumn("_source_file",  F.lit(filename))
        .withColumn("_source_system", F.lit(filename.split("_")[0].upper()))
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{BRONZE_SCHEMA}.{table_name}")
    )

    row_count = df.count()
    ingestion_log.append({"table": f"{BRONZE_SCHEMA}.{table_name}", "rows": row_count, "file": filename})
    print(f"  ✓ {row_count:,} rows written")

# ─── Cell 3: Ingestion summary ────────────────────────────────────────────────
import pandas as pd

log_df = pd.DataFrame(ingestion_log)
print("\n=== Bronze Ingestion Summary ===")
print(log_df.to_string(index=False))
print(f"\nTotal tables: {len(ingestion_log)}")
print(f"Total rows:   {sum(r['rows'] for r in ingestion_log):,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
