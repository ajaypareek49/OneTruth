# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": "",
# META       "known_lakehouses": []
# META     },
# META     "warehouse": {
# META       "default_warehouse": "7c896135-7e0c-43f6-8cfa-abb1104879fb",
# META       "known_warehouses": [
# META         {
# META           "id": "7c896135-7e0c-43f6-8cfa-abb1104879fb",
# META           "type": "Lakewarehouse"
# META         },
# META         {
# META           "id": "519a8a4c-3402-473c-b668-486d21565b5a",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Fabric Notebook — Silver Transformation
# Runtime: PySpark (Spark 3.4+)
# Purpose: Cleanse, deduplicate, standardise types and apply conformed keys
#          for all 6 subject areas.

# ─── Cell 1: Shared helpers ───────────────────────────────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.types import *

BRONZE = "Bronse_OneTruth_Logistics"
SILVER = "Silver_OneTruth_Logistics"


def write_silver(df, table_name: str):
    (
        df.withColumn("_silver_processed_at", F.current_timestamp())
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{SILVER}.dbo.{table_name}")
    )
    print(f"✓ {SILVER}.dbo.{table_name}: {df.count():,} rows")


# ─── Cell 2: DimDate helper ────────────────────────────────────────────────────
# Generate a full date dimension 2023-01-01 → 2025-12-31
from pyspark.sql.functions import explode, sequence, to_date

date_range = spark.sql("""
    SELECT explode(sequence(
        to_date('2023-01-01'), to_date('2025-12-31'), interval 1 day
    )) AS date_key
""")

dim_date = (
    date_range
    .withColumn("date_id",          F.date_format("date_key", "yyyyMMdd").cast(IntegerType()))
    .withColumn("year",             F.year("date_key"))
    .withColumn("quarter",          F.quarter("date_key"))
    .withColumn("month_num",        F.month("date_key"))
    .withColumn("month_name",       F.date_format("date_key", "MMMM"))
    .withColumn("week_of_year",     F.weekofyear("date_key"))
    .withColumn("day_of_week",      F.dayofweek("date_key"))
    .withColumn("day_name",         F.date_format("date_key", "EEEE"))
    .withColumn("is_weekday",       (F.dayofweek("date_key").isin([1,7]) == False).cast(BooleanType()))
    .withColumn("fiscal_year",      F.when(F.month("date_key") >= 7, F.year("date_key") + 1).otherwise(F.year("date_key")))
    .withColumn("fiscal_quarter",   F.when(F.month("date_key").between(7, 9), 1)
                                     .when(F.month("date_key").between(10, 12), 2)
                                     .when(F.month("date_key").between(1, 3), 3)
                                     .otherwise(4))
    .withColumn("year_month",       F.date_format("date_key", "yyyy-MM"))
    .withColumn("_silver_processed_at", F.current_timestamp())
)

write_silver(dim_date, "dim_date")


# ─── Cell 3: Carriers (silver) ────────────────────────────────────────────────
carriers_bronze = spark.table(f"{BRONZE}.dbo.tms_carriers")

carriers_silver = (
    carriers_bronze
    .filter(F.col("active_flag") == 1)
    .withColumn("carrier_id",    F.trim(F.upper(F.col("carrier_id"))))
    .withColumn("carrier_name",  F.trim(F.col("carrier_name")))
    .withColumn("carrier_type",  F.trim(F.col("carrier_type")))
    .withColumn("service_level", F.trim(F.col("service_level")))
    .dropDuplicates(["carrier_id"])
)

write_silver(carriers_silver, "dim_carrier_stg")


# ─── Cell 4: Routes (silver) ──────────────────────────────────────────────────
routes_bronze = spark.table(f"{BRONZE}.dbo.tms_routes")

routes_silver = (
    routes_bronze
    .withColumn("route_id",              F.trim(F.upper(F.col("route_id"))))
    .withColumn("origin_city",           F.trim(F.col("origin_city")))
    .withColumn("destination_city",      F.trim(F.col("destination_city")))
    .withColumn("transport_mode",        F.trim(F.col("transport_mode")))
    .withColumn("standard_transit_days", F.col("standard_transit_days").cast(IntegerType()))
    .dropDuplicates(["route_id"])
)

write_silver(routes_silver, "dim_route_stg")


# ─── Cell 5: Products (silver) ────────────────────────────────────────────────
products_bronze = spark.table(f"{BRONZE}.dbo.erp_products")

products_silver = (
    products_bronze
    .withColumn("sku",              F.trim(F.upper(F.col("sku"))))
    .withColumn("product_name",     F.trim(F.col("product_name")))
    .withColumn("category",         F.trim(F.col("category")))
    .withColumn("brand",            F.trim(F.col("brand")))
    .withColumn("unit_weight_kg",   F.col("unit_weight_kg").cast(DoubleType()))
    .withColumn("unit_cost_usd",    F.col("unit_cost_usd").cast(DoubleType()))
    .withColumn("lead_time_days",   F.col("lead_time_days").cast(IntegerType()))
    .dropDuplicates(["sku"])
)

write_silver(products_silver, "dim_product_stg")


# ─── Cell 6: Customers (silver) ───────────────────────────────────────────────
customers_bronze = spark.table(f"{BRONZE}.dbo.erp_customers")

customers_silver = (
    customers_bronze
    .withColumn("customer_id",   F.trim(F.upper(F.col("customer_id"))))
    .withColumn("customer_name", F.trim(F.col("customer_name")))
    .withColumn("segment",       F.trim(F.col("segment")))
    .withColumn("region",        F.trim(F.col("region")))
    .withColumn("country",       F.trim(F.col("country")))
    .withColumn("credit_limit",  F.col("credit_limit").cast(DoubleType()))
    .dropDuplicates(["customer_id"])
)

write_silver(customers_silver, "dim_customer_stg")


# ─── Cell 7: Suppliers (silver) ───────────────────────────────────────────────
suppliers_bronze = spark.table(f"{BRONZE}.dbo.erp_suppliers")

suppliers_silver = (
    suppliers_bronze
    .withColumn("supplier_id",       F.trim(F.upper(F.col("supplier_id"))))
    .withColumn("supplier_name",     F.trim(F.col("supplier_name")))
    .withColumn("supplier_type",     F.trim(F.col("supplier_type")))
    .withColumn("supplier_category", F.trim(F.col("supplier_category")))
    .withColumn("country",           F.trim(F.col("country")))
    .withColumn("lead_time_days",    F.col("lead_time_days").cast(IntegerType()))
    .dropDuplicates(["supplier_id"])
)

write_silver(suppliers_silver, "dim_supplier_stg")


# ─── Cell 8: Shipments (silver) ───────────────────────────────────────────────
shipments_bronze = spark.table(f"{BRONZE}.dbo.tms_shipments")

shipments_silver = (
    shipments_bronze
    .withColumn("shipment_id",              F.trim(F.upper(F.col("shipment_id"))))
    .withColumn("order_id",                 F.trim(F.upper(F.col("order_id"))))
    .withColumn("carrier_id",              F.trim(F.upper(F.col("carrier_id"))))
    .withColumn("route_id",                F.trim(F.upper(F.col("route_id"))))
    .withColumn("planned_ship_date",        F.to_date("planned_ship_date"))
    .withColumn("actual_ship_date",         F.to_date("actual_ship_date"))
    .withColumn("planned_delivery_date",    F.to_date("planned_delivery_date"))
    .withColumn("actual_delivery_date",     F.to_date("actual_delivery_date"))
    .withColumn("total_weight_kg",          F.col("total_weight_kg").cast(DoubleType()))
    .withColumn("shipment_status",          F.trim(F.col("shipment_status")))
    # Derived: delivery variance in days (positive = late, negative = early)
    .withColumn("delivery_variance_days",
        F.when(
            F.col("actual_delivery_date").isNotNull() & F.col("planned_delivery_date").isNotNull(),
            F.datediff("actual_delivery_date", "planned_delivery_date")
        ).otherwise(F.lit(None).cast(IntegerType()))
    )
    # Derived: on_time flag (delivered and variance <= 0)
    .withColumn("is_on_time",
        F.when(
            (F.col("shipment_status") == "Delivered") & (F.col("delivery_variance_days") <= 0),
            F.lit(True)
        ).when(
            F.col("shipment_status") == "Delivered",
            F.lit(False)
        ).otherwise(F.lit(None).cast(BooleanType()))
    )
    .filter(F.col("shipment_id").isNotNull())
    .dropDuplicates(["shipment_id"])
)

write_silver(shipments_silver, "fact_shipment_stg")


# ─── Cell 9: Orders (silver) ──────────────────────────────────────────────────
orders_bronze  = spark.table(f"{BRONZE}.dbo.oms_orders")
lines_bronze   = spark.table(f"{BRONZE}.dbo.oms_order_lines")

orders_silver = (
    orders_bronze
    .withColumn("order_id",               F.trim(F.upper(F.col("order_id"))))
    .withColumn("customer_id",            F.trim(F.upper(F.col("customer_id"))))
    .withColumn("order_date",             F.to_date("order_date"))
    .withColumn("required_delivery_date", F.to_date("required_delivery_date"))
    .withColumn("total_order_value_usd",  F.col("total_order_value_usd").cast(DoubleType()))
    .withColumn("order_status",           F.trim(F.col("order_status")))
    .withColumn("order_channel",          F.trim(F.col("order_channel")))
    .dropDuplicates(["order_id"])
)

lines_silver = (
    lines_bronze
    .withColumn("order_line_id",   F.trim(F.upper(F.col("order_line_id"))))
    .withColumn("order_id",        F.trim(F.upper(F.col("order_id"))))
    .withColumn("sku",             F.trim(F.upper(F.col("sku"))))
    .withColumn("quantity",        F.col("quantity").cast(IntegerType()))
    .withColumn("unit_price_usd",  F.col("unit_price_usd").cast(DoubleType()))
    .withColumn("fulfillment_date",F.to_date("fulfillment_date"))
    .withColumn("line_status",     F.trim(F.col("line_status")))
    # is_fulfilled: line has fulfillment date and status is Delivered
    .withColumn("is_fulfilled",
        F.when(
            F.col("line_status").isin(["Delivered","Fulfilled"]) & F.col("fulfillment_date").isNotNull(),
            F.lit(True)
        ).otherwise(F.lit(False))
    )
    .dropDuplicates(["order_line_id"])
)

write_silver(orders_silver,  "fact_order_stg")
write_silver(lines_silver,   "fact_order_line_stg")


# ─── Cell 10: Warehouse tasks (silver) ────────────────────────────────────────
tasks_bronze = spark.table(f"{BRONZE}.dbo.wms_warehouse_tasks")

tasks_silver = (
    tasks_bronze
    .withColumn("task_id",            F.trim(F.upper(F.col("task_id"))))
    .withColumn("warehouse_id",       F.trim(F.upper(F.col("warehouse_id"))))
    .withColumn("task_type",          F.trim(F.col("task_type")))
    .withColumn("sku",                F.trim(F.upper(F.col("sku"))))
    .withColumn("shipment_id",        F.trim(F.upper(F.col("shipment_id"))))
    .withColumn("planned_start_time", F.to_timestamp("planned_start_time"))
    .withColumn("actual_start_time",  F.to_timestamp("actual_start_time"))
    .withColumn("planned_end_time",   F.to_timestamp("planned_end_time"))
    .withColumn("actual_end_time",    F.to_timestamp("actual_end_time"))
    .withColumn("task_status",        F.trim(F.col("task_status")))
    # task_date for joining to DimDate
    .withColumn("task_date",          F.to_date("planned_start_time"))
    # dispatch late flag: Dispatch tasks where actual > planned end
    .withColumn("is_late_dispatch",
        F.when(
            (F.col("task_type") == "Dispatch") &
            F.col("actual_end_time").isNotNull() &
            (F.col("actual_end_time") > F.col("planned_end_time")),
            F.lit(True)
        ).otherwise(F.lit(False))
    )
    .dropDuplicates(["task_id"])
)

write_silver(tasks_silver, "fact_warehouse_task_stg")


# ─── Cell 11: Inventory snapshots (silver) ────────────────────────────────────
inv_bronze = spark.table(f"{BRONZE}.dbo.wms_inventory_snapshot")

inv_silver = (
    inv_bronze
    .withColumn("snapshot_id",    F.trim(F.upper(F.col("snapshot_id"))))
    .withColumn("snapshot_date",  F.to_date("snapshot_date"))
    .withColumn("warehouse_id",   F.trim(F.upper(F.col("warehouse_id"))))
    .withColumn("sku",            F.trim(F.upper(F.col("sku"))))
    .withColumn("available_qty",  F.col("available_qty").cast(IntegerType()))
    .withColumn("reserved_qty",   F.col("reserved_qty").cast(IntegerType()))
    .withColumn("in_transit_qty", F.col("in_transit_qty").cast(IntegerType()))
    .withColumn("reorder_point",  F.col("reorder_point").cast(IntegerType()))
    .withColumn("max_capacity",   F.col("max_capacity").cast(IntegerType()))
    # is_available: available qty > 0
    .withColumn("is_available",   F.col("available_qty") > 0)
    # is_below_reorder: at or below reorder point
    .withColumn("is_below_reorder", F.col("available_qty") <= F.col("reorder_point"))
    .dropDuplicates(["snapshot_id"])
)

write_silver(inv_silver, "fact_inventory_snapshot_stg")


# ─── Cell 12: Inbound receipts (silver) ───────────────────────────────────────
receipts_bronze = spark.table(f"{BRONZE}.dbo.procurement_inbound_receipts")

receipts_silver = (
    receipts_bronze
    .withColumn("receipt_id",           F.trim(F.upper(F.col("receipt_id"))))
    .withColumn("supplier_id",          F.trim(F.upper(F.col("supplier_id"))))
    .withColumn("sku",                  F.trim(F.upper(F.col("sku"))))
    .withColumn("warehouse_id",         F.trim(F.upper(F.col("warehouse_id"))))
    .withColumn("purchase_order_id",    F.trim(F.upper(F.col("purchase_order_id"))))
    .withColumn("planned_receipt_date", F.to_date("planned_receipt_date"))
    .withColumn("actual_receipt_date",  F.to_date("actual_receipt_date"))
    .withColumn("planned_qty",          F.col("planned_qty").cast(IntegerType()))
    .withColumn("received_qty",         F.col("received_qty").cast(IntegerType()))
    .withColumn("receipt_status",       F.trim(F.col("receipt_status")))
    .withColumn("quality_check_passed", F.col("quality_check_passed").cast(BooleanType()))
    # receipt variance: actual vs planned date
    .withColumn("receipt_variance_days",
        F.when(
            F.col("actual_receipt_date").isNotNull(),
            F.datediff("actual_receipt_date", "planned_receipt_date")
        ).otherwise(F.lit(None).cast(IntegerType()))
    )
    .withColumn("is_on_time_inbound",
        F.when(
            (F.col("receipt_status") == "Received") & (F.col("receipt_variance_days") <= 0),
            F.lit(True)
        ).when(
            F.col("receipt_status") == "Received",
            F.lit(False)
        ).otherwise(F.lit(None).cast(BooleanType()))
    )
    .dropDuplicates(["receipt_id"])
)

write_silver(receipts_silver, "fact_inbound_receipt_stg")


# ─── Cell 13: Freight invoices (silver) ───────────────────────────────────────
invoices_bronze = spark.table(f"{BRONZE}.dbo.finance_freight_invoices")

invoices_silver = (
    invoices_bronze
    .withColumn("invoice_id",          F.trim(F.upper(F.col("invoice_id"))))
    .withColumn("shipment_id",         F.trim(F.upper(F.col("shipment_id"))))
    .withColumn("carrier_id",         F.trim(F.upper(F.col("carrier_id"))))
    .withColumn("invoice_date",        F.to_date("invoice_date"))
    .withColumn("invoice_amount_usd",  F.col("invoice_amount_usd").cast(DoubleType()))
    .withColumn("base_freight_usd",    F.col("base_freight_usd").cast(DoubleType()))
    .withColumn("fuel_surcharge_usd",  F.col("fuel_surcharge_usd").cast(DoubleType()))
    .withColumn("accessorial_usd",     F.col("accessorial_usd").cast(DoubleType()))
    .withColumn("cost_type",           F.trim(F.col("cost_type")))
    .withColumn("cost_category",       F.trim(F.col("cost_category")))
    .withColumn("invoice_status",      F.trim(F.col("invoice_status")))
    .dropDuplicates(["invoice_id"])
)

write_silver(invoices_silver, "fact_freight_invoice_stg")


# ─── Cell 14: Exceptions (silver) ─────────────────────────────────────────────
exc_bronze = spark.table(f"{BRONZE}.dbo.operations_exceptions")

exc_silver = (
    exc_bronze
    .withColumn("exception_id",         F.trim(F.upper(F.col("exception_id"))))
    .withColumn("exception_type",       F.trim(F.col("exception_type")))
    .withColumn("related_entity_type",  F.trim(F.col("related_entity_type")))
    .withColumn("related_entity_id",    F.trim(F.upper(F.col("related_entity_id"))))
    .withColumn("exception_date",       F.to_date("exception_date"))
    .withColumn("severity",             F.trim(F.col("severity")))
    .withColumn("status",               F.trim(F.col("status")))
    .withColumn("resolution_date",      F.to_date("resolution_date"))
    .dropDuplicates(["exception_id"])
)

write_silver(exc_silver, "fact_exception_stg")

print("\n=== Silver transformation complete ===")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
