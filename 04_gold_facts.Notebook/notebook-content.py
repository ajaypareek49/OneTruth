# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "fc8d20a0-fbd3-4e8b-b0a3-9e2296abe7bf",
# META       "default_lakehouse_name": "Silver_OneTruth_Logistics",
# META       "default_lakehouse_workspace_id": "fd2b1bb0-e015-4db1-b2c4-5768a0cba224",
# META       "known_lakehouses": [
# META         {
# META           "id": "fc8d20a0-fbd3-4e8b-b0a3-9e2296abe7bf"
# META         },
# META         {
# META           "id": "2504128c-4ee3-4bcf-8047-52c01edd6cf8"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# CELL ********************

# Fabric Notebook — Gold Fact Tables
# Runtime: PySpark (Spark 3.4+)
# Purpose: Build 7 conformed Gold-layer fact tables using dimension surrogate keys

from pyspark.sql import functions as F
from pyspark.sql.types import *


SILVER = "Silver_OneTruth_Logistics"
GOLD   = "Gold_OneTruth_Logistics"

def write_gold(df, table_name: str):
    (
        df.withColumn("_gold_processed_at", F.current_timestamp())
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{GOLD}.dbo.{table_name}")
    )
    print(f"✓ {GOLD}.dbo.{table_name}: {df.count():,} rows")

# Pre-load dimension lookup tables
dim_carrier   = spark.table(f"{GOLD}.dbo.DimCarrier").select("carrier_sk", "carrier_nk")
dim_route     = spark.table(f"{GOLD}.dbo.DimRoute").select("route_sk", "route_nk")
dim_warehouse = spark.table(f"{GOLD}.dbo.DimWarehouse").select("warehouse_sk", "warehouse_nk")
dim_product   = spark.table(f"{GOLD}.dbo.DimProduct").select("product_sk", "product_nk")
dim_customer  = spark.table(f"{GOLD}.dbo.DimCustomer").select("customer_sk", "customer_nk")
dim_supplier  = spark.table(f"{GOLD}.dbo.DimSupplier").select("supplier_sk", "supplier_nk")
dim_date      = spark.table(f"{GOLD}.dbo.DimDate").select("date_id", "date_key")
dim_cost_type = spark.table(f"{GOLD}.dbo.DimCostType").select("cost_type_sk", "cost_type", "cost_category")


def resolve_date_fk(df, date_col: str, sk_name: str):
    """Join a date column to DimDate and add date surrogate key."""
    return (
        df.join(
            dim_date.withColumnRenamed("date_id", sk_name).withColumnRenamed("date_key", f"_dt_{sk_name}"),
            F.col(date_col) == F.col(f"_dt_{sk_name}"),
            "left"
        )
        .drop(f"_dt_{sk_name}")
        .withColumn(sk_name, F.coalesce(F.col(sk_name), F.lit(-1)))
    )


# ─── Cell 2: FactShipment ─────────────────────────────────────────────────────
shipments_stg = spark.table(f"{SILVER}.dbo.fact_shipment_stg")

fact_shipment = (
    shipments_stg
    # Resolve carrier FK
    .join(dim_carrier.withColumnRenamed("carrier_sk","carrier_sk_"), dim_carrier.carrier_nk == shipments_stg.carrier_id, "left")
    .withColumn("carrier_sk", F.coalesce(F.col("carrier_sk_"), F.lit(-1))).drop("carrier_sk_","carrier_nk")
    # Resolve route FK
    .join(dim_route.withColumnRenamed("route_sk","route_sk_"), dim_route.route_nk == shipments_stg.route_id, "left")
    .withColumn("route_sk", F.coalesce(F.col("route_sk_"), F.lit(-1))).drop("route_sk_","route_nk")
    # Resolve origin warehouse
    .join(dim_warehouse.withColumnRenamed("warehouse_sk","origin_wh_sk").withColumnRenamed("warehouse_nk","_owh_nk"),
          F.col("origin_warehouse_id") == F.col("_owh_nk"), "left")
    .withColumn("origin_warehouse_sk", F.coalesce(F.col("origin_wh_sk"), F.lit(-1))).drop("origin_wh_sk","_owh_nk")
    # Resolve destination warehouse
    .join(dim_warehouse.withColumnRenamed("warehouse_sk","dest_wh_sk").withColumnRenamed("warehouse_nk","_dwh_nk"),
          F.col("destination_warehouse_id") == F.col("_dwh_nk"), "left")
    .withColumn("destination_warehouse_sk", F.coalesce(F.col("dest_wh_sk"), F.lit(-1))).drop("dest_wh_sk","_dwh_nk")
)

# Resolve date FKs
fact_shipment = resolve_date_fk(fact_shipment, "actual_ship_date",       "ship_date_id")
fact_shipment = resolve_date_fk(fact_shipment, "actual_delivery_date",   "delivery_date_id")
fact_shipment = resolve_date_fk(fact_shipment, "planned_delivery_date",  "planned_delivery_date_id")

fact_shipment = fact_shipment.select(
    "shipment_id",
    "order_id",
    "carrier_sk",
    "route_sk",
    "origin_warehouse_sk",
    "destination_warehouse_sk",
    "ship_date_id",
    "delivery_date_id",
    "planned_delivery_date_id",
    "planned_ship_date",
    "actual_ship_date",
    "planned_delivery_date",
    "actual_delivery_date",
    "shipment_status",
    "total_weight_kg",
    "shipment_mode",
    "delivery_variance_days",
    "is_on_time",
)

write_gold(fact_shipment, "FactShipment")


# ─── Cell 3: FactOrder ────────────────────────────────────────────────────────
orders_stg = spark.table(f"{SILVER}.dbo.fact_order_stg")

fact_order = (
    orders_stg
    .join(dim_customer.withColumnRenamed("customer_sk","customer_sk_"), dim_customer.customer_nk == orders_stg.customer_id, "left")
    .withColumn("customer_sk", F.coalesce(F.col("customer_sk_"), F.lit(-1))).drop("customer_sk_","customer_nk")
    .join(dim_warehouse.withColumnRenamed("warehouse_sk","fulfillment_wh_sk").withColumnRenamed("warehouse_nk","_fwh_nk"),
          F.col("fulfillment_warehouse_id") == F.col("_fwh_nk"), "left")
    .withColumn("fulfillment_warehouse_sk", F.coalesce(F.col("fulfillment_wh_sk"), F.lit(-1))).drop("fulfillment_wh_sk","_fwh_nk")
)

fact_order = resolve_date_fk(fact_order, "order_date", "order_date_id")

fact_order = fact_order.select(
    "order_id",
    "customer_sk",
    "fulfillment_warehouse_sk",
    "order_date_id",
    "order_date",
    "required_delivery_date",
    "order_status",
    "order_channel",
    "total_order_value_usd",
)

write_gold(fact_order, "FactOrder")


# ─── Cell 4: FactInventorySnapshot ────────────────────────────────────────────
inv_stg = spark.table(f"{SILVER}.dbo.fact_inventory_snapshot_stg")

fact_inv = (
    inv_stg
    .join(dim_warehouse.withColumnRenamed("warehouse_sk","warehouse_sk_").withColumnRenamed("warehouse_nk","_wh_nk"),
          F.col("warehouse_id") == F.col("_wh_nk"), "left")
    .withColumn("warehouse_sk", F.coalesce(F.col("warehouse_sk_"), F.lit(-1))).drop("warehouse_sk_","_wh_nk")
    .join(dim_product.withColumnRenamed("product_sk","product_sk_").withColumnRenamed("product_nk","_p_nk"),
          F.col("sku") == F.col("_p_nk"), "left")
    .withColumn("product_sk", F.coalesce(F.col("product_sk_"), F.lit(-1))).drop("product_sk_","_p_nk")
)

fact_inv = resolve_date_fk(fact_inv, "snapshot_date", "snapshot_date_id")

fact_inv = fact_inv.select(
    "snapshot_id",
    "warehouse_sk",
    "product_sk",
    "snapshot_date_id",
    "snapshot_date",
    "available_qty",
    "reserved_qty",
    "in_transit_qty",
    "reorder_point",
    "max_capacity",
    "is_available",
    "is_below_reorder",
)

write_gold(fact_inv, "FactInventorySnapshot")


# ─── Cell 5: FactWarehouseTask ────────────────────────────────────────────────
tasks_stg = spark.table(f"{SILVER}.dbo.fact_warehouse_task_stg")

fact_task = (
    tasks_stg
    .join(dim_warehouse.withColumnRenamed("warehouse_sk","warehouse_sk_").withColumnRenamed("warehouse_nk","_wh_nk"),
          F.col("warehouse_id") == F.col("_wh_nk"), "left")
    .withColumn("warehouse_sk", F.coalesce(F.col("warehouse_sk_"), F.lit(-1))).drop("warehouse_sk_","_wh_nk")
    .join(dim_product.withColumnRenamed("product_sk","product_sk_").withColumnRenamed("product_nk","_p_nk"),
          F.col("sku") == F.col("_p_nk"), "left")
    .withColumn("product_sk", F.coalesce(F.col("product_sk_"), F.lit(-1))).drop("product_sk_","_p_nk")
)

fact_task = resolve_date_fk(fact_task, "task_date", "task_date_id")

fact_task = fact_task.select(
    "task_id",
    "warehouse_sk",
    "product_sk",
    "task_date_id",
    "task_type",
    "shipment_id",
    "planned_start_time",
    "actual_start_time",
    "planned_end_time",
    "actual_end_time",
    "task_status",
    "operator_id",
    "is_late_dispatch",
)

write_gold(fact_task, "FactWarehouseTask")


# ─── Cell 6: FactInboundReceipt ───────────────────────────────────────────────
receipts_stg = spark.table(f"{SILVER}.dbo.fact_inbound_receipt_stg")

fact_receipt = (
    receipts_stg
    .join(dim_supplier.withColumnRenamed("supplier_sk","supplier_sk_").withColumnRenamed("supplier_nk","_s_nk"),
          F.col("supplier_id") == F.col("_s_nk"), "left")
    .withColumn("supplier_sk", F.coalesce(F.col("supplier_sk_"), F.lit(-1))).drop("supplier_sk_","_s_nk")
    .join(dim_product.withColumnRenamed("product_sk","product_sk_").withColumnRenamed("product_nk","_p_nk"),
          F.col("sku") == F.col("_p_nk"), "left")
    .withColumn("product_sk", F.coalesce(F.col("product_sk_"), F.lit(-1))).drop("product_sk_","_p_nk")
    .join(dim_warehouse.withColumnRenamed("warehouse_sk","warehouse_sk_").withColumnRenamed("warehouse_nk","_wh_nk"),
          F.col("warehouse_id") == F.col("_wh_nk"), "left")
    .withColumn("warehouse_sk", F.coalesce(F.col("warehouse_sk_"), F.lit(-1))).drop("warehouse_sk_","_wh_nk")
)

fact_receipt = resolve_date_fk(fact_receipt, "actual_receipt_date", "receipt_date_id")

fact_receipt = fact_receipt.select(
    "receipt_id",
    "supplier_sk",
    "product_sk",
    "warehouse_sk",
    "receipt_date_id",
    "purchase_order_id",
    "planned_receipt_date",
    "actual_receipt_date",
    "planned_qty",
    "received_qty",
    "receipt_status",
    "quality_check_passed",
    "receipt_variance_days",
    "is_on_time_inbound",
)

write_gold(fact_receipt, "FactInboundReceipt")


# ─── Cell 7: FactFreightInvoice ───────────────────────────────────────────────
invoices_stg = spark.table(f"{SILVER}.dbo.fact_freight_invoice_stg")

fact_invoice = (
    invoices_stg
    .join(dim_carrier.withColumnRenamed("carrier_sk","carrier_sk_"), dim_carrier.carrier_nk == invoices_stg.carrier_id, "left")
    .withColumn("carrier_sk", F.coalesce(F.col("carrier_sk_"), F.lit(-1))).drop("carrier_sk_","carrier_nk")
    .join(dim_cost_type.withColumnRenamed("cost_type_sk","cost_type_sk_"),
          (dim_cost_type.cost_type     == invoices_stg.cost_type) &
          (dim_cost_type.cost_category == invoices_stg.cost_category), "left")
    .withColumn("cost_type_sk", F.coalesce(F.col("cost_type_sk_"), F.lit(-1))).drop("cost_type_sk_")
)

fact_invoice = resolve_date_fk(fact_invoice, "invoice_date", "invoice_date_id")

fact_invoice = fact_invoice.select(
    "invoice_id",
    "shipment_id",
    "carrier_sk",
    "cost_type_sk",
    "invoice_date_id",
    "invoice_date",
    "invoice_amount_usd",
    "base_freight_usd",
    "fuel_surcharge_usd",
    "accessorial_usd",
    "invoice_status",
)

write_gold(fact_invoice, "FactFreightInvoice")


# ─── Cell 8: FactException ────────────────────────────────────────────────────
exc_stg = spark.table(f"{SILVER}.dbo.fact_exception_stg")

fact_exception = resolve_date_fk(exc_stg, "exception_date", "exception_date_id")

fact_exception = fact_exception.select(
    "exception_id",
    "exception_date_id",
    "exception_type",
    "related_entity_type",
    "related_entity_id",
    "exception_date",
    "severity",
    "status",
    "description",
    "assigned_to",
    "resolution_date",
)

write_gold(fact_exception, "FactException")

print("\n=== Gold facts complete ===")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
