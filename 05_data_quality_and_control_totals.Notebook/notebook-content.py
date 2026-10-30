# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2504128c-4ee3-4bcf-8047-52c01edd6cf8",
# META       "default_lakehouse_name": "Gold_OneTruth_Logistics",
# META       "default_lakehouse_workspace_id": "fd2b1bb0-e015-4db1-b2c4-5768a0cba224",
# META       "known_lakehouses": [
# META         {
# META           "id": "2504128c-4ee3-4bcf-8047-52c01edd6cf8"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Fabric Notebook — Data Quality & Control Totals
# Runtime: PySpark (Spark 3.4+)
# Purpose: Run DQ checks and validate certified measures against control totals.
#          Gate the semantic model release — ALL checks must pass before promotion.

from pyspark.sql import functions as F
from pyspark.sql.types import *
import json



GOLD = "Gold_OneTruth_Logistics.dbo"
SILVER= "Silver_OneTruth_Logistics.dbo"

dq_results   = []
ct_results   = []

def check(name, passed, actual=None, expected=None, threshold_pct=0.01):
    status = "PASS" if passed else "FAIL"
    rec = {
        "check":    name,
        "status":   status,
        "actual":   actual,
        "expected": expected,
    }
    dq_results.append(rec)
    icon = "✓" if passed else "✗"
    print(f"  {icon} [{status}] {name}" + (f"  actual={actual}" if actual is not None else ""))
    return passed

def control_total(measure_name, computed, control, tolerance_pct=0.01):
    variance = abs(computed - control) / max(abs(control), 1)
    passed   = variance <= tolerance_pct
    ct_results.append({
        "measure":     measure_name,
        "computed":    round(computed, 6),
        "control":     round(control, 6),
        "variance_pct": round(variance * 100, 4),
        "status":      "PASS" if passed else "FAIL",
    })
    icon = "✓" if passed else "✗"
    print(f"  {icon} {measure_name}: computed={computed:.6f}  control={control:.6f}  variance={variance*100:.4f}%")
    return passed


# ──────────────────────────────────────────────────────────────────────────────
# Section A: Referential Integrity
# ──────────────────────────────────────────────────────────────────────────────
print("\n=== A. Referential Integrity ===")

fact_shipment = spark.table(f"{GOLD}.FactShipment")
dim_carrier   = spark.table(f"{GOLD}.DimCarrier")
dim_route     = spark.table(f"{GOLD}.DimRoute")
dim_date      = spark.table(f"{GOLD}.DimDate")

# Unresolved carrier FKs
unresolved_carriers = fact_shipment.filter(F.col("carrier_sk") == -1).count()
check("FactShipment: no unresolved carrier_sk", unresolved_carriers == 0, unresolved_carriers)

# Unresolved route FKs
unresolved_routes = fact_shipment.filter(F.col("route_sk") == -1).count()
check("FactShipment: no unresolved route_sk", unresolved_routes == 0, unresolved_routes)

# FactOrder customer FKs
fact_order = spark.table(f"{GOLD}.FactOrder")
unresolved_customers = fact_order.filter(F.col("customer_sk") == -1).count()
check("FactOrder: no unresolved customer_sk", unresolved_customers == 0, unresolved_customers)

# FactInventorySnapshot product FKs
fact_inv = spark.table(f"{GOLD}.FactInventorySnapshot")
unresolved_products = fact_inv.filter(F.col("product_sk") == -1).count()
check("FactInventorySnapshot: no unresolved product_sk", unresolved_products == 0, unresolved_products)


# ──────────────────────────────────────────────────────────────────────────────
# Section B: Null Key Checks
# ──────────────────────────────────────────────────────────────────────────────
print("\n=== B. Null Key Checks ===")

null_shipment_id = fact_shipment.filter(F.col("shipment_id").isNull()).count()
check("FactShipment: no null shipment_id", null_shipment_id == 0, null_shipment_id)

null_order_id = fact_order.filter(F.col("order_id").isNull()).count()
check("FactOrder: no null order_id", null_order_id == 0, null_order_id)

fact_receipt = spark.table(f"{GOLD}.FactInboundReceipt")
null_receipt_id = fact_receipt.filter(F.col("receipt_id").isNull()).count()
check("FactInboundReceipt: no null receipt_id", null_receipt_id == 0, null_receipt_id)

fact_invoice = spark.table(f"{GOLD}.FactFreightInvoice")
null_invoice_id = fact_invoice.filter(F.col("invoice_id").isNull()).count()
check("FactFreightInvoice: no null invoice_id", null_invoice_id == 0, null_invoice_id)


# ──────────────────────────────────────────────────────────────────────────────
# Section C: Duplicate Key Checks
# ──────────────────────────────────────────────────────────────────────────────
print("\n=== C. Duplicate Key Checks ===")

def dup_check(table_df, pk_col, label):
    dupes = table_df.groupBy(pk_col).count().filter(F.col("count") > 1).count()
    return check(f"{label}: no duplicate {pk_col}", dupes == 0, dupes)

dup_check(fact_shipment, "shipment_id", "FactShipment")
dup_check(fact_order, "order_id", "FactOrder")
dup_check(fact_receipt, "receipt_id", "FactInboundReceipt")
dup_check(fact_invoice, "invoice_id", "FactFreightInvoice")
dup_check(spark.table(f"{GOLD}.DimCarrier"), "carrier_nk", "DimCarrier")
dup_check(spark.table(f"{GOLD}.DimProduct"), "product_nk", "DimProduct")


# ──────────────────────────────────────────────────────────────────────────────
# Section D: Control Totals for Certified Measures
# ──────────────────────────────────────────────────────────────────────────────
print("\n=== D. Certified Measure Control Totals ===")
# Control totals are computed directly from Silver staging (source of truth)
# and compared against Gold. Variance must be ≤ 1%.

silver_shipment = spark.table("Silver_OneTruth_Logistics.dbo.fact_shipment_stg")

# --- Measure 1: On-Time Delivery % ---
# Eligible: Delivered shipments with non-null actual_delivery_date
silver_eligible_otd = silver_shipment.filter(F.col("shipment_status") == "Delivered").count()
silver_ontime_otd   = silver_shipment.filter(
    (F.col("shipment_status") == "Delivered") & (F.col("is_on_time") == True)
).count()
silver_otd_pct = silver_ontime_otd / silver_eligible_otd if silver_eligible_otd else 0

gold_eligible_otd = fact_shipment.filter(F.col("shipment_status") == "Delivered").count()
gold_ontime_otd   = fact_shipment.filter(
    (F.col("shipment_status") == "Delivered") & (F.col("is_on_time") == True)
).count()
gold_otd_pct = gold_ontime_otd / gold_eligible_otd if gold_eligible_otd else 0

control_total("On-Time Delivery %", gold_otd_pct, silver_otd_pct)

# --- Measure 2: Total Freight Cost ---
silver_inv = spark.table("{SILVER}.fact_freight_invoice_stg")
silver_total_cost = silver_inv.agg(F.sum("invoice_amount_usd")).collect()[0][0] or 0
gold_total_cost   = fact_invoice.agg(F.sum("invoice_amount_usd")).collect()[0][0] or 0
control_total("Total Freight Cost (USD)", gold_total_cost, silver_total_cost)

# --- Measure 3: Order Fulfilment Rate ---
silver_orders = spark.table("{SILVER}.fact_order_stg")
silver_total_orders     = silver_orders.count()
silver_fulfilled_orders = silver_orders.filter(F.col("order_status") == "Delivered").count()
silver_ofr = silver_fulfilled_orders / silver_total_orders if silver_total_orders else 0

gold_total_orders     = fact_order.count()
gold_fulfilled_orders = fact_order.filter(F.col("order_status") == "Delivered").count()
gold_ofr = gold_fulfilled_orders / gold_total_orders if gold_total_orders else 0

control_total("Order Fulfilment Rate", gold_ofr, silver_ofr)

# --- Measure 4: Supplier On-Time Inbound % ---
silver_receipts = spark.table("{SILVER}.fact_inbound_receipt_stg")
silver_receipt_total  = silver_receipts.filter(F.col("receipt_status") == "Received").count()
silver_ontime_receipt = silver_receipts.filter(
    (F.col("receipt_status") == "Received") & (F.col("is_on_time_inbound") == True)
).count()
silver_soti = silver_ontime_receipt / silver_receipt_total if silver_receipt_total else 0

gold_receipt_total  = fact_receipt.filter(F.col("receipt_status") == "Received").count()
gold_ontime_receipt = fact_receipt.filter(
    (F.col("receipt_status") == "Received") & (F.col("is_on_time_inbound") == True)
).count()
gold_soti = gold_ontime_receipt / gold_receipt_total if gold_receipt_total else 0

control_total("Supplier On-Time Inbound %", gold_soti, silver_soti)

# --- Measure 5: Exception Count ---
silver_exc = spark.table("{SILVER}.fact_exception_stg")
silver_exc_count = silver_exc.count()
gold_exc_count   = spark.table(f"{GOLD}.FactException").count()
control_total("Exception Count (row count)", float(gold_exc_count), float(silver_exc_count))


# ──────────────────────────────────────────────────────────────────────────────
# Section E: Summary & Gate
# ──────────────────────────────────────────────────────────────────────────────
print("\n=== E. Summary ===")

all_dq_pass = all(r["status"] == "PASS" for r in dq_results)
all_ct_pass = all(r["status"] == "PASS" for r in ct_results)

print(f"\nDQ checks:  {sum(1 for r in dq_results if r['status']=='PASS')}/{len(dq_results)} passed")
print(f"CT checks:  {sum(1 for r in ct_results if r['status']=='PASS')}/{len(ct_results)} passed")

if all_dq_pass and all_ct_pass:
    print("\n✓ ALL CHECKS PASSED — Gold layer approved for semantic model build")
    dbutils.notebook.exit("PASS")
else:
    failed_dq = [r for r in dq_results if r["status"] == "FAIL"]
    failed_ct = [r for r in ct_results if r["status"] == "FAIL"]
    print("\n✗ FAILURES — resolve before promoting to semantic model:")
    for r in failed_dq + failed_ct:
        print(f"  - {r}")
    dbutils.notebook.exit("FAIL")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
