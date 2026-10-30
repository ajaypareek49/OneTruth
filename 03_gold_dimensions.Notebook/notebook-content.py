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
# META         },
# META         {
# META           "id": "fc8d20a0-fbd3-4e8b-b0a3-9e2296abe7bf"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# CELL ********************

# Fabric Notebook — Gold Dimensions
# Runtime: PySpark (Spark 3.4+)
# Purpose: Build conformed Gold-layer dimension tables with surrogate keys

from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window



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


# ─── Cell 2: DimDate ──────────────────────────────────────────────────────────
dim_date_silver = spark.table(f"{SILVER}.dbo.dim_date")
write_gold(dim_date_silver, "DimDate")


# ─── Cell 3: DimCarrier ───────────────────────────────────────────────────────
carriers_stg = spark.table(f"{SILVER}.dbo.dim_carrier_stg")

dim_carrier = (
    carriers_stg
    .withColumn("carrier_sk",
        F.row_number().over(Window.orderBy("carrier_id"))
    )
    .select(
        F.col("carrier_sk"),
        F.col("carrier_id").alias("carrier_nk"),
        F.col("carrier_name"),
        F.col("carrier_type"),
        F.col("service_level"),
        F.col("contact_email"),
        F.col("active_flag"),
        F.current_date().alias("effective_date"),
        F.lit("9999-12-31").cast(DateType()).alias("expiry_date"),
        F.lit(True).alias("is_current"),
    )
)

write_gold(dim_carrier, "DimCarrier")


# ─── Cell 4: DimRoute ─────────────────────────────────────────────────────────
routes_stg = spark.table(f"{SILVER}.dbo.dim_route_stg")

dim_route = (
    routes_stg
    .withColumn("route_sk",
        F.row_number().over(Window.orderBy("route_id"))
    )
    .select(
        F.col("route_sk"),
        F.col("route_id").alias("route_nk"),
        F.col("route_name"),
        F.col("origin_city"),
        F.col("destination_city"),
        F.col("transport_mode"),
        F.col("standard_transit_days"),
    )
)

write_gold(dim_route, "DimRoute")


# ─── Cell 5: DimWarehouse ─────────────────────────────────────────────────────
# Warehouse dim is seeded directly (small reference table)
warehouse_data = [
    ("WH001","Chicago Central DC",      "Chicago",     "Midwest",    "Distribution Centre"),
    ("WH002","Los Angeles West DC",     "Los Angeles", "West",       "Distribution Centre"),
    ("WH003","New York East DC",        "New York",    "East",       "Distribution Centre"),
    ("WH004","Houston South DC",        "Houston",     "South",      "Distribution Centre"),
    ("WH005","Atlanta Southeast DC",    "Atlanta",     "Southeast",  "Fulfillment Centre"),
    ("WH006","Dallas Central DC",       "Dallas",      "South",      "Distribution Centre"),
    ("WH007","Seattle Northwest DC",    "Seattle",     "Northwest",  "Fulfillment Centre"),
    ("WH008","Miami Hub",               "Miami",       "Southeast",  "Cross-dock"),
    ("WH009","Detroit Midwest Hub",     "Detroit",     "Midwest",    "Fulfillment Centre"),
    ("WH010","Phoenix Southwest DC",    "Phoenix",     "Southwest",  "Distribution Centre"),
]

wh_schema = StructType([
    StructField("warehouse_nk",   StringType()),
    StructField("warehouse_name", StringType()),
    StructField("city",           StringType()),
    StructField("region",         StringType()),
    StructField("facility_type",  StringType()),
])

dim_warehouse = (
    spark.createDataFrame(warehouse_data, wh_schema)
    .withColumn("warehouse_sk", F.row_number().over(Window.orderBy("warehouse_nk")))
)

write_gold(dim_warehouse, "DimWarehouse")


# ─── Cell 6: DimProduct ───────────────────────────────────────────────────────
products_stg = spark.table(f"{SILVER}.dbo.dim_product_stg")

dim_product = (
    products_stg
    .withColumn("product_sk",
        F.row_number().over(Window.orderBy("sku"))
    )
    .select(
        F.col("product_sk"),
        F.col("sku").alias("product_nk"),
        F.col("product_name"),
        F.col("category"),
        F.col("brand"),
        F.col("unit_weight_kg"),
        F.col("unit_cost_usd"),
        F.col("lead_time_days"),
    )
)

write_gold(dim_product, "DimProduct")


# ─── Cell 7: DimCustomer ──────────────────────────────────────────────────────
customers_stg = spark.table(f"{SILVER}.dbo.dim_customer_stg")

dim_customer = (
    customers_stg
    .withColumn("customer_sk",
        F.row_number().over(Window.orderBy("customer_id"))
    )
    .select(
        F.col("customer_sk"),
        F.col("customer_id").alias("customer_nk"),
        F.col("customer_name"),
        F.col("segment"),
        F.col("region"),
        F.col("country"),
        F.col("credit_limit"),
    )
)

write_gold(dim_customer, "DimCustomer")


# ─── Cell 8: DimSupplier ──────────────────────────────────────────────────────
suppliers_stg = spark.table(f"{SILVER}.dbo.dim_supplier_stg")

dim_supplier = (
    suppliers_stg
    .withColumn("supplier_sk",
        F.row_number().over(Window.orderBy("supplier_id"))
    )
    .select(
        F.col("supplier_sk"),
        F.col("supplier_id").alias("supplier_nk"),
        F.col("supplier_name"),
        F.col("supplier_type"),
        F.col("supplier_category"),
        F.col("country"),
        F.col("lead_time_days"),
    )
)

write_gold(dim_supplier, "DimSupplier")


# ─── Cell 9: DimCostType ──────────────────────────────────────────────────────
cost_data = [
    (1, "Freight",        "Line Haul",   "Transportation"),
    (2, "Fuel Surcharge", "Line Haul",   "Variable"),
    (3, "Accessorial",    "Last Mile",   "Accessorial"),
    (4, "Freight",        "Last Mile",   "Transportation"),
    (5, "Freight",        "Cross Dock",  "Transportation"),
    (6, "Accessorial",    "Cross Dock",  "Accessorial"),
]

ct_schema = StructType([
    StructField("cost_type_sk",   IntegerType()),
    StructField("cost_type",      StringType()),
    StructField("cost_category",  StringType()),
    StructField("cost_group",     StringType()),
])

dim_cost_type = spark.createDataFrame(cost_data, ct_schema)
write_gold(dim_cost_type, "DimCostType")

print("\n=== Gold dimensions complete ===")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
