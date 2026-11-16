# OneTruth Logistics — Microsoft Fabric Hackathon Submission

> **One unified, trusted version of logistics truth — powered by Microsoft Fabric, Delta Lake, and Azure AI Foundry.**

---

## Overview

Supply-chain organisations run separate dashboards for transportation, warehousing, orders and procurement — executives get **conflicting answers to the same question**. OneTruth Logistics fixes this by building a single governed semantic layer over a Microsoft Fabric Medallion lakehouse.

Every KPI is computed once, certified against Silver control totals (≤1% tolerance), and surfaced through **Power BI Direct Lake** reports *and* a **Fabric Data Agent** backed by the Fabric IQ ontology. A separate **Azure AI Foundry notebook** demonstrates Foundry-SDK querying of the same layer.

| Item | Detail |
|------|--------|
| Hackathon | Microsoft Fabric Hackathon |
| Platform | Microsoft Fabric (Lakehouse · PySpark · Direct Lake · Data Agent · AI Foundry) |
| Workspace | Three Pilots Hackthon Workspace |
| Data scope | Jan – Jun 2024 · 14 source tables · 6 simulated source systems · ~3,900 rows |
| Star schema | **8 dimensions · 7 fact tables** |
| Certified measures | **8** |
| Validation result | **8/8 Q&A match** · 0 discrepancies · 2/2 RLS tests pass |

---

## Architecture

```
Source Systems (TMS · ERP · OMS · WMS · Procurement · Finance · Operations)
        │  14 CSV files  ←  generate_source_data.py
        ▼
┌───────────────────────────────────────────────┐
│  Bronse_OneTruth_Logistics  (Bronze Lakehouse) │  ← 01_bronze_ingestion
│  14 raw Delta tables                          │
│  Audit: _ingested_at · _source_file           │
└──────────────────────┬────────────────────────┘
                       │
                       ▼
┌───────────────────────────────────────────────┐
│  Silver_OneTruth_Logistics  (Silver Lakehouse) │  ← 02_silver_transformation
│  13 cleansed staging tables + dim_date        │
│  Dedup · Type-cast · Derived boolean flags    │
└──────────────────────┬────────────────────────┘
                       │
                ┌──────┴──────┐
                ▼             ▼
        03_gold_dims    04_gold_facts
                │             │
                └──────┬──────┘
                       ▼
┌───────────────────────────────────────────────┐
│  Gold_OneTruth_Logistics  (Gold Lakehouse)    │
│  8 Dimension tables · 7 Fact tables           │
│  Surrogate keys · Unknown member (-1)         │
└──────────────────────┬────────────────────────┘
                       │
          ┌────────────┼────────────────────────┐
          ▼            ▼                        ▼
   DQ Gate         Direct Lake SM           Fabric Data Agent
   (notebook 05)   8 Certified Measures     + Ontology (16 entities)
                       │                        │
                       ▼                        ▼
              Power BI Reports           Natural Language Q&A
              (8/8 Q&A validated)        (8/8 answers match PBI)
```

---

## Repository Structure

```
OneTruth-main/
├── README.md
├── 01_bronze_ingestion.Notebook/                  # Ingest 14 CSVs → Bronze Delta
│   └── notebook-content.py
├── 02_silver_transformation.Notebook/             # Cleanse, type-cast, derive flags
│   └── notebook-content.py
├── 03_gold_dimensions.Notebook/                   # 8 conformed dimension tables
│   └── notebook-content.py
├── 04_gold_facts.Notebook/                        # 7 fact tables with surrogate keys
│   └── notebook-content.py
├── 05_data_quality_and_control_totals.Notebook/   # DQ gate before SM promotion
│   └── notebook-content.py
├── Foundry_AI_Agent.Notebook/                     # Azure AI Foundry SDK demo notebook
│   └── notebook-content.py
├── Bronse_OneTruth_Logistics.Lakehouse/
├── Silver_OneTruth_Logistics.Lakehouse/
├── Gold_OneTruth_Logistics.Lakehouse/
├── OneTruth_Logistics_SM.SemanticModel/           # Power BI Direct Lake model
├── OneTruth_Logistics_Ontology.Ontology/          # Fabric IQ ontology (16 entities)
├── OneTruth_Logistics_Data_Agent.DataAgent/       # Fabric-native Data Agent (validated)
├── Pipeline_OneTruth_Logistics.DataPipeline/      # Orchestration pipeline
└── Business Questions list verification.Report/   # Power BI validation report (8/8 match)
```

---

## Notebooks

### `01_bronze_ingestion`
**Lakehouse:** `Bronse_OneTruth_Logistics` | **Runtime:** PySpark (Spark 3.4+)

Reads 14 source CSV files from OneLake Files (`abfss://…/Files/`) and writes each as a raw Delta table. Appends three audit columns: `_ingested_at`, `_source_file`, `_source_system`.

| Bronze Table | Source System | Rows |
|---|---|---|
| `tms_carriers` | TMS | 12 |
| `tms_routes` | TMS | 15 |
| `tms_shipments` | TMS | 600 |
| `erp_customers` | ERP | 60 |
| `erp_suppliers` | ERP | 30 |
| `erp_products` | ERP | 100 |
| `oms_orders` | OMS | 400 |
| `oms_order_lines` | OMS | ~800 |
| `wms_warehouse_tasks` | WMS | 500 |
| `wms_inventory_snapshot` | WMS | ~800 |
| `procurement_inbound_receipts` | Procurement | 300 |
| `finance_freight_invoices` | Finance | 400 |
| `operations_exceptions` | Operations | 100 |
| `accounts` | Reference | varies |

---

### `02_silver_transformation`
**Reads:** `Bronse_OneTruth_Logistics` | **Writes:** `Silver_OneTruth_Logistics` | **Runtime:** PySpark (Spark 3.4+)

Cleanses, deduplicates, and strongly-types all Bronze tables. All ID/key columns are trimmed and upper-cased for consistent join conformance. Generates `dim_date` spanning 2023-01-01 → 2025-12-31 with full calendar and fiscal year/quarter attributes (fiscal year starts July).

**Key derived boolean flags:**

| Staging Table | Derived Column | Logic |
|---|---|---|
| `fact_shipment_stg` | `delivery_variance_days` | `datediff(actual_delivery_date, planned_delivery_date)` |
| `fact_shipment_stg` | `is_on_time` | status = `Delivered` AND variance ≤ 0 |
| `fact_order_line_stg` | `is_fulfilled` | status IN (`Delivered`,`Fulfilled`) AND fulfillment_date NOT NULL |
| `fact_warehouse_task_stg` | `is_late_dispatch` | task_type = `Dispatch` AND actual_end > planned_end |
| `fact_inventory_snapshot_stg` | `is_below_reorder` | available_qty ≤ reorder_point |
| `fact_inventory_snapshot_stg` | `is_available` | available_qty > 0 |
| `fact_inbound_receipt_stg` | `receipt_variance_days` | `datediff(actual_receipt_date, planned_receipt_date)` |
| `fact_inbound_receipt_stg` | `is_on_time_inbound` | status = `Received` AND variance ≤ 0 |

---

### `03_gold_dimensions`
**Reads:** `Silver_OneTruth_Logistics` | **Writes:** `Gold_OneTruth_Logistics` | **Runtime:** PySpark (Spark 3.4+)

Builds **8 conformed analytical dimension tables** with integer surrogate keys via `row_number()` ordered on the natural key. `DimWarehouse` (10 US DCs) and `DimCostType` (6 cost categories) are seeded inline.

> **Note:** `DimAccounts` is an RLS filtering table (not a conformed analytical dimension) — it maps user email to warehouse access and is used by `Regional_Warehouse_Manager` role only.

| Dimension | Natural Key | SK Column | Notes |
|---|---|---|---|
| `DimDate` | `date_key` | `date_id` | 2023–2025 · fiscal year starts July |
| `DimCarrier` | `carrier_nk` | `carrier_sk` | Active-only · SCD-ready (effective/expiry dates) |
| `DimRoute` | `route_nk` | `route_sk` | 15 US corridors · origin/destination/mode |
| `DimWarehouse` | `warehouse_nk` | `warehouse_sk` | 10 US DCs seeded inline |
| `DimProduct` | `product_nk` (SKU) | `product_sk` | 7 categories · unit cost/weight/lead time |
| `DimCustomer` | `customer_nk` | `customer_sk` | 3 segments (Enterprise/Mid-Market/SMB) · 7 regions |
| `DimSupplier` | `supplier_nk` | `supplier_sk` | 5 countries · lead time |
| `DimCostType` | composite | `cost_type_sk` | 6 cost type/category combos seeded inline |

RLS table (not counted as analytical dimension):

| Table | Purpose |
|---|---|
| `DimAccounts` | `dimaccounts[email] = USERPRINCIPALNAME()` for `Regional_Warehouse_Manager` role |

---

### `04_gold_facts`
**Reads:** `Silver_OneTruth_Logistics` + `Gold_OneTruth_Logistics` | **Writes:** `Gold_OneTruth_Logistics` | **Runtime:** PySpark (Spark 3.4+)

Resolves all natural keys to dimension surrogate keys via left joins. The `resolve_date_fk()` helper maps any date column to `DimDate.date_id`. Unresolved FKs are set to `-1` (unknown member pattern).

| Fact Table | Grain | Key Measures / Flags |
|---|---|---|
| `FactShipment` | 1 row per shipment | `delivery_variance_days`, `is_on_time`, `total_weight_kg` |
| `FactOrder` | 1 row per customer order | `total_order_value_usd` |
| `FactInventorySnapshot` | SKU × warehouse × date | `available_qty`, `reserved_qty`, `in_transit_qty`, `is_available`, `is_below_reorder` |
| `FactWarehouseTask` | 1 row per warehouse task | `is_late_dispatch` |
| `FactInboundReceipt` | 1 row per inbound receipt | `receipt_variance_days`, `is_on_time_inbound`, `quality_check_passed` |
| `FactFreightInvoice` | 1 row per freight invoice | `invoice_amount_usd`, `base_freight_usd`, `fuel_surcharge_usd`, `accessorial_usd` |
| `FactException` | 1 row per operations exception | `severity`, `status`, `resolution_date` |

---

### `05_data_quality_and_control_totals`
**Reads:** `Gold_OneTruth_Logistics` + `Silver_OneTruth_Logistics` | **Runtime:** PySpark (Spark 3.4+)

Hard quality gate — ALL checks must pass before the semantic model is promoted. Exits via `notebookutils.notebook.exit("PASS" | "FAIL")`.

| Section | Check Type | Detail |
|---|---|---|
| A — Referential Integrity | FK resolution | No `-1` sentinel SKs in `FactShipment`, `FactOrder`, `FactInventorySnapshot` |
| B — Null Key Checks | PK completeness | No null PKs across all 4 main fact tables |
| C — Duplicate Checks | PK uniqueness | No duplicate PKs in facts + `DimCarrier` + `DimProduct` |
| D — Control Totals | Gold ↔ Silver parity | 5 measures validated within **≤ 1% tolerance** |

**Control totals validated (5 of 8 measures — the computationally intensive ones):**
1. On-Time Delivery %
2. Total Freight Cost (USD)
3. Order Fulfilment Rate
4. Supplier On-Time Inbound %
5. Exception Count (row count)

---

### `Foundry_AI_Agent`
**Runtime:** Python 3.12 (Jupyter kernel) | **Dependencies:** `azure-identity`, `azure-ai-projects>=2.1.0`

**Note:** This is a separate demo notebook showing Azure AI Foundry SDK connectivity. The **primary validated agent** is `OneTruth_Logistics_Data_Agent.DataAgent` (Fabric-native), which achieved 8/8 Q&A match in the Technical Validation Report. This Foundry notebook connects to a different endpoint using the same underlying semantic model.

```python
endpoint   = "https://odl-user-2448917-5109-resource.services.ai.azure.com/api/projects/odl-user-2448917-5109"
agent_name = "OneTruth-logistics-Foundry-AI"
# Example: "What is our overall on-time delivery performance this year (2024)?"
```

---

## Data Model — Star Schema

**8 Dimensions · 7 Fact Tables**

```
                            DimDate
                              │
 DimCarrier ──────────────────┤
 DimRoute ────────────────────┼──── FactShipment      (delivery_variance_days · is_on_time)
 DimWarehouse (origin+dest) ──┘

 DimCustomer ──────────────────────── FactOrder         (total_order_value_usd)
 DimWarehouse (fulfilment) ────────────

 DimWarehouse ─────────────────────── FactInventorySnapshot  (available_qty · is_available · is_below_reorder)
 DimProduct ─────────────────────────

 DimWarehouse ─────────────────────── FactWarehouseTask  (is_late_dispatch)
 DimProduct ─────────────────────────

 DimSupplier ─────────────────────── FactInboundReceipt  (receipt_variance_days · is_on_time_inbound)
 DimProduct ─────────────────────────
 DimWarehouse ───────────────────────

 DimCarrier ──────────────────────── FactFreightInvoice  (invoice_amount_usd · base_freight · fuel · accessorial)
 DimCostType ────────────────────────

                                     FactException       (severity · status)
```

---

## 8 Certified Measures — Validated Results

All 8 measures were validated in the Technical Validation Report. Power BI certified answer = Fabric Data Agent answer for all 8 questions (8/8 match, 0 discrepancies).

| # | Measure | Owner | Certified Value | Definition |
|---|---|---|---|---|
| Q01 | **On-Time Delivery %** | Transportation Owner | **84.21%** (464/551 delivered) | `is_on_time = TRUE` ÷ Delivered shipments · year 2024 |
| Q02 | **On-Time Delivery % (by carrier)** | Transportation Owner | Worst: **UPS Freight 69.64%** (39/56) | Same measure sliced by `DimCarrier[carrier_name]` |
| Q03 | **Total Freight Cost** | Logistics Finance Owner | **USD 1,538,176.55** | `SUM(invoice_amount_usd)` WHERE `invoice_status ≠ 'Disputed'` · H1 2024 |
| Q04 | **Freight Cost per Shipment** | Transportation Owner | **USD 4,638.44** | Total Freight Cost ÷ distinct shipments invoiced (both exclude Disputed) |
| Q05 | **Order Fulfilment Rate** | Order Management Owner | **76.4%** (76.36%) | Delivered orders ÷ **non-cancelled orders** (`order_status ≠ 'Cancelled'`) |
| Q06 | **Inventory Availability %** | Inventory Owner | **91.1%** | `is_available = TRUE` ÷ all snapshot rows (`is_available` means `available_qty > 0`) |
| Q07 | **Supplier On-Time Inbound %** | Procurement Owner | **79.7%** (79.72%) | `is_on_time_inbound = TRUE` ÷ Received receipts |
| Q08 | **Exception Count** | Control Tower | **17 open** (4 High · 13 Medium) | Count of exceptions WHERE `severity IN ('High','Medium')` AND `status ≠ 'Resolved'` |

> ⚠️ **Definition notes:**
> - Total Freight Cost **excludes Disputed invoices** — not simply `SUM(invoice_amount_usd)`
> - Order Fulfilment Rate denominator is **non-cancelled orders**, not total orders
> - Exception Count is **open High/Medium** — not total rows in FactException

---

## Fabric IQ Ontology

| Property | Value |
|---|---|
| Entity types | **16** |
| Relationships | **18** |
| Properties | **166** |
| Synonyms | **57** |

**16 Entity types:** `dimcarrier`, `dimcosttype`, `dimcustomer`, `dimdate`, `dimproduct`, `dimroute`, `dimsupplier`, `dimwarehouse`, `factexception`, `factfreightinvoice`, `factinboundreceipt`, `factinventorysnapshot`, `factorder`, `factshipment`, `factwarehousetask`, `Measure_tables`

Synonyms enable the Data Agent to understand terms like `"OTD"`, `"delivery SLA"`, `"how many deliveries were on time"`, `"shipping spend"`, `"logistics cost"`, `"vendor on-time"`, `"inbound SLA"`, `"fill rate"`, `"order completion"` — mapped to certified measure names in the semantic model.

---

## Row-Level Security

Two roles, **2/2 test cases pass**:

| Role | Filter | Test Result |
|---|---|---|
| `Executive_Read_All` | No filter — sees all 10 warehouses, all 7 regions | ✓ PASS |
| `Regional_Warehouse_Manager` | `dimaccounts[email] = USERPRINCIPALNAME()` | ✓ PASS |

**Test evidence:** User `odl_user_2448790` assigned as Viewer under `Regional_Warehouse_Manager` → saw only **New York East DC** (1 warehouse, East region). `Executive_Read_All` test user saw all 10 warehouses.

---

## Agent Architecture

Three agent layers built on the same Gold semantic model:

| Agent | Technology | Purpose |
|---|---|---|
| **Fabric Data Agent** (`OneTruth_Logistics_Data_Agent`) | Fabric-native · grounded on SM + ontology | Primary validated agent — 8/8 Q&A match |
| **Copilot Studio Logistics Copilot** | 4 topics (Transportation, Finance, Warehouse, Order Fulfilment) · Entra auth · RLS · Teams channel | Business-user conversational interface |
| **Azure AI Data Quality Agent** | Azure OpenAI + Notebook 05 · Data Activator alerts | Monitors Bronze→Silver→Gold row counts; fires Teams alert if anomaly > 10% |
| **Foundry AI Agent** (`Foundry_AI_Agent.Notebook`) | `azure-ai-projects>=2.1.0` SDK | Demonstrates Foundry-SDK querying of the same model |

---

## Source Data (`generate_source_data.py`)

Generates synthetic US logistics data. Run from repo root — outputs to `source_data/`.

```bash
python generate_source_data.py
```

| File | Rows | Key Details |
|---|---|---|
| `tms_carriers.csv` | 12 | FedEx, UPS, XPO, J.B. Hunt, Werner, Schneider, DHL, Amazon, SAIA, Old Dominion, ABF, Estes |
| `tms_routes.csv` | 15 | Major US corridors · Road + Rail |
| `erp_customers.csv` | 60 | 3 segments · 7 US regions |
| `erp_suppliers.csv` | 30 | 5 countries · 4 categories |
| `erp_products.csv` | 100 | 7 categories |
| `tms_shipments.csv` | 600 | Jan–Jun 2024 · ~84% OTD (validated) |
| `oms_orders.csv` | 400 | Jan–Jun 2024 · ~76% fulfilment (validated) |
| `oms_order_lines.csv` | ~800 | 1–3 lines per order |
| `wms_warehouse_tasks.csv` | 500 | 10 DCs · 5 task types |
| `wms_inventory_snapshot.csv` | ~800 | Monthly snapshots Jan–Jun 2024 · 91.1% availability (validated) |
| `procurement_inbound_receipts.csv` | 300 | ~79.7% on-time inbound (validated) |
| `finance_freight_invoices.csv` | 400 | Total H1: $1,538,176.55 (validated) |
| `operations_exceptions.csv` | 100 | 7 types · 3 severities · 17 open H+M (validated) |

---

## Setup & Deployment

### Prerequisites
- Microsoft Fabric workspace with Lakehouse capacity
- PySpark runtime (Spark 3.4+)
- Azure AI Foundry project (for `Foundry_AI_Agent` only)

### Steps

1. **Generate source data** (local):
   ```bash
   python generate_source_data.py
   ```

2. **Create 3 Lakehouses** in your Fabric workspace:
   - `Bronse_OneTruth_Logistics`
   - `Silver_OneTruth_Logistics`
   - `Gold_OneTruth_Logistics`

3. **Upload CSV files** to `Bronse_OneTruth_Logistics/Files/`

4. **Update `# META` blocks** in each notebook — replace workspace ID and lakehouse IDs with your own.

5. **Run notebooks in order** (or trigger via `Pipeline_OneTruth_Logistics`):
   ```
   01_bronze_ingestion
       → 02_silver_transformation
           → 03_gold_dimensions
               → 04_gold_facts
                   → 05_data_quality_and_control_totals  ← must exit PASS
   ```

6. **Import the Semantic Model** (`OneTruth_Logistics_SM.SemanticModel`) pointing to `Gold_OneTruth_Logistics`.

7. **Deploy Ontology** (`OneTruth_Logistics_Ontology.Ontology`) — 16 entities, 18 relationships, 57 synonyms.

8. **Deploy Fabric Data Agent** (`OneTruth_Logistics_Data_Agent.DataAgent`) with grounding rules pointing to `OneTruth_Logistics_SM`.

9. **Configure AI Foundry** — update `endpoint` and `tenant_id` in `Foundry_AI_Agent.Notebook/notebook-content.py`.

---

## Tech Stack

| Component | Technology |
|---|---|
| Lakehouse storage | Microsoft Fabric OneLake (Delta Lake) |
| Processing engine | PySpark (Spark 3.4+) — Synapse Runtime |
| Pipeline orchestration | Fabric Data Pipeline |
| Semantic layer | Power BI Direct Lake Semantic Model (8 certified measures) |
| Ontology | Fabric IQ Ontology (16 entities · 18 relationships · 57 synonyms) |
| Primary AI agent | Fabric Data Agent — 8/8 Q&A validated |
| Extended AI | Copilot Studio · Azure AI Foundry (`azure-ai-projects>=2.1.0`) · Data Activator |
| Authentication | Azure DeviceCodeCredential / ManagedIdentityCredential · Entra |
| Data quality | Custom DQ framework — Silver→Gold control totals with ≤1% tolerance gate |

---

## Validation Summary

> Full evidence in `OneTruth_Logistics_Technical_Validation_Report.docx`

| Check | Result |
|---|---|
| Q&A accuracy | **8 / 8 questions matched** (0 discrepancies) |
| OTD (Q01) | Certified 84.21% vs Agent 84.2% ✓ |
| Worst carrier (Q02) | Certified UPS Freight 69.64% vs Agent 69.6% ✓ |
| Total freight cost (Q03) | Certified $1,538,176.55 vs Agent $1,538,176.55 ✓ |
| Avg cost/shipment (Q04) | Certified $4,638.44 vs Agent $4,638.44 ✓ |
| Order fulfilment (Q05) | Certified 76.4% vs Agent 76.4% ✓ |
| Inventory availability (Q06) | Certified 91.1% vs Agent 91.1% ✓ |
| Supplier OTD (Q07) | Certified 79.7% vs Agent 79.7% ✓ |
| Open exceptions H+M (Q08) | Certified 17 vs Agent 17 ✓ |
| RLS Executive_Read_All | All 10 warehouses visible ✓ |
| RLS Regional_Warehouse_Manager | 1 warehouse (New York East DC) ✓ |
| DQ gate | All sections A–D PASS ✓ |

---

## Documents

| Document | Description |
|---|---|
| `OneTruth_Logistics_Architecture_Blueprint.docx` | End-to-end solution design · 6 conformed subject areas |
| `OneTruth_Logistics_Direct_Lake_Semantic_Model_Design.docx` | Star schema · 8 certified measure definitions with owner + calculation logic |
| `OneTruth_Logistics_Fabric_IQ_Ontology_Design.docx` | Ontology entity/relationship design · 16 entities · 57 synonyms |
| `OneTruth_Logistics_Hackathon_Execution_Plan.docx` | 10-day build plan · task breakdown · 3-agent architecture |
| `OneTruth_Logistics_Technical_Validation_Report.docx` | **Full validation evidence** · 8/8 Q&A · RLS tests · control totals |

---

*Built for the Microsoft Fabric Hackathon — workspace: Three Pilots Hackthon Workspace. Source data fully synthetic.*
