# Sales Analytics Project

An end-to-end retail sales data pipeline built on **Databricks Free Edition** using **PySpark**, **Spark SQL**, **Delta Lake**, and **Unity Catalog**. The project ingests raw retail order and customer data, cleans and standardizes it through a medallion architecture (Bronze → Silver → Gold), and powers an interactive **AI/BI Dashboard** for sales analytics.

---

## Architecture: Medallion (Bronze → Silver → Gold)

```
┌─────────────────┐     ┌──────────────────────┐     ┌──────────────────────┐
│     BRONZE       │     │       SILVER        │     │        GOLD          │
│  (Raw Data)      │────▶│  (Cleaned & Typed)   │────▶│  (Aggregated Analytics)│
├─────────────────┤     ├──────────────────────┤     ├──────────────────────┤
│ retail_dataset   │     │ silver_orders        │     │ gold_aggregates      │
│   .csv           │     │ silver_customers     │     │                      │
│ customer_dataset │     │                      │     │                      │
│   .json          │     │                      │     │                      │
└─────────────────┘     └──────────────────────┘     └──────────────────────┘
   Unity Catalog            Unity Catalog              Unity Catalog
   Volume (bronze)          Delta Tables               Delta Table
```

### Bronze Layer (Raw Ingestion)

Raw data files are stored in a Unity Catalog volume at `/Volumes/project/sales/data/bronze/`:

* **retail_dataset.csv** — Order-level data: `order_id`, `order_date`, `customer_id`, `customer_name`, `product_id`, `product_name`, `category`, `quantity`, `price`, `payment_type`, `order_status`, `returned`
* **customer_dataset.json** — Customer profiles: `age`, `city`, `customer_id`, `gender`, `loyalty_tier`, `signup_date`

### Silver Layer (Cleaning & Standardization)

Raw data is cleaned and written to Delta tables in the `project.sales` schema:

* **silver_orders** — Cleaned order records with:
  * Date parsing from multiple formats (`yyyy-MM-dd`, `dd/MM/yyyy`, `d-MMM-yyyy`, `MM-dd-yyyy`, `dd-MM-yyyy`)
  * Price cleaning: strips `$` and `,` characters, casts to `double`
  * Quantity validation: nulls and negatives replaced with `1`
  * Text normalization: `lower()`, `trim()` on categorical fields
  * Deduplication on `(order_id, product_id)`
  * Computed `total_amount = quantity × price`

* **silver_customers** — Cleaned customer records with:
  * Date parsing from multiple formats (`yyyy-MM-dd`, `dd/MM/yyyy`, `d-MMM-yyyy`, `yyyy/MM/dd`)
  * Age validation: nulls and negatives set to `None`
  * Text normalization: `lower()`, `trim()` on `gender`, `city`, `loyalty_tier`

### Gold Layer (Aggregated Analytics)

The gold table `gold_aggregates` is built by joining silver orders with silver customers and aggregating:

| Column | Description |
| --- | --- |
| `product_id`, `product_name` | Product identifier and name |
| `customer_id` | Customer identifier |
| `category` | Product category |
| `year`, `month`, `order_date` | Time dimensions extracted from order timestamp |
| `gender`, `age`, `city`, `loyalty_tier` | Customer demographics |
| `toatl_sales` | Sum of `total_amount` (total sales revenue) |
| `total_quantity` | Sum of quantity sold |
| `total_orders` | Count of distinct orders |
| `min_price`, `max_price`, `avg_unit_price` | Price statistics |
| `returned_count`, `returned_amount` | Return metrics |

---

## Notebook: `project_notebook`

The notebook (`project_notebook.ipynb`) contains the full pipeline. Cells are organized as:

| Cell | Title | Language | Purpose |
| --- | --- | --- | --- |
| 1 | Create Database | SQL | `CREATE DATABASE project.sales` |
| 2 | Create Volume | SQL | `CREATE VOLUME project.sales.data` |
| 3 | Load Raw Orders (Bronze) | Python | Read `retail_dataset.csv` from UC Volume |
| 4 | Load Raw Customers (Bronze) | Python | Read `customer_dataset.json` from UC Volume |
| 5 | Preview Raw Orders | Python | `display(raw_orders.limit(5))` |
| 6 | Clean Orders (Silver) | Python | Date parsing, price cleaning, dedup, write to `silver_orders` |
| 7 | Preview Raw Customers | Python | `raw_customers.limit(7).display()` |
| 8 | Clean Customers (Silver) | Python | Date parsing, age validation, write to `silver_customers` |
| 9 | Gold Aggregates | Python | Join silver tables, aggregate, write to `gold_aggregates` |
| 10 | Query Gold Aggregates | SQL | `SELECT * FROM project.sales.gold_aggregates` |
| 11–18 | Exploration | Python/SQL | Ad-hoc exploration with temp views (`sample`, `sample2`) |

---

## Dashboard: `Project_Dashboard`

The AI/BI Dashboard (`Project_Dashboard.lvdash.json`) is built on top of the `project.sales.gold_aggregates` table. It includes:

### Dataset

* **Source:** `project.sales.gold_aggregates`
* **Custom Measures:**
  * `avg_order_value` = `SUM(toatl_sales) / SUM(total_orders)`
  * `avg_price_per_item` = `SUM(toatl_sales) / SUM(total_quantity)`

### Widgets

| Widget | Type | Description |
| --- | --- | --- |
| SALES ANALYSIS | Text | Title banner |
| Total Sales by Category | Pie Chart | Sales split by product category |
| Average Order Value by City | Bar Chart | AOV ranked by city |
| Total Quantity by Month | Bar Chart | Monthly quantity trend |
| Average Price per Item by Loyalty Tier | Pie Chart | Price per item by loyalty tier |
| Total Sales by Month | Line Chart | Monthly sales trend |
| Average Order Value by Gender | Bar Chart | AOV by gender |
| Total Sales by Product Name | Bar Chart | Top 10 products by revenue |

### Filters

* **Age** — Single-select filter
* **Product Name** — Single-select filter
* **Category** — Single-select filter

---

## How to Reproduce

### Prerequisites

* Databricks workspace (built and tested on **Free Edition**)
* Unity Catalog enabled
* Serverless compute (CPU)

### Steps

1. **Upload raw data** to a Unity Catalog volume:
   * Create a catalog and schema (e.g., `project.sales`)
   * Create a volume (e.g., `project.sales.data`)
   * Upload `retail_dataset.csv` and `customer_dataset.json` to `/Volumes/project/sales/data/bronze/`

2. **Run the notebook** (`project_notebook.ipynb`):
   * Execute cells top-to-bottom
   * Cells 1–2 create the database and volume
   * Cells 3–4 load raw data from the volume
   * Cells 6 and 8 clean the data and write silver Delta tables
   * Cell 9 joins and aggregates into the gold table

3. **Import the dashboard** (`Project_Dashboard.lvdash.json`):
   * In Databricks, go to **Dashboards** → click **Import**
   * Select the `.lvdash.json` file
   * The dashboard connects to `project.sales.gold_aggregates` automatically

---

## Tech Stack

| Component | Technology |
| --- | --- |
| Compute | Databricks Serverless (CPU) |
| Processing | Apache Spark / PySpark |
| Storage | Delta Lake (Unity Catalog managed tables) |
| Data Governance | Unity Catalog (catalogs, schemas, volumes) |
| Visualization | Databricks AI/BI Dashboard (Lakeview) |
| Version Control | Git (Databricks Git Folders) |

---

## Key Data Engineering Techniques

* **Multi-format date parsing** using `coalesce()` with multiple `to_timestamp()` patterns
* **Data cleaning** with `trim()`, `lower()`, `translate()` for string normalization
* **Type casting** with null and negative value handling
* **Deduplication** using `dropDuplicates()` with composite keys
* **Medallion architecture** with clear separation between raw, cleaned, and aggregated layers
* **Delta tables** with `mode("overwrite")` for idempotent pipeline re-runs
* **Unity Catalog** for centralized data governance and volume-based file storage

---

## Project Structure

```
project_sales/
├── project_notebook.ipynb          # Main pipeline notebook (Bronze → Silver → Gold)
├── Project_Dashboard.lvdash.json   # AI/BI Dashboard definition
└── README.md                       # This file
```

---

*Built on Databricks Free Edition as a learning project for data engineering with PySpark, Delta Lake, and Unity Catalog.*