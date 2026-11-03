# 01 Deduplication Practice — dropDuplicates vs Window

Rendered snapshot of `01 Deduplication Practice - dropDuplicates vs Window.ipynb` **with cell outputs included** (generated 2026-11-03). The `.ipynb` notebook is the source of truth — regenerate this file after updates. Values in backticks show padded whitespace; NULLs are written as NULL.

## Dataset — `dirty_orders`

24 rows, 20 unique order IDs, all-STRING schema, realistic problems: exact duplicate (`ORD1002`), NULLs in 4 columns, negative quantity (`ORD1004` −5, `ORD1017` −2), zero quantity/price, `$`-prefixed price strings, padded spaces (` jane doe `, ` USA `, ` 2 `), mixed country casing (usa/USA/CANADA), a future date (`ORD1010` = 2050-01-01), and same-order dedup scenarios (`ORD1007` null-vs-dated, `ORD1009` later corrected row, `ORD1018` same-date tie broken only by `ingested_at`).

```python
dirty_orders = spark.createDataFrame(
    dirty_rows,
    "order_id STRING, customer_name STRING, country STRING, product STRING, quantity STRING, unit_price STRING, order_date STRING, ingested_at STRING",
)

display(dirty_orders)
```

**Output — 24 rows:**

| order_id | customer_name | country | product | quantity | unit_price | order_date | ingested_at |
| --- | --- | --- | --- | --- | --- | --- | --- |
| ORD1001 | John Smith | USA | Laptop | 2 | $1200.00 | 2024-05-13 | 2024-06-02 08:15:00 |
| ORD1002 | ` jane doe ` | ` canada` | Mouse | 5 | $25.50 | 2024-05-14 | 2024-06-02 08:15:00 |
| ORD1002 | ` jane doe ` | ` canada` | Mouse | 5 | $25.50 | 2024-05-14 | 2024-06-02 08:15:00 |
| ORD1003 | NULL | USA | Keyboard | 1 | $45.00 | 2024-05-15 | 2024-06-02 08:15:00 |
| ORD1004 | Alice Johnson | UK | Monitor | -5 | $180.00 | 2024-05-16 | 2024-06-02 08:15:00 |
| ORD1005 | bob brown | usa | Laptop | 3 | NULL | 2024-05-17 | 2024-06-02 08:15:00 |
| ORD1006 | Charlie Davis | Canada | Headphones | 0 | $60.00 | 2024-05-18 | 2024-06-02 08:15:00 |
| ORD1007 | NULL | USA | Mouse | 2 | $25.50 | NULL | 2024-06-02 08:15:00 |
| ORD1007 | NULL | USA | Mouse | 9 | $25.50 | 2024-05-22 | 2024-06-03 10:05:00 |
| ORD1008 | David Lee | ` USA ` | Keyboard | 1 | $45.00 | 2024-05-20 | 2024-06-02 08:15:00 |
| ORD1009 | Eve Wilson | usa | Monitor | 4 | $180.00 | 2024-05-21 | 2024-06-02 08:15:00 |
| ORD1009 | Eve Wilson | usa | Monitor | 2 | $180.00 | 2024-06-10 | 2024-06-03 09:40:00 |
| ORD1010 | Frank Miller | Canada | Laptop | 1 | $1200.00 | 2050-01-01 | 2024-06-02 08:15:00 |
| ORD1011 | ` grace taylor ` | USA | Mouse | NULL | $25.50 | 2024-05-23 | 2024-06-02 08:15:00 |
| ORD1012 | Henry Adams | UK | Headphones | 2 | $0.00 | 2024-05-24 | 2024-06-02 08:15:00 |
| ORD1013 | Ivy Clark | CANADA | Monitor | 2 | $180.00 | 2024-05-25 | 2024-06-02 08:15:00 |
| ORD1014 | NULL | usa | Keyboard | 3 | $45.00 | 2024-05-26 | 2024-06-02 08:15:00 |
| ORD1015 | Jack White | USA | Laptop | NULL | NULL | NULL | 2024-06-02 08:15:00 |
| ORD1016 | Karen Hall | ` Canada` | Mouse | 5 | $25.50 | 2024-05-28 | 2024-06-02 08:15:00 |
| ORD1017 | Leo King | usa | Monitor | -2 | $180.00 | 2024-05-29 | 2024-06-02 08:15:00 |
| ORD1018 | Mona Scott | USA | Headphones | 1 | $60.00 | 2024-05-30 | 2024-06-02 08:15:00 |
| ORD1018 | Mona Scott | USA | Headphones | 7 | $60.00 | 2024-05-30 | 2024-06-03 11:20:00 |
| ORD1019 | Nick Green | UK | Keyboard | ` 2 ` | $45.00 | 2024-05-31 | 2024-06-02 08:15:00 |
| ORD1020 | Olivia Baker | Canada | Mouse | 3 | $25.50 | 2024-06-01 | 2024-06-02 08:15:00 |

## 1. Quick dedup — dropDuplicates()

**Question:** remove duplicate orders so each order appears exactly once.

* `dropDuplicates()` — no arguments — removes rows identical across **ALL** columns (exact duplicates only)
* `dropDuplicates(['order_id'])` — keeps **one row per order_id**, ignoring other columns — the survivor is **arbitrary** (non-deterministic)
* Interview point: if the duplicate rows disagree (quantity 5 vs 3), by-key dedup picks one at random — to keep the *latest* record you need a window function with a business rule

```python
dirty_orders.dropDuplicates(['order_id']).display()
```

**Output — 20 rows, one per order_id (survivor arbitrary):**

| order_id | customer_name | country | product | quantity | unit_price | order_date | ingested_at |
| --- | --- | --- | --- | --- | --- | --- | --- |
| ORD1001 | John Smith | USA | Laptop | 2 | $1200.00 | 2024-05-13 | 2024-06-02 08:15:00 |
| ORD1002 | ` jane doe ` | ` canada` | Mouse | 5 | $25.50 | 2024-05-14 | 2024-06-02 08:15:00 |
| ORD1003 | NULL | USA | Keyboard | 1 | $45.00 | 2024-05-15 | 2024-06-02 08:15:00 |
| ORD1004 | Alice Johnson | UK | Monitor | -5 | $180.00 | 2024-05-16 | 2024-06-02 08:15:00 |
| ORD1005 | bob brown | usa | Laptop | 3 | NULL | 2024-05-17 | 2024-06-02 08:15:00 |
| ORD1006 | Charlie Davis | Canada | Headphones | 0 | $60.00 | 2024-05-18 | 2024-06-02 08:15:00 |
| ORD1007 | NULL | USA | Mouse | 2 | $25.50 | NULL | 2024-06-02 08:15:00 |
| ORD1008 | David Lee | ` USA ` | Keyboard | 1 | $45.00 | 2024-05-20 | 2024-06-02 08:15:00 |
| ORD1009 | Eve Wilson | usa | Monitor | 4 | $180.00 | 2024-05-21 | 2024-06-02 08:15:00 |
| ORD1010 | Frank Miller | Canada | Laptop | 1 | $1200.00 | 2050-01-01 | 2024-06-02 08:15:00 |
| ORD1011 | ` grace taylor ` | USA | Mouse | NULL | $25.50 | 2024-05-23 | 2024-06-02 08:15:00 |
| ORD1012 | Henry Adams | UK | Headphones | 2 | $0.00 | 2024-05-24 | 2024-06-02 08:15:00 |
| ORD1013 | Ivy Clark | CANADA | Monitor | 2 | $180.00 | 2024-05-25 | 2024-06-02 08:15:00 |
| ORD1014 | NULL | usa | Keyboard | 3 | $45.00 | 2024-05-26 | 2024-06-02 08:15:00 |
| ORD1015 | Jack White | USA | Laptop | NULL | NULL | NULL | 2024-06-02 08:15:00 |
| ORD1016 | Karen Hall | ` Canada` | Mouse | 5 | $25.50 | 2024-05-28 | 2024-06-02 08:15:00 |
| ORD1017 | Leo King | usa | Monitor | -2 | $180.00 | 2024-05-29 | 2024-06-02 08:15:00 |
| ORD1018 | Mona Scott | USA | Headphones | 1 | $60.00 | 2024-05-30 | 2024-06-02 08:15:00 |
| ORD1019 | Nick Green | UK | Keyboard | ` 2 ` | $45.00 | 2024-05-31 | 2024-06-02 08:15:00 |
| ORD1020 | Olivia Baker | Canada | Mouse | 3 | $25.50 | 2024-06-01 | 2024-06-02 08:15:00 |

⚠ **Non-determinism, live proof:** this run kept the NULL-dated `ORD1007` row (qty 2), the qty-4 `ORD1009`, and the qty-1 `ORD1018` — a previous run kept qty 9 / qty 2 / qty 7. Different run, different survivors: exactly the arbitrary-survivor risk.

## 2. Latest record per order — SQL window function

**Question:** keep the latest row per order_id, deterministically, via a temp view.

* `row_number() over (partition by order_id order by ...)` ranks rows within each order — same engine and execution plan as the DataFrame API
* A window function can't be filtered in the same SELECT level → wrap in a subquery, filter `rn = 1` outside
* `desc nulls last` — in Spark, `DESC` sorts NULLs **FIRST**; without `NULLS LAST` a NULL-dated row would wrongly win
* Known gap in this version: no `ingested_at` tiebreaker → same-date ties (`ORD1018`) are still arbitrary

```python
dirty_orders.createOrReplaceTempView("sample")
```

```sql
select * from (
  select *, row_number() over (
    partition by order_id
    order by order_date desc nulls last
  ) as rn
  from sample
) where rn = 1
```

**Output — 20 rows (deterministic survivors except same-date ties):**

| order_id | customer_name | country | product | quantity | unit_price | order_date | ingested_at | rn |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ORD1001 | John Smith | USA | Laptop | 2 | $1200.00 | 2024-05-13 | 2024-06-02 08:15:00 | 1 |
| ORD1002 | ` jane doe ` | ` canada` | Mouse | 5 | $25.50 | 2024-05-14 | 2024-06-02 08:15:00 | 1 |
| ORD1003 | NULL | USA | Keyboard | 1 | $45.00 | 2024-05-15 | 2024-06-02 08:15:00 | 1 |
| ORD1004 | Alice Johnson | UK | Monitor | -5 | $180.00 | 2024-05-16 | 2024-06-02 08:15:00 | 1 |
| ORD1005 | bob brown | usa | Laptop | 3 | NULL | 2024-05-17 | 2024-06-02 08:15:00 | 1 |
| ORD1006 | Charlie Davis | Canada | Headphones | 0 | $60.00 | 2024-05-18 | 2024-06-02 08:15:00 | 1 |
| ORD1007 | NULL | USA | Mouse | 9 | $25.50 | 2024-05-22 | 2024-06-03 10:05:00 | 1 |
| ORD1008 | David Lee | ` USA ` | Keyboard | 1 | $45.00 | 2024-05-20 | 2024-06-02 08:15:00 | 1 |
| ORD1009 | Eve Wilson | usa | Monitor | 2 | $180.00 | 2024-06-10 | 2024-06-03 09:40:00 | 1 |
| ORD1010 | Frank Miller | Canada | Laptop | 1 | $1200.00 | 2050-01-01 | 2024-06-02 08:15:00 | 1 |
| ORD1011 | ` grace taylor ` | USA | Mouse | NULL | $25.50 | 2024-05-23 | 2024-06-02 08:15:00 | 1 |
| ORD1012 | Henry Adams | UK | Headphones | 2 | $0.00 | 2024-05-24 | 2024-06-02 08:15:00 | 1 |
| ORD1013 | Ivy Clark | CANADA | Monitor | 2 | $180.00 | 2024-05-25 | 2024-06-02 08:15:00 | 1 |
| ORD1014 | NULL | usa | Keyboard | 3 | $45.00 | 2024-05-26 | 2024-06-02 08:15:00 | 1 |
| ORD1015 | Jack White | USA | Laptop | NULL | NULL | NULL | 2024-06-02 08:15:00 | 1 |
| ORD1016 | Karen Hall | ` Canada` | Mouse | 5 | $25.50 | 2024-05-28 | 2024-06-02 08:15:00 | 1 |
| ORD1017 | Leo King | usa | Monitor | -2 | $180.00 | 2024-05-29 | 2024-06-02 08:15:00 | 1 |
| ORD1018 | Mona Scott | USA | Headphones | 1 | $60.00 | 2024-05-30 | 2024-06-02 08:15:00 | 1 |
| ORD1019 | Nick Green | UK | Keyboard | ` 2 ` | $45.00 | 2024-05-31 | 2024-06-02 08:15:00 | 1 |
| ORD1020 | Olivia Baker | Canada | Mouse | 3 | $25.50 | 2024-06-01 | 2024-06-02 08:15:00 | 1 |

Note: `ORD1007` → qty 9 and `ORD1009` → qty 2 survived correctly, but `ORD1018` kept **qty 1** — its two rows share the same `order_date` and this version has no `ingested_at` tiebreaker, so the tie is still arbitrary. The next solution fixes that.

## 3. Same dedup — PySpark DataFrame API (both fixes)

**Question:** latest row per order_id, no temp view, deterministic — with `nulls last` AND the `ingested_at` tiebreaker.

* SQL → PySpark translation: spaces become **DOTS** — `row_number().over(...)`, `Window.partitionBy().orderBy(...)`
* Plain column-name strings work for "use this column as-is"; `col()` is required when modifying the column (`.desc()`, `.cast()`)
* 3-second self-check: **dot before `over` — chain on `Window` — comparison inside `filter`**

```python
orders_dedup = dirty_orders.withColumn(
    'rn', row_number().over(Window.partitionBy(col('order_id')).orderBy(col('order_date').desc_nulls_last(), col('ingested_at').desc()))
).filter(col('rn') == 1)

orders_dedup.display()
```

**Output — 20 rows, `orders_dedup` (fully deterministic survivors):**

| order_id | customer_name | country | product | quantity | unit_price | order_date | ingested_at | rn |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ORD1001 | John Smith | USA | Laptop | 2 | $1200.00 | 2024-05-13 | 2024-06-02 08:15:00 | 1 |
| ORD1002 | ` jane doe ` | ` canada` | Mouse | 5 | $25.50 | 2024-05-14 | 2024-06-02 08:15:00 | 1 |
| ORD1003 | NULL | USA | Keyboard | 1 | $45.00 | 2024-05-15 | 2024-06-02 08:15:00 | 1 |
| ORD1004 | Alice Johnson | UK | Monitor | -5 | $180.00 | 2024-05-16 | 2024-06-02 08:15:00 | 1 |
| ORD1005 | bob brown | usa | Laptop | 3 | NULL | 2024-05-17 | 2024-06-02 08:15:00 | 1 |
| ORD1006 | Charlie Davis | Canada | Headphones | 0 | $60.00 | 2024-05-18 | 2024-06-02 08:15:00 | 1 |
| ORD1007 | NULL | USA | Mouse | 9 | $25.50 | 2024-05-22 | 2024-06-03 10:05:00 | 1 |
| ORD1008 | David Lee | ` USA ` | Keyboard | 1 | $45.00 | 2024-05-20 | 2024-06-02 08:15:00 | 1 |
| ORD1009 | Eve Wilson | usa | Monitor | 2 | $180.00 | 2024-06-10 | 2024-06-03 09:40:00 | 1 |
| ORD1010 | Frank Miller | Canada | Laptop | 1 | $1200.00 | 2050-01-01 | 2024-06-02 08:15:00 | 1 |
| ORD1011 | ` grace taylor ` | USA | Mouse | NULL | $25.50 | 2024-05-23 | 2024-06-02 08:15:00 | 1 |
| ORD1012 | Henry Adams | UK | Headphones | 2 | $0.00 | 2024-05-24 | 2024-06-02 08:15:00 | 1 |
| ORD1013 | Ivy Clark | CANADA | Monitor | 2 | $180.00 | 2024-05-25 | 2024-06-02 08:15:00 | 1 |
| ORD1014 | NULL | usa | Keyboard | 3 | $45.00 | 2024-05-26 | 2024-06-02 08:15:00 | 1 |
| ORD1015 | Jack White | USA | Laptop | NULL | NULL | NULL | 2024-06-02 08:15:00 | 1 |
| ORD1016 | Karen Hall | ` Canada` | Mouse | 5 | $25.50 | 2024-05-28 | 2024-06-02 08:15:00 | 1 |
| ORD1017 | Leo King | usa | Monitor | -2 | $180.00 | 2024-05-29 | 2024-06-02 08:15:00 | 1 |
| ORD1018 | Mona Scott | USA | Headphones | 7 | $60.00 | 2024-05-30 | 2024-06-03 11:20:00 | 1 |
| ORD1019 | Nick Green | UK | Keyboard | ` 2 ` | $45.00 | 2024-05-31 | 2024-06-02 08:15:00 | 1 |
| ORD1020 | Olivia Baker | Canada | Mouse | 3 | $25.50 | 2024-06-01 | 2024-06-02 08:15:00 | 1 |

With the `ingested_at` tiebreaker, `ORD1018` now keeps **qty 7** (latest ingest). `orders_dedup` is the input for Notebook 02.

## Key Concepts Summary

| Concept | Rule / Distinction |
| --- | --- |
| `dropDuplicates()` vs `dropDuplicates(['id'])` | exact duplicates vs one row per key — by-key survivor is arbitrary |
| Deterministic dedup | `row_number` + `partition by` key + justified ordering; keep `rn = 1` |
| Subquery pattern | a window function can't be filtered in its own SELECT level |
| `DESC` + NULLs | DESC sorts NULLs first — always `NULLS LAST` |
| Ties | same key + same date = still arbitrary without a tiebreaker (`ingested_at`) |
| SQL ↔ PySpark | spaces → dots; `over()` is a method; chain on `Window`; compare inside `filter()` |
| `col()` vs strings | string = column as-is; `col()` = about to modify the column |

**Expected results on this dataset:** 24 rows → 20 unique orders. Window survivors: `ORD1007` → qty 9, `ORD1009` → qty 2 (corrected), `ORD1018` → qty 7 (latest `ingested_at`).

**Pipeline so far:** `dirty_orders` (24 rows) → `orders_dedup` (20 rows, this notebook) → `orders_clean` (Notebook 02).
