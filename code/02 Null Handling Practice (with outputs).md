# 02 Null Handling Practice — dropna vs fillna vs when/otherwise

Rendered snapshot of `02 Null Handling Practice - dropna vs fillna vs when.ipynb` **with cell outputs included** (generated 2026-11-03). The `.ipynb` notebook is the source of truth — regenerate this file after updates. Values in backticks show padded whitespace; NULLs are written as NULL.

Part 2 of the interview drill. Picks up exactly where **01 Deduplication Practice** left off: `orders_dedup` — one row per `order_id`, 20 rows, `rn` already dropped. All remaining dirt (NULLs, negatives, padded values, `$` prices, mixed casing, future date) is still present, waiting for later stages.

**The question:** `quantity` is a STRING column containing NULLs, negatives (−5, −2) and a padded `' 2 '`. Produce a clean **INT** column `quantity_clean` where NULL → 1 and negative → 1, valid values unchanged.

## Dataset — `orders_dedup` (output of Notebook 01, 20 rows)

```python
orders_dedup = spark.createDataFrame(
    orders_dedup_rows,
    "order_id STRING, customer_name STRING, country STRING, product STRING, quantity STRING, unit_price STRING, order_date STRING, ingested_at STRING",
)

display(orders_dedup)
```

**Output — 20 rows:**

| order_id | customer_name | country | product | quantity | unit_price | order_date | ingested_at |
| --- | --- | --- | --- | --- | --- | --- | --- |
| ORD1001 | John Smith | USA | Laptop | 2 | $1200.00 | 2024-05-13 | 2024-06-02 08:15:00 |
| ORD1002 | ` jane doe ` | ` canada` | Mouse | 5 | $25.50 | 2024-05-14 | 2024-06-02 08:15:00 |
| ORD1003 | NULL | USA | Keyboard | 1 | $45.00 | 2024-05-15 | 2024-06-02 08:15:00 |
| ORD1004 | Alice Johnson | UK | Monitor | -5 | $180.00 | 2024-05-16 | 2024-06-02 08:15:00 |
| ORD1005 | bob brown | usa | Laptop | 3 | NULL | 2024-05-17 | 2024-06-02 08:15:00 |
| ORD1006 | Charlie Davis | Canada | Headphones | 0 | $60.00 | 2024-05-18 | 2024-06-02 08:15:00 |
| ORD1007 | NULL | USA | Mouse | 9 | $25.50 | 2024-05-22 | 2024-06-03 10:05:00 |
| ORD1008 | David Lee | ` USA ` | Keyboard | 1 | $45.00 | 2024-05-20 | 2024-06-02 08:15:00 |
| ORD1009 | Eve Wilson | usa | Monitor | 2 | $180.00 | 2024-06-10 | 2024-06-03 09:40:00 |
| ORD1010 | Frank Miller | Canada | Laptop | 1 | $1200.00 | 2050-01-01 | 2024-06-02 08:15:00 |
| ORD1011 | ` grace taylor ` | USA | Mouse | NULL | $25.50 | 2024-05-23 | 2024-06-02 08:15:00 |
| ORD1012 | Henry Adams | UK | Headphones | 2 | $0.00 | 2024-05-24 | 2024-06-02 08:15:00 |
| ORD1013 | Ivy Clark | CANADA | Monitor | 2 | $180.00 | 2024-05-25 | 2024-06-02 08:15:00 |
| ORD1014 | NULL | usa | Keyboard | 3 | $45.00 | 2024-05-26 | 2024-06-02 08:15:00 |
| ORD1015 | Jack White | USA | Laptop | NULL | NULL | NULL | 2024-06-02 08:15:00 |
| ORD1016 | Karen Hall | ` Canada` | Mouse | 5 | $25.50 | 2024-05-28 | 2024-06-02 08:15:00 |
| ORD1017 | Leo King | usa | Monitor | -2 | $180.00 | 2024-05-29 | 2024-06-02 08:15:00 |
| ORD1018 | Mona Scott | USA | Headphones | 7 | $60.00 | 2024-05-30 | 2024-06-03 11:20:00 |
| ORD1019 | Nick Green | UK | Keyboard | ` 2 ` | $45.00 | 2024-05-31 | 2024-06-02 08:15:00 |
| ORD1020 | Olivia Baker | Canada | Mouse | 3 | $25.50 | 2024-06-01 | 2024-06-02 08:15:00 |

## 1. Null handling on `quantity` — notes

**Decision rules — which tool, when:**

* `dropna` — row-level **deletion**. Use when a row is too incomplete to be trusted and the business accepts losing it (e.g. ORD1015: NULL quantity AND price AND date — no revenue can ever come from it). It's data loss, so ask first.
* `fillna` — unconditional blanket replacement for the whole column. Safe only when one constant is correct for every NULL (e.g. empty discount → 0). Could NOT solve this question — "negative → 1" depends on the value.
* `when/otherwise` — conditional, value-dependent replacement. NULLs are just one branch, and it's the only one of the three that can also transform non-null values.

**Why dedup BEFORE cleaning:** cleaning rows you're about to throw away is wasted work, and a transform like "negative → 1" can make conflicting duplicates look identical — destroying the evidence needed to pick the right survivor.

**Business risk of filling NULL quantity with 1:** fabricates demand — 1 implies a real order of one unit, so revenue (qty × price) and inventory forecasts count phantom units. NULL might mean a cancelled/incomplete order, not "one unit". Safer: keep NULL (SUM/AVG ignore NULLs anyway), fill 0 if business says "no items ordered", or flag for review — and document the choice.

## Solution — `quantity_clean`

```python
from pyspark.sql.functions import lit, when, trim, col

orders_clean = orders_dedup.withColumn(
    "quantity_clean",
    when(
        trim(col("quantity")).isNull() | (trim(col("quantity")).cast("int") < 0),
        lit(1),
    ).otherwise(trim(col("quantity")).cast("int")),
)

orders_clean.display()
```

**Output — 20 rows, `orders_clean` (`quantity_clean` is INT):**

| order_id | customer_name | country | product | quantity | unit_price | order_date | ingested_at | quantity_clean |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ORD1001 | John Smith | USA | Laptop | 2 | $1200.00 | 2024-05-13 | 2024-06-02 08:15:00 | 2 |
| ORD1002 | ` jane doe ` | ` canada` | Mouse | 5 | $25.50 | 2024-05-14 | 2024-06-02 08:15:00 | 5 |
| ORD1003 | NULL | USA | Keyboard | 1 | $45.00 | 2024-05-15 | 2024-06-02 08:15:00 | 1 |
| ORD1004 | Alice Johnson | UK | Monitor | -5 | $180.00 | 2024-05-16 | 2024-06-02 08:15:00 | 1 |
| ORD1005 | bob brown | usa | Laptop | 3 | NULL | 2024-05-17 | 2024-06-02 08:15:00 | 3 |
| ORD1006 | Charlie Davis | Canada | Headphones | 0 | $60.00 | 2024-05-18 | 2024-06-02 08:15:00 | 0 |
| ORD1007 | NULL | USA | Mouse | 9 | $25.50 | 2024-05-22 | 2024-06-03 10:05:00 | 9 |
| ORD1008 | David Lee | ` USA ` | Keyboard | 1 | $45.00 | 2024-05-20 | 2024-06-02 08:15:00 | 1 |
| ORD1009 | Eve Wilson | usa | Monitor | 2 | $180.00 | 2024-06-10 | 2024-06-03 09:40:00 | 2 |
| ORD1010 | Frank Miller | Canada | Laptop | 1 | $1200.00 | 2050-01-01 | 2024-06-02 08:15:00 | 1 |
| ORD1011 | ` grace taylor ` | USA | Mouse | NULL | $25.50 | 2024-05-23 | 2024-06-02 08:15:00 | 1 |
| ORD1012 | Henry Adams | UK | Headphones | 2 | $0.00 | 2024-05-24 | 2024-06-02 08:15:00 | 2 |
| ORD1013 | Ivy Clark | CANADA | Monitor | 2 | $180.00 | 2024-05-25 | 2024-06-02 08:15:00 | 2 |
| ORD1014 | NULL | usa | Keyboard | 3 | $45.00 | 2024-05-26 | 2024-06-02 08:15:00 | 3 |
| ORD1015 | Jack White | USA | Laptop | NULL | NULL | NULL | 2024-06-02 08:15:00 | 1 |
| ORD1016 | Karen Hall | ` Canada` | Mouse | 5 | $25.50 | 2024-05-28 | 2024-06-02 08:15:00 | 5 |
| ORD1017 | Leo King | usa | Monitor | -2 | $180.00 | 2024-05-29 | 2024-06-02 08:15:00 | 1 |
| ORD1018 | Mona Scott | USA | Headphones | 7 | $60.00 | 2024-05-30 | 2024-06-03 11:20:00 | 7 |
| ORD1019 | Nick Green | UK | Keyboard | ` 2 ` | $45.00 | 2024-05-31 | 2024-06-02 08:15:00 | 2 |
| ORD1020 | Olivia Baker | Canada | Mouse | 3 | $25.50 | 2024-06-01 | 2024-06-02 08:15:00 | 3 |

## Verification — the six test rows

```python
orders_clean.select("order_id", "quantity", "quantity_clean") \
    .filter(col("order_id").isin("ORD1011", "ORD1015", "ORD1004", "ORD1017", "ORD1019", "ORD1006")) \
    .display()
```

**Output — 6 rows:**

| order_id | quantity | quantity_clean |
| --- | --- | --- |
| ORD1004 | -5 | 1 |
| ORD1006 | 0 | 0 |
| ORD1011 | NULL | 1 |
| ORD1015 | NULL | 1 |
| ORD1017 | -2 | 1 |
| ORD1019 | ` 2 ` | 2 |

## Key Concepts Summary

| Concept | Rule / Distinction |
| --- | --- |
| `dropna` | deletes whole rows — only when the row can't be trusted and losing it is acceptable |
| `fillna` | blanket constant for every NULL — safe only if one value is right for all of them |
| `when/otherwise` | value-dependent logic — the only one that handles "negative → 1" and transforms non-nulls too |
| New column vs overwrite | `withColumn("quantity_clean", ...)` keeps the raw `quantity` for audit — don't destroy source values |
| Pipeline order | dedup first, then standardize — cleaning before dedup wastes work and can mask conflicting duplicates |
| NULL → 1 risk | fabricates demand (revenue, inventory); alternatives: keep NULL, fill 0, or flag for review |

**Lessons from the attempts:**

* Two chained `withColumn`s on the same column, where the second runs `trim()` on an already-INT column, only works via Spark's implicit cast — it fails under ANSI SQL mode. One `when(isNull() | (cast < 0), 1).otherwise(cast)` does it safely.
* Drop the window helper column (`rn`) before a table moves to the next pipeline stage.

**Pipeline so far:** `dirty_orders` (24 rows) → `orders_dedup` (20 rows, Notebook 01) → `orders_clean` (20 rows, this notebook) → next: standardize `country` / clean `unit_price`.
