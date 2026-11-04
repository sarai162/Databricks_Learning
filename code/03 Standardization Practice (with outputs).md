# 03 Standardization Practice — trim & upper (country)

Rendered snapshot of `03 Standardization Practice - trim & upper (country).ipynb` **with cell outputs included** (generated 2026-11-04). The `.ipynb` notebook is the source of truth — regenerate this file after updates. Values in backticks show padded whitespace; NULLs are written as NULL.

Part 3 of the interview drill on the dirty retail-orders dataset. Picks up exactly where **02 Null Handling Practice** left off: `orders_clean` — 20 deduplicated rows with `quantity_clean` (INT) added. The remaining dirt: `$`-prefixed prices, mixed country casing, and a future order date.

**The question:** `country` is a STRING containing `' canada'`, `'USA'`, `'usa'`, `' USA '`, `'CANADA'`, `' Canada'`. Add a NEW column `country_clean` (don't overwrite `country`) so every country converges to one consistent form — exactly 3 distinct values expected: `USA`, `UK`, `CANADA`.

## Dataset — `orders_clean` (output of Notebook 02, 20 rows)

```python
orders_clean = spark.createDataFrame(
    orders_clean_rows,
    "order_id STRING, customer_name STRING, country STRING, product STRING, quantity STRING, unit_price STRING, order_date STRING, ingested_at STRING, quantity_clean INT",
)

display(orders_clean)
```

**Output — 20 rows:**

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

## 1. Country standardization — notes

**Which form to standardize to — and why:**

* `upper(trim(...))` — the standard for **join keys and matching**. Silver/gold layers standardize to a canonical match form: `usa` and `USA` must become the same entity before any GROUP BY or join.
* `initcap` — for the **presentation layer** (reports, dashboards humans read), never for keys. It also mangles abbreviations: `'UK'` → `'Uk'`, `'usa'` → `'Usa'`.

**Why standardize BEFORE aggregating revenue per country:** `GROUP BY country` on dirty strings puts `usa` and `USA` in separate groups — one country's revenue splits into fragments, and it fails **silently**: no error, just wrong numbers in the report.

**When a mapping/dimension table beats string functions:** string functions fix only **mechanical** variation (casing, whitespace). A mapping/dim table wins when variation is **semantic** — `"U.S."`, `"U.S.A."`, `"United States"` — no string function maps those together. Also: many-to-one mappings, business can change mappings without code deploys, governance/audit (who changed what, when). Rule of thumb: **string functions for formatting, mapping tables for meaning**.

## Solution — `country_clean`

```python
from pyspark.sql.functions import col, trim, upper

orders_std = orders_clean.withColumn("country_clean", upper(trim(col("country"))))

orders_std.display()
```

**Output — 20 rows (`orders_std`), raw `country` kept, `country_clean` added:**

| order_id | customer_name | country | product | quantity | unit_price | order_date | ingested_at | quantity_clean | country_clean |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ORD1001 | John Smith | USA | Laptop | 2 | $1200.00 | 2024-05-13 | 2024-06-02 08:15:00 | 2 | USA |
| ORD1002 | ` jane doe ` | ` canada` | Mouse | 5 | $25.50 | 2024-05-14 | 2024-06-02 08:15:00 | 5 | CANADA |
| ORD1003 | NULL | USA | Keyboard | 1 | $45.00 | 2024-05-15 | 2024-06-02 08:15:00 | 1 | USA |
| ORD1004 | Alice Johnson | UK | Monitor | -5 | $180.00 | 2024-05-16 | 2024-06-02 08:15:00 | 1 | UK |
| ORD1005 | bob brown | usa | Laptop | 3 | NULL | 2024-05-17 | 2024-06-02 08:15:00 | 3 | USA |
| ORD1006 | Charlie Davis | Canada | Headphones | 0 | $60.00 | 2024-05-18 | 2024-06-02 08:15:00 | 0 | CANADA |
| ORD1007 | NULL | USA | Mouse | 9 | $25.50 | 2024-05-22 | 2024-06-03 10:05:00 | 9 | USA |
| ORD1008 | David Lee | ` USA ` | Keyboard | 1 | $45.00 | 2024-05-20 | 2024-06-02 08:15:00 | 1 | USA |
| ORD1009 | Eve Wilson | usa | Monitor | 2 | $180.00 | 2024-06-10 | 2024-06-03 09:40:00 | 2 | USA |
| ORD1010 | Frank Miller | Canada | Laptop | 1 | $1200.00 | 2050-01-01 | 2024-06-02 08:15:00 | 1 | CANADA |
| ORD1011 | ` grace taylor ` | USA | Mouse | NULL | $25.50 | 2024-05-23 | 2024-06-02 08:15:00 | 1 | USA |
| ORD1012 | Henry Adams | UK | Headphones | 2 | $0.00 | 2024-05-24 | 2024-06-02 08:15:00 | 2 | UK |
| ORD1013 | Ivy Clark | CANADA | Monitor | 2 | $180.00 | 2024-05-25 | 2024-06-02 08:15:00 | 2 | CANADA |
| ORD1014 | NULL | usa | Keyboard | 3 | $45.00 | 2024-05-26 | 2024-06-02 08:15:00 | 3 | USA |
| ORD1015 | Jack White | USA | Laptop | NULL | NULL | NULL | 2024-06-02 08:15:00 | 1 | USA |
| ORD1016 | Karen Hall | ` Canada` | Mouse | 5 | $25.50 | 2024-05-28 | 2024-06-02 08:15:00 | 5 | CANADA |
| ORD1017 | Leo King | usa | Monitor | -2 | $180.00 | 2024-05-29 | 2024-06-02 08:15:00 | 1 | USA |
| ORD1018 | Mona Scott | USA | Headphones | 7 | $60.00 | 2024-05-30 | 2024-06-03 11:20:00 | 7 | USA |
| ORD1019 | Nick Green | UK | Keyboard | ` 2 ` | $45.00 | 2024-05-31 | 2024-06-02 08:15:00 | 2 | UK |
| ORD1020 | Olivia Baker | Canada | Mouse | 3 | $25.50 | 2024-06-01 | 2024-06-02 08:15:00 | 3 | CANADA |

## Verification — the five test rows + distinct values

```python
orders_std.select("order_id", "country", "country_clean") \
    .filter(col("order_id").isin("ORD1002", "ORD1005", "ORD1008", "ORD1013", "ORD1016")) \
    .display()

display(orders_std.select("country_clean").distinct())
```

**Output — 5 test rows, all converged:**

| order_id | country | country_clean |
| --- | --- | --- |
| ORD1002 | ` canada` | CANADA |
| ORD1005 | usa | USA |
| ORD1008 | ` USA ` | USA |
| ORD1013 | CANADA | CANADA |
| ORD1016 | ` Canada` | CANADA |

**Output — distinct `country_clean`, exactly 3 values:**

| country_clean |
| --- |
| USA |
| CANADA |
| UK |

## Key Concepts Summary

| Concept | Rule / Distinction |
| --- | --- |
| `trim` + `upper` | mechanical standardization — fixes casing and whitespace only |
| UPPER vs initcap | UPPER for join keys / matching (silver layer); initcap for presentation only; initcap mangles `'UK'` → `'Uk'` |
| New column vs overwrite | `country_clean` added, raw `country` kept for audit — same pattern as `quantity_clean` |
| Standardize before aggregate | dirty strings split one country into multiple GROUP BY groups — silent wrong revenue |
| Mapping/dim table | beats string functions when variation is semantic (`"U.S."`, `"United States"`), many-to-one, or changes without code deploys |
| Assign the result | `.display()` alone stores nothing — assign to a stage variable (`orders_std`) so the next stage can build on it |

**Lessons from my attempts:**

* `initcap` also converges the variants but produces `Usa` / `Uk` / `Canada` — fails the acceptance form (USA, UK, CANADA) and mangles abbreviations.
* First attempt ended in `.display()` without assigning — the kernel kept no `country_clean`, so the next stage had nothing to build on.
* `UPPER` was the right choice; the missing interview point was justifying **why** (join keys / canonical match form), not only naming the function.
