# 04 Price Cleaning Practice — strip the $ and cast to DECIMAL

Rendered snapshot of `04 Price Cleaning Practice - strip & cast to decimal.ipynb` **with cell outputs included** (generated 2026-11-04). The `.ipynb` notebook is the source of truth — regenerate this file after updates. Values in backticks show padded whitespace; NULLs are written as NULL.

Part 4 of the interview drill on the dirty retail-orders dataset. Picks up exactly where **03 Standardization Practice** left off: `orders_std` — 20 rows with `quantity_clean` (INT) and `country_clean` (`USA` / `UK` / `CANADA`) added. The remaining dirt: `$`-prefixed price strings and a future order date.

**The question:** `unit_price` is a STRING like `'$1200.00'`. Add a NEW column `unit_price_clean` with a usable money value that supports later revenue math (`quantity * price`). A `$0.00` price must stay a distinguishable zero, and rows with no price (ORD1005, ORD1015) must not crash the cast.

## Dataset — `orders_std` (output of Notebook 03, 20 rows)

```python
orders_std = spark.createDataFrame(
    orders_std_rows,
    "order_id STRING, customer_name STRING, country STRING, product STRING, quantity STRING, unit_price STRING, order_date STRING, ingested_at STRING, quantity_clean INT, country_clean STRING",
)

display(orders_std)
```

**Output — 20 rows:**

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

## 1. Cleaning `unit_price` — notes

**Type choice — DECIMAL(12,2), not double:**

* `double` is binary floating point — values like 0.1 aren't exactly representable, so `SUM()` across many rows can drift by fractions of a cent. Fine for rough analytics, unacceptable for financial reporting.
* `DECIMAL(12,2)` is stored as an exact scaled integer — sums are exact, rounding is controlled. This is the interview answer for money.

**`decimal(p, s)` decoded** (my chat question in this session): `p` = total digits (precision), `s` = digits after the decimal point (scale). So `decimal(12,2)` = 10 digits before + 2 after → max value 9,999,999,999.99. Under the hood it's stored as an exact integer scaled by 10^-s — that's why sums don't drift. To read it aloud: "twelve total digits, two of them after the decimal point."

**The `$` belongs to the presentation layer:** store the amount as a plain number; the symbol is formatting applied by BI/reporting tools. Bronze keeps `'$1200.00'` raw; silver/gold store typed decimals.

**Multi-currency rule:** a separate `currency_code` column (ISO 4217: `USD`, `CAD`), never the symbol in the value — one symbol `$` is ambiguous across USD/CAD/AUD. Needed only once a second currency appears.

**`translate` vs `regexp_replace`:** `translate(col, '$', '')` removes exact characters — fine for one symbol. `regexp_replace(col, '[$,]', '')` handles richer production dirt like `'$1,200.00'`.

**Nulls:** `translate(NULL)` → NULL and `cast(NULL)` → NULL, so ORD1005/ORD1015 prices stay NULL. For revenue, NULL must stay NULL (unknown) — never 0 (which means known-free).

## Solution — `unit_price_clean`

```python
from pyspark.sql.functions import col, translate

orders_priced = orders_std.withColumn("unit_price_clean", translate(col("unit_price"), "$", "").cast("decimal(12,2)"))

orders_priced.display()
```

**Output — 20 rows (`orders_priced`), `unit_price_clean` as `decimal(12,2)`, shown here for the key rows:**

| order_id | unit_price | ... | unit_price_clean |
| --- | --- | --- | --- |
| ORD1001 | $1200.00 | ... | 1200.00 |
| ORD1002 | $25.50 | ... | 25.50 |
| ORD1003 | $45.00 | ... | 45.00 |
| ORD1004 | $180.00 | ... | 180.00 |
| ORD1005 | NULL | ... | NULL |
| ORD1006 | $60.00 | ... | 60.00 |
| ORD1007 | $25.50 | ... | 25.50 |
| ORD1008 | $45.00 | ... | 45.00 |
| ORD1009 | $180.00 | ... | 180.00 |
| ORD1010 | $1200.00 | ... | 1200.00 |
| ORD1011 | $25.50 | ... | 25.50 |
| ORD1012 | $0.00 | ... | 0.00 |
| ORD1013 | $180.00 | ... | 180.00 |
| ORD1014 | $45.00 | ... | 45.00 |
| ORD1015 | NULL | ... | NULL |
| ORD1016 | $25.50 | ... | 25.50 |
| ORD1017 | $180.00 | ... | 180.00 |
| ORD1018 | $60.00 | ... | 60.00 |
| ORD1019 | $45.00 | ... | 45.00 |
| ORD1020 | $25.50 | ... | 25.50 |

All other columns pass through unchanged from `orders_std` (see the dataset table above).

## Verification — numeric cast, zero, nulls, dtype

```python
orders_priced.select("order_id", "unit_price", "unit_price_clean") \
    .filter(col("order_id").isin("ORD1001", "ORD1012", "ORD1005", "ORD1015")) \
    .display()

orders_priced.printSchema()
```

**Output — 4 test rows:**

| order_id | unit_price | unit_price_clean |
| --- | --- | --- |
| ORD1001 | $1200.00 | 1200.00 |
| ORD1005 | NULL | NULL |
| ORD1012 | $0.00 | 0.00 |
| ORD1015 | NULL | NULL |

**Output — printSchema (money type confirmed):**

```text
root
 |-- order_id: string (nullable = true)
 |-- customer_name: string (nullable = true)
 |-- country: string (nullable = true)
 |-- product: string (nullable = true)
 |-- quantity: string (nullable = true)
 |-- unit_price: string (nullable = true)
 |-- order_date: string (nullable = true)
 |-- ingested_at: string (nullable = true)
 |-- quantity_clean: integer (nullable = true)
 |-- country_clean: string (nullable = true)
 |-- unit_price_clean: decimal(12,2) (nullable = true)
```

## Key Concepts Summary

| Concept | Rule / Distinction |
| --- | --- |
| DECIMAL(12,2) vs double | DECIMAL is exact (scaled integer) — the money type; double can drift on sums, only for rough analytics |
| `$` = formatting, not data | currency symbols are applied at the presentation layer; stored values are plain numbers |
| currency_code column | for multi-currency data: separate ISO-4217 code column (`USD`, `CAD`) — never the symbol in the value |
| translate vs regexp_replace | translate removes exact characters (fine for `$`); regexp_replace handles richer patterns (`$1,200.00`) |
| NULL price → NULL revenue | NULL means unknown; 0 means known-free — don't convert one into the other |
| Assign the result | `.display()` without assignment stores nothing — assign to the stage variable (`orders_priced`) |

**Lessons from my attempts:**

* `cast("double")` passes checks on small clean values but is the wrong money type — exactness matters once you SUM across rows.
* Ending the cell in `.display()` without assigning lost the result — the next stage had nothing to build on.
* `$0.00` must cast to a real zero, not null or blank — a zero price is data, not missing data.
