# Data Cleaning — Interview Notes

## 1. Deduplication

* `dropDuplicates()` / `DISTINCT` — removes duplicate rows. Survivor is **arbitrary, non-deterministic** (reruns may keep different rows). Exact duplicates only.
* **Window dedup** — one row per key, with a rule:

```python
row_number().over(
    Window.partitionBy("order_id")
          .orderBy(order_date.desc_nulls_last(), ingested_at.desc())
)

.filter(col("rn") == 1)
```

* `partitionBy(key)` — one group per key.
* `orderBy(date desc, ingested_at desc)` — newest first; `ingested_at` = **tiebreaker** when dates tie.
* `rn == 1` — the survivor.

## 2. Null handling

* `dropna` — deletes rows. Data loss → business approval first.
* `fillna` — one constant for **all** NULLs. Only if one value is correct for every NULL.
* `when/otherwise` — conditional, value-dependent (e.g. "negative → 1"). Most flexible.
* **NULL = unknown. 0 = known zero. 1 = known one.** Replacing NULL with a made-up value fabricates data (revenue, inventory).
* `SUM` / `AVG` ignore NULLs automatically — keeping NULL is often the safest choice.

## 3. Standardization

* `upper(trim(...))` → **keys, joins, GROUP BY** (silver layer).
* `initcap(...)` → **display only**. Mangles abbreviations: `UK` → `Uk`.
* **Standardize BEFORE aggregating** — dirty keys (`usa` + `USA`) split one country's revenue into fragments. **Silently.**
* String functions fix **mechanical** variation only — casing, whitespace.
* Mapping/dimension table needed for **semantic** variation — `"U.S."`, `"United States"`; also many-to-one mappings, updates without code deploys, audit trail.
* Rule: **string functions for formatting, mapping tables for meaning.**

## 4. Money cleaning

* Strip symbol: `translate(col, "$", "")` (exact characters) or `regexp_replace(col, "[$,]", "")` (handles `$1,200.00`).
* **`decimal(p, s)`** — p = total digits, s = digits after the point.
  * `decimal(12,2)` = 10 digits before + 2 after → max 9,999,999,999.99. Exact sums.
* `double` — floating point; `SUM()` drifts fractions of a cent. **Never for money.**
* `$` symbol = **presentation layer** (BI tools format it). Never stored in the value.
* Multi-currency → separate `currency_code` column (ISO 4217: `USD`, `CAD`). `$` alone is ambiguous.
* NULL price stays **NULL** (unknown ≠ free).

## 5. Pipeline order

`dedup → nulls → standardize → cast types → dates → aggregate`

* Dedup first → no wasted cleaning of doomed rows; transforms can mask duplicate conflicts.
* Standardize before GROUP BY → dirty keys split revenue silently.
* Cast before math → `'$1200.00'` strings can't be multiplied.

## 6. Always

* Keep the raw column, add `*_clean` (`quantity_clean`, `country_clean`, `unit_price_clean`) — audit trail.
* Assign every result to a variable. `.display()` is **not** an assignment.
* Valid values pass through unchanged — fix only what's broken.
* One stage, one job.
* Silent wrong numbers are worse than loud errors.

## 7. Syntax traps

* Dot before `.over()`; `orderBy` chains on the **Window**, not on `col()`.
* Comparison goes **inside** `filter()`: `filter(col("rn") == 1)`.
* `col()` required for expressions (`.desc()`, `.desc_nulls_last()`, casts) — plain strings only for plain column names.
* No `import *` — import exactly what is used.

## 8. Quick Q&A

* **dropDuplicates vs window dedup?** Arbitrary survivor vs deterministic (rule-based) survivor.
* **Why dedup before cleaning?** Wasted work otherwise; cleaning can mask duplicate conflicts.
* **dropna vs fillna vs when?** Row deletion vs one constant for all NULLs vs conditional rules.
* **Why not fillna(0) for missing price?** 0 = known free; NULL = unknown. Different facts.
* **Why standardize before aggregation?** Dirty keys split groups silently.
* **Mapping table vs string function?** Semantic variation vs mechanical variation.
* **Why DECIMAL not double?** Exact sums; floating point drifts.
* **What is decimal(12,2)?** Twelve total digits, two after the point.
