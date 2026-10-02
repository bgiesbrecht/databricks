# Databricks notebook source
# MAGIC %md
# MAGIC # Column Statistics Profiler — privacy-safe
# MAGIC
# MAGIC Gathers **only aggregate statistics and metadata** per column so a *similar-shaped*
# MAGIC synthetic dataset can be generated elsewhere. **No actual data values ever leave this
# MAGIC notebook.**
# MAGIC
# MAGIC **What it collects (safe to share):**
# MAGIC - column name & data type (schema metadata)
# MAGIC - row count, and per column: **null fraction**
# MAGIC - **approximate distinct count** (HyperLogLog cardinality — a *count*, not the values)
# MAGIC - for string columns: **average / max character length** (lengths, not content)
# MAGIC
# MAGIC **What it deliberately does NOT collect (no leakage):**
# MAGIC - no actual values, no samples, no `display`/`show` of rows, no `collect()` of data
# MAGIC - no min/max/mean of numeric *values*, no histograms, no top-value frequencies
# MAGIC
# MAGIC The only output is a small stats table (a few hundred rows of aggregates) that is safe
# MAGIC to export and send back.

# COMMAND ----------

# Point this at your tables (runs in YOUR environment)
dbutils.widgets.text("catalog", "", "Catalog")
dbutils.widgets.text("schema", "", "Schema")
dbutils.widgets.text("tables", "", "Tables (comma-separated)")
dbutils.widgets.text("sample_percent", "100", "Sample % (lower = cheaper, est. only)")
dbutils.widgets.text("output_table", "", "Output stats table (optional, e.g. mycat.myschema.column_profile)")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA  = dbutils.widgets.get("schema").strip()
TABLES  = [t.strip() for t in dbutils.widgets.get("tables").split(",") if t.strip()]
SAMPLE  = float(dbutils.widgets.get("sample_percent") or "100")
OUT     = dbutils.widgets.get("output_table").strip()

# COMMAND ----------

from pyspark.sql import functions as F

def profile_table(fqtn):
    """One scan; aggregate-only. Returns a list of per-column stat dicts (no values)."""
    df = spark.table(fqtn)
    if SAMPLE < 100:
        df = df.sample(fraction=SAMPLE/100.0, seed=42)   # still aggregates only
    cols = df.dtypes  # [(name, type), ...] -- metadata

    aggs = [F.count(F.lit(1)).alias("__rows")]
    for name, dtype in cols:
        c = F.col("`" + name + "`")
        aggs.append(F.count(c).alias(name + "::nonnull"))              # non-null count
        aggs.append(F.approx_count_distinct(c).alias(name + "::ndv"))  # HLL cardinality
        if dtype == "string":
            aggs.append(F.avg(F.length(c)).alias(name + "::avglen"))   # length, not content
            aggs.append(F.max(F.length(c)).alias(name + "::maxlen"))

    r = df.agg(*aggs).collect()[0].asDict()                            # aggregates only
    rows_total = r["__rows"] or 0
    out = []
    for name, dtype in cols:
        nonnull = r.get(name + "::nonnull") or 0
        ndv     = r.get(name + "::ndv")
        out.append({
            "table_name":        fqtn,
            "column_name":       name,
            "data_type":         dtype,
            "row_count":         int(rows_total),
            "null_fraction":     round(1 - (nonnull / rows_total), 6) if rows_total else None,
            "approx_distinct":   int(ndv) if ndv is not None else None,
            "distinct_pct_of_nonnull": round((ndv / nonnull), 6) if (ndv and nonnull) else None,
            "avg_char_len":      round(r[name + "::avglen"], 2) if (name + "::avglen") in r and r[name + "::avglen"] is not None else None,
            "max_char_len":      int(r[name + "::maxlen"]) if (name + "::maxlen") in r and r[name + "::maxlen"] is not None else None,
        })
    return out

# COMMAND ----------

stats = []
for t in TABLES:
    fqtn = ".".join([p for p in [CATALOG, SCHEMA, t] if p])
    print("profiling", fqtn, "...")
    stats += profile_table(fqtn)

profile_df = spark.createDataFrame(stats)
print("collected stats for", profile_df.count(), "columns across", len(TABLES), "tables")

# COMMAND ----------

# Review the aggregates (this is the ONLY thing produced -- no raw data).
display(profile_df.orderBy("table_name", "column_name"))

# COMMAND ----------

# Optional: persist the stats table to share. It contains only aggregates/metadata.
if OUT:
    profile_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(OUT)
    print("wrote stats to", OUT, "-- safe to export/share (no data values).")
else:
    print("No output_table set. You can export the displayed table above to CSV to share.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sharing
# MAGIC Export the `column_profile` table (or the displayed grid) to CSV and send it back.
# MAGIC It contains **only**: column name, type, row count, null fraction, approximate distinct
# MAGIC count, and string lengths. That is sufficient to generate a dataset with the same
# MAGIC **sparsity and cardinality** (hence the same compression/size behavior) **without any
# MAGIC of your data**.