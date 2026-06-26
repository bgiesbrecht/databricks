# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup: Oracle → UC metadata sync
# MAGIC Idempotent. Creates the `bg.metadata_syn` control plane (tables, resolver, default mapping)
# MAGIC and loads `config/sync_config.yaml` into `sync_config` + `annotation_promotion_policy`.
# MAGIC Run once, and re-run whenever the YAML changes.

# COMMAND ----------

# MAGIC %pip install pyyaml --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("yaml_path",
    "/Workspace/Users/brice.giesbrecht@databricks.com/oracle_uc_sync/config/sync_config.yaml",
    "Path to sync_config.yaml")
dbutils.widgets.text("metadata_schema", "bg.metadata_syn", "Control-plane schema")
YAML_PATH = dbutils.widgets.get("yaml_path")
MS = dbutils.widgets.get("metadata_schema")

# COMMAND ----------

# MAGIC %md ## 1. Control-plane DDL (idempotent)

# COMMAND ----------

cat = MS.split(".")[0]
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {MS} COMMENT 'Oracle->UC metadata sync control plane'")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {MS}.oracle_to_uc_mapping (
  oracle_schema STRING NOT NULL, oracle_object STRING, target_catalog STRING NOT NULL,
  target_schema STRING NOT NULL, target_object STRING, name_prefix STRING, name_suffix STRING,
  case_mode STRING, target_type STRING, notes STRING, updated_at TIMESTAMP
) COMMENT 'Oracle schema/object -> UC object mapping (schema default + per-object overrides).'""")

spark.sql(f"""CREATE OR REPLACE FUNCTION {MS}.resolve_uc_name(p_oracle_schema STRING, p_oracle_object STRING, p_target_type STRING)
RETURNS STRING
COMMENT 'Resolve Oracle (schema, object) to a UC name for a target_type. Exact override > schema default.'
RETURN (
  SELECT min_by(candidate, priority) FROM (
    SELECT
      CASE
        WHEN upper(oracle_object) = upper(p_oracle_object) AND target_object IS NOT NULL
          THEN target_catalog||'.'||target_schema||'.'||target_object
        WHEN oracle_object IS NULL
          THEN target_catalog||'.'||target_schema||'.'||coalesce(name_prefix,'')||
               CASE coalesce(case_mode,'lower') WHEN 'lower' THEN lower(p_oracle_object)
                    WHEN 'upper' THEN upper(p_oracle_object) ELSE p_oracle_object END||coalesce(name_suffix,'')
      END AS candidate,
      CASE WHEN upper(oracle_object)=upper(p_oracle_object) AND target_object IS NOT NULL THEN 1
           WHEN oracle_object IS NULL THEN 2 END AS priority
    FROM {MS}.oracle_to_uc_mapping
    WHERE upper(oracle_schema)=upper(p_oracle_schema)
      AND upper(coalesce(target_type,'VIEW'))=upper(p_target_type)
  ) WHERE candidate IS NOT NULL)""")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {MS}.sync_config (
  name STRING NOT NULL, source_connection STRING, source_catalog STRING, source_schema STRING,
  metadata_catalog STRING, metadata_schema STRING,
  target_catalog STRING, target_type STRING, sync_comments BOOLEAN, sync_annotations BOOLEAN,
  apply_annotations_to_objects BOOLEAN, object_include ARRAY<STRING>, object_exclude ARRAY<STRING>,
  on_change_notebook STRING, genie_space_id STRING, metadata_only BOOLEAN, enabled BOOLEAN, updated_at TIMESTAMP
) COMMENT 'One row per sync job; loaded from sync_config.yaml. source_*=data (federated app schema); metadata_*=where the v_metadata helper view lives.'""")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {MS}.annotation_promotion_policy (
  annotation_name STRING NOT NULL, route STRING, uc_tag_key STRING, value_mode STRING,
  scope STRING, notes STRING, updated_at TIMESTAMP
) COMMENT 'Which Oracle annotations promote beyond the registry. Default route REGISTRY.'""")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {MS}.oracle_annotations (
  sync_name STRING, oracle_schema STRING, oracle_object STRING, oracle_column STRING, level STRING,
  object_type STRING, annotation_name STRING, annotation_value STRING, uc_name STRING, is_active BOOLEAN,
  first_seen_at TIMESTAMP, last_changed_at TIMESTAMP, last_synced_at TIMESTAMP
) COMMENT 'Full registry of ALL Oracle annotations regardless of promotion.'""")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {MS}.metadata_state (
  sync_name STRING, kind STRING NOT NULL, target_type STRING, oracle_schema STRING, oracle_object STRING,
  oracle_column STRING, level STRING, uc_name STRING, meta_key STRING, value STRING, is_active BOOLEAN,
  first_seen_at TIMESTAMP, last_changed_at TIMESTAMP, last_synced_at TIMESTAMP
) COMMENT 'Unified current state (kind=COMMENT|TAG).'""")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {MS}.metadata_change_log (
  run_id STRING, changed_at TIMESTAMP, sync_name STRING, kind STRING, target_type STRING,
  oracle_schema STRING, oracle_object STRING, oracle_column STRING, level STRING, meta_key STRING,
  change_type STRING, old_value STRING, new_value STRING
) COMMENT 'Append-only unified audit log.'""")

spark.sql(f"""CREATE OR REPLACE VIEW {MS}.sync_change_feed AS
SELECT run_id, changed_at, sync_name, kind, change_type, target_type,
       oracle_schema, oracle_object, oracle_column, meta_key, old_value, new_value
FROM {MS}.metadata_change_log ORDER BY changed_at DESC""")
print("control-plane DDL ensured")

# COMMAND ----------

# MAGIC %md ## 2. (Schema-default object mapping is built from the YAML in section 3 — config-driven.)
# MAGIC Each sync maps its Oracle `source.schema` → `target.catalog`.`target.schema` (per `target.type`);
# MAGIC objects land at `<lower(name)>` there. Per-object `objects.overrides` take precedence.

# COMMAND ----------

# MAGIC %md ## 3. Load YAML -> sync_config + annotation_promotion_policy

# COMMAND ----------

import yaml
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, BooleanType, ArrayType)

with open(YAML_PATH) as f:
    cfg = yaml.safe_load(f)

# Schema-default mappings (config-driven): each sync's Oracle source_schema -> target.catalog.target.schema
# for its target_type; objects land at <lower(name)>. target.schema defaults to the source schema name.
# Per-object overrides (loaded further below) take precedence. Declarative: edit YAML + re-run setup to update.
_seeded = set()
for s in cfg.get("syncs", []):
    osch = (s.get("source", {}).get("schema") or "").upper()
    tg = s.get("target", {}) or {}
    tcat = tg.get("catalog"); tsch = (tg.get("schema") or s.get("source", {}).get("schema") or "").lower()
    ttype = (tg.get("type") or "VIEW").upper()
    if not (osch and tcat and tsch) or (osch, ttype) in _seeded:
        continue
    _seeded.add((osch, ttype))
    spark.sql(f"""MERGE INTO {MS}.oracle_to_uc_mapping t
      USING (SELECT '{osch}' oracle_schema, CAST(NULL AS STRING) oracle_object, '{ttype}' target_type) s
      ON upper(t.oracle_schema)=s.oracle_schema AND t.oracle_object IS NULL
         AND upper(coalesce(t.target_type,'VIEW'))=s.target_type
      WHEN MATCHED THEN UPDATE SET target_catalog='{tcat}', target_schema='{tsch}', case_mode='lower', updated_at=current_timestamp()
      WHEN NOT MATCHED THEN INSERT (oracle_schema, oracle_object, target_catalog, target_schema, case_mode, target_type, notes, updated_at)
        VALUES ('{osch}', NULL, '{tcat}', '{tsch}', 'lower', '{ttype}', 'schema default {osch}.* -> {tcat}.{tsch}', current_timestamp())""")
print(f"schema-default mappings ensured: {sorted(_seeded)}")

sync_rows = []
for s in cfg.get("syncs", []):
    src, meta, tgt, ann, objs, hooks = (s.get("source", {}), s.get("metadata", {}), s.get("target", {}),
        s.get("annotations", {}), s.get("objects", {}), s.get("hooks", {}))
    sync_rows.append((s["name"], src.get("connection"), src.get("catalog"), src.get("schema"),
        meta.get("catalog") or src.get("catalog"), meta.get("schema") or src.get("schema"),
        tgt.get("catalog"), (tgt.get("type") or "VIEW"), bool(s.get("comments", True)),   # type optional → VIEW
        bool(ann.get("enabled", False)), bool(ann.get("apply_to_objects", False)),
        list(objs.get("include", []) or []), list(objs.get("exclude", []) or []),
        hooks.get("on_change_notebook", "") or "", hooks.get("genie_space_id", "") or "",
        bool(s.get("metadata_only", True)), bool(s.get("enabled", True))))   # default: metadata-only (decorate existing)

sync_schema = StructType([
    StructField("name", StringType()), StructField("source_connection", StringType()),
    StructField("source_catalog", StringType()), StructField("source_schema", StringType()),
    StructField("metadata_catalog", StringType()), StructField("metadata_schema", StringType()),
    StructField("target_catalog", StringType()), StructField("target_type", StringType()),
    StructField("sync_comments", BooleanType()), StructField("sync_annotations", BooleanType()),
    StructField("apply_annotations_to_objects", BooleanType()),
    StructField("object_include", ArrayType(StringType())), StructField("object_exclude", ArrayType(StringType())),
    StructField("on_change_notebook", StringType()), StructField("genie_space_id", StringType()),
    StructField("metadata_only", BooleanType()), StructField("enabled", BooleanType())])
(spark.createDataFrame(sync_rows, sync_schema).withColumn("updated_at", F.current_timestamp())
   .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{MS}.sync_config"))

pol_rows = [(p["name"], p.get("route", "REGISTRY").upper(), p.get("uc_tag_key"),
             p.get("value_mode", "asis"), p.get("scope", "BOTH"), p.get("notes"))
            for p in cfg.get("annotation_promotion", [])]
pol_schema = StructType([StructField("annotation_name", StringType()), StructField("route", StringType()),
    StructField("uc_tag_key", StringType()), StructField("value_mode", StringType()),
    StructField("scope", StringType()), StructField("notes", StringType())])
(spark.createDataFrame(pol_rows, pol_schema).withColumn("updated_at", F.current_timestamp())
   .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{MS}.annotation_promotion_policy"))

# per-object overrides from YAML -> oracle_to_uc_mapping (declarative: replace prior YAML overrides).
# oracle_schema + target_type are inferred from the sync; you only declare the object + where it goes.
ov_rows = []
for s in cfg.get("syncs", []):
    osch = (s.get("source", {}).get("schema") or "").upper()
    ottype = (s.get("target", {}).get("type") or "VIEW").upper()
    for o in ((s.get("objects", {}) or {}).get("overrides", []) or []):
        ov_rows.append((osch, o["oracle_object"].upper(), o.get("target_catalog"), o.get("target_schema"),
                        o.get("target_object"), o.get("name_prefix"), o.get("name_suffix"),
                        o.get("case_mode", "lower"), ottype, "yaml-override"))
spark.sql(f"DELETE FROM {MS}.oracle_to_uc_mapping WHERE oracle_object IS NOT NULL AND notes='yaml-override'")
if ov_rows:
    ov_schema = StructType([StructField(n, StringType()) for n in
        ["oracle_schema","oracle_object","target_catalog","target_schema","target_object",
         "name_prefix","name_suffix","case_mode","target_type","notes"]])
    (spark.createDataFrame(ov_rows, ov_schema).withColumn("updated_at", F.current_timestamp())
        .write.mode("append").saveAsTable(f"{MS}.oracle_to_uc_mapping"))

print(f"loaded {len(sync_rows)} syncs, {len(pol_rows)} promotion rules, {len(ov_rows)} object overrides")

# COMMAND ----------

display(spark.sql(f"SELECT name, source_connection, target_type, sync_comments, sync_annotations, apply_annotations_to_objects, enabled FROM {MS}.sync_config"))
display(spark.sql(f"SELECT annotation_name, route, scope FROM {MS}.annotation_promotion_policy ORDER BY route, annotation_name"))
dbutils.notebook.exit("setup-ok")
