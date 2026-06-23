# Databricks notebook source
# MAGIC %md
# MAGIC # Post-sync hook: push Oracle-derived config into a Genie space
# MAGIC Fired by the sync engine (`on_change_notebook`) after a run that changed something. It is handed
# MAGIC `run_id` + `sync_name`, looks up the sync's `genie_space_id`, and reconstructs the room's **joins**
# MAGIC from `RELATED_TO` annotations in the registry (`bg.metadata_syn.oracle_annotations`).
# MAGIC
# MAGIC **Two halves of "update the room":**
# MAGIC - **Comments (automatic):** the sync already wrote table/column comments onto the UC objects, and Genie
# MAGIC   reads UC comments directly — nothing to push here. We just log it.
# MAGIC - **Joins (explicit):** Oracle annotations carry the join criteria. We parse them and `apply_config`
# MAGIC   the `joins` section (idempotent full-replace of that section; text/examples/sample_questions untouched).
# MAGIC
# MAGIC ### RELATED_TO annotation convention
# MAGIC Put a **column-level** annotation named `RELATED_TO` on the foreign-key column. Value:
# MAGIC ```
# MAGIC <right_table>.<right_col>[;rt=<MANY_TO_ONE|ONE_TO_MANY|ONE_TO_ONE|MANY_TO_MANY>]
# MAGIC ```
# MAGIC The annotated table/column is the **left** side; the value names the **right** side. Default
# MAGIC relationship is `MANY_TO_ONE`. Example on `ORDERS.CUSTOMER_ID`: `customers.customer_id;rt=MANY_TO_ONE`.
# MAGIC `RELATED_TO` is a neutral relationship annotation — Oracle code can use it too; it is not Genie-specific.

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Run id (from the sync)")
dbutils.widgets.text("sync_name", "sales", "Sync name (from sync_config)")
dbutils.widgets.text("metadata_schema", "bg.metadata_syn", "Control-plane schema")
RUN_ID = dbutils.widgets.get("run_id")
SYNC = dbutils.widgets.get("sync_name")
MS = dbutils.widgets.get("metadata_schema")

import json

# The vendored SDK (update_genie_space.py) sits beside this notebook; Databricks puts the notebook's own
# directory on sys.path, so a sibling import works in both bundle and workspace-files deployments.
from update_genie_space import GenieSpaces

# COMMAND ----------

cfg = spark.sql(f"SELECT * FROM {MS}.sync_config WHERE name='{SYNC}'").collect()
assert cfg, f"no sync_config row named '{SYNC}'"
c = cfg[0]
space_id = (c.genie_space_id or "").strip()
if not space_id:
    print(f"sync '{SYNC}' has no genie_space_id — nothing to push.")
    dbutils.notebook.exit(json.dumps({"sync": SYNC, "genie": "skipped (no space id)"}))

src_schema = c.source_schema
tt = c.target_type.upper()
resolver = f"{MS}.resolve_uc_name"
print(f"genie_push sync={SYNC} space_id={space_id} target_type={tt} run_id={RUN_ID}")

def uc_of(obj):
    return spark.sql(f"SELECT {resolver}('{src_schema}','{obj}','{tt}') n").collect()[0].n

# COMMAND ----------

# MAGIC %md ## 1. Comments leg — already applied to the UC objects by the sync; Genie reads them directly.

# COMMAND ----------

ncmt = spark.sql(f"""
  SELECT count(*) n FROM {MS}.metadata_state
  WHERE sync_name='{SYNC}' AND target_type='{tt}' AND kind='COMMENT' AND is_active
""").collect()[0].n
print(f"comments live on the UC objects for this sync: {ncmt} (Genie reads UC comments automatically — no push needed)")

# COMMAND ----------

# MAGIC %md ## 2. Joins leg — rebuild join_specs from RELATED_TO annotations.

# COMMAND ----------

REL_PREFIX = "FROM_RELATIONSHIP_TYPE_"
VALID_REL = {"MANY_TO_ONE", "ONE_TO_MANY", "ONE_TO_ONE", "MANY_TO_MANY"}

def parse_related_to(value):
    """'customers.customer_id;rt=MANY_TO_ONE' -> (right_table, right_col, RELATIONSHIP)."""
    parts = [p.strip() for p in (value or "").split(";") if p.strip()]
    if not parts:
        return None
    target = parts[0]
    rel = "MANY_TO_ONE"
    for p in parts[1:]:
        if p.lower().startswith("rt="):
            cand = p.split("=", 1)[1].strip().upper()
            if cand in VALID_REL:
                rel = cand
    if "." not in target:
        return None
    rtbl, rcol = target.rsplit(".", 1)
    return rtbl.strip(), rcol.strip(), REL_PREFIX + rel

ann = spark.sql(f"""
  SELECT oracle_object, oracle_column, annotation_value
  FROM {MS}.oracle_annotations
  WHERE sync_name='{SYNC}' AND is_active
    AND upper(annotation_name)='RELATED_TO' AND oracle_column IS NOT NULL
""").collect()

joins, skipped = [], []
for r in ann:
    parsed = parse_related_to(r.annotation_value)
    if not parsed:
        skipped.append((r.oracle_object, r.oracle_column, r.annotation_value, "unparseable"))
        continue
    rtbl, rcol, rel = parsed
    left_fqn, right_fqn = uc_of(r.oracle_object), uc_of(rtbl)
    if not left_fqn or not right_fqn:
        skipped.append((r.oracle_object, r.oracle_column, r.annotation_value, "object not in mapping"))
        continue
    lalias, ralias = r.oracle_object.lower(), rtbl.lower()
    if lalias == ralias:                       # self-join: disambiguate the right alias
        ralias = ralias + "_2"
    lcol, rcol = r.oracle_column.lower(), rcol.lower()
    joins.append({
        "left":  {"table": left_fqn,  "alias": lalias},
        "right": {"table": right_fqn, "alias": ralias},
        "on":    f"`{lalias}`.`{lcol}` = `{ralias}`.`{rcol}`",
        "relationship_type": rel,
        "instruction": f"Join {left_fqn} to {right_fqn} (from Oracle RELATED_TO on {r.oracle_object}.{r.oracle_column}).",
    })

print(f"derived {len(joins)} join(s) from RELATED_TO annotations:")
for j in joins:
    print(f"  {j['on']}  [{j['relationship_type']}]")
for s in skipped:
    print(f"  SKIP {s[0]}.{s[1]} = {s[2]!r} ({s[3]})")

# COMMAND ----------

# MAGIC %md ## 3. Push to the Genie space (idempotent — replaces just the joins section).

# COMMAND ----------

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
host = ctx.apiUrl().get()
token = ctx.apiToken().get()
client = GenieSpaces(host, token)

# apply_config replaces only the sections present in the dict; omitted sections (text_instruction,
# examples, sample_questions) are left exactly as a human authored them.
client.apply_config(space_id, {"joins": joins})

summary = {"run_id": RUN_ID, "sync": SYNC, "space_id": space_id,
           "joins_pushed": len(joins), "joins_skipped": len(skipped),
           "comments_live": ncmt}
print(json.dumps(summary, indent=2))
dbutils.notebook.exit(json.dumps(summary))
