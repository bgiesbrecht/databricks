# Databricks notebook source
# MAGIC %md
# MAGIC # DEMO — Oracle → Unity Catalog metadata sync (comments + annotations)
# MAGIC
# MAGIC A thin, repeatable driver that **uses the single sync engine** (`sync_oracle_metadata`). It makes a
# MAGIC source-side change in Oracle (via a direct JDBC connection — federation is read-only), runs the
# MAGIC engine, and shows the change propagate into Unity Catalog with full change-tracking.
# MAGIC
# MAGIC **Version-aware:** on **Oracle 23ai/26ai** it demos comments **and** annotations→tags; on
# MAGIC **pre-23ai** (e.g. 19c) it auto-detects the absence of annotations and demos **comments only**.
# MAGIC
# MAGIC Pick the source via `sync_name` (+ matching `secret_scope`):
# MAGIC - `sales` → 26ai (`bg-oracle-23ai`), scope `bg_oracle_23ai`  *(comments + annotations)*
# MAGIC - a pre-23ai sync (e.g. `sales_19c_compat` → `bg-oracle-01`), scope `bg_oracle_demo`  *(comments only)*

# COMMAND ----------

# MAGIC %pip install oracledb --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("sync_name", "sales", "Sync to demo (from sync_config)")
dbutils.widgets.text("secret_scope", "bg_oracle_23ai", "Secret scope with the source Oracle creds")
dbutils.widgets.text("engine", "./sync_oracle_metadata", "Sync engine notebook (relative)")
dbutils.widgets.text("metadata_schema", "bg.metadata_syn", "Control-plane schema")
SYNC = dbutils.widgets.get("sync_name"); SCOPE = dbutils.widgets.get("secret_scope")
ENGINE = dbutils.widgets.get("engine"); MS = dbutils.widgets.get("metadata_schema")

import oracledb, json

cfg = spark.sql(f"SELECT * FROM {MS}.sync_config WHERE name='{SYNC}'").collect()[0]
src_sch, tt = cfg.source_schema, cfg.target_type.upper()
def uc(obj): return spark.sql(f"SELECT {MS}.resolve_uc_name('{src_sch}','{obj}','{tt}') n").collect()[0].n
ORDERS_UC, CUSTOMERS_UC = uc("ORDERS"), uc("CUSTOMERS")

_cfg = {k: dbutils.secrets.get(SCOPE, k) for k in ("host", "port", "service", "username", "password")}
_dsn = oracledb.makedsn(_cfg["host"], int(_cfg["port"]), service_name=_cfg["service"])
def _con(): return oracledb.connect(user=_cfg["username"], password=_cfg["password"], dsn=_dsn)

def oracle_exec(stmts, ignore_errors=False):
    if isinstance(stmts, str): stmts = [stmts]
    con = _con(); cur = con.cursor()
    for s in stmts:
        try: cur.execute(s)
        except oracledb.DatabaseError:
            if not ignore_errors: raise
    con.commit(); con.close()

def set_comment(target, text):
    oracle_exec(f"COMMENT ON {target} IS '{text.replace(chr(39), chr(39)*2)}'")
def set_annotation(obj, name, value):
    oracle_exec(f"ALTER TABLE {obj} ANNOTATIONS (DROP {name})", ignore_errors=True)
    oracle_exec(f"ALTER TABLE {obj} ANNOTATIONS (ADD {name} '{value.replace(chr(39), chr(39)*2)}')")

# version / annotation-capability detection
con = _con(); cur = con.cursor()
cur.execute("SELECT banner FROM v$version WHERE ROWNUM=1"); banner = cur.fetchone()[0]
try:
    cur.execute("SELECT 1 FROM user_annotations_usage WHERE ROWNUM=1"); cur.fetchall(); HAS_ANN = True
except oracledb.DatabaseError: HAS_ANN = False
con.close()
print(f"source: {banner}")
print(f"annotations supported: {HAS_ANN}   target: {tt}  orders->{ORDERS_UC}  customers->{CUSTOMERS_UC}")

def run_engine():
    return json.loads(dbutils.notebook.run(ENGINE, 600, {"sync_name": SYNC, "apply": "true", "metadata_schema": MS}))

def split(fqn): return fqn.split(".")  # [catalog, schema, table]
def show_comment():
    cat, sch, tbl = split(ORDERS_UC)
    display(spark.sql(f"SELECT '{ORDERS_UC}' AS obj, column_name, comment FROM {cat}.information_schema.columns "
                      f"WHERE table_catalog='{cat}' AND table_schema='{sch}' AND table_name='{tbl}' AND column_name='status'"))
def show_tag():
    cat, sch, tbl = split(CUSTOMERS_UC)
    display(spark.sql(f"SELECT '{CUSTOMERS_UC}' AS obj, tag_name, tag_value FROM {cat}.information_schema.table_tags "
                      f"WHERE schema_name='{sch}' AND table_name='{tbl}' AND tag_name='OWNER_TEAM'"))

# COMMAND ----------

# MAGIC %md ## ⓪ SETUP / RESET — establish a known baseline (run before presenting)

# COMMAND ----------

set_comment("COLUMN orders.status", "Order fulfillment status: PENDING, SHIPPED, DELIVERED, CANCELLED, RETURNED")
if HAS_ANN: set_annotation("customers", "Owner_Team", "Revenue")
print("baseline set in Oracle; establishing baseline in UC ...")
print("baseline sync:", run_engine())

# COMMAND ----------

# MAGIC %md ## ① BEFORE

# COMMAND ----------

show_comment()
if HAS_ANN: show_tag()
else: print("(pre-23ai source — no annotations/tags)")

# COMMAND ----------

# MAGIC %md ## ② Source change in Oracle (direct JDBC — the read-only federation cannot do this)

# COMMAND ----------

set_comment("COLUMN orders.status", "Fulfillment status. Allowed: PENDING, SHIPPED, DELIVERED, CANCELLED, RETURNED. (updated 2026-06)")
print("changed orders.status comment in Oracle")
if HAS_ANN:
    set_annotation("customers", "Owner_Team", "Revenue Operations")
    print("changed customers Owner_Team annotation: Revenue -> Revenue Operations")

# COMMAND ----------

# MAGIC %md ## ③ Run the sync engine

# COMMAND ----------

summary = run_engine()
print(json.dumps(summary, indent=2))
RUN_ID = summary["run_id"]

# COMMAND ----------

# MAGIC %md ## ④ AFTER — propagation + audit trail

# COMMAND ----------

show_comment()
if HAS_ANN: show_tag()

# COMMAND ----------

print("=== change feed for this run ===")
display(spark.sql(f"""SELECT kind, change_type, oracle_object, oracle_column, meta_key, old_value, new_value
FROM {MS}.sync_change_feed WHERE run_id = '{RUN_ID}' ORDER BY kind, oracle_object"""))

# COMMAND ----------

if HAS_ANN:
    print("=== annotation registry (all annotations; incl. any kept registry-only) ===")
    display(spark.sql(f"""SELECT oracle_object, oracle_column, annotation_name, annotation_value, (uc_name IS NOT NULL) AS taggable
    FROM {MS}.oracle_annotations WHERE oracle_schema='{src_sch.upper()}' AND is_active
    ORDER BY oracle_object, oracle_column NULLS FIRST, annotation_name"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap
# MAGIC - Oracle changed via a **direct JDBC** write (UC federation/connection is read-only by design).
# MAGIC - The **single engine** `sync_oracle_metadata` detected the change, applied it (`CREATE OR REPLACE VIEW`
# MAGIC   for views / in-place `ALTER` for MVs — both Genie-safe), and logged it to `sync_change_feed`.
# MAGIC - **26ai:** comments + annotation→tag both propagate; governed tags (e.g. `PII`) are kept registry-only.
# MAGIC - **pre-23ai:** the same notebook auto-runs **comments only** (annotation leg no-ops) — backward compatible.
