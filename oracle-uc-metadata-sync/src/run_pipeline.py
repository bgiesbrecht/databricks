# Databricks notebook source
# MAGIC %md
# MAGIC # Oracle → UC metadata sync — one-notebook runner
# MAGIC Runs the whole pipeline from props: optionally (re)load the config, then sync one or all syncs
# MAGIC (preview or apply). Use it as a **single job task**, or as a template for a parameterized job.
# MAGIC It just orchestrates the other notebooks (`00_setup`, `sync_oracle_metadata`) — no logic is duplicated.
# MAGIC
# MAGIC | Prop (widget) | Meaning |
# MAGIC |---|---|
# MAGIC | `setup` | `true` = (re)load the YAML into the control plane first. Do this after any config edit. |
# MAGIC | `sync_name` | a sync's `name`, or `ALL` to run every enabled sync. |
# MAGIC | `apply` | `false` = preview (writes nothing); `true` = apply. |
# MAGIC | `yaml_path` | config path for `setup`. Blank = auto-find `config/sync_config.yaml` next to this notebook. |
# MAGIC | `metadata_schema` | control-plane schema (default `bg.metadata_syn`). |

# COMMAND ----------

dbutils.widgets.dropdown("setup", "false", ["true", "false"], "Load config first?")
dbutils.widgets.text("sync_name", "ALL", "Sync name (or ALL)")
dbutils.widgets.dropdown("apply", "false", ["true", "false"], "Apply changes?")
dbutils.widgets.text("yaml_path", "", "Config path (blank = auto-find)")
dbutils.widgets.text("metadata_schema", "bg.metadata_syn", "Control-plane schema")

SETUP = dbutils.widgets.get("setup") == "true"
SYNC  = dbutils.widgets.get("sync_name").strip()
APPLY = dbutils.widgets.get("apply")
YAML  = dbutils.widgets.get("yaml_path").strip()
MS    = dbutils.widgets.get("metadata_schema")

import os, json
print(f"setup={SETUP} sync_name={SYNC!r} apply={APPLY} metadata_schema={MS}")

# COMMAND ----------

# MAGIC %md ## 1. (Optional) load the config into the control plane

# COMMAND ----------

if SETUP:
    # Resolve the config path if not given: look for config/ next to this notebook (workspace layout) or one
    # level up (bundle src/ layout). Pass an absolute path so 00_setup's open() works regardless of its CWD.
    if not YAML:
        for cand in ("config/sync_config.yaml", "../config/sync_config.yaml"):
            if os.path.exists(cand):
                YAML = os.path.abspath(cand); break
        assert YAML, "could not auto-find config/sync_config.yaml — set the yaml_path prop"
    print("loading config from:", YAML)
    print("setup ->", dbutils.notebook.run("00_setup", 600, {"yaml_path": YAML, "metadata_schema": MS}))
else:
    print("setup skipped (setup=false) — using the already-loaded config")

# COMMAND ----------

# MAGIC %md ## 2. Resolve which syncs to run (one, or ALL enabled)

# COMMAND ----------

if SYNC.upper() in ("ALL", "*", ""):
    names = [r.name for r in
             spark.sql(f"SELECT name FROM {MS}.sync_config WHERE enabled ORDER BY name").collect()]
else:
    names = [SYNC]
assert names, "no syncs to run (empty/enabled sync_config?) — run with setup=true first"
print(f"running {len(names)} sync(s): {names}  apply={APPLY}")

# COMMAND ----------

# MAGIC %md ## 3. Run each sync (preview or apply)

# COMMAND ----------

results = []
for n in names:
    out = dbutils.notebook.run("sync_oracle_metadata", 1800,
                               {"sync_name": n, "apply": APPLY, "metadata_schema": MS})
    try:
        summary = json.loads(out)
    except Exception:
        summary = {"sync": n, "raw": out}
    results.append(summary)
    print(json.dumps(summary, indent=2))

# COMMAND ----------

# MAGIC %md ## 4. Combined summary

# COMMAND ----------

rollup = {"setup": SETUP, "apply": APPLY == "true",
          "syncs_run": [s.get("sync") for s in results],
          "comment_changes": sum(int(s.get("comment_changes", 0) or 0) for s in results),
          "tag_changes": sum(int(s.get("tag_changes", 0) or 0) for s in results),
          "results": results}
print(json.dumps(rollup, indent=2))
dbutils.notebook.exit(json.dumps(rollup))
