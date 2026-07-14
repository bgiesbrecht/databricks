# Databricks notebook source
# MAGIC %md
# MAGIC # Post-sync hook: push Oracle-derived config into a Genie space
# MAGIC Fired by the sync engine (`on_change_notebook`) after a run that changed something. It reads the
# MAGIC **annotation registry**, parses each annotation's JSON value with the shared pipeline
# MAGIC (`annotation_parser` → `annotation_to_genie`), and applies the result to the Genie space.
# MAGIC
# MAGIC **What maps where** (see ANNOTATION_PARSING.md for the full spec):
# MAGIC - `foreign_key` → Genie **joins** (relationship + join_condition from the JSON; role-playing dims aliased)
# MAGIC - `sql_expression_*` (Type=Filter) → **sql_filters**; other types → **sql_expressions**
# MAGIC - `sample_query_*` → **examples** (Oracle schema prefix rewritten to the UC name)
# MAGIC - `AI_GUIDANCE` / DESCRIPTION → **text_instruction** (merged into a managed block, human prose preserved)
# MAGIC
# MAGIC **Comments** are NOT pushed here — the sync already wrote them onto the UC objects and Genie reads UC
# MAGIC comments directly. Only sections we produced content for are touched; everything else in the room is
# MAGIC left as-authored. Join targets are resolved via `{metadata_schema}.resolve_uc_name` (honors overrides).

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Run id (from the sync)")
dbutils.widgets.text("sync_name", "sales", "Sync name (from sync_config)")
dbutils.widgets.text("metadata_schema", "bg.metadata_syn", "Control-plane schema")
RUN_ID = dbutils.widgets.get("run_id")
SYNC = dbutils.widgets.get("sync_name")
MS = dbutils.widgets.get("metadata_schema")

import json
import os
import sys

# The vendored SDK (update_genie_space.py) is a sibling in src/hooks/; the parser + renderer live in
# src/ (one level up). Databricks puts the notebook's own dir on sys.path — add its parent so the
# shared pipeline modules import too, in both bundle and workspace-files deployments.
def _ensure_paths():
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        nb_dir = os.path.dirname("/Workspace" + ctx.notebookPath().get())   # .../src/hooks
        for p in (nb_dir, os.path.dirname(nb_dir)):                          # src/hooks, then src
            if p not in sys.path:
                sys.path.insert(0, p)
    except Exception as e:
        print(f"  path setup note: {str(e).splitlines()[0][:80]}")
_ensure_paths()

from update_genie_space import GenieSpaces, set_text_instruction
from annotation_parser import parse_rows
from annotation_to_genie import render_genie_config

# COMMAND ----------

cfg = spark.sql(f"SELECT * FROM {MS}.sync_config WHERE name='{SYNC}'").collect()
assert cfg, f"no sync_config row named '{SYNC}'"
c = cfg[0]
space_id = (c.genie_space_id or "").strip()
if not space_id:
    print(f"sync '{SYNC}' has no genie_space_id — nothing to push.")
    dbutils.notebook.exit(json.dumps({"sync": SYNC, "genie": "skipped (no space id)"}))

src_schema = c.source_schema
tt = (c.target_type or "VIEW").upper()
resolver = f"{MS}.resolve_uc_name"
print(f"genie_push sync={SYNC} space_id={space_id} target_type={tt} run_id={RUN_ID}")

# Authoritative Oracle-object -> UC-name resolution (honors per-object overrides). Cached; returns
# None on any failure so the renderer can fall back to its heuristic and report the miss.
_uc_cache = {}
def resolve_uc(obj):
    key = (obj or "").upper()
    if key not in _uc_cache:
        try:
            _uc_cache[key] = spark.sql(f"SELECT {resolver}('{src_schema}','{obj}','{tt}') n").collect()[0].n
        except Exception:
            _uc_cache[key] = None
    return _uc_cache[key]

# COMMAND ----------

# MAGIC %md ## 1. Read the active annotations for this sync

# COMMAND ----------

rows = spark.sql(f"""
  SELECT sync_name, oracle_schema, oracle_object, oracle_column, level,
         object_type, annotation_name, annotation_value, uc_name, is_active
  FROM {MS}.oracle_annotations
  WHERE sync_name='{SYNC}' AND is_active
""").collect()
print(f"{len(rows)} active annotation row(s)")

# COMMAND ----------

# MAGIC %md ## 2. Parse + render into a portable Genie config

# COMMAND ----------

parsed = parse_rows(rows)
config, report = render_genie_config(parsed, resolve=resolve_uc)
print("parse:", parsed.summary())
print("render:", report.summary())
for s in report.skipped:  print(f"  SKIP     {s}")
for w in report.warnings: print(f"  WARN     {w}")
for r in report.repaired: print(f"  REPAIRED {r}")

# text_instruction is applied via a managed merge-block (below), not apply_config (which replaces).
text_instruction = config.pop("text_instruction", None)

if not config and not text_instruction:
    print("nothing to push (no renderable annotations).")
    dbutils.notebook.exit(json.dumps({"sync": SYNC, "space_id": space_id, "genie": "skipped (empty)",
                                      "skipped": len(report.skipped), "repaired": len(report.repaired)}))

# COMMAND ----------

# MAGIC %md ## 3. Apply to the Genie space

# COMMAND ----------

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
client = GenieSpaces(ctx.apiUrl().get(), ctx.apiToken().get())

# 3a. Section-replace via apply_config — ONLY the sections we produced (others left untouched).
#     Each section this sync manages is fully owned by it: a re-run replaces it.
if config:
    client.apply_config(space_id, config)

# 3b. Instructions MERGED into a sentinel-delimited managed block so human-authored prose survives.
MARK_B, MARK_E = "<!-- BEGIN oracle-sync -->", "<!-- END oracle-sync -->"
instr_pushed = 0
if text_instruction:
    import re
    _, inner, etag = client.get(space_id)
    ti = inner.get("instructions", {}).get("text_instructions", [])
    cur = ""
    if ti:
        content = ti[0].get("content")
        cur = "".join(content) if isinstance(content, list) else (content or "")
    human = re.sub(re.escape(MARK_B) + r".*?" + re.escape(MARK_E), "", cur, flags=re.S).strip()
    block = f"{MARK_B}\n{text_instruction}\n{MARK_E}"
    set_text_instruction(inner, (human + "\n\n" + block).strip())
    client.patch(space_id, inner, etag)
    instr_pushed = 1

# COMMAND ----------

summary = {"run_id": RUN_ID, "sync": SYNC, "space_id": space_id,
           "joins": report.joins, "sql_filters": report.sql_filters,
           "sql_expressions": report.sql_expressions, "examples": report.examples,
           "instructions": instr_pushed, "skipped": len(report.skipped),
           "warnings": len(report.warnings), "repaired": len(report.repaired)}
print(json.dumps(summary, indent=2))
dbutils.notebook.exit(json.dumps(summary))
