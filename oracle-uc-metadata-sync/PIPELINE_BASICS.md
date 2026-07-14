# Oracle → Unity Catalog → Genie — the basics

A short orientation for an engineer running the pipeline. For full setup/options see `README.md`.

## What it does
When metadata changes in **Oracle**, this pipeline carries it into **Databricks** and keeps a **Genie space**
current:

- **Comments** (table/column descriptions) → land on the Databricks objects, and Genie reads them.
- **Joins, filters, measures, example queries** (from JSON annotations) → pushed into the Genie space so Genie
  answers cross-table and domain questions.

You change things in **Oracle**; everything downstream follows on the next sync.

## The flow

```
  Oracle (source of truth)                 Databricks
  ┌─────────────────────┐                  ┌────────────────────────────────────────────┐
  │ COMMENT ON …        │   sync job       │ sync_oracle_metadata (engine)              │
  │ ANNOTATIONS (JSON:  │ ───────────────► │   • comments  → bg.<schema>.* objects      │
  │   foreign_key,      │                  │   • annotations → registry table           │
  │   sql_expression_*, │                  │        │ (fires only if something changed) │
  │   sample_query_*)   │                  │        ▼                                   │
  └─────────────────────┘                  │   genie_push (hook) → Genie space          │
                                           │      • joins / filters / expressions /     │
                                           │        examples (from JSON annotations)    │
                                           └────────────────────────────────────────────┘
```

## The moving parts (4)
| Part | What it is |
|---|---|
| **`sync_oracle_metadata`** | The engine. Reads Oracle metadata, applies comments to the UC objects, records annotations. |
| **`bg.metadata_syn`** | Bookkeeping: which annotations exist, change history, config. (The "registry".) |
| **`genie_push`** (hook) | Runs automatically after a sync that changed something; updates the Genie space. |
| **`update_genie_space.py`** | The small SDK the hook uses to talk to the Genie API. |

## The two propagation paths
- **Comments → Genie: automatic, nothing to configure.** The sync writes comments onto the Databricks
  views/tables; Genie reads Unity Catalog comments directly.
- **Joins / filters / measures / examples → Genie: from JSON annotations.** You author annotations whose
  value is a JSON object; the hook parses them and writes the matching Genie sections. The
  `annotation_name` prefix picks the kind. Full spec + examples: **[ANNOTATION_PARSING.md](ANNOTATION_PARSING.md)**.

### What you author: JSON annotations
On the FK column, a `foreign_key` annotation (Oracle 26ai) — the value is JSON naming both sides:
```sql
ALTER MATERIALIZED VIEW po_edd_mv MODIFY per_intr_no_buy ANNOTATIONS (REPLACE foreign_key '{
  "left_table": "po_edd_mv", "right_table": "all_users_v1_mv", "join_condition": "=",
  "left_column": "per_intr_no_buy", "right_column": "per_intr_no",
  "relationship": "Many to One", "Type": "Join" }');
```
- `foreign_key` → a Genie **join** (relationship + condition come from the JSON; role-playing dims aliased).
- `sql_expression_<label>` → a **filter** (`Type: "Filter"`) or **measure/expression** (other types).
- `sample_query_<label>` → an **example** query.
- Governance annotations (`PII`, `Classification`, …) are untouched by the hook and flow to the registry/tags.

## How to run it
1. **Point a sync at a Genie space** — in `config/sync_config.yaml`, on your sync:
   ```yaml
   hooks: {on_change_notebook: "hooks/genie_push", genie_space_id: "<your space id>"}
   ```
   (Create a space with `demo_genie_pipeline.py`, or use an existing one. Leave `genie_space_id` blank to
   skip the Genie step entirely.)
2. **Load the config:** run the **`setup`** notebook/job. **NOTE:** Editing the YAML has no effect until you re-run
   `setup` — the engine reads the loaded config table, not the file.
3. **Run the sync:** run **`sync_oracle_metadata`** with `apply=true`. If anything changed, the hook fires and
   updates the room. (`apply=false` previews without writing.)

## Good to know
- The hook fires **only on an `apply=true` run that actually changed** a comment, tag, or annotation. No
  change → no Genie call. Dry runs never touch Genie.
- The hook **replaces only the sections it manages** (joins, filters, expressions, examples) and merges
  instructions into a managed block. Other human-authored content in the room is left untouched.
- If the hook fails, the **sync still succeeds** — the failure is reported in the run summary, not fatal.
- A sync with `genie_space_id` blank runs normally and just **skips** the Genie step.

## When something looks off
| Symptom | Check |
|---|---|
| Genie didn't update | Was it an `apply=true` run? Did anything actually change? Is `genie_space_id` set (and `setup` re-run after editing the YAML)? |
| A join didn't appear | Is there a `foreign_key` JSON annotation on the FK column with valid `right_table`/`right_column`? Did that table get synced? Check the hook summary for `skipped`. |
| A comment didn't appear | Did the sync run for that schema? Comments only update on `apply=true`. |
