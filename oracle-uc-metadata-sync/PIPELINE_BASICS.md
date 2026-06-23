# Oracle → Unity Catalog → Genie — the basics

A short orientation for an engineer running the pipeline. For full setup/options see `README.md`.

## What it does
When metadata changes in **Oracle**, this pipeline carries it into **Databricks** and keeps a **Genie space**
current:

- **Comments** (table/column descriptions) → land on the Databricks objects, and Genie reads them.
- **Joins** (how tables relate) → pushed into the Genie space so Genie can answer cross-table questions.

You change things in **Oracle**; everything downstream follows on the next sync.

## The flow

```
  Oracle (source of truth)                 Databricks
  ┌─────────────────────┐                  ┌────────────────────────────────────────────┐
  │ COMMENT ON …        │   sync job       │ sync_oracle_metadata (engine)              │
  │ ANNOTATIONS (incl.  │ ───────────────► │   • comments  → bg.<schema>.* objects      │
  │   RELATED_TO)       │                  │   • annotations → registry table           │
  └─────────────────────┘                  │        │ (fires only if something changed) │
                                           │        ▼                                   │
                                           │   genie_push (hook) → Genie space          │
                                           │      • joins (from RELATED_TO)             │
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
- **Joins → Genie: you author one annotation.** On the **foreign-key column** in Oracle, add a `RELATED_TO`
  annotation naming the table/column it points to. The hook turns each one into a Genie join.

### The one thing you author: `RELATED_TO`
On the FK column (Oracle 23ai+):
```sql
ALTER TABLE orders MODIFY (customer_id
  ANNOTATIONS (ADD OR REPLACE RELATED_TO 'customers.customer_id;rt=MANY_TO_ONE'));
```
- The annotated column is the **left** side; the value `customers.customer_id` is the **right** side.
- `rt=` is the relationship (default `MANY_TO_ONE`; also `ONE_TO_MANY`, `ONE_TO_ONE`, `MANY_TO_MANY`).
- `RELATED_TO` is a plain relationship annotation — your own Oracle code can read it too. It is **not**
  Genie-specific, and it does not collide with governance annotations (`PII`, `Classification`, …).

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
- The hook **replaces only the joins** in the room. Human-authored instructions, examples, and sample
  questions are left untouched.
- If the hook fails, the **sync still succeeds** — the failure is reported in the run summary, not fatal.
- A sync with `genie_space_id` blank runs normally and just **skips** the Genie step.

## When something looks off
| Symptom | Check |
|---|---|
| Genie didn't update | Was it an `apply=true` run? Did anything actually change? Is `genie_space_id` set (and `setup` re-run after editing the YAML)? |
| A join didn't appear | Is there a `RELATED_TO` annotation on the FK column, value `table.column`? Did that table get synced? |
| A comment didn't appear | Did the sync run for that schema? Comments only update on `apply=true`. |
