# Oracle → Unity Catalog metadata sync — Getting Started

A complete, self-contained guide. No prior reading required. By the end you'll understand what the tool is,
the pieces it's made of, how they fit, and how to set it up and run it — with sample configs to copy.

> **In one sentence:** it copies Oracle's table/column **descriptions (comments)** and **governance labels
> (annotations)** onto your Databricks Unity Catalog objects, automatically and repeatably — and can keep a
> **Genie space** current as a bonus.

---

## 1. Why this exists

Databricks can query Oracle live with **Lakehouse Federation** (a read-only "foreign catalog" that mirrors
Oracle tables). But federation brings over **structure and data only** — Oracle's **comments and annotations
do not come across**, and you **can't write** comments/tags onto the read-only federated objects.

This tool closes that gap. It reads Oracle's comments and annotations and applies them to Databricks objects
**you own**, so Catalog Explorer, search, and **Genie** show the same business context your Oracle users see.

There are **two ways** it can put metadata into Databricks:
- **Metadata-only mode** (the **default**): your objects **already exist** (e.g. built by Lakeflow Connect or
  by hand) and it **only applies metadata to them in place** — it creates nothing.
- **Create mode** (`metadata_only: false`): it creates an object in your catalog (a view, materialized view,
  or table) over the federated source and decorates that.

---

## 2. The pieces

### On the Oracle side (set up once, read-only)
| Piece | What it is |
|---|---|
| **Read-only login** (e.g. `dbx_fed`) | A service account that owns no data; used by the Databricks connection. |
| **Scoping view + synonym** (`all_users_filtered` / `all_users`) | Limits which Oracle schemas Databricks even sees (so a big database doesn't flood UC). It's the single source of truth for "which schemas we expose." |
| **`v_metadata` helper view** | One view that exposes the comments and annotations as queryable rows. Federation can't surface them otherwise; this re-exposes them inside a schema Databricks can read. Scoped automatically by `all_users_filtered`. |

### On the Databricks side
| Piece | What it is |
|---|---|
| **UC connection** (e.g. `bg-oracle-ro`) | The saved Oracle login (host/port/service/user/password). Read-only by design. |
| **Foreign catalog** (e.g. `bg-oracle-ro_catalog`) | The Databricks-side mirror of the Oracle schemas you read through the connection. |
| **Sync engine** (`sync_oracle_metadata`) | The notebook that does the work: reads `v_metadata`, applies comments/tags, tracks changes. |
| **Control plane** (`bg.metadata_syn`) | A schema of bookkeeping tables the tool creates: config, object mapping, the annotation **registry**, change log, and a change feed. |
| **Target objects** (e.g. `bg.sales.*`) | The UC objects that carry the synced comments/tags — what your users and Genie actually see. |
| **Setup notebook** (`00_setup`) | Builds the control plane and loads your YAML config. |

### Optional — Genie integration
| Piece | What it is |
|---|---|
| **`genie_push` hook** | Runs after a sync that changed something; pushes join definitions into a Genie space. |
| **`demo_genie_pipeline.py`** | One-shot helper to create a Genie space over your synced tables. |

### Comments vs. annotations vs. tags (key concepts)
- **Comment** — free-text description on a table/column (Oracle `COMMENT ON`). Synced onto the UC object; Genie reads it automatically.
- **Annotation** (Oracle 23ai+) — a *named* label (e.g. `PII`, `Classification`, or Genie-feeding ones like `foreign_key` / `sql_expression_*` / `sample_query_*`). **Everything** is captured into the **registry** table; a curated subset you choose is promoted to **UC tags**, and the Genie-feeding ones drive the `genie_push` hook.
- **UC tag** — Databricks' key-value governance label (drives search, policies, Genie). This is where promoted annotations land.

### Files in the repo
| File | What it does | Required to run the sync? |
|---|---|---|
| `src/sync_oracle_metadata.py` | **The sync engine** — reads Oracle metadata via `v_metadata`, applies comments + tags, tracks changes. | ✅ **Required** |
| `src/00_setup.py` | Builds the control plane (`bg.metadata_syn`) and loads `sync_config.yaml`. Run once / after any config edit. | ✅ **Required** |
| `config/sync_config.yaml` | Your configuration — the `syncs` and the `annotation_promotion` policy. | ✅ **Required** |
| `src/run_pipeline.py` | One-notebook runner (props: `setup` / `sync_name`=name or `ALL` / `apply`) that orchestrates `00_setup` + the engine in a single task. | ⭐ Optional (convenience / scheduled job) |
| `databricks.yml` | Databricks Asset Bundle definition (jobs + targets). | Optional — only for the **bundle** run path (§5 Option B); not needed to run from the workspace. |
| `src/hooks/genie_push.py` | Post-sync hook that pushes joins/filters/measures/examples (from JSON annotations) into a Genie space. | Optional — only if you use the **Genie** integration (§ see README §10, ANNOTATION_PARSING.md). |
| `src/hooks/update_genie_space.py` | The Genie-space SDK the hook calls. | Optional — Genie only (sits beside the hook). |
| `demo_genie_pipeline.py` | One-shot script to create a Genie space over your synced tables. | Optional — Genie only. |
| `src/demo_oracle_metadata_sync.py` | End-to-end **demo**: edits a comment/annotation in Oracle, then syncs (needs writeable Oracle creds). | Optional — demo only. |
| `README.md`, `GETTING_STARTED.md`, `PIPELINE_BASICS.md`, `GENIE_UPDATE_GUIDE.md`, `ROADMAP.md` | Documentation. | No |

**Minimum to run a sync:** `src/sync_oracle_metadata.py` + `src/00_setup.py` + `config/sync_config.yaml`.

> Two things are **not** repo files: the Oracle-side **`v_metadata` helper view** + read-only login (created in
> Oracle — see §4 Step 1), and the `.databricks/` folder (local bundle build state, git-ignored).

---

## 3. How it works (one sync run)

```
 Oracle                                    Databricks
 ┌───────────────────┐   federation       ┌──────────────────────────────────────────────┐
 │ comments          │  (read-only)       │ sync_oracle_metadata                         │
 │ annotations  ─────────────────────────►│  1. read v_metadata (comments + annotations) │
 │ (via v_metadata)  │                    │  2. resolve each Oracle object → UC name     │
 └───────────────────┘                    │  3. comments → UC objects                    │
                                          │     annotations → registry (+ promoted tags) │
                                          │  4. diff vs. state, write change log         │
                                          │  5. (optional) fire genie_push hook          │
                                          └──────────────────────────────────────────────┘
```

1. **Read** Oracle comments/annotations through the `v_metadata` helper view.
2. **Resolve** each Oracle object to its UC name (schema-default rule + optional per-object overrides).
3. **Comments** → applied to the UC object (created over the federated source, or in place if metadata-only).
4. **Annotations** → **all** captured to the `oracle_annotations` registry; the policy-approved subset applied as **UC tags** (with key sanitization, value truncation, and a 50-tags/securable cap).
5. **Change-track** everything (so re-runs only touch what changed) and optionally fire the Genie hook.

**Two modes, set per sync:**
- **`metadata_only: true`** (the **default**): target already exists — apply comments/tags **in place**, create nothing. Works for tables, MVs, **and** views (view definitions are preserved). The engine **auto-detects each target's real type** and uses the correct DDL, so one sync can cover a mix. Objects map by `source.schema` → `target.catalog`.`target.schema`; use `objects.overrides` for exceptions.
- **`metadata_only: false`** (create): builds the target object. `target.type` = `VIEW` (zero-copy, always live), `MATERIALIZED_VIEW` (refreshable copy), or `TABLE` (one-time snapshot).

`apply=false` (dry run) previews every change and writes nothing. `apply=true` applies.

---

## 4. Setup guide

### Prerequisites
- An Oracle database (23ai/26ai for annotations; 19c/21c works for **comments only**).
- A Databricks workspace with Unity Catalog and a **serverless SQL warehouse** (needed for federation + MVs).
- Permission to create a UC connection + foreign catalog, and a target catalog you can write to.

> Names below (`dbx_fed`, `sales`/`SALES`, `bg-oracle-ro`, `bg`) are **placeholders** — substitute your own.

### Step 1 — Oracle (run once; read-only, no data changes)
Have your DBA run this. `<password>` is the DBA's choice; it only ever goes into the UC connection.

```sql
-- 1. read-only login (owns no data)
CREATE USER dbx_fed IDENTIFIED BY <password>;
GRANT CREATE SESSION, CREATE VIEW, CREATE SYNONYM TO dbx_fed;   -- RESOURCE does NOT include CREATE VIEW

-- 2. read access to the tables/views you want in Databricks
GRANT SELECT ON sales.customers TO dbx_fed;
GRANT SELECT ON sales.orders    TO dbx_fed;     -- one per object, or loop over the schema

-- 3. limit which schemas Databricks sees (single source of truth for scope), connected AS dbx_fed:
CREATE OR REPLACE VIEW    all_users_filtered AS
  SELECT * FROM sys.all_users WHERE username IN ('SALES','DBX_FED');   -- schemas to expose
CREATE OR REPLACE SYNONYM all_users FOR all_users_filtered;

-- 4. the helper view that exposes comments + annotations (scoped by step 3), AS dbx_fed:
CREATE OR REPLACE VIEW v_metadata AS
  SELECT CAST('COMMENT' AS VARCHAR2(16)) AS kind, owner, table_name AS object_name,
         CAST(table_type AS VARCHAR2(23)) AS object_type,
         CAST(NULL AS VARCHAR2(128)) AS column_name, CAST(NULL AS VARCHAR2(128)) AS meta_name,
         comments AS meta_value
  FROM all_tab_comments
  WHERE comments IS NOT NULL AND table_type IN ('TABLE','VIEW')
    AND owner IN (SELECT username FROM all_users_filtered)
  UNION ALL
  SELECT 'COMMENT', owner, table_name, NULL, column_name, NULL, comments
  FROM all_col_comments
  WHERE comments IS NOT NULL
    AND owner IN (SELECT username FROM all_users_filtered)
  UNION ALL
  -- annotations: Oracle 23ai+ only — DROP THIS BLOCK on 19c/21c (comments still sync).
  SELECT 'ANNOTATION', o.owner, a.object_name, a.object_type, a.column_name, a.annotation_name, a.annotation_value
  FROM all_annotations_usage a
  JOIN all_objects o ON o.object_name = a.object_name AND o.object_type = a.object_type
  WHERE o.owner IN (SELECT username FROM all_users_filtered);
```
Verify: `SELECT username FROM all_users;` → only your schemas; `SELECT kind, count(*) FROM v_metadata GROUP BY kind;`.

### Step 2 — Network
The Oracle listener (port 1521) must be reachable from your workspace's **serverless egress**. Allowlist your
workspace's serverless stable egress IPs on the Oracle host's firewall. (Azure: filter
`service=Databricks, platform=azure, type=outbound, region=<your region>` in
`https://www.databricks.com/networking/v1/ip-ranges.json`; AWS has an equivalent list in your workspace's
network settings.)

### Step 3 — Databricks connection + foreign catalog
UI: **Catalog → External Data → Create connection** (type **Oracle**; host, port, `service_name`, user,
password). Then create a **foreign catalog** over it. Or via SQL:
```sql
CREATE CONNECTION `bg-oracle-ro` TYPE oracle
  OPTIONS (host '<host>', port '1521', user 'dbx_fed', password '<password>');   -- + service_name via UI/API
CREATE FOREIGN CATALOG `bg-oracle-ro_catalog` USING CONNECTION `bg-oracle-ro`;
```
Grant the identity that runs the jobs read on the foreign catalog and write on the target catalog (`bg`).

### Step 4 — Configure
Edit `config/sync_config.yaml` (see §6 for samples): point each sync at your connection + schema + target.

### Step 5 — Load the config & run
- Run **`00_setup`** (loads the YAML into the control plane). **Re-run it whenever you edit the YAML** — the engine reads the loaded config table, not the file.
- Run **`sync_oracle_metadata`** with `apply=false` to preview, then `apply=true` to apply.

---

## 5. How to run

You can run the notebooks two ways — they're identical; the bundle just packages them. **Run order is always:
edit config → `00_setup` → `sync` (preview) → `sync` (apply).**

**Widgets / parameters** on the notebooks:
- `00_setup`: `yaml_path` (path to your `sync_config.yaml`), `metadata_schema` (control-plane location, default `bg.metadata_syn`).
- `sync_oracle_metadata`: `sync_name` (which sync to run), `apply` (`true`/`false`), `metadata_schema`.

### End-to-end walkthrough
This uses the shipped `sales` sync (metadata-only over `bg.sales`). Run it via the DAB **or** the workspace UI —
both shown at each step.

**1. Configure.** Edit `config/sync_config.yaml` — point `source`/`metadata`/`target` at your connection,
schema, and target catalog/schema (see §6). For this walkthrough the shipped `sales` sync is ready as-is.

**2. Load the config into the control plane** (run `00_setup`). Re-run this **every time** you edit the YAML —
the engine reads the loaded `sync_config` table, not the file.
```bash
# DAB:
databricks bundle deploy -t dev
databricks bundle run setup -t dev
# Workspace UI: open 00_setup, set yaml_path to your config, Run all.
```
Expected: `schema-default mappings ensured: [('SALES', 'VIEW')]` and `loaded 1 syncs, 6 promotion rules, 0 object overrides`.

**3. Preview (dry run — writes nothing).** Always do this first.
```bash
# DAB:
databricks bundle run sync -t dev --params sync_name=sales,apply=false
# Workspace UI: open sync_oracle_metadata, set sync_name=sales, apply=false, Run all.
```
The run prints exactly what *would* change and a JSON summary, e.g.:
```json
{"sync": "sales", "applied": false, "metadata_only": true,
 "comment_changes": 8, "tag_changes": 9, "tags_blocked": [],
 "registry_active": 23, "registry_changed": true,
 "affected_objects": ["CUSTOMERS", "ORDERS", "ORDER_SUMMARY"]}
```

**4. Apply.** Same command with `apply=true`:
```bash
databricks bundle run sync -t dev --params sync_name=sales,apply=true
```
Summary now shows `"applied": true` with the counts actually written. `tags_blocked` lists any governed-tag
rejections (kept registry-only — not failures).

**5. Verify** (SQL — substitute your catalog/schema):
```sql
-- comments now on the objects:
SELECT column_name, comment FROM bg.information_schema.columns
WHERE table_schema='sales' AND table_name='customers' AND comment IS NOT NULL;
-- promoted tags:
SELECT table_name, column_name, tag_name, tag_value FROM bg.information_schema.column_tags
WHERE schema_name='sales';
-- every annotation captured (even un-promoted ones):
SELECT oracle_object, annotation_name, annotation_value FROM bg.metadata_syn.oracle_annotations WHERE is_active;
-- audit trail for the run:
SELECT * FROM bg.metadata_syn.sync_change_feed ORDER BY changed_at DESC LIMIT 50;
```

**6. Schedule.** The DAB ships the **`pipeline`** job with a **paused daily schedule** and `apply=false` for
safety. To go live: set its `apply` parameter to `true` and **unpause** the schedule. (Workspace: **Workflows →
Create job →** a notebook task on `run_pipeline` with parameters `setup=false`, `sync_name=ALL`, `apply=true`,
on your schedule.)

> Re-runs are **incremental**: with no Oracle change, the next run is a no-op (nothing rewritten). Change a
> comment/annotation in Oracle and only that change flows through.

### Shortcut: the one-notebook runner (`run_pipeline`)
Steps 2–4 above are three separate notebook runs. `src/run_pipeline.py` does all of them in **one** props-driven
notebook — ideal as a single scheduled job task. Props:

| Prop | Meaning |
|---|---|
| `setup` | `true` = (re)load the config first (do this after editing the YAML); `false` = use the loaded config. |
| `sync_name` | a sync's `name`, or **`ALL`** to run every enabled sync. |
| `apply` | `false` = preview; `true` = apply. |
| `yaml_path` | config path for setup (blank = auto-find `config/sync_config.yaml` beside the notebook). |
| `metadata_schema` | control-plane schema (default `bg.metadata_syn`). |

It just orchestrates `00_setup` + `sync_oracle_metadata` (no duplicated logic) and returns a combined JSON summary.

- **Workspace UI:** open `run_pipeline`, set props (e.g. `setup=true, sync_name=ALL, apply=false`), Run all.
- **As a job / DAB:** the bundle ships a **`pipeline`** job that runs `run_pipeline` with job parameters
  `setup` / `sync_name` / `apply`:
  ```bash
  databricks bundle run pipeline -t dev --params setup=true,sync_name=ALL,apply=false   # preview everything
  databricks bundle run pipeline -t dev --params setup=false,sync_name=ALL,apply=true   # apply everything
  ```
  Schedule this one job instead of wiring setup + sync separately.

> Tip: this notebook is also the easy path to a **pure-Python job** — its props are plain widgets, so you can
> point a Python/notebook job at it (or port the four cells to a `.py` task) and drive everything from job parameters.

---

## 6. Sample configs

The config has two parts: **`syncs`** (one entry per job) and **`annotation_promotion`** (which annotations
become tags). Run a sync by its `name`.

### The three blocks: `source`, `metadata`, `target`
Each sync points at three places:

| Block | What it is | Example |
|---|---|---|
| `source` | **Where the data is** — the Oracle app schema (federated) whose tables/columns you're describing. | `bg-oracle-ro_catalog.sales` |
| `metadata` | **Where the comments/annotations are read from** — the schema holding the `v_metadata` helper view. | `bg-oracle-ro_catalog.dbx_fed` |
| `target` | **Where the UC objects you're decorating live.** | `bg.sales` |

`metadata` points at the **`v_metadata` helper view** (the one view created in Oracle setup, §1/§4, that exposes
comments + annotations as rows). The engine reads:
- comments/annotations from `{metadata.catalog}.{metadata.schema}.v_metadata`, and
- table/column structure from `{source.catalog}.{source.schema}.<object>`.

**Why `metadata` is separate from `source`:** the helper view is built on Oracle's data dictionary, and Oracle
won't let you grant access to a dictionary-based view owned by the app schema (`ORA-01720`). So the view must be
**owned by the read-only login** and live in **its own schema** (e.g. `dbx_fed`), not the data's schema
(`sales`). `metadata` is how you tell the engine where that view ended up. Omit it and it defaults to `source`.
*(Advanced: `metadata` can even point at a different connection/catalog — a separate "metadata" login — but most
setups use one connection and just a different schema.)*

### A. Minimal — map a schema to a UC catalog + schema (default: metadata-only)
```yaml
syncs:
  - name: sales
    source:   {connection: bg-oracle-ro, catalog: bg-oracle-ro_catalog, schema: sales}
    metadata: {catalog: bg-oracle-ro_catalog, schema: dbx_fed}
    target:   {catalog: bg, schema: sales}
```
Applies comments to the **existing** objects in `bg.sales` (every object in the Oracle schema). Comments are
on by default; annotations are off until enabled. Add `metadata_only: false` (and a `target.type`) to
**create** the objects instead.

### B. Create three target types over one schema (`metadata_only: false`)
```yaml
syncs:
  - name: sales                       # build zero-copy views in bg.sales
    source:   {connection: bg-oracle-ro, catalog: bg-oracle-ro_catalog, schema: sales}
    metadata: {catalog: bg-oracle-ro_catalog, schema: dbx_fed}
    target:   {catalog: bg, schema: sales, type: VIEW}
    metadata_only: false
    annotations: {enabled: true, apply_to_objects: true}
    objects: {default: all}
  - name: sales_mv                    # build refreshable copies in bg.sales_mv
    source:   {connection: bg-oracle-ro, catalog: bg-oracle-ro_catalog, schema: sales}
    metadata: {catalog: bg-oracle-ro_catalog, schema: dbx_fed}
    target:   {catalog: bg, schema: sales_mv, type: MATERIALIZED_VIEW}
    metadata_only: false
    annotations: {enabled: true, apply_to_objects: true}
    objects: {default: all}
  - name: sales_table                 # build static snapshots in bg.sales_tbl
    source:   {connection: bg-oracle-ro, catalog: bg-oracle-ro_catalog, schema: sales}
    metadata: {catalog: bg-oracle-ro_catalog, schema: dbx_fed}
    target:   {catalog: bg, schema: sales_tbl, type: TABLE}
    metadata_only: false
    annotations: {enabled: true, apply_to_objects: true}
    objects: {default: all}

annotation_promotion:                 # default = registry-only; only names here become tags
  - {name: PII,            route: TAG, scope: COLUMN}
  - {name: Classification, route: TAG, scope: BOTH}
  - {name: Owner_Team,     route: TAG, scope: TABLE}
  - {name: Geo,            route: TAG, scope: COLUMN}
```

### C. Metadata-only — decorate objects that ALREADY exist (no creation)
Use when your UC objects were built by Lakeflow Connect / by hand. Map each Oracle object to its existing
UC object explicitly.
```yaml
syncs:
  - name: cust_existing_mv
    source:   {connection: bg-oracle-ro, catalog: bg-oracle-ro_catalog, schema: sales}
    metadata: {catalog: bg-oracle-ro_catalog, schema: dbx_fed}
    target:   {catalog: bg, type: MATERIALIZED_VIEW}   # used to resolve the target; real type is auto-detected
    annotations: {enabled: true, apply_to_objects: true}
    metadata_only: true                                # ← do not create; apply in place
    objects:
      default: all
      overrides:
        - {oracle_object: CUSTOMERS, target_catalog: bg, target_schema: prod, target_object: customers_mv}
```
Tables, MVs, and views are all decorated in place; a view's existing definition is preserved.

### D. With the Genie integration (optional)
```yaml
syncs:
  - name: sales
    source:   {connection: bg-oracle-ro, catalog: bg-oracle-ro_catalog, schema: sales}
    metadata: {catalog: bg-oracle-ro_catalog, schema: dbx_fed}
    target:   {catalog: bg, type: VIEW}
    annotations: {enabled: true, apply_to_objects: true}
    objects: {default: all}
    hooks: {on_change_notebook: "hooks/genie_push", genie_space_id: "<your space id>"}
```
Comments reach Genie automatically; **joins** (and filters/measures/examples) come from JSON annotations —
a `foreign_key` annotation on the FK column carries both sides (see [ANNOTATION_PARSING.md](ANNOTATION_PARSING.md)):
```sql
ALTER MATERIALIZED VIEW po_edd_mv MODIFY per_intr_no_buy ANNOTATIONS (REPLACE foreign_key '{
  "left_table": "po_edd_mv", "right_table": "all_users_v1_mv", "join_condition": "=",
  "left_column": "per_intr_no_buy", "right_column": "per_intr_no",
  "relationship": "Many to One", "Type": "Join" }');
```

### Config field quick-reference
| Field | Meaning |
|---|---|
| `source.connection` / `.catalog` / `.schema` | UC connection, foreign catalog, and Oracle schema to read |
| `metadata.catalog` / `.schema` | Where the `v_metadata` helper view lives (the login's schema) |
| `target.catalog` / `.type` | Target catalog; `VIEW` \| `MATERIALIZED_VIEW` \| `TABLE` |
| `comments` | Sync table/column comments (default `true`) |
| `annotations.enabled` / `.apply_to_objects` | Capture annotations to the registry / also apply promoted ones as tags |
| `objects.default` / `.exclude` / `.overrides` | `all` objects in the schema; names to skip; per-object remaps |
| `metadata_only` | `true` = apply to pre-existing objects in place; create nothing |
| `hooks.on_change_notebook` / `.genie_space_id` | Optional post-sync hook + Genie space to update |

---

## 7. What success looks like
After `apply=true`:
- Your target objects show Oracle comments in **Catalog Explorer** (`system.information_schema.columns.comment`).
- Promoted annotations appear as **tags** (`…information_schema.column_tags` / `table_tags`).
- **All** annotations are queryable in the registry: `SELECT * FROM bg.metadata_syn.oracle_annotations`.
- Every change is logged: `SELECT * FROM bg.metadata_syn.sync_change_feed WHERE run_id = '<run_id>'`.
- Re-running with no Oracle change is a no-op (nothing rewritten).

---

## 8. Troubleshooting
| Symptom | Cause / fix |
|---|---|
| `FAILED_JDBC.CONNECTION` | Oracle firewall doesn't allow the workspace's serverless egress IPs (§2); or wrong creds/encryption. |
| `Tag value … not allowed for tag policy key …` | A **governed tag** rejected a value — set a conforming value in the promotion policy, or leave it registry-only. |
| A view/MV/table wasn't built or decorated | Object not visible to the login (missing `SELECT`); or `v_metadata` missing; in `metadata_only`, the mapped target doesn't exist. |
| "Annotation step skipped" | Source is pre-23ai (no annotations) — expected; comments still sync. |
| Genie didn't update | Was it an `apply=true` run that actually changed something? Is `genie_space_id` set and `setup` re-run after editing the YAML? |

---

## 9. Where to go next
- **`README.md`** — full reference (every option, limits, target-type comparison, governed tags).
- **`PIPELINE_BASICS.md`** — the Oracle→Genie pipeline in brief.
- **`GENIE_UPDATE_GUIDE.md`** — pushing annotations/joins into Genie with the SDK directly.
- **`ROADMAP.md`** — what's done and what's next.
