# Oracle → Unity Catalog metadata sync

Copies *descriptions* (comments) and *governance labels* (annotations) from your Oracle tables and columns onto the matching objects in Databricks Unity Catalog — automatically and repeatably.

When Databricks queries Oracle live (Lakehouse Federation), Oracle's comments and annotations **don't come across**. This tool brings them over, so your Databricks tables/views show the same business context — which makes Catalog Explorer, search, and **Genie** (Databricks' AI assistant) far more useful.

**What you get**
- Oracle table/column **comments** → descriptions on the Databricks objects.
- Oracle **annotations** (governance metadata; Oracle 23ai+) → a queryable **registry** of everything, plus
  the ones you choose promoted to Unity Catalog **tags** (e.g. `PII`, `Classification`).
- An **audit log** of every change, a safe **preview (dry-run)** mode, and a **schedule**.
- Runs as plain Databricks notebooks you import and run — with an **optional** Databricks Asset Bundle for one-command, multi-environment deploys.

---

## Quick start — set it up in order

This is set up **once**; after that it runs on a schedule. Each step links to full detail.

| # | Do this |
|---|---------|
| 1 | Create a **read-only** Oracle login + one helper view (read-only — your data is not modified). Copy-paste SQL is in §1. |
| 2 | Allowlist Databricks' network on the Oracle firewall *(§2b)*, create a UC **connection** + **foreign catalog** using the step-1 login *(§2a)*, and run the **grants** *(§2c)*. |
| 3 | Edit `config/sync_config.yaml`: point it at the connection + schema, and list which annotations should become tags. *(detail: §3)* |
| 4 | Run it — **either** import the notebooks into a workspace and run them, **or** deploy the bundle. Preview with `apply=false`, then `apply=true`. *(detail: §4)* |
| 5 | Happy with the result? In the `pipeline` job set `apply=true` and unpause its schedule. Done. |

**Want to see it work first?** After steps 1–2, run the **`demo`** job — it changes a comment/annotation in
Oracle and shows it flow into Databricks end-to-end.

## Names you'll replace
The examples below use placeholder names — substitute your own everywhere they appear:
| In the docs | Replace with **your**… |
|---|---|
| `dbx_fed` | Oracle login (the read-only service account) |
| `sales` / `SALES` | Oracle application schema (the one whose tables you want in Databricks) |
| `bg-oracle-ro` / `bg-oracle-ro_catalog` | UC connection name / foreign catalog name |
| `bg` | target catalog in Databricks (where the synced views/tables + control plane live) |

## Glossary
- **Lakehouse Federation** — Databricks querying Oracle live, *without copying the data*.
- **Connection** / **foreign catalog** — the saved Oracle login, and the Databricks-side "mirror" of the Oracle schema you read through it.
- **Comment** — free-text description on a table/column (Oracle `COMMENT ON`; shows in Databricks Catalog Explorer).
- **Annotation** (Oracle 23ai+) — a named governance label on a table/column (e.g. `PII`). Richer/structured vs. a comment.
- **Tag** (Unity Catalog) — Databricks' key-value governance label (used for search, access policies, Genie). Where we put selected annotations.
- **Registry** — a Databricks table holding *every* Oracle annotation we read — even ones not turned into tags.
- **DAB (Databricks Asset Bundle)** — a folder of code + jobs + config you deploy with the Databricks CLI.
- **Genie** — Databricks' natural-language query assistant; better comments/tags = better answers.
- **MV / CTAS table** — a *materialized* (refreshable) copy / a one-time *snapshot* copy of an Oracle object in Databricks.

> Sections **§6–§7** explain how it works internally — **you don't need them to set it up.** Start with the table above.

---

## 1. Oracle setup

Everything here is **read-only plus one helper view — no application data is changed.** The DBA chooses
the login's password and gives it to you for the Databricks connection (§2). The same login serves both data and metadata (one Databricks connection).

Substitute your own names for `dbx_fed` (the new login), `sales` / `SALES` (your application schema), and the table names. `<password>` is whatever the DBA chooses.

**1. Create a read-only login** (owns no data):
```sql
CREATE USER dbx_fed IDENTIFIED BY <password>;
GRANT CREATE SESSION, CREATE VIEW, CREATE SYNONYM TO dbx_fed;   -- note: RESOURCE does NOT include CREATE VIEW
```
**2. Grant read access** to the tables/views you want in Databricks (this login federates them directly):
```sql
GRANT SELECT ON sales.customers TO dbx_fed;     -- one per object, or a quick loop over the schema
GRANT SELECT ON sales.orders    TO dbx_fed;
```
**3. Limit which schemas Databricks sees.** Databricks lists Oracle schemas via unqualified `ALL_USERS`
(readable by everyone), so by default it would show **every** schema in the database. A private synonym →
filtered view, owned by the login, scopes that to just the schemas you choose — for this login only.
Connected **as `dbx_fed`**:
```sql
CREATE OR REPLACE VIEW    all_users_filtered AS
  SELECT * FROM sys.all_users WHERE username IN ('SALES','DBX_FED');   -- the schemas to expose
CREATE OR REPLACE SYNONYM all_users FOR all_users_filtered;
-- This list is the single source of truth for scope: it drives both schema enumeration AND the
-- v_metadata helper view (step 4). To expose more schemas later, add them here only.
```
**4. Create 1 helper view** that re-exposes the comments/annotations so Databricks can read them. As `dbx_fed`:

> *Why this is needed:* federation mirrors table structure and data, but it does **not** copy Oracle's
> comments/annotations onto the Unity Catalog objects (federated tables show NULL comments). This view
> surfaces them inside a schema Databricks can see. It scopes itself from `all_users_filtered` (step 3), so
> the schemas you expose are defined in **one place**. One view carries both comments and annotations,
> tagged by a `kind` column.
```sql
CREATE OR REPLACE VIEW v_metadata AS
  -- table / view comments
  SELECT CAST('COMMENT' AS VARCHAR2(16)) AS kind, owner, table_name AS object_name,
         CAST(table_type AS VARCHAR2(23)) AS object_type,
         CAST(NULL AS VARCHAR2(128)) AS column_name, CAST(NULL AS VARCHAR2(128)) AS meta_name,
         comments AS meta_value
  FROM all_tab_comments
  WHERE comments IS NOT NULL AND table_type IN ('TABLE','VIEW')
    AND owner IN (SELECT username FROM all_users_filtered)
  UNION ALL
  -- column comments
  SELECT 'COMMENT', owner, table_name, NULL, column_name, NULL, comments
  FROM all_col_comments
  WHERE comments IS NOT NULL
    AND owner IN (SELECT username FROM all_users_filtered)
  UNION ALL
  -- annotations: Oracle 23ai+ only — DROP THIS BLOCK on 19c/21c (comments still sync).
  -- ALL_ANNOTATIONS_USAGE has no object-owner column, so join ALL_OBJECTS to scope by owner.
  SELECT 'ANNOTATION', o.owner, a.object_name, a.object_type, a.column_name, a.annotation_name, a.annotation_value
  FROM all_annotations_usage a
  JOIN all_objects o ON o.object_name = a.object_name AND o.object_type = a.object_type
  WHERE o.owner IN (SELECT username FROM all_users_filtered);
```
*(The login's own schema is in `all_users_filtered` too, but it owns no commented objects, so it contributes
nothing. Each sync still narrows to its own `source.schema`, so one `v_metadata` can serve several schemas.)*
**5. Verify** (as `dbx_fed`): `SELECT username FROM all_users;` → only your schemas;
`SELECT kind, count(*) FROM v_metadata GROUP BY kind;` → your comments and annotations.

**Notes**
- `ALL_*` and `ALL_USERS` are readable via `PUBLIC` — **no** `SELECT ANY DICTIONARY` / DBA role needed.
- The helper view must live in the login's schema (granting `SELECT` on a dictionary-based view in the app
  schema fails with `ORA-01720`). So in the config, `source.schema` = the app schema (the data) and
  `metadata.schema` = the login's schema (the helper view) — see §3.
- The login is **read-only**; authoring Oracle comments/annotations is done separately by whoever owns the data.
- *(Advanced)* you can split this into two logins/connections — a **data** login (the `SELECT` grants) and a
  **metadata** login (the helper view, or `SELECT_CATALOG_ROLE` + `DBA_*` with no data grants). Most setups
  use one login for both, as above.

---

## 2. Prerequisites — Unity Catalog / Databricks

### 2a. Connection + foreign catalog
Create a **UC connection** to Oracle and a **foreign catalog** over it (UI: Catalog → External Data, or SQL
`CREATE CONNECTION` / `CREATE FOREIGN CATALOG`). Use the login from §1. Connection options: host, port,
`service_name`, user, password. Leave encryption unset unless the Oracle server is configured for Native
Network Encryption. (Oracle federation connections are read-only by design.)

### 2b. Network
The Oracle listener (port 1521) must be reachable from **your Databricks workspace's serverless egress**.
Allowlist **your workspace's** serverless stable egress IPs on the Oracle host's firewall/security group.
These IPs are specific to your workspace + cloud region — find them in Databricks' *stable outbound IP*
documentation for your cloud (or your workspace's network settings). Don't reuse another workspace's IPs.

### 2c. Grants for the sync principal (the identity the jobs run as)
Run as a metastore admin / catalog owner. `<sync_principal>` is the user or **service principal** that
deploys and runs the bundle (use a service principal for production).
```sql
-- one-time: create the connection & foreign catalog (or an admin does this)
GRANT CREATE CONNECTION ON METASTORE TO `<sync_principal>`;
GRANT CREATE FOREIGN CATALOG ON CONNECTION `bg-oracle-ro` TO `<sync_principal>`;

-- read the federated source (data + helper view)
GRANT USE CONNECTION ON CONNECTION `bg-oracle-ro` TO `<sync_principal>`;
GRANT USE CATALOG, USE SCHEMA, SELECT ON CATALOG `bg-oracle-ro_catalog` TO `<sync_principal>`;

-- target catalog for the synced views/MVs/tables + tags + the control-plane schema `metadata_syn`
-- (the tool's bookkeeping tables — created by the `setup` job; see §7). Simplest if the principal owns it:
GRANT ALL PRIVILEGES ON CATALOG `bg` TO `<sync_principal>`;
-- ...or least-privilege instead of ALL PRIVILEGES:
-- GRANT USE CATALOG, CREATE SCHEMA, APPLY TAG ON CATALOG `bg` TO `<sync_principal>`;
-- GRANT USE SCHEMA, CREATE TABLE, CREATE MATERIALIZED VIEW, CREATE FUNCTION, SELECT, MODIFY, APPLY TAG
--   ON SCHEMA `bg`.`metadata_syn` TO `<sync_principal>`;   -- and likewise on each target schema
```
- **`APPLY TAG`** is required to set tags. If a tag key is a **governed tag**, assigning it may also need
  permission on that tag policy (see §2d).
- **Compute:** a **serverless SQL warehouse** and/or **serverless notebook** — required for materialized
  views and federated reads.

### 2d. Governed tags (heads-up)
A tag key can be a **governed tag** with an allowed-value list. Promoting an annotation to such a key with a
non-conforming value is **rejected** by UC; the tool catches this, leaves the annotation **registry-only**,
and reports it under `tags_blocked` (it never corrupts governance). To promote into a governed key, supply a
conforming value via the promotion policy's `uc_tag_key` / `value_mode`.

---

## 3. Configuration (`config/sync_config.yaml`)

This one file controls everything the tool does. It has two parts: **`syncs`** (one entry per "sync job" you
can run) and **`annotation_promotion`** (which annotations become tags). Edit it, then run the `setup` job
(§4) to load it. You run a sync by its `name`.

### The three blocks: `source`, `metadata`, `target`
Each sync points at three places:

| Block | What it is | Example |
|---|---|---|
| `source` | **Where the data is** — the Oracle app schema (federated) whose tables/columns you're describing. | `bg-oracle-ro_catalog.sales` |
| `metadata` | **Where the comments/annotations are read from** — the schema holding the `v_metadata` helper view. | `bg-oracle-ro_catalog.dbx_fed` |
| `target` | **Where the UC objects you're decorating live.** | `bg.sales` |

`metadata` points at the **`v_metadata` helper view** (created in §1 — it exposes comments + annotations as rows).
The engine reads comments/annotations from `{metadata.catalog}.{metadata.schema}.v_metadata`, and table/column
structure from `{source.catalog}.{source.schema}.<object>`. **Why it's separate from `source`:** the helper
view is built on Oracle's data dictionary, which Oracle won't let you expose from the app schema (`ORA-01720`),
so it must be **owned by the read-only login** in **its own schema** (e.g. `dbx_fed`), not the data's schema
(`sales`). Omit `metadata` and it defaults to `source`. *(Advanced: `metadata` can point at a different
connection/catalog — a separate "metadata" login — but most setups use one connection, just a different schema.)*

> **Default mode is metadata-only:** the tool applies metadata to UC objects that **already exist** (objects
> map by `source.schema` → `target.catalog`.`target.schema`) and creates nothing. To have it **create** the
> objects from the federated source, add `metadata_only: false` to the sync.

### Minimal — the smallest sync that works
```yaml
syncs:
  - name: sales
    source:   {connection: bg-oracle-ro, catalog: bg-oracle-ro_catalog, schema: sales}
    metadata: {catalog: bg-oracle-ro_catalog, schema: dbx_fed}
    target:   {catalog: bg, schema: sales}          # no `type` needed — metadata-only, auto-detected
```
That applies **comments** to the existing objects in `bg.sales`. Comments are on by default; annotations are
**off** until you enable them; `objects` defaults to *all*. Add `metadata_only: false` (and a `target.type`)
to create the objects instead.

### Full — every option
```yaml
syncs:
  - name: sales                                          # a label; you run this sync by name
    source:   {connection: bg-oracle-ro, catalog: bg-oracle-ro_catalog, schema: sales}
    metadata: {catalog: bg-oracle-ro_catalog, schema: dbx_fed}
    target:   {catalog: bg, schema: sales, type: VIEW}
    comments: true
    annotations: {enabled: true, apply_to_objects: true}
    metadata_only: true               # default; set false to CREATE the objects from the federated source
    objects: {default: all, exclude: [], overrides: []}
    hooks: {on_change_notebook: ""}

annotation_promotion:                                    # default = registry-only; opt names in here
  - {name: PII,            route: TAG, scope: COLUMN}
  - {name: Classification, route: TAG, scope: BOTH}
```

### Field reference
| Field | What it means | Typical value |
|---|---|---|
| `name` | Label for this sync; run it with `--sync_name <name>`. Add more list entries for more schemas/targets. | `sales` |
| `source.connection` | The UC **connection** (Oracle login) to read through. | `bg-oracle-ro` |
| `source.catalog` | The **foreign catalog** over that connection. | `bg-oracle-ro_catalog` |
| `source.schema` | The Oracle **application schema** that holds the data. | `sales` |
| `metadata.catalog` | Catalog holding the helper view — normally the same foreign catalog. | `bg-oracle-ro_catalog` |
| `metadata.schema` | Schema holding the `v_metadata` helper view = the **login's** schema (§1). Omit → defaults to `source.schema`. | `dbx_fed` |
| `target.catalog` | Databricks catalog for this sync. With `target.schema` it defines the schema-level mapping `source.schema` → `target.catalog`.`target.schema`. | `bg` |
| `target.schema` | Databricks schema the objects map to (each lands at `<lower(name)>` there). Omit → defaults to `source.schema`. | `sales` |
| `target.type` | *(optional in metadata-only mode — defaults to `VIEW`)* In **create** mode, what to build (see **Target type**). In **metadata-only** mode the real type is auto-detected, so this is just a routing/partition key — set it only if you map one Oracle schema to **several** targets in separate syncs. | *(omit)* |
| `metadata_only` | **Default `true`** = **don't create anything**; apply comments/tags to **pre-existing** UC objects in place. The engine **auto-detects** each target's real type (table / MV / view) and uses the correct DDL, so **one sync can cover a mix** and view definitions are preserved. Set **`false`** to have the tool **create** the objects from the federated source. | `true` |
| `comments` | Sync table/column comments. | `true` |
| `annotations.enabled` | Read annotations into the **registry** (Oracle 23ai+). | `true` |
| `annotations.apply_to_objects` | Also apply *promoted* annotations as **tags**. `false` = registry only. | `true` |
| `objects.default` | `all` = sync every object in the schema. | `all` |
| `objects.exclude` | Object names to skip. | `[]` |
| `objects.overrides` | Per-object renames/remaps — see **Renaming objects**. | `[]` |
| `hooks.on_change_notebook` | Workspace path of a notebook to run after a sync that changed something (receives `run_id`, `sync_name`). Blank = none. | `""` |
| `hooks.genie_space_id` | *(optional)* Genie space the `genie_push` hook updates — see §10. Blank = hook no-ops. | `""` |

### Target type — which to choose
| `target.type` | What you get | Use when |
|---|---|---|
| **`VIEW`** *(start here)* | A live, zero-copy view over the Oracle object. Always current; no storage, no refresh. | Most cases. |
| **`MATERIALIZED_VIEW`** | A stored, refreshable copy in Databricks. | You want Databricks-side speed, or to keep working if Oracle is offline. |
| **`TABLE`** | A one-time snapshot copy (CTAS). Not auto-refreshed. | Static / reference data that rarely changes. |

### Which annotations become tags (`annotation_promotion`)
By default **nothing** becomes a tag — every annotation is captured in the registry (queryable), but tags are
opt-in, name by name (tags drive governance/access, so this is deliberate). For each name you want promoted:
- **`name`** — the Oracle annotation name, e.g. `PII`.
- **`route`** — `TAG` (apply as a UC tag), `COMMENT` (apply as a comment on the object), or `REGISTRY` (default; registry only).
- **`scope`** — `TABLE`, `COLUMN`, or `BOTH` (where it applies).
- **`uc_tag_key`** *(optional)* — the UC tag key to use (defaults to the annotation name, sanitized).
- **`value_mode`** *(optional)* — `asis` (default) or `flag` (presence only, empty value).

Names you don't list still land in the registry — they just don't become tags. (Governed tags can reject
non-conforming values; those stay registry-only and are reported — see §2d.)

### Mapping a schema to a UC catalog + schema (with optional object-level overrides)
The **schema-level** mapping is just `source.schema` + `target.catalog` + `target.schema`: every object in the
Oracle schema maps to `target.catalog`.`target.schema`.`<lower(name)>`.
```yaml
syncs:
  - name: hr                                  # Oracle HR -> main.people  (all existing objects)
    source:   {connection: bg-oracle-ro, catalog: bg-oracle-ro_catalog, schema: hr}
    metadata: {catalog: bg-oracle-ro_catalog, schema: dbx_fed}
    target:   {catalog: main, schema: people}   # metadata-only default; no type needed
    objects:  {default: all}
```
Add **object-level overrides** to send specific objects somewhere else — each points one object at **any**
catalog/schema/name (overrides win over the schema default):
```yaml
    objects:
      default: all
      overrides:
        - {oracle_object: EMPLOYEES, target_catalog: main, target_schema: people, target_object: employee}
        - {oracle_object: PAYROLL,   target_catalog: secure, target_schema: hr_secure, target_object: payroll}
```
The `setup` job loads both the schema default and the overrides into the mapping table **declaratively** —
re-running reconciles them (remove one from the YAML and it's removed). Extra knobs on an override:
`name_prefix`, `name_suffix`, `case_mode` (`lower`/`upper`/`preserve`).

---

## 4. Deploy & run

There are **two ways** to run this — pick whichever fits. They use the **same notebooks**; the bundle just
packages them for one-command, repeatable deploys. **Previewing first is the same in both:** run a sync with
`apply=false` (it prints what *would* change and writes nothing), then `apply=true` to apply.

### Option A — Run from files in a workspace *(no bundle / no CLI; simplest)*
1. **Import the files** into a workspace folder (e.g. `/Workspace/Users/<you>/oracle_uc_sync/`): the three
   notebooks from `src/` and `config/sync_config.yaml`. Use **Workspace → Import**, a **Git folder** pointed
   at this repo, or `databricks workspace import`.
2. **Run `00_setup`** — open it, set the `yaml_path` widget to the workspace path of your `sync_config.yaml`,
   and **Run all**. (Creates the control plane and loads your config; re-run after any config change.)
3. **Run `sync_oracle_metadata`** — set the `sync_name` widget (e.g. `sales`) and `apply`
   (`false` = preview, `true` = apply), and **Run all**.
4. *(Optional)* **`demo_oracle_metadata_sync`** — end-to-end demo; set `sync_name` + `secret_scope`.
5. **To schedule:** Workflows → Create job → a **notebook task** on `sync_oracle_metadata`, with job
   parameters `sync_name` and `apply=true`, on the schedule you want.

Everything is in the workspace UI — no CLI needed.

### Option B — Databricks Asset Bundle (DAB) *(optional; repeatable / multi-env / CI-CD)*
A DAB packages the code + jobs + config so you deploy with one CLI command and promote dev→prod consistently.
It creates four jobs: **`setup`**, **`sync`** (parameterized `sync_name`/`apply`, schedulable), **`pipeline`**
(one task that does setup+sync from `setup`/`sync_name`/`apply` params — see §5 of GETTING_STARTED.md), and **`demo`**.
```
oracle-uc-metadata-sync/
├── databricks.yml          # the bundle: variables, targets (dev/prod), jobs
├── README.md / ROADMAP.md
├── demo_genie_pipeline.py  # optional: create a Genie space for the §10 integration
├── config/sync_config.yaml # the sync config (§3)
└── src/                    # 00_setup, sync_oracle_metadata, run_pipeline, demo_oracle_metadata_sync, hooks/ (§10)
```
```bash
databricks auth login --host https://<your-workspace>
# edit databricks.yml (targets.dev.workspace.host, var.metadata_schema) and config/sync_config.yaml (§3)
databricks bundle validate -t dev
databricks bundle deploy   -t dev                                       # uploads code + creates the 3 jobs
databricks bundle run setup -t dev
databricks bundle run sync  -t dev --params sync_name=sales,apply=false   # PREVIEW: writes nothing
databricks bundle run sync  -t dev --params sync_name=sales,apply=true    # APPLY
# when ready: in the `pipeline` job (the scheduled one) set apply=true and unpause (ships PAUSED, apply=false)
# promote to prod:  databricks bundle deploy -t prod   (set run_as to a service principal)
```
`databricks bundle destroy -t dev` removes everything the bundle created.

---

## 5. Hooks & change feed
Every run records what changed; query it:
```sql
SELECT changed_at, kind, change_type, oracle_object, oracle_column, meta_key, old_value, new_value
FROM bg.metadata_syn.sync_change_feed WHERE run_id = '<run_id>';
```
- Set `hooks.on_change_notebook` to have the tool run a notebook after applying (it passes `run_id` +
  `sync_name`); the hook reads the feed and acts (e.g. notify, kick off downstream jobs).
- Each run also returns a JSON summary (`run_id`, counts, `tags_blocked`, `affected_objects`).

---

## 6. Reference — how it works
*(You don't need this to set up or run the tool.)*

```
  Oracle (source of truth)                   Databricks / Unity Catalog
 ┌─────────────────────────┐   federation   ┌──────────────────────────────────────────────┐
 │ tables/cols             │  (read-only)   │ foreign catalog  ──► sync engine             │
 │  • COMMENT ON           │ ─────────────► │  helper view         (sync_oracle_metadata)  │
 │  • ANNOTATIONS (23ai+)  │                │  v_metadata          │                       │
 │ one helper view exposes │                │  (kind=COMMENT|      ▼                       │
 │  them as queryable rows │                │   ANNOTATION)    diff vs state               │
 └─────────────────────────┘                │              comments → objects              │
        ▲  writes (DDL) happen              │              annotations → registry (+ tags) │
        │  out-of-band (direct JDBC,        │              change log + feed + hook        │
        │  NOT through UC)                  └──────────────────────────────────────────────┘
```

**Per run (one `sync_config` entry):**
1. **Read** Oracle comments/annotations through the `v_metadata` helper view (via federation).
2. **Resolve** each Oracle object to its UC name (`resolve_uc_name`: schema default + per-object overrides).
3. **Comments** → apply to the UC object. The exact DDL depends on the target type (see §8's comparison) —
   views are rebuilt with comments baked in; materialized views and tables get comments applied in place.
4. **Annotations** → **always** written to the `oracle_annotations` **registry**. Then, only if
   `apply_to_objects=true`, the policy-approved subset is applied as **UC tags** (with key sanitization,
   value truncation, and a 50-tags/securable cap; overflow stays registry-only). Index/domain annotations
   stay registry-only (nothing to tag in UC).
5. **Change-track** (diff vs `metadata_state`, append `metadata_change_log`, expose `sync_change_feed`), then
   fire the optional hook.

**Two-tier model.** The registry holds *everything*; only a curated, opt-in subset becomes UC tags. *Which
annotation names* promote is your policy (default = registry-only); *which objects* get a promoted tag is
automatic (the mapping).

**Version-aware.** Oracle 23ai/26ai → comments **and** annotations; pre-23ai → comments only (auto-detected).

---

## 7. Reference — components

**This repo** — `src/` notebooks + `config/`:
| Artifact | Role |
|---|---|
| `config/sync_config.yaml` | The sync config (§3) |
| `src/00_setup.py` | Idempotent: creates the control plane (tables, resolver, default mapping) + loads the YAML |
| `src/sync_oracle_metadata.py` | **The sync engine** — run per `sync_name` (dry-run by default) |
| `src/run_pipeline.py` | **One-notebook runner** — props `setup`/`sync_name`(or `ALL`)/`apply`; orchestrates setup + sync in one task |
| `src/demo_oracle_metadata_sync.py` | End-to-end demo that uses the engine (version-aware) |
| `src/hooks/genie_push.py` + `update_genie_space.py` | *(optional)* the Genie integration hook + its SDK — see §10 |
| `demo_genie_pipeline.py` | *(optional)* create a Genie space over the synced tables — see §10 |

**Control plane** — tables/views created in `bg.metadata_syn`:
| Object | Role |
|---|---|
| `sync_config` | One row per sync job (loaded from YAML) |
| `oracle_to_uc_mapping` + `resolve_uc_name()` | Oracle→UC object mapping (schema default + per-object overrides) |
| `annotation_promotion_policy` | Which annotation names promote beyond the registry (default REGISTRY) |
| `oracle_annotations` | **Registry** of all annotations |
| `metadata_state` | Current synced state (`kind` = COMMENT \| TAG) |
| `metadata_change_log` | Append-only audit log |
| `sync_change_feed` (view) | Unified change feed for downstream consumers |

---

## 8. Reference — limits, gaps & target types
| Dimension | Oracle annotation | UC tag | Tool handling |
|---|---|---|---|
| Value length | 4000 bytes | 1000 chars | truncates to 1000 |
| Count / securable | ~unlimited | 50 (+1000 col-tags/table) | space-aware cap → overflow registry-only |
| Tag key chars | identifier | `. , - = / :` disallowed, no edge spaces | sanitizes keys |
| Object types | tables, views, MVs, **indexes, domains** | tables, columns, views, MVs, volumes, functions, models | index/domain → registry-only |
| JSON value | text | text | passes through (not parsed) |

**Target type comparison:**
| target_type | data | comment apply | refresh |
|---|---|---|---|
| `VIEW` | zero-copy / live | `CREATE OR REPLACE VIEW` (rebuilt; tags re-asserted) | n/a (always live) |
| `MATERIALIZED_VIEW` | materialized copy | in-place `ALTER MATERIALIZED VIEW` | MV refresh |
| `TABLE` | static CTAS snapshot | in-place `ALTER TABLE` (fewest restrictions) | **none — point-in-time** |

**Behavioral notes:**
- `CREATE OR REPLACE VIEW` **drops** the view's tags → the tool applies comments first, then re-asserts tags.
  It's **Genie-safe** (same UC name → grants + Genie associations preserved).
- A source object may be an Oracle **table or view** (`TABLE` target = a snapshot of either).
- **Multiple Oracle sources:** the control plane (`bg.metadata_syn`) keys on the Oracle **schema name** — both
  the `oracle_annotations` registry and `resolve_uc_name` — not on the sync. So two sources whose Oracle
  schemas share a name (e.g. both `SALES`) collide in `apply` mode: the registry partition is overwritten and
  per-object mapping overrides leak between syncs. Run each such source against its **own control-plane schema**
  (the `metadata_schema` job parameter, e.g. `bg.metadata_syn_b`). A single source is unaffected.
  *(Verified with a 19c source running alongside a 23ai one.)*

See `ROADMAP.md` for current capabilities and the prioritized backlog (e.g. PK/FK constraints,
metadata-only mode).

---

## 9. Troubleshooting
| Symptom | Cause / fix |
|---|---|
| `FAILED_JDBC.CONNECTION` | Oracle host firewall doesn't allow the Databricks serverless egress IPs (§2b); or wrong creds/encryption setting |
| `Tag value … is not an allowed value for tag policy key …` | governed tag — set a conforming value in the promotion policy, or leave it registry-only |
| A view didn't get built / columns missing | source object not visible to the connection login (missing `SELECT`), or the `v_metadata` helper view missing |
| `__materialization_*` / `event_log_*` tables under an MV schema | normal materialized-view internals — ignore |
| Annotation step "skipped" | source is pre-23ai (no ANNOTATION rows in `v_metadata`) — expected; comments still sync |

---

## 10. Optional integration — keep a Genie space in sync
A post-sync hook can push the freshly-synced metadata into a **Genie space** so the room stays current as
Oracle changes. Two halves:

- **Comments → automatic.** The sync already writes table/column comments onto the UC objects, and Genie reads
  UC comments directly — nothing extra to push. (`CREATE OR REPLACE VIEW` is Genie-safe: same UC name keeps
  the room's table attached.)
- **Joins/instructions → explicit, from annotations.** Oracle annotations carry the join criteria. The hook
  reads them from the registry and writes the room's join definitions via the Genie API.

### `RELATED_TO` annotation convention
Put a **column-level** annotation named `RELATED_TO` on the foreign-key column. The annotated table/column is
the **left** side; the value names the **right** side:
```
RELATED_TO = '<right_table>.<right_col>[;rt=<MANY_TO_ONE|ONE_TO_MANY|ONE_TO_ONE|MANY_TO_MANY>]'
```
Example — on `ORDERS.CUSTOMER_ID` (Oracle 23ai+):
```sql
ALTER TABLE orders MODIFY (customer_id
  ANNOTATIONS (ADD OR REPLACE RELATED_TO 'customers.customer_id;rt=MANY_TO_ONE'));
```
The hook resolves both sides to their UC names (via `resolve_uc_name`) and emits a join
`ON orders.customer_id = customers.customer_id`. Relationship defaults to `MANY_TO_ONE`. `RELATED_TO` is a
**neutral** relationship annotation — Oracle code can read it for its own purposes too; it isn't Genie-specific.
Governance annotations (`PII`, `Classification`, …) are untouched by the hook and keep flowing to the
registry/tags as usual.

### Wiring
*Prerequisite: run a sync first (§4) so the target tables (e.g. `bg.sales.*`) exist — the room is created over them.*
1. **Create the room** over the synced tables and note its `space_id`:
   ```bash
   python3 demo_genie_pipeline.py --warehouse-id <id>      # seeds vocab + sample questions, no joins yet
   ```
2. **Point the sync at it** — in `config/sync_config.yaml`, on the `sales` sync:
   ```yaml
   hooks: {on_change_notebook: "hooks/genie_push", genie_space_id: "<space_id>"}
   ```
   Re-run `setup` to load it.
3. **From now on**, any `apply=true` run that changes something fires `src/hooks/genie_push.py`, which rebuilds
   the room's joins from `RELATED_TO` annotations (idempotent; replaces only the joins section, leaving
   human-authored instructions/examples/sample questions intact).

### Components
| Artifact | Role |
|---|---|
| `src/hooks/genie_push.py` | The hook — registry `RELATED_TO` → Genie join specs; called via `on_change_notebook` |
| `src/hooks/update_genie_space.py` | Vendored Genie-space SDK (`GenieSpaces` client + config helpers) |
| `demo_genie_pipeline.py` | One-shot: create the room over `bg.sales` with seed config |

> The Genie **Create/Update** APIs are **Beta** (Nov 2025) — treat the request/response shape as subject to
> change. The hook degrades safely: if a sync has no `genie_space_id`, it no-ops.
