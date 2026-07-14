# Roadmap & gap analysis — Oracle → Unity Catalog metadata sync

Where the project stands today and what an Oracle customer running this against a real estate would want next.

## Implemented today
- Config-driven (YAML → `sync_config`) engine; per-sync selection.
- **Comments** (table + column) → UC **VIEW**, **MATERIALIZED_VIEW**, or **TABLE** (static CTAS snapshot).
- **Annotations** (Oracle 23ai+) → **registry of everything** + policy-based promotion to **UC tags**
  (default-deny, key sanitization, value truncation, 50-tag/securable cap, governed-tag aware).
- Source = Oracle **table or view**; version-aware (26ai + pre-23ai comments-only).
- Object → UC **mapping** (schema default + per-object overrides incl. cross catalog/schema; prefix/suffix/case).
- Unified **change tracking** (`metadata_state`, `metadata_change_log`, `sync_change_feed`) + first_seen/last_changed.
- **Hooks** (post-sync notebook) + JSON run summary. Packaged as a **DAB** (this repo).
- **Annotation → Genie pipeline**: parses the JSON annotation format (`foreign_key`, `sql_expression_*`,
  `sample_query_*`) into a portable Genie config — joins (role-playing dims aliased), filters/expressions,
  example queries (Oracle→UC schema rewrite), and instructions. Tolerant of invalid JSON; unit-tested.
  See [ANNOTATION_PARSING.md](ANNOTATION_PARSING.md).
- **Production deployment model** (validated): a **read-only service account that owns nothing** — `ALL_*`
  helper views scoped via `USER_SYNONYMS`, synonyms as the curated access layer, and a private `all_users`
  synonym → filtered view to cap UC's schema enumeration (validated **31 → 2** schemas). See README §3.

## Gaps (prioritized) — grounded observations
| # | Gap | Why it matters | Evidence | Tier |
|---|---|---|---|---|
| 1 | **Constraints: PK/FK/NOT NULL → UC informational constraints** | Powers Genie joins, BI, lineage | synced UC objects have **0 constraints**; Oracle PKs not propagated | **1** |
| 2 | **Operationalize: scheduled jobs, multi-env, alerting** (this DAB is step 1) | Run unattended dev→prod with failure alerts | was manual notebook runs | **1** |
| 3 | **Full object coverage + scale** | Real schemas have 100s–1000s of objects; sync *all*, not just metadata-bearing ones | only objects with comments/annotations are mirrored; `uc_of`/schema reads are per-object | **1** |
| 4 | **Lifecycle: source drop/rename + state↔reality reconciliation** | Retire UC objects when source goes away; avoid drift | engine only adds/updates; comment state can be written even if apply fails | **2** |
| 5 | **Refresh strategy for CTAS tables** | "Static" data still changes; need scheduled re-snapshot / MERGE | TABLE target is created once, never refreshed | **2** |
| 6 | **Tag-driven governance: column masks / row filters from tags** (map Oracle VPD/OLS) | Regulated shops want PII/Classification to *enforce*, not just label | tags set; no masking/row-filter policies created | **2** |
| 7 | **Type fidelity** | NUMBER/DATE/LOB surprises | `customer_id` (Oracle NUMBER PK) → `decimal(38,10)` | **2** |
| 8 | **Domain-driven promotion (23ai)** | A domain's annotation fans out to every column using it (high-confidence) | not built | 3 |
| 9 | **Observability dashboard** over change-feed + registry | Stewards want a UI, not SQL | data exists; no dashboard | 3 |
| 10 | **Multi-source hardening** — key the registry **and** the resolver by `sync_name`/connection, not just Oracle schema; least-priv user; SP; dry-run approval gate | Two sources that share an Oracle schema name collide in the shared control plane | **Verified 2026-06-16 (19c RDS test):** a second sync whose Oracle schema was also named `SALES` (a) overwrote the other source's registry partition (`oracle_annotations` writes `replaceWhere oracle_schema='SALES'`), and (b) a per-object mapping override leaked into the first sync (`resolve_uc_name` keys on schema+object+target_type, with no notion of *which* sync). Workaround today: give each source its **own `metadata_schema`** (separate control plane). | **2** |
| 11 | ~~**Metadata-only mode**~~ **(DONE 2026-06-25)** — applies comments/tags to **pre-existing** UC objects (Lakeflow Connect, hand-built, etc.) without creating them. Set `metadata_only: true` + explicit `objects.overrides`. Verified: one FC table → existing VIEW/MV/TABLE all decorated in place, view definition preserved. *(Still open: separate data-vs-metadata Oracle connections for separation of duties.)* | was: engine created **and** applied in one pass | **done** |

## Positioning
This is a **metadata/governance** layer — it complements, not replaces, **Lakeflow Connect** (Databricks'
Oracle data ingestion/CDC). If the need is continuously-replicated *data*, that's Lakeflow Connect; this
layer rides on top to bring **descriptions, classifications, and (next) constraints** into UC.

## Recommended next order
1. **PK/FK constraint sync** (`USER_CONSTRAINTS` → UC informational constraints) — most visible win, amplifies Genie.
2. **Full object coverage + scale** (sync all objects; batch the per-object work).
3. **Lifecycle/reconciliation** (drops, renames, drift).
4. **Tag-driven masking** (highest-impact governance for regulated customers).
