# Databricks notebook source
# MAGIC %md
# MAGIC # Oracle → UC metadata sync (config-driven: comments + annotations)
# MAGIC Reads `bg.metadata_syn.sync_config` for the chosen `sync_name` and syncs:
# MAGIC - **Comments** → views (`CREATE OR REPLACE VIEW`) or MVs (in-place `COMMENT ON` / `ALTER MATERIALIZED VIEW`).
# MAGIC - **Annotations** → always captured to the `oracle_annotations` registry; promoted to **UC tags** only when
# MAGIC   `apply_annotations_to_objects=true`, per `annotation_promotion_policy` (default REGISTRY), space-aware (50/securable).
# MAGIC
# MAGIC Unified change tracking (`metadata_state` / `metadata_change_log`, kind=COMMENT|TAG); fires an optional
# MAGIC post-sync hook notebook; returns a JSON summary. Reads one `v_metadata` helper view (kind=COMMENT|ANNOTATION);
# MAGIC pre-23ai-safe (annotation leg no-ops when the view has no ANNOTATION rows).

# COMMAND ----------

dbutils.widgets.text("sync_name", "sales", "Sync name (from sync_config)")
dbutils.widgets.dropdown("apply", "false", ["true", "false"], "Apply changes")
dbutils.widgets.text("metadata_schema", "bg.metadata_syn", "Control-plane schema")
SYNC = dbutils.widgets.get("sync_name")
MS = dbutils.widgets.get("metadata_schema")
do_apply = dbutils.widgets.get("apply") == "true"

import uuid, json, re
from datetime import datetime, timezone
from collections import defaultdict, Counter
run_id = str(uuid.uuid4()); now = datetime.now(timezone.utc)

cfg = spark.sql(f"SELECT * FROM {MS}.sync_config WHERE name='{SYNC}' AND enabled").collect()
assert cfg, f"no enabled sync_config row named '{SYNC}'"
c = cfg[0]
src_data_fqn = f"`{c.source_catalog}`.{c.source_schema}"          # DATA: real app schema, federated directly
meta_cat = c.metadata_catalog or c.source_catalog
meta_sch = c.metadata_schema  or c.source_schema
src_meta_fqn = f"`{meta_cat}`.{meta_sch}"                          # METADATA: the owner-filtered v_metadata helper view
owner = c.source_schema.upper()                                   # Oracle owner of the synced objects
# target.type is optional. In metadata_only mode the real type is auto-detected per object; target_type is
# only a routing/partition key here, so it defaults to VIEW when unset. (In create mode it picks what to build.)
tt = (c.target_type or "VIEW").upper()
# Default mode is metadata-only (decorate pre-existing objects). NULL/unset -> metadata_only.
metadata_only = True if c.metadata_only is None else bool(c.metadata_only)
resolver = f"{MS}.resolve_uc_name"
exclude = set(o.upper() for o in (c.object_exclude or []))
print(f"sync={SYNC} data={c.source_catalog}.{c.source_schema} meta={meta_cat}.{meta_sch} owner={owner} "
      f"target_type={tt} comments={c.sync_comments} annotations={c.sync_annotations} "
      f"apply_tags={c.apply_annotations_to_objects} metadata_only={metadata_only} "
      f"mode={'APPLY' if do_apply else 'DRY RUN'} run_id={run_id}")

def sql_str(s): return "'" + (s or "").replace("'", "''") + "'"
def sanitize_key(name):  # UC tag keys: no . , - = / : and no edge spaces
    return re.sub(r'[.,=/:\-]', '_', name or '').strip()

history = []   # COMMENT changes: (kind, target_type, schema, object, column, level, meta_key, change_type, old, new)
state_rows = []  # COMMENT state: (kind, target_type, schema, object, column, level, uc_name, meta_key, value, is_active, first_seen, last_changed)
stmts = []     # comment apply statements: (label, sql)
rebuilt_objects = set()
tag_ops, tag_carry = [], []   # tag plan (applied + recorded from real results in the apply cell)
changed_c = []                # comment-changed objects (set by the comments leg)
registry_changed = False      # any annotation registry row new/changed/removed (set by the annotation leg)

def prev_state(kind):
    return {(r.oracle_object, r.oracle_column, r.meta_key): r for r in spark.table(f"{MS}.metadata_state")
            .where(f"sync_name='{SYNC}' AND target_type='{tt}' AND kind='{kind}' AND is_active").collect()}

def read_optional(sql):
    try: return spark.sql(sql).collect()
    except Exception as e:
        print(f"  (skipped, source lacks it: {str(e).splitlines()[0][:80]})"); return None

# ordered foreign columns per object — read the LIVE foreign-table schema (federated
# system.information_schema is lazily/incompletely populated, so don't rely on it).
_col_cache = {}
def get_cols(obj):
    if obj not in _col_cache:
        try:
            _col_cache[obj] = [f.name for f in spark.table(f"`{c.source_catalog}`.`{c.source_schema}`.`{obj}`").schema.fields]
        except Exception as e:
            print(f"  WARN: cannot read columns for {obj}: {str(e).splitlines()[0][:80]}"); _col_cache[obj] = []
    return _col_cache[obj]

def uc_of(obj):
    return spark.sql(f"SELECT {resolver}('{c.source_schema}','{obj}','{tt}') n").collect()[0].n

# Effective object type for choosing apply DDL. In create mode it's the sync's target_type. In metadata_only
# mode we AUTO-DETECT the existing object's real type (TABLE / MATERIALIZED_VIEW / VIEW) so one sync can cover
# a mix and never picks the wrong verb. Bookkeeping (state/registry) still keys on the sync-level target_type.
_type_cache = {}
def effective_type(uc):
    if not metadata_only or not uc:
        return tt
    if uc in _type_cache:
        return _type_cache[uc]
    et = tt
    try:
        cat, sch, tbl = uc.split(".")[-3], uc.split(".")[-2], uc.split(".")[-1]
        rows = spark.sql(f"SELECT table_type FROM {cat}.information_schema.tables "
                         f"WHERE table_schema='{sch}' AND table_name='{tbl}'").collect()
        if rows:
            raw = (rows[0].table_type or "").upper()
            et = "VIEW" if raw == "VIEW" else ("MATERIALIZED_VIEW" if raw == "MATERIALIZED_VIEW" else "TABLE")
    except Exception as e:
        print(f"  type-detect {uc}: defaulting to {tt} ({str(e).splitlines()[0][:60]})")
    _type_cache[uc] = et
    return et

# COMMAND ----------

# MAGIC %md ## 1. Comments leg

# COMMAND ----------

if c.sync_comments:
    cmts = read_optional(f"SELECT upper(object_name) o, upper(column_name) col, meta_value cmt FROM {src_meta_fqn}.v_metadata WHERE kind='COMMENT' AND meta_value IS NOT NULL AND upper(owner)='{owner}'")
    cmts = cmts or []
    cur = {}   # (object, column, '') -> comment ; column_name NULL = table/view-level
    for r in cmts:
        if r.o not in exclude: cur[(r.o, r.col, '')] = r.cmt
    prev = prev_state('COMMENT')
    for k, v in cur.items():
        obj, col, mk = k; p = prev.get(k)
        if p is None: ch, fs, lc = 'NEW', now, now
        elif (p.value or '') != (v or ''): ch, fs, lc = 'CHANGED', p.first_seen_at, now
        else: ch, fs, lc = 'UNCHANGED', p.first_seen_at, p.last_changed_at
        lvl = 'TABLE' if col is None else 'COLUMN'
        if ch in ('NEW', 'CHANGED'): history.append(('COMMENT', tt, c.source_schema.upper(), obj, col, lvl, mk, ch, (p.value if p else None), v))
        state_rows.append(('COMMENT', tt, c.source_schema.upper(), obj, col, lvl, uc_of(obj), mk, v, True, fs, lc))
    for k, p in prev.items():
        if k not in cur:
            obj, col, mk = k
            history.append(('COMMENT', tt, p.oracle_schema, obj, col, p.level, mk, 'REMOVED', p.value, None))
            state_rows.append(('COMMENT', tt, p.oracle_schema, obj, col, p.level, p.uc_name, mk, p.value, False, p.first_seen_at, now))

    changed_c = sorted({h[3] for h in history if h[0] == 'COMMENT'})
    tab_cmt = {k[0]: v for k, v in cur.items() if k[1] is None}
    col_cmt = {(k[0], k[1]): v for k, v in cur.items() if k[1] is not None}
    # Object creation — SKIPPED in metadata_only mode (objects already exist; we only decorate them).
    if not metadata_only:
        # Source may be an Oracle TABLE or VIEW — `SELECT * FROM {src}.{obj}` works for both.
        for obj in changed_c:
            uc = uc_of(obj); cols = get_cols(obj)
            if not uc or not cols: continue
            sel = ", ".join(f"{cc} AS {cc.lower()}" for cc in cols)
            if tt == 'VIEW':   # zero-copy view; column comments baked into the column list (rebuild)
                lines = [cc.lower() + (f" COMMENT {sql_str(col_cmt[(obj,cc.upper())])}" if (obj,cc.upper()) in col_cmt else "") for cc in cols]
                cc_clause = f"\nCOMMENT {sql_str(tab_cmt[obj])}" if obj in tab_cmt else ""
                stmts.append((f"REPLACE VIEW {uc}", f"CREATE OR REPLACE VIEW {uc} (\n  " + ",\n  ".join(lines) + f"\n){cc_clause}\nAS SELECT * FROM {src_data_fqn}.{obj}"))
                rebuilt_objects.add(uc)
            elif tt == 'MATERIALIZED_VIEW':   # refreshable materialized copy
                stmts.append((f"ensure MV {uc}", f"CREATE MATERIALIZED VIEW IF NOT EXISTS {uc} AS SELECT {sel} FROM {src_data_fqn}.{obj}"))
            else:   # TABLE: static CTAS snapshot (created once; data not auto-refreshed)
                stmts.append((f"ensure TABLE {uc}", f"CREATE TABLE IF NOT EXISTS {uc} AS SELECT {sel} FROM {src_data_fqn}.{obj}"))
    # Comments applied IN PLACE: MV/TABLE always; in metadata_only, ALL types incl VIEW (never rebuilt).
    if tt != 'VIEW' or metadata_only:
        for h in [x for x in history if x[0] == 'COMMENT']:
            _, _, _, obj, col, lvl, _, ch, _, newc = h; uc = uc_of(obj)
            if not uc: continue
            if metadata_only and not spark.catalog.tableExists(uc):
                print(f"  metadata_only: target {uc} not found — skipped"); continue
            et = effective_type(uc)   # detected real type in metadata_only; sync target_type otherwise
            txt = "" if ch == 'REMOVED' else (newc or "")
            if lvl == 'TABLE':
                stmts.append((f"comment {uc}", f"COMMENT ON {'VIEW' if et=='VIEW' else 'TABLE'} {uc} IS {sql_str(txt)}"))
            elif et == 'VIEW':   # view columns decorate in place — no rebuild, original DDL preserved
                stmts.append((f"comment {uc}.{col.lower()}", f"COMMENT ON COLUMN {uc}.{col.lower()} IS {sql_str(txt)}"))
            else:
                col_verb = "MATERIALIZED VIEW" if et == 'MATERIALIZED_VIEW' else "TABLE"
                stmts.append((f"comment {uc}.{col.lower()}", f"ALTER {col_verb} {uc} ALTER COLUMN {col.lower()} COMMENT {sql_str(txt)}"))
    print(f"comments: {Counter(h[7] for h in history if h[0]=='COMMENT')}" + (" [metadata_only]" if metadata_only else ""))
else:
    print("comments leg disabled")

# COMMAND ----------

# MAGIC %md ## 2. Annotations — registry (always) + tag promotion (optional, space-aware)

# COMMAND ----------

registry_rows = []
if c.sync_annotations:
    anns = read_optional(f"SELECT object_name o, object_type otype, column_name col, meta_name an, meta_value av FROM {src_meta_fqn}.v_metadata WHERE kind='ANNOTATION' AND upper(owner)='{owner}'")
    if anns is None:
        print("annotation leg: source has no v_metadata — skipping gracefully")
    else:
        # registry: ALL annotations (uc_name only for taggable object types: TABLE/VIEW/MATERIALIZED VIEW)
        taggable = {'TABLE', 'VIEW', 'MATERIALIZED VIEW'}
        prev_reg = {(r.oracle_object, r.oracle_column, r.annotation_name): r for r in spark.table(f"{MS}.oracle_annotations")
                    .where(f"oracle_schema='{c.source_schema.upper()}' AND is_active").collect()}
        cur_ann = {}
        for r in anns:
            obj = (r.o or '').upper()
            if obj in exclude: continue
            col = (r.col.upper() if r.col else None)
            uc = uc_of(obj) if r.otype in taggable else None
            cur_ann[(obj, col, r.an)] = (r.otype, col, r.av, uc)
        for key, (otype, col, av, uc) in cur_ann.items():
            obj, col2, an = key; p = prev_reg.get(key)
            if p is None or (p.annotation_value or '') != (av or ''): registry_changed = True
            fs = p.first_seen_at if p else now
            lc = now if (p is None or (p.annotation_value or '') != (av or '')) else p.last_changed_at
            lvl = 'TABLE' if col2 is None else 'COLUMN'
            registry_rows.append((SYNC, c.source_schema.upper(), obj, col2, lvl, otype, an, av, uc, True, fs, lc, now))
        for key, p in prev_reg.items():
            if key not in cur_ann:
                registry_changed = True
                registry_rows.append((SYNC, p.oracle_schema, p.oracle_object, p.oracle_column, p.level, p.object_type,
                                      p.annotation_name, p.annotation_value, p.uc_name, False, p.first_seen_at, now, now))
        print(f"annotations in registry: {sum(1 for r in registry_rows if r[9])} active (registry_changed={registry_changed})")

        # tag promotion — built as per-key ops; recorded from ACTUAL apply results (governed-tag safe)
        if c.apply_annotations_to_objects:
            policy = {r.annotation_name.upper(): r for r in spark.table(f"{MS}.annotation_promotion_policy")
                      .where("upper(route)='TAG'").collect()}
            desired = defaultdict(dict)   # (object, column) -> {tag_key: value}
            for key, (otype, col, av, uc) in cur_ann.items():
                obj, col2, an = key; pol = policy.get(an.upper())
                if not pol or not uc: continue
                scope = (pol.scope or 'BOTH').upper(); lvl = 'TABLE' if col2 is None else 'COLUMN'
                if scope != 'BOTH' and scope != lvl: continue
                tag_key = sanitize_key(pol.uc_tag_key or an)
                val = '' if (pol.value_mode or 'asis') == 'flag' else (av or '')
                desired[(obj, col2)][tag_key] = val[:1000]
            for sec, tags in list(desired.items()):
                if len(tags) > 50:
                    keep = dict(list(tags.items())[:50])
                    print(f"  cap: {sec} has {len(tags)}>50 tags; {sorted(set(tags)-set(keep))} stay registry-only")
                    desired[sec] = keep
            prev_tag = prev_state('TAG')
            cur_tag = {(o, c2, tk): tv for (o, c2), tags in desired.items() for tk, tv in tags.items()}
            # tag_ops rows: (op, uc, col2, lvl, object, tag_key, new_value, old_value, first_seen, change_type)
            for k, v in cur_tag.items():
                obj, col2, tk = k; uc = uc_of(obj); p = prev_tag.get(k); lvl = 'TABLE' if col2 is None else 'COLUMN'
                forced = uc in rebuilt_objects
                if p is None or forced: tag_ops.append(('SET', uc, col2, lvl, obj, tk, v, (p.value if p else None), (p.first_seen_at if p else now), 'NEW'))
                elif (p.value or '') != (v or ''): tag_ops.append(('SET', uc, col2, lvl, obj, tk, v, p.value, p.first_seen_at, 'CHANGED'))
                else: tag_carry.append((obj, col2, lvl, uc, tk, v, p.first_seen_at, p.last_changed_at))  # unchanged, already on object
            for k, p in prev_tag.items():
                if k not in cur_tag:
                    obj, col2, tk = k; tag_ops.append(('UNSET', p.uc_name, col2, p.level, obj, tk, None, p.value, p.first_seen_at, 'REMOVED'))
            print(f"tags planned: {Counter(o[9] for o in tag_ops)} (+{len(tag_carry)} unchanged)")
            for o in tag_ops: print(f"   {o[9]:8} {o[1]}{('.'+o[2].lower()) if o[2] else ''} [{o[5]}]={o[6]!r}")
        else:
            print("apply_annotations_to_objects=false → registry only, no tags")
else:
    print("annotation leg disabled")

# COMMAND ----------

# MAGIC %md ## 3. Apply (persist state/registry, run statements) unless dry-run

# COMMAND ----------

def tag_sql(op, uc, col2, tag_key, value):
    et = effective_type(uc)   # detected real type in metadata_only; sync target_type otherwise
    if col2 is None:
        verb = {'MATERIALIZED_VIEW': 'MATERIALIZED VIEW', 'TABLE': 'TABLE'}.get(et, 'VIEW')
        target = f"ALTER {verb} {uc}"
    elif et == 'MATERIALIZED_VIEW':
        target = f"ALTER MATERIALIZED VIEW {uc} ALTER COLUMN {col2.lower()}"
    else:   # TABLE and VIEW both use ALTER TABLE ... ALTER COLUMN for column tags
        target = f"ALTER TABLE {uc} ALTER COLUMN {col2.lower()}"
    return f"{target} SET TAGS ({sql_str(tag_key)} = {sql_str(value)})" if op == 'SET' \
        else f"{target} UNSET TAGS ({sql_str(tag_key)})"

print(f"comment statements: {len(stmts)} | tag ops: {len(tag_ops)}")
for lbl, s in stmts: print("\n" + "="*70 + f"\n-- {lbl}\n{s}")

comment_failed, tag_blocked, tag_state_final, tag_history = [], [], [], []

if not do_apply:
    print("\nDRY RUN — nothing written.")
else:
    from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType
    import pyspark.sql.functions as F
    SCHEMA_U = c.source_schema.upper()
    # 0) ensure target schema(s) exist (objects may route to several via overrides)
    tschemas = {".".join(u.split(".")[:2]) for u in
                ({uc_of(o) for o in changed_c} | {op[1] for op in tag_ops} | {t[3] for t in tag_carry}) if u}
    for sch in tschemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {sch}")
    # 1) registry (always, full snapshot for this source schema)
    if c.sync_annotations and registry_rows:
        rsch = StructType([StructField(n, t) for n, t in [
            ("sync_name",StringType()),("oracle_schema",StringType()),("oracle_object",StringType()),("oracle_column",StringType()),
            ("level",StringType()),("object_type",StringType()),("annotation_name",StringType()),("annotation_value",StringType()),
            ("uc_name",StringType()),("is_active",BooleanType()),("first_seen_at",TimestampType()),("last_changed_at",TimestampType()),("last_synced_at",TimestampType())]])
        (spark.createDataFrame(registry_rows, rsch).write.format("delta").mode("overwrite")
            .option("replaceWhere", f"oracle_schema='{SCHEMA_U}'").saveAsTable(f"{MS}.oracle_annotations"))
    # 2) comments (run first; view rebuilds wipe tags, which the tag step then re-asserts)
    for lbl, s in stmts:
        try: spark.sql(s)
        except Exception as e: comment_failed.append((lbl, str(e).splitlines()[0][:160]))
    # 3) tags — one key at a time; record from ACTUAL result (governed-tag rejections -> blocked/registry-only)
    for (obj, col2, lvl, uc, tk, v, fs, lcg) in tag_carry:
        tag_state_final.append(('TAG', tt, SCHEMA_U, obj, col2, lvl, uc, tk, v, True, fs, lcg))
    for (op, uc, col2, lvl, obj, tk, nv, ov, fs, ch) in tag_ops:
        try:
            spark.sql(tag_sql(op, uc, col2, tk, nv))
            if op == 'SET':
                tag_state_final.append(('TAG', tt, SCHEMA_U, obj, col2, lvl, uc, tk, nv, True, fs, now))
                tag_history.append(('TAG', tt, SCHEMA_U, obj, col2, lvl, tk, ch, ov, nv))
            else:
                tag_history.append(('TAG', tt, SCHEMA_U, obj, col2, lvl, tk, 'REMOVED', ov, None))
        except Exception as e:
            tag_blocked.append((f"{uc}{('.'+col2.lower()) if col2 else ''} [{tk}]", str(e).splitlines()[0][:160]))
    # 4) write unified state (COMMENT from state_rows + TAG from results) and change log
    all_state = state_rows + tag_state_final
    all_hist = history + tag_history
    ssch = StructType([StructField(n,t) for n,t in [
        ("kind",StringType()),("target_type",StringType()),("oracle_schema",StringType()),("oracle_object",StringType()),
        ("oracle_column",StringType()),("level",StringType()),("uc_name",StringType()),("meta_key",StringType()),
        ("value",StringType()),("is_active",BooleanType()),("first_seen_at",TimestampType()),("last_changed_at",TimestampType())]])
    if all_state:
        sdf = spark.createDataFrame(all_state, ssch).withColumn("sync_name", F.lit(SYNC)).withColumn("last_synced_at", F.lit(now))
        for kind in {r[0] for r in all_state}:
            (sdf.where(F.col("kind")==kind).write.format("delta").mode("overwrite")
                .option("replaceWhere", f"sync_name='{SYNC}' AND target_type='{tt}' AND kind='{kind}'").saveAsTable(f"{MS}.metadata_state"))
    if all_hist:
        hsch = StructType([StructField(n,t) for n,t in [
            ("kind",StringType()),("target_type",StringType()),("oracle_schema",StringType()),("oracle_object",StringType()),
            ("oracle_column",StringType()),("level",StringType()),("meta_key",StringType()),("change_type",StringType()),
            ("old_value",StringType()),("new_value",StringType())]])
        (spark.createDataFrame(all_hist, hsch).withColumn("run_id",F.lit(run_id)).withColumn("changed_at",F.lit(now))
            .withColumn("sync_name",F.lit(SYNC)).write.mode("append").saveAsTable(f"{MS}.metadata_change_log"))
    print(f"\ncomments applied: {len(stmts)-len(comment_failed)}/{len(stmts)} | tags applied: {len(tag_history)} | blocked: {len(tag_blocked)}")
    for lbl, e in comment_failed: print(f"  COMMENT FAILED {lbl}: {e}")
    for lbl, e in tag_blocked: print(f"  TAG BLOCKED {lbl}: {e}")

# COMMAND ----------

# MAGIC %md ## 4. Post-sync hook + summary

# COMMAND ----------

summary = {"run_id": run_id, "sync": SYNC, "target_type": tt, "applied": do_apply,
           "comment_changes": sum(1 for h in history if h[0]=='COMMENT'),
           "tag_changes": (len(tag_history) if do_apply else len(tag_ops)),
           "tags_blocked": [b[0] for b in tag_blocked],
           "comment_failures": [f[0] for f in comment_failed],
           "registry_active": sum(1 for r in registry_rows if r[9]) if c.sync_annotations else 0,
           "registry_changed": registry_changed, "metadata_only": metadata_only,
           "affected_objects": sorted({h[3] for h in history} | {o[4] for o in tag_ops})}
if do_apply and (history or tag_history or registry_changed) and (c.on_change_notebook or "").strip():
    try:
        hook_res = dbutils.notebook.run(c.on_change_notebook, 300, {"run_id": run_id, "sync_name": SYNC, "metadata_schema": MS})
        summary["hook_result"] = hook_res
    except Exception as e:
        summary["hook_error"] = str(e)[:200]
print(json.dumps(summary, indent=2))
dbutils.notebook.exit(json.dumps(summary))
