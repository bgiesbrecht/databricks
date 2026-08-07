#!/usr/bin/env python3
"""
ir_to_sparksql.py — Forward-generate Databricks SparkSQL notebooks from the
dtsx IR (produced by dtsx_to_ir.py), using an LLM only for the parts that
genuinely need judgment.

Division of labor (the whole point of IR + LLM):
  DETERMINISTIC (this file, no LLM):
    - topologically order the data-flow nodes from the IR edges
    - assemble notebook cells + temp-view naming
    - build a GROUNDED prompt per node that pins the facts the LLM must not
      invent (lookup join keys, no-match disposition, source/target tables)
  LLM (pluggable `generate` callable):
    - translate each node's T-SQL / transform semantics into Spark SQL,
      honoring the pinned no-match behavior

The LLM backend is injected so the core is testable offline. `--dry-run`
prints the grounded prompts and does no LLM call at all — run that first to
see exactly what the model is (and isn't) asked to decide.

Usage:
    # See the grounded prompts without any LLM (recommended first run):
    python3 ir_to_sparksql.py out/ir/custom_lesson1.json --dry-run

    # Generate via a Databricks serving endpoint (works with the CLI auth here):
    python3 ir_to_sparksql.py out/ir/custom_lesson1.json \
        --backend databricks --endpoint databricks-claude-3-7-sonnet \
        --profile e2-demo-field-eng --out out/gen/

    # Generate via the Anthropic SDK (needs ANTHROPIC_API_KEY or `ant auth login`):
    python3 ir_to_sparksql.py out/ir/custom_lesson1.json --backend anthropic --out out/gen/
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Callable

# A generate() takes (system_prompt, user_prompt) and returns model text.
Generate = Callable[[str, str], str]

SYSTEM_PROMPT = """You are a precise SSIS-to-Databricks migration engineer.
You convert ONE SSIS data-flow component into a Spark SQL statement BODY for a \
Databricks notebook cell. You are given a grounded fact sheet extracted from the \
package; treat those facts as authoritative and do not invent table names, columns, \
join keys, or filters that are not present.

Rules:
- Output ONLY the SQL body. No prose, no markdown fences.
- Do NOT emit `CREATE OR REPLACE TEMP VIEW` yourself — the harness wraps your output.
  * For a SOURCE or TRANSFORM: emit a single SELECT statement (the harness turns it
    into a temp view).
  * For a TABLE TARGET: emit an `INSERT INTO <table> SELECT ...` statement.
- The upstream component's output is available as a temp view named exactly as given.
- Preserve the lookup's semantics EXACTLY as specified by `joinType` in the fact sheet
  (the harness has already resolved fail_component / error-redirect / ignore into the
  correct JOIN type — do not second-guess it):
    * INNER JOIN -> non-matching rows are dropped (fail_component with no error branch).
    * LEFT JOIN  -> non-matching rows are RETAINED with NULLs in the looked-up columns,
                    so a downstream match/error branch can split them.
- When the fact sheet says this node consumes a MATCH output, keep only matched rows
  (the looked-up key is NOT NULL). When it consumes an ERROR / NO-MATCH output, keep
  only the non-matching rows (the looked-up key IS NULL).
- Convert T-SQL identifier quoting [x] to Spark backticks `x`.
- Do not add columns the fact sheet does not mention."""


def _normalize_sql(sql: str) -> str:
    """Strip markdown fences and any leading CREATE...VIEW...AS the model added anyway,
    plus a trailing semicolon — the harness owns the wrapper."""
    s = sql.strip()
    if s.startswith("```"):
        s = "\n".join(l for l in s.splitlines() if not l.strip().startswith("```")).strip()
    m = re.match(r"(?is)^\s*create\s+or\s+replace\s+temp(orary)?\s+view\s+\S+\s+as\s+", s)
    if m:
        s = s[m.end():].strip()
    return s.rstrip(";").strip()


def topo_order(nodes: list[dict], edges: list[dict]) -> list[dict]:
    """Order nodes so every node comes after the ones feeding it (Kahn's algorithm).

    Falls back to source-first document order if the graph has a cycle or
    dangling edges (SSIS data flows are DAGs, but IR extraction can be partial).
    """
    by_name = {n["name"]: n for n in nodes}
    indeg = {n["name"]: 0 for n in nodes}
    adj: dict[str, list[str]] = {n["name"]: [] for n in nodes}
    for e in edges:
        f, t = e.get("from"), e.get("to")
        if f in by_name and t in by_name:
            adj[f].append(t)
            indeg[t] += 1

    # Seed with in-degree-0 nodes, preferring declared sources for stable output.
    queue = [n["name"] for n in nodes if indeg[n["name"]] == 0]
    queue.sort(key=lambda name: (by_name[name].get("role") != "source", name))
    ordered, seen = [], set()
    while queue:
        name = queue.pop(0)
        if name in seen:
            continue
        seen.add(name)
        ordered.append(by_name[name])
        for nxt in adj[name]:
            indeg[nxt] -= 1
            if indeg[nxt] == 0:
                queue.append(nxt)

    if len(ordered) != len(nodes):  # cycle or unreachable — fall back
        return sorted(nodes, key=lambda n: n.get("role") != "source")
    return ordered


def view_name(node_name: str) -> str:
    """Deterministic, SQL-safe temp-view name for a component."""
    safe = "".join(c if c.isalnum() else "_" for c in node_name).strip("_")
    return f"v_{safe.lower()}"


def incoming_edges(node: dict, edges: list[dict]) -> list[dict]:
    return [e for e in edges if e.get("to") == node["name"]]


def upstream_of(node: dict, edges: list[dict]) -> list[str]:
    return [view_name(e["from"]) for e in incoming_edges(node, edges)]


def _port_kind(port: str | None) -> str:
    """Classify an SSIS output-port name into match / error / plain."""
    p = (port or "").lower()
    if "error" in p or "no match" in p or "nomatch" in p:
        return "error"
    if "match" in p:  # "Lookup Match Output"
        return "match"
    return "plain"


def _lookup_join_type(node: dict, edges: list[dict], all_nodes: list[dict]) -> str:
    """Resolve the effective JOIN type for a Lookup.

    SSIS subtlety: an error/no-match REDIRECT overrides `fail_component`. If the
    lookup has an outgoing error/no-match edge, non-matching rows are diverted
    (not failed), so the join must be LEFT to retain them for the branch.
    """
    logic = node.get("logic") or {}
    nmb = logic.get("noMatchBehavior")
    has_error_branch = any(
        e.get("from") == node["name"] and _port_kind(e.get("fromPort")) == "error"
        for e in edges
    )
    if nmb in ("redirect_to_error_output", "redirect_to_no_match_output", "ignore_failure"):
        return "LEFT JOIN"
    if has_error_branch:  # error-redirect wiring overrides a fail_component setting
        return "LEFT JOIN"
    return "INNER JOIN"


def ground_node(node: dict, edges: list[dict], nodes: list[dict]) -> dict | None:
    """Build the grounded fact sheet for one node.

    Returns a dict {facts, kind} where kind is 'select' (wrap as a temp view) or
    'statement' (emit verbatim, e.g. INSERT). None if the node needs no cell.
    """
    role = node.get("role")
    logic = node.get("logic") or {}
    ins = incoming_edges(node, edges)
    ups = [view_name(e["from"]) for e in ins]
    kind = "statement" if role == "target" else "select"

    facts: list[str] = [
        f"Component: {node['name']}",
        f"SSIS type: {node['componentType']}  (role: {role})",
    ]

    # Which upstream output port feeds THIS node?
    by_name = {n["name"]: n for n in nodes}
    for e in ins:
        up_node = by_name.get(e["from"], {})
        up_type = up_node.get("componentType")
        port = e.get("fromPort")
        upv = view_name(e["from"])
        if up_type == "Microsoft.ConditionalSplit":
            # The fromPort IS the branch name the split assigned to _split_branch.
            facts.append(
                f"Upstream view: {upv} via CONDITIONAL-SPLIT branch '{port}'. This node "
                f"receives ONLY that branch's rows — you MUST filter with "
                f"`WHERE `_split_branch` = '{port}'` (that column was added upstream)."
            )
        else:
            pk = _port_kind(port)
            tag = {"match": "  (consumes the MATCH output — keep matched rows only)",
                   "error": "  (consumes the ERROR/NO-MATCH output — keep non-matching rows only)",
                   "plain": ""}[pk]
            facts.append(f"Upstream view: {upv} via port '{port}'{tag}")

    if node["componentType"] == "Microsoft.Lookup":
        facts += [
            f"Reference query (T-SQL): {logic.get('referenceQuery')}",
            f"Parameterized form (shows the join predicate): {logic.get('parameterizedQuery')}",
            f"Input column(s) used as the join key: {logic.get('joinInputColumns')}",
            f"noMatchBehavior: {logic.get('noMatchBehavior')}",
            f"joinType: {_lookup_join_type(node, edges, nodes)}   <-- USE THIS EXACT JOIN TYPE",
            f"cacheType: {logic.get('cacheType')}",
            "Emit a SELECT that joins the upstream view to the reference query on the "
            "join key(s) and adds the looked-up column(s).",
        ]
    elif role == "source" and node["componentType"] == "Microsoft.FlatFileSource":
        facts += [
            f"Flat-file output columns: {logic.get('outputColumns')}",
            "This is a SOURCE: emit a SELECT over the landed flat-file data "
            "(assume an external/temp table or a read of the raw file already exists). "
            "Project ONLY the data columns; drop SSIS error-plumbing columns such as "
            "'Flat File Source Error Output Column', 'ErrorCode', 'ErrorColumn'.",
        ]
    elif node["componentType"] == "Microsoft.OLEDBSource":
        facts += [
            f"Source table (OpenRowset): {logic.get('openRowset')}",
            f"Source SQL query (if any): {logic.get('sqlCommand')}",
            "This is a SOURCE: emit a SELECT. If a source SQL query is given, translate it "
            "to Spark SQL (a `?` is a runtime parameter — replace with a clearly-named "
            "placeholder or a sensible default and add a comment). Otherwise SELECT * from "
            "the source table. Convert T-SQL [x] quoting to Spark backticks.",
        ]
    elif node["componentType"] == "Microsoft.DerivedColumn":
        derivs = "; ".join(f"{d['column']} = {d['expression']}"
                           for d in logic.get("derivations", []))
        facts += [
            f"Derived columns (SSIS expression syntax): {derivs}",
            "This is a DERIVED COLUMN: emit `SELECT src.*, <expr> AS <col>, ...` from the "
            "upstream view. Translate SSIS expression syntax to Spark SQL: the ternary "
            "`cond ? a : b` becomes `CASE WHEN cond THEN a ELSE b END`; `!=` stays `!=`; "
            "`GETDATE()` becomes `current_timestamp()`; string concat `+` becomes `||`. "
            "Keep the derived column names exactly.",
        ]
    elif node["componentType"] == "Microsoft.ConditionalSplit":
        conds = logic.get("conditions", [])
        lines_c = "; ".join(f"output '{c['output']}' WHEN {c['condition']}" for c in conds)
        facts += [
            f"Conditional split routes (in evaluation order): {lines_c}",
            f"Default output (rows matching no condition): {logic.get('defaultOutput')}",
            "This is a CONDITIONAL SPLIT: it has ONE upstream view and multiple named "
            "downstream branches. Emit a SELECT that adds a routing column, e.g. "
            "`SELECT src.*, CASE WHEN <cond1> THEN '<out1>' WHEN <cond2> THEN '<out2>' "
            "ELSE '<default>' END AS `_split_branch``. Each downstream node filters on "
            "`_split_branch = '<its branch>'`. Translate SSIS expression syntax (== -> =, "
            "&& -> AND, || -> OR) to Spark SQL. Evaluate conditions in the given order.",
        ]
    elif node["componentType"] == "Microsoft.OLEDBCommand":
        facts += [
            f"Per-row SQL command (SSIS runs this once per input row; `?` are bound to "
            f"input columns in order): {logic.get('sqlCommand')}",
            "This is an OLE DB COMMAND (row-by-row DML — typically an UPDATE/DELETE). "
            "Convert to a SET-BASED Spark SQL statement: e.g. an `UPDATE <table> SET ... "
            "WHERE key = ?` over each row becomes a single `MERGE INTO <table> USING "
            "<upstream view> ON <table>.<key> = src.<key> WHEN MATCHED THEN UPDATE SET ...`. "
            "Map each `?` to the corresponding upstream column by position. Convert "
            "GETDATE() -> current_timestamp() and T-SQL [x] -> `x`.",
        ]
        kind = "statement"  # DML, not a view
    elif node["componentType"] == "Microsoft.Aggregate":
        gb = logic.get("groupBy", [])
        aggs = "; ".join(f"{a['function']}(...) AS {a['column']}" for a in logic.get("aggregates", []))
        facts += [
            f"GROUP BY columns: {gb}",
            f"Aggregate output columns (function -> alias): {aggs}",
            "This is an AGGREGATE: emit `SELECT <group-by cols>, <FUNC(col) AS alias>, ... "
            "FROM <upstream view> GROUP BY <group-by cols>`. COUNT_DISTINCT -> "
            "COUNT(DISTINCT col). If an aggregate's source column isn't obvious from the "
            "name, use the group-by columns and add a comment to confirm the measure.",
        ]
    elif node["componentType"] == "Microsoft.UnionAll":
        facts += [
            f"Unified output columns: {logic.get('outputColumns')}",
            "This is a UNION ALL of ALL the upstream views listed above. Emit "
            "`SELECT <cols> FROM <view1> UNION ALL SELECT <cols> FROM <view2> ...`, "
            "aligning each input's columns to the unified output column order.",
        ]
    elif node["componentType"] == "Microsoft.Multicast":
        facts += [
            "This is a MULTICAST: it fans one input to several identical downstream "
            "branches with NO transformation. Emit simply `SELECT * FROM <upstream view>`; "
            "downstream nodes each read this view.",
        ]
    elif node["componentType"] == "Microsoft.MergeJoin":
        keys = logic.get("joinKeysByInput", {})
        facts += [
            f"Join type: {logic.get('joinType')}   <-- USE THIS EXACT JOIN TYPE",
            f"Join keys per input (input name -> key columns): {keys}",
            f"Merged output columns: {logic.get('outputColumns')}",
            "This is a MERGE JOIN of the TWO upstream views listed above. Emit "
            "`SELECT <output cols> FROM <left view> <joinType> <right view> ON <left.key = "
            "right.key>` using the join keys (paired by position across the two inputs). "
            "SSIS pre-sorts inputs for merge join; Spark does not need that — join directly.",
        ]
    elif node["componentType"] == "Microsoft.Sort":
        sk = ", ".join(f"{s['column']}{' DESC' if s['descending'] else ''}"
                       for s in logic.get("sortKeys", []))
        facts += [
            f"Sort keys: {sk}",
            "This is a SORT. Emit `SELECT * FROM <upstream view> ORDER BY <sort keys>`. "
            "NOTE: if this sort exists only to feed a downstream Merge Join, it is "
            "unnecessary in Spark and may be treated as a passthrough `SELECT *` — but "
            "when in doubt, keep the ORDER BY to preserve behavior.",
        ]
    elif node["componentType"] == "Microsoft.DataConvert":
        convs = "; ".join(
            f"{c['column']} = CAST(<source> AS {c['targetType']}"
            + (f"({c['length']})" if c.get('length') and 'DECIMAL' not in c['targetType'] else "")
            + ")" for c in logic.get("conversions", []))
        facts += [
            f"Type conversions (new column = cast of a source column): {convs}",
            "This is a DATA CONVERSION: emit `SELECT src.*, CAST(<source col> AS <type>) AS "
            "<new col>, ...` from the upstream view. Infer each source column from the new "
            "column's name (SSIS often appends a suffix like '_Numeric'); add a comment if "
            "the source is ambiguous. Drop the SSIS ErrorCode/ErrorColumn plumbing columns.",
        ]
    elif node["componentType"] == "Microsoft.SCD":
        facts += [
            f"Business (natural) key column(s): {logic.get('businessKeys')}",
            f"Changing attributes: {logic.get('changingAttributes')}",
            f"Fixed attributes: {logic.get('fixedAttributes')}",
            f"History handling: {logic.get('historyType')}  "
            f"(type1_inplace = overwrite; type2_historical = expire old row + insert new)",
            f"SCD outputs present: {logic.get('outputs')}",
            "This is a SLOWLY CHANGING DIMENSION. Emit ONE set-based `MERGE INTO "
            "<dimension table> AS tgt USING <upstream view> AS src ON tgt.<business key> = "
            "src.<business key>`. For type1_inplace: `WHEN MATCHED AND (any changing attr "
            "differs) THEN UPDATE SET <changing attrs>` and `WHEN NOT MATCHED THEN INSERT "
            "(...)`. For type2_historical: `WHEN MATCHED AND (changing attr differs) THEN "
            "UPDATE SET <expiry columns e.g. current-flag/end-date>` plus a separate INSERT "
            "of the new version for changed + brand-new keys. Add a comment noting the "
            "dimension table name must be set (it comes from the downstream destination).",
        ]
        kind = "statement"
    elif node["componentType"] == "Microsoft.RowCount":
        facts += [
            f"Target variable that receives the row count: {logic.get('variableName')}",
            "This is a ROW COUNT: it passes rows through unchanged while counting them into "
            "an SSIS variable. Emit a passthrough `SELECT * FROM <upstream view>`. The count "
            "itself is `.count()` on the resulting view if the pipeline needs it — add a "
            "one-line comment showing how (`# rows = spark.table('<view>').count()`).",
        ]
    elif node["componentType"] == "Microsoft.UnPivot":
        piv = logic.get("pivotedColumns", [])
        pairs = ", ".join(f"'{p['keyValue']}', `{p['sourceColumn']}`" for p in piv)
        facts += [
            f"Passthrough (identity) columns: {logic.get('passthroughColumns')}",
            f"New key column name: {logic.get('keyColumn')}",
            f"New value column name: {logic.get('valueColumn')}",
            f"Pivoted source columns -> key label: {[(p['sourceColumn'], p['keyValue']) for p in piv]}",
            "This is an UNPIVOT (wide -> long). Emit Spark SQL using the STACK function: "
            f"`SELECT <passthrough cols>, STACK({len(piv)}, {pairs}) AS "
            f"(`{logic.get('keyColumn')}`, `{logic.get('valueColumn')}`) FROM <upstream view>`.",
        ]
    elif node["componentType"] == "Microsoft.Pivot":
        facts += [
            f"Set (group/identity) columns: {logic.get('setColumns')}",
            f"Pivot-key column (its values become new columns): {logic.get('pivotKeyColumns')}",
            f"Value (measure) column: {logic.get('valueColumns')}",
            f"Output columns (include the generated pivoted columns): {logic.get('outputColumns')}",
            "This is a PIVOT (long -> wide). Emit Spark SQL: `SELECT * FROM <upstream view> "
            "PIVOT (<agg>(<value col>) FOR <pivot-key col> IN (<the pivoted values, inferred "
            "from the output column names>))` grouping by the set columns. Add a comment if "
            "the pivoted value list must be confirmed.",
        ]
    elif node["componentType"] == "Microsoft.FuzzyLookup":
        facts += [
            f"Reference table: {logic.get('referenceTable')}",
            f"Input column(s) matched fuzzily: {logic.get('joinInputColumns')}",
            f"Minimum similarity threshold (0..1): {logic.get('minSimilarity')}",
            "This is a FUZZY LOOKUP (approximate string match) — Spark has no built-in "
            "fuzzy join. Emit a best-effort LEFT JOIN on the reference table and add a "
            "⚠️ comment that exact equality is a placeholder for fuzzy matching; suggest "
            "`levenshtein()`/`soundex()` or a similarity UDF with the given threshold as "
            "the real implementation. Do NOT silently pretend it is an exact match.",
        ]
    elif node["componentType"] == "Microsoft.Cache":
        facts += [
            f"Cached columns: {logic.get('cachedColumns')}",
            "This is a CACHE TRANSFORM: in SSIS it loads its input into an in-memory cache "
            "reused by downstream cache-connected Lookups. In Spark this is simply a temp "
            "view (optionally CACHE'd). Emit `SELECT * FROM <upstream view>` — the caching "
            "is handled by Spark; add a comment noting downstream lookups read this view.",
        ]
    elif node["componentType"] == "Microsoft.ManagedComponentHost":
        # Handled via presolve (deterministic, code-preserving) — see presolve_script().
        facts += [
            f"Script language: {logic.get('scriptLanguage')}",
            f"Declared output columns: {logic.get('outputColumns')}",
            "This is a SCRIPT COMPONENT (arbitrary C#/VB) — NOT auto-translated.",
        ]
    elif node["componentType"] == "Microsoft.FlatFileDestination":
        facts += [
            "This is a FLAT FILE DESTINATION: SSIS wrote the input stream to a file. In "
            "Spark, write the upstream view to a Volume path. Emit a comment-led statement: "
            "`-- write to file`  then `INSERT OVERWRITE DIRECTORY '/Volumes/main/default/"
            "ssis_output/<name>' USING csv OPTIONS(header true) SELECT * FROM <upstream "
            "view>`. Note the output path must be set by the user.",
        ]
        kind = "statement"
    elif role == "target":
        facts += [
            f"Destination table: {logic.get('openRowset')}",
            "This is a TABLE TARGET: emit `INSERT INTO <destination> SELECT ...` from the "
            "single upstream view. Do NOT wrap it in a view. Select only the destination's "
            "data columns — do not carry SSIS error-plumbing columns into the target.",
        ]
    else:
        facts.append("No specialized extractor for this component type; translate "
                     "conservatively from the type and upstream view.")
    return {"facts": "\n".join(f"- {f}" for f in facts), "kind": kind}


def presolve_script(node: dict) -> str:
    """Deterministic, code-PRESERVING passthrough for a Script Component. We never
    fabricate a translation of arbitrary C#/VB — instead we pass rows through and embed
    the original script + a clear MANUAL-REVIEW banner for a human to port."""
    logic = node.get("logic") or {}
    ups = [view_name(e["from"]) for e in []]  # filled by caller context; see build_prompts
    lang = logic.get("scriptLanguage") or "unknown"
    ro = logic.get("readOnlyVariables") or ""
    rw = logic.get("readWriteVariables") or ""
    out_cols = logic.get("outputColumns") or []
    src = logic.get("sourceCode")
    banner = [
        f"-- ⚠️ MANUAL REVIEW REQUIRED: SSIS Script Component '{node['name']}' ({lang}).",
        "--    Arbitrary script logic is NOT auto-translated. Rows are passed through "
        "unchanged below.",
        f"--    Declared output columns to reproduce: {out_cols}",
    ]
    if ro:
        banner.append(f"--    ReadOnly variables: {ro}")
    if rw:
        banner.append(f"--    ReadWrite variables: {rw}")
    if src:
        banner.append("--    Original script (first 4000 chars) preserved for porting:")
        banner += ["--    " + line for line in src.splitlines()]
    return "\n".join(banner) + "\n-- Passthrough (replace with ported logic):\nSELECT * FROM {UPSTREAM}"


def iter_dataflow_nodes(pkg: dict):
    """Yield (dataflow_name, nodes, edges, loop) for every data flow.

    `loop` is the enclosing ForeachLoop spec (or None) — a data flow nested inside a
    ForEach *File* loop runs once per file, which we convert to one set-based glob read.
    """
    for ex in pkg["controlFlow"]["executables"]:
        ex_loop = ex.get("loop")  # present when ex is a ForeachLoop
        # A ForeachLoop's own dataFlow is None; its body lives in children.
        if ex.get("dataFlow"):
            yield ex["dataFlow"]["name"], ex["dataFlow"]["nodes"], ex["dataFlow"]["edges"], ex_loop
        for child in ex.get("children", []):
            df = child.get("dataFlow")
            if df:
                yield df["name"], df["nodes"], df["edges"], ex_loop


def _win_to_glob(folder: str | None, pattern: str | None) -> str:
    """Turn an SSIS Windows folder + FileSpec into a portable glob for read_files().

    The original folder is a Windows path that won't exist on Databricks, so we keep
    only the leaf folder name and prefix a Volume placeholder the user edits once.
    """
    leaf = ""
    if folder:
        leaf = folder.replace("\\", "/").rstrip("/").split("/")[-1]
    pat = pattern or "*"
    base = f"/Volumes/main/default/ssis_input/{leaf}".rstrip("/")
    return f"{base}/{pat}"


def presolve_source_glob(node: dict, loop: dict) -> str:
    """Deterministic SELECT for a FlatFileSource that ran inside a ForEach-file loop.

    SSIS looped the data flow once per matching file; Spark reads them all at once.
    read_files() glob-expands the pattern, so every file the loop would have processed
    is covered in a single set-based read. `_metadata.file_path` preserves per-file
    lineage (the equivalent of the loop's file-name variable)."""
    cols = [c for c in (node.get("logic") or {}).get("outputColumns", [])
            if c and c not in ("Flat File Source Error Output Column", "ErrorCode", "ErrorColumn")]
    glob = _win_to_glob(loop.get("folder"), loop.get("filePattern"))
    proj = ",\n  ".join(f"`{c}`" for c in cols) if cols else "*"
    return (
        f"-- ForEach-file loop over '{loop.get('filePattern')}' -> one set-based glob read\n"
        f"SELECT\n  {proj},\n  _metadata.file_path AS `_source_file`\n"
        f"FROM read_files(\n"
        f"  '{glob}',\n"
        f"  format => 'csv', header => true, inferSchema => true\n"
        f")"
    )


def order_executables(pkg: dict) -> list[dict]:
    """Order top-level executables by precedence constraints (control-flow edges).

    SSIS precedence constraints define task execution order (`from` runs before `to`).
    We topo-sort the executables so the generated notebook's cells run in the same order
    — e.g. an Execute SQL Task watermark query before the Data Flow that uses it.
    """
    execs = pkg["controlFlow"]["executables"]
    constraints = pkg["controlFlow"].get("precedenceConstraints", [])
    by_name = {e["name"]: e for e in execs}

    # Precedence refs look like 'Package\\Execute SQL Task'; map the leaf to the exec.
    def _leaf(ref):
        return (ref or "").split("\\")[-1]

    indeg = {e["name"]: 0 for e in execs}
    adj: dict[str, list[str]] = {e["name"]: [] for e in execs}
    for c in constraints:
        f, t = _leaf(c.get("from")), _leaf(c.get("to"))
        if f in by_name and t in by_name:
            adj[f].append(t)
            indeg[t] += 1
    queue = [n for n in indeg if indeg[n] == 0]
    ordered, seen = [], set()
    while queue:
        n = queue.pop(0)
        if n in seen:
            continue
        seen.add(n)
        ordered.append(by_name[n])
        for nxt in adj[n]:
            indeg[nxt] -= 1
            if indeg[nxt] == 0:
                queue.append(nxt)
    if len(ordered) != len(execs):  # cycle/partial — fall back to document order
        return execs
    return ordered


def ground_task(ex: dict) -> dict:
    """Grounded prompt/output for a control-flow TASK (Execute SQL, Execute Package, ...)."""
    task = ex["task"]
    t = task["type"]
    if t == "execute_sql":
        return {"kind": "statement",
                "facts": "\n".join([
                    f"- Control-flow task: Execute SQL Task '{ex['name']}'",
                    f"- SQL statement (T-SQL): {task.get('sql')}",
                    "- Emit this as a Spark SQL statement. Translate T-SQL to Spark SQL "
                    "(`[x]`->`` `x` ``, GETDATE()->current_timestamp()). A `?` is a runtime "
                    "parameter — replace with a named placeholder and add a comment. If it "
                    "returns a single value (a watermark), note it can be captured with "
                    "spark.sql(...).first()[0].",
                ])}
    if t == "execute_package":
        pkgname = task.get("packageName")
        child = pkgname or "<child_package>"
        note = (f"child package '{pkgname}'" if pkgname
                else "a project-reference child package (name not in this file — set it)")
        return {"kind": "presolved",
                "presolved": (
                    f"-- Execute Package Task '{ex['name']}' called {note}.\n"
                    f"-- In Databricks, run the converted child notebook, e.g.:\n"
                    f"-- dbutils.notebook.run('{child}', 0)")}
    if t == "script_task":
        return {"kind": "presolved",
                "presolved": (
                    f"-- ⚠️ MANUAL REVIEW: SSIS Script Task '{ex['name']}' "
                    f"({task.get('scriptLanguage') or 'script'}). Arbitrary code is not "
                    f"auto-translated — port the logic here.")}
    if t == "execute_process":
        return {"kind": "presolved",
                "presolved": (
                    f"-- ⚠️ MANUAL REVIEW: SSIS Execute Process Task '{ex['name']}' ran an "
                    f"external process: {task.get('executable')} {task.get('arguments') or ''}\n"
                    f"-- Re-implement as a Databricks job step / %sh cell as appropriate.")}
    # other_task
    return {"kind": "presolved",
            "presolved": (
                f"-- ⚠️ MANUAL REVIEW: SSIS task '{ex['name']}' "
                f"({task.get('taskKind')}) has no automatic Spark equivalent — port manually.")}


def build_prompts(pkg: dict) -> list[dict]:
    """Deterministic step: produce an ordered list of per-node generation prompts."""
    prompts = []
    # Map a data-flow name back to its enclosing loop (for the file-glob presolve).
    df_loop = {}
    for ex in pkg["controlFlow"]["executables"]:
        lp = ex.get("loop")
        if ex.get("dataFlow"):
            df_loop[ex["dataFlow"]["name"]] = lp
        for child in ex.get("children", []):
            if child.get("dataFlow"):
                df_loop[child["dataFlow"]["name"]] = lp

    # Walk executables in precedence order; emit a task cell or its data-flow node cells.
    for ex in order_executables(pkg):
        holders = [ex, *ex.get("children", [])]
        for holder in holders:
            if holder.get("task"):
                g = ground_task(holder)
                entry = {"dataflow": holder["name"], "node": holder["name"],
                         "view": view_name(holder["name"]), "kind": g["kind"],
                         "isTask": True}
                if "presolved" in g:
                    entry["kind"] = "statement"
                    entry["presolved"] = g["presolved"]
                else:
                    entry["user_prompt"] = (
                        f"Convert this SSIS control-flow task to Spark SQL.\n\n"
                        f"FACTS (authoritative):\n{g['facts']}\n\nReturn only the SQL body.")
                prompts.append(entry)
            df = holder.get("dataFlow")
            if not df:
                continue
            df_name, nodes, edges = df["name"], df["nodes"], df["edges"]
            loop = df_loop.get(df_name)
            prompts.extend(_dataflow_prompts(df_name, nodes, edges, loop))
    return prompts


def _dataflow_prompts(df_name, nodes, edges, loop) -> list[dict]:
    """Per-node prompts for one data flow (nodes in topo order)."""
    prompts = []
    for node in topo_order(nodes, edges):
        grounded = ground_node(node, edges, nodes)
        if grounded is None:
            continue
        entry = {
            "dataflow": df_name,
            "node": node["name"],
            "view": view_name(node["name"]),
            "kind": grounded["kind"],  # 'select' (wrap as view) or 'statement' (verbatim)
            "user_prompt": (
                f"Convert this SSIS component to Spark SQL.\n\n"
                f"FACTS (authoritative):\n{grounded['facts']}\n\n"
                f"Return only the SQL body."
            ),
        }
        # A FlatFileSource inside a ForEach-file loop is converted DETERMINISTICALLY
        # to a set-based glob read — no LLM (correctness-critical, mechanical).
        if (loop and loop.get("enumerator") == "file"
                and node.get("role") == "source"
                and node["componentType"] == "Microsoft.FlatFileSource"):
            entry["presolved"] = presolve_source_glob(node, loop)
        # A Script Component is passed through with its code preserved for manual
        # porting — never a fabricated translation of arbitrary C#/VB.
        elif node["componentType"] == "Microsoft.ManagedComponentHost":
            ups = [view_name(e["from"]) for e in incoming_edges(node, edges)]
            upstream = ups[0] if ups else "/* upstream view */"
            entry["presolved"] = presolve_script(node).replace("{UPSTREAM}", upstream)
        prompts.append(entry)
    return prompts


# ---- LLM provider abstraction -------------------------------------------------
#
# A "provider" is a factory: given **kwargs from the CLI (model, endpoint, ...),
# it returns a `Generate` callable  (system_prompt, user_prompt) -> str.
#
# Providers register themselves by name in PROVIDERS via @register_provider.
# To plug in your own LLM (GitHub Copilot, an OpenAI-compatible gateway, a local
# model, an internal proxy), add ONE function decorated with @register_provider
# and it becomes selectable via `--backend <name>`. See the README, "Plugging in
# your own LLM provider".
#
# Contract for the returned callable:
#   - input : (system_prompt: str, user_prompt: str)
#   - output: the model's raw text (SQL body). The harness calls _normalize_sql()
#             on it, so returning a stray ```fence``` or CREATE VIEW wrapper is
#             tolerated but not required.

ProviderFactory = Callable[..., Generate]
PROVIDERS: dict[str, ProviderFactory] = {}


def register_provider(name: str) -> Callable[[ProviderFactory], ProviderFactory]:
    def deco(factory: ProviderFactory) -> ProviderFactory:
        PROVIDERS[name] = factory
        return factory
    return deco


def get_provider(name: str, **kwargs) -> Generate:
    if name not in PROVIDERS:
        raise SystemExit(f"Unknown backend '{name}'. Available: {', '.join(sorted(PROVIDERS))}")
    return PROVIDERS[name](**kwargs)


@register_provider("anthropic")
def _anthropic(model: str = "claude-opus-5", **_) -> Generate:
    """Anthropic SDK (Claude). Needs `anthropic` + ANTHROPIC_API_KEY or `ant auth login`.
    Uses adaptive thinking + streaming per Anthropic API guidance."""
    import anthropic  # lazy — --dry-run and other backends don't need it

    client = anthropic.Anthropic()

    def generate(system_prompt: str, user_prompt: str) -> str:
        with client.messages.stream(
            model=model, max_tokens=4096, system=system_prompt,
            thinking={"type": "adaptive"},
            messages=[{"role": "user", "content": user_prompt}],
        ) as stream:
            msg = stream.get_final_message()
        return "".join(b.text for b in msg.content if b.type == "text").strip()

    return generate


@register_provider("databricks")
def _databricks(endpoint: str = "databricks-claude-opus-5", profile: str | None = None, **_) -> Generate:
    """Databricks serving endpoint (Foundation Model API). Any chat model hosted in
    the workspace — Claude, GPT, Llama. Auth via the ~/.databrickscfg profile."""
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

    ws = WorkspaceClient(profile=profile) if profile else WorkspaceClient()

    def _text_of(content) -> str:
        # Claude endpoints return reasoning + text blocks; extract only text.
        if isinstance(content, str):
            return content
        return "".join(b.get("text", "") for b in content
                       if isinstance(b, dict) and b.get("type") == "text")

    def generate(system_prompt: str, user_prompt: str) -> str:
        resp = ws.serving_endpoints.query(
            name=endpoint, max_tokens=4096,
            messages=[ChatMessage(role=ChatMessageRole.SYSTEM, content=system_prompt),
                      ChatMessage(role=ChatMessageRole.USER, content=user_prompt)],
        )
        return _text_of(resp.choices[0].message.content).strip()

    return generate


@register_provider("openai")
def _openai(model: str = "gpt-4o", base_url: str | None = None, **_) -> Generate:
    """Any OpenAI-compatible endpoint (OpenAI, Azure OpenAI, GitHub Copilot / Models,
    Ollama, vLLM, LiteLLM, a corporate gateway). Needs `openai`; reads OPENAI_API_KEY
    (and OPENAI_BASE_URL, or pass --base-url). This is the easiest way to plug in
    Copilot or an internal proxy — just point base_url at it."""
    from openai import OpenAI

    client = OpenAI(base_url=base_url) if base_url else OpenAI()

    def generate(system_prompt: str, user_prompt: str) -> str:
        resp = client.chat.completions.create(
            model=model, max_tokens=4096,
            messages=[{"role": "system", "content": system_prompt},
                      {"role": "user", "content": user_prompt}],
        )
        return (resp.choices[0].message.content or "").strip()

    return generate


# ---- Notebook assembly (deterministic) ---------------------------------------

def assemble_notebook(pkg: dict, prompts: list[dict], results: dict[str, str]) -> str:
    """Stitch generated per-node SQL into a Databricks notebook source file.

    A 'select' node is wrapped as CREATE OR REPLACE TEMP VIEW; a 'statement' node
    (e.g. a table target's INSERT) is emitted verbatim — wrapping an INSERT in a
    view is invalid SQL.
    """
    lines = ["# Databricks notebook source",
             f"# Generated from SSIS package: {pkg['package']}",
             "# IR-grounded forward generation (ir_to_sparksql.py)", ""]

    # Note any ForEach loops. File loops become a set-based glob at the source cell;
    # other enumerators (ADO recordset, item, ...) get a clear manual-review banner —
    # a row-driven loop usually becomes a set-based join/operation in Spark.
    for ex in pkg["controlFlow"]["executables"]:
        lp = ex.get("loop")
        if not lp:
            continue
        if lp.get("enumerator") == "file":
            lines += ["# COMMAND ----------",
                      f"# NOTE: SSIS ForeachLoop '{ex['name']}' iterated files matching "
                      f"'{lp.get('filePattern')}' in a folder.",
                      "# Converted to a single set-based read_files() glob at the source "
                      "cell below (edit the Volume path).", ""]
        elif lp.get("enumerator") == "ado":
            lines += ["# COMMAND ----------",
                      f"# ⚠️ NOTE: SSIS ForeachLoop '{ex['name']}' iterated over an ADO "
                      f"recordset in variable '{lp.get('sourceVariable')}', binding "
                      f"{lp.get('variableMappings')}.",
                      "# In Spark, prefer a SET-BASED rewrite: join/operate on the whole "
                      "recordset instead of looping row-by-row. The body cells below were "
                      "generated as if processing the full set; review the per-row logic.", ""]
        else:
            lines += ["# COMMAND ----------",
                      f"# ⚠️ NOTE: SSIS ForeachLoop '{ex['name']}' uses a "
                      f"'{lp.get('enumerator')}' enumerator (not file/ADO). Review the "
                      "iteration semantics — this was not specially converted.", ""]

    for p in prompts:
        sql = p["presolved"] if p.get("presolved") else _normalize_sql(results.get(p["node"], "") or "")
        lines += ["# COMMAND ----------"]
        if p.get("kind") == "statement":
            lines += [f"# {p['dataflow']} :: {p['node']}  (statement)",
                      "spark.sql(\"\"\"",
                      sql or "-- (no output generated)",
                      "\"\"\")", ""]
        else:
            body = sql or "SELECT /* no output generated */ 1 WHERE 1=0"
            lines += [f"# {p['dataflow']} :: {p['node']}  ->  view {p['view']}",
                      f"spark.sql(\"\"\"CREATE OR REPLACE TEMP VIEW {p['view']} AS",
                      body,
                      "\"\"\")", ""]
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description="Forward-generate SparkSQL from dtsx IR via an LLM.")
    ap.add_argument("ir_json", help="IR file from dtsx_to_ir.py (single package)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Print the grounded prompts and exit (no LLM call)")
    ap.add_argument("--backend", default="anthropic",
                    help=f"LLM provider (registered: {', '.join(sorted(PROVIDERS)) or 'anthropic,databricks,openai'})")
    ap.add_argument("--model", default="claude-opus-5",
                    help="Model id (anthropic/openai backends)")
    ap.add_argument("--endpoint", default="databricks-claude-opus-5",
                    help="Serving endpoint name (databricks backend)")
    ap.add_argument("--profile", default=None, help="Databricks profile (databricks backend)")
    ap.add_argument("--base-url", default=None,
                    help="OpenAI-compatible base URL (openai backend: Copilot, Azure, gateways)")
    ap.add_argument("--out", help="Output dir for the generated notebook")
    args = ap.parse_args()

    pkg = json.loads(Path(args.ir_json).read_text())
    if "packages" in pkg:  # accept multi-package files, take the first
        pkg = pkg["packages"][0]

    prompts = build_prompts(pkg)

    if args.dry_run:
        print(f"# {len(prompts)} grounded node prompt(s) for package '{pkg['package']}'\n")
        for p in prompts:
            print("=" * 78)
            print(f"NODE: {p['node']}  ({p['dataflow']})  ->  {p['view']}  [{p['kind']}]")
            print("-" * 78)
            print(p["user_prompt"])
            print()
        return 0

    # Any registered provider is selectable by name; extra flags are passed through.
    generate: Generate = get_provider(
        args.backend, model=args.model, endpoint=args.endpoint,
        profile=args.profile, base_url=args.base_url,
    )

    results: dict[str, str] = {}
    for p in prompts:
        if p.get("presolved"):  # deterministic (e.g. ForEach-file glob) — skip the LLM
            print(f"[presolved] {p['node']}", file=sys.stderr)
            results[p["node"]] = p["presolved"]
            continue
        print(f"[generate] {p['node']} ...", file=sys.stderr)
        results[p["node"]] = generate(SYSTEM_PROMPT, p["user_prompt"])

    notebook = assemble_notebook(pkg, prompts, results)
    if args.out:
        out_path = Path(args.out) / f"{pkg['package']}.py"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(notebook)
        print(f"Notebook written to: {out_path}", file=sys.stderr)
    else:
        print(notebook)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
