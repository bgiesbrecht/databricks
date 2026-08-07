# Databricks notebook source
# MAGIC %md
# MAGIC # SSIS IR → SparkSQL (Foundation Model API)
# MAGIC
# MAGIC Forward-generates a Databricks SparkSQL notebook from the `dtsx_to_ir.py` IR,
# MAGIC using a Databricks-hosted model via the **Foundation Model API** — no `openai`
# MAGIC or `anthropic` install, notebook auth is automatic.
# MAGIC
# MAGIC **Division of labor** (the point of IR + LLM):
# MAGIC - *Deterministic (no LLM):* topo-sort the data-flow nodes, name temp views,
# MAGIC   and build a **grounded** prompt per node that pins facts the model must not
# MAGIC   invent (lookup join keys, `noMatchBehavior`, source/target tables).
# MAGIC - *LLM:* translate each node's T-SQL / transform semantics into Spark SQL,
# MAGIC   honoring the pinned no-match disposition.
# MAGIC
# MAGIC **Before running:** upload the raw SSIS `.dtsx` package to the Volume path in
# MAGIC the widgets below. The notebook parses it into the IR itself — no local step.

# COMMAND ----------

dbutils.widgets.text("dtsx_path", "/Volumes/main/default/ssis/Lesson 1.dtsx", "SSIS .dtsx path (Volume)")
dbutils.widgets.text("out_path", "/Volumes/main/default/ssis/generated_Lesson1.py", "Output notebook path (Volume)")
dbutils.widgets.text("endpoint", "databricks-claude-opus-5", "FMAPI serving endpoint")
dbutils.widgets.dropdown("dry_run", "false", ["true", "false"], "Dry run (print prompts, no LLM)")

DTSX_PATH = dbutils.widgets.get("dtsx_path")
OUT_PATH = dbutils.widgets.get("out_path")
ENDPOINT = dbutils.widgets.get("endpoint")
DRY_RUN = dbutils.widgets.get("dry_run") == "true"

# COMMAND ----------

# MAGIC %md ## Parse the `.dtsx` into IR
# MAGIC
# MAGIC Stdlib `xml.etree.ElementTree` only — no `lxml`/pip install. Captures what a
# MAGIC naive transpile drops: data-flow **edges**, lookup **join keys**, lookup
# MAGIC **`noMatchBehavior`**, connections, variables, precedence constraints.

# COMMAND ----------

import json
import xml.etree.ElementTree as ET

DTS = "www.microsoft.com/SqlServer/Dts"
NS = {"DTS": DTS}

# SSIS enum decodings (from MS docs) so the IR is human-readable, not magic ints.
_NO_MATCH = {"0": "fail_component", "1": "ignore_failure",
             "2": "redirect_to_error_output", "3": "redirect_to_no_match_output"}
_CACHE_TYPE = {"0": "full", "1": "partial", "2": "none"}
_EVAL_OP = {"0": "Constraint", "1": "Expression",
            "2": "ExpressionAndConstraint", "3": "ExpressionOrConstraint"}
_VALUE = {"0": "Success", "1": "Failure", "2": "Completion"}


def _dts(el, attr):
    return el.get(f"{{{DTS}}}{attr}") if el is not None else None


def _prop(component, name):
    for p in component.iter("property"):
        if p.get("name") == name:
            return p.text
    return None


def _parse_connections(root):
    out = []
    for cm in root.findall(".//DTS:ConnectionManager", NS):
        name = _dts(cm, "ObjectName")
        if not name:
            continue
        conn = cm.find(".//DTS:ConnectionManager", NS)
        out.append({"name": name, "type": _dts(cm, "CreationName"),
                    "connectionString": _dts(conn, "ConnectionString") if conn is not None else None})
    return out


def _parse_variables(root):
    out = []
    for v in root.findall(".//DTS:Variable", NS):
        vv = v.find("DTS:VariableValue", NS)
        out.append({"name": _dts(v, "ObjectName"), "namespace": _dts(v, "Namespace"),
                    "value": (vv.text if vv is not None else None)})
    return out


def _parse_lookup(c):
    jc = [ic.get("cachedName") or ic.get("name")
          for ic in c.iter("inputColumn") if (ic.get("cachedName") or ic.get("name"))]
    return {"referenceQuery": (_prop(c, "SqlCommand") or "").strip() or None,
            "parameterizedQuery": (_prop(c, "SqlCommandParam") or "").strip() or None,
            "joinInputColumns": jc,
            "cacheType": _CACHE_TYPE.get(_prop(c, "CacheType") or "", _prop(c, "CacheType")),
            "noMatchBehavior": _NO_MATCH.get(_prop(c, "NoMatchBehavior") or "", _prop(c, "NoMatchBehavior"))}


def _parse_flatfile_source(c):
    return {"outputColumns": [oc.get("name") for oc in c.iter("outputColumn") if oc.get("name")]}


def _parse_destination(c):
    return {"openRowset": _prop(c, "OpenRowset"), "accessMode": _prop(c, "AccessMode"),
            "sqlCommand": (_prop(c, "SqlCommand") or None)}


def _parse_flatfile_destination(c):
    conn = None
    for cm in c.iter("connection"):
        conn = cm.get("connectionManagerID") or cm.get("connectionManagerRefId")
        if conn:
            break
    return {"connectionManagerID": conn, "header": _prop(c, "Header")}


def _col_prop(col, name):
    for p in col.iter("property"):
        if p.get("name") == name:
            return (p.text or "").strip() or None
    return None


def _parse_oledb_source(c):
    return {"openRowset": _prop(c, "OpenRowset"),
            "sqlCommand": (_prop(c, "SqlCommand") or "").strip() or None,
            "accessMode": _prop(c, "AccessMode")}


def _parse_derived_column(c):
    derivations = []
    for oc in c.iter("outputColumn"):
        expr = _col_prop(oc, "FriendlyExpression") or _col_prop(oc, "Expression")
        if oc.get("name") and expr:
            derivations.append({"column": oc.get("name"), "expression": expr})
    return {"derivations": derivations}


def _parse_conditional_split(c):
    conditions, default_output = [], None
    for o in c.iter("output"):
        name = o.get("name")
        is_default, expr, order = False, None, None
        for p in o.iter("property"):
            pn, pv = p.get("name"), (p.text or "").strip()
            if pn == "IsDefaultOut" and pv.lower() == "true":
                is_default = True
            elif pn == "FriendlyExpression" and pv:
                expr = pv
            elif pn == "Expression" and pv and not expr:
                expr = pv
            elif pn == "EvaluationOrder" and pv:
                order = pv
        if is_default:
            default_output = name
        elif name and expr:
            conditions.append({"output": name, "condition": expr, "evaluationOrder": order})
    conditions.sort(key=lambda c: int(c["evaluationOrder"]) if (c["evaluationOrder"] or "").lstrip("-").isdigit() else 0)
    return {"conditions": conditions, "defaultOutput": default_output}


def _parse_oledb_command(c):
    return {"sqlCommand": (_prop(c, "SqlCommand") or "").strip() or None}


_AGG_FUNC = {"0": "group_by", "1": "SUM", "2": "COUNT", "3": "COUNT_DISTINCT",
             "4": "MIN", "5": "MAX", "7": "AVG"}
_MJ_JOIN = {"0": "FULL OUTER JOIN", "1": "LEFT JOIN", "2": "INNER JOIN"}
_SSIS_TYPE = {"i1": "TINYINT", "i2": "SMALLINT", "i4": "INT", "i8": "BIGINT",
              "ui1": "SMALLINT", "ui2": "INT", "ui4": "BIGINT", "ui8": "BIGINT",
              "r4": "FLOAT", "r8": "DOUBLE", "numeric": "DECIMAL(38,10)",
              "decimal": "DECIMAL(38,10)", "cy": "DECIMAL(19,4)", "bool": "BOOLEAN",
              "str": "STRING", "wstr": "STRING", "text": "STRING", "ntext": "STRING",
              "dbdate": "DATE", "date": "DATE", "dbtimestamp": "TIMESTAMP",
              "dbtimestamp2": "TIMESTAMP", "dbtime": "STRING", "guid": "STRING"}


def _parse_aggregate(c):
    group_by, aggregates = [], []
    for oc in c.iter("outputColumn"):
        name = oc.get("name")
        if not name:
            continue
        func = _AGG_FUNC.get(_col_prop(oc, "AggregationType") or "", None)
        if func == "group_by":
            group_by.append(name)
        elif func:
            aggregates.append({"column": name, "function": func})
    return {"groupBy": group_by, "aggregates": aggregates}


def _parse_union_all(c):
    return {"outputColumns": [oc.get("name") for oc in c.iter("outputColumn") if oc.get("name")]}


def _parse_multicast(c):
    return {"passthrough": True}


def _parse_merge_join(c):
    jt = _MJ_JOIN.get(_prop(c, "JoinType") or "", _prop(c, "JoinType"))
    keys = {}
    for inp in c.iter("input"):
        for col in inp.iter("inputColumn"):
            pos = None
            for p in col.iter("property"):
                if p.get("name") in ("SortKeyPosition", "NewSortKeyPosition") and (p.text or "").strip():
                    pos = (p.text or "").strip()
            if pos and pos != "0":
                keys.setdefault(inp.get("name"), []).append(col.get("cachedName") or col.get("name"))
    return {"joinType": jt, "joinKeysByInput": keys,
            "outputColumns": [oc.get("name") for oc in c.iter("outputColumn") if oc.get("name")]}


def _parse_sort(c):
    keys = []
    for col in c.iter("inputColumn"):
        pos = None
        for p in col.iter("property"):
            if p.get("name") in ("NewSortKeyPosition", "SortKeyPosition") and (p.text or "").strip():
                pos = (p.text or "").strip()
        if pos and pos.lstrip("-").isdigit() and int(pos) != 0:
            keys.append((int(pos), col.get("cachedName") or col.get("name")))
    keys.sort(key=lambda k: abs(k[0]))
    return {"sortKeys": [{"column": c2, "descending": p < 0} for p, c2 in keys]}


def _parse_data_convert(c):
    conversions = []
    for oc in c.iter("outputColumn"):
        name, dt = oc.get("name"), oc.get("dataType")
        if name and dt:
            conversions.append({"column": name, "targetType": _SSIS_TYPE.get(dt, dt.upper()),
                                "length": oc.get("length")})
    return {"conversions": conversions}


_SCD_COLTYPE = {"1": "business_key", "2": "fixed", "3": "changing"}


def _parse_scd(c):
    keys, changing, fixed = [], [], []
    for col in c.iter("inputColumn"):
        nm = col.get("cachedName") or col.get("name")
        ct = None
        for p in col.iter("property"):
            if p.get("name") == "ColumnType":
                ct = (p.text or "").strip()
        role = _SCD_COLTYPE.get(ct or "")
        if role == "business_key":
            keys.append(nm)
        elif role == "changing":
            changing.append(nm)
        elif role == "fixed":
            fixed.append(nm)
    hist = (_prop(c, "UpdateChangingAttributeHistory") or "").lower() == "true"
    return {"businessKeys": keys, "changingAttributes": changing, "fixedAttributes": fixed,
            "historyType": "type2_historical" if hist else "type1_inplace",
            "currentRowWhere": _prop(c, "CurrentRowWhere"),
            "outputs": [o.get("name") for o in c.iter("output")]}


def _parse_script_component(c):
    src = _prop(c, "SourceCode")
    out_cols = [oc.get("name") for o in c.iter("output") for oc in o.iter("outputColumn") if oc.get("name")]
    return {"scriptLanguage": _prop(c, "ScriptLanguage"),
            "readOnlyVariables": _prop(c, "ReadOnlyVariables"),
            "readWriteVariables": _prop(c, "ReadWriteVariables"),
            "outputColumns": out_cols,
            "sourceCode": (src[:4000] if src else None),
            "requiresManualReview": True}


def _parse_row_count(c):
    return {"variableName": _prop(c, "VariableName")}


def _parse_unpivot(c):
    def _clean(v):
        return None if (v is None or v.startswith("#{")) else v
    pivoted, passthrough, dest_hint = [], [], None
    for col in c.iter("inputColumn"):
        nm = col.get("cachedName") or col.get("name")
        pkv = _col_prop(col, "PivotKeyValue")
        if pkv is not None:
            pivoted.append({"sourceColumn": nm, "keyValue": pkv})
            dest_hint = dest_hint or _clean(_col_prop(col, "DestinationColumn"))
        else:
            passthrough.append(nm)
    out_cols = [oc.get("name") for oc in c.iter("outputColumn")
                if oc.get("name") and not (oc.get("name") or "").startswith("#{")]
    new_cols = [x for x in out_cols if x not in passthrough]
    key_col = new_cols[0] if len(new_cols) >= 1 else "PivotKey"
    value_col = dest_hint or (new_cols[1] if len(new_cols) >= 2 else "PivotValue")
    return {"pivotedColumns": pivoted, "passthroughColumns": passthrough,
            "keyColumn": key_col, "valueColumn": value_col}


def _parse_pivot(c):
    roles = {"0": [], "1": [], "2": [], "3": []}
    for col in c.iter("inputColumn"):
        nm = col.get("cachedName") or col.get("name")
        roles.setdefault(_col_prop(col, "PivotUsage") or "0", []).append(nm)
    return {"setColumns": roles.get("1", []), "pivotKeyColumns": roles.get("2", []),
            "valueColumns": roles.get("3", []),
            "outputColumns": [oc.get("name") for oc in c.iter("outputColumn") if oc.get("name")]}


def _parse_fuzzy_lookup(c):
    join_cols = [ic.get("cachedName") or ic.get("name")
                 for ic in c.iter("inputColumn") if (ic.get("cachedName") or ic.get("name"))]
    return {"referenceTable": _prop(c, "ReferenceTableName") or _prop(c, "OpenRowset"),
            "joinInputColumns": join_cols,
            "minSimilarity": _prop(c, "MinSimilarity"),
            "maxOutputMatches": _prop(c, "MaxOutputMatchesPerInput")}


def _parse_cache_transform(c):
    return {"cachedColumns": [oc.get("name") for oc in c.iter("outputColumn") if oc.get("name")],
            "connectionManagerID": _prop(c, "ConnectionManagerID")}


_PARSERS = {"Microsoft.Lookup": _parse_lookup,
            "Microsoft.FlatFileSource": _parse_flatfile_source,
            "Microsoft.OLEDBSource": _parse_oledb_source,
            "Microsoft.OLEDBDestination": _parse_destination,
            "Microsoft.FlatFileDestination": _parse_flatfile_destination,
            "Microsoft.DerivedColumn": _parse_derived_column,
            "Microsoft.ConditionalSplit": _parse_conditional_split,
            "Microsoft.OLEDBCommand": _parse_oledb_command,
            "Microsoft.Aggregate": _parse_aggregate,
            "Microsoft.UnionAll": _parse_union_all,
            "Microsoft.Multicast": _parse_multicast,
            "Microsoft.MergeJoin": _parse_merge_join,
            "Microsoft.Sort": _parse_sort,
            "Microsoft.DataConvert": _parse_data_convert,
            "Microsoft.SCD": _parse_scd,
            "Microsoft.ManagedComponentHost": _parse_script_component,
            "Microsoft.RowCount": _parse_row_count,
            "Microsoft.UnPivot": _parse_unpivot,
            "Microsoft.Pivot": _parse_pivot,
            "Microsoft.FuzzyLookup": _parse_fuzzy_lookup,
            "Microsoft.Cache": _parse_cache_transform}


def _short(ref_id):
    if not ref_id:
        return ref_id
    return ref_id.split("\\")[-1].split(".")[0]


def _parse_dataflow(ex):
    """Only the executable's OWN pipeline (a container has none; its children do)."""
    pipeline = ex.find("DTS:ObjectData/pipeline", NS)
    if pipeline is None:
        return None
    nodes = []
    for comp in pipeline.iter("component"):
        cls = comp.get("componentClassID")
        node = {"name": comp.get("name"), "refId": comp.get("refId"), "componentType": cls,
                "role": {"Microsoft.FlatFileSource": "source", "Microsoft.OLEDBSource": "source",
                         "Microsoft.OLEDBDestination": "target", "Microsoft.FlatFileDestination": "target",
                         "Microsoft.Lookup": "transform"}.get(cls, "transform")}
        parser = _PARSERS.get(cls)
        if parser:
            node["logic"] = parser(comp)
        nodes.append(node)
    edges = []
    for pth in pipeline.iter("path"):
        sid = pth.get("startId") or ""
        edges.append({"name": pth.get("name"), "from": _short(pth.get("startId")),
                      "to": _short(pth.get("endId")),
                      "fromPort": sid.split("Outputs[")[-1].rstrip("]") if "Outputs[" in sid else None})
    return {"name": _dts(ex, "ObjectName"), "type": "DataFlow", "nodes": nodes, "edges": edges}


def _clean_expr(val):
    if val is None:
        return None
    v = val.strip()
    if len(v) >= 2 and v[0] == '"' and v[-1] == '"':
        v = v[1:-1]
    return v


def _find_attr_anywhere(el, suffix):
    """Return the first attribute value whose local-name ends with `suffix`, scanning
    the element and its descendants (control-flow tasks bury data in namespaced attrs)."""
    for node in el.iter():
        for k, v in node.attrib.items():
            if k.split("}")[-1].endswith(suffix) and v:
                return v
    return None


def _parse_task(ex, creation):
    """Extract the logic of a control-flow TASK (not a data flow). Returns a `task`
    dict, or None if this executable isn't a task we specifically handle."""
    if creation == "Microsoft.ExecuteSQLTask":
        return {"type": "execute_sql",
                "sql": _find_attr_anywhere(ex, "SqlStatementSource"),
                "connection": _find_attr_anywhere(ex, "Connection")}
    if creation == "Microsoft.ExecutePackageTask":
        return {"type": "execute_package",
                "packageName": _find_attr_anywhere(ex, "PackageName")
                               or _find_attr_anywhere(ex, "RefName")}
    if creation == "Microsoft.ScriptTask":
        return {"type": "script_task",
                "scriptLanguage": _find_attr_anywhere(ex, "ScriptLanguage"),
                "requiresManualReview": True}
    if creation == "Microsoft.ExecuteProcess":
        return {"type": "execute_process",
                "executable": _find_attr_anywhere(ex, "Executable"),
                "arguments": _find_attr_anywhere(ex, "Arguments"),
                "requiresManualReview": True}
    if creation in ("Microsoft.FileSystemTask", "Microsoft.XMLTask",
                    "Microsoft.WmiDataReaderTask", "Microsoft.BulkInsertTask",
                    "Microsoft.DataProfilingTask"):
        return {"type": "other_task", "taskKind": creation, "requiresManualReview": True}
    return None


def _parse_foreach(ex):
    """ForEach *File* enumerator -> {folder, filePattern, recurse, fileNameVariable}."""
    en = ex.find(".//DTS:ForEachEnumerator", NS)
    if en is None or _dts(en, "CreationName") != "Microsoft.ForEachFileEnumerator":
        return None
    folder = filespec = None
    recurse = "0"
    for p in en.iter("FEFEProperty"):
        if p.get("Folder") is not None:
            folder = _clean_expr(p.get("Folder"))
        if p.get("FileSpec") is not None:
            filespec = p.get("FileSpec")
        if p.get("Recurse") is not None:
            recurse = p.get("Recurse")
    var = None
    for vm in ex.findall(".//DTS:ForEachVariableMapping", NS):
        if _dts(vm, "VariableName") and (vm.get("ValueIndex") in (None, "0")):
            var = _dts(vm, "VariableName")
            break
    return {"enumerator": "file", "folder": folder, "filePattern": filespec,
            "recurse": recurse == "1", "fileNameVariable": var}


def _parse_control_flow(root):
    items = []
    for ex in root.findall("DTS:Executables/DTS:Executable", NS):
        creation = _dts(ex, "CreationName") or ""
        entry = {"name": _dts(ex, "ObjectName"), "creationName": creation}
        df = _parse_dataflow(ex)
        if df:
            entry["dataFlow"] = df
        if creation == "STOCK:FOREACHLOOP":
            loop = _parse_foreach(ex)
            if loop:
                entry["loop"] = loop
        # Control-flow TASK (Execute SQL, Execute Package, Script, ...)
        task = _parse_task(ex, creation)
        if task:
            entry["task"] = task
        # Nested executables (e.g. ForeachLoop / Sequence bodies)
        nested = []
        for n in ex.findall("DTS:Executables/DTS:Executable", NS):
            ncreation = _dts(n, "CreationName") or ""
            child = {"name": _dts(n, "ObjectName"), "creationName": ncreation,
                     "dataFlow": _parse_dataflow(n)}
            ntask = _parse_task(n, ncreation)
            if ntask:
                child["task"] = ntask
            nested.append(child)
        if nested:
            entry["children"] = nested
        items.append(entry)
    constraints = []
    for pc in root.findall(".//DTS:PrecedenceConstraint", NS):
        constraints.append({"from": _dts(pc, "From"), "to": _dts(pc, "To"),
                            "value": _VALUE.get(_dts(pc, "Value") or "", _dts(pc, "Value")),
                            "evalOp": _EVAL_OP.get(_dts(pc, "EvalOp") or "", _dts(pc, "EvalOp")),
                            "expression": _dts(pc, "Expression")})
    return items, constraints


def extract_ir(dtsx_path):
    root = ET.parse(dtsx_path).getroot()
    executables, constraints = _parse_control_flow(root)
    return {"package": _dts(root, "ObjectName") or dtsx_path.split("/")[-1],
            "sourceFile": dtsx_path.split("/")[-1],
            "connections": _parse_connections(root),
            "variables": _parse_variables(root),
            "controlFlow": {"executables": executables, "precedenceConstraints": constraints}}


import os

# Accept either a single .dtsx or a directory (use the first .dtsx in it).
if os.path.isdir(DTSX_PATH):
    dtsx_files = sorted(f for f in os.listdir(DTSX_PATH) if f.lower().endswith(".dtsx"))
    if not dtsx_files:
        raise FileNotFoundError(f"No .dtsx files found in directory: {DTSX_PATH}")
    resolved_path = os.path.join(DTSX_PATH, dtsx_files[0])
    print(f"Directory provided — using first .dtsx file: {resolved_path}")
    if len(dtsx_files) > 1:
        print(f"  (other files available: {dtsx_files[1:]})")
else:
    resolved_path = DTSX_PATH

pkg = extract_ir(resolved_path)
print("Parsed package:", pkg["package"])
print(json.dumps(pkg, indent=2)[:1500], "...")

# COMMAND ----------

# MAGIC %md ## Deterministic layer — topo-sort, view names, grounding

# COMMAND ----------

def view_name(node_name: str) -> str:
    safe = "".join(c if c.isalnum() else "_" for c in node_name).strip("_")
    return f"v_{safe.lower()}"


def topo_order(nodes, edges):
    """Kahn's algorithm; falls back to source-first order on a cycle/partial graph."""
    by_name = {n["name"]: n for n in nodes}
    indeg = {n["name"]: 0 for n in nodes}
    adj = {n["name"]: [] for n in nodes}
    for e in edges:
        f, t = e.get("from"), e.get("to")
        if f in by_name and t in by_name:
            adj[f].append(t)
            indeg[t] += 1
    queue = sorted(
        [n["name"] for n in nodes if indeg[n["name"]] == 0],
        key=lambda name: (by_name[name].get("role") != "source", name),
    )
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
    if len(ordered) != len(nodes):
        return sorted(nodes, key=lambda n: n.get("role") != "source")
    return ordered


import re


def incoming_edges(node, edges):
    return [e for e in edges if e.get("to") == node["name"]]


def _port_kind(port):
    p = (port or "").lower()
    if "error" in p or "no match" in p or "nomatch" in p:
        return "error"
    if "match" in p:
        return "match"
    return "plain"


def _lookup_join_type(node, edges):
    """error/no-match REDIRECT overrides fail_component -> LEFT JOIN to retain rows."""
    nmb = (node.get("logic") or {}).get("noMatchBehavior")
    has_error_branch = any(
        e.get("from") == node["name"] and _port_kind(e.get("fromPort")) == "error"
        for e in edges
    )
    if nmb in ("redirect_to_error_output", "redirect_to_no_match_output", "ignore_failure"):
        return "LEFT JOIN"
    if has_error_branch:
        return "LEFT JOIN"
    return "INNER JOIN"


def ground_node(node, edges, nodes):
    """Return {facts, kind}; kind is 'select' (wrap as view) or 'statement' (verbatim)."""
    role = node.get("role")
    logic = node.get("logic") or {}
    ins = incoming_edges(node, edges)
    kind = "statement" if role == "target" else "select"
    facts = [f"Component: {node['name']}",
             f"SSIS type: {node['componentType']}  (role: {role})"]
    by_name = {n["name"]: n for n in nodes}
    for e in ins:
        up_node = by_name.get(e["from"], {})
        port = e.get("fromPort")
        upv = view_name(e["from"])
        if up_node.get("componentType") == "Microsoft.ConditionalSplit":
            facts.append(
                f"Upstream view: {upv} via CONDITIONAL-SPLIT branch '{port}'. This node "
                f"receives ONLY that branch's rows — you MUST filter with "
                f"`WHERE `_split_branch` = '{port}'` (that column was added upstream).")
        else:
            pk = _port_kind(port)
            tag = {"match": "  (consumes the MATCH output — keep matched rows only)",
                   "error": "  (consumes the ERROR/NO-MATCH output — keep non-matching rows only)",
                   "plain": ""}[pk]
            facts.append(f"Upstream view: {upv} via port '{port}'{tag}")
    if node["componentType"] == "Microsoft.Lookup":
        facts += [
            f"Reference query (T-SQL): {logic.get('referenceQuery')}",
            f"Parameterized form (join predicate): {logic.get('parameterizedQuery')}",
            f"Input column(s) used as the join key: {logic.get('joinInputColumns')}",
            f"noMatchBehavior: {logic.get('noMatchBehavior')}",
            f"joinType: {_lookup_join_type(node, edges)}   <-- USE THIS EXACT JOIN TYPE",
            f"cacheType: {logic.get('cacheType')}",
            "Emit a SELECT that joins the upstream view to the reference query on the "
            "join key(s) and adds the looked-up column(s).",
        ]
    elif role == "source" and node["componentType"] == "Microsoft.FlatFileSource":
        facts += [
            f"Flat-file output columns: {logic.get('outputColumns')}",
            "This is a SOURCE: emit a SELECT over the landed flat-file data (assume an "
            "external/temp table or a read of the raw file already exists). Project ONLY "
            "the data columns; drop SSIS error-plumbing columns such as 'Flat File Source "
            "Error Output Column', 'ErrorCode', 'ErrorColumn'.",
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
            "`cond ? a : b` becomes `CASE WHEN cond THEN a ELSE b END`; `GETDATE()` becomes "
            "`current_timestamp()`; string concat `+` becomes `||`. Keep column names exactly.",
        ]
    elif node["componentType"] == "Microsoft.ConditionalSplit":
        conds = logic.get("conditions", [])
        lines_c = "; ".join(f"output '{c['output']}' WHEN {c['condition']}" for c in conds)
        facts += [
            f"Conditional split routes (in evaluation order): {lines_c}",
            f"Default output (rows matching no condition): {logic.get('defaultOutput')}",
            "This is a CONDITIONAL SPLIT: ONE upstream view, multiple named downstream "
            "branches. Emit a SELECT that adds a routing column, e.g. `SELECT src.*, "
            "CASE WHEN <cond1> THEN '<out1>' ... ELSE '<default>' END AS `_split_branch``. "
            "Each downstream node filters on `_split_branch`. Translate SSIS expression "
            "syntax (== -> =, && -> AND, || -> OR). Evaluate conditions in the given order.",
        ]
    elif node["componentType"] == "Microsoft.OLEDBCommand":
        facts += [
            f"Per-row SQL command (SSIS runs this once per input row; `?` are bound to "
            f"input columns in order): {logic.get('sqlCommand')}",
            "This is an OLE DB COMMAND (row-by-row DML). Convert to a SET-BASED Spark SQL "
            "statement: an `UPDATE <table> SET ... WHERE key = ?` becomes a single "
            "`MERGE INTO <table> USING <upstream view> ON <table>.<key> = src.<key> "
            "WHEN MATCHED THEN UPDATE SET ...`. Map each `?` to the upstream column by "
            "position. Convert GETDATE() -> current_timestamp() and T-SQL [x] -> `x`.",
        ]
        kind = "statement"
    elif node["componentType"] == "Microsoft.Aggregate":
        gb = logic.get("groupBy", [])
        aggs = "; ".join(f"{a['function']}(...) AS {a['column']}" for a in logic.get("aggregates", []))
        facts += [
            f"GROUP BY columns: {gb}",
            f"Aggregate output columns (function -> alias): {aggs}",
            "This is an AGGREGATE: emit `SELECT <group-by cols>, <FUNC(col) AS alias>, ... "
            "FROM <upstream view> GROUP BY <group-by cols>`. COUNT_DISTINCT -> "
            "COUNT(DISTINCT col). Add a comment if a measure's source column is ambiguous.",
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
            "This is a MULTICAST: fans one input to several identical downstream branches "
            "with NO transformation. Emit `SELECT * FROM <upstream view>`.",
        ]
    elif node["componentType"] == "Microsoft.MergeJoin":
        facts += [
            f"Join type: {logic.get('joinType')}   <-- USE THIS EXACT JOIN TYPE",
            f"Join keys per input (input name -> key columns): {logic.get('joinKeysByInput')}",
            f"Merged output columns: {logic.get('outputColumns')}",
            "This is a MERGE JOIN of the TWO upstream views above. Emit `SELECT <output "
            "cols> FROM <left view> <joinType> <right view> ON <left.key = right.key>` using "
            "the join keys (paired by position). Spark needs no pre-sort — join directly.",
        ]
    elif node["componentType"] == "Microsoft.Sort":
        sk = ", ".join(f"{s['column']}{' DESC' if s['descending'] else ''}"
                       for s in logic.get("sortKeys", []))
        facts += [
            f"Sort keys: {sk}",
            "This is a SORT: emit `SELECT * FROM <upstream view> ORDER BY <sort keys>`. If "
            "it only feeds a downstream Merge Join it is unnecessary in Spark (passthrough "
            "SELECT *), but when in doubt keep the ORDER BY.",
        ]
    elif node["componentType"] == "Microsoft.DataConvert":
        convs = "; ".join(
            f"{c['column']} = CAST(<source> AS {c['targetType']}"
            + (f"({c['length']})" if c.get('length') and 'DECIMAL' not in c['targetType'] else "")
            + ")" for c in logic.get("conversions", []))
        facts += [
            f"Type conversions (new column = cast of a source column): {convs}",
            "This is a DATA CONVERSION: emit `SELECT src.*, CAST(<source col> AS <type>) AS "
            "<new col>, ...`. Infer each source column from the new column's name (SSIS "
            "often appends a suffix like '_Numeric'); add a comment if ambiguous. Drop the "
            "SSIS ErrorCode/ErrorColumn plumbing columns.",
        ]
    elif node["componentType"] == "Microsoft.SCD":
        facts += [
            f"Business (natural) key column(s): {logic.get('businessKeys')}",
            f"Changing attributes: {logic.get('changingAttributes')}",
            f"Fixed attributes: {logic.get('fixedAttributes')}",
            f"History handling: {logic.get('historyType')}  (type1_inplace = overwrite; "
            f"type2_historical = expire old row + insert new)",
            "This is a SLOWLY CHANGING DIMENSION. Emit ONE set-based `MERGE INTO <dimension "
            "table> AS tgt USING <upstream view> AS src ON tgt.<business key> = src.<business "
            "key>`. type1_inplace: `WHEN MATCHED AND (changing attr differs) THEN UPDATE SET "
            "<changing attrs>` + `WHEN NOT MATCHED THEN INSERT (...)`. type2_historical: "
            "expire the old row (current-flag/end-date) on change, then INSERT the new "
            "version for changed + new keys. Comment that the dimension table name must be "
            "set (from the downstream destination).",
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
    return {"facts": "\n".join(f"- {x}" for x in facts), "kind": kind}


def _win_to_glob(folder, pattern):
    """SSIS Windows folder + FileSpec -> a portable read_files() glob (leaf folder kept,
    prefixed with a Volume placeholder the user edits once)."""
    leaf = folder.replace("\\", "/").rstrip("/").split("/")[-1] if folder else ""
    base = f"/Volumes/main/default/ssis_input/{leaf}".rstrip("/")
    return f"{base}/{pattern or '*'}"


def presolve_source_glob(node, loop):
    """Deterministic SELECT for a FlatFileSource that ran inside a ForEach-file loop:
    SSIS looped once per file; Spark reads them all at once via a read_files() glob."""
    cols = [c for c in (node.get("logic") or {}).get("outputColumns", [])
            if c and c not in ("Flat File Source Error Output Column", "ErrorCode", "ErrorColumn")]
    glob = _win_to_glob(loop.get("folder"), loop.get("filePattern"))
    proj = ",\n  ".join(f"`{c}`" for c in cols) if cols else "*"
    return (f"-- ForEach-file loop over '{loop.get('filePattern')}' -> one set-based glob read\n"
            f"SELECT\n  {proj},\n  _metadata.file_path AS `_source_file`\n"
            f"FROM read_files(\n  '{glob}',\n"
            f"  format => 'csv', header => true, inferSchema => true\n)")


def _presolve_script(node):
    """Deterministic, code-PRESERVING passthrough for a Script Component. We never
    fabricate a translation of arbitrary C#/VB — instead we pass rows through and embed
    the original script + a clear MANUAL-REVIEW banner for a human to port."""
    logic = node.get("logic") or {}
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


def order_executables(pkg):
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
    adj = {e["name"]: [] for e in execs}
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


def ground_task(ex):
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


def _dataflow_prompts(df_name, nodes, edges, loop):
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
            entry["presolved"] = _presolve_script(node).replace("{UPSTREAM}", upstream)
        prompts.append(entry)
    return prompts


def build_prompts(pkg):
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


def _normalize_sql(sql: str) -> str:
    """Strip markdown fences / a stray CREATE...VIEW...AS the model may add, and a
    trailing semicolon — the harness owns the wrapper."""
    s = (sql or "").strip()
    if s.startswith("```"):
        s = "\n".join(l for l in s.splitlines() if not l.strip().startswith("```")).strip()
    m = re.match(r"(?is)^\s*create\s+or\s+replace\s+temp(orary)?\s+view\s+\S+\s+as\s+", s)
    if m:
        s = s[m.end():].strip()
    return s.rstrip(";").strip()


prompts = build_prompts(pkg)
print(f"{len(prompts)} grounded node prompt(s)")

# COMMAND ----------

SYSTEM_PROMPT = """You are a precise SSIS-to-Databricks migration engineer.
You convert ONE SSIS data-flow component into a Spark SQL statement BODY for a \
Databricks notebook cell. Treat the grounded fact sheet as authoritative; do not \
invent table names, columns, join keys, or filters that are not present.

Rules:
- Output ONLY the SQL body. No prose, no markdown fences.
- Do NOT emit `CREATE OR REPLACE TEMP VIEW` yourself — the harness wraps your output.
  * SOURCE or TRANSFORM: emit a single SELECT.
  * TABLE TARGET: emit `INSERT INTO <table> SELECT ...`.
- The upstream component's output is available as a temp view named exactly as given.
- Use the EXACT `joinType` from the fact sheet (the harness already resolved
  fail_component / error-redirect / ignore into the correct JOIN type):
    * INNER JOIN -> non-matching rows are dropped.
    * LEFT JOIN  -> non-matching rows are RETAINED with NULLs in the looked-up columns.
- MATCH output -> keep matched rows (looked-up key IS NOT NULL); ERROR/NO-MATCH output
  -> keep non-matching rows (looked-up key IS NULL).
- Convert T-SQL identifier quoting [x] to Spark backticks `x`.
- Do not add columns the fact sheet does not mention."""

# COMMAND ----------

# MAGIC %md ## Dry run — inspect the grounded prompts (no LLM)

# COMMAND ----------

if DRY_RUN:
    for p in prompts:
        print("=" * 78)
        print(f"NODE: {p['node']}  ({p['dataflow']})  ->  {p['view']}  [{p['kind']}]")
        print("-" * 78)
        print(p["user_prompt"])
        print()

# COMMAND ----------

# Exit in its own cell so the printed prompts above are flushed first.
if DRY_RUN:
    dbutils.notebook.exit("dry-run complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## LLM layer — pluggable provider
# MAGIC
# MAGIC Default is the **Foundation Model API** (`serving_endpoints.query`) — native to
# MAGIC the preinstalled `databricks-sdk`, no `openai`/`anthropic` install, automatic
# MAGIC notebook auth. To use a different LLM (Claude direct, an OpenAI-compatible
# MAGIC gateway, GitHub Copilot/Models, an internal proxy), replace the body of
# MAGIC `generate(system_prompt, user_prompt) -> str` below — nothing else changes.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

_ws = WorkspaceClient()  # notebook auth is automatic


def _text_of(content) -> str:
    """FMAPI content may be a str, or a list of {type,text} dicts and/or bare strs
    (Claude endpoints return reasoning + text blocks). Extract only text."""
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    parts = []
    for b in content:
        if isinstance(b, str):
            parts.append(b)
        elif isinstance(b, dict) and b.get("type") == "text":
            parts.append(b.get("text", ""))
    return "".join(parts)


# --- PROVIDER: swap this one function to plug in a different LLM -----------------
# Contract: (system_prompt, user_prompt) -> raw model text. The harness runs
# _normalize_sql() on the result, so a stray ```fence``` or CREATE VIEW is tolerated.
def generate(system_prompt: str, user_prompt: str) -> str:
    # Default: Databricks Foundation Model API (endpoint from the widget).
    resp = _ws.serving_endpoints.query(
        name=ENDPOINT,
        messages=[ChatMessage(role=ChatMessageRole.SYSTEM, content=system_prompt),
                  ChatMessage(role=ChatMessageRole.USER, content=user_prompt)],
        max_tokens=4096,
    )
    return _text_of(resp.choices[0].message.content).strip()

    # --- OpenAI-compatible alternative (OpenAI, Azure, Copilot Models, gateway) ---
    # %pip install openai   # then restart Python, and replace the body above with:
    # from openai import OpenAI
    # client = OpenAI(base_url="<gateway-or-copilot-url>")  # reads OPENAI_API_KEY
    # r = client.chat.completions.create(
    #     model="gpt-4o", max_tokens=4096,
    #     messages=[{"role": "system", "content": system_prompt},
    #               {"role": "user", "content": user_prompt}])
    # return (r.choices[0].message.content or "").strip()

    # --- Anthropic (Claude) direct -----------------------------------------------
    # %pip install anthropic   # needs ANTHROPIC_API_KEY (e.g. via a secret scope)
    # import anthropic
    # msg = anthropic.Anthropic().messages.create(
    #     model="claude-opus-5", max_tokens=4096, system=system_prompt,
    #     thinking={"type": "adaptive"},
    #     messages=[{"role": "user", "content": user_prompt}])
    # return "".join(b.text for b in msg.content if b.type == "text").strip()


results = {}
for p in prompts:
    if p.get("presolved"):  # deterministic (ForEach-file glob) — skip the LLM
        print(f"[presolved] {p['node']}")
        results[p["node"]] = p["presolved"]
        continue
    print(f"[generate] {p['node']} ...")
    results[p["node"]] = generate(SYSTEM_PROMPT, p["user_prompt"])

# COMMAND ----------

# MAGIC %md ## Assemble and write the generated notebook
# MAGIC
# MAGIC A `select` node is wrapped as a temp view; a `statement` node (a table target's
# MAGIC `INSERT`) is emitted verbatim — wrapping an `INSERT` in a view is invalid SQL.

# COMMAND ----------

lines = [
    "# Databricks notebook source",
    f"# Generated from SSIS package: {pkg['package']}",
    "# IR-grounded forward generation (Foundation Model API)",
    "",
]
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
                  'spark.sql("""',
                  sql or "-- (no output generated)",
                  '""")', ""]
    else:
        body = sql or "SELECT /* no output generated */ 1 WHERE 1=0"
        lines += [f"# {p['dataflow']} :: {p['node']}  ->  view {p['view']}",
                  f'spark.sql("""CREATE OR REPLACE TEMP VIEW {p["view"]} AS',
                  body,
                  '""")', ""]
notebook_src = "\n".join(lines)

with open(OUT_PATH, "w") as f:
    f.write(notebook_src)
print(f"Wrote generated notebook to: {OUT_PATH}\n")
print(notebook_src)
