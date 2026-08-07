#!/usr/bin/env python3
"""
dtsx_to_ir.py — Extract SSIS transform logic from a .dtsx file into a
purpose-built, tool-agnostic intermediate representation (IR).

This is a *representation* extractor, not a code generator. It records what the
package does — control flow, data-flow graph, per-component transform logic
(lookup join keys + no-match disposition, sources, destinations), connections,
variables — as a JSON document you can diff, query, or re-express however you like.

Design goals vs. Lakebridge's analyzer JSON:
  * capture data-flow EDGES (analyzer only lists node counts)
  * capture lookup JOIN KEYS + no-match disposition (analyzer drops these)
  * capture connection managers, variables, precedence constraints
  * stay dependency-light: stdlib + lxml only, no native binary, no license

Usage:
    python3 dtsx_to_ir.py <file-or-dir> [--out out.json]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from lxml import etree

DTS = "www.microsoft.com/SqlServer/Dts"
NS = {"DTS": DTS}


def _dts(el: etree._Element, attr: str) -> str | None:
    return el.get(f"{{{DTS}}}{attr}")


# SSIS enum decodings (from MS docs) so the IR is human-readable, not magic ints.
_NO_MATCH = {"0": "fail_component", "1": "ignore_failure", "2": "redirect_to_error_output", "3": "redirect_to_no_match_output"}
_CACHE_TYPE = {"0": "full", "1": "partial", "2": "none"}
_EVAL_OP = {"0": "Constraint", "1": "Expression", "2": "ExpressionAndConstraint", "3": "ExpressionOrConstraint"}
_VALUE = {"0": "Success", "1": "Failure", "2": "Completion"}


def _prop(component: etree._Element, name: str) -> str | None:
    for p in component.iter("property"):
        if p.get("name") == name:
            return p.text
    return None


def parse_connections(root: etree._Element) -> list[dict[str, Any]]:
    out = []
    for cm in root.findall(".//DTS:ConnectionManager", NS):
        name = _dts(cm, "ObjectName")
        if not name:
            continue
        conn = cm.find(".//DTS:ConnectionManager", NS)
        out.append({
            "name": name,
            "type": _dts(cm, "CreationName"),
            "connectionString": _dts(conn, "ConnectionString") if conn is not None else None,
        })
    return out


def parse_variables(root: etree._Element) -> list[dict[str, Any]]:
    out = []
    for v in root.findall(".//DTS:Variable", NS):
        vv = v.find("DTS:VariableValue", NS)
        out.append({
            "name": _dts(v, "ObjectName"),
            "namespace": _dts(v, "Namespace"),
            "value": (vv.text if vv is not None else None),
        })
    return out


def parse_lookup(component: etree._Element) -> dict[str, Any]:
    """Extract the transform logic the naive transpile loses: join keys + disposition."""
    join_cols = [ic.get("cachedName") or ic.get("name")
                 for ic in component.iter("inputColumn")
                 if (ic.get("cachedName") or ic.get("name"))]
    return {
        "referenceQuery": (_prop(component, "SqlCommand") or "").strip() or None,
        # SqlCommandParam holds the parameterized form incl. the WHERE join predicate
        "parameterizedQuery": (_prop(component, "SqlCommandParam") or "").strip() or None,
        "joinInputColumns": join_cols,
        "cacheType": _CACHE_TYPE.get(_prop(component, "CacheType") or "", _prop(component, "CacheType")),
        "noMatchBehavior": _NO_MATCH.get(_prop(component, "NoMatchBehavior") or "", _prop(component, "NoMatchBehavior")),
    }


def parse_flatfile_source(component: etree._Element) -> dict[str, Any]:
    cols = [oc.get("name") for oc in component.iter("outputColumn") if oc.get("name")]
    return {"outputColumns": cols}


def parse_destination(component: etree._Element) -> dict[str, Any]:
    return {
        "openRowset": _prop(component, "OpenRowset"),
        "accessMode": _prop(component, "AccessMode"),
        "sqlCommand": (_prop(component, "SqlCommand") or None),
    }


def parse_flatfile_destination(component: etree._Element) -> dict[str, Any]:
    """Flat File destination: writes the input stream out to a file (via a flat-file
    connection manager). In Spark this is a DataFrame write to a Volume path."""
    conn = None
    for cm in component.iter("connection"):
        conn = cm.get("connectionManagerID") or cm.get("connectionManagerRefId")
        if conn:
            break
    return {"connectionManagerID": conn, "header": _prop(component, "Header")}


def _col_prop(col: etree._Element, name: str) -> str | None:
    for p in col.iter("property"):
        if p.get("name") == name:
            return (p.text or "").strip() or None
    return None


def parse_oledb_source(component: etree._Element) -> dict[str, Any]:
    """OLE DB source: either a table (OpenRowset) or a SqlCommand query."""
    return {
        "openRowset": _prop(component, "OpenRowset"),
        "sqlCommand": (_prop(component, "SqlCommand") or "").strip() or None,
        "accessMode": _prop(component, "AccessMode"),
    }


def parse_derived_column(component: etree._Element) -> dict[str, Any]:
    """Derived Column: each derived outputColumn carries an SSIS Expression and a
    human-readable FriendlyExpression (e.g. `(City != CURRENTCity) ? 1 : 0`)."""
    derivations = []
    for oc in component.iter("outputColumn"):
        expr = _col_prop(oc, "FriendlyExpression") or _col_prop(oc, "Expression")
        if oc.get("name") and expr:
            derivations.append({"column": oc.get("name"), "expression": expr})
    return {"derivations": derivations}


def parse_conditional_split(component: etree._Element) -> dict[str, Any]:
    """Conditional Split: each named <output> carries a boolean FriendlyExpression and
    an EvaluationOrder; one output is the default (IsDefaultOut)."""
    conditions = []
    default_output = None
    for o in component.iter("output"):
        name = o.get("name")
        is_default = False
        expr = order = None
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
            conditions.append({"output": name, "condition": expr,
                               "evaluationOrder": order})
    conditions.sort(key=lambda c: int(c["evaluationOrder"]) if (c["evaluationOrder"] or "").lstrip("-").isdigit() else 0)
    return {"conditions": conditions, "defaultOutput": default_output}


def parse_oledb_command(component: etree._Element) -> dict[str, Any]:
    """OLE DB Command: a parameterized SQL statement run per row (? are input params)."""
    return {"sqlCommand": (_prop(component, "SqlCommand") or "").strip() or None}


# SSIS Aggregate AggregationType enum -> SQL aggregate function.
_AGG_FUNC = {"0": "group_by", "1": "SUM", "2": "COUNT", "3": "COUNT_DISTINCT",
             "4": "MIN", "5": "MAX", "7": "AVG"}


def parse_aggregate(component: etree._Element) -> dict[str, Any]:
    """Aggregate: output columns are either group-by keys or aggregate functions."""
    group_by, aggregates = [], []
    for oc in component.iter("outputColumn"):
        name = oc.get("name")
        if not name:
            continue
        atype = _col_prop(oc, "AggregationType")
        func = _AGG_FUNC.get(atype or "", None)
        if func == "group_by":
            group_by.append(name)
        elif func:
            aggregates.append({"column": name, "function": func})
    return {"groupBy": group_by, "aggregates": aggregates}


def parse_union_all(component: etree._Element) -> dict[str, Any]:
    """Union All: concatenates all inputs; output columns are the unified schema."""
    return {"outputColumns": [oc.get("name") for oc in component.iter("outputColumn")
                              if oc.get("name")]}


def parse_multicast(component: etree._Element) -> dict[str, Any]:
    """Multicast: fans one input to N identical outputs (no transform)."""
    return {"passthrough": True}


# SSIS Merge Join JoinType enum -> SQL join.
_MJ_JOIN = {"0": "FULL OUTER JOIN", "1": "LEFT JOIN", "2": "INNER JOIN"}

# SSIS SSISDataType codes -> Spark SQL types (common subset).
_SSIS_TYPE = {
    "i1": "TINYINT", "i2": "SMALLINT", "i4": "INT", "i8": "BIGINT",
    "ui1": "SMALLINT", "ui2": "INT", "ui4": "BIGINT", "ui8": "BIGINT",
    "r4": "FLOAT", "r8": "DOUBLE", "numeric": "DECIMAL(38,10)", "decimal": "DECIMAL(38,10)",
    "cy": "DECIMAL(19,4)", "bool": "BOOLEAN",
    "str": "STRING", "wstr": "STRING", "text": "STRING", "ntext": "STRING",
    "dbdate": "DATE", "date": "DATE",
    "dbtimestamp": "TIMESTAMP", "dbtimestamp2": "TIMESTAMP", "dbtime": "STRING",
    "guid": "STRING",
}


def parse_merge_join(component: etree._Element) -> dict[str, Any]:
    """Merge Join: JoinType + the join key columns (SortKeyPosition on each input)."""
    jt = _MJ_JOIN.get(_prop(component, "JoinType") or "", _prop(component, "JoinType"))
    keys = {}
    for inp in component.iter("input"):
        iname = inp.get("name")
        for col in inp.iter("inputColumn"):
            pos = None
            for p in col.iter("property"):
                if p.get("name") in ("SortKeyPosition", "NewSortKeyPosition") and (p.text or "").strip():
                    pos = (p.text or "").strip()
            if pos and pos not in ("0",):
                keys.setdefault(iname, []).append(col.get("cachedName") or col.get("name"))
    out_cols = [oc.get("name") for oc in component.iter("outputColumn") if oc.get("name")]
    return {"joinType": jt, "joinKeysByInput": keys, "outputColumns": out_cols}


def parse_sort(component: etree._Element) -> dict[str, Any]:
    """Sort: ORDER BY the columns with a non-zero (New)SortKeyPosition, in that order."""
    keys = []
    for col in component.iter("inputColumn"):
        pos = None
        for p in col.iter("property"):
            if p.get("name") in ("NewSortKeyPosition", "SortKeyPosition") and (p.text or "").strip():
                pos = (p.text or "").strip()
        if pos and pos.lstrip("-").isdigit() and int(pos) != 0:
            keys.append((int(pos), col.get("cachedName") or col.get("name")))
    keys.sort(key=lambda k: abs(k[0]))
    return {"sortKeys": [{"column": c, "descending": pos < 0} for pos, c in keys]}


def parse_data_convert(component: etree._Element) -> dict[str, Any]:
    """Data Conversion: each output column CASTs a source column to a new type."""
    conversions = []
    for oc in component.iter("outputColumn"):
        name = oc.get("name")
        if not name or "Error Output" in (oc.getparent().getparent().get("name") or ""):
            # skip the error-output columns (ErrorCode/ErrorColumn)
            pass
        dt = oc.get("dataType")
        if name and dt:
            conversions.append({
                "column": name,
                "targetType": _SSIS_TYPE.get(dt, dt.upper()),
                "length": oc.get("length"),
            })
    return {"conversions": conversions}


# SSIS SCD column-role enum.
_SCD_COLTYPE = {"1": "business_key", "2": "fixed", "3": "changing"}


def parse_scd(component: etree._Element) -> dict[str, Any]:
    """Slowly Changing Dimension wizard: classify input columns by role and record
    Type-1 (changing) vs Type-2 (historical) behavior + the dimension table."""
    keys, changing, fixed = [], [], []
    for col in component.iter("inputColumn"):
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
    # UpdateChangingAttributeHistory=true => Type 2 (new row); false => Type 1 (in-place)
    hist = (_prop(component, "UpdateChangingAttributeHistory") or "").lower() == "true"
    return {
        "businessKeys": keys,
        "changingAttributes": changing,
        "fixedAttributes": fixed,
        "historyType": "type2_historical" if hist else "type1_inplace",
        "currentRowWhere": _prop(component, "CurrentRowWhere"),
        "outputs": [o.get("name") for o in component.iter("output")],
    }


def parse_script_component(component: etree._Element) -> dict[str, Any]:
    """Script Component (managed C#/VB). Arbitrary code — cannot be auto-translated
    reliably; capture everything needed for a human to port it, and the output contract."""
    src = _prop(component, "SourceCode")
    out_cols = []
    for o in component.iter("output"):
        for oc in o.iter("outputColumn"):
            if oc.get("name"):
                out_cols.append(oc.get("name"))
    return {
        "scriptLanguage": _prop(component, "ScriptLanguage"),
        "readOnlyVariables": _prop(component, "ReadOnlyVariables"),
        "readWriteVariables": _prop(component, "ReadWriteVariables"),
        "outputColumns": out_cols,
        "sourceCode": (src[:4000] if src else None),  # cap; scripts can be large
        "requiresManualReview": True,
    }


def parse_row_count(component: etree._Element) -> dict[str, Any]:
    """Row Count: counts rows passing through, stores into a package variable."""
    return {"variableName": _prop(component, "VariableName")}


def parse_fuzzy_lookup(component: etree._Element) -> dict[str, Any]:
    """Fuzzy Lookup: approximate match against a reference table with a similarity
    threshold. Captures the reference table, join columns, and threshold."""
    join_cols = [ic.get("cachedName") or ic.get("name")
                 for ic in component.iter("inputColumn") if (ic.get("cachedName") or ic.get("name"))]
    return {
        "referenceTable": _prop(component, "ReferenceTableName") or _prop(component, "OpenRowset"),
        "joinInputColumns": join_cols,
        "minSimilarity": _prop(component, "MinSimilarity"),
        "maxOutputMatches": _prop(component, "MaxOutputMatchesPerInput"),
    }


def parse_cache_transform(component: etree._Element) -> dict[str, Any]:
    """Cache Transform: writes its input into an in-memory cache (later read by a
    cache-connected Lookup). In Spark this is just a persisted/broadcast temp view."""
    cols = [oc.get("name") for oc in component.iter("outputColumn") if oc.get("name")]
    return {"cachedColumns": cols,
            "connectionManagerID": _prop(component, "ConnectionManagerID")}


def parse_unpivot(component: etree._Element) -> dict[str, Any]:
    """Unpivot: wide->long. Each pivoted input column has a PivotKeyValue (the label)
    and a DestinationColumn (where its value lands). Passthrough cols have neither."""
    def _clean(v):  # DestinationColumn may be a raw #{lineage-id} ref in real packages
        return None if (v is None or v.startswith("#{")) else v

    pivoted, passthrough = [], []
    dest_hint = None
    for col in component.iter("inputColumn"):
        nm = col.get("cachedName") or col.get("name")
        pkv = _col_prop(col, "PivotKeyValue")
        if pkv is not None:
            pivoted.append({"sourceColumn": nm, "keyValue": pkv})
            dest_hint = dest_hint or _clean(_col_prop(col, "DestinationColumn"))
        else:
            passthrough.append(nm)
    # The two NEW output columns (not passthrough) are the key column (holds the label)
    # and the value column (holds the unpivoted measure). Derive both from the output —
    # do NOT trust DestinationColumn, which is a lineage ref in real packages.
    out_cols = [oc.get("name") for oc in component.iter("outputColumn")
                if oc.get("name") and not (oc.get("name") or "").startswith("#{")]
    new_cols = [c for c in out_cols if c not in passthrough]
    key_col = new_cols[0] if len(new_cols) >= 1 else "PivotKey"
    value_col = dest_hint or (new_cols[1] if len(new_cols) >= 2 else "PivotValue")
    return {"pivotedColumns": pivoted, "passthroughColumns": passthrough,
            "keyColumn": key_col, "valueColumn": value_col}


def parse_pivot(component: etree._Element) -> dict[str, Any]:
    """Pivot: long->wide. Capture the set/key/value column roles and output columns.
    PivotUsage on input columns: 0=passthrough, 1=set (group), 2=pivot key, 3=value."""
    roles = {"0": [], "1": [], "2": [], "3": []}
    for col in component.iter("inputColumn"):
        nm = col.get("cachedName") or col.get("name")
        usage = _col_prop(col, "PivotUsage") or "0"
        roles.setdefault(usage, []).append(nm)
    return {
        "setColumns": roles.get("1", []),      # group-by / row identity
        "pivotKeyColumns": roles.get("2", []), # column whose values become new columns
        "valueColumns": roles.get("3", []),    # measure
        "outputColumns": [oc.get("name") for oc in component.iter("outputColumn") if oc.get("name")],
    }


_PARSERS = {
    "Microsoft.Lookup": parse_lookup,
    "Microsoft.FlatFileSource": parse_flatfile_source,
    "Microsoft.OLEDBSource": parse_oledb_source,
    "Microsoft.OLEDBDestination": parse_destination,
    "Microsoft.FlatFileDestination": parse_flatfile_destination,
    "Microsoft.DerivedColumn": parse_derived_column,
    "Microsoft.ConditionalSplit": parse_conditional_split,
    "Microsoft.OLEDBCommand": parse_oledb_command,
    "Microsoft.Aggregate": parse_aggregate,
    "Microsoft.UnionAll": parse_union_all,
    "Microsoft.Multicast": parse_multicast,
    "Microsoft.MergeJoin": parse_merge_join,
    "Microsoft.Sort": parse_sort,
    "Microsoft.DataConvert": parse_data_convert,
    "Microsoft.SCD": parse_scd,
    "Microsoft.ManagedComponentHost": parse_script_component,
    "Microsoft.RowCount": parse_row_count,
    "Microsoft.UnPivot": parse_unpivot,
    "Microsoft.Pivot": parse_pivot,
    "Microsoft.FuzzyLookup": parse_fuzzy_lookup,
    "Microsoft.Cache": parse_cache_transform,
}


def _short(ref_id: str | None) -> str | None:
    """Trim the verbose 'Package\\DataFlow\\Component' refId to the leaf name."""
    if not ref_id:
        return ref_id
    tail = ref_id.split("\\")[-1]
    return tail.split(".")[0]


def parse_dataflow(executable: etree._Element) -> dict[str, Any] | None:
    """A Data Flow Task: extract components (nodes) and paths (edges).

    Only the executable's OWN pipeline (under its direct DTS:ObjectData) counts —
    a container (e.g. ForeachLoop) has no pipeline itself; its child Data Flow
    Tasks do, and those are handled separately as nested executables.
    """
    pipeline = executable.find("DTS:ObjectData/pipeline", NS)
    if pipeline is None:
        return None

    nodes = []
    for comp in pipeline.iter("component"):
        cls = comp.get("componentClassID")
        node = {
            "name": comp.get("name"),
            "refId": comp.get("refId"),
            "componentType": cls,
            "role": {"Microsoft.FlatFileSource": "source",
                     "Microsoft.OLEDBSource": "source",
                     "Microsoft.OLEDBDestination": "target",
                     "Microsoft.FlatFileDestination": "target",
                     "Microsoft.Lookup": "transform"}.get(cls, "transform"),
        }
        parser = _PARSERS.get(cls)
        if parser:
            node["logic"] = parser(comp)
        nodes.append(node)

    edges = []
    for p in pipeline.iter("path"):
        edges.append({
            "name": p.get("name"),
            "from": _short(p.get("startId")),
            "to": _short(p.get("endId")),
            # 'Match Output' vs 'Error Output' / 'No Match Output' is the disposition path
            "fromPort": (p.get("startId") or "").split("Outputs[")[-1].rstrip("]") if "Outputs[" in (p.get("startId") or "") else None,
        })

    return {"name": _dts(executable, "ObjectName"), "type": "DataFlow", "nodes": nodes, "edges": edges}


def _clean_expr(val: str | None) -> str | None:
    """SSIS wraps literal strings in embedded double-quotes; strip them."""
    if val is None:
        return None
    v = val.strip()
    if len(v) >= 2 and v[0] == '"' and v[-1] == '"':
        v = v[1:-1]
    return v


_FOREACH_KIND = {
    "Microsoft.ForEachFileEnumerator": "file",
    "Microsoft.ForEachADOEnumerator": "ado",         # loop over a recordset (a variable holding rows)
    "Microsoft.ForEachItemEnumerator": "item",       # loop over a hardcoded item collection
    "Microsoft.ForEachNodeListEnumerator": "nodelist",
    "Microsoft.ForEachFromVarEnumerator": "fromvar",
    "Microsoft.ForEachSMOEnumerator": "smo",
}


def parse_foreach(executable: etree._Element) -> dict[str, Any] | None:
    """Extract a ForEach loop's enumerator spec. File enumerators get full detail;
    other kinds (ADO recordset, item, nodelist, ...) get a generic spec so codegen can
    still represent the iteration and flag what needs manual attention."""
    en = executable.find(".//DTS:ForEachEnumerator", NS)
    if en is None:
        return None
    kind = _FOREACH_KIND.get(_dts(en, "CreationName") or "", "other")

    # Variable(s) each iteration populates (ValueIndex order = column order for ADO).
    var_maps = []
    for vm in executable.findall(".//DTS:ForEachVariableMapping", NS):
        if _dts(vm, "VariableName"):
            var_maps.append({"variable": _dts(vm, "VariableName"),
                             "valueIndex": _dts(vm, "ValueIndex") or vm.get("ValueIndex")})
    var0 = var_maps[0]["variable"] if var_maps else None

    if kind == "file":
        props = {p.get("Folder") is not None and "Folder"
                 or p.get("FileSpec") is not None and "FileSpec"
                 or p.get("Recurse") is not None and "Recurse" or "": p
                 for p in en.iter("FEFEProperty")}
        folder = _clean_expr(props.get("Folder").get("Folder")) if props.get("Folder") is not None else None
        filespec = props.get("FileSpec").get("FileSpec") if props.get("FileSpec") is not None else None
        recurse = (props.get("Recurse").get("Recurse") if props.get("Recurse") is not None else "0") == "1"
        return {"enumerator": "file", "folder": folder, "filePattern": filespec,
                "recurse": recurse, "fileNameVariable": var0}

    if kind == "ado":
        # The recordset lives in a package variable named in FEADOProperty VariableName.
        src_var = None
        for fp in en.iter("FEADOProperty"):
            if fp.get("VariableName"):
                src_var = fp.get("VariableName")
                break
        return {"enumerator": "ado",
                "sourceVariable": src_var or _dts(en, "VariableName"),
                "variableMappings": var_maps}

    return {"enumerator": kind, "variableMappings": var_maps}


def _find_attr_anywhere(el: etree._Element, suffix: str) -> str | None:
    """Return the first attribute value whose local-name ends with `suffix`, scanning
    the element and its descendants (control-flow tasks bury data in namespaced attrs)."""
    for node in el.iter():
        for k, v in node.attrib.items():
            if k.split("}")[-1].endswith(suffix) and v:
                return v
    return None


def parse_task(executable: etree._Element, creation: str) -> dict[str, Any] | None:
    """Extract the logic of a control-flow TASK (not a data flow). Returns a `task`
    dict, or None if this executable isn't a task we specifically handle."""
    if creation == "Microsoft.ExecuteSQLTask":
        return {"type": "execute_sql",
                "sql": _find_attr_anywhere(executable, "SqlStatementSource"),
                "connection": _find_attr_anywhere(executable, "Connection")}
    if creation == "Microsoft.ExecutePackageTask":
        return {"type": "execute_package",
                "packageName": _find_attr_anywhere(executable, "PackageName")
                               or _find_attr_anywhere(executable, "RefName")}
    if creation == "Microsoft.ScriptTask":
        return {"type": "script_task",
                "scriptLanguage": _find_attr_anywhere(executable, "ScriptLanguage"),
                "requiresManualReview": True}
    if creation == "Microsoft.ExecuteProcess":
        return {"type": "execute_process",
                "executable": _find_attr_anywhere(executable, "Executable"),
                "arguments": _find_attr_anywhere(executable, "Arguments"),
                "requiresManualReview": True}
    if creation in ("Microsoft.FileSystemTask", "Microsoft.XMLTask",
                    "Microsoft.WmiDataReaderTask", "Microsoft.BulkInsertTask",
                    "Microsoft.DataProfilingTask"):
        return {"type": "other_task", "taskKind": creation, "requiresManualReview": True}
    return None


def parse_control_flow(root: etree._Element) -> list[dict[str, Any]]:
    """Top-level executables + precedence constraints (control-flow graph)."""
    items = []
    # direct child executables of the package
    for ex in root.findall("DTS:Executables/DTS:Executable", NS):
        creation = _dts(ex, "CreationName") or ""
        entry = {"name": _dts(ex, "ObjectName"), "creationName": creation}
        df = parse_dataflow(ex)
        if df:
            entry["dataFlow"] = df
        # ForeachLoop: capture the enumerator spec so codegen can drive a per-file read
        if creation == "STOCK:FOREACHLOOP":
            loop = parse_foreach(ex)
            if loop:
                entry["loop"] = loop
        # Control-flow TASK (Execute SQL, Execute Package, Script, ...) — capture its logic
        task = parse_task(ex, creation)
        if task:
            entry["task"] = task
        # nested executables (e.g. ForeachLoop / Sequence bodies)
        nested = []
        for n in ex.findall("DTS:Executables/DTS:Executable", NS):
            ncreation = _dts(n, "CreationName") or ""
            child = {"name": _dts(n, "ObjectName"), "creationName": ncreation,
                     "dataFlow": parse_dataflow(n)}
            ntask = parse_task(n, ncreation)
            if ntask:
                child["task"] = ntask
            nested.append(child)
        if nested:
            entry["children"] = nested
        items.append(entry)

    constraints = []
    for pc in root.findall(".//DTS:PrecedenceConstraint", NS):
        constraints.append({
            "from": _dts(pc, "From"),
            "to": _dts(pc, "To"),
            "value": _VALUE.get(_dts(pc, "Value") or "", _dts(pc, "Value")),
            "evalOp": _EVAL_OP.get(_dts(pc, "EvalOp") or "", _dts(pc, "EvalOp")),
            "expression": _dts(pc, "Expression"),
        })
    return items, constraints


def extract(dtsx_path: Path) -> dict[str, Any]:
    tree = etree.parse(str(dtsx_path))
    root = tree.getroot()
    executables, constraints = parse_control_flow(root)
    return {
        "package": _dts(root, "ObjectName") or dtsx_path.stem,
        "sourceFile": dtsx_path.name,
        "connections": parse_connections(root),
        "variables": parse_variables(root),
        "controlFlow": {"executables": executables, "precedenceConstraints": constraints},
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Extract SSIS .dtsx transform logic into a JSON IR.")
    ap.add_argument("path", help=".dtsx file or a directory of them")
    ap.add_argument("--out", help="output JSON path (default: stdout)")
    args = ap.parse_args()

    target = Path(args.path)
    files = sorted(target.glob("*.dtsx")) if target.is_dir() else [target]
    if not files:
        print(f"No .dtsx files found at {target}", file=sys.stderr)
        return 1

    packages = [extract(f) for f in files]
    result = packages[0] if len(packages) == 1 else {"packages": packages}
    text = json.dumps(result, indent=2, ensure_ascii=False)

    if args.out:
        Path(args.out).write_text(text, encoding="utf-8")
        print(f"IR written to: {args.out}", file=sys.stderr)
    else:
        print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
