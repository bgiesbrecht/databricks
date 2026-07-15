"""
annotation_parser.py — Oracle annotation parser (JSON value format, 2026-07 spec).

Turns rows from the `oracle_annotations` registry (as produced by the Oracle -> UC metadata
sync, or exported to CSV) into typed, structured annotations grouped per UC object. Intent:
feed downstream consumers — Genie / Select AI space instructions, join hints, named SQL
expressions, and sample queries (see hooks/genie_push and annotation_to_genie).

Annotation grammar (JSON value spec, 2026-07)
--------------------------------------------------------
The annotation VALUE is now a JSON object that mirrors the Databricks structure. The
`annotation_name` prefix selects the shape:

  foreign_key            (column)  {"left_table","right_table","join_condition","left_column",
                                    "right_column","relationship","Instructions","Type":"Join"}
  sql_expression_<label> (table)   {"name","code","synonyms","instructions","Type":"Filter"|...}
  sample_query_<label>   (table)   {"name","question","query"}

Notes / hazards this parser defends against:
  * The authored JSON can be INVALID — `instructions` often contains unescaped double quotes
    (e.g. searches for "Contracts"). We try strict json.loads first, then fall back to a
    lenient flat-object parser and set `parse_note` so the caller can flag it back to the source.
  * Key casing is inconsistent (Instructions/Type vs instructions) — keys are matched
    case-insensitively.
  * Table names in the JSON carry no schema/catalog — UC resolution happens in the renderer.
  * System instructions are NOT annotations here; they live in the AI_GUIDANCE table COMMENT
    and flow through the comments leg (Genie reads UC comments directly).

Usable two ways:
  * locally against a CSV export:   parse_csv("annotations.csv")
  * inside the sync notebook:        parse_rows(spark.table(f"{MS}.oracle_annotations").collect())
Both dict rows and attribute-style Spark Row objects are accepted.
"""

from __future__ import annotations

import csv
import json
import re
from dataclasses import dataclass, field, asdict
from typing import Any, Optional

FK_ANNOTATION = "foreign_key"
SQL_EXPRESSION_PREFIX = "sql_expression"   # sql_expression_<label>
SAMPLE_QUERY_PREFIX = "sample_query"       # sample_query_<label>

# Human relationship phrasing -> Genie relationship_type suffix.
_REL_MAP = {
    "many to one": "MANY_TO_ONE", "one to many": "ONE_TO_MANY",
    "one to one": "ONE_TO_ONE",   "many to many": "MANY_TO_MANY",
}
_DEFAULT_REL = "MANY_TO_ONE"

_KEY_RE = re.compile(r'"([A-Za-z_]\w*)"\s*:\s*')


# --------------------------------------------------------------------------------------
# Typed model
# --------------------------------------------------------------------------------------
@dataclass
class ForeignKey:
    left_table: str
    left_column: str
    right_table: str
    right_column: str
    join_condition: str = "="
    relationship: str = _DEFAULT_REL          # normalized (MANY_TO_ONE, ...)
    instructions: str = ""
    raw_relationship: str = ""
    parse_note: Optional[str] = None          # set when tolerant repair was needed
    raw: str = ""


@dataclass
class SqlExpression:
    label: str                                # suffix after sql_expression_
    name: str                                 # authored display name
    code: str                                 # the SQL fragment
    kind: str = "expression"                  # 'filter' | 'expression' (from JSON "Type")
    synonyms: list[str] = field(default_factory=list)
    instructions: str = ""
    raw_type: str = ""
    parse_note: Optional[str] = None
    raw: str = ""


@dataclass
class SampleQuery:
    label: str                                # suffix after sample_query_
    name: str
    question: str
    query: str                                # SQL (Oracle dialect as authored)
    parse_note: Optional[str] = None
    raw: str = ""


@dataclass
class UnknownAnnotation:
    name: str
    value: str
    level: str
    column: Optional[str]
    reason: str


@dataclass
class ObjectAnnotations:
    oracle_schema: str
    oracle_object: str
    uc_name: Optional[str]
    description: Optional[str] = None
    foreign_keys: list[ForeignKey] = field(default_factory=list)
    sql_expressions: list[SqlExpression] = field(default_factory=list)
    sample_queries: list[SampleQuery] = field(default_factory=list)
    unknown: list[UnknownAnnotation] = field(default_factory=list)


@dataclass
class ParsedAnnotations:
    objects: dict[str, ObjectAnnotations] = field(default_factory=dict)  # keyed by "SCHEMA.OBJECT"
    global_instructions: list[str] = field(default_factory=list)
    unknown: list[UnknownAnnotation] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "objects": {k: asdict(v) for k, v in self.objects.items()},
            "global_instructions": self.global_instructions,
            "unknown": [asdict(u) for u in self.unknown],
        }

    def repaired(self) -> list[str]:
        """Annotations that only parsed via the lenient fallback — worth fixing at the source."""
        out = []
        for key, oa in self.objects.items():
            for item in (*oa.foreign_keys, *oa.sql_expressions, *oa.sample_queries):
                if getattr(item, "parse_note", None):
                    label = getattr(item, "name", None) or getattr(item, "label", "?")
                    out.append(f"{key} [{label}]: {item.parse_note}")
        return out

    def summary(self) -> str:
        n_fk = sum(len(o.foreign_keys) for o in self.objects.values())
        n_sql = sum(len(o.sql_expressions) for o in self.objects.values())
        n_sq = sum(len(o.sample_queries) for o in self.objects.values())
        n_unk = len(self.unknown) + sum(len(o.unknown) for o in self.objects.values())
        return (f"{len(self.objects)} objects | {n_fk} foreign keys | {n_sql} sql expressions | "
                f"{n_sq} sample queries | {len(self.global_instructions)} global instructions | "
                f"{n_unk} unknown/invalid | {len(self.repaired())} repaired")


# --------------------------------------------------------------------------------------
# Field access (works for both dict rows and attribute-style Spark Row objects)
# --------------------------------------------------------------------------------------
def _get(row: Any, key: str) -> Optional[str]:
    val = row.get(key) if isinstance(row, dict) else getattr(row, key, None)
    if val is None:
        return None
    s = str(val).strip()
    return None if s == "" or s.lower() == "null" else s


def _ci_get(d: dict, *names: str) -> Optional[str]:
    """Case-insensitive lookup across candidate key names (Instructions vs instructions)."""
    lower = {k.lower(): v for k, v in d.items()}
    for n in names:
        v = lower.get(n.lower())
        if v is not None:
            return v if isinstance(v, str) else str(v)
    return None


# --------------------------------------------------------------------------------------
# Tolerant JSON: strict first, then a lenient flat-object parser
# --------------------------------------------------------------------------------------
def _strip_json_string(v: str) -> str:
    v = v.strip()
    if len(v) >= 2 and v[0] == '"' and v[-1] == '"':
        v = v[1:-1]
    return v.replace('\\"', '"').replace("\\n", "\n").replace("\\t", "\t")


def parse_json_tolerant(raw: str) -> "tuple[dict, Optional[str]]":
    """Parse a flat JSON object. Returns (dict, note). note is None on a clean strict parse,
    else a short string explaining the lenient repair.

    The lenient path handles the common source defect: unescaped double quotes inside string
    values. It locates top-level `"key":` positions and slices each value between them, so a
    stray quote in the middle of a value (not followed by a colon) can't fool it.
    """
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict):
            return obj, None
        return {}, "top-level JSON was not an object"
    except Exception:
        pass

    s = raw.strip()
    lb, rb = s.find("{"), s.rfind("}")
    if lb != -1 and rb > lb:
        s = s[lb + 1:rb]
    keys = list(_KEY_RE.finditer(s))
    if not keys:
        return {}, "not JSON / no keys found"
    out: dict = {}
    for i, m in enumerate(keys):
        start = m.end()
        end = keys[i + 1].start() if i + 1 < len(keys) else len(s)
        val = s[start:end].strip()
        if val.endswith(","):
            val = val[:-1].rstrip()
        out[m.group(1)] = _strip_json_string(val)
    return out, "repaired (lenient parse — invalid JSON at source)"


def _norm_relationship(s: Optional[str]) -> str:
    return _REL_MAP.get((s or "").strip().lower(), _DEFAULT_REL)


# --------------------------------------------------------------------------------------
# Value parsers (JSON object -> typed item)
# --------------------------------------------------------------------------------------
def parse_foreign_key(value: str) -> "tuple[Optional[ForeignKey], Optional[str]]":
    d, note = parse_json_tolerant(value)
    lt, lc = _ci_get(d, "left_table"), _ci_get(d, "left_column")
    rt, rc = _ci_get(d, "right_table"), _ci_get(d, "right_column")
    if not (lt and lc and rt and rc):
        return None, "foreign_key JSON missing left/right table or column"
    rel_raw = _ci_get(d, "relationship") or ""
    return ForeignKey(
        left_table=lt, left_column=lc, right_table=rt, right_column=rc,
        join_condition=(_ci_get(d, "join_condition") or "=").strip(),
        relationship=_norm_relationship(rel_raw), raw_relationship=rel_raw,
        instructions=_ci_get(d, "instructions") or "", parse_note=note, raw=value,
    ), None


def parse_sql_expression(name: str, value: str) -> "tuple[Optional[SqlExpression], Optional[str]]":
    d, note = parse_json_tolerant(value)
    code = _ci_get(d, "code", "sql")
    if not code:
        return None, "sql_expression JSON missing 'code'"
    raw_type = (_ci_get(d, "type") or "").strip()
    kind = "filter" if raw_type.lower() == "filter" else "expression"
    syn = _ci_get(d, "synonyms") or ""
    label = name[len(SQL_EXPRESSION_PREFIX) + 1:] if name.lower().startswith(SQL_EXPRESSION_PREFIX + "_") else name
    return SqlExpression(
        label=label, name=_ci_get(d, "name") or label, code=code, kind=kind,
        synonyms=[t.strip() for t in syn.split(",") if t.strip()],
        instructions=_ci_get(d, "instructions") or "", raw_type=raw_type,
        parse_note=note, raw=value,
    ), None


def parse_sample_query(name: str, value: str) -> "tuple[Optional[SampleQuery], Optional[str]]":
    d, note = parse_json_tolerant(value)
    question, query = _ci_get(d, "question"), _ci_get(d, "query", "sql")
    if not (question and query):
        return None, "sample_query JSON missing 'question' or 'query'"
    label = name[len(SAMPLE_QUERY_PREFIX) + 1:] if name.lower().startswith(SAMPLE_QUERY_PREFIX + "_") else name
    return SampleQuery(
        label=label, name=_ci_get(d, "name") or label,
        question=question, query=query, parse_note=note, raw=value,
    ), None


# --------------------------------------------------------------------------------------
# Row -> model
# --------------------------------------------------------------------------------------
def parse_rows(rows: list[Any], active_only: bool = True) -> ParsedAnnotations:
    out = ParsedAnnotations()

    for row in rows:
        if active_only:
            is_active = _get(row, "is_active")
            if is_active is not None and is_active.lower() in ("false", "0"):
                continue

        name = _get(row, "annotation_name")
        value = _get(row, "annotation_value") or ""
        if not name:
            continue

        schema = _get(row, "oracle_schema") or ""
        obj = _get(row, "oracle_object") or ""
        column = _get(row, "oracle_column")
        level = (_get(row, "level") or ("COLUMN" if column else "TABLE")).upper()
        uc_name = _get(row, "uc_name")
        name_l = name.lower()

        key = f"{schema}.{obj}"
        oa = out.objects.get(key)
        if oa is None:
            oa = ObjectAnnotations(oracle_schema=schema, oracle_object=obj, uc_name=uc_name)
            out.objects[key] = oa
        elif uc_name and not oa.uc_name:
            oa.uc_name = uc_name

        def flag(reason: str):
            oa.unknown.append(UnknownAnnotation(name, value, level, column, reason))

        if name_l == FK_ANNOTATION:
            item, err = parse_foreign_key(value)
            if item is None:
                flag(err)
            else:
                item.left_column = item.left_column or (column or "")
                oa.foreign_keys.append(item)

        elif name_l.startswith(SQL_EXPRESSION_PREFIX):
            item, err = parse_sql_expression(name, value)
            oa.sql_expressions.append(item) if item else flag(err)

        elif name_l.startswith(SAMPLE_QUERY_PREFIX):
            item, err = parse_sample_query(name, value)
            oa.sample_queries.append(item) if item else flag(err)

        elif name_l == "description":
            oa.description = value

        else:
            flag("unrecognized annotation_name")

    return out


def parse_csv(path: str, active_only: bool = True) -> ParsedAnnotations:
    with open(path, newline="", encoding="utf-8") as f:
        return parse_rows(list(csv.DictReader(f)), active_only=active_only)


if __name__ == "__main__":
    import sys
    path = sys.argv[1] if len(sys.argv) > 1 else "annotations.csv"
    parsed = parse_csv(path)
    print(parsed.summary(), file=sys.stderr)
    for r in parsed.repaired():
        print("  REPAIRED " + r, file=sys.stderr)
    print(json.dumps(parsed.to_dict(), indent=2))
