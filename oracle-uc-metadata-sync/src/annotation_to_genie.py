"""
annotation_to_genie.py — render parsed Oracle annotations into a portable Genie config.

Second layer on top of `annotation_parser`: turns a ParsedAnnotations into the portable
config dict consumed by `update_genie_space.GenieSpaces.apply_config()` / `.create()`
(schema documented in update_genie_space.py, "Portable config → inner-doc applier").

Mapping (LLNL JSON annotation spec, 2026-07)
--------------------------------------------
  foreign_key             -> joins             relationship + join_condition come straight from
                                               the JSON; role-playing dims get unique aliases
  sql_expression (Filter) -> sql_filters       display_name=name, sql=code, synonyms authored
  sql_expression (other)  -> sql_expressions   (measures/derived columns)
  sample_query            -> examples          {question, sql: query}
  DESCRIPTION             -> text_instruction  (rare; system prompt normally lives in the
                                               AI_GUIDANCE table comment, not an annotation)

Only sections with content are emitted — matching genie_push.py's "touch only the sections
we produced" contract. `render_genie_config` returns (config, RenderReport); the report
counts each section, lists skips (e.g. an FK whose UC table couldn't be resolved), and
surfaces `repaired` items (annotations that only parsed via the lenient fallback — tell LLNL).

Integration:
    from annotation_parser import parse_csv
    from annotation_to_genie import render_genie_config
    config, report = render_genie_config(parse_csv("annotations.csv"))
    # then, in the workspace:  GenieSpaces(host, token).apply_config(space_id, config)
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Callable, Optional

from annotation_parser import ParsedAnnotations, ObjectAnnotations, ForeignKey

# A resolver maps an Oracle object name -> its Unity Catalog FQN (or None). When provided (e.g.
# wired to {MS}.resolve_uc_name in the hook) it is authoritative; otherwise the _guess_uc
# heuristic is used. Signature: (oracle_table_name) -> "catalog.schema.table" | None.
Resolver = Callable[[str], Optional[str]]

_REL_PREFIX = "FROM_RELATIONSHIP_TYPE_"
_BIND_RE = re.compile(r"(?<![:\w]):(\w+)")   # :name bind variables (Oracle-style)


def translate_oracle_sql(sql: str, catalog: str, schema: str,
                         oracle_schema: str) -> "tuple[str, list[str]]":
    """First-pass Oracle -> Databricks rewrite for sample-query SQL.

    Deterministic transforms only:
      * `<oracle_schema>.<table>`  ->  `<catalog>.<schema>.<table>` (lowercased)
    Reports (but cannot resolve) `:bind` variables — those need literal example values or
    Genie parameters, which we can't invent. Returns (translated_sql, unresolved_binds).
    Note: a genuine SQL transpile (functions, date arithmetic, outer-join syntax, etc.) is
    out of scope — this only fixes the schema qualification we know how to map safely.
    """
    out = sql
    if oracle_schema:
        pat = re.compile(rf"\b{re.escape(oracle_schema)}\.(\w+)", re.I)
        out = pat.sub(lambda m: f"{catalog}.{schema}.{m.group(1).lower()}", out)
    binds = sorted(set(_BIND_RE.findall(out)))
    return out, binds


@dataclass
class RenderReport:
    joins: int = 0
    sql_filters: int = 0
    sql_expressions: int = 0
    examples: int = 0
    instructions: int = 0
    skipped: list[str] = field(default_factory=list)
    repaired: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def summary(self) -> str:
        return (f"joins={self.joins} filters={self.sql_filters} expressions={self.sql_expressions} "
                f"examples={self.examples} instructions={self.instructions} "
                f"skipped={len(self.skipped)} repaired={len(self.repaired)} warnings={len(self.warnings)}")


def _guess_uc(uc_name: str, table: str) -> "str | None":
    """Derive a UC name for `table` by reusing an anchor UC name's catalog.schema and lowercasing.
    Assumes the referenced table lives in the same target schema with the default name convention;
    resolve authoritatively via {MS}.resolve_uc_name when wiring into the sync."""
    parts = (uc_name or "").split(".")
    if len(parts) < 3:
        return None
    return f"{parts[-3]}.{parts[-2]}.{table.lower()}"


def _unique_alias(base: str, used: set[str]) -> str:
    alias, n = base, 2
    while alias in used:
        alias = f"{base}_{n}"
        n += 1
    used.add(alias)
    return alias


def _render_joins_for_object(oa: ObjectAnnotations, report: RenderReport,
                             resolve: Optional[Resolver] = None) -> list[dict]:
    if not oa.foreign_keys:
        return []
    left_alias = oa.oracle_object.lower()
    used_aliases = {left_alias}
    joins = []
    for fk in oa.foreign_keys:
        # Resolver (if given) is authoritative; fall back to the row's uc_name / heuristic.
        left_uc = (resolve(oa.oracle_object) if resolve else None) or oa.uc_name
        right_uc = (resolve(fk.right_table) if resolve else None) or (
            _guess_uc(oa.uc_name, fk.right_table) if oa.uc_name else None)
        if not left_uc or not right_uc:
            report.skipped.append(f"FK {oa.oracle_object}.{fk.left_column} -> "
                                  f"{fk.right_table}.{fk.right_column}: unresolved UC name")
            continue
        op = fk.join_condition or "="
        right_alias = _unique_alias(fk.right_table.lower(), used_aliases)
        joins.append({
            "left":  {"table": left_uc,  "alias": left_alias},
            "right": {"table": right_uc, "alias": right_alias},
            "on": f"`{left_alias}`.`{fk.left_column.lower()}` {op} "
                  f"`{right_alias}`.`{fk.right_column.lower()}`",
            "relationship_type": _REL_PREFIX + fk.relationship,
            "instruction": fk.instructions or f"Foreign key from Oracle annotation on "
                                              f"{oa.oracle_schema}.{oa.oracle_object}.{fk.left_column}.",
        })
    return joins


def render_genie_config(parsed: ParsedAnnotations,
                        resolve: Optional[Resolver] = None) -> "tuple[dict, RenderReport]":
    """Render ParsedAnnotations into a portable Genie config dict + a RenderReport.

    `resolve` (optional) maps an Oracle table name to its UC FQN; when omitted, join targets use
    the row's uc_name plus a lowercase-name heuristic for referenced tables.
    """
    report = RenderReport()
    report.repaired = parsed.repaired()

    joins: list[dict] = []
    sql_filters: list[dict] = []
    sql_expressions: list[dict] = []
    examples: list[dict] = []

    for oa in parsed.objects.values():
        joins.extend(_render_joins_for_object(oa, report, resolve))

        for se in oa.sql_expressions:
            snippet = {"display_name": se.name, "sql": se.code,
                       "instruction": se.instructions, "synonyms": se.synonyms}
            (sql_filters if se.kind == "filter" else sql_expressions).append(snippet)

        for sq in oa.sample_queries:
            # Rewrite the Oracle schema prefix to the UC name (deterministic); flag bind vars.
            parts = (oa.uc_name or "").split(".")
            translated, binds = sq.query, []
            if len(parts) >= 3:
                translated, binds = translate_oracle_sql(
                    sq.query, parts[-3], parts[-2], oa.oracle_schema.lower())
            if binds:
                report.warnings.append(f"sample_query '{sq.name}': unresolved bind vars "
                                       f"{binds} — need literal example values or Genie parameters")
            examples.append({"question": sq.question, "sql": translated})

    instruction_lines = list(parsed.global_instructions)
    for oa in parsed.objects.values():
        if oa.description:
            instruction_lines.append(f"{oa.oracle_object}: {oa.description}")

    config: dict = {}
    if joins:
        config["joins"] = joins; report.joins = len(joins)
    if sql_filters:
        config["sql_filters"] = sql_filters; report.sql_filters = len(sql_filters)
    if sql_expressions:
        config["sql_expressions"] = sql_expressions; report.sql_expressions = len(sql_expressions)
    if examples:
        config["examples"] = examples; report.examples = len(examples)
    if instruction_lines:
        config["text_instruction"] = "\n".join(instruction_lines); report.instructions = len(instruction_lines)

    return config, report


if __name__ == "__main__":
    import sys, json
    from annotation_parser import parse_csv
    path = sys.argv[1] if len(sys.argv) > 1 else "annotations.csv"
    cfg, rep = render_genie_config(parse_csv(path))
    print(rep.summary(), file=sys.stderr)
    for s in rep.skipped:  print("  SKIP " + s, file=sys.stderr)
    for w in rep.warnings: print("  WARN " + w, file=sys.stderr)
    for r in rep.repaired: print("  REPAIRED " + r, file=sys.stderr)
    print(json.dumps(cfg, indent=2))
