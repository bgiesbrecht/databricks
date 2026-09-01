#!/usr/bin/env python3
"""Verification + fix/re-verify loop for generated Spark SQL.

Tiers (increasing cost):
  structural  - no Spark, no data: completeness/placeholder scan, DAG integrity, optional
                sqlglot parse. Catches the "incomplete SQL" class of bug directly (the empty
                error-handling cell we hit on Lesson 6).
  compile     - (stub here) resolve each statement against schema-only temp views in Spark.
  behavioral  - (stub here) run on sample data, compare to golden.

The fix loop regenerates ONLY failing LLM-generated cells, feeding the failure back into the
original authoritative grounding. Presolved (deterministic) cells are pinned: if one fails it
is a bug in our parser/grounding, surfaced as a hard finding, never sent to the loop.

NOTE on placeholders: SSIS grounding intentionally emits angle-bracket user-fill markers for
things not in the package metadata (e.g. `<dimension_table>` in an SCD MERGE, `<destination>`).
Those are documented limitations a human sets, NOT incomplete-cell bugs, so they are deliberately
NOT in `_PLACEHOLDERS` — we only flag markers that mean the generator itself left a hole.
"""
from __future__ import annotations

import re
from typing import Callable

# Markers that mean the generator left a hole (a genuinely incomplete cell).
_PLACEHOLDERS = ["{UPSTREAM}", "/* upstream view */", "no output generated"]
_SQL_KEYWORDS = ("select", "insert", "merge", "with", "update", "delete", "create")
_VIEW_RE = re.compile(r"\bv_[a-z0-9_]+\b")


def build_cells(prompts: list[dict], results: dict[str, str]) -> list[dict]:
    """Flatten prompts+results into ordered cells: {node, view, kind, body, presolved}."""
    from ir_to_sparksql import _normalize_sql
    cells = []
    for p in prompts:
        body = p["presolved"] if p.get("presolved") else _normalize_sql(results.get(p["node"], "") or "")
        cells.append({"node": p["node"], "view": p["view"], "kind": p.get("kind"),
                      "body": body, "presolved": bool(p.get("presolved"))})
    return cells


def _strip_comments(body: str) -> str:
    return "\n".join(l for l in body.splitlines() if not l.strip().startswith("--")).strip()


def _try_sqlglot(sql: str, kind: str) -> str | None:
    try:
        import sqlglot
    except Exception:
        return None  # optional dependency; skip parse if absent
    wrapped = sql if kind == "statement" else f"CREATE OR REPLACE TEMP VIEW _v AS {sql}"
    try:
        sqlglot.parse_one(wrapped, read="spark")
        return None
    except Exception as e:  # sqlglot.errors.ParseError and friends
        return f"sqlglot parse error: {e}"


def verify_cells(cells: list[dict]) -> list[dict]:
    """Tier-1 structural checks. Returns findings [{node, tier, category, error, presolved, sql}]."""
    findings = []
    defined: set[str] = set()  # views defined by earlier cells
    for c in cells:
        body = c["body"]
        no_comments = _strip_comments(body)

        # 1) completeness / placeholder scan (the "incomplete SQL" bug)
        hit = next((m for m in _PLACEHOLDERS if m in body), None)
        if hit:
            findings.append(_f(c, "incomplete", f"unresolved placeholder '{hit}'"))
        elif not no_comments or not any(k in no_comments.lower() for k in _SQL_KEYWORDS):
            findings.append(_f(c, "incomplete", "empty or comment-only body (no SQL statement)"))
        else:
            # 2) unbalanced parentheses (cheap truncation signal)
            if no_comments.count("(") != no_comments.count(")"):
                findings.append(_f(c, "syntax", "unbalanced parentheses"))
            # 3) DAG integrity: referenced v_* views must be defined by an earlier cell
            refs = {v for v in _VIEW_RE.findall(no_comments) if v != c["view"]}
            missing = sorted(refs - defined)
            if missing:
                findings.append(_f(c, "dag", f"references undefined upstream view(s): {missing}"))
            # 4) optional formal parse
            err = _try_sqlglot(no_comments, c["kind"])
            if err:
                findings.append(_f(c, "syntax", err))

        if c["kind"] != "statement":
            defined.add(c["view"])
    return findings


def _f(cell: dict, category: str, error: str) -> dict:
    return {"node": cell["node"], "tier": "structural", "category": category,
            "error": error, "presolved": cell["presolved"], "sql": cell["body"]}


def _corrective_prompt(original_user_prompt: str, finding: dict) -> str:
    return (f"{original_user_prompt}\n\n"
            f"Your previous attempt FAILED verification ({finding['category']}): {finding['error']}\n"
            f"You produced:\n{finding['sql']}\n\n"
            f"Return corrected Spark SQL only.")


def verify_and_fix(pkg: dict, prompts: list[dict], results: dict[str, str],
                   generate: Callable[[str, str], str], system_prompt: str,
                   assemble_fn, tier: str = "structural", max_iters: int = 3):
    """Verify; regenerate failing LLM cells with the failure fed back; repeat. Returns
    (notebook_text, residual_findings, iters_used)."""
    prompt_by_node = {p["node"]: p for p in prompts}
    iters = 0
    findings = verify_cells(build_cells(prompts, results))
    while iters < max_iters:
        fixable = [f for f in findings if not f["presolved"]]
        if not fixable:
            break
        for f in fixable:
            p = prompt_by_node[f["node"]]
            results[f["node"]] = generate(system_prompt, _corrective_prompt(p["user_prompt"], f))
        iters += 1
        findings = verify_cells(build_cells(prompts, results))
    notebook = assemble_fn(pkg, prompts, results)
    return notebook, findings, iters


def format_report(findings: list[dict], iters: int) -> str:
    if not findings:
        return f"[verify] PASS — 0 findings after {iters} fix iteration(s)."
    lines = [f"[verify] {len(findings)} finding(s) after {iters} fix iteration(s):"]
    for f in findings:
        pin = " (PINNED deterministic cell — parser/grounding bug)" if f["presolved"] else ""
        lines.append(f"  - {f['node']} [{f['category']}]: {f['error']}{pin}")
    return "\n".join(lines)


if __name__ == "__main__":  # verify a saved notebook file (structural only)
    import sys
    text = open(sys.argv[1]).read()
    # Reconstruct minimal cells from a generated notebook for standalone checking.
    cells, cur = [], None
    for line in text.splitlines():
        if line.startswith("# COMMAND"):
            cur = None
        elif line.startswith("spark.sql("):
            cur = {"node": "?", "view": "?",
                   "kind": "statement" if line.strip() == 'spark.sql("""' else "select",
                   "body": [], "presolved": False}
        elif cur is not None and line.strip() == '""")':
            body = "\n".join(cur["body"])
            m = re.match(r"CREATE OR REPLACE TEMP VIEW (v_\w+) AS", body)
            if m:
                cur["view"] = m.group(1); cur["kind"] = "select"
                body = body[m.end():].strip()
            cur["body"] = body; cur["node"] = cur["view"]; cells.append(cur); cur = None
        elif cur is not None:
            cur["body"].append(line)
    print(format_report(verify_cells(cells), 0))
