#!/usr/bin/env python3
"""Verify/fix-loop regression tests for the SSIS tool — runnable offline (no LLM, no Spark).

    python3 tests/test_verify.py     # or: pytest tests/test_verify.py

Uses the real Lesson 6 IR (built via the notebook's stdlib parser, since lxml may be absent)
to exercise the verifier against actual grounded prompts, plus fault injection to prove the
fix/re-verify loop repairs an incomplete cell — the class of bug we hit on Lesson 6.
"""
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import ir_to_sparksql as G
import verify as V

LESSON6_DTSX = ROOT / "samples/external/SSIS-Examples/SSIS Tutorial1/SSIS Tutorial1/Lesson 6.dtsx"


def _load_pkg():
    """Parse Lesson 6 to IR via the notebook's stdlib parser (no lxml dependency)."""
    nb = ROOT / "notebooks/ir_to_sparksql_databricks.py"
    raw = nb.read_text()

    class _W:
        def get(self, k): return str(LESSON6_DTSX) if k == "dtsx_path" else ""
        def text(self, *a, **k): pass
        def dropdown(self, *a, **k): pass

    class _DB:
        widgets = _W()

    class _S:
        def sql(self, *a, **k): return None

    ns = {"spark": _S(), "dbutils": _DB(), "displayHTML": lambda *a, **k: None}
    for c in raw.split("# COMMAND ----------"):
        src = "\n".join(l for l in c.splitlines() if not l.lstrip().startswith("# MAGIC"))
        if not src.strip():
            continue
        try:
            exec(compile(src, str(nb), "exec"), ns)
        except Exception:
            pass  # driver cells needing a real runtime; defs before the error persist
    pkg = ns["extract_ir"](str(LESSON6_DTSX))
    return pkg[0] if isinstance(pkg, list) else pkg


def _requires_samples():
    if not LESSON6_DTSX.exists():
        print("SKIP: run scripts/fetch_samples.sh first (Lesson 6 not present)")
        sys.exit(0)


def test_clean_pipeline_passes():
    """Mock LLM (passthrough) over the real Lesson 6 prompts should verify clean —
    destinations are presolved, so there is no incomplete error-handling cell."""
    pkg = _load_pkg()
    prompts = G.build_prompts(pkg)
    gen = G.get_provider("mock")
    results = {p["node"]: gen(G.SYSTEM_PROMPT, p["user_prompt"])
               for p in prompts if not p.get("presolved")}
    findings = V.verify_cells(V.build_cells(prompts, results))
    assert findings == [], f"expected clean, got {findings}"


def test_incomplete_cell_caught_and_fixed():
    """Inject an empty body for an LLM cell (the Lesson 6 bug class); the loop must catch
    the incomplete cell and repair it, ending with zero findings."""
    pkg = _load_pkg()
    prompts = G.build_prompts(pkg)
    llm_nodes = [p["node"] for p in prompts if not p.get("presolved")]
    victim = llm_nodes[0]

    def flaky(system, user):
        up = re.search(r"Upstream view: (v_\w+)", user)
        up = up.group(1) if up else "v_x"
        if "FAILED verification" in user:          # corrective pass -> valid SQL
            return f"SELECT * FROM {up}"
        if victim.replace(" ", "") in user.replace(" ", "") and flaky.first:
            flaky.first = False
            return ""                               # first pass -> incomplete
        return f"SELECT * FROM {up}"
    flaky.first = True

    results = {p["node"]: flaky(G.SYSTEM_PROMPT, p["user_prompt"])
               for p in prompts if not p.get("presolved")}

    before = V.verify_cells(V.build_cells(prompts, results))
    assert any(f["category"] == "incomplete" for f in before), "should detect the empty cell"

    _, findings, iters = V.verify_and_fix(pkg, prompts, results, flaky, G.SYSTEM_PROMPT,
                                          G.assemble_notebook, max_iters=3)
    assert findings == [], f"loop should repair all findings, got {findings}"
    assert iters >= 1


def test_presolved_cells_are_pinned():
    """A failing presolved (deterministic) cell is reported but never sent to the fix loop."""
    cells = [{"node": "T", "view": "v_t", "kind": "statement",
              "body": "-- only a comment", "presolved": True}]
    findings = V.verify_cells(cells)
    assert findings and findings[0]["presolved"] is True
    rpt = V.format_report(findings, 0)
    assert "PINNED" in rpt


if __name__ == "__main__":
    _requires_samples()
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for t in tests:
        t()
        print(f"PASS {t.__name__}")
    print(f"\n{len(tests)} passed")
