# Annotation Parsing → Genie Configuration

How Oracle 26ai **annotations** are parsed and rendered into a **Databricks Genie (space) agent
configuration**. This documents the two modules added under `src/`:

| Module | Role |
|---|---|
| `src/annotation_parser.py` | Layer 1 — registry rows → typed, validated annotation model |
| `src/annotation_to_genie.py` | Layer 2 — typed model → portable Genie space config |
| `tests/test_annotation_pipeline.py` | Unit tests (stdlib `unittest`, no external deps) |

The output is the **portable config dict** consumed by `src/hooks/update_genie_space.py`
(`GenieSpaces.apply_config()` / `.create()`), so annotations flow: **Oracle → registry →
parser → renderer → Genie space**.

---

## 1. Background: where annotations come from

LLNL authors annotations directly on Oracle 26ai objects to drive Oracle's native Select AI.
The metadata sync captures every annotation into the `oracle_annotations` registry table
(see `00_setup.py`). Each registry row is one annotation:

| column | meaning |
|---|---|
| `oracle_schema`, `oracle_object`, `oracle_column` | what the annotation is on |
| `level` | `TABLE` or `COLUMN` |
| `annotation_name` | selects the annotation *type* by prefix (see below) |
| `annotation_value` | a **JSON object** (LLNL 2026-07 format) |
| `uc_name` | the resolved Unity Catalog name of the annotated object |
| `is_active` | inactive rows are skipped |

**System instructions are not annotations.** They live in the `AI_GUIDANCE` table's *comment*
and reach Genie through the comment sync leg (Genie reads UC comments directly).

---

## 2. The annotation format (JSON value)

As of the 2026-07 spec, the `annotation_value` is a JSON object that mirrors the Databricks
structure. The **`annotation_name` prefix** picks the shape:

| `annotation_name` | Level | JSON shape |
|---|---|---|
| `foreign_key` | COLUMN | `{left_table, right_table, join_condition, left_column, right_column, relationship, Instructions, Type}` |
| `sql_expression_<label>` | TABLE | `{name, code, synonyms, instructions, Type}` |
| `sample_query_<label>` | TABLE | `{name, question, query}` |

Notes the parser defends against:

- **Keys are matched case-insensitively** (`Instructions` vs `instructions`, `Type` vs `type`).
- **The authored JSON can be invalid** — `instructions` often contains unescaped double quotes.
  The parser tries strict `json.loads` first, then falls back to a lenient flat-object parser
  and records a `parse_note` so the item can be flagged back to the source (see §6).
- **Table names carry no schema/catalog** — Unity Catalog names are resolved in the renderer.

---

## 3. Type-by-type: parse → produce

### 3a. `foreign_key` → Genie **join**

**Oracle annotation (column-level), JSON value:**
```json
{
  "left_table": "po_edd_mv",
  "right_table": "all_users_v1_mv",
  "join_condition": "=",
  "left_column": "per_intr_no_buy",
  "right_column": "per_intr_no",
  "relationship": "Many to One",
  "Instructions": "",
  "Type": "Join"
}
```

**Parsed into** a `ForeignKey` (relationship normalized `Many to One` → `MANY_TO_ONE`).

**Produces** a Genie join spec:
```json
{
  "left":  { "table": "llnl_livit_catalog_genie_lab_623_poc.oracle_26ai_test4.po_edd_mv", "alias": "po_edd_mv" },
  "right": { "table": "llnl_livit_catalog_genie_lab_623_poc.oracle_26ai_test4.all_users_v1_mv", "alias": "all_users_v1_mv" },
  "on": "`po_edd_mv`.`per_intr_no_buy` = `all_users_v1_mv`.`per_intr_no`",
  "relationship_type": "FROM_RELATIONSHIP_TYPE_MANY_TO_ONE",
  "instruction": "Foreign key from Oracle annotation on LINCSVECTR.PO_EDD_MV.per_intr_no_buy."
}
```

**Role-playing dimensions** are handled: if a table is joined more than once (e.g.
`per_intr_no_buy` *and* `per_intr_no_rqst_by` both reference `all_users_v1_mv`), each join gets
a distinct right-side alias — `all_users_v1_mv`, then `all_users_v1_mv_2` — so Genie can join
the same dimension twice without an alias clash.

> **UC name resolution:** the left table uses the row's authoritative `uc_name`. The right
> table reuses that catalog/schema and lowercases the name (a first-pass heuristic). Wire it to
> `{MS}.resolve_uc_name` for authoritative resolution (honors per-object overrides). Any FK whose
> UC name can't be resolved is skipped and listed in `report.skipped`.

### 3b. `sql_expression_<label>` → Genie **filter** or **expression**

The JSON `Type` decides the bucket: `"Filter"` → `sql_filters`, anything else
(`"Expression"`, measures, …) → `sql_expressions`. `synonyms` is a comma-separated string,
split into a list.

**Oracle annotation (table-level), JSON value:**
```json
{
  "name": "Contracts",
  "code": "PO_APPL_DESC in ('LINCS Subcontract', 'PARIS PO')",
  "synonyms": "Subcontracts, Contracts",
  "instructions": "When the user searches for \"Contracts\" or \"Subcontracts\" apply this filter.",
  "Type": "Filter"
}
```

**Produces** a Genie filter snippet:
```json
{
  "display_name": "Contracts",
  "sql": "PO_APPL_DESC in ('LINCS Subcontract', 'PARIS PO')",
  "instruction": "When the user searches for \"Contracts\" or \"Subcontracts\" apply this filter.",
  "synonyms": ["Subcontracts", "Contracts"]
}
```

A measure like `{"name":"Total Spend","code":"SUM(PO_DOL_GRS_AMT)","Type":"Expression"}` produces
the same snippet shape but lands in `sql_expressions` instead of `sql_filters`.

### 3c. `sample_query_<label>` → Genie **example**

**Oracle annotation (table-level), JSON value:**
```json
{
  "name": "Orders for Project Task",
  "question": "Which orders are tied to P/T 40160 1.03.04.21.1602.04?",
  "query": "select distinct po.po_no from lincsvectr.po_edd_mv po where pt.project_no = :project"
}
```

**Produces** a Genie example, with the **Oracle schema prefix rewritten** to the Unity Catalog
name:
```json
{
  "question": "Which orders are tied to P/T 40160 1.03.04.21.1602.04?",
  "sql": "select distinct po.po_no from llnl_livit_catalog_genie_lab_623_poc.oracle_26ai_test4.po_edd_mv po where pt.project_no = :project"
}
```

`translate_oracle_sql()` rewrites `<oracle_schema>.<table>` → `<catalog>.<schema>.<table>`
(deterministic, case-insensitive, table aliases untouched). **Bind variables** (`:project`,
`:task`) can't be auto-valued and are reported as warnings — Genie examples need runnable SQL,
so those need literal values or Genie parameters. A full SQL transpile (functions, date math,
outer-join syntax) is out of scope.

### 3d. `AI_GUIDANCE` → `text_instruction`

System instructions normally arrive via the `AI_GUIDANCE` table **comment** (comment leg). If a
`DESCRIPTION` annotation is present, it is consolidated into the single allowed
`text_instruction` string.

---

## 4. End-to-end example

Input registry rows (5 annotations on `PO_EDD_MV`): two `foreign_key`, one `sql_expression`
filter, one `sql_expression` measure, one `sample_query`. Rendering produces:

```
joins=2 filters=1 expressions=1 examples=1 instructions=0 skipped=0 repaired=1 warnings=1
```

The resulting portable config has `joins` (2, with `all_users_v1_mv` / `all_users_v1_mv_2`
aliases), `sql_filters` (1: Contracts), `sql_expressions` (1: Total Spend), and `examples` (1,
schema-rewritten). `repaired=1` flags the filter whose JSON needed lenient recovery; `warnings=1`
flags the sample query's `:project` bind variable.

---

## 5. Running it

**Locally against a CSV export:**
```bash
python src/annotation_to_genie.py path/to/annotations.csv   # prints the Genie config JSON
python src/annotation_parser.py   path/to/annotations.csv   # prints the parsed model
python -m unittest discover -s tests -v
```

**Inside the sync notebook / hook:**
```python
from annotation_parser import parse_rows
from annotation_to_genie import render_genie_config
from update_genie_space import GenieSpaces

parsed = parse_rows(spark.table(f"{MS}.oracle_annotations")
                    .where(f"sync_name='{SYNC}' AND is_active").collect())
config, report = render_genie_config(parsed)
print(report.summary())
GenieSpaces(host, token).apply_config(space_id, config)   # only sections with content are touched
```

`parse_rows` accepts both dict rows and Spark `Row` objects.

---

## 6. Reports & data quality

`render_genie_config` returns `(config, RenderReport)`. The report never silently drops data:

- **`skipped`** — annotations that couldn't be rendered (e.g. an FK with an unresolvable UC name).
- **`warnings`** — rendered but needs attention (e.g. sample-query bind variables).
- **`repaired`** — annotations that only parsed via the lenient fallback, i.e. **invalid JSON at
  the source**. These should be fixed upstream by escaping embedded quotes (`\"`).

The parser mirrors this: malformed JSON that still yields the required fields is recovered and
noted; JSON missing required fields becomes a flagged `UnknownAnnotation` rather than being dropped.

---

## 7. Open items (pending confirmation from LLNL)

- **Full enumeration of `Type` values** for `sql_expression` — currently `"Filter"` → filter,
  everything else → expression.
- **Sample-query SQL** — whether the Oracle SQL should be transpiled to Databricks dialect on our
  side (beyond schema qualification), or re-authored in Databricks dialect at the source.
- **Bind variables** in sample queries — literal example values vs. Genie parameters.
