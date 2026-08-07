# SSIS → Databricks: Logic Extraction & SparkSQL Generation

Tools for converting SQL Server Integration Services (SSIS) `.dtsx` packages into
Databricks SparkSQL. This is a **complement to Databricks Lakebridge**, exploring an
LLM-assisted approach to the data-flow-logic portion of an SSIS migration.

Two complementary paths live here:

1. **Lakebridge** (`analyze` + `transpile`) — Databricks' official, supported migration
   tooling. Use it for assessment/inventory and as the baseline transpiler; it's the
   right starting point for any real migration and the path that receives ongoing
   Databricks support and improvement.
2. **A custom IR + LLM pipeline** (this repo) — an experimental technique that parses
   `.dtsx` into a purpose-built intermediate representation (IR) and forward-generates
   SparkSQL with a grounded LLM prompt, entirely inside a Databricks notebook. It's a
   research prototype useful for exploring where LLM grounding can add value on specific
   data-flow patterns; it is **not** a supported product.

---

## TL;DR

| I want to… | Do this |
|---|---|
| Assess a folder of `.dtsx` (inventory + complexity) | `./extract_ir_lakebridge.sh <dtsx_dir> <out_dir>` — reads Lakebridge's analyzer |
| Transpile with the official tool | `databricks labs lakebridge transpile --source-dialect ssis --target-technology SPARKSQL …` |
| Extract transform logic into a JSON IR (local) | `python3 dtsx_to_ir.py <file-or-dir> --out ir.json` |
| Generate SparkSQL from IR (local, needs a model) | `python3 ir_to_sparksql.py ir.json --dry-run` then `--backend databricks …` |
| **Do it all in Databricks from a raw `.dtsx`** | Import `notebooks/ir_to_sparksql_databricks.py`, set the `dtsx_path` widget, run |

**New here? Start with the Databricks notebook** (section 4) — it's the most complete
path and needs nothing installed locally.

---

## Why this exists (the key facts)

- **Lakebridge reads `.dtsx` from disk** — the package XML *is* the logic. You don't
  need a running SQL Server to analyze or transpile; you only need SQL Server to
  *build* new packages (SSDT) or to *run* originals for output validation.
- **Lakebridge's transpiler is powered by the BladeBridge converter engine** (a native
  binary configured via JSON + hook files). This repo takes a different, complementary
  tack — parsing the `.dtsx` into an open IR you control and generating from it with an
  LLM — so the two can be compared and combined.
- **SSIS transpile targets `SPARKSQL`** in the current BladeBridge dialect set; SSIS
  support landed in Lakebridge 0.12.0 / bb-plugin ≥ 0.2.
- **Where the IR + LLM approach can add value:** grounding an LLM on the extracted IR
  lets the generated SQL carry semantics that are easy to under-specify in a
  template-based conversion — e.g. an SSIS Lookup's join keys *and* its `noMatchBehavior`
  disposition, so a `fail_component` lookup becomes an `INNER JOIN` with the row
  disposition made explicit. See the comparison in "When to consider this approach."

---

## Input formats — is SSIS always a `.dtsx` file?

**No.** `.dtsx` is the individual *package* format, and it's what every tool here
consumes — but SSIS logic is often delivered in other containers. All of them reduce
to `.dtsx` files with one extraction step.

| You have… | What it is | Get `.dtsx` by… |
|---|---|---|
| `.dtsx` | A single package (the logic *is* this XML) | Use it directly |
| `.ispac` | A deployment bundle — **just a ZIP** of packages + project metadata | `unzip file.ispac -d out/` → `.dtsx` files land inside |
| Project folder | `.dtproj` + `.dtsx` + `.conmgr` + `.params` | Use the `.dtsx` files as-is (see project-model caveat below) |
| **SSIS Catalog (SSISDB)** | Deployed project stored as an `.ispac` blob in SQL Server | SSMS → right-click project → **Export** (or query `catalog.get_project`), then unzip |
| **MSDB storage** | Legacy packages stored inside the `msdb` database | Export to `.dtsx` via SSMS / `dtutil` |

**Sibling files you may see alongside `.dtsx`:**

- `.dtproj` — the SSIS *project* file (references packages, holds project params)
- `.conmgr` — project-level shared connection managers (referenced, not inline)
- `.params` — project parameters (project deployment model)
- `.dtsConfig` — package configurations (older package deployment model; externalized
  connection strings and variable values)

> ⚠️ **Project deployment model caveat.** With project-scoped deployment, connection
> strings and parameters often live in `.conmgr` / `.params` / the `.dtproj` — *outside*
> each `.dtsx`. The custom IR parser (`dtsx_to_ir.py` and the notebook) currently reads
> connections and variables from **within** each `.dtsx` only. If your real packages come
> as a full `.ispac` / project rather than standalone `.dtsx` files, project-scoped
> connections and parameters won't be captured — you'd extend the parser to also read the
> sibling project files. The bundled tutorial packages are self-contained, so this doesn't
> affect them.

**Bottom line:** if a package isn't already a `.dtsx` on disk, unzip the `.ispac` or
export from SSISDB/msdb first. Everything downstream in this README is identical.

---

## Files

```
extract_ir_lakebridge.sh              # Lakebridge analyzer → JSON IR (reads the JSON directly)
dtsx_to_ir.py                         # standalone .dtsx → custom JSON IR (lxml)
ir_to_sparksql.py                     # IR → SparkSQL via pluggable LLM (local CLI)
notebooks/
  ir_to_sparksql_databricks.py        # all-in-one Databricks notebook (raw .dtsx → SparkSQL)
packages/ms-tutorial/Lesson 1-6.dtsx  # sample packages (MS "Creating a Simple ETL Package")
out/                                  # example outputs (analysis reports, IR, transpiled)
```

---

## Prerequisites

| Path | Needs |
|---|---|
| Lakebridge (`analyze`/`transpile`) | Databricks CLI + `databricks labs lakebridge`, Java, a workspace profile |
| `dtsx_to_ir.py` (local) | Python 3.9+, `lxml` |
| `ir_to_sparksql.py` (local) | Python 3.9+; one backend SDK — `databricks-sdk` (databricks), `anthropic` (anthropic), or `openai` (openai). `--dry-run` needs none. |
| **Databricks notebook** | **Nothing extra** — `databricks-sdk` is preinstalled; stdlib-only parser; notebook auth is automatic |

> **Internal Databricks note:** if `pip`/`labs install` fails with a 403 on
> `databricks-switch-plugin` or similar, point pip at the working proxy:
> `--index-url https://pypi-proxy.cloud.databricks.com/simple` (the `.dev` proxy 403s).

---

## 1. Assess packages with Lakebridge (optional)

Produces a 25-sheet Excel assessment (component inventory, "Supported?" flags, complexity,
and the embedded SQL from each component) plus a JSON inventory.

```bash
# Wrapper that calls the analyzer binary directly. On some package sets, the
# --generate-json flag hits a strict output-schema check; the JSON is written before
# that check, so this wrapper reads it directly. (Worth reporting upstream if you hit it.)
./extract_ir_lakebridge.sh "packages/ms-tutorial" "out/ir"
# → out/ir/report.xlsx  and  out/ir/ir.json
```

Or the supported CLI form (on these particular packages `--generate-json` may report a
schema-validation error, but the `.xlsx` report still lands):

```bash
export DATABRICKS_CONFIG_PROFILE=e2-demo-field-eng   # avoids multi-profile auth ambiguity
databricks labs lakebridge analyze \
  --source-directory "$(pwd)/packages/ms-tutorial" \
  --report-file "$(pwd)/out/analysis_report.xlsx" \
  --source-tech SSIS
```

The analyzer's JSON gives you a **census + embedded SQL**, but **not** data-flow edges,
lookup join keys, or the no-match disposition — that's the gap the custom IR fills.

---

## 2. Transpile with Lakebridge (the official tool)

First confirm the transpiler is current — SSIS needs bb-plugin ≥ 0.2 / Lakebridge ≥ 0.12:

```bash
databricks labs lakebridge describe-transpile      # look for "ssis" under Supported Source Dialects
```

If `ssis` is missing but the BladeBridge `Converter/Configs/SSIS/` folder exists, the
transpiler's `config.yml` is stale — add `ssis` to its `dialects:` list and an `ssis:`
`target-tech` CHOICE block (see the config at
`~/.databricks/labs/remorph-transpilers/bladebridge/lib/config.yml`).

```bash
export DATABRICKS_CONFIG_PROFILE=e2-demo-field-eng
BBCONF=~/.databricks/labs/remorph-transpilers/bladebridge/lib/config.yml
databricks labs lakebridge transpile \
  --input-source "$(pwd)/packages/ms-tutorial" \
  --output-folder "$(pwd)/out/transpiled" \
  --source-dialect ssis \
  --target-technology SPARKSQL \
  --transpiler-config-path "$BBCONF" \
  --skip-validation true
```

**Output:** `.py` notebooks of `spark.sql(...)` cells, produced quickly and reliably —
a solid first-draft scaffold for the conversion. As with any automated transpile, the
generated code is a starting point for review: some constructs (complex orchestration,
custom Script Components, source/destination bindings) are flagged for follow-up rather
than fully generated, which is expected and appropriate for a migration accelerator.
The IR + LLM path in this repo (sections 3–5) explores generating more of that
data-flow detail automatically; see "When to consider this approach."

---

## 3. Extract logic into a custom IR (local)

A dependency-light parser (`lxml`) that builds an open IR capturing the data-flow graph
in detail — edges, lookup join keys and dispositions, and per-component transform logic.

```bash
python3 dtsx_to_ir.py "packages/ms-tutorial/Lesson 1.dtsx" --out out/ir/custom_lesson1.json
python3 dtsx_to_ir.py "packages/ms-tutorial" --out out/ir/custom_all.json   # whole folder
```

**IR shape** (per package):

```jsonc
{
  "package": "Lesson 1",
  "connections": [ { "name", "type", "connectionString" } ],
  "variables":   [ { "name", "namespace", "value" } ],
  "controlFlow": {
    "executables": [ {
      "name", "creationName",
      "dataFlow": {                       // a container (ForeachLoop) has none; its children do
        "nodes": [ {
          "name", "componentType", "role",   // source | transform | target
          "logic": {                          // Lookup example:
            "referenceQuery", "parameterizedQuery",
            "joinInputColumns": ["CurrencyID"],
            "noMatchBehavior": "fail_component",   // ← drives JOIN type
            "cacheType": "full"
          }
        } ],
        "edges": [ { "from", "to", "fromPort" } ]   // the data-flow DAG
      },
      "children": [ /* nested executables, e.g. inside a ForeachLoop */ ]
    } ],
    "precedenceConstraints": [ { "from", "to", "value", "evalOp", "expression" } ]
  }
}
```

**Relative to Lakebridge's analyzer JSON** (which is optimized for assessment — inventory,
complexity scoring, and embedded SQL): this IR is optimized for *code generation*, so it
additionally materializes data-flow **edges**, lookup **join keys**, lookup
**`noMatchBehavior`**, connection managers, variables, and precedence constraints. Both
read the same raw XML; they serve different downstream purposes.

---

## 4. Generate SparkSQL in a Databricks notebook (recommended)

`notebooks/ir_to_sparksql_databricks.py` is the all-in-one path: upload a raw `.dtsx`,
get a generated SparkSQL notebook back. No local step, nothing to install.

### How it works

| Stage | Layer | Notes |
|---|---|---|
| Parse `.dtsx` → IR | **deterministic** | stdlib `xml.etree.ElementTree` only |
| Topo-sort nodes, name temp views | **deterministic** | DAG order from the IR edges |
| Build a **grounded** prompt per node | **deterministic** | pins join keys, `noMatchBehavior`, source/target tables so the model can't invent them |
| Translate each node to Spark SQL | **LLM** | Foundation Model API (`serving_endpoints.query`) — no `openai`/`anthropic` install |
| Assemble + write output notebook | **deterministic** | one `CREATE OR REPLACE TEMP VIEW` per node |

The split is the point: the LLM only does fuzzy SQL translation; wiring, ordering, and
semantics are pinned by code.

### Steps

1. **Upload a `.dtsx` to a Unity Catalog Volume**, e.g.
   `/Volumes/main/default/ssis/Lesson 6.dtsx`. (UI: Catalog → your volume → Upload;
   or `databricks fs cp "Lesson 6.dtsx" dbfs:/Volumes/main/default/ssis/`.)
2. **Import the notebook** into your workspace
   (Workspace → Import → `notebooks/ir_to_sparksql_databricks.py`).
3. **Attach to any cluster/serverless** with network access to the serving endpoint.
4. **Set the widgets:**
   - `dtsx_path` → your uploaded file
   - `out_path` → where to write the generated notebook (a Volume path)
   - `endpoint` → a chat serving endpoint, e.g. `databricks-claude-opus-5`
   - `dry_run` → `true` to inspect the grounded prompts first (no LLM call), then `false`
5. **Run all.** With `dry_run=false` it writes the generated notebook to `out_path` and
   prints it inline.

### Example generated output (Lookup)

For the currency-rate lookup with `noMatchBehavior: fail_component`:

```sql
CREATE OR REPLACE TEMP VIEW v_lookup_currency_key AS
SELECT src.*, refTable.`CurrencyKey`
FROM v_extract_sample_currency_data AS src
INNER JOIN (
  SELECT * FROM `dbo`.`DimCurrency`
  WHERE `CurrencyAlternateKey` IN ('ARS','AUD',/* … */'USD','VEB')
) AS refTable
  ON src.`CurrencyID` = refTable.`CurrencyAlternateKey`
```

Correct join key, preserved `IN(...)` filter, T-SQL `[x]` → Spark `` `x` ``, and the
lookup's `fail_component` disposition surfaced explicitly as an `INNER JOIN` — an example
of the semantic detail the IR-grounded prompt is designed to carry through.

---

## 5. Generate SparkSQL locally (alternative to the notebook)

`ir_to_sparksql.py` runs the same logic from your machine against a pluggable backend.

```bash
# 1. Inspect the grounded prompts — no LLM, no dependencies:
python3 ir_to_sparksql.py out/ir/custom_lesson1.json --dry-run

# 2a. Databricks serving endpoint (needs `databricks-sdk`; auth via ~/.databrickscfg):
export DATABRICKS_CONFIG_PROFILE=e2-demo-field-eng
python3 ir_to_sparksql.py out/ir/custom_lesson1.json \
  --backend databricks --endpoint databricks-claude-opus-5 --out out/gen/

# 2b. Anthropic / Claude direct (needs `anthropic` + ANTHROPIC_API_KEY or `ant auth login`):
python3 ir_to_sparksql.py out/ir/custom_lesson1.json --backend anthropic --model claude-opus-5 --out out/gen/

# 2c. Any OpenAI-compatible endpoint — OpenAI, Azure, Copilot, gateway (needs `openai`):
python3 ir_to_sparksql.py out/ir/custom_lesson1.json \
  --backend openai --model gpt-4o --base-url https://your-gateway/v1 --out out/gen/
```

The notebook (section 4) is preferred because it avoids the `openai`/`anthropic`
install and the local Databricks auth setup.

---

## 6. Plugging in your own LLM provider

Both generators treat the LLM as a single swappable function:

```python
Generate = Callable[[str, str], str]   # (system_prompt, user_prompt) -> raw model text
```

The harness runs `_normalize_sql()` on whatever the callable returns, so a stray
```` ```fence ```` or a `CREATE VIEW` wrapper the model adds is tolerated. Three
providers ship in `ir_to_sparksql.py`: **`anthropic`** (Claude via the SDK),
**`databricks`** (Foundation Model API — any model hosted in the workspace), and
**`openai`** (any OpenAI-compatible endpoint).

### In the CLI (`ir_to_sparksql.py`) — add a provider to the registry

Add one decorated factory. It becomes selectable via `--backend <name>`:

```python
@register_provider("copilot")          # -> use with:  --backend copilot
def _copilot(model: str = "gpt-4o", **_) -> Generate:
    """GitHub Copilot / Models (OpenAI-compatible)."""
    from openai import OpenAI
    client = OpenAI(base_url="https://models.github.ai/inference")  # reads the token env var
    def generate(system_prompt: str, user_prompt: str) -> str:
        r = client.chat.completions.create(
            model=model, max_tokens=4096,
            messages=[{"role": "system", "content": system_prompt},
                      {"role": "user", "content": user_prompt}])
        return (r.choices[0].message.content or "").strip()
    return generate
```

The factory receives all CLI flags as kwargs (`model`, `endpoint`, `profile`,
`base_url`) — accept the ones you need and `**_` for the rest. For most hosted
services you don't even need a new provider: the built-in `openai` backend already
covers anything OpenAI-compatible — just pass `--base-url`.

### In the notebook — swap one function

`notebooks/ir_to_sparksql_databricks.py` has a single `generate(system_prompt,
user_prompt)` function (in the "LLM layer" cell) that defaults to the Foundation
Model API. Commented alternatives for **OpenAI-compatible** and **Anthropic direct**
sit right below it — replace the body, `%pip install` the SDK if needed, and nothing
else changes.

**Guidance:**
- **Prefer the Databricks FMAPI backend inside Databricks** — it's the only one that
  needs no extra install and no manual credentials.
- **For Copilot / internal gateways / Azure OpenAI**, the `openai` backend with a
  `--base-url` is almost always enough; write a named provider only if the auth or
  request shape is non-standard.
- **Keep secrets out of code** — providers read from env vars (`OPENAI_API_KEY`,
  `ANTHROPIC_API_KEY`) or the Databricks profile. In a notebook, pull keys from a
  Databricks **secret scope**, not inline.
- **Return only text** from the callable; let `_normalize_sql()` handle fences/wrappers.

---

## When to consider this approach

**Lakebridge is the recommended, supported path for SSIS→Databricks migrations** — start
there for assessment, inventory, complexity scoring, and the baseline transpile. It is
maintained by Databricks, improves over time, and is the right tool to standardize on.

This repo's IR + LLM technique is an **experimental complement** worth considering in a
few specific situations:

| Situation | Why this approach may help |
|---|---|
| You want to **prototype** LLM-assisted conversion on your own model/gateway | Pluggable provider abstraction (Foundation Model API, Anthropic, OpenAI-compatible, Copilot) |
| You need **data-flow SQL that runs with minimal edits** for a demo or POC | IR-grounded generation emits runnable Spark SQL for the covered component set |
| You want the conversion to **carry specific SSIS semantics explicitly** | Lookup dispositions, conditional-split routing, SCD merge logic, control-flow ordering are pinned in the prompt |
| You want an **open, inspectable IR** to build your own tooling on | `dtsx_to_ir.py` emits JSON you fully control |

Realistically, the strongest workflow is **both together**: use Lakebridge for
assessment and coverage, and use an IR-grounded LLM pass to accelerate the data-flow
detail on the packages that need it. The two are not mutually exclusive — this repo
exists to explore that combination, and any technique proven useful here is a candidate
to fold back into the supported tooling.

**This is a research prototype**, not a supported product: no SLA, no guarantees, and
its output — like any automated conversion — requires review before production use.

---

## Known limitations & caveats

- **Source cells invent a landing-table name** (the flat-file ingest isn't in the IR).
  Treat sources as stubs and wire the real read (`spark.read.csv(...)` / an external
  table) yourself.
- **`src.*` propagates columns** the lookup added plus flat-file error columns
  (`ErrorCode`, `ErrorColumn`) downstream — project explicitly if you want them gone.
- **ForeachLoop (file enumerator) → set-based glob read.** An SSIS ForEach *File* loop
  is converted deterministically (no LLM) to a single `read_files('<Volume>/<leaf>/<pattern>')`
  at the source cell, which processes every file the loop would have. `_metadata.file_path`
  is added as `_source_file` for per-file lineage (the equivalent of the loop's file-name
  variable). **Edit the Volume path** — the original Windows folder can't exist on Databricks,
  so only the leaf folder name is kept under a `/Volumes/main/default/ssis_input/` placeholder.
  Non-file enumerators (ADO recordset, item, nodelist) are **not** yet handled.
- **Handled component types** (extract logic + convert): FlatFileSource, OLEDBSource,
  OLEDBDestination, Lookup, DerivedColumn, ConditionalSplit (with port-aware branch
  routing), OLEDBCommand (row→`MERGE`), Aggregate (→`GROUP BY`), UnionAll (→`UNION ALL`),
  Multicast, MergeJoin (→`JOIN`), Sort (→`ORDER BY`), DataConvert (→`CAST`), and the native
  Slowly Changing Dimension wizard (`Microsoft.SCD` → set-based `MERGE INTO`). Verified
  end-to-end on real GitHub packages up to 40 nodes / 7 component types each.
  Also handled: Pivot (→`PIVOT`), UnPivot (→`STACK`), Row Count, Fuzzy Lookup (→best-effort
  join + ⚠️ note; Spark has no fuzzy join), Cache Transform (→temp view), Flat File
  Destination (→`INSERT OVERWRITE DIRECTORY`), Script Component (→code-preserving passthrough
  with a MANUAL-REVIEW banner — never a fabricated C#/VB translation), and non-file ForEach
  enumerators (ADO/item/nodelist → loop captured + ⚠️ set-based-rewrite note). Validated
  against 114 real+synthetic packages (525 pipeline nodes); synthetic examples for types rare
  on public GitHub live in `packages/synthetic/`.
- **Still unhandled** (conservative pass-through, not real logic) — the long tail, ~5% of
  nodes across the test corpus: `ExcelDestination`, `Merge` (sorted merge, distinct from
  Merge Join), `FuzzyGrouping`, `CharacterMap`, `TermExtraction`/`TermLookup` (text mining),
  `RowSampling`/`PctSampling`, `Lineage`, `CopyMap`, `Inserter`/`Extractor`. Extend `_PARSERS`
  / `ground_node` to add handlers — the pattern is well-established (an extractor in
  `dtsx_to_ir.py` + a grounding branch in `ir_to_sparksql.py`, mirrored in the notebook).
- **SCD dimension table name** is not in the SCD component's own metadata (it comes from the
  downstream destination), so the generated `MERGE INTO` uses a `<dimension_table>` placeholder
  with a comment — set it before running.
- **No output validation.** Generated SQL is not run against data here. For a true
  apples-to-apples check, stand up `AdventureWorksDW` tables (or point at real UC
  tables) and compare row counts / values against the original package.
- **`AdventureWorksDW2014` table names** in the generated SQL (`dbo.DimCurrency`, etc.)
  assume those tables exist in your catalog/schema — adjust references to your UC layout.

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `describe-transpile` doesn't list `ssis` | Upgrade Lakebridge/bb-plugin; add `ssis` to the BladeBridge `config.yml` (§2) |
| `Invalid value for '--source-dialect': 'ssis'` | Same as above — stale transpiler config |
| `No mapping for source tech SSIS and target tech PYSPARK` | SSIS only maps to `SPARKSQL`; use `--target-technology SPARKSQL` |
| `403` on `databricks-switch-plugin` / pip installs | Use `--index-url https://pypi-proxy.cloud.databricks.com/simple` |
| `DEFAULT and default and … match <host>` auth error | Set `export DATABRICKS_CONFIG_PROFILE=<profile>` (multiple profiles share one host) |
| Notebook: `ModuleNotFoundError: openai` | Not applicable — the notebook uses `serving_endpoints.query`, not the OpenAI client |
| Analyzer `--generate-json` reports a schema-validation error on some packages | The JSON is written before the check runs, so `extract_ir_lakebridge.sh` reads it directly; consider reporting the case upstream |

---

## Sample data

**`samples/synthetic/`** — small `.dtsx` packages authored for this project to exercise
specific components (Pivot, Unpivot, Fuzzy Lookup + Cache, ADO ForEach enumerator). These
are the only packages committed here, and they carry no third-party content.

**Third-party real-world packages** used during testing are **not** redistributed (they
are GPL or unlicensed — see [`ATTRIBUTION.md`](ATTRIBUTION.md) for full credit). To pull
them locally for your own testing:

```bash
./scripts/fetch_samples.sh   # clones into samples/external/ (gitignored)
```

That set includes Microsoft's "Creating a Simple ETL Package" tutorial (Lessons 1–6, via
the GoodmanNeil/SSIS-Examples mirror), several dimensional-warehouse/SCD projects, and
NirmalAndrews/IntegrationServicesSamples (a broad reference covering the rarer transforms).
