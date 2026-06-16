# Genie Space Configuration via API

A small Python module — `update_genie_space.py` — that reads and edits a
Databricks Genie Space's configuration (text instructions, worked SQL examples,
table joins, SQL Expressions, and sample questions) using the documented public
Genie API. Configuration is supplied as a portable JSON / Python dict.

## Try the canned demo

A self-contained demo provisions a Genie Space over a small synthetic
solar-panel dataset. The default target is `bg.solar.panels`, but `demo.py`
accepts `--catalog`, `--schema`, and `--table` flags — pass anything that
matches what you provisioned in step 1.

### Defaults (catalog=`bg`, schema=`solar`, table=`panels`)

1. **Populate the demo table.** Run `setup_bg_solar_panels.sql` against the
   SQL warehouse the space will use (Databricks SQL editor or
   `databricks api post /api/2.0/sql/statements ...`). Creates
   `bg.solar.panels` and inserts 200 rows spanning 2023–2026, including ~10
   "high-wattage" panels (≥5500W) so the bundled filter snippet has matches.

2. **Set credentials.**
   ```bash
   export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
   export DATABRICKS_TOKEN="<PAT or OAuth access token>"
   ```

3. **Create the space.**
   ```bash
   python3 demo.py --warehouse-id <warehouse_id>
   ```

   Prints the new space's ID and a URL you can open in the browser. The space
   ships pre-configured with vocabulary, six worked SQL examples (including a
   self-join), a join spec, a "High Wattage" filter, an "Install year"
   derived expression, and five suggested sample questions — everything
   Genie needs to answer questions like *"How many panels are installed?"*
   and *"Year-over-year growth in installed capacity?"*

### Using a different catalog/schema/table

1. **Provision the table at your chosen FQN** — generate a tailored SQL file
   and run it:
   ```bash
   sed -e 's/bg\.solar/my_cat.my_sch/g' \
       -e 's/\.panels/\.my_table/g' \
       setup_bg_solar_panels.sql > my_setup.sql
   # then run my_setup.sql in the Databricks SQL editor or via databricks api
   ```

2. **Pass the matching values to `demo.py`** — it substitutes the FQN
   throughout `solar_panels_config.json` automatically (instructions, joins,
   SQL examples):
   ```bash
   python3 demo.py --warehouse-id <warehouse_id> \
       --catalog my_cat --schema my_sch --table my_table
   ```

### Cleanup

Delete the space afterward (soft-delete to trash):

```bash
python3 -c "
import os; from update_genie_space import GenieSpaces
GenieSpaces(os.environ['DATABRICKS_HOST'], os.environ['DATABRICKS_TOKEN']).delete('<space_id>')
"
```

## Official API

The Genie Space configuration is exposed via the public Genie Conversation API
under `/api/2.0/genie/spaces/...`.

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/api/2.0/genie/spaces/{space_id}?include_serialized_space=true` | Return the full space config as a `serialized_space` JSON string and an `etag` |
| `PATCH` | `/api/2.0/genie/spaces/{space_id}` | Replace the space config; body `{"serialized_space": "...", "etag": "..."}` |
| `POST` | `/api/2.0/genie/spaces` | Create a new space. Required body: `warehouse_id`, `serialized_space` (containing at least `data_sources.tables`). Optional: `title`, `parent_path`. |
| `DELETE` | `/api/2.0/genie/spaces/{space_id}` | **Soft-delete** — moves the space to the workspace trash. Purge from the UI for permanent removal. |
| `GET` | `/api/2.0/genie/spaces` | List all (non-trashed) spaces in the workspace; supports pagination via `next_page_token`. |

The Update (PATCH) and Create APIs are marked **Beta** in the November 2025
release notes — treat the request/response shape as still subject to change.

References:

- [Use the Genie API to integrate Genie into your applications](https://docs.databricks.com/aws/en/genie/conversation-api)
- [Genie API · REST API reference](https://docs.databricks.com/api/workspace/genie)
- [AI/BI and Genie release notes 2025](https://docs.databricks.com/aws/en/ai-bi/release-notes/2025)
  — "Genie Create and Update APIs have been released to Beta. Get API has been
  updated to allow users to retrieve a serialized definition of the Genie Space."

## How to update a room

A single GET → mutate → PATCH cycle replaces the whole config. There is no
per-field PATCH and no in-place update of individual list items.

1. `GET` the space with `?include_serialized_space=true`. Capture two things:
   the `serialized_space` (a JSON string) and the `etag`.
2. `json.loads(serialized_space)` to get the canonical inner config dict.
3. Mutate the dict — change `text_instructions`, append to
   `example_question_sqls`, add a `join_specs` entry, etc.
4. `PATCH` the space with the modified dict re-serialized to JSON, echoing the
   `etag` back so the server can detect concurrent edits.
5. The response carries a new `etag`. Use it for the next round-trip.

### Canonical inner shape (raw API)

After `json.loads(serialized_space)`:

```jsonc
{
  "version": 2,
  "data_sources": [ /* tables on the space — leave untouched */ ],
  "config": {
    "sample_questions": [ {"id": "<hex>", "question": ["..."]} ]
  },
  "instructions": {
    "text_instructions":     [ {"id": "<hex>", "content": ["..."]} ],
    "example_question_sqls": [ {"id": "<hex>", "question": ["..."], "sql": ["..."]} ],
    "join_specs": [
      {
        "id": "<hex>",
        "left":  {"identifier": "<fqn>", "alias": "<a>"},
        "right": {"identifier": "<fqn>", "alias": "<a>"},
        "sql": ["<on-clause>", "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"],
        "instruction": ["..."]
      }
    ],
    "sql_snippets": {
      "filters":     [ {"id": "<hex>", "display_name": "...", "sql": ["..."], "instruction": ["..."], "synonyms": ["..."]} ],
      "expressions": [ /* same shape */ ]
    }
  }
}
```

### Validator quirks (learned by failure)

These are not stated in the public docs; the PATCH endpoint rejects payloads
without them:

- **String fields are lists.** `content`, `question`, `sql`, `instruction`,
  `synonyms` are all `[str, ...]`. A single-element list works fine.
- **Every list item needs an `id`** — a lowercase 32-hex UUID with no hyphens
  (e.g. `uuid.uuid4().hex`).
- **Lists must be sorted by id ascending** before PATCH.
- **`text_instructions` is capped at one item.** Combine all guidance into a
  single content string.
- **`relationship_type` for joins is encoded as a magic SQL comment** appended
  to the `sql` list: `"--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"`. Valid
  values: `FROM_RELATIONSHIP_TYPE_MANY_TO_ONE`, `ONE_TO_MANY`, `ONE_TO_ONE`,
  `MANY_TO_MANY`.
- **Sample questions live under `config.sample_questions`**, not under
  `instructions`.

The module hides all of these — you write the ergonomic config format described
below.

## Portable config format

The module accepts a config dict (or JSON file) in this shape. Every key is
optional; omitted sections leave that part of the room untouched. Plain
strings, no required ids, no manual sort order — the loader adds those.

```jsonc
{
  "text_instruction": "<single string of guidance>",

  "examples": [
    {"question": "...", "sql": "..."}
  ],

  "joins": [
    {
      "left":  {"table": "<fqn>", "alias": "<a>"},
      "right": {"table": "<fqn>", "alias": "<a>"},
      "on":    "<sql on-clause>",
      "relationship_type": "FROM_RELATIONSHIP_TYPE_MANY_TO_ONE",   // optional
      "instruction": "<optional note>"                              // optional
    }
  ],

  "sql_filters": [
    {
      "display_name": "...",
      "sql":          "<where-clause fragment>",
      "instruction":  "...",
      "synonyms":     ["...", "..."]
    }
  ],

  "sql_expressions": [
    // Same shape as sql_filters. `sql` is a derived-column expression.
  ],

  "sample_questions": ["...", "..."]
}
```

A worked sample (`solar_panels_config.json`, shipped alongside the script)
configures a demo space on top of `bg.solar.panels`. Use it as a template.

## How Genie uses your configuration

Different sections of the config influence Genie differently. Two are
deterministic-ish (Genie applies them closely when they're matched to a
question), and two are advisory (Genie may or may not act on them).

| Section | How Genie uses it | Reliability |
| --- | --- | --- |
| `examples` (`example_question_sqls`) | Worked Q+SQL pairs. When a user's question is similar to one of these, Genie uses the SQL pattern. | Strong — Genie copies the example closely |
| `joins` (`join_specs`) | Pre-defined join relationships between tables. Used when Genie needs to join those tables. | Strong when the join is needed |
| `sql_filters` / `sql_expressions` (`sql_snippets`) | Reusable WHERE-clause fragments and derived-column expressions, with `display_name` + `synonyms` + `instruction`. Genie applies the SQL when a user's wording matches the snippet's vocabulary. | Best-effort — depends on the user's wording matching `synonyms` / `display_name` |
| `text_instruction` | Free-text guidance (the "system prompt"). | Best-effort — interpreted, not enforced |
| `sample_questions` | UI-only suggested-question chips. Does not affect SQL planning. | UI-only |

**To make snippets engage reliably:**

- Provide `synonyms` covering the colloquial ways users actually phrase the
  concept. The official guidance describes the synonyms field as *"common
  ways that users might refer to the expressions colloquially"*.
- Write `instruction` text that explains *when* Genie should use the snippet
  (e.g., "Use when the user asks about large or high-value orders") — not
  just what it computes. Genie reads this when deciding whether to apply the
  snippet.
- Keep one semantic concept per snippet (one filter, one expression, one
  measure). Don't bundle unrelated logic.

**Per the official best-practices guide**, prefer SQL expressions and example
SQL over text instructions for any business logic you can encode as SQL —
SQL is applied much more reliably than prose.

**On non-determinism:** even with well-tuned snippets, Genie's planner can
produce different SQL for the same question across attempts. The
`text_instruction` and `example_question_sqls` are read on every message
(verified in this code's testing — a follow-up message in an existing
conversation picks up the latest config), but whether a given snippet is
engaged on any particular question depends on the planner.

References:
- [Curate an effective Genie Space](https://docs.databricks.com/aws/en/genie/best-practices)
- [Tune Genie Space quality](https://docs.databricks.com/aws/en/genie/tune-quality)

## Our code: overview

`update_genie_space.py` is a single-file SDK with two layers:

1. **`GenieSpaces` client class** — handles all I/O. Construct once with
   workspace host + auth, then call methods to manage spaces:
   - `get(space_id) -> (outer, inner, etag)`
   - `dump(space_id) -> inner`
   - `patch(space_id, inner, etag) -> outer`
   - `apply_config(space_id, config) -> outer`
   - `create(*, warehouse_id, table_identifiers, ...) -> outer`
   - `delete(space_id)`  *(soft-delete to trash)*
   - `list(*, max_spaces=None) -> [spaces...]`
2. **Module-level inner-doc helpers** — pure functions that mutate a parsed
   inner dict in place. They generate ids, wrap strings in lists, sort, and
   otherwise hide the schema quirks. They don't touch the network — call
   `client.patch()` after mutating.
   - *Full-replace* per section: `set_text_instruction`,
     `replace_example_question_sqls`, `replace_join_specs`,
     `replace_sql_snippets`, `replace_sample_questions`.
   - *Append-or-upsert* (idempotent, matched by natural key):
     `add_example_question_sql`, `add_join_spec`, `add_sql_snippet`,
     `add_sample_question`.
   - Item builders for use with `replace_*`: `make_join_spec`, `make_sql_snippet`.
   - `apply_config_to_inner(inner, config)` — apply a portable config dict
     directly to an inner doc (e.g., when building a new space offline).

The CLI in `main()` builds a client from env vars (`DATABRICKS_HOST`,
`DATABRICKS_TOKEN`, `GENIE_SPACE_ID`) and applies a JSON config file.

The pre-PATCH normalization step (`_normalize_inner`) sorts every id-bearing
list and `data_sources.tables` by identifier; `patch()` and `create()` call
it automatically.

## How it works

```text
    ┌──────────────────────────────────────┐
    │ client.get(space_id)                 │  GET /api/2.0/genie/spaces/{id}
    └──────────────┬───────────────────────┘     ?include_serialized_space=true
                   │
                   ▼
            outer, inner, etag                   inner = json.loads(outer["serialized_space"])
                   │
                   ▼
    ┌──────────────────────────────────────┐    apply_config_to_inner(...) or:
    │ mutate inner in place                │      set_text_instruction
    │ (any number of helper calls)         │      replace_example_question_sqls
    └──────────────┬───────────────────────┘      replace_join_specs
                   │                              replace_sql_snippets
                   ▼                              replace_sample_questions
    ┌──────────────────────────────────────┐
    │ client.patch(space_id, inner, etag)  │   _normalize_inner sorts by id, then
    └──────────────┬───────────────────────┘   PATCH /api/2.0/genie/spaces/{id}
                   │                            body: {serialized_space, etag}
                   ▼
                new outer (carries the next etag)
```

Each round-trip is atomic: the entire `serialized_space` is replaced. Two
concurrent editors will see a 412 (etag conflict) on whichever PATCH lands
second; re-fetch and retry. All API errors raise `GenieSpacesError` carrying
the method, path, status code, and body.

## Usage

### Construct a client

```python
from update_genie_space import GenieSpaces

client = GenieSpaces(
    host="https://your-workspace.cloud.databricks.com",
    token="<PAT or OAuth access token>",     # or a zero-arg callable for refresh
)
```

The token can be a string (fixed) or a zero-arg callable returning a fresh
string each call (handy when wrapping a refreshing source). Pass `session=`
to share a `requests.Session` across many clients; pass `timeout=` to
override the per-request timeout (default 60s).

### CLI: apply a config file

```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="$(databricks auth token --host $DATABRICKS_HOST \
    | python3 -c 'import json,sys;print(json.load(sys.stdin)["access_token"])')"
export GENIE_SPACE_ID="<space-id from /genie/rooms/<ID>>"

python3 update_genie_space.py solar_panels_config.json
```

Prints before/after counts of every config section. Idempotent — safe to re-run.

### Apply a config dict (update an existing room)

```python
client.apply_config(space_id, {
    "text_instruction": "All revenue figures are in USD. Round to 2 decimals.",
    "examples": [
        {"question": "How many rows are in my_table?",
         "sql": "SELECT COUNT(*) FROM my_table"},
    ],
    "sample_questions": ["How many rows are in my_table?"],
})
```

### Create a fresh room

```python
import json

with open("solar_panels_config.json") as f:
    config = json.load(f); config.pop("_comment", None)

created = client.create(
    warehouse_id="<warehouse_id>",
    table_identifiers=["catalog.schema.table"],   # one or more FQNs
    title="My new space",                          # optional
    parent_path="/Users/you@example.com",          # optional
    description="...",                             # optional
    config=config,                                 # optional; same shape as apply_config
)
print(created["space_id"], created["etag"])
```

The space is fully configured in a single POST — the SDK compiles your
config dict into the initial `serialized_space` before sending. Continue
editing with `client.apply_config(created["space_id"], ...)`.

### Delete a room (soft-delete to trash)

```python
client.delete(space_id)
```

The space is moved to the workspace **trash**, not destroyed. Subsequent
GETs return 404 ("has been trashed"). To purge permanently, use the
workspace UI's Trash. Eventual consistency: a GET immediately after delete
may briefly return 200 before the trash transition propagates.

### List spaces

```python
spaces = client.list(max_spaces=100)              # auto-paginates
for s in spaces:
    print(s["space_id"], "—", s.get("title"))
```

### Dump a room (read-only)

```bash
python3 -c '
import json, os
from update_genie_space import GenieSpaces
c = GenieSpaces(os.environ["DATABRICKS_HOST"], os.environ["DATABRICKS_TOKEN"])
print(json.dumps(c.dump(os.environ["GENIE_SPACE_ID"]), indent=2))
'
```

### Partial updates: section-level vs within-section

There are two levels of "partial":

**Section-level — via `apply_config` with a partial config dict.** Omitted
keys are not touched (the unchanged sections still ride along in the PATCH
from the GET). Use this when you want to *replace* a whole section but leave
other sections alone:

```python
# Only updates joins; text/examples/snippets/sample_questions stay as they are.
client.apply_config(space_id, {
    "joins": [
        {
            "left":  {"table": "catalog.schema.orders",    "alias": "orders"},
            "right": {"table": "catalog.schema.customers", "alias": "customers"},
            "on":    "`orders`.`customer_id` = `customers`.`id`",
        },
    ],
})
```

**Within-section — via `add_*` helpers.** When you want to append one item
to a section without disturbing the existing items, mutate `inner` directly
with the append-or-upsert helpers. Each one matches against a natural key,
so calling them repeatedly with the same input is idempotent.

| Helper | Section | Natural key (upsert match) |
| --- | --- | --- |
| `add_example_question_sql(inner, question, sql)` | `examples` | `question` (exact text) |
| `add_join_spec(inner, left_table, left_alias, right_table, right_alias, join_on, ...)` | `joins` | `(left_table, left_alias, right_table, right_alias)` |
| `add_sql_snippet(inner, kind, display_name, sql, ...)` | `sql_filters` or `sql_expressions` | `display_name` |
| `add_sample_question(inner, question)` | `sample_questions` | `question` (exact text) |

Use `kind=SNIPPET_KIND_FILTER` or `SNIPPET_KIND_EXPRESSION` with
`add_sql_snippet`.

```python
from update_genie_space import (
    GenieSpaces,
    add_example_question_sql, add_join_spec, add_sql_snippet, add_sample_question,
    SNIPPET_KIND_FILTER,
)

_, inner, etag = client.get(space_id)

# Append one worked example (no-op if the question already exists).
add_example_question_sql(inner,
    question="Top 5 customers by revenue last quarter",
    sql="SELECT customer_id, SUM(amount) ...")

# Append one join.
add_join_spec(inner,
    left_table="catalog.schema.orders",    left_alias="orders",
    right_table="catalog.schema.customers", right_alias="customers",
    join_on="`orders`.`customer_id` = `customers`.`id`",
    relationship_type="FROM_RELATIONSHIP_TYPE_MANY_TO_ONE",
    instruction="Standard orders → customers join.")

# Append one SQL Expression (filter).
add_sql_snippet(inner, SNIPPET_KIND_FILTER,
    display_name="High value orders",
    sql="orders.amount > 1000",
    instruction="Orders worth more than $1000.",
    synonyms=["high value", "large orders"])

# Append one sample question.
add_sample_question(inner, "Top 5 customers by revenue last quarter")

client.patch(space_id, inner, etag)
```

All four helpers preserve existing item ids when upserting, so other
references (e.g., dashboards that link to a specific instruction) stay
stable across re-runs.

## Errors

All API failures raise `GenieSpacesError`, which carries:

- `method`, `path` — the failing request
- `status` — HTTP status code
- `body` — server response body (often a JSON `{error_code, message, ...}`)

Concurrent edits surface as `status=412` (etag conflict) — re-fetch and retry.

## Files

- `update_genie_space.py` — the SDK (client + mutators + config applier + CLI).
- `solar_panels_config.json` — a worked sample config for a demo space on
  `bg.solar.panels`.
- `setup_bg_solar_panels.sql` — DDL + sample data for the demo table.
- `demo.py` — one-shot script: create a space from the JSON config.
- `README.md` — this file.
