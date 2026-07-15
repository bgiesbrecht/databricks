# Applying annotations (and other metadata) to a Genie space

> **Looking for the annotation → Genie pipeline that this repo ships?** See
> **[ANNOTATION_PARSING.md](ANNOTATION_PARSING.md)**. These annotations use a **JSON value format**
> (`foreign_key`, `sql_expression_*`, `sample_query_*`) parsed by `src/annotation_parser.py` and rendered
> by `src/annotation_to_genie.py`, then pushed by the `genie_push` hook — you do **not** hand-roll the
> routing/parsing shown below. This guide remains the reference for the underlying **SDK** and the general
> pattern (useful for ad-hoc pushes or new annotation shapes). The `RELATED_TO` string convention below is a
> generic illustration and predates the JSON format.

A how-to for an engineer who wants to push metadata into a Genie space **directly with the
`update_genie_space.py` SDK** — without our sync notebooks. The shape is always the same:

1. **Extract** the source metadata (Oracle annotations, or anything else).
2. **Determine its type** and decide which Genie section it maps to.
3. **Construct** the Genie config and **apply** it with the SDK.

> You need: `update_genie_space.py` (the `GenieSpaces` client), a Databricks **host + token**, and a Genie
> **space_id**. `pip install requests` if it isn't already present.

---

## 0. The SDK in 60 seconds

A Genie space's config has these sections (all optional, all independently settable):

| Section | What it drives in Genie | Reliability |
|---|---|---|
| `joins` | how tables relate (used when a question spans tables) | strong |
| `examples` | worked question→SQL pairs (Genie copies the pattern) | strong |
| `sql_expressions` | reusable derived columns / measures | best-effort (matched by wording) |
| `sql_filters` | reusable WHERE-clause fragments | best-effort (matched by wording) |
| `text_instruction` | free-text guidance ("system prompt") | advisory |
| `sample_questions` | suggested-question chips in the UI | UI only |

Two ways to write — pick per task:

```python
from update_genie_space import GenieSpaces
client = GenieSpaces(host, token)

# (A) Section-level replace — give a portable dict; omitted sections are left alone.
client.apply_config(space_id, {"joins": [ ... ]})        # replaces ALL joins, nothing else

# (B) Within-section upsert — append one item without disturbing the others (idempotent by natural key).
_, inner, etag = client.get(space_id)
add_join_spec(inner, ...)          # or add_example_question_sql / add_sql_snippet / add_sample_question
client.patch(space_id, inner, etag)
```

Rule of thumb: **rebuilding a whole section from a source of truth → `apply_config`**. **Adding one item
alongside hand-authored content → `get` + `add_*` + `patch`.**

---

## 1. Extract the annotations

You can read from **either** place. Both give you rows of `(object, column, name, value)`.

### Option A — from Oracle directly (`oracledb`)
```python
import oracledb
con = oracledb.connect(user="...", password="...",
                       dsn=oracledb.makedsn("host", 1521, service_name="FREEPDB1"))
rows = con.cursor().execute("""
    SELECT a.object_name, a.column_name, a.annotation_name, a.annotation_value
    FROM   all_annotations_usage a
    JOIN   all_objects o
      ON   o.object_name = a.object_name AND o.object_type = a.object_type
    WHERE  o.owner = :owner
""", owner="SALES").fetchall()
# rows: [('ORDERS','CUSTOMER_ID','RELATED_TO','customers.customer_id;rt=MANY_TO_ONE'), ...]
```
> `ALL_ANNOTATIONS_USAGE` has **no owner column**, so join `ALL_OBJECTS` to scope by owner. (Annotation
> names are per-schema, not global — `SALES.CURRENCY` and `SYS.CURRENCY` can both exist.)

### Option B — from the sync's registry (Databricks SQL)
If the sync ran, every annotation is already in `bg.metadata_syn.oracle_annotations`:
```sql
SELECT oracle_object, oracle_column, annotation_name, annotation_value, uc_name
FROM   bg.metadata_syn.oracle_annotations
WHERE  is_active AND oracle_schema = 'SALES';
```
The registry also gives you `uc_name` (the resolved Unity Catalog name) for free — handy in step 3.

---

## 2. Determine the type → route to a Genie section

You decide what each annotation *means* from its **name** (your naming convention). A typical mapping:

| Annotation name | Meaning | Genie section |
|---|---|---|
| `RELATED_TO` | foreign-key relationship | `joins` |
| `ENUM` | allowed values for a column | `sql_filters` and/or `text_instruction` |
| `METRIC` / `CURRENCY` | a measure / money column | `sql_expressions` or `text_instruction` |
| `DESCRIPTION` (free text) | guidance | `text_instruction` |
| `PII`, `CLASSIFICATION` | governance | usually UC **tags**, not Genie — or fold into `text_instruction` |

A routing function just dispatches on the name:

```python
def route(name):
    return {
        "RELATED_TO": "join",
        "ENUM":       "filter",
        "METRIC":     "expression",
        "CURRENCY":   "expression",
        "DESCRIPTION":"instruction",
    }.get(name.upper())     # None = ignore for Genie purposes
```

You also need to turn Oracle object names into **Unity Catalog FQNs**. If you used the sync, take `uc_name`
from the registry. Otherwise apply your own rule, e.g.:

```python
def uc(obj, catalog="bg", schema="sales"):
    return f"{catalog}.{schema}.{obj.lower()}"
```

---

## 3. Construct + apply — examples per type

### 3a. `RELATED_TO` → a join
Value syntax used here: `right_table.right_col[;rt=MANY_TO_ONE]`. The annotated column is the **left** side.

```python
# parse 'customers.customer_id;rt=MANY_TO_ONE'
def parse_related_to(value):
    parts = value.split(";")
    rtbl, rcol = parts[0].split(".")
    rel = "MANY_TO_ONE"
    for p in parts[1:]:
        if p.lower().startswith("rt="):
            rel = p.split("=", 1)[1].upper()
    return rtbl, rcol, "FROM_RELATIONSHIP_TYPE_" + rel

joins = []
for obj, col, name, val in rows:
    if name.upper() != "RELATED_TO":
        continue
    rtbl, rcol, rel = parse_related_to(val)
    lalias, ralias = obj.lower(), rtbl.lower()
    joins.append({
        "left":  {"table": uc(obj),  "alias": lalias},
        "right": {"table": uc(rtbl), "alias": ralias},
        "on":    f"`{lalias}`.`{col.lower()}` = `{ralias}`.`{rcol.lower()}`",
        "relationship_type": rel,
        "instruction": f"{obj} relates to {rtbl} (from RELATED_TO annotation).",
    })

client.apply_config(space_id, {"joins": joins})   # rebuilds the joins section from the annotations
```

### 3b. `CURRENCY` / `METRIC` → a SQL expression (a measure)
```python
client.apply_config(space_id, {
    "sql_expressions": [
        {"display_name": "Order revenue",
         "sql":          "SUM(orders.amount)",
         "instruction":  "Total order revenue in USD. Use for 'revenue'/'sales' questions.",
         "synonyms":     ["revenue", "sales", "total amount"]},
    ],
})
```

### 3c. `ENUM` → a reusable filter (and/or guidance)
```python
# annotation: STATUS / 'PENDING,SHIPPED,DELIVERED,CANCELLED,RETURNED'
allowed = "PENDING,SHIPPED,DELIVERED,CANCELLED,RETURNED".split(",")
client.apply_config(space_id, {
    "sql_filters": [
        {"display_name": "Delivered orders",
         "sql":          "orders.status = 'DELIVERED'",
         "instruction":  "Orders that have been delivered.",
         "synonyms":     ["delivered", "completed orders"]},
    ],
})
# Or, just tell Genie the allowed set, as guidance:
client.apply_config(space_id, {
    "text_instruction": "orders.status is one of: " + ", ".join(allowed) + "."
})
```

### 3d. Free-text → `text_instruction`
`text_instruction` is a **single** combined string (the SDK caps it at one item), so concatenate everything:
```python
notes = [v for (_, _, n, v) in rows if n.upper() == "DESCRIPTION" and v]
client.apply_config(space_id, {"text_instruction": "\n".join(notes)})
```

### 3e. Worked examples + sample questions (not from annotations — author them directly)
```python
client.apply_config(space_id, {
    "examples": [
        {"question": "Total revenue by customer city",
         "sql": "SELECT c.city, SUM(o.amount) AS revenue "
                "FROM bg.sales.orders o JOIN bg.sales.customers c "
                "ON o.customer_id = c.customer_id GROUP BY c.city"},
    ],
    "sample_questions": ["What is total revenue by customer city?"],
})
```

### Append instead of replace (preserve existing items)
When you want to add one item without wiping a section a human curated:
```python
from update_genie_space import (add_join_spec, add_sql_snippet,
                                 add_example_question_sql, add_sample_question,
                                 SNIPPET_KIND_EXPRESSION)
_, inner, etag = client.get(space_id)
add_join_spec(inner, left_table=uc("ORDERS"), left_alias="orders",
              right_table=uc("CUSTOMERS"), right_alias="customers",
              join_on="`orders`.`customer_id` = `customers`.`customer_id`",
              relationship_type="FROM_RELATIONSHIP_TYPE_MANY_TO_ONE")
add_sql_snippet(inner, SNIPPET_KIND_EXPRESSION, display_name="Order revenue",
                sql="SUM(orders.amount)", instruction="Revenue in USD.",
                synonyms=["revenue", "sales"])
client.patch(space_id, inner, etag)     # re-runs are idempotent (matched by natural key)
```

---

## 4. End-to-end: annotations → Genie, in one script

```python
import os, oracledb
from update_genie_space import GenieSpaces

HOST, TOKEN = os.environ["DATABRICKS_HOST"], os.environ["DATABRICKS_TOKEN"]
SPACE_ID = "<your space id>"
client = GenieSpaces(HOST, TOKEN)

# 1. extract
con = oracledb.connect(user="dbx_fed", password="...",
                       dsn=oracledb.makedsn("host", 1521, service_name="FREEPDB1"))
rows = con.cursor().execute("""
  SELECT a.object_name, a.column_name, a.annotation_name, a.annotation_value
  FROM all_annotations_usage a JOIN all_objects o
    ON o.object_name=a.object_name AND o.object_type=a.object_type
  WHERE o.owner=:o""", o="SALES").fetchall()

def uc(obj): return f"bg.sales.{obj.lower()}"

# 2+3. route + construct
joins, exprs, notes = [], [], []
for obj, col, name, val in rows:
    n = (name or "").upper()
    if n == "RELATED_TO" and val:
        rtbl, rcol = val.split(";")[0].split(".")
        joins.append({"left": {"table": uc(obj), "alias": obj.lower()},
                      "right": {"table": uc(rtbl), "alias": rtbl.lower()},
                      "on": f"`{obj.lower()}`.`{col.lower()}` = `{rtbl.lower()}`.`{rcol.lower()}`",
                      "relationship_type": "FROM_RELATIONSHIP_TYPE_MANY_TO_ONE"})
    elif n in ("METRIC", "CURRENCY") and col:
        exprs.append({"display_name": f"{obj}.{col} measure",
                      "sql": f"SUM({obj.lower()}.{col.lower()})",
                      "instruction": f"Aggregate of {obj}.{col}.", "synonyms": [col.lower()]})
    elif n == "DESCRIPTION" and val:
        notes.append(val)

# apply — each section replaced from the annotations
config = {"joins": joins, "sql_expressions": exprs}
if notes:
    config["text_instruction"] = "\n".join(notes)
client.apply_config(SPACE_ID, config)
print(f"applied {len(joins)} joins, {len(exprs)} expressions")
```

---

## Gotchas
- **`apply_config` replaces an entire section.** Passing `{"joins": [...]}` overwrites *all* joins. To add
  without clobbering hand-authored content, use `get` + `add_*` + `patch`.
- **Map Oracle names to UC FQNs.** Genie needs three-part names (`catalog.schema.table`). Use the registry's
  `uc_name`, or your own rule.
- **The SDK hides the wire format** — you write plain strings; it adds the required ids, list-wrapping, and
  sort order. Don't hand-build the raw `serialized_space`.
- **Concurrency:** `patch` echoes an `etag`; a second writer gets `412` — re-`get` and retry. `apply_config`
  handles the get/patch for you.
- **What actually moves the needle:** `joins` and `examples` are applied strongly; `sql_filters`/
  `sql_expressions` engage when the user's wording matches their `synonyms`/`display_name`; `text_instruction`
  is advisory. Prefer encoding business logic as SQL (expressions/examples) over prose.
- **Comments are separate.** Table/column descriptions reach Genie via Unity Catalog comments, not this API —
  you don't push those here.

See `update_genie_space.py`'s module docstring/README for the full SDK reference.
