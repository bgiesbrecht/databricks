# Postgres Foreign Catalog Demo

A small Databricks App that demonstrates a common hybrid pattern for working with an
external Postgres database from Databricks:

- **Reads** go through a Unity Catalog **foreign catalog** (Lakehouse Federation) — governed,
  read-only, and queryable with plain Databricks SQL.
- **Writes** go **directly** to the same Postgres instance via **psycopg** — because foreign
  catalogs are read-only and cannot accept `INSERT`/`UPDATE`/`DELETE`.

Both paths talk to the *same* underlying Postgres RDS, so a write made via psycopg is visible
on the next read through the foreign catalog.

```
Browser ──▶ App (FastAPI)
              │  reads  ─▶ Serverless SQL Warehouse ─▶ pg_foreign (UC foreign catalog) ─┐
              │  writes ─▶ psycopg ───────────────────────────────────────────────────▶ RDS Postgres
              └────────────────────────────────────────────  read-back  ◀──────────────┘
```

## Why two paths?

A **foreign catalog** federates an external database into Unity Catalog. It's the right tool for
reads: queries run through a Databricks SQL warehouse, inherit UC governance (grants, lineage,
audit), and let you join federated tables with anything else in the lakehouse — all without
copying data.

But federation is **read-only by design**. There is no supported way to write back through a
foreign catalog. So when the app needs to mutate the source data, it opens an ordinary Postgres
connection straight to the database and issues SQL there. The two paths are complementary:
governance and unified querying on the way in, a direct connection for mutations on the way out.

## The read path — foreign catalog via Databricks SQL

Reads use the Databricks SDK **Statement Execution API** against a serverless SQL warehouse,
querying the foreign catalog with normal SQL (`reads.py`):

```python
resp = client.statement_execution.execute_statement(
    warehouse_id=config.WAREHOUSE_ID,
    statement="SELECT id, name, category, price, stock "
              "FROM pg_foreign.public.products ORDER BY id",
    wait_timeout="30s",
)
```

Notes:
- `pg_foreign.public.products` is `<foreign-catalog>.<schema>.<table>` — the federated view of the
  RDS table. You can join federated tables together (the app joins `orders` back to `products`
  for names) just like native UC tables.
- The Statement Execution API returns all values as **strings**, so the app coerces numeric
  columns (`price`, `stock`, …) after fetching.
- Auth is automatic: in a deployed App the `WorkspaceClient` uses the app's **service principal**;
  locally it falls back to a CLI profile. The service principal needs
  `USE_CATALOG` / `USE_SCHEMA` / `SELECT` on the foreign catalog and `CAN_USE` on the warehouse.

## The write path — direct psycopg connection

Writes bypass Databricks entirely and connect to Postgres with a psycopg **connection pool**
(`writes.py`):

```python
pool = ConnectionPool(conninfo=_conninfo, min_size=1, max_size=5, open=False)

def update_product(product_id, name, category, price, stock):
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE products SET name=%s, category=%s, price=%s, stock=%s WHERE id=%s",
                (name, category, price, stock, product_id),
            )
        conn.commit()
```

Notes:
- Parameters are passed as a tuple (`%s` placeholders), never string-formatted — this is the
  psycopg-safe way to avoid SQL injection.
- The pool is opened lazily in the FastAPI **lifespan** hook (`open=False` at import) so importing
  the module never blocks; the app fails fast on startup if the database is unreachable.
- The connection string is assembled from env vars (host, port, db, user, password, `sslmode`).
  The **password comes from a Databricks secret scope**, injected as `PGPASSWORD` — never
  committed to config or code.

## App structure

| File | Responsibility |
|---|---|
| `app.py` | FastAPI routes: `/` (read + render), `/products/{id}` (edit), `/orders` (insert), `/health` |
| `reads.py` | Read path — Statement Execution against the foreign catalog |
| `writes.py` | Write path — psycopg connection pool to Postgres |
| `config.py` | Dual-mode config (deployed App vs. local) from env vars |
| `templates/index.html` | Bootstrap UI: product table with inline edit + order form |

The app is **dual-mode**: it detects whether it's running as a deployed Databricks App (via an
injected env var) or locally, and picks the right authentication accordingly — so the same code
runs in both places.

## Key takeaways

1. **Foreign catalogs are read-only.** Use them for governed, federated reads; don't expect to
   write through them.
2. **Write directly to the source** when you need mutations — a normal Postgres driver connection
   is the supported path, and it hits the same data the foreign catalog reads.
3. **Reads and writes stay consistent** because both target the one underlying database; there's
   no copy or sync step to reconcile.
4. **Keep secrets in a secret scope**, inject at runtime, and use parameterized SQL on the write
   path.

> See `DB_SETUP.md` for the infrastructure setup (RDS, foreign catalog, warehouse, networking,
> and teardown).
