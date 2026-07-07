"""READ path: query the Postgres RDS *through the Unity Catalog foreign catalog*.

We use the Databricks SDK Statement Execution API against a serverless SQL
warehouse. In a deployed App the WorkspaceClient authenticates automatically
with the app's service principal; locally it uses the CLI profile.
"""
from databricks.sdk import WorkspaceClient

import config

_w = None


def _client() -> WorkspaceClient:
    global _w
    if _w is None:
        _w = (
            WorkspaceClient()
            if config.IS_DATABRICKS_APP
            else WorkspaceClient(profile=config.DATABRICKS_PROFILE)
        )
    return _w


def _run(sql: str) -> list[dict]:
    """Execute a read-only query on the foreign catalog, return list of dict rows."""
    resp = _client().statement_execution.execute_statement(
        warehouse_id=config.WAREHOUSE_ID,
        statement=sql,
        wait_timeout="30s",
    )
    if resp.status and resp.status.state and resp.status.state.value != "SUCCEEDED":
        msg = resp.status.error.message if resp.status.error else str(resp.status.state)
        raise RuntimeError(f"Foreign-catalog query failed: {msg}")

    cols = [c.name for c in resp.manifest.schema.columns] if resp.manifest else []
    data = resp.result.data_array if (resp.result and resp.result.data_array) else []
    return [dict(zip(cols, row)) for row in data]


def _num(v, cast):
    """Statement Execution returns all values as strings; coerce for templating."""
    try:
        return cast(v)
    except (TypeError, ValueError):
        return v


def get_products() -> list[dict]:
    fq = f"{config.FOREIGN_CATALOG}.{config.FOREIGN_SCHEMA}.products"
    rows = _run(f"SELECT id, name, category, price, stock FROM {fq} ORDER BY id")
    for r in rows:
        r["id"] = _num(r["id"], int)
        r["price"] = _num(r["price"], float)
        r["stock"] = _num(r["stock"], int)
    return rows


def get_recent_orders(limit: int = 20) -> list[dict]:
    cat, sch = config.FOREIGN_CATALOG, config.FOREIGN_SCHEMA
    # Join back to products through the same foreign catalog to show product names.
    rows = _run(
        f"""
        SELECT o.id, o.customer_name, o.quantity, o.created_at,
               p.name AS product_name, p.price
        FROM {cat}.{sch}.orders o
        JOIN {cat}.{sch}.products p ON p.id = o.product_id
        ORDER BY o.created_at DESC
        LIMIT {int(limit)}
        """
    )
    for r in rows:
        r["id"] = _num(r["id"], int)
        r["quantity"] = _num(r["quantity"], int)
        r["price"] = _num(r["price"], float)
    return rows
