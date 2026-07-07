"""WRITE path: insert directly into Postgres RDS via psycopg.

The Unity Catalog foreign catalog is read-only, so writes bypass it and go
straight to the RDS instance over a normal Postgres connection.
"""
from psycopg_pool import ConnectionPool

import config

_conninfo = (
    f"host={config.PGHOST} port={config.PGPORT} dbname={config.PGDATABASE} "
    f"user={config.PGUSER} password={config.PGPASSWORD} sslmode={config.PGSSLMODE}"
)

# Deferred open so import never blocks; opened in the FastAPI lifespan hook.
pool = ConnectionPool(conninfo=_conninfo, min_size=1, max_size=5, open=False)


def update_product(product_id: int, name: str, category: str, price: float, stock: int) -> None:
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE products
                SET name = %s, category = %s, price = %s, stock = %s
                WHERE id = %s
                """,
                (name, category, price, stock, product_id),
            )
        conn.commit()


def create_order(product_id: int, quantity: int, customer_name: str) -> dict:
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO orders (product_id, quantity, customer_name)
                VALUES (%s, %s, %s)
                RETURNING id, created_at
                """,
                (product_id, quantity, customer_name),
            )
            row = cur.fetchone()
        conn.commit()
    return {"id": row[0], "created_at": row[1].isoformat()}
