"""FastAPI app: reads via UC foreign catalog, writes directly to Postgres RDS."""
from contextlib import asynccontextmanager

from fastapi import FastAPI, Form, Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates

import config
import reads
import writes

templates = Jinja2Templates(directory="templates")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Open the RDS write pool; fail fast if RDS is unreachable.
    writes.pool.open(wait=True, timeout=30.0)
    yield
    writes.pool.close()


app = FastAPI(title="Postgres Foreign Catalog Demo", lifespan=lifespan)


@app.get("/health")
def health():
    return {"ok": True, "mode": "app" if config.IS_DATABRICKS_APP else "local"}


@app.get("/")
def index(request: Request):
    error = request.query_params.get("error")
    placed = request.query_params.get("placed")
    updated = request.query_params.get("updated")
    products, orders, read_error = [], [], None
    try:
        products = reads.get_products()
        orders = reads.get_recent_orders()
    except Exception as e:  # surface read-path issues in the UI
        read_error = str(e)
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "products": products,
            "orders": orders,
            "read_error": read_error,
            "error": error,
            "placed": placed,
            "updated": updated,
            "catalog": config.FOREIGN_CATALOG,
        },
    )


@app.post("/products/{product_id}")
def edit_product(
    product_id: int,
    name: str = Form(...),
    category: str = Form(...),
    price: float = Form(...),
    stock: int = Form(...),
):
    try:
        writes.update_product(product_id, name.strip(), category.strip(), price, stock)
    except Exception as e:
        return RedirectResponse(url=f"/?error={e}", status_code=303)
    return RedirectResponse(url=f"/?updated={product_id}", status_code=303)


@app.post("/orders")
def place_order(
    product_id: int = Form(...),
    quantity: int = Form(...),
    customer_name: str = Form(...),
):
    try:
        result = writes.create_order(product_id, quantity, customer_name.strip())
    except Exception as e:
        return RedirectResponse(url=f"/?error={e}", status_code=303)
    return RedirectResponse(url=f"/?placed={result['id']}", status_code=303)
