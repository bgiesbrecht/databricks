"""
demo_genie_pipeline.py — create the Genie room for the Oracle→UC→Genie pipeline.

Creates a Genie space over the synced bg.sales tables, seeded with vocabulary +
sample questions but **no joins** — the joins are added later by the sync's
`genie_push` hook from Oracle `RELATED_TO` annotations, so you can see the
before/after.

End-to-end flow:
  1. python3 demo_genie_pipeline.py --warehouse-id <id>      # creates room, prints space_id
  2. paste the space_id into config/sync_config.yaml (sales sync -> hooks.genie_space_id)
  3. run the `setup` job/notebook to reload sync_config
  4. add a RELATED_TO annotation in Oracle (see README §10) + edit a comment
  5. run the `sync` job/notebook with apply=true  ->  genie_push hook updates the room

Auth: uses DATABRICKS_HOST/DATABRICKS_TOKEN if set, else falls back to the
Databricks CLI profile (--profile, default e2-demo-west).
"""

import argparse
import json
import os
import subprocess
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src", "hooks"))
from update_genie_space import GenieSpaces


def cli_auth(profile):
    tok = subprocess.run(["databricks", "auth", "token", "--profile", profile],
                         capture_output=True, text=True)
    if tok.returncode != 0:
        sys.exit(f"could not get token from profile {profile!r}: {tok.stderr.strip()}")
    token = json.loads(tok.stdout)["access_token"]
    h = subprocess.run(["databricks", "auth", "describe", "--profile", profile, "-o", "json"],
                       capture_output=True, text=True)
    host = None
    if h.returncode == 0:
        try:
            host = json.loads(h.stdout).get("details", {}).get("host")
        except Exception:
            host = None
    return host, token


SEED = {
    "text_instruction": (
        "## Vocabulary\n"
        "- A 'customer' is one row in bg.sales.customers (key: customer_id).\n"
        "- An 'order' is one row in bg.sales.orders (key: order_id).\n"
        "- 'Revenue' / 'sales' = SUM(orders.amount), in USD.\n"
        "- bg.sales.order_summary is pre-aggregated order metrics by fulfillment status.\n"
        "## Conventions\n"
        "- Always use three-part names (catalog.schema.table).\n"
        "- Round money to 2 decimals.\n"
        "## Note\n"
        "- Table and column descriptions are synced from Oracle and kept current; trust them."
    ),
    "sample_questions": [
        "How many customers do we have?",
        "What is total revenue by order status?",
        "Which customers have placed the most orders?",
        "List orders with their customer name and city.",
    ],
}


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--warehouse-id", required=True)
    ap.add_argument("--catalog", default="bg")
    ap.add_argument("--schema", default="sales")
    ap.add_argument("--tables", default="customers,orders,order_summary",
                    help="comma-separated table names in the schema")
    ap.add_argument("--title", default="Oracle Sales (synced) — demo")
    ap.add_argument("--parent-path", default="")
    ap.add_argument("--profile", default="e2-demo-west")
    args = ap.parse_args()

    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")
    if not (host and token):
        host, token = cli_auth(args.profile)

    fqns = [f"{args.catalog}.{args.schema}.{t.strip()}" for t in args.tables.split(",") if t.strip()]
    client = GenieSpaces(host, token)
    print(f"Creating Genie space {args.title!r} over: {', '.join(fqns)}")
    created = client.create(
        warehouse_id=args.warehouse_id,
        table_identifiers=fqns,
        title=args.title,
        parent_path=args.parent_path,
        config=SEED,
    )
    sid = created["space_id"]
    print(f"\n  space_id: {sid}")
    print(f"  open:     {host.rstrip('/')}/genie/rooms/{sid}")
    print(f"\nNext: set genie_space_id: \"{sid}\" on the `sales` sync in config/sync_config.yaml,")
    print("then run `setup`, make an Oracle RELATED_TO change, and run `sync` with apply=true.")


if __name__ == "__main__":
    main()
