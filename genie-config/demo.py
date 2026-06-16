"""
demo.py — one-shot: create a fresh Genie Space from solar_panels_config.json.

Prerequisites:
  1. Run setup_bg_solar_panels.sql against your SQL warehouse to populate the
     demo table. The default FQN is bg.solar.panels; to target a different
     catalog/schema, pass --catalog / --schema / --table here AND substitute
     the same names in the SQL (see that file's header for a sed one-liner).
  2. Export DATABRICKS_HOST and DATABRICKS_TOKEN.

Usage:
    python3 demo.py --warehouse-id <warehouse_id>
    python3 demo.py --warehouse-id <id> --catalog my_cat --schema my_sch \\
                    --table panels --title "My demo"

Prints the URL of the new space when finished.
"""

import argparse
import json
import os
import sys

from update_genie_space import GenieSpaces


# The FQN used in the bundled config and setup SQL.
DEFAULT_FQN = "bg.solar.panels"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--warehouse-id", required=True,
                    help="SQL warehouse the space will run queries against.")
    ap.add_argument("--catalog", default="bg",
                    help="Catalog containing the demo table (default: bg).")
    ap.add_argument("--schema", default="solar",
                    help="Schema containing the demo table (default: solar).")
    ap.add_argument("--table", default="panels",
                    help="Table name within the schema (default: panels).")
    ap.add_argument("--title", default="Solar Panel Installations (demo)",
                    help="Display name for the new space.")
    ap.add_argument("--parent-path", default="",
                    help='Workspace folder, e.g. "/Users/you@example.com".')
    ap.add_argument("--config", default="solar_panels_config.json",
                    help="Path to the portable config JSON (default: solar_panels_config.json).")
    args = ap.parse_args()

    try:
        host  = os.environ["DATABRICKS_HOST"]
        token = os.environ["DATABRICKS_TOKEN"]
    except KeyError as missing:
        sys.exit(f"missing env var: {missing.args[0]}")

    fqn = f"{args.catalog}.{args.schema}.{args.table}"

    # Load the config as raw text so we can rewrite the table FQN before parsing.
    # The bundled config references `bg.solar.panels` in instructions, joins,
    # and SQL examples — a single string replace covers all of them.
    with open(args.config) as f:
        raw = f.read()
    if fqn != DEFAULT_FQN:
        raw = raw.replace(DEFAULT_FQN, fqn)
    config = json.loads(raw)
    config.pop("_comment", None)

    client = GenieSpaces(host, token)
    print(f"Creating space {args.title!r} on warehouse {args.warehouse_id} ...")
    print(f"  table:    {fqn}")
    created = client.create(
        warehouse_id=args.warehouse_id,
        table_identifiers=[fqn],
        title=args.title,
        parent_path=args.parent_path,
        config=config,
    )

    sid = created["space_id"]
    url = f"{host.rstrip('/')}/genie/rooms/{sid}"
    print(f"  space_id: {sid}")
    print(f"  title:    {created.get('title')!r}")
    print(f"  open:     {url}")


if __name__ == "__main__":
    main()
