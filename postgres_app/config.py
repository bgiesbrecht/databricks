"""Runtime configuration and dual-mode (local vs Databricks App) settings.

This app demonstrates a real customer pattern:
  * READS  -> go through a Unity Catalog *foreign catalog* that federates to
              an external Postgres RDS. Foreign catalogs are READ-ONLY, so all
              SELECTs run against Databricks SQL (Lakehouse Federation).
  * WRITES -> go DIRECTLY to the same Postgres RDS via psycopg, because the
              foreign catalog cannot be written to.
"""
import os

# True when running inside a deployed Databricks App (env injected by runtime).
IS_DATABRICKS_APP = bool(os.environ.get("DATABRICKS_APP_NAME"))

# Local dev: which CLI profile to authenticate the WorkspaceClient with.
DATABRICKS_PROFILE = os.environ.get("DATABRICKS_PROFILE", "oneenv-bgies-pgforeign")

# ---- Read path (foreign catalog via Databricks SQL warehouse) ----
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
FOREIGN_CATALOG = os.environ.get("FOREIGN_CATALOG", "pg_foreign")
FOREIGN_SCHEMA = os.environ.get("FOREIGN_SCHEMA", "public")

# ---- Write path (direct psycopg connection to RDS Postgres) ----
PGHOST = os.environ.get("PGHOST", "")
PGPORT = os.environ.get("PGPORT", "5432")
PGDATABASE = os.environ.get("PGDATABASE", "appdb")
PGUSER = os.environ.get("PGUSER", "dbadmin")
PGPASSWORD = os.environ.get("PGPASSWORD", "")
PGSSLMODE = os.environ.get("PGSSLMODE", "require")
