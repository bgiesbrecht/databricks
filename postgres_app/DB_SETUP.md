# Postgres Foreign Catalog Demo — DB Setup (One-Pager)

A Databricks App that **reads** from Postgres RDS through a Unity Catalog **foreign catalog**
(read-only, Lakehouse Federation) and **writes** directly to the same RDS via **psycopg**
(orders + product edits). Foreign catalogs cannot be written to, so writes bypass them.

```
Browser ──▶ App (FastAPI)
              │  reads  ─▶ Serverless SQL Warehouse ─▶ pg_foreign (UC foreign catalog) ─┐
              │  writes ─▶ psycopg ───────────────────────────────────────────────────▶ RDS Postgres
              └────────────────────────────────────────────  read-back  ◀──────────────┘
```

## RDS Postgres (AWS)
| Item | Value |
|---|---|
| Instance | `<rds-instance-name>` (e.g. db.t3.micro, publicly accessible) |
| Endpoint | `<rds-endpoint>:5432` |
| Database | `<database>` · user `<db-user>` · `sslmode=require` |
| Region / VPC | `<region>` / `<vpc-id>` |
| Security group | `<security-group-id>` (Postgres 5432 ingress) |
| Tables | `products` (read/edit), `orders` (insert) |

Password is **not** stored in config or docs — keep it in a Databricks secret scope
(e.g. scope `<secret-scope>`, key `<secret-key>`) and inject it into the app as `PGPASSWORD`
via an app resource.

## Networking (the important part)
Serverless compute (SQL warehouse **and** the App) reaches RDS over **public IPs**, so the RDS
security group must allowlist Databricks' serverless egress CIDRs. `0.0.0.0/0` is auto-revoked by
sandbox automation — use the published ranges instead.

Source of truth: `https://www.databricks.com/networking/v1/ip-ranges.json`
(filter `service=Databricks, type=outbound, platform=aws, region=<region>`). These rotate ~monthly.

Allowlist the current serverless outbound CIDRs for your region on the RDS security group
(port 5432), plus your operator IP (`/32`) if you need to seed data directly.

## Unity Catalog
| Object | Value |
|---|---|
| Connection | `<connection-name>` (type `POSTGRESQL` → RDS endpoint) |
| Foreign catalog | `pg_foreign` (database `<database>`; exposes `pg_foreign.public.products` / `.orders`) |
| SQL Warehouse | `<warehouse-id>` (Serverless Starter) |

Read example (runs against the warehouse, federated to RDS):
```sql
SELECT id, name, category, price, stock FROM pg_foreign.public.products ORDER BY id;
```

## App
| Item | Value |
|---|---|
| Name | `<app-name>` (FastAPI + Bootstrap) |
| Service principal | `<service-principal-id>` — granted `USE_CATALOG/USE_SCHEMA/SELECT` on `pg_foreign` + `CAN_USE` on the warehouse |
| Reads | `reads.py` → SDK Statement Execution on the warehouse |
| Writes | `writes.py` → psycopg connection pool to RDS |

## Environment / cleanup
Deploy in a sandbox account (e.g. One-Env) that has automated cleanup, so demo infra is
short-lived. To tear down: delete the app, foreign catalog `pg_foreign`, the connection,
the secret scope, the RDS instance, its security group, and the workspace.
