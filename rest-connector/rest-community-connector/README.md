# REST — Lakeflow Connect community connector

A [Lakeflow Connect](https://learn.microsoft.com/en-us/azure/databricks/ingestion/custom-connectors)
custom (community) connector that ingests tables from a bearer-authenticated REST API into
Unity Catalog. Snapshot ingestion with keyset pagination; supports additive schema evolution
and SCD1/SCD2 (set per-table in the pipeline spec).

> **Beta:** custom/community connectors are a Beta Lakeflow Connect feature — a workspace
> admin must enable it on the **Previews** page first.

## Layout

```
src/databricks/labs/community_connector/sources/rest/
├── rest.py            # RestLakeflowConnect: list_tables / get_table_schema / read_table
├── __init__.py        # RestDataSource(LakeflowSource) wiring
├── connector_spec.yaml# connection params (base_url, secret token) + external_options_allowlist
├── pyproject.toml     # packaging metadata
└── _generated_rest_python_source.py  # merged single-file build artifact
```

Drop `sources/rest/` into a clone of the community-connectors framework at the same path,
or `pip install -e .` this tree.

## Connection parameters (`connector_spec.yaml`)

| Param | Notes |
|---|---|
| `base_url` | Base URL of the REST API |
| `token` | Bearer token (secret) — sent as `Authorization: Bearer <token>` |

Table-level options passed through the connection: `limit`, `after` (keyset pagination).

## Publish + use (community-connector CLI)

```bash
# from the framework repo, venv active, DATABRICKS_CONFIG_PROFILE set
# 1) publish wheels to a UC volume + register the Custom tile
community-connector publish rest \
  -s src/databricks/labs/community_connector/sources/rest/connector_spec.yaml \
  -c <catalog> -t <schema>

# 2) create the UC connection
community-connector create_connection rest rest_conn \
  -o "{\"base_url\":\"$API_BASE_URL\",\"token\":\"$API_TOKEN\"}"

# 3) create an ingestion pipeline against your own pipeline spec
community-connector create_pipeline rest rest_ingest \
  -ps <your_pipeline_spec>.yaml -c <catalog> -t <schema>
```

The API this expects: `GET /tables` (list), `GET /tables/{t}/schema`
(INFORMATION_SCHEMA-style columns), `GET /tables/{t}?limit=&after=` (keyset page). See
`rest.py` for the exact contract and the type-token → Spark type mapping.
