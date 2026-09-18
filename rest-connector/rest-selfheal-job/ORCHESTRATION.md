# Self-healing pipeline runs + error callbacks

`pipeline_run_selfheal.py` runs the ingestion pipeline and **automatically recovers from
breaking schema changes** (dropped columns, incompatible type changes) with a full refresh —
while surfacing every *other* failure instead of masking it.

## Why

Lakeflow managed ingestion does **additive** schema evolution automatically, but a **drop**
or **incompatible type change** fails the incremental update with
`SAAS_INCOMPATIBLE_SCHEMA_CHANGES` ("could have been renamed … perform a full refresh").
A full refresh reconciles it. This runner detects that specific error and does the full
refresh for you — once — then re-checks.

## Logic

```
start incremental update ──► COMPLETED ──► exit 0
        │ FAILED
        ▼
  read failure reason (event log)
        │
        ├─ breaking schema change (SAAS_INCOMPATIBLE_SCHEMA_CHANGES / "full refresh")
        │        └─► start FULL REFRESH ──► COMPLETED ─► exit 0
        │                                └─ FAILED ────► alert + exit 1
        └─ any other failure ───────────────────────► alert + exit 1 (NOT auto-refreshed)
```

A blind "always full refresh on failure" would hide real bugs and (for SCD2) discard
history, so it only full-refreshes for the known-recoverable schema signatures. Pass
`--no-auto-refresh` to detect + alert only.

## Run locally

```bash
pip install databricks-sdk           # only dependency
python pipeline_run_selfheal.py \
  --pipeline-id <your-scd1-pipeline-id> \
  --profile <your-cli-profile> \
  --webhook "$SLACK_WEBHOOK_URL"        # optional: Slack/Teams/PagerDuty incoming webhook
```

## Deployed as a scheduled Databricks Job (DAB)

The self-heal runner is codified in `databricks.yml` (bundle `rest-selfheal`) as a
serverless Python-task Job that self-heals unattended. In a Job the SDK auth comes from the
job context, so `--profile` is omitted. The `dev` target deploys the schedule **PAUSED**;
the `prod` target runs it **UNPAUSED** with `run_as` the deploying user.

```bash
export DATABRICKS_CONFIG_PROFILE=<your-cli-profile>
# set the workspace host in databricks.yml (targets.*.workspace.host) first
databricks bundle deploy -t dev                       # deploy the job (schedule paused)
databricks bundle run   rest_ingest_selfheal -t dev   # run once now
databricks bundle deploy -t prod                      # production: schedule unpaused, run_as user
```

The job passes `--pipeline-id ${var.pipeline_id}` (default = the SCD1 pipeline). Add
`webhook_notifications.on_failure` with a notification-destination id for Slack/Teams/PagerDuty.

## Prerequisites (CLI-managed — NOT in the bundle)

This Databricks CLI (v0.299.2) does **not** model the UC **connection** (`resources.connections`
is an unknown field) or the **community/custom-connector ingestion pipelines**
(`source_type: COMMUNITY` is not a recognized value — Beta). So the bundle manages only the
Job; the connection, wheels, and pipelines are created with the `community-connector` CLI and
are **prerequisites** the job depends on. Revisit codifying them when the CLI adds support.

The `rest` connector itself ships separately (its own package); publish it first, then
create the connection and pipelines. `pipeline_spec.yaml` (SCD1) and `pipeline_spec_scd2.yaml`
(SCD2) in this package are the pipeline definitions the commands below reference.

```bash
# framework repo cloned + rest connector published, venv active, DATABRICKS_CONFIG_PROFILE set
# 1) publish connector wheels to a UC volume + register the Custom tile (from the connector pkg)
community-connector publish rest \
  -s src/databricks/labs/community_connector/sources/rest/connector_spec.yaml \
  -c <catalog> -t <schema>

# 2) create the UC connection (base_url + bearer token for your REST API)
community-connector create_connection rest rest_conn \
  -o "{\"base_url\":\"$API_BASE_URL\",\"token\":\"$API_TOKEN\"}"

# 3) create the ingestion pipelines from the specs in this package
community-connector create_pipeline rest rest_ingest \
  -ps ./pipeline_spec.yaml       -c <catalog> -t <schema>
community-connector create_pipeline rest rest_scd2_ingest \
  -ps ./pipeline_spec_scd2.yaml  -c <catalog> -t <schema>
```

Then set `var.pipeline_id` in `databricks.yml` (or pass `--var pipeline_id=<id>` on deploy)
to the resulting **SCD1** pipeline id, which is the pipeline the self-heal Job drives.

## Error callbacks — the layers

| Mechanism | What it does |
|---|---|
| Pipeline `notifications` block | Email on `on-update-failure` / `on-flow-failure` (declarative, no code) |
| Job `email_notifications` / `webhook_notifications` | Email + Slack/Teams/PagerDuty/webhook when the Job (self-heal wrapper) fails |
| `--webhook` in this script | Immediate Slack/webhook POST at the moment of failure/recovery |
| Pipeline event log (`list_pipeline_events`) | Full error history; this script reads it to classify the failure |

So "coding callbacks for errors" = the Job's webhook/email notifications + this script's
inline `--webhook`, driven by classifying the event-log error.
