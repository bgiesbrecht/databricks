# REST ingestion — self-healing Job + DAB

The **Databricks side** of the REST → Unity Catalog pipeline: a scheduled, self-healing
Databricks Job (packaged as an Asset Bundle) that runs a Lakeflow ingestion pipeline and
automatically recovers from breaking schema changes.

> The `rest` **community connector** it drives ships as a separate package. Publish that and
> create the connection + pipelines first (see `ORCHESTRATION.md` → Prerequisites).

## Contents

| File | What it is |
|---|---|
| `databricks.yml` | **The DAB** — bundle `rest-selfheal`, `dev` (schedule paused) + `prod` (schedule unpaused, `run_as` user) targets, serverless Python-task Job. |
| `pipeline_run_selfheal.py` | **Job code** — runs an incremental update; on a *breaking* schema failure it selectively full-refreshes only the affected table(s), surfaces every other failure. Deps: `databricks-sdk`. |
| `pipeline_spec.yaml` | **SCD1** ingestion definition (10 tables). |
| `pipeline_spec_scd2.yaml` | **SCD2** ingestion definition (members + advances, `__START_AT`/`__END_AT`). |
| `ORCHESTRATION.md` | Self-heal logic, error-callback layers, run-locally + deploy commands, CLI-managed prerequisites. |

## Quick start

1. **Prerequisites** — publish the `rest` connector, create the `rest_conn` UC connection, and
   create the ingestion pipeline(s) from the specs here. Commands in `ORCHESTRATION.md`.
2. **Configure `databricks.yml`** — set `targets.*.workspace.host`, the prod `root_path` /
   `run_as`, and `var.pipeline_id` = your **SCD1** pipeline id (or pass `--var pipeline_id=<id>`).
   Set `var.notify_email` for job-failure alerts.
3. **Deploy**

   ```bash
   export DATABRICKS_CONFIG_PROFILE=<your-cli-profile>
   databricks bundle deploy -t dev                       # schedule PAUSED
   databricks bundle run   rest_ingest_selfheal -t dev   # run once now
   databricks bundle deploy -t prod                      # schedule UNPAUSED, run_as user
   ```

## Self-heal behavior

Additive schema evolution is automatic. A breaking change (incompatible type change, rename
ambiguity → `SAAS_INCOMPATIBLE_SCHEMA_CHANGES`) fails the incremental; the Job detects it from
the pipeline event log and retries once with a **selective** full refresh of only the failed
table(s), falling back to a full refresh of all tables if the table can't be pinned down. Any
*other* failure is surfaced (exit 1), never silently full-refreshed. Alerts go out via the
Job's `email_notifications` and the script's optional `--webhook`.

> **Note:** the `prod` target deploys the schedule UNPAUSED — it arms a daily 06:00 CT run as
> soon as you deploy. Make sure the upstream source/pipeline is available at run time (or adjust
> the cron / deploy `dev` first).
