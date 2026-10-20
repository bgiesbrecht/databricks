# databricks

A collection of Databricks programs, notebooks, and scripts covering a range of tasks, from application logging and metadata sync to example apps and migration tooling. Each project lives in its own directory and is self-contained, and most have their own README with setup and usage details.

## Contents

| Directory | Description |
|---|---|
| [`centralized_logging/`](centralized_logging/) | Asset Bundle for centralized application logging: apps write structlog JSONL to S3, and a file-triggered Auto Loader pipeline ingests it into a Delta table. |
| [`logging/`](logging/) | Notebook that sends structlog output straight to a Unity Catalog Delta table so you can query logs with SQL. |
| [`genie-config/`](genie-config/) | Python module for reading and editing a Genie Space's configuration (instructions, SQL examples, joins, sample questions) through the public API. |
| [`oracle-uc-metadata-sync/`](oracle-uc-metadata-sync/) | Copies Oracle table and column comments and annotations onto federated Unity Catalog objects, and can push them into Genie. |
| [`postgres_app/`](postgres_app/) | Example Databricks App that reads from an external Postgres database through a foreign catalog and writes back to it directly. |
| [`rest-connector/`](rest-connector/) | A Lakeflow Connect community connector for bearer-authenticated REST APIs, plus a self-healing Job/DAB that recovers from breaking schema changes. |
| [`ssis-to-databricks/`](ssis-to-databricks/) | Tools for converting SSIS `.dtsx` packages to an intermediate representation and then to SparkSQL, as a complement to Lakebridge. |
| [`misc/`](misc/) | Standalone utilities: a privacy-safe column statistics profiler, and notebooks and scripts that refresh Snowflake Horizon bearer tokens for Iceberg REST catalog connections. |

## Usage

These are examples and working tools, not a packaged library. Start with the README in the directory you want to use. Projects with a `databricks.yml` are Declarative Automation Bundles (formerly Asset Bundles) and deploy with `databricks bundle deploy`. Files that begin with `# Databricks notebook source` can be imported into a workspace as notebooks.
