# Centralized Logging

A Databricks Asset Bundle (DAB) for centralized application logging on Databricks.

Applications log with [structlog](https://www.structlog.org/) and write JSONL files to S3. A Unity
Catalog external Volume sits over the S3 prefix. A job with a file arrival trigger runs a serverless
Lakeflow Spark Declarative Pipeline whenever new files land. The pipeline uses Auto Loader to ingest
them into the bronze table `<CATALOG>.<SCHEMA>.centralized_logs`.

```
any app (structlog, etc.) ──PutObject──> s3://<BUCKET>/logs/application_logs/*.jsonl
                                                │
                    external location (file events, managed SQS) + external Volume
                                                ▼
                       /Volumes/<CATALOG>/<SCHEMA>/<VOLUME>/
                                                │ file arrival trigger
                                                ▼
                       job: centralized-logging-trigger ──> pipeline: centralized-logging-ingest (Auto Loader)
                                                │
                                                ▼
                                  <CATALOG>.<SCHEMA>.centralized_logs (bronze)
```

### Why S3 rather than a managed Volume?

Anything that can write to S3 can send logs, including apps that don't run on Databricks. That makes
this truly centralized logging. A managed Volume also works with Auto Loader and needs no AWS setup,
but only Databricks workloads can write to it. The `Structlog Vol` notebook covers that variant.

## Project layout

| Path | Purpose |
| --- | --- |
| `databricks.yml` | Bundle config, variables, `dev` / `prod` targets |
| `resources/logs.external_location.yml` | UC external location over the bucket, file events enabled (managed SQS) |
| `resources/application_logs.volume.yml` | External Volume over `logs/application_logs` |
| `resources/logging_ingest.pipeline.yml` | Serverless, Photon SDP pipeline |
| `resources/logging_ingest.job.yml` | Job with a file arrival trigger on the Volume that runs the pipeline |
| `src/pipeline/autoloader_ingest.py` | Auto Loader bronze table `centralized_logs` |
| `src/structlog/Structlog S3 - 1.2.py` | Sample producer that writes batched JSONL to S3 via boto3 |
| `src/structlog/Structlog Vol - 1.2.py` | Sample producer that writes batched JSONL to the Volume (Databricks only) |
| `aws/` | IAM policy templates (replace the `<PLACEHOLDERS>`) |

## Current deployment (`<workspace-profile>`, dev target)

| Resource | Name |
| --- | --- |
| AWS account / region | `<AWS_ACCOUNT_ID>` (profile `<aws-profile>`) / `<AWS_REGION>` |
| S3 bucket | `<BUCKET>` (public access blocked) |
| IAM role (used by UC) | `<ROLE_NAME>` |
| IAM user (used by external writers) | `<WRITER_USER>` (put-only on `logs/application_logs/*`) |
| UC storage credential | `<CREDENTIAL>` |
| UC external location | `<EXTERNAL_LOCATION>` → `s3://<BUCKET>/` |
| UC Volume | `<CATALOG>.<SCHEMA>.<VOLUME>` → `s3://<BUCKET>/logs/application_logs` |
| Secret scope | `<SCOPE>` (`aws-access-key-id`, `aws-secret-access-key`) |
| Pipeline | `[dev <user>] centralized-logging-ingest` |
| Job | `[dev <user>] centralized-logging-trigger` |
| Bronze table | `<CATALOG>.<SCHEMA>.centralized_logs` |

## Setup from scratch

The bundle manages the Databricks side (external location, Volume, pipeline, job). The AWS resources,
storage credential and secrets are set up once, as follows.

### 1. AWS: bucket

```bash
export AWS_PROFILE=<aws-profile> AWS_REGION=us-west-2
aws s3api create-bucket --bucket <BUCKET> --create-bucket-configuration LocationConstraint=us-west-2
aws s3api put-public-access-block --bucket <BUCKET> --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
```

### 2. AWS: IAM role for Unity Catalog

Create the role with `aws/trust_policy.json`, using a placeholder external ID for now. Then attach
these inline policies (fill in the placeholders):

| Policy | Purpose |
| --- | --- |
| `aws/s3_access_policy.json` | Read/write/list on the bucket |
| `aws/file_events_policy.json` | Lets UC provision the SNS/SQS resources for managed file events |
| `aws/self_assume_policy.json` | UC requires the role to be able to assume itself |

### 3. Databricks: storage credential, then the trust policy

```bash
databricks storage-credentials create --profile <profile> --json '{
  "name": "<CREDENTIAL>",
  "aws_iam_role": {"role_arn": "arn:aws:iam::<AWS_ACCOUNT_ID>:role/<ROLE_NAME>"},
  "skip_validation": true}'
```

Copy `aws_iam_role.external_id` from the output into `aws/trust_policy.json` and apply it with
`aws iam update-assume-role-policy`. Then validate:

```bash
databricks storage-credentials validate --storage-credential-name <CREDENTIAL> \
  --url s3://<BUCKET>/ --profile <profile>
```

New roles can take about 5 minutes to propagate in IAM. Until then, validation results flip between
checks (READ `403`, "non self-assuming"). Wait until every check passes on consecutive runs before
deploying.

### 4. Writer credentials for external producers (S3 logger only)

```bash
aws iam create-user --user-name <WRITER_USER>
aws iam put-user-policy --user-name <WRITER_USER> --policy-name write-logs \
  --policy-document file://aws/writer_user_policy.json
aws iam create-access-key --user-name <WRITER_USER>   # store the output straight into secrets; don't commit it

databricks secrets create-scope <SCOPE> --profile <profile>
databricks secrets put-secret <SCOPE> aws-access-key-id --profile <profile>
databricks secrets put-secret <SCOPE> aws-secret-access-key --profile <profile>
```

Producers outside Databricks get these keys from your own secret store.

### 5. Deploy the bundle

```bash
databricks bundle validate --strict -t dev --profile <profile>
databricks bundle plan     -t dev --profile <profile>
databricks bundle deploy   -t dev --profile <profile>
```

Variables marked *required* below have no default. Supply them with `--var name=value`,
`BUNDLE_VAR_<name>`, or a git-ignored `.databricks/bundle/<target>/variable-overrides.json`:

```json
{"catalog": "<CATALOG>", "schema": "<SCHEMA>", "storage_credential": "<CREDENTIAL>",
 "s3_bucket": "<BUCKET>", "external_location_name": "<EXTERNAL_LOCATION>", "volume_name": "<VOLUME>"}
```

## Bundle variables

| Variable | Default | Description |
| --- | --- | --- |
| `catalog` / `schema` | required | Location of the Volume and bronze table |
| `storage_credential` | required | UC storage credential for the IAM role |
| `s3_bucket` | required | Logs bucket |
| `s3_logs_prefix` | `logs/application_logs` | Prefix that backs the Volume |
| `external_location_name` | required | UC external location name |
| `volume_name` | required | UC Volume name |
| `notification_email` | deploying user | Notified when the job fails |
| `clean_source` | `OFF` | Auto Loader handling of ingested files: `OFF`, `DELETE`, or `MOVE` |
| `clean_source_retention` | `30 days` | How long to wait before deleting/moving ingested files |
| `clean_source_move_destination` | empty | Archive path, required when `clean_source` is `MOVE` |

## Sending logs

Both sample notebooks configure structlog with an `event_organizer` processor. It reshapes every event
into this format:

```json
{"timestamp": "...", "level": "INFO", "event": "Processing record", "logger": "root",
 "application": "my-app", "details": {"record_id": 42}}
```

Wrap your entry point with the decorator. Logs are buffered (`batch_size`, default 100) and flushed to
one `.jsonl` file per batch, plus a final flush when the function exits.

```python
# S3 (works anywhere; credentials from the <SCOPE> secret scope)
@log_cfg_s3(s3_bucket="<BUCKET>", s3_prefix="logs/application_logs", application="my-app")
def main():
    log = structlog.get_logger()
    log.info("Processing record", record_id=42)

# Volume (Databricks only, no AWS credentials needed)
@log_cfg_volume(volume="/Volumes/<CATALOG>/<SCHEMA>/<VOLUME>", application="my-app")
def main(): ...
```

Any other producer only has to write newline-delimited JSON objects with that shape to
`s3://<BUCKET>/logs/application_logs/`.

## Querying logs

```sql
SELECT timestamp, level, application, event, details, loaded_timestamp
FROM <CATALOG>.<SCHEMA>.centralized_logs
WHERE level = 'ERROR' AND timestamp > current_timestamp() - INTERVAL 1 DAY
ORDER BY timestamp DESC;
```

The schema is inferred: `cloudFiles.inferColumnTypes`, with `timestamp` hinted as `TIMESTAMP`. New
keys in `details` are added as the schema evolves.

## Operational notes

- **Trigger:** `pause_status` is set to `UNPAUSED`, so the trigger is active in every target,
  including `dev`. Runs are queued, and alerts are suppressed for skipped and canceled runs.
- **Exactly once:** files that arrive during a run are picked up. Files already processed are never
  reprocessed, because the Auto Loader checkpoint tracks them.
- **Dev naming:** in the `dev` target, job and pipeline names get a `[dev <user>]` prefix. UC objects
  (external location, Volume, table) are not prefixed.
- **Existing UC objects:** if the external location or Volume already exists, bind it instead of
  recreating it:
  `databricks bundle deployment bind logs_location <name> -t <target>` (same for `application_logs`
  with the Volume's full name).
- **Changing the source path:** after pointing the pipeline at a different Volume, run a full refresh
  of the pipeline so the Auto Loader checkpoint starts clean.

## Troubleshooting

| Symptom | Likely cause |
| --- | --- |
| `the AWS IAM role in the Unity Catalog credential is not configured correctly` | Role or trust policy is wrong, or the role was deleted. The field-eng sandbox account deletes buckets and roles over time, so check they still exist in AWS. |
| `storage-credentials validate` results change between runs | IAM propagation. Wait a few minutes. |
| `s3:GetBucketNotification ... no identity-based policy allows` | `file_events_policy.json` isn't attached to the role |
| Job never triggers | Trigger paused, or file events not provisioned on the external location (`databricks external-locations get <name>` should show `enable_file_events: true` and a `managed_sqs` queue) |

## References

- [Auto Loader](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/)
- [Auto Loader file events](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/file-events-explained)
- [Auto Loader options](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/options)
- [File arrival triggers](https://docs.databricks.com/aws/en/jobs/file-arrival-triggers)
- [Declarative Automation Bundles](https://docs.databricks.com/dev-tools/bundles/)
