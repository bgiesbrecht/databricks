# Structlog → Delta Table Logging

A Databricks notebook that shows how to send [structlog](https://www.structlog.org/) output to a Unity Catalog Delta table, so you can query application logs with SQL.

The notebook is `Structlog Table logging.py`. It's in Databricks source format, so it opens as a notebook when you import it into a workspace.

## How it works

1. **Installs structlog** with `pip install structlog`.
2. **Creates the log table** if it doesn't already exist (`CREATE TABLE IF NOT EXISTS ... USING DELTA`).
3. **Defines `DeltaTableProcessor`**, a custom structlog processor that:
   - pulls `level`, `event`, and `logger` out of each event dict,
   - serializes all other key/value pairs to a JSON string in `additional_fields`,
   - holds records in an in-memory buffer and appends them to the table in batches of `batch_size` (default 10),
   - passes the event dict on unchanged, so later processors still run (here, `JSONRenderer` prints each event to stdout).
4. **Configures structlog** with this processor chain:

   ```
   add_log_level → TimeStamper(iso) → StackInfoRenderer → format_exc_info → DeltaTableProcessor → JSONRenderer
   ```

5. **Writes three example log lines**, flushes the buffer, and shows the 10 most recent rows from the table.

## Table schema

| Column              | Type        | Description                                                        |
|---------------------|-------------|--------------------------------------------------------------------|
| `timestamp`         | `TIMESTAMP` | When the processor handled the event (`datetime.now()` on the driver) |
| `level`             | `STRING`    | Log level, uppercase (`INFO`, `WARNING`, `ERROR`, ...)              |
| `event`             | `STRING`    | The log message                                                    |
| `logger`            | `STRING`    | Logger name; `root` if none is bound                               |
| `additional_fields` | `STRING`    | JSON object holding all other key/value context, or `NULL` if there is none |

## Usage

1. Import the notebook into your Databricks workspace.
2. Change `LOG_TABLE_NAME` to a three-part name (`catalog.schema.table`) that you can write to:

   ```python
   LOG_TABLE_NAME = "my_catalog.my_schema.application_logs"
   ```

3. Attach the notebook to a cluster or serverless compute and run all cells.

To log from your own code, use the configured logger:

```python
log = structlog.get_logger()

log.info("Application started", user_id=123, action="login")
log.warning("High memory usage", memory_mb=1024, threshold_mb=800)
log.error("Failed to process record", record_id=456, error_type="ValidationError")

# Always flush at the end of the run, or buffered records are lost
delta_processor.flush()
```

Each call also prints a JSON line to stdout, for example:

```json
{"user_id": 123, "action": "login", "event": "Application started", "level": "info", "timestamp": "2026-10-19T18:30:00.000000Z"}
```

## Querying logs

```sql
-- Recent errors
SELECT timestamp, event, additional_fields
FROM my_catalog.my_schema.application_logs
WHERE level = 'ERROR'
ORDER BY timestamp DESC;

-- Pull a field out of the JSON context
SELECT timestamp, event, additional_fields:user_id AS user_id
FROM my_catalog.my_schema.application_logs
WHERE additional_fields:action = 'login';
```

## Configuration

| Setting          | Where                        | Default                         |
|------------------|------------------------------|---------------------------------|
| `LOG_TABLE_NAME` | top of the main cell         | `bg.admin.application_logs`     |
| `batch_size`     | `DeltaTableProcessor(...)`   | `10` records per write          |

## Caveats and limitations

- **Call `flush()` before the run ends.** Records still in the buffer are lost if the notebook or job ends first. Consider wrapping your job in `try/finally` and calling `delta_processor.flush()` in the `finally` block.
- **Failed writes drop records.** If the append fails, the processor prints the error and clears the buffer. It doesn't retry.
- **Values must be JSON-serializable.** `additional_fields` is built with `json.dumps`, which runs outside the `try` block. Passing a value like a `datetime`, `Decimal`, or a custom object raises an exception at the logging call. Convert such values to strings first, or change the call to `json.dumps(..., default=str)`.
- **The timestamp is the driver's clock.** The ISO timestamp from `TimeStamper` is left out of the table. The processor records `datetime.now()` instead, which is the driver's local time (normally UTC on Databricks).
- **Driver only.** The buffer and the Spark writes live on the driver. Don't call the logger from inside UDFs or executor code.
- **Small files.** Writing every 10 records creates many small Delta files. For heavy logging, raise `batch_size` and run `OPTIMIZE` on the table now and then.
- **Exceptions.** `log.exception(...)` or `exc_info=True` adds the formatted traceback to `additional_fields` under the `exception` key.

## Requirements

- Databricks Runtime or serverless compute with Unity Catalog
- `CREATE TABLE` and `MODIFY` permissions on the target schema and table
- `structlog` (installed by the notebook)
