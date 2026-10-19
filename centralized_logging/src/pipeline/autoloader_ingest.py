from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp

# Set via the pipeline `configuration` block in resources/logging_ingest.pipeline.yml
logs_path = spark.conf.get("logs_path")
clean_source = spark.conf.get("clean_source", "OFF").upper()
clean_source_retention = spark.conf.get("clean_source_retention", "30 days")
clean_source_move_destination = spark.conf.get("clean_source_move_destination", "")


@dp.table(table_properties={'quality': 'bronze'})
def centralized_logs():
  reader = (
     spark.readStream.format('cloudFiles')
     .option('cloudFiles.format', 'json')
     .option("cloudFiles.inferColumnTypes", "true")
     .option("cloudFiles.schemaHints", "timestamp TIMESTAMP")
  )

  # Optional handling of files already processed by Auto Loader
  if clean_source in ("DELETE", "MOVE"):
    reader = (
       reader.option("cloudFiles.cleanSource", clean_source)
       .option("cloudFiles.cleanSource.retentionDuration", clean_source_retention)
    )
    if clean_source == "MOVE":
      reader = reader.option("cloudFiles.cleanSource.moveDestination", clean_source_move_destination)

  return (
     reader.load(logs_path)
     .withColumn("loaded_timestamp", current_timestamp())
  )
