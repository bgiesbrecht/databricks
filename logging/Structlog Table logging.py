# Databricks notebook source
!pip install structlog

# COMMAND ----------

import structlog
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import json

spark = SparkSession.builder.getOrCreate()

# Define your log table schema
log_schema = StructType([
    StructField("timestamp", TimestampType(), False),
    StructField("level", StringType(), False),
    StructField("event", StringType(), False),
    StructField("logger", StringType(), True),
    StructField("additional_fields", StringType(), True)
])

# Define your table name (update with your catalog and schema)
LOG_TABLE_NAME = "bg.admin.application_logs"

# Create the table if it doesn't exist
def create_log_table_if_not_exists():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {LOG_TABLE_NAME} (
            timestamp TIMESTAMP,
            level STRING,
            event STRING,
            logger STRING,
            additional_fields STRING
        )
        USING DELTA
    """)

create_log_table_if_not_exists()

# Custom processor to write to Delta table
class DeltaTableProcessor:
    def __init__(self, table_name, batch_size=10):
        self.table_name = table_name
        self.batch_size = batch_size
        self.log_buffer = []
    
    def __call__(self, logger, method_name, event_dict):
        """Process log entry and write to Delta table"""
        # Extract standard fields
        timestamp = datetime.now()
        level = event_dict.get("level", method_name).upper()
        event = event_dict.get("event", "")
        logger_name = event_dict.get("logger", "root")
        
        # Collect additional fields (exclude standard ones)
        excluded_keys = {"timestamp", "level", "event", "logger"}
        additional_fields = {k: v for k, v in event_dict.items() if k not in excluded_keys}
        
        # Create log record
        log_record = (
            timestamp,
            level,
            event,
            logger_name,
            json.dumps(additional_fields) if additional_fields else None
        )
        
        self.log_buffer.append(log_record)
        
        # Write to table when buffer reaches batch_size
        if len(self.log_buffer) >= self.batch_size:
            self._flush_logs()
        
        return event_dict
    
    def _flush_logs(self):
        """Write buffered logs to Delta table"""
        if not self.log_buffer:
            return
        
        try:
            # Create DataFrame from buffer
            df = spark.createDataFrame(self.log_buffer, schema=log_schema)
            
            # Append to Delta table
            df.write.format("delta").mode("append").saveAsTable(self.table_name)
            
            # Clear buffer
            self.log_buffer = []
        except Exception as e:
            print(f"Error writing logs to table: {e}")
            self.log_buffer = []
    
    def flush(self):
        """Manually flush remaining logs"""
        self._flush_logs()

# Initialize the Delta table processor
delta_processor = DeltaTableProcessor(LOG_TABLE_NAME, batch_size=10)

# Configure structlog with the Delta table processor
structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        delta_processor, 
        structlog.processors.JSONRenderer(),  
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=True,
)

# Create logger instance
log = structlog.get_logger()

# Example usage
log.info("Application started", user_id=123, action="login")
log.warning("High memory usage", memory_mb=1024, threshold_mb=800)
log.error("Failed to process record", record_id=456, error_type="ValidationError")

# Important: Flush any remaining logs at the end
delta_processor.flush()

# Query your logs
display(spark.sql(f"SELECT * FROM {LOG_TABLE_NAME} ORDER BY timestamp DESC LIMIT 10"))

