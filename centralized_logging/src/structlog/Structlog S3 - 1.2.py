# Databricks notebook source
!pip install structlog

# COMMAND ----------

import structlog

main_keys = {"timestamp", "level", "event", "logger", "application"}

def event_organizer(logger, method_name, event_dict):
    """
    Sample processor function to organize log event into a specific format.
    Note that we take in event_dict, create log_entry, and return log_entry
      as the new event_dict which will be consumed downstream.
    """
    log_entry = {
            "timestamp": event_dict.get("timestamp", None),
            "level": event_dict.get("level", method_name).upper(),
            "event": event_dict.get("event", ""),
            "logger": event_dict.get("logger", "root"),
            "application": event_dict.get("application"),
            "details": {}
        }

    for key in event_dict:
        if key not in main_keys:
            log_entry['details'][key] = event_dict[key]

    return log_entry

# COMMAND ----------

from datetime import datetime
import json
import boto3
from io import StringIO
import uuid


class S3JSONProcessor:
    def __init__(self, bucket, prefix, application_name, batch_size=10):
        """
        Write JSON logs to S3 in batches.
        
        Args:
            bucket: S3 bucket name
            prefix: S3 prefix/folder path
            batch_size: Number of logs to buffer before writing
            application_name: Optional application identifier

        Set AWS credentials as environment variables or change this code 
        to pass them in. Prefer secrets to manage credentials.
        """
        self.bucket = bucket
        self.prefix = prefix.rstrip('/')
        self.application_name = application_name
        self.batch_size = batch_size
        self.log_buffer = []
        self.s3_client = boto3.client(
            's3', 
            # aws_access_key_id=aws_access_key,     # env AWS_ACCESS_KEY_ID
            # aws_secret_access_key=aws_secret_key  # env AWS_SECRET_ACCESS_KEY
        )
    
    def __call__(self, logger, method_name, event_dict):
        """Process log entry and buffer for S3 write"""
        
        # Add to buffer
        self.log_buffer.append(event_dict)
        
        # Write to S3 when buffer reaches batch_size
        if len(self.log_buffer) >= self.batch_size:
            self._flush_logs()
        
        return event_dict
    
    def _flush_logs(self):
        """Write buffered logs to S3 as JSONL file"""
        if not self.log_buffer:
            return
        
        try:
            # Create JSONL content (one JSON object per line)
            jsonl_content = '\n'.join(json.dumps(log) for log in self.log_buffer)
            
            # Generate unique filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            batch_id = str(uuid.uuid4())[:8]
            filename = f"{self.prefix}/{timestamp}_{batch_id}.jsonl"
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=filename,
                Body=jsonl_content,
                ContentType='application/x-ndjson'
            )
            
            print(f"Wrote {len(self.log_buffer)} logs to s3://{self.bucket}/{filename}")
            
            # Clear buffer
            self.log_buffer = []
            
        except Exception as e:
            print(f"Error writing logs to S3: {e}")
            self.log_buffer = []
    
    def flush(self):
        """Manually flush remaining logs"""
        self._flush_logs()

# COMMAND ----------

from functools import wraps

def log_cfg_s3(s3_bucket, s3_prefix, application, batch_size=100):
    """Decorator that sets up logging and ensures flush at the end"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Setup
            s3_processor = S3JSONProcessor(
                bucket=s3_bucket,
                prefix=s3_prefix,
                application_name=application,
                batch_size=batch_size
            )

            # Configure structlog
            structlog.configure(
                processors=[
                    structlog.stdlib.add_log_level,
                    structlog.processors.TimeStamper(fmt="iso"),
                    structlog.processors.StackInfoRenderer(),
                    structlog.processors.format_exc_info,
                    event_organizer,
                    s3_processor,
                    structlog.processors.JSONRenderer(),
                ],
                wrapper_class=structlog.stdlib.BoundLogger,
                context_class=dict,
                logger_factory=structlog.PrintLoggerFactory(),
                cache_logger_on_first_use=True,
            )
            
            try:
                # Run the function
                result = func(*args, **kwargs)
                return result
            finally:
                # Always flush
                print(f"Flushing {len(s3_processor.log_buffer)} remaining logs...")
                s3_processor.flush()
        
        return wrapper
    return decorator

# COMMAND ----------

import os

os.environ['AWS_ACCESS_KEY_ID'] = dbutils.secrets.get(scope="<SCOPE>", key="aws-access-key-id")
os.environ['AWS_SECRET_ACCESS_KEY'] = dbutils.secrets.get(scope="<SCOPE>", key="aws-secret-access-key")


# Usage
@log_cfg_s3(
    s3_bucket="<BUCKET>",
    s3_prefix="logs/application_logs",
    application='decorated'
)
def main():
    log = structlog.get_logger()
    
    for i in range(1000):
        log.info("Processing record", record_id=i)
        log.info("Application started", user_id=123, action="login")
        log.warning("High memory usage", memory_mb=1024, threshold_mb=800)
        log.error("Failed to process record", record_id=456, error_type="ValidationError")
    
    log.info("Processing record AFTER", record_id=i)
    log.info("Application started AFTER", user_id=123, action="login")
    log.warning("High memory usage AFTER", memory_mb=1024, threshold_mb=800)
    log.error("Failed to process record AFTER", record_id=456, error_type="ValidationError")

# Run it
main()