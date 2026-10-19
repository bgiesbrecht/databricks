# Databricks notebook source
pip install structlog

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
import uuid
import json


class VolumeProcessor:
    def __init__(self, volume, application, batch_size=100):
        """
        Write JSON logs to UC Volume in batches.
        
        Args:
            volume: Fully qualified volume path
            batch_size: Number of logs to buffer before writing
        """
        self.volume = volume
        self.application = application
        self.batch_size = batch_size
        self.log_buffer = []

        if self.volume.endswith('/'):
            self.volume = self.volume[:-1]
    
    def __call__(self, logger, method_name, event_dict):
        """Process log entry and buffer"""
        event_dict['application'] = self.application
        
        # Add to buffer
        self.log_buffer.append(event_dict)
        
        # Write when buffer reaches batch_size
        if len(self.log_buffer) >= self.batch_size:
            self._flush_logs()
        
        return event_dict
    
    def _flush_logs(self):
        """Write buffered logs to volume as JSONL file"""
        if not self.log_buffer:
            return
        
        try:
            # Create JSONL content (one JSON object per line)
            jsonl_content = '\n'.join(json.dumps(log) for log in self.log_buffer)
            
            # Generate unique filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            batch_id = str(uuid.uuid4())[:8]
            filename = f"{timestamp}_{batch_id}.jsonl"
            
            # Write to volume
            with open(f"{self.volume}/{filename}", "w") as f:
              f.write(jsonl_content)
            
            print(f"Wrote {len(self.log_buffer)} logs to {self.volume}/{filename}")
            
            # Clear buffer
            self.log_buffer = []
            
        except Exception as e:
            print(f"Error writing logs: {e}")
            self.log_buffer = []
    
    def flush(self):
        """Manually flush remaining logs"""
        self._flush_logs()

# COMMAND ----------

from functools import wraps

def log_cfg_volume(volume, application, batch_size=100):
    """Decorator that sets up logging and ensures flush at the end"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Setup
            vol_processor = VolumeProcessor(
                volume=volume,
                application=application,
                batch_size=batch_size
            )
            
            structlog.configure(
                processors=[
                    structlog.stdlib.add_log_level,
                    structlog.processors.TimeStamper(fmt="iso"),
                    structlog.processors.StackInfoRenderer(),
                    structlog.processors.format_exc_info,
                    event_organizer,
                    vol_processor,
                    structlog.processors.JSONRenderer(),
                ],
                wrapper_class=structlog.stdlib.BoundLogger,
                context_class=dict,
                logger_factory=structlog.PrintLoggerFactory(),
                cache_logger_on_first_use=True,
            )
            
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                # Always flush
                print(f"Flushing {len(vol_processor.log_buffer)} remaining logs...")
                vol_processor.flush()
        
        return wrapper
    return decorator

# COMMAND ----------

@log_cfg_volume(
    volume="/Volumes/<CATALOG>/<SCHEMA>/<VOLUME>",
    application='gw1',
    batch_size=100
)
def main():
    log = structlog.get_logger()
    
    for i in range(5000):
        log.info("Processing record", record_id=i)
        log.info("Application started", user_id=123, action="login")
        log.warning("High memory usage", memory_mb=1024, threshold_mb=800)
        log.error("Failed to process record", record_id=i, error_type="ValidationError")
    
    log.info("Processing record AFTER", record_id=i)
    log.info("Application started AFTER", user_id=123, action="login")
    log.warning("High memory usage AFTER", memory_mb=1024, threshold_mb=800)
    log.error("Failed to process record AFTER", record_id=456, error_type="ValidationError")

main()