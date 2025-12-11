"""Logging configuration for synchronization events."""

import logging
import sys
import time
from datetime import datetime
from typing import Dict, Any, Optional


class SyncEventFormatter(logging.Formatter):
    """Custom formatter for synchronization events."""
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record with sync-specific information."""
        # Add timestamp if not present
        if not hasattr(record, 'timestamp'):
            record.timestamp = datetime.now().isoformat()
        
        # Add sync-specific fields if present
        sync_fields = []
        for field in ['user_id', 'record_id', 'event_type', 'connection_id']:
            if hasattr(record, field):
                sync_fields.append(f"{field}={getattr(record, field)}")
        
        # Format the base message
        base_msg = super().format(record)
        
        # Add sync fields if any
        if sync_fields:
            return f"{base_msg} [{', '.join(sync_fields)}]"
        
        return base_msg


def setup_sync_logging(log_level: str = "INFO") -> logging.Logger:
    """Set up logging for synchronization components.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        
    Returns:
        Configured logger for sync operations
    """
    logger = logging.getLogger("mvh_copy_mb.sync")
    
    # Avoid duplicate handlers
    if logger.handlers:
        return logger
    
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Create console handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(getattr(logging, log_level.upper()))
    
    # Create formatter
    formatter = SyncEventFormatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    handler.setFormatter(formatter)
    
    logger.addHandler(handler)
    
    return logger


def log_sync_event(logger: logging.Logger, event_type: str, record_id: str, 
                  user_id: str, message: str, **kwargs) -> None:
    """Log a synchronization event with structured data.
    
    Args:
        logger: Logger instance
        event_type: Type of sync event
        record_id: ID of the record involved
        user_id: ID of the user who triggered the event
        message: Human-readable message
        **kwargs: Additional fields to include in log
    """
    extra = {
        'event_type': event_type,
        'record_id': record_id,
        'user_id': user_id,
        'timestamp': datetime.now().isoformat(),
        **kwargs
    }
    
    logger.info(message, extra=extra)


def log_connection_event(logger: logging.Logger, connection_id: str, 
                        user_id: str, event: str, message: str, **kwargs) -> None:
    """Log a connection-related event with diagnostic information.
    
    Args:
        logger: Logger instance
        connection_id: ID of the connection
        user_id: ID of the user
        event: Type of connection event
        message: Human-readable message
        **kwargs: Additional fields to include in log
    """
    # Calculate connection duration if provided
    connection_duration_ms = kwargs.get('connection_duration_ms')
    if 'connection_start' in kwargs and event in ['disconnected', 'timeout']:
        connection_duration_ms = (time.time() - kwargs['connection_start']) * 1000
    
    # Add performance metrics
    extra = {
        'connection_id': connection_id,
        'user_id': user_id,
        'event_type': f"connection_{event}",
        'timestamp': datetime.now().isoformat(),
        'connection_duration_ms': connection_duration_ms,
        **kwargs
    }
    
    # Log at appropriate level based on event type
    if event in ['connected', 'sync_completed']:
        logger.info(message, extra=extra)
    elif event in ['disconnected', 'sync_no_updates']:
        logger.info(message, extra=extra)
    elif event in ['timeout', 'error', 'sync_failed']:
        logger.warning(message, extra=extra)
    else:
        logger.debug(message, extra=extra)


def log_conflict_event(logger: logging.Logger, record_id: str, 
                      users: list, conflict_type: str, resolution: str, 
                      **kwargs) -> None:
    """Log a conflict resolution event with detailed diagnostic information.
    
    Args:
        logger: Logger instance
        record_id: ID of the record with conflict
        users: List of user IDs involved in conflict
        conflict_type: Type of conflict
        resolution: How the conflict was resolved
        **kwargs: Additional fields to include in log
    """
    # Calculate conflict resolution time if provided
    resolution_time_ms = kwargs.get('resolution_time_ms')
    if 'start_time' in kwargs and 'end_time' in kwargs:
        resolution_time_ms = (kwargs['end_time'] - kwargs['start_time']) * 1000
    
    extra = {
        'record_id': record_id,
        'event_type': 'conflict_resolution',
        'conflict_type': conflict_type,
        'resolution': resolution,
        'involved_users': ','.join(users),
        'user_count': len(users),
        'timestamp': datetime.now().isoformat(),
        'resolution_time_ms': resolution_time_ms,
        **kwargs
    }
    
    # Create detailed message including user information
    users_str = ','.join(users) if users else 'unknown'
    message = f"Conflict resolved: {resolution} for record {record_id} [users={users_str}, record_id={record_id}, event_type=conflict_resolution]"
    
    # Log at appropriate level based on conflict severity
    if conflict_type in ['version_conflict', 'concurrent_edit']:
        logger.warning(message, extra=extra)
    else:
        logger.error(f"Critical conflict resolved: {resolution} for record {record_id} [users={users_str}]", extra=extra)


def log_performance_metrics(logger: logging.Logger, operation: str, 
                          latency_ms: float, throughput_ops_per_sec: Optional[float] = None,
                          **kwargs) -> None:
    """Log performance metrics for sync operations.
    
    Args:
        logger: Logger instance
        operation: Name of the operation being measured
        latency_ms: Operation latency in milliseconds
        throughput_ops_per_sec: Operations per second (optional)
        **kwargs: Additional performance metrics
    """
    extra = {
        'event_type': 'performance_metrics',
        'operation': operation,
        'latency_ms': round(latency_ms, 2),
        'throughput_ops_per_sec': round(throughput_ops_per_sec, 2) if throughput_ops_per_sec else None,
        'timestamp': datetime.now().isoformat(),
        **kwargs
    }
    
    # Log performance warnings for slow operations
    if latency_ms > 1000:  # > 1 second
        logger.warning(f"Slow operation detected: {operation} took {latency_ms:.2f}ms", extra=extra)
    elif latency_ms > 500:  # > 500ms
        logger.info(f"Performance: {operation} took {latency_ms:.2f}ms", extra=extra)
    else:
        logger.debug(f"Performance: {operation} took {latency_ms:.2f}ms", extra=extra)


def log_batch_metrics(logger: logging.Logger, batch_size: int, 
                     processing_time_ms: float, success_count: int, 
                     failure_count: int, **kwargs) -> None:
    """Log metrics for batch operations.
    
    Args:
        logger: Logger instance
        batch_size: Total number of items in batch
        processing_time_ms: Time to process the batch
        success_count: Number of successful operations
        failure_count: Number of failed operations
        **kwargs: Additional batch metrics
    """
    throughput = (success_count / (processing_time_ms / 1000)) if processing_time_ms > 0 else 0
    success_rate = (success_count / batch_size) if batch_size > 0 else 0
    
    extra = {
        'event_type': 'batch_metrics',
        'batch_size': batch_size,
        'processing_time_ms': round(processing_time_ms, 2),
        'success_count': success_count,
        'failure_count': failure_count,
        'success_rate': round(success_rate, 3),
        'throughput_ops_per_sec': round(throughput, 2),
        'timestamp': datetime.now().isoformat(),
        **kwargs
    }
    
    if failure_count > 0:
        logger.warning(f"Batch processed with {failure_count} failures: {success_count}/{batch_size} succeeded", extra=extra)
    else:
        logger.info(f"Batch processed successfully: {batch_size} items in {processing_time_ms:.2f}ms", extra=extra)


class PerformanceTimer:
    """Context manager for measuring operation performance."""
    
    def __init__(self, logger: logging.Logger, operation: str, **kwargs):
        self.logger = logger
        self.operation = operation
        self.kwargs = kwargs
        self.start_time = None
        
    def __enter__(self):
        self.start_time = time.time()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.start_time:
            latency_ms = (time.time() - self.start_time) * 1000
            
            # Add error information if exception occurred
            if exc_type:
                self.kwargs['error'] = str(exc_val)
                self.kwargs['error_type'] = exc_type.__name__
                
            log_performance_metrics(
                self.logger, self.operation, latency_ms, **self.kwargs
            )


def get_logger(name: str, log_level: str = "INFO") -> logging.Logger:
    """Get a configured logger for sync components.
    
    Args:
        name: Logger name (usually __name__)
        log_level: Logging level
        
    Returns:
        Configured logger instance
    """
    # Set up sync logging if not already done
    setup_sync_logging(log_level)
    
    # Return logger for the specific component
    return logging.getLogger(name)