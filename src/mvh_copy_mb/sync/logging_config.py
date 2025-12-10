"""Logging configuration for synchronization events."""

import logging
import sys
from datetime import datetime
from typing import Dict, Any


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
    """Log a connection-related event.
    
    Args:
        logger: Logger instance
        connection_id: ID of the connection
        user_id: ID of the user
        event: Type of connection event
        message: Human-readable message
        **kwargs: Additional fields to include in log
    """
    extra = {
        'connection_id': connection_id,
        'user_id': user_id,
        'event_type': f"connection_{event}",
        'timestamp': datetime.now().isoformat(),
        **kwargs
    }
    
    logger.info(message, extra=extra)


def log_conflict_event(logger: logging.Logger, record_id: str, 
                      users: list, conflict_type: str, resolution: str, 
                      **kwargs) -> None:
    """Log a conflict resolution event.
    
    Args:
        logger: Logger instance
        record_id: ID of the record with conflict
        users: List of user IDs involved in conflict
        conflict_type: Type of conflict
        resolution: How the conflict was resolved
        **kwargs: Additional fields to include in log
    """
    extra = {
        'record_id': record_id,
        'event_type': 'conflict_resolution',
        'conflict_type': conflict_type,
        'resolution': resolution,
        'involved_users': ','.join(users),
        'timestamp': datetime.now().isoformat(),
        **kwargs
    }
    
    logger.warning(f"Conflict resolved: {resolution}", extra=extra)


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