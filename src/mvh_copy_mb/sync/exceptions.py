"""Custom exceptions for synchronization operations."""

from typing import Optional, Dict, Any
from datetime import datetime


class SyncError(Exception):
    """Base exception for synchronization errors."""
    
    def __init__(self, message: str, error_code: str = "sync_error", 
                 details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.details = details or {}
        self.timestamp = datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to dictionary for serialization."""
        return {
            "error_code": self.error_code,
            "message": self.message,
            "details": self.details,
            "timestamp": self.timestamp.isoformat()
        }


class VersionConflictError(SyncError):
    """Raised when a version conflict occurs during record update."""
    
    def __init__(self, record_id: str, expected_version: int, actual_version: int):
        message = (f"Version conflict for record {record_id}: "
                  f"expected version {expected_version}, got {actual_version}")
        details = {
            "record_id": record_id,
            "expected_version": expected_version,
            "actual_version": actual_version,
            "conflict_type": "version_mismatch"
        }
        super().__init__(message, "version_conflict", details)


class LockAcquisitionError(SyncError):
    """Raised when a lock cannot be acquired."""
    
    def __init__(self, record_id: str, user_id: str, held_by: Optional[str] = None):
        if held_by:
            message = f"Record {record_id} is locked by user {held_by}"
        else:
            message = f"Cannot acquire lock for record {record_id}"
        
        details = {
            "record_id": record_id,
            "requesting_user": user_id,
            "held_by": held_by,
            "lock_type": "optimistic"
        }
        super().__init__(message, "lock_acquisition_failed", details)


class ConnectionError(SyncError):
    """Raised when connection-related errors occur."""
    
    def __init__(self, connection_id: str, reason: str):
        message = f"Connection error for {connection_id}: {reason}"
        details = {
            "connection_id": connection_id,
            "reason": reason
        }
        super().__init__(message, "connection_error", details)


class BroadcastError(SyncError):
    """Raised when event broadcasting fails."""
    
    def __init__(self, event_type: str, failed_clients: int, total_clients: int, 
                 underlying_error: Optional[str] = None):
        message = f"Broadcast failed for {failed_clients}/{total_clients} clients"
        details = {
            "event_type": event_type,
            "failed_clients": failed_clients,
            "total_clients": total_clients,
            "underlying_error": underlying_error
        }
        super().__init__(message, "broadcast_failed", details)


class DataIntegrityError(SyncError):
    """Raised when data integrity issues are detected."""
    
    def __init__(self, record_id: str, integrity_check: str, details: Optional[Dict[str, Any]] = None):
        message = f"Data integrity violation for record {record_id}: {integrity_check}"
        error_details = {
            "record_id": record_id,
            "integrity_check": integrity_check,
            **(details or {})
        }
        super().__init__(message, "data_integrity_error", error_details)


class SyncServiceUnavailableError(SyncError):
    """Raised when sync service is unavailable."""
    
    def __init__(self, service_name: str, reason: str):
        message = f"Sync service {service_name} is unavailable: {reason}"
        details = {
            "service_name": service_name,
            "reason": reason
        }
        super().__init__(message, "service_unavailable", details)


class BulkOperationError(SyncError):
    """Raised when bulk operations encounter errors."""
    
    def __init__(self, total_operations: int, failed_operations: int, 
                 errors: Optional[list] = None):
        message = f"Bulk operation failed: {failed_operations}/{total_operations} operations failed"
        details = {
            "total_operations": total_operations,
            "failed_operations": failed_operations,
            "success_operations": total_operations - failed_operations,
            "errors": errors or []
        }
        super().__init__(message, "bulk_operation_failed", details)


class CircuitBreakerError(SyncError):
    """Raised when circuit breaker is open."""
    
    def __init__(self, service_name: str, failure_count: int, threshold: int):
        message = f"Circuit breaker open for {service_name}: {failure_count} failures (threshold: {threshold})"
        details = {
            "service_name": service_name,
            "failure_count": failure_count,
            "threshold": threshold,
            "circuit_state": "open"
        }
        super().__init__(message, "circuit_breaker_open", details)