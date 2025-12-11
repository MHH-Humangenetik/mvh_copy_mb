"""Audit trail manager for integrating audit logging with sync operations."""

import logging
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, List, Union
import uuid

from .audit import (
    AuditDatabase, AuditLogger, AuditEvent, AuditEventType, 
    AuditSeverity
)

logger = logging.getLogger(__name__)


class AuditTrailManager:
    """Manages comprehensive audit trail for multi-user synchronization."""
    
    def __init__(self, audit_db_path: Path):
        """Initialize audit trail manager.
        
        Args:
            audit_db_path: Path to the audit database file
        """
        self.audit_db_path = audit_db_path
        self.audit_db: Optional[AuditDatabase] = None
        self.audit_logger: Optional[AuditLogger] = None
        self._active_sessions: Dict[str, Dict[str, Any]] = {}
        self._active_operations: Dict[str, Dict[str, Any]] = {}
    
    def __enter__(self) -> 'AuditTrailManager':
        """Context manager entry: initialize audit database and logger."""
        try:
            self.audit_db = AuditDatabase(self.audit_db_path)
            self.audit_db.__enter__()
            self.audit_logger = AuditLogger(self.audit_db)
            logger.info("Audit trail manager initialized")
            return self
        except Exception as e:
            logger.error(f"Failed to initialize audit trail manager: {e}", exc_info=True)
            raise
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit: cleanup audit database."""
        try:
            if self.audit_db:
                self.audit_db.__exit__(exc_type, exc_val, exc_tb)
        except Exception as e:
            logger.warning(f"Error while closing audit trail manager: {e}", exc_info=True)
    
    def start_user_session(
        self,
        user_id: str,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None
    ) -> str:
        """Start a new user session and log the event.
        
        Args:
            user_id: ID of the user starting the session
            ip_address: User's IP address
            user_agent: User's browser/client info
            
        Returns:
            Session ID for tracking
        """
        session_id = str(uuid.uuid4())
        session_start = datetime.now()
        
        self._active_sessions[session_id] = {
            "user_id": user_id,
            "start_time": session_start,
            "ip_address": ip_address,
            "user_agent": user_agent,
            "last_activity": session_start
        }
        
        if self.audit_logger:
            self.audit_logger.log_user_action(
                user_id=user_id,
                action=f"User session started",
                event_type=AuditEventType.USER_SESSION_START,
                session_id=session_id,
                ip_address=ip_address,
                user_agent=user_agent,
                details={
                    "session_start": session_start.isoformat()
                }
            )
        
        logger.info(f"User session started: {session_id} for user {user_id}")
        return session_id
    
    def end_user_session(self, session_id: str) -> None:
        """End a user session and log the event.
        
        Args:
            session_id: ID of the session to end
        """
        if session_id not in self._active_sessions:
            logger.warning(f"Attempted to end unknown session: {session_id}")
            return
        
        session_info = self._active_sessions[session_id]
        session_end = datetime.now()
        duration_ms = (session_end - session_info["start_time"]).total_seconds() * 1000
        
        if self.audit_logger:
            self.audit_logger.log_user_action(
                user_id=session_info["user_id"],
                action=f"User session ended",
                event_type=AuditEventType.USER_SESSION_END,
                session_id=session_id,
                ip_address=session_info.get("ip_address"),
                user_agent=session_info.get("user_agent"),
                duration_ms=duration_ms,
                details={
                    "session_duration_ms": duration_ms,
                    "session_end": session_end.isoformat()
                }
            )
        
        del self._active_sessions[session_id]
        logger.info(f"User session ended: {session_id} (duration: {duration_ms:.2f}ms)")
    
    def log_connection_event(
        self,
        user_id: str,
        connection_id: str,
        event_type: AuditEventType,
        session_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        success: bool = True,
        error_message: Optional[str] = None
    ) -> None:
        """Log a WebSocket connection event.
        
        Args:
            user_id: ID of the user
            connection_id: WebSocket connection ID
            event_type: Type of connection event
            session_id: User session ID
            details: Additional connection details
            success: Whether the connection event succeeded
            error_message: Error message if event failed
        """
        if self.audit_logger:
            self.audit_logger.log_sync_event(
                user_id=user_id,
                action=f"WebSocket {event_type.value}",
                event_type=event_type,
                connection_id=connection_id,
                details={
                    "session_id": session_id,
                    **(details or {})
                },
                success=success,
                error_message=error_message
            )
        
        # Update session activity
        if session_id and session_id in self._active_sessions:
            self._active_sessions[session_id]["last_activity"] = datetime.now()
    
    def log_record_view(
        self,
        user_id: str,
        record_id: str,
        session_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ) -> None:
        """Log when a user views a record.
        
        Args:
            user_id: ID of the user viewing the record
            record_id: ID of the record being viewed
            session_id: User session ID
            details: Additional view details
        """
        if self.audit_logger:
            self.audit_logger.log_record_operation(
                user_id=user_id,
                record_id=record_id,
                action=f"Record viewed: {record_id}",
                event_type=AuditEventType.RECORD_VIEW,
                session_id=session_id,
                details=details or {}
            )
        
        # Update session activity
        if session_id and session_id in self._active_sessions:
            self._active_sessions[session_id]["last_activity"] = datetime.now()
    
    @contextmanager
    def track_record_edit(
        self,
        user_id: str,
        record_id: str,
        session_id: Optional[str] = None,
        before_state: Optional[Dict[str, Any]] = None
    ):
        """Context manager to track a record edit operation.
        
        Args:
            user_id: ID of the user editing the record
            record_id: ID of the record being edited
            session_id: User session ID
            before_state: Record state before editing
            
        Yields:
            Operation tracking context
        """
        operation_id = str(uuid.uuid4())
        start_time = time.time()
        
        # Log edit start
        if self.audit_logger:
            self.audit_logger.log_record_operation(
                user_id=user_id,
                record_id=record_id,
                action=f"Record edit started: {record_id}",
                event_type=AuditEventType.RECORD_EDIT_START,
                session_id=session_id,
                before_state=before_state,
                details={"operation_id": operation_id}
            )
        
        self._active_operations[operation_id] = {
            "user_id": user_id,
            "record_id": record_id,
            "session_id": session_id,
            "start_time": start_time,
            "before_state": before_state
        }
        
        try:
            yield operation_id
            
            # Log successful completion
            duration_ms = (time.time() - start_time) * 1000
            if self.audit_logger:
                self.audit_logger.log_record_operation(
                    user_id=user_id,
                    record_id=record_id,
                    action=f"Record edit completed: {record_id}",
                    event_type=AuditEventType.RECORD_EDIT_COMPLETE,
                    session_id=session_id,
                    before_state=before_state,
                    duration_ms=duration_ms,
                    details={"operation_id": operation_id}
                )
            
        except Exception as e:
            # Log edit failure
            duration_ms = (time.time() - start_time) * 1000
            if self.audit_logger:
                self.audit_logger.log_record_operation(
                    user_id=user_id,
                    record_id=record_id,
                    action=f"Record edit failed: {record_id}",
                    event_type=AuditEventType.RECORD_EDIT_COMPLETE,
                    session_id=session_id,
                    before_state=before_state,
                    duration_ms=duration_ms,
                    success=False,
                    error_message=str(e),
                    details={"operation_id": operation_id}
                )
            raise
        finally:
            if operation_id in self._active_operations:
                del self._active_operations[operation_id]
            
            # Update session activity
            if session_id and session_id in self._active_sessions:
                self._active_sessions[session_id]["last_activity"] = datetime.now()
    
    def log_record_status_change(
        self,
        user_id: str,
        record_id: str,
        before_status: bool,
        after_status: bool,
        session_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ) -> None:
        """Log when a record's status changes.
        
        Args:
            user_id: ID of the user changing the status
            record_id: ID of the record
            before_status: Status before change
            after_status: Status after change
            session_id: User session ID
            details: Additional change details
        """
        if self.audit_logger:
            self.audit_logger.log_record_operation(
                user_id=user_id,
                record_id=record_id,
                action=f"Record status changed: {record_id} from {before_status} to {after_status}",
                event_type=AuditEventType.RECORD_STATUS_CHANGE,
                session_id=session_id,
                before_state={"is_done": before_status},
                after_state={"is_done": after_status},
                details=details or {}
            )
        
        # Update session activity
        if session_id and session_id in self._active_sessions:
            self._active_sessions[session_id]["last_activity"] = datetime.now()
    
    def log_lock_operation(
        self,
        user_id: str,
        record_id: str,
        operation: str,  # "acquire", "release", "timeout"
        session_id: Optional[str] = None,
        lock_version: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
        success: bool = True,
        error_message: Optional[str] = None
    ) -> None:
        """Log a record lock operation.
        
        Args:
            user_id: ID of the user performing lock operation
            record_id: ID of the record being locked
            operation: Type of lock operation
            session_id: User session ID
            lock_version: Version of the lock
            details: Additional lock details
            success: Whether the lock operation succeeded
            error_message: Error message if operation failed
        """
        event_type_map = {
            "acquire": AuditEventType.RECORD_LOCK_ACQUIRE,
            "release": AuditEventType.RECORD_LOCK_RELEASE,
            "timeout": AuditEventType.RECORD_LOCK_TIMEOUT
        }
        
        event_type = event_type_map.get(operation, AuditEventType.RECORD_LOCK_ACQUIRE)
        
        if self.audit_logger:
            self.audit_logger.log_record_operation(
                user_id=user_id,
                record_id=record_id,
                action=f"Record lock {operation}: {record_id}",
                event_type=event_type,
                session_id=session_id,
                details={
                    "lock_version": lock_version,
                    **(details or {})
                },
                success=success,
                error_message=error_message
            )
        
        # Update session activity
        if session_id and session_id in self._active_sessions:
            self._active_sessions[session_id]["last_activity"] = datetime.now()
    
    def log_sync_conflict(
        self,
        record_id: str,
        involved_users: List[str],
        conflict_type: str,
        resolution: str,
        details: Optional[Dict[str, Any]] = None,
        resolution_time_ms: Optional[float] = None
    ) -> None:
        """Log a synchronization conflict and its resolution.
        
        Args:
            record_id: ID of the record with conflict
            involved_users: List of user IDs involved in conflict
            conflict_type: Type of conflict
            resolution: How the conflict was resolved
            details: Additional conflict details
            resolution_time_ms: Time taken to resolve conflict
        """
        if self.audit_logger:
            # Log conflict detection
            self.audit_logger.log_sync_event(
                user_id=",".join(involved_users),
                action=f"Sync conflict detected: {conflict_type} on record {record_id}",
                event_type=AuditEventType.SYNC_CONFLICT_DETECTED,
                record_id=record_id,
                details={
                    "conflict_type": conflict_type,
                    "involved_users": involved_users,
                    "user_count": len(involved_users),
                    **(details or {})
                }
            )
            
            # Log conflict resolution
            self.audit_logger.log_sync_event(
                user_id=",".join(involved_users),
                action=f"Sync conflict resolved: {resolution} for record {record_id}",
                event_type=AuditEventType.SYNC_CONFLICT_RESOLVED,
                record_id=record_id,
                duration_ms=resolution_time_ms,
                details={
                    "conflict_type": conflict_type,
                    "resolution": resolution,
                    "involved_users": involved_users,
                    "user_count": len(involved_users),
                    **(details or {})
                }
            )
    
    def log_bulk_operation(
        self,
        user_id: str,
        operation_type: str,
        record_count: int,
        session_id: Optional[str] = None,
        success_count: Optional[int] = None,
        failure_count: Optional[int] = None,
        duration_ms: Optional[float] = None,
        details: Optional[Dict[str, Any]] = None
    ) -> None:
        """Log a bulk operation (e.g., CSV upload, bulk status change).
        
        Args:
            user_id: ID of the user performing bulk operation
            operation_type: Type of bulk operation
            record_count: Total number of records processed
            session_id: User session ID
            success_count: Number of successful operations
            failure_count: Number of failed operations
            duration_ms: Operation duration in milliseconds
            details: Additional operation details
        """
        event_type_map = {
            "csv_upload": AuditEventType.CSV_UPLOAD,
            "bulk_status_change": AuditEventType.BULK_OPERATION,
            "data_export": AuditEventType.DATA_EXPORT
        }
        
        event_type = event_type_map.get(operation_type, AuditEventType.BULK_OPERATION)
        
        if self.audit_logger:
            self.audit_logger.log_user_action(
                user_id=user_id,
                action=f"Bulk operation: {operation_type} ({record_count} records)",
                event_type=event_type,
                session_id=session_id,
                duration_ms=duration_ms,
                success=failure_count == 0 if failure_count is not None else True,
                details={
                    "operation_type": operation_type,
                    "record_count": record_count,
                    "success_count": success_count,
                    "failure_count": failure_count,
                    "success_rate": success_count / record_count if success_count and record_count else None,
                    **(details or {})
                }
            )
        
        # Update session activity
        if session_id and session_id in self._active_sessions:
            self._active_sessions[session_id]["last_activity"] = datetime.now()
    
    def log_system_error(
        self,
        error_message: str,
        error_type: str,
        details: Optional[Dict[str, Any]] = None,
        severity: AuditSeverity = AuditSeverity.ERROR
    ) -> None:
        """Log a system error event.
        
        Args:
            error_message: Error message
            error_type: Type of error
            details: Additional error details
            severity: Error severity level
        """
        if self.audit_logger:
            self.audit_logger.log_system_event(
                action=f"System error: {error_type}",
                event_type=AuditEventType.SYSTEM_ERROR,
                severity=severity,
                error_message=error_message,
                details={
                    "error_type": error_type,
                    **(details or {})
                }
            )
    
    def generate_audit_report(
        self,
        start_time: datetime,
        end_time: datetime,
        group_by: str = "user_id"
    ) -> Dict[str, Any]:
        """Generate an audit report for the specified time period.
        
        Args:
            start_time: Report start time
            end_time: Report end time
            group_by: Field to group by
            
        Returns:
            Dictionary containing report data
        """
        if not self.audit_db:
            raise RuntimeError("Audit database not initialized")
        
        return self.audit_db.generate_audit_report(start_time, end_time, group_by)
    
    def query_audit_events(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        user_id: Optional[str] = None,
        event_types: Optional[List[AuditEventType]] = None,
        record_id: Optional[str] = None,
        session_id: Optional[str] = None,
        limit: int = 1000,
        offset: int = 0
    ) -> List[AuditEvent]:
        """Query audit events with filtering options.
        
        Args:
            start_time: Filter events after this time
            end_time: Filter events before this time
            user_id: Filter by user ID
            event_types: Filter by event types
            record_id: Filter by record ID
            session_id: Filter by session ID
            limit: Maximum number of results
            offset: Number of results to skip
            
        Returns:
            List of matching audit events
        """
        if not self.audit_db:
            raise RuntimeError("Audit database not initialized")
        
        return self.audit_db.query_audit_events(
            start_time=start_time,
            end_time=end_time,
            user_id=user_id,
            event_types=event_types,
            record_id=record_id,
            session_id=session_id,
            limit=limit,
            offset=offset
        )
    
    def cleanup_old_audit_events(self, dry_run: bool = True) -> Dict[str, int]:
        """Clean up old audit events based on retention policies.
        
        Args:
            dry_run: If True, only count events that would be deleted
            
        Returns:
            Dictionary with cleanup statistics
        """
        if not self.audit_db:
            raise RuntimeError("Audit database not initialized")
        
        return self.audit_db.cleanup_old_events(dry_run=dry_run)
    
    def get_active_sessions(self) -> Dict[str, Dict[str, Any]]:
        """Get information about currently active user sessions.
        
        Returns:
            Dictionary of active sessions
        """
        return self._active_sessions.copy()
    
    def get_session_activity_report(self, hours: int = 24) -> Dict[str, Any]:
        """Generate a report of session activity for the last N hours.
        
        Args:
            hours: Number of hours to look back
            
        Returns:
            Session activity report
        """
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        if not self.audit_db:
            return {"error": "Audit database not initialized"}
        
        # Query session events
        session_events = self.query_audit_events(
            start_time=cutoff_time,
            event_types=[
                AuditEventType.USER_SESSION_START,
                AuditEventType.USER_SESSION_END,
                AuditEventType.CONNECTION_ESTABLISHED,
                AuditEventType.CONNECTION_LOST
            ],
            limit=10000
        )
        
        # Analyze session data
        unique_users = set()
        session_durations = []
        connection_events = []
        
        for event in session_events:
            unique_users.add(event.user_id)
            
            if event.event_type == AuditEventType.USER_SESSION_END and event.duration_ms:
                session_durations.append(event.duration_ms)
            
            if event.event_type in [AuditEventType.CONNECTION_ESTABLISHED, AuditEventType.CONNECTION_LOST]:
                connection_events.append(event)
        
        avg_session_duration = sum(session_durations) / len(session_durations) if session_durations else 0
        
        return {
            "period_hours": hours,
            "unique_users": len(unique_users),
            "total_sessions": len([e for e in session_events if e.event_type == AuditEventType.USER_SESSION_START]),
            "active_sessions": len(self._active_sessions),
            "avg_session_duration_ms": avg_session_duration,
            "connection_events": len(connection_events),
            "session_events": len(session_events)
        }