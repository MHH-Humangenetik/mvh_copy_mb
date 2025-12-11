"""Comprehensive audit trail system for multi-user synchronization."""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
import json
import duckdb

logger = logging.getLogger(__name__)


class AuditEventType(Enum):
    """Types of audit events."""
    # User actions
    USER_LOGIN = "user_login"
    USER_LOGOUT = "user_logout"
    USER_SESSION_START = "user_session_start"
    USER_SESSION_END = "user_session_end"
    
    # Record operations
    RECORD_VIEW = "record_view"
    RECORD_EDIT_START = "record_edit_start"
    RECORD_EDIT_COMPLETE = "record_edit_complete"
    RECORD_EDIT_CANCEL = "record_edit_cancel"
    RECORD_STATUS_CHANGE = "record_status_change"
    RECORD_LOCK_ACQUIRE = "record_lock_acquire"
    RECORD_LOCK_RELEASE = "record_lock_release"
    RECORD_LOCK_TIMEOUT = "record_lock_timeout"
    
    # Synchronization events
    SYNC_EVENT_BROADCAST = "sync_event_broadcast"
    SYNC_EVENT_RECEIVED = "sync_event_received"
    SYNC_CONFLICT_DETECTED = "sync_conflict_detected"
    SYNC_CONFLICT_RESOLVED = "sync_conflict_resolved"
    
    # Connection events
    CONNECTION_ESTABLISHED = "connection_established"
    CONNECTION_LOST = "connection_lost"
    CONNECTION_RECONNECTED = "connection_reconnected"
    CONNECTION_TIMEOUT = "connection_timeout"
    
    # System events
    SYSTEM_ERROR = "system_error"
    SYSTEM_WARNING = "system_warning"
    BULK_OPERATION = "bulk_operation"
    CSV_UPLOAD = "csv_upload"
    DATA_EXPORT = "data_export"


class AuditSeverity(Enum):
    """Severity levels for audit events."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class AuditEvent:
    """Represents a comprehensive audit event."""
    event_id: str
    event_type: AuditEventType
    severity: AuditSeverity
    timestamp: datetime
    user_id: str
    session_id: Optional[str]
    connection_id: Optional[str]
    record_id: Optional[str]
    action: str
    details: Dict[str, Any]
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    duration_ms: Optional[float] = None
    success: bool = True
    error_message: Optional[str] = None
    before_state: Optional[Dict[str, Any]] = None
    after_state: Optional[Dict[str, Any]] = None


class AuditDatabase:
    """Manages audit trail database operations."""
    
    def __init__(self, db_path: Path):
        """Initialize audit database connection.
        
        Args:
            db_path: Path to the audit database file
        """
        self.db_path = db_path
        self.conn: Optional[duckdb.DuckDBPyConnection] = None
    
    def __enter__(self) -> 'AuditDatabase':
        """Context manager entry: open database connection and create schema."""
        try:
            self.conn = duckdb.connect(str(self.db_path))
            logger.info(f"Connected to audit database at {self.db_path}")
            self._create_schema()
            return self
        except Exception as e:
            logger.error(f"Failed to initialize audit database at {self.db_path}: {e}", exc_info=True)
            raise
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit: close database connection."""
        try:
            self.close()
        except Exception as e:
            logger.warning(f"Error while closing audit database connection: {e}", exc_info=True)
    
    def _create_schema(self) -> None:
        """Create the audit database schema if it doesn't exist."""
        if self.conn is None:
            raise RuntimeError("Audit database connection not established")
        
        try:
            # Main audit events table
            create_audit_table_sql = """
            CREATE TABLE IF NOT EXISTS audit_events (
                event_id VARCHAR NOT NULL PRIMARY KEY,
                event_type VARCHAR NOT NULL,
                severity VARCHAR NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                user_id VARCHAR NOT NULL,
                session_id VARCHAR,
                connection_id VARCHAR,
                record_id VARCHAR,
                action VARCHAR NOT NULL,
                details JSON,
                ip_address VARCHAR,
                user_agent VARCHAR,
                duration_ms DOUBLE,
                success BOOLEAN NOT NULL DEFAULT TRUE,
                error_message VARCHAR,
                before_state JSON,
                after_state JSON
            )
            """
            
            # Index for performance
            create_indexes_sql = [
                "CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_events(timestamp)",
                "CREATE INDEX IF NOT EXISTS idx_audit_user_id ON audit_events(user_id)",
                "CREATE INDEX IF NOT EXISTS idx_audit_event_type ON audit_events(event_type)",
                "CREATE INDEX IF NOT EXISTS idx_audit_record_id ON audit_events(record_id)",
                "CREATE INDEX IF NOT EXISTS idx_audit_session_id ON audit_events(session_id)",
            ]
            
            # Audit summary table for reporting
            create_summary_table_sql = """
            CREATE TABLE IF NOT EXISTS audit_summary (
                summary_date DATE NOT NULL,
                event_type VARCHAR NOT NULL,
                user_id VARCHAR NOT NULL,
                event_count INTEGER NOT NULL,
                error_count INTEGER NOT NULL,
                total_duration_ms DOUBLE,
                PRIMARY KEY (summary_date, event_type, user_id)
            )
            """
            
            # Audit retention policy table
            create_retention_table_sql = """
            CREATE TABLE IF NOT EXISTS audit_retention_policy (
                event_type VARCHAR NOT NULL PRIMARY KEY,
                retention_days INTEGER NOT NULL,
                archive_after_days INTEGER,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
            
            self.conn.execute(create_audit_table_sql)
            for index_sql in create_indexes_sql:
                self.conn.execute(index_sql)
            self.conn.execute(create_summary_table_sql)
            self.conn.execute(create_retention_table_sql)
            
            # Insert default retention policies
            self._insert_default_retention_policies()
            
            logger.debug("Audit database schema created or verified")
        except Exception as e:
            logger.error(f"Failed to create audit database schema: {e}", exc_info=True)
            raise
    
    def _insert_default_retention_policies(self) -> None:
        """Insert default retention policies for different event types."""
        if self.conn is None:
            raise RuntimeError("Audit database connection not established")
        
        default_policies = [
            # Critical events - keep longer
            (AuditEventType.SYSTEM_ERROR.value, 365, 180),
            (AuditEventType.SYSTEM_WARNING.value, 180, 90),
            (AuditEventType.SYNC_CONFLICT_DETECTED.value, 180, 90),
            (AuditEventType.SYNC_CONFLICT_RESOLVED.value, 180, 90),
            
            # User actions - moderate retention
            (AuditEventType.USER_LOGIN.value, 90, 30),
            (AuditEventType.USER_LOGOUT.value, 90, 30),
            (AuditEventType.RECORD_STATUS_CHANGE.value, 365, 180),
            (AuditEventType.CSV_UPLOAD.value, 365, 180),
            (AuditEventType.DATA_EXPORT.value, 180, 90),
            
            # Connection events - shorter retention
            (AuditEventType.CONNECTION_ESTABLISHED.value, 30, 7),
            (AuditEventType.CONNECTION_LOST.value, 90, 30),
            (AuditEventType.CONNECTION_RECONNECTED.value, 90, 30),
            
            # Record operations - moderate retention
            (AuditEventType.RECORD_VIEW.value, 30, 7),
            (AuditEventType.RECORD_EDIT_START.value, 180, 90),
            (AuditEventType.RECORD_EDIT_COMPLETE.value, 365, 180),
            (AuditEventType.RECORD_LOCK_ACQUIRE.value, 90, 30),
            (AuditEventType.RECORD_LOCK_RELEASE.value, 90, 30),
            
            # Sync events - shorter retention
            (AuditEventType.SYNC_EVENT_BROADCAST.value, 30, 7),
            (AuditEventType.SYNC_EVENT_RECEIVED.value, 7, 1),
        ]
        
        try:
            for event_type, retention_days, archive_days in default_policies:
                insert_sql = """
                INSERT OR IGNORE INTO audit_retention_policy 
                (event_type, retention_days, archive_after_days)
                VALUES (?, ?, ?)
                """
                self.conn.execute(insert_sql, [event_type, retention_days, archive_days])
            
            self.conn.commit()
            logger.debug("Default retention policies inserted")
        except Exception as e:
            logger.error(f"Failed to insert default retention policies: {e}", exc_info=True)
            raise
    
    def log_audit_event(self, event: AuditEvent) -> None:
        """Log an audit event to the database.
        
        Args:
            event: The audit event to log
        """
        if self.conn is None:
            raise RuntimeError("Audit database connection not established")
        
        try:
            insert_sql = """
            INSERT INTO audit_events (
                event_id, event_type, severity, timestamp, user_id, session_id,
                connection_id, record_id, action, details, ip_address, user_agent,
                duration_ms, success, error_message, before_state, after_state
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            self.conn.execute(insert_sql, [
                event.event_id,
                event.event_type.value,
                event.severity.value,
                event.timestamp,
                event.user_id,
                event.session_id,
                event.connection_id,
                event.record_id,
                event.action,
                json.dumps(event.details) if event.details else None,
                event.ip_address,
                event.user_agent,
                event.duration_ms,
                event.success,
                event.error_message,
                json.dumps(event.before_state) if event.before_state else None,
                json.dumps(event.after_state) if event.after_state else None,
            ])
            
            self.conn.commit()
            logger.debug(f"Audit event logged: {event.event_id}")
        except Exception as e:
            logger.error(f"Failed to log audit event {event.event_id}: {e}", exc_info=True)
            raise
    
    def query_audit_events(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        user_id: Optional[str] = None,
        event_types: Optional[List[AuditEventType]] = None,
        record_id: Optional[str] = None,
        session_id: Optional[str] = None,
        severity: Optional[AuditSeverity] = None,
        success: Optional[bool] = None,
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
            severity: Filter by severity level
            success: Filter by success status
            limit: Maximum number of results
            offset: Number of results to skip
            
        Returns:
            List of matching audit events
        """
        if self.conn is None:
            raise RuntimeError("Audit database connection not established")
        
        try:
            # Build dynamic query
            where_clauses = []
            params = []
            
            if start_time:
                where_clauses.append("timestamp >= ?")
                params.append(start_time)
            
            if end_time:
                where_clauses.append("timestamp <= ?")
                params.append(end_time)
            
            if user_id:
                where_clauses.append("user_id = ?")
                params.append(user_id)
            
            if event_types:
                placeholders = ",".join(["?" for _ in event_types])
                where_clauses.append(f"event_type IN ({placeholders})")
                params.extend([et.value for et in event_types])
            
            if record_id:
                where_clauses.append("record_id = ?")
                params.append(record_id)
            
            if session_id:
                where_clauses.append("session_id = ?")
                params.append(session_id)
            
            if severity:
                where_clauses.append("severity = ?")
                params.append(severity.value)
            
            if success is not None:
                where_clauses.append("success = ?")
                params.append(success)
            
            where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"
            
            query_sql = f"""
            SELECT 
                event_id, event_type, severity, timestamp, user_id, session_id,
                connection_id, record_id, action, details, ip_address, user_agent,
                duration_ms, success, error_message, before_state, after_state
            FROM audit_events
            WHERE {where_clause}
            ORDER BY timestamp DESC
            LIMIT ? OFFSET ?
            """
            
            params.extend([limit, offset])
            
            results = self.conn.execute(query_sql, params).fetchall()
            
            events = []
            for row in results:
                events.append(AuditEvent(
                    event_id=row[0],
                    event_type=AuditEventType(row[1]),
                    severity=AuditSeverity(row[2]),
                    timestamp=row[3],
                    user_id=row[4],
                    session_id=row[5],
                    connection_id=row[6],
                    record_id=row[7],
                    action=row[8],
                    details=json.loads(row[9]) if row[9] else {},
                    ip_address=row[10],
                    user_agent=row[11],
                    duration_ms=row[12],
                    success=row[13],
                    error_message=row[14],
                    before_state=json.loads(row[15]) if row[15] else None,
                    after_state=json.loads(row[16]) if row[16] else None,
                ))
            
            logger.debug(f"Retrieved {len(events)} audit events")
            return events
            
        except Exception as e:
            logger.error(f"Failed to query audit events: {e}", exc_info=True)
            raise
    
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
            group_by: Field to group by (user_id, event_type, severity)
            
        Returns:
            Dictionary containing report data
        """
        if self.conn is None:
            raise RuntimeError("Audit database connection not established")
        
        try:
            # Summary statistics
            summary_sql = """
            SELECT 
                COUNT(*) as total_events,
                COUNT(DISTINCT user_id) as unique_users,
                COUNT(DISTINCT session_id) as unique_sessions,
                COUNT(CASE WHEN success = FALSE THEN 1 END) as error_count,
                AVG(duration_ms) as avg_duration_ms,
                MAX(duration_ms) as max_duration_ms
            FROM audit_events
            WHERE timestamp BETWEEN ? AND ?
            """
            
            summary_result = self.conn.execute(summary_sql, [start_time, end_time]).fetchone()
            
            # Group by statistics
            group_sql = f"""
            SELECT 
                {group_by},
                COUNT(*) as event_count,
                COUNT(CASE WHEN success = FALSE THEN 1 END) as error_count,
                AVG(duration_ms) as avg_duration_ms
            FROM audit_events
            WHERE timestamp BETWEEN ? AND ?
            GROUP BY {group_by}
            ORDER BY event_count DESC
            """
            
            group_results = self.conn.execute(group_sql, [start_time, end_time]).fetchall()
            
            # Event type distribution
            event_type_sql = """
            SELECT 
                event_type,
                COUNT(*) as count,
                COUNT(CASE WHEN success = FALSE THEN 1 END) as errors
            FROM audit_events
            WHERE timestamp BETWEEN ? AND ?
            GROUP BY event_type
            ORDER BY count DESC
            """
            
            event_type_results = self.conn.execute(event_type_sql, [start_time, end_time]).fetchall()
            
            # Top errors
            error_sql = """
            SELECT 
                error_message,
                COUNT(*) as count,
                event_type
            FROM audit_events
            WHERE timestamp BETWEEN ? AND ? AND success = FALSE
            GROUP BY error_message, event_type
            ORDER BY count DESC
            LIMIT 10
            """
            
            error_results = self.conn.execute(error_sql, [start_time, end_time]).fetchall()
            
            report = {
                "period": {
                    "start_time": start_time.isoformat(),
                    "end_time": end_time.isoformat()
                },
                "summary": {
                    "total_events": summary_result[0] or 0,
                    "unique_users": summary_result[1] or 0,
                    "unique_sessions": summary_result[2] or 0,
                    "error_count": summary_result[3] or 0,
                    "error_rate": (summary_result[3] or 0) / max(summary_result[0] or 1, 1),
                    "avg_duration_ms": summary_result[4] or 0,
                    "max_duration_ms": summary_result[5] or 0
                },
                f"by_{group_by}": [
                    {
                        group_by: row[0],
                        "event_count": row[1],
                        "error_count": row[2],
                        "error_rate": row[2] / max(row[1], 1),
                        "avg_duration_ms": row[3] or 0
                    }
                    for row in group_results
                ],
                "event_types": [
                    {
                        "event_type": row[0],
                        "count": row[1],
                        "errors": row[2],
                        "error_rate": row[2] / max(row[1], 1)
                    }
                    for row in event_type_results
                ],
                "top_errors": [
                    {
                        "error_message": row[0],
                        "count": row[1],
                        "event_type": row[2]
                    }
                    for row in error_results
                ]
            }
            
            logger.info(f"Generated audit report for period {start_time} to {end_time}")
            return report
            
        except Exception as e:
            logger.error(f"Failed to generate audit report: {e}", exc_info=True)
            raise
    
    def cleanup_old_events(self, dry_run: bool = True) -> Dict[str, int]:
        """Clean up old audit events based on retention policies.
        
        Args:
            dry_run: If True, only count events that would be deleted
            
        Returns:
            Dictionary with cleanup statistics
        """
        if self.conn is None:
            raise RuntimeError("Audit database connection not established")
        
        try:
            # Get retention policies
            policies_sql = """
            SELECT event_type, retention_days, archive_after_days
            FROM audit_retention_policy
            """
            
            policies = self.conn.execute(policies_sql).fetchall()
            
            cleanup_stats = {
                "events_to_delete": 0,
                "events_to_archive": 0,
                "events_deleted": 0,
                "events_archived": 0,
                "by_event_type": {}
            }
            
            for event_type, retention_days, archive_days in policies:
                delete_before = datetime.now() - timedelta(days=retention_days)
                archive_before = datetime.now() - timedelta(days=archive_days) if archive_days else None
                
                # Count events to delete
                count_delete_sql = """
                SELECT COUNT(*) FROM audit_events
                WHERE event_type = ? AND timestamp < ?
                """
                
                delete_count = self.conn.execute(count_delete_sql, [event_type, delete_before]).fetchone()[0]
                
                # Count events to archive
                archive_count = 0
                if archive_before:
                    count_archive_sql = """
                    SELECT COUNT(*) FROM audit_events
                    WHERE event_type = ? AND timestamp < ? AND timestamp >= ?
                    """
                    archive_count = self.conn.execute(count_archive_sql, [event_type, archive_before, delete_before]).fetchone()[0]
                
                cleanup_stats["events_to_delete"] += delete_count
                cleanup_stats["events_to_archive"] += archive_count
                cleanup_stats["by_event_type"][event_type] = {
                    "to_delete": delete_count,
                    "to_archive": archive_count,
                    "deleted": 0,
                    "archived": 0
                }
                
                if not dry_run:
                    # Archive events if needed
                    if archive_before and archive_count > 0:
                        # In a real implementation, you might move to a separate archive table
                        # For now, we'll just mark them as archived in the details
                        archive_sql = """
                        UPDATE audit_events
                        SET details = JSON_SET(COALESCE(details, '{}'), '$.archived', TRUE)
                        WHERE event_type = ? AND timestamp < ? AND timestamp >= ?
                        """
                        self.conn.execute(archive_sql, [event_type, archive_before, delete_before])
                        cleanup_stats["events_archived"] += archive_count
                        cleanup_stats["by_event_type"][event_type]["archived"] = archive_count
                    
                    # Delete old events
                    if delete_count > 0:
                        delete_sql = """
                        DELETE FROM audit_events
                        WHERE event_type = ? AND timestamp < ?
                        """
                        self.conn.execute(delete_sql, [event_type, delete_before])
                        cleanup_stats["events_deleted"] += delete_count
                        cleanup_stats["by_event_type"][event_type]["deleted"] = delete_count
            
            if not dry_run:
                self.conn.commit()
                logger.info(f"Cleanup completed: deleted {cleanup_stats['events_deleted']}, archived {cleanup_stats['events_archived']}")
            else:
                logger.info(f"Cleanup dry run: would delete {cleanup_stats['events_to_delete']}, archive {cleanup_stats['events_to_archive']}")
            
            return cleanup_stats
            
        except Exception as e:
            logger.error(f"Failed to cleanup old events: {e}", exc_info=True)
            raise
    
    def close(self) -> None:
        """Close the audit database connection."""
        if self.conn is not None:
            try:
                self.conn.close()
                logger.debug("Audit database connection closed")
            except Exception as e:
                logger.warning(f"Error closing audit database connection: {e}", exc_info=True)
            finally:
                self.conn = None


class AuditLogger:
    """High-level audit logging interface."""
    
    def __init__(self, audit_db: AuditDatabase):
        """Initialize audit logger.
        
        Args:
            audit_db: Audit database instance
        """
        self.audit_db = audit_db
        self._event_counter = 0
    
    def _generate_event_id(self) -> str:
        """Generate a unique event ID."""
        self._event_counter += 1
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"audit_{timestamp}_{self._event_counter:06d}"
    
    def log_user_action(
        self,
        user_id: str,
        action: str,
        event_type: AuditEventType = AuditEventType.USER_SESSION_START,
        session_id: Optional[str] = None,
        record_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        duration_ms: Optional[float] = None,
        success: bool = True,
        error_message: Optional[str] = None
    ) -> None:
        """Log a user action audit event.
        
        Args:
            user_id: ID of the user performing the action
            action: Description of the action
            event_type: Type of audit event
            session_id: User session ID
            record_id: Related record ID
            details: Additional event details
            ip_address: User's IP address
            user_agent: User's browser/client info
            duration_ms: Action duration in milliseconds
            success: Whether the action succeeded
            error_message: Error message if action failed
        """
        severity = AuditSeverity.ERROR if not success else AuditSeverity.INFO
        
        event = AuditEvent(
            event_id=self._generate_event_id(),
            event_type=event_type,
            severity=severity,
            timestamp=datetime.now(),
            user_id=user_id,
            session_id=session_id,
            connection_id=None,
            record_id=record_id,
            action=action,
            details=details or {},
            ip_address=ip_address,
            user_agent=user_agent,
            duration_ms=duration_ms,
            success=success,
            error_message=error_message
        )
        
        self.audit_db.log_audit_event(event)
    
    def log_record_operation(
        self,
        user_id: str,
        record_id: str,
        action: str,
        event_type: AuditEventType,
        session_id: Optional[str] = None,
        before_state: Optional[Dict[str, Any]] = None,
        after_state: Optional[Dict[str, Any]] = None,
        details: Optional[Dict[str, Any]] = None,
        duration_ms: Optional[float] = None,
        success: bool = True,
        error_message: Optional[str] = None
    ) -> None:
        """Log a record operation audit event.
        
        Args:
            user_id: ID of the user performing the operation
            record_id: ID of the record being operated on
            action: Description of the operation
            event_type: Type of audit event
            session_id: User session ID
            before_state: Record state before operation
            after_state: Record state after operation
            details: Additional operation details
            duration_ms: Operation duration in milliseconds
            success: Whether the operation succeeded
            error_message: Error message if operation failed
        """
        severity = AuditSeverity.ERROR if not success else AuditSeverity.INFO
        
        event = AuditEvent(
            event_id=self._generate_event_id(),
            event_type=event_type,
            severity=severity,
            timestamp=datetime.now(),
            user_id=user_id,
            session_id=session_id,
            connection_id=None,
            record_id=record_id,
            action=action,
            details=details or {},
            duration_ms=duration_ms,
            success=success,
            error_message=error_message,
            before_state=before_state,
            after_state=after_state
        )
        
        self.audit_db.log_audit_event(event)
    
    def log_sync_event(
        self,
        user_id: str,
        action: str,
        event_type: AuditEventType,
        connection_id: Optional[str] = None,
        record_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        duration_ms: Optional[float] = None,
        success: bool = True,
        error_message: Optional[str] = None
    ) -> None:
        """Log a synchronization audit event.
        
        Args:
            user_id: ID of the user involved in sync
            action: Description of the sync action
            event_type: Type of audit event
            connection_id: WebSocket connection ID
            record_id: Related record ID
            details: Additional sync details
            duration_ms: Sync operation duration in milliseconds
            success: Whether the sync succeeded
            error_message: Error message if sync failed
        """
        severity = AuditSeverity.ERROR if not success else AuditSeverity.INFO
        
        event = AuditEvent(
            event_id=self._generate_event_id(),
            event_type=event_type,
            severity=severity,
            timestamp=datetime.now(),
            user_id=user_id,
            session_id=None,
            connection_id=connection_id,
            record_id=record_id,
            action=action,
            details=details or {},
            duration_ms=duration_ms,
            success=success,
            error_message=error_message
        )
        
        self.audit_db.log_audit_event(event)
    
    def log_system_event(
        self,
        action: str,
        event_type: AuditEventType,
        severity: AuditSeverity = AuditSeverity.INFO,
        user_id: str = "system",
        details: Optional[Dict[str, Any]] = None,
        error_message: Optional[str] = None
    ) -> None:
        """Log a system audit event.
        
        Args:
            action: Description of the system action
            event_type: Type of audit event
            severity: Event severity level
            user_id: User ID (defaults to "system")
            details: Additional system details
            error_message: Error message if applicable
        """
        event = AuditEvent(
            event_id=self._generate_event_id(),
            event_type=event_type,
            severity=severity,
            timestamp=datetime.now(),
            user_id=user_id,
            session_id=None,
            connection_id=None,
            record_id=None,
            action=action,
            details=details or {},
            success=error_message is None,
            error_message=error_message
        )
        
        self.audit_db.log_audit_event(event)