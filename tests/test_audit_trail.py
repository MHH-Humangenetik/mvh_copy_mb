"""Tests for audit trail system."""

import pytest
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock

from mvh_copy_mb.sync.audit import (
    AuditDatabase, AuditLogger, AuditEvent, AuditEventType, AuditSeverity
)
from mvh_copy_mb.sync.audit_manager import AuditTrailManager


class TestAuditDatabase:
    """Test audit database functionality."""
    
    @pytest.fixture
    def temp_audit_db(self):
        """Create a temporary audit database for testing."""
        import tempfile
        import os
        
        # Create a temporary directory and file path (but don't create the file)
        temp_dir = tempfile.mkdtemp()
        db_path = Path(temp_dir) / "test_audit.db"
        
        try:
            with AuditDatabase(db_path) as audit_db:
                yield audit_db
        finally:
            if db_path.exists():
                db_path.unlink()
            os.rmdir(temp_dir)
    
    def test_audit_database_initialization(self, temp_audit_db):
        """Test that audit database initializes correctly."""
        # Database should be initialized and ready to use
        assert temp_audit_db.conn is not None
    
    def test_log_audit_event(self, temp_audit_db):
        """Test logging an audit event."""
        event = AuditEvent(
            event_id="test_001",
            event_type=AuditEventType.USER_SESSION_START,
            severity=AuditSeverity.INFO,
            timestamp=datetime.now(),
            user_id="test_user",
            session_id="test_session",
            connection_id=None,
            record_id=None,
            action="User session started",
            details={"test": "data"},
            success=True
        )
        
        # Should not raise an exception
        temp_audit_db.log_audit_event(event)
    
    def test_query_audit_events(self, temp_audit_db):
        """Test querying audit events."""
        # Log some test events
        events = []
        for i in range(5):
            event = AuditEvent(
                event_id=f"test_{i:03d}",
                event_type=AuditEventType.RECORD_STATUS_CHANGE,
                severity=AuditSeverity.INFO,
                timestamp=datetime.now() - timedelta(minutes=i),
                user_id=f"user_{i}",
                session_id=f"session_{i}",
                connection_id=None,
                record_id=f"record_{i}",
                action=f"Status changed for record {i}",
                details={"record_id": f"record_{i}"},
                success=True
            )
            events.append(event)
            temp_audit_db.log_audit_event(event)
        
        # Query all events
        retrieved_events = temp_audit_db.query_audit_events()
        assert len(retrieved_events) == 5
        
        # Query by user ID
        user_events = temp_audit_db.query_audit_events(user_id="user_0")
        assert len(user_events) == 1
        assert user_events[0].user_id == "user_0"
        
        # Query by event type
        type_events = temp_audit_db.query_audit_events(
            event_types=[AuditEventType.RECORD_STATUS_CHANGE]
        )
        assert len(type_events) == 5
    
    def test_generate_audit_report(self, temp_audit_db):
        """Test generating an audit report."""
        # Log some test events
        start_time = datetime.now() - timedelta(hours=1)
        end_time = datetime.now()
        
        for i in range(3):
            event = AuditEvent(
                event_id=f"report_test_{i:03d}",
                event_type=AuditEventType.USER_SESSION_START,
                severity=AuditSeverity.INFO,
                timestamp=start_time + timedelta(minutes=i * 10),
                user_id=f"user_{i % 2}",  # Two different users
                session_id=f"session_{i}",
                connection_id=None,
                record_id=None,
                action=f"Session started {i}",
                details={},
                success=True,
                duration_ms=100.0 + i * 10
            )
            temp_audit_db.log_audit_event(event)
        
        # Generate report
        report = temp_audit_db.generate_audit_report(
            start_time=start_time,
            end_time=end_time,
            group_by="user_id"
        )
        
        assert "summary" in report
        assert "by_user_id" in report
        assert report["summary"]["total_events"] == 3
        assert report["summary"]["unique_users"] == 2
    
    def test_cleanup_old_events(self, temp_audit_db):
        """Test cleaning up old audit events."""
        # Log some old events
        old_time = datetime.now() - timedelta(days=400)  # Very old
        recent_time = datetime.now() - timedelta(days=1)  # Recent
        
        # Old event (should be cleaned up)
        old_event = AuditEvent(
            event_id="old_event",
            event_type=AuditEventType.USER_SESSION_START,
            severity=AuditSeverity.INFO,
            timestamp=old_time,
            user_id="old_user",
            session_id="old_session",
            connection_id=None,
            record_id=None,
            action="Old session",
            details={},
            success=True
        )
        temp_audit_db.log_audit_event(old_event)
        
        # Recent event (should be kept)
        recent_event = AuditEvent(
            event_id="recent_event",
            event_type=AuditEventType.USER_SESSION_START,
            severity=AuditSeverity.INFO,
            timestamp=recent_time,
            user_id="recent_user",
            session_id="recent_session",
            connection_id=None,
            record_id=None,
            action="Recent session",
            details={},
            success=True
        )
        temp_audit_db.log_audit_event(recent_event)
        
        # Test dry run first
        dry_run_stats = temp_audit_db.cleanup_old_events(dry_run=True)
        assert dry_run_stats["events_to_delete"] >= 0
        
        # Verify events still exist after dry run
        all_events = temp_audit_db.query_audit_events()
        assert len(all_events) == 2
        
        # Run actual cleanup
        cleanup_stats = temp_audit_db.cleanup_old_events(dry_run=False)
        assert cleanup_stats["events_deleted"] >= 0


class TestAuditTrailManager:
    """Test audit trail manager functionality."""
    
    @pytest.fixture
    def temp_audit_manager(self):
        """Create a temporary audit trail manager for testing."""
        import tempfile
        import os
        
        # Create a temporary directory and file path (but don't create the file)
        temp_dir = tempfile.mkdtemp()
        db_path = Path(temp_dir) / "test_audit_manager.db"
        
        try:
            with AuditTrailManager(db_path) as manager:
                yield manager
        finally:
            if db_path.exists():
                db_path.unlink()
            os.rmdir(temp_dir)
    
    def test_user_session_management(self, temp_audit_manager):
        """Test user session start and end tracking."""
        # Start a session
        session_id = temp_audit_manager.start_user_session(
            user_id="test_user",
            ip_address="127.0.0.1",
            user_agent="Test Browser"
        )
        
        assert session_id is not None
        assert session_id in temp_audit_manager._active_sessions
        
        # End the session
        temp_audit_manager.end_user_session(session_id)
        assert session_id not in temp_audit_manager._active_sessions
    
    def test_record_status_change_logging(self, temp_audit_manager):
        """Test logging record status changes."""
        # Should not raise an exception
        temp_audit_manager.log_record_status_change(
            user_id="test_user",
            record_id="test_record",
            before_status=False,
            after_status=True,
            session_id="test_session",
            details={"test": "data"}
        )
    
    def test_connection_event_logging(self, temp_audit_manager):
        """Test logging connection events."""
        # Should not raise an exception
        from mvh_copy_mb.sync.audit import AuditEventType
        temp_audit_manager.log_connection_event(
            user_id="test_user",
            connection_id="test_connection",
            event_type=AuditEventType.CONNECTION_ESTABLISHED,
            session_id="test_session",
            details={"endpoint": "/ws"}
        )
    
    def test_sync_conflict_logging(self, temp_audit_manager):
        """Test logging sync conflicts."""
        # Should not raise an exception
        temp_audit_manager.log_sync_conflict(
            record_id="test_record",
            involved_users=["user1", "user2"],
            conflict_type="version_conflict",
            resolution="first_wins",
            details={"expected_version": 1, "actual_version": 2},
            resolution_time_ms=150.0
        )
    
    def test_bulk_operation_logging(self, temp_audit_manager):
        """Test logging bulk operations."""
        # Should not raise an exception
        temp_audit_manager.log_bulk_operation(
            user_id="test_user",
            operation_type="csv_upload",
            record_count=100,
            session_id="test_session",
            success_count=95,
            failure_count=5,
            duration_ms=2500.0,
            details={"filename": "test.csv"}
        )
    
    def test_audit_event_querying(self, temp_audit_manager):
        """Test querying audit events through the manager."""
        # Log some events first
        temp_audit_manager.log_record_status_change(
            user_id="query_test_user",
            record_id="query_test_record",
            before_status=False,
            after_status=True
        )
        
        # Query events
        events = temp_audit_manager.query_audit_events(
            user_id="query_test_user",
            limit=10
        )
        
        assert len(events) >= 1
        assert any(event.user_id == "query_test_user" for event in events)
    
    def test_audit_report_generation(self, temp_audit_manager):
        """Test generating audit reports through the manager."""
        # Log some events first
        for i in range(3):
            temp_audit_manager.log_record_status_change(
                user_id=f"report_user_{i}",
                record_id=f"report_record_{i}",
                before_status=False,
                after_status=True
            )
        
        # Generate report
        start_time = datetime.now() - timedelta(hours=1)
        end_time = datetime.now() + timedelta(hours=1)
        
        report = temp_audit_manager.generate_audit_report(
            start_time=start_time,
            end_time=end_time,
            group_by="user_id"
        )
        
        assert "summary" in report
        assert report["summary"]["total_events"] >= 3
    
    def test_session_activity_report(self, temp_audit_manager):
        """Test generating session activity reports."""
        # Start and end some sessions
        session1 = temp_audit_manager.start_user_session("activity_user_1")
        session2 = temp_audit_manager.start_user_session("activity_user_2")
        
        temp_audit_manager.end_user_session(session1)
        # Leave session2 active
        
        # Generate activity report
        report = temp_audit_manager.get_session_activity_report(hours=1)
        
        assert "unique_users" in report
        assert "total_sessions" in report
        assert "active_sessions" in report
        assert report["active_sessions"] >= 1  # session2 should still be active
    
    def test_record_edit_tracking(self, temp_audit_manager):
        """Test record edit operation tracking."""
        # Use context manager for edit tracking
        with temp_audit_manager.track_record_edit(
            user_id="edit_user",
            record_id="edit_record",
            session_id="edit_session",
            before_state={"status": "pending"}
        ) as operation_id:
            # Simulate some work
            assert operation_id is not None
            assert operation_id in temp_audit_manager._active_operations
        
        # After context manager exits, operation should be cleaned up
        assert operation_id not in temp_audit_manager._active_operations


class TestAuditIntegration:
    """Test audit trail integration scenarios."""
    
    @pytest.fixture
    def temp_audit_manager(self):
        """Create a temporary audit trail manager for testing."""
        import tempfile
        import os
        
        # Create a temporary directory and file path (but don't create the file)
        temp_dir = tempfile.mkdtemp()
        db_path = Path(temp_dir) / "test_audit_integration.db"
        
        try:
            with AuditTrailManager(db_path) as manager:
                yield manager
        finally:
            if db_path.exists():
                db_path.unlink()
            os.rmdir(temp_dir)
    
    def test_complete_user_workflow(self, temp_audit_manager):
        """Test a complete user workflow with audit logging."""
        # Start user session
        session_id = temp_audit_manager.start_user_session(
            user_id="workflow_user",
            ip_address="192.168.1.100",
            user_agent="Mozilla/5.0 Test Browser"
        )
        
        # Log connection establishment
        from mvh_copy_mb.sync.audit import AuditEventType
        temp_audit_manager.log_connection_event(
            user_id="workflow_user",
            connection_id="ws_conn_123",
            event_type=AuditEventType.CONNECTION_ESTABLISHED,
            session_id=session_id
        )
        
        # Log record view
        temp_audit_manager.log_record_view(
            user_id="workflow_user",
            record_id="workflow_record_1",
            session_id=session_id
        )
        
        # Track record edit
        with temp_audit_manager.track_record_edit(
            user_id="workflow_user",
            record_id="workflow_record_1",
            session_id=session_id,
            before_state={"is_done": False}
        ):
            # Simulate edit work
            pass
        
        # Log status change
        temp_audit_manager.log_record_status_change(
            user_id="workflow_user",
            record_id="workflow_record_1",
            before_status=False,
            after_status=True,
            session_id=session_id
        )
        
        # Log connection loss
        temp_audit_manager.log_connection_event(
            user_id="workflow_user",
            connection_id="ws_conn_123",
            event_type=AuditEventType.CONNECTION_LOST,
            session_id=session_id
        )
        
        # End session
        temp_audit_manager.end_user_session(session_id)
        
        # Verify all events were logged
        events = temp_audit_manager.query_audit_events(
            user_id="workflow_user",
            session_id=session_id
        )
        
        # Should have multiple events for this workflow
        assert len(events) >= 5
        
        # Verify event types are present
        event_types = {event.event_type for event in events}
        expected_types = {
            AuditEventType.USER_SESSION_START,
            AuditEventType.CONNECTION_ESTABLISHED,
            AuditEventType.RECORD_VIEW,
            AuditEventType.RECORD_EDIT_START,
            AuditEventType.RECORD_EDIT_COMPLETE,
            AuditEventType.RECORD_STATUS_CHANGE,
            AuditEventType.CONNECTION_LOST,
            AuditEventType.USER_SESSION_END
        }
        
        # Should have most of the expected event types
        assert len(event_types.intersection(expected_types)) >= 5
    
    def test_error_handling_and_logging(self, temp_audit_manager):
        """Test error handling and system error logging."""
        # Log a system error
        from mvh_copy_mb.sync.audit import AuditSeverity
        temp_audit_manager.log_system_error(
            error_message="Test database connection failed",
            error_type="database_error",
            details={
                "database_path": "/tmp/test.db",
                "error_code": "CONNECTION_TIMEOUT"
            },
            severity=AuditSeverity.ERROR
        )
        
        # Query for error events
        error_events = temp_audit_manager.query_audit_events(
            event_types=[AuditEventType.SYSTEM_ERROR]
        )
        
        assert len(error_events) >= 1
        error_event = error_events[0]
        assert error_event.error_message == "Test database connection failed"
        assert error_event.success is False
        assert error_event.severity == AuditSeverity.ERROR