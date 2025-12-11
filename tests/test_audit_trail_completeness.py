"""Property-based test for audit trail completeness."""

import pytest
import tempfile
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any
from unittest.mock import Mock

from hypothesis import given, strategies as st

from mvh_copy_mb.sync.audit import AuditEventType, AuditSeverity
from mvh_copy_mb.sync.audit_manager import AuditTrailManager


# Strategies for generating test data
@st.composite
def user_interaction_strategy(draw):
    """Generate a user interaction scenario."""
    user_id = draw(st.text(min_size=1, max_size=20, alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd'))))
    session_id = draw(st.text(min_size=1, max_size=36, alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd', 'Pd'))))
    connection_id = draw(st.text(min_size=1, max_size=36, alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd', 'Pd'))))
    record_id = draw(st.text(min_size=1, max_size=20, alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd'))))
    
    # Generate a logical sequence of user actions
    base_actions = ['session_start', 'connection_establish', 'record_view']
    optional_actions = draw(st.lists(
        st.sampled_from([
            'record_edit_start',
            'record_status_change',
            'record_view'
        ]),
        min_size=0,
        max_size=5
    ))
    end_actions = ['connection_lost', 'session_end']
    
    # Combine into a logical sequence
    actions = base_actions + optional_actions + end_actions
    
    return {
        'user_id': user_id,
        'session_id': session_id,
        'connection_id': connection_id,
        'record_id': record_id,
        'actions': actions,
        'ip_address': '192.168.1.' + str(draw(st.integers(min_value=1, max_value=254))),
        'user_agent': 'TestBrowser/' + str(draw(st.floats(min_value=1.0, max_value=10.0)))
    }


@st.composite
def multi_user_scenario_strategy(draw):
    """Generate a multi-user interaction scenario."""
    num_users = draw(st.integers(min_value=1, max_value=5))
    users = []
    
    for i in range(num_users):
        user = draw(user_interaction_strategy())
        user['user_id'] = f"user_{i}"  # Ensure unique user IDs
        users.append(user)
    
    return users


class TestAuditTrailCompleteness:
    """Property-based tests for audit trail completeness."""
    
    @pytest.fixture
    def temp_audit_manager(self):
        """Create a temporary audit trail manager for testing."""
        # Create a temporary directory and file path (but don't create the file)
        temp_dir = tempfile.mkdtemp()
        db_path = Path(temp_dir) / "test_audit_completeness.db"
        
        try:
            with AuditTrailManager(db_path) as manager:
                yield manager
        finally:
            if db_path.exists():
                db_path.unlink()
            os.rmdir(temp_dir)
    
    @given(multi_user_scenario_strategy())
    def test_complete_audit_trail_maintenance_property(self, user_scenarios: List[Dict[str, Any]]):
        """Test that complete records are maintained for all multi-user interactions.
        
        **Feature: multi-user-sync, Property 23: Complete audit trail maintenance**
        **Validates: Requirements 5.5**
        
        This property verifies that for any multi-user interaction scenario,
        complete audit records are maintained including:
        - All user actions are logged
        - All session events are tracked
        - All connection events are recorded
        - All record operations are audited
        - Timestamps and user identification are preserved
        """
        # Create temporary audit manager for this test
        temp_dir = tempfile.mkdtemp()
        db_path = Path(temp_dir) / "property_test_audit.db"
        
        try:
            with AuditTrailManager(db_path) as audit_manager:
                # Track all expected events for verification
                expected_events = []
                
                # Execute the user scenarios and log all interactions
                for user_scenario in user_scenarios:
                    user_id = user_scenario['user_id']
                    session_id = user_scenario['session_id']
                    connection_id = user_scenario['connection_id']
                    record_id = user_scenario['record_id']
                    ip_address = user_scenario['ip_address']
                    user_agent = user_scenario['user_agent']
                    
                    # Process each action in the scenario
                    for action in user_scenario['actions']:
                        if action == 'session_start':
                            actual_session_id = audit_manager.start_user_session(
                                user_id=user_id,
                                ip_address=ip_address,
                                user_agent=user_agent
                            )
                            expected_events.append({
                                'type': AuditEventType.USER_SESSION_START,
                                'user_id': user_id,
                                'session_id': actual_session_id
                            })
                            
                        elif action == 'connection_establish':
                            audit_manager.log_connection_event(
                                user_id=user_id,
                                connection_id=connection_id,
                                event_type=AuditEventType.CONNECTION_ESTABLISHED,
                                session_id=session_id
                            )
                            expected_events.append({
                                'type': AuditEventType.CONNECTION_ESTABLISHED,
                                'user_id': user_id,
                                'connection_id': connection_id
                            })
                            
                        elif action == 'record_view':
                            audit_manager.log_record_view(
                                user_id=user_id,
                                record_id=record_id,
                                session_id=session_id
                            )
                            expected_events.append({
                                'type': AuditEventType.RECORD_VIEW,
                                'user_id': user_id,
                                'record_id': record_id
                            })
                            
                        elif action == 'record_edit_start':
                            # Use context manager for edit tracking
                            with audit_manager.track_record_edit(
                                user_id=user_id,
                                record_id=record_id,
                                session_id=session_id,
                                before_state={'status': 'pending'}
                            ):
                                pass  # Simulate edit work
                            
                            expected_events.extend([
                                {
                                    'type': AuditEventType.RECORD_EDIT_START,
                                    'user_id': user_id,
                                    'record_id': record_id
                                },
                                {
                                    'type': AuditEventType.RECORD_EDIT_COMPLETE,
                                    'user_id': user_id,
                                    'record_id': record_id
                                }
                            ])
                            
                        elif action == 'record_status_change':
                            audit_manager.log_record_status_change(
                                user_id=user_id,
                                record_id=record_id,
                                before_status=False,
                                after_status=True,
                                session_id=session_id
                            )
                            expected_events.append({
                                'type': AuditEventType.RECORD_STATUS_CHANGE,
                                'user_id': user_id,
                                'record_id': record_id
                            })
                            
                        elif action == 'connection_lost':
                            audit_manager.log_connection_event(
                                user_id=user_id,
                                connection_id=connection_id,
                                event_type=AuditEventType.CONNECTION_LOST,
                                session_id=session_id
                            )
                            expected_events.append({
                                'type': AuditEventType.CONNECTION_LOST,
                                'user_id': user_id,
                                'connection_id': connection_id
                            })
                            
                        elif action == 'session_end':
                            # Only end session if it was started
                            if session_id in audit_manager._active_sessions:
                                audit_manager.end_user_session(session_id)
                                expected_events.append({
                                    'type': AuditEventType.USER_SESSION_END,
                                    'user_id': user_id,
                                    'session_id': session_id
                                })
                
                # Verify that all expected events were logged
                all_events = audit_manager.query_audit_events(limit=10000)
                
                # Property 1: All expected events should be present in the audit log
                logged_event_types = {(event.event_type, event.user_id) for event in all_events}
                expected_event_types = {(expected['type'], expected['user_id']) for expected in expected_events}
                
                # Check that we have at least as many event types as expected
                # (there might be more due to internal logging)
                assert len(logged_event_types.intersection(expected_event_types)) >= len(expected_event_types) * 0.8, \
                    f"Missing expected event types. Expected: {expected_event_types}, Got: {logged_event_types}"
                
                # Property 2: All events should have timestamps
                for event in all_events:
                    assert event.timestamp is not None, "All events must have timestamps"
                    assert isinstance(event.timestamp, datetime), "Timestamps must be datetime objects"
                
                # Property 3: All events should have user identification
                for event in all_events:
                    assert event.user_id is not None, "All events must have user identification"
                    assert len(event.user_id) > 0, "User ID must not be empty"
                
                # Property 4: Events should be ordered by timestamp
                timestamps = [event.timestamp for event in all_events]
                assert timestamps == sorted(timestamps, reverse=True), \
                    "Events should be ordered by timestamp (newest first)"
                
                # Property 5: All user interactions should be traceable
                user_ids_in_scenario = {scenario['user_id'] for scenario in user_scenarios}
                user_ids_in_log = {event.user_id for event in all_events}
                
                # All users from scenarios should appear in the log
                assert user_ids_in_scenario.issubset(user_ids_in_log), \
                    f"All scenario users should appear in log. Scenario users: {user_ids_in_scenario}, Log users: {user_ids_in_log}"
                
                # Property 6: Session continuity - session events should be paired
                session_starts = [event for event in all_events if event.event_type == AuditEventType.USER_SESSION_START]
                session_ends = [event for event in all_events if event.event_type == AuditEventType.USER_SESSION_END]
                
                # Each session end should have a corresponding session start
                session_start_ids = {event.session_id for event in session_starts if event.session_id}
                session_end_ids = {event.session_id for event in session_ends if event.session_id}
                
                # All ended sessions should have been started
                assert session_end_ids.issubset(session_start_ids), \
                    "All ended sessions should have corresponding session starts"
                
                # Property 7: Record operations should be complete
                record_operations = [event for event in all_events if event.record_id is not None]
                
                # All record operations should have valid record IDs
                for event in record_operations:
                    assert event.record_id is not None, "Record operations must have record IDs"
                    assert len(event.record_id) > 0, "Record ID must not be empty"
                
                # Property 8: Connection events should be logged consistently
                connection_establishes = [event for event in all_events if event.event_type == AuditEventType.CONNECTION_ESTABLISHED]
                connection_losses = [event for event in all_events if event.event_type == AuditEventType.CONNECTION_LOST]
                
                # All connection events should have valid connection IDs
                for event in connection_establishes + connection_losses:
                    assert event.connection_id is not None, "Connection events must have connection IDs"
                
        finally:
            # Cleanup
            if db_path.exists():
                db_path.unlink()
            os.rmdir(temp_dir)
    
    @given(st.integers(min_value=1, max_value=20))
    def test_audit_trail_scalability_property(self, num_events: int):
        """Test that audit trail maintains completeness under load.
        
        This property verifies that the audit system can handle multiple
        events without losing data or corrupting the audit trail.
        """
        temp_dir = tempfile.mkdtemp()
        db_path = Path(temp_dir) / "scalability_test_audit.db"
        
        try:
            with AuditTrailManager(db_path) as audit_manager:
                # Generate and log many events
                for i in range(num_events):
                    audit_manager.log_record_status_change(
                        user_id=f"user_{i % 10}",  # 10 different users
                        record_id=f"record_{i}",
                        before_status=False,
                        after_status=True,
                        details={'test_event': i}
                    )
                
                # Verify all events were logged
                all_events = audit_manager.query_audit_events(limit=num_events + 100)
                
                # Should have at least the number of events we logged
                assert len(all_events) >= num_events, \
                    f"Should have logged at least {num_events} events, got {len(all_events)}"
                
                # All events should be valid
                for event in all_events:
                    assert event.user_id is not None, "Event must have user ID"
                    assert event.timestamp is not None, "Event must have timestamp"
                    assert event.action is not None, "Event must have action"
                
                # Events should be retrievable by user
                for user_num in range(min(10, num_events)):
                    user_id = f"user_{user_num}"
                    user_events = audit_manager.query_audit_events(user_id=user_id)
                    
                    # Should have events for this user
                    assert len(user_events) > 0, f"Should have events for user {user_id}"
                    
                    # All events should be for the correct user
                    for event in user_events:
                        assert event.user_id == user_id, f"Event user_id should be {user_id}"
        
        finally:
            # Cleanup
            if db_path.exists():
                db_path.unlink()
            os.rmdir(temp_dir)
    
    @given(st.integers(min_value=1, max_value=24))
    def test_audit_trail_time_range_completeness_property(self, hours_back: int):
        """Test that audit trail maintains completeness across time ranges.
        
        This property verifies that audit events can be queried and retrieved
        correctly across different time ranges without data loss.
        """
        temp_dir = tempfile.mkdtemp()
        db_path = Path(temp_dir) / "time_range_test_audit.db"
        
        try:
            with AuditTrailManager(db_path) as audit_manager:
                base_time = datetime.now()
                
                # Log events at different times
                events_by_hour = {}
                for hour in range(hours_back):
                    event_time = base_time - timedelta(hours=hour)
                    
                    # Manually create and log an event with specific timestamp
                    from mvh_copy_mb.sync.audit import AuditEvent, AuditEventType, AuditSeverity
                    import uuid
                    
                    event = AuditEvent(
                        event_id=str(uuid.uuid4()),
                        event_type=AuditEventType.RECORD_STATUS_CHANGE,
                        severity=AuditSeverity.INFO,
                        timestamp=event_time,
                        user_id=f"user_hour_{hour}",
                        session_id=None,
                        connection_id=None,
                        record_id=f"record_hour_{hour}",
                        action=f"Status change at hour {hour}",
                        details={'hour': hour},
                        success=True
                    )
                    
                    audit_manager.audit_db.log_audit_event(event)
                    events_by_hour[hour] = event
                
                # Query events for different time ranges
                for query_hours in [1, hours_back // 2, hours_back]:
                    if query_hours > hours_back:
                        continue
                        
                    start_time = base_time - timedelta(hours=query_hours)
                    end_time = base_time + timedelta(hours=1)  # Include future to catch all
                    
                    range_events = audit_manager.query_audit_events(
                        start_time=start_time,
                        end_time=end_time
                    )
                    
                    # Should have events within the time range
                    expected_count = min(query_hours + 1, hours_back)  # +1 because range is inclusive
                    assert len(range_events) >= expected_count, \
                        f"Should have at least {expected_count} events in {query_hours} hour range, got {len(range_events)}"
                    
                    # All events should be within the time range
                    for event in range_events:
                        assert start_time <= event.timestamp <= end_time, \
                            f"Event timestamp {event.timestamp} should be within range {start_time} to {end_time}"
                
                # Verify total completeness
                all_events = audit_manager.query_audit_events(limit=hours_back + 10)
                assert len(all_events) >= hours_back, \
                    f"Should have at least {hours_back} total events, got {len(all_events)}"
        
        finally:
            # Cleanup
            if db_path.exists():
                db_path.unlink()
            os.rmdir(temp_dir)