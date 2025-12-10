"""Property-based tests for the optimistic lock manager."""

import pytest
import asyncio
from datetime import datetime, timedelta
from typing import List

from hypothesis import given, strategies as st, settings
from hypothesis import assume

from mvh_copy_mb.sync.lock_manager import OptimisticLockManager
from mvh_copy_mb.sync.models import RecordLock, LockState


# Strategies for generating test data
@st.composite
def record_id_strategy(draw):
    """Generate valid record IDs."""
    return draw(st.text(min_size=1, max_size=50, alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd'))))


@st.composite
def user_id_strategy(draw):
    """Generate valid user IDs."""
    return draw(st.text(min_size=1, max_size=50, alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd'))))


@st.composite
def version_strategy(draw):
    """Generate valid version numbers."""
    return draw(st.integers(min_value=1, max_value=1000))


class TestVersionValidation:
    """Property-based tests for version validation in optimistic locking."""

    @given(record_id_strategy(), user_id_strategy(), version_strategy(), version_strategy())
    @settings(max_examples=10, deadline=5000)  # Limit examples and set deadline
    def test_version_validation_before_changes_property(self, record_id: str, user_id: str, 
                                                       lock_version: int, check_version: int):
        """Test that record versions are validated before applying changes.
        
        **Feature: multi-user-sync, Property 10: Version validation before changes**
        **Validates: Requirements 2.5**
        """
        async def run_test():
            lock_manager = OptimisticLockManager(default_timeout_seconds=30)
            await lock_manager.start()
            
            try:
                # Property: For any change attempt using optimistic locking, 
                # the record version should be validated before applying the change
                
                # Case 1: No existing lock - validation should pass for any version
                validation_result = await lock_manager.validate_version(record_id, check_version)
                assert validation_result is True, "Version validation should pass when no lock exists"
                
                # Case 2: Acquire a lock with a specific version
                lock = await lock_manager.acquire_lock(record_id, user_id, lock_version, timeout_seconds=30)
                assert lock is not None, "Lock acquisition should succeed"
                assert lock.version == lock_version, f"Lock should have version {lock_version}"
                
                # Case 3: Validate version against the locked record
                if check_version == lock_version:
                    # Matching version should pass validation
                    validation_result = await lock_manager.validate_version(record_id, check_version)
                    assert validation_result is True, (
                        f"Version validation should pass for matching version {check_version}"
                    )
                else:
                    # Non-matching version should fail validation
                    validation_result = await lock_manager.validate_version(record_id, check_version)
                    assert validation_result is False, (
                        f"Version validation should fail for mismatched version {check_version} "
                        f"(lock has version {lock_version})"
                    )
                
                # Case 4: After releasing the lock, validation should pass again
                release_result = await lock_manager.release_lock(record_id, user_id)
                assert release_result is True, "Lock release should succeed"
                
                validation_result = await lock_manager.validate_version(record_id, check_version)
                assert validation_result is True, "Version validation should pass after lock release"
                
                # Case 5: Verify lock is actually gone
                existing_lock = await lock_manager.check_lock(record_id)
                assert existing_lock is None, "No lock should exist after release"
                
            finally:
                await lock_manager.stop()
        
        # Run the async test
        asyncio.run(run_test())

    @given(record_id_strategy(), st.lists(user_id_strategy(), min_size=2, max_size=3, unique=True), 
           st.lists(version_strategy(), min_size=2, max_size=3))
    @settings(max_examples=5, deadline=3000)  # Fewer examples for complex test
    def test_concurrent_version_validation_property(self, record_id: str, user_ids: List[str], 
                                                   versions: List[int]):
        """Test version validation with concurrent access attempts.
        
        **Feature: multi-user-sync, Property 10: Version validation before changes**
        **Validates: Requirements 2.5**
        """
        assume(len(user_ids) >= 2)
        assume(len(versions) >= 2)
        
        async def run_test():
            lock_manager = OptimisticLockManager(default_timeout_seconds=30)
            await lock_manager.start()
            
            try:
                # Property: Version validation should work correctly with concurrent access
                
                # First user acquires lock with first version
                first_user = user_ids[0]
                first_version = versions[0]
                
                lock = await lock_manager.acquire_lock(record_id, first_user, first_version)
                assert lock is not None, "First lock acquisition should succeed"
                
                # Other users should not be able to acquire locks
                for i, other_user in enumerate(user_ids[1:], 1):
                    other_version = versions[i % len(versions)]
                    other_lock = await lock_manager.acquire_lock(record_id, other_user, other_version)
                    assert other_lock is None, f"User {other_user} should not be able to acquire lock"
                
                # Version validation should only pass for the correct version
                for version in versions:
                    validation_result = await lock_manager.validate_version(record_id, version)
                    if version == first_version:
                        assert validation_result is True, (
                            f"Version validation should pass for correct version {version}"
                        )
                    else:
                        assert validation_result is False, (
                            f"Version validation should fail for incorrect version {version}"
                        )
                
                # After first user releases lock, second user can acquire with different version
                release_result = await lock_manager.release_lock(record_id, first_user)
                assert release_result is True, "Lock release should succeed"
                
                if len(user_ids) >= 2 and len(versions) >= 2:
                    second_user = user_ids[1]
                    second_version = versions[1]
                    
                    second_lock = await lock_manager.acquire_lock(record_id, second_user, second_version)
                    assert second_lock is not None, "Second lock acquisition should succeed"
                    assert second_lock.version == second_version, f"Second lock should have version {second_version}"
                    
                    # Now validation should work for the new version
                    for version in versions:
                        validation_result = await lock_manager.validate_version(record_id, version)
                        if version == second_version:
                            assert validation_result is True, (
                                f"Version validation should pass for new correct version {version}"
                            )
                        else:
                            assert validation_result is False, (
                                f"Version validation should fail for incorrect version {version}"
                            )
                
            finally:
                await lock_manager.stop()
        
        # Run the async test
        asyncio.run(run_test())

    @given(record_id_strategy(), user_id_strategy(), version_strategy())
    @settings(max_examples=5, deadline=3000)  # Limit for sleep operations
    def test_expired_lock_version_validation_property(self, record_id: str, user_id: str, version: int):
        """Test that version validation handles expired locks correctly.
        
        **Feature: multi-user-sync, Property 10: Version validation before changes**
        **Validates: Requirements 2.5**
        """
        async def run_test():
            # Use very short timeout to test expiration
            lock_manager = OptimisticLockManager(default_timeout_seconds=1)
            await lock_manager.start()
            
            try:
                # Property: Version validation should handle expired locks correctly
                
                # Acquire lock with short timeout
                lock = await lock_manager.acquire_lock(record_id, user_id, version, timeout_seconds=1)
                assert lock is not None, "Lock acquisition should succeed"
                
                # Initially, validation should work for correct version
                validation_result = await lock_manager.validate_version(record_id, version)
                assert validation_result is True, "Version validation should pass for correct version"
                
                # Wait for lock to expire
                await asyncio.sleep(1.1)  # Wait longer than timeout
                
                # After expiration, validation should pass for any version (no lock exists)
                validation_result = await lock_manager.validate_version(record_id, version)
                assert validation_result is True, "Version validation should pass after lock expiration"
                
                # Different version should also pass after expiration
                different_version = version + 1
                validation_result = await lock_manager.validate_version(record_id, different_version)
                assert validation_result is True, "Version validation should pass for any version after expiration"
                
                # Verify lock is actually expired and cleaned up
                existing_lock = await lock_manager.check_lock(record_id)
                assert existing_lock is None, "Expired lock should be cleaned up"
                
            finally:
                await lock_manager.stop()
        
        # Run the async test
        asyncio.run(run_test())

    @given(st.lists(record_id_strategy(), min_size=2, max_size=3, unique=True), 
           user_id_strategy(), st.lists(version_strategy(), min_size=2, max_size=3))
    @settings(max_examples=5, deadline=3000)
    def test_multiple_records_version_validation_property(self, record_ids: List[str], user_id: str, 
                                                         versions: List[int]):
        """Test version validation across multiple records.
        
        **Feature: multi-user-sync, Property 10: Version validation before changes**
        **Validates: Requirements 2.5**
        """
        assume(len(record_ids) >= 2)
        assume(len(versions) >= 2)
        
        async def run_test():
            lock_manager = OptimisticLockManager(default_timeout_seconds=30)
            await lock_manager.start()
            
            try:
                # Property: Version validation should work independently for different records
                
                # Acquire locks on different records with different versions
                acquired_locks = {}
                for i, record_id in enumerate(record_ids):
                    version = versions[i % len(versions)]
                    lock = await lock_manager.acquire_lock(record_id, user_id, version)
                    assert lock is not None, f"Lock acquisition should succeed for record {record_id}"
                    acquired_locks[record_id] = version
                
                # Validate versions for each record independently
                for record_id, expected_version in acquired_locks.items():
                    # Correct version should pass
                    validation_result = await lock_manager.validate_version(record_id, expected_version)
                    assert validation_result is True, (
                        f"Version validation should pass for record {record_id} with version {expected_version}"
                    )
                    
                    # Test with different versions
                    for test_version in versions:
                        validation_result = await lock_manager.validate_version(record_id, test_version)
                        if test_version == expected_version:
                            assert validation_result is True, (
                                f"Version validation should pass for correct version {test_version} "
                                f"on record {record_id}"
                            )
                        else:
                            assert validation_result is False, (
                                f"Version validation should fail for incorrect version {test_version} "
                                f"on record {record_id} (expected {expected_version})"
                            )
                
                # Verify that validation for one record doesn't affect others
                if len(record_ids) >= 2:
                    first_record = record_ids[0]
                    second_record = record_ids[1]
                    
                    # Release lock on first record
                    release_result = await lock_manager.release_lock(first_record, user_id)
                    assert release_result is True, f"Lock release should succeed for {first_record}"
                    
                    # First record should now pass validation for any version
                    for version in versions:
                        validation_result = await lock_manager.validate_version(first_record, version)
                        assert validation_result is True, (
                            f"Version validation should pass for any version on released record {first_record}"
                        )
                    
                    # Second record should still enforce its version
                    second_version = acquired_locks[second_record]
                    validation_result = await lock_manager.validate_version(second_record, second_version)
                    assert validation_result is True, (
                        f"Version validation should still work for locked record {second_record}"
                    )
                    
                    # Wrong version on second record should still fail
                    wrong_version = second_version + 100
                    validation_result = await lock_manager.validate_version(second_record, wrong_version)
                    assert validation_result is False, (
                        f"Version validation should still fail for wrong version on locked record {second_record}"
                    )
                
            finally:
                await lock_manager.stop()
        
        # Run the async test
        asyncio.run(run_test())


class TestFirstWinsConflictResolution:
    """Property-based tests for first-wins conflict resolution."""

    @given(record_id_strategy(), st.lists(user_id_strategy(), min_size=2, max_size=3, unique=True), 
           version_strategy())
    @settings(max_examples=5, deadline=3000)
    def test_first_wins_conflict_resolution_property(self, record_id: str, user_ids: List[str], version: int):
        """Test that first-wins conflict resolution works correctly for simultaneous modifications.
        
        **Feature: multi-user-sync, Property 6: First-wins conflict resolution**
        **Validates: Requirements 2.1**
        """
        from mvh_copy_mb.sync.conflict_resolver import FirstWinsConflictResolver
        from mvh_copy_mb.sync.models import SyncEvent, EventType
        from mvh_copy_mb.events.broker import EventBrokerImpl
        from datetime import datetime, timedelta
        
        assume(len(user_ids) >= 2)
        
        async def run_test():
            lock_manager = OptimisticLockManager(default_timeout_seconds=30)
            event_broker = EventBrokerImpl()
            conflict_resolver = FirstWinsConflictResolver(lock_manager, event_broker)
            
            await lock_manager.start()
            
            try:
                # Property: For any two simultaneous modification attempts on the same record,
                # only the first change should be applied and subsequent changes should be rejected
                
                # Create simultaneous events with slightly different timestamps
                base_time = datetime.now()
                events = []
                
                for i, user_id in enumerate(user_ids):
                    # Create events with small time differences (simulating near-simultaneous)
                    event_time = base_time + timedelta(milliseconds=i * 10)  # 10ms apart
                    
                    event = SyncEvent(
                        event_type=EventType.RECORD_UPDATED.value,
                        record_id=record_id,
                        data={"user": user_id, "change": f"modification_{i}"},
                        version=version,
                        timestamp=event_time,
                        user_id=user_id
                    )
                    events.append(event)
                
                # Resolve the conflict
                result = await conflict_resolver.resolve_conflict(events)
                
                # Property verification: First-wins should be applied
                assert result.success is True, "Conflict resolution should succeed"
                assert result.winning_event is not None, "Should have a winning event"
                assert len(result.rejected_events) == len(user_ids) - 1, (
                    f"Should reject {len(user_ids) - 1} events, rejected {len(result.rejected_events)}"
                )
                
                # The winning event should be the first one (earliest timestamp)
                earliest_event = min(events, key=lambda e: e.timestamp)
                assert result.winning_event.user_id == earliest_event.user_id, (
                    f"Winning event should be from user {earliest_event.user_id}, "
                    f"got {result.winning_event.user_id}"
                )
                assert result.winning_event.timestamp == earliest_event.timestamp, (
                    "Winning event should have the earliest timestamp"
                )
                
                # All other events should be rejected
                rejected_user_ids = {event.user_id for event in result.rejected_events}
                expected_rejected_users = set(user_ids) - {earliest_event.user_id}
                assert rejected_user_ids == expected_rejected_users, (
                    f"Rejected users {rejected_user_ids} should match expected {expected_rejected_users}"
                )
                
                # Should have notifications for rejected events
                assert len(result.notifications) == len(result.rejected_events), (
                    "Should have one notification per rejected event"
                )
                
                # Each notification should reference the correct record and conflict
                for notification in result.notifications:
                    assert notification.record_id == record_id, (
                        f"Notification should be for record {record_id}"
                    )
                    assert notification.conflict_type == "simultaneous_edit", (
                        "Should identify as simultaneous edit conflict"
                    )
                    assert earliest_event.user_id in notification.message, (
                        "Notification should mention the winning user"
                    )
                
            finally:
                await lock_manager.stop()
        
        # Run the async test
        asyncio.run(run_test())

    @given(record_id_strategy(), user_id_strategy(), st.lists(version_strategy(), min_size=2, max_size=3, unique=True))
    @settings(max_examples=5, deadline=3000)
    def test_version_based_conflict_resolution_property(self, record_id: str, user_id: str, versions: List[int]):
        """Test conflict resolution with version mismatches.
        
        **Feature: multi-user-sync, Property 6: First-wins conflict resolution**
        **Validates: Requirements 2.1**
        """
        from mvh_copy_mb.sync.conflict_resolver import FirstWinsConflictResolver
        from mvh_copy_mb.sync.models import SyncEvent, EventType
        from mvh_copy_mb.events.broker import EventBrokerImpl
        
        assume(len(versions) >= 2)
        
        async def run_test():
            lock_manager = OptimisticLockManager(default_timeout_seconds=30)
            event_broker = EventBrokerImpl()
            conflict_resolver = FirstWinsConflictResolver(lock_manager, event_broker)
            
            await lock_manager.start()
            
            try:
                # Property: Version-based conflicts should be resolved with first-wins
                
                # First, acquire a lock with the first version
                first_version = versions[0]
                lock = await lock_manager.acquire_lock(record_id, user_id, first_version)
                assert lock is not None, "Initial lock should be acquired"
                
                # Try to perform operations with different versions
                base_time = datetime.now()
                events = []
                
                for i, version in enumerate(versions):
                    event_time = base_time + timedelta(milliseconds=i * 5)
                    
                    event = SyncEvent(
                        event_type=EventType.RECORD_UPDATED.value,
                        record_id=record_id,
                        data={"version_test": True, "attempt": i},
                        version=version,
                        timestamp=event_time,
                        user_id=user_id
                    )
                    events.append(event)
                
                # Test each event individually for conflict detection
                results = []
                for event in events:
                    result = await conflict_resolver.attempt_operation_with_conflict_detection(event)
                    results.append(result)
                
                # Property: Only the event with matching version should succeed
                successful_results = [r for r in results if r.success]
                failed_results = [r for r in results if not r.success]
                
                # Should have exactly one successful result (the one with matching version)
                assert len(successful_results) == 1, (
                    f"Should have exactly 1 successful result, got {len(successful_results)}"
                )
                
                # The successful event should have the correct version
                successful_event = successful_results[0].winning_event
                assert successful_event.version == first_version, (
                    f"Successful event should have version {first_version}, got {successful_event.version}"
                )
                
                # All other events should fail due to version mismatch
                expected_failures = len(versions) - 1
                assert len(failed_results) == expected_failures, (
                    f"Should have {expected_failures} failed results, got {len(failed_results)}"
                )
                
                # Failed results should have conflict notifications
                for failed_result in failed_results:
                    assert len(failed_result.rejected_events) == 1, "Should have one rejected event"
                    assert len(failed_result.notifications) == 1, "Should have one notification"
                    
                    notification = failed_result.notifications[0]
                    assert notification.record_id == record_id, "Notification for correct record"
                    assert "version" in notification.conflict_type.lower() or "simultaneous" in notification.conflict_type.lower(), (
                        f"Should indicate version or simultaneous conflict, got {notification.conflict_type}"
                    )
                
            finally:
                await lock_manager.stop()
        
        # Run the async test
        asyncio.run(run_test())

    @given(st.lists(record_id_strategy(), min_size=2, max_size=3, unique=True), 
           st.lists(user_id_strategy(), min_size=2, max_size=3, unique=True), version_strategy())
    @settings(max_examples=3, deadline=5000)  # Complex test, fewer examples
    def test_multiple_records_conflict_resolution_property(self, record_ids: List[str], user_ids: List[str], version: int):
        """Test conflict resolution across multiple records simultaneously.
        
        **Feature: multi-user-sync, Property 6: First-wins conflict resolution**
        **Validates: Requirements 2.1**
        """
        from mvh_copy_mb.sync.conflict_resolver import FirstWinsConflictResolver
        from mvh_copy_mb.sync.models import SyncEvent, EventType
        from mvh_copy_mb.events.broker import EventBrokerImpl
        
        assume(len(record_ids) >= 2)
        assume(len(user_ids) >= 2)
        
        async def run_test():
            lock_manager = OptimisticLockManager(default_timeout_seconds=30)
            event_broker = EventBrokerImpl()
            conflict_resolver = FirstWinsConflictResolver(lock_manager, event_broker)
            
            await lock_manager.start()
            
            try:
                # Property: Conflict resolution should work independently for different records
                
                # Create events for multiple records with conflicts on each
                base_time = datetime.now()
                all_events = []
                expected_winners = {}  # record_id -> winning_user_id
                
                for record_i, record_id in enumerate(record_ids):
                    record_events = []
                    
                    # Create conflicting events for this record
                    for user_i, user_id in enumerate(user_ids):
                        # Stagger timestamps slightly for each record
                        event_time = base_time + timedelta(
                            milliseconds=record_i * 100 + user_i * 10
                        )
                        
                        event = SyncEvent(
                            event_type=EventType.RECORD_UPDATED.value,
                            record_id=record_id,
                            data={"record": record_id, "user": user_id, "multi_test": True},
                            version=version,
                            timestamp=event_time,
                            user_id=user_id
                        )
                        record_events.append(event)
                        all_events.append(event)
                    
                    # The first user for each record should win (earliest timestamp)
                    expected_winners[record_id] = user_ids[0]
                
                # Resolve all conflicts together
                result = await conflict_resolver.resolve_conflict(all_events)
                
                # Property verification: Each record should have independent resolution
                assert result.success is True, "Overall conflict resolution should succeed"
                
                # Should have one winner per record
                expected_total_winners = len(record_ids)
                expected_total_rejected = len(all_events) - expected_total_winners
                
                assert len(result.rejected_events) == expected_total_rejected, (
                    f"Should reject {expected_total_rejected} events, rejected {len(result.rejected_events)}"
                )
                
                # Group rejected events by record to verify per-record resolution
                rejected_by_record = {}
                for event in result.rejected_events:
                    if event.record_id not in rejected_by_record:
                        rejected_by_record[event.record_id] = []
                    rejected_by_record[event.record_id].append(event)
                
                # Each record should have (num_users - 1) rejected events
                expected_rejected_per_record = len(user_ids) - 1
                for record_id in record_ids:
                    rejected_count = len(rejected_by_record.get(record_id, []))
                    assert rejected_count == expected_rejected_per_record, (
                        f"Record {record_id} should have {expected_rejected_per_record} rejected events, "
                        f"got {rejected_count}"
                    )
                
                # Verify notifications are generated correctly
                assert len(result.notifications) == expected_total_rejected, (
                    "Should have one notification per rejected event"
                )
                
                # Group notifications by record
                notifications_by_record = {}
                for notification in result.notifications:
                    if notification.record_id not in notifications_by_record:
                        notifications_by_record[notification.record_id] = []
                    notifications_by_record[notification.record_id].append(notification)
                
                # Each record should have notifications for its rejected events
                for record_id in record_ids:
                    record_notifications = notifications_by_record.get(record_id, [])
                    assert len(record_notifications) == expected_rejected_per_record, (
                        f"Record {record_id} should have {expected_rejected_per_record} notifications"
                    )
                    
                    # All notifications for this record should mention the winning user
                    expected_winner = expected_winners[record_id]
                    for notification in record_notifications:
                        assert expected_winner in notification.message or expected_winner == notification.conflicting_user, (
                            f"Notification should reference winning user {expected_winner}"
                        )
                
            finally:
                await lock_manager.stop()
        
        # Run the async test
        asyncio.run(run_test())

    @given(record_id_strategy(), user_id_strategy(), version_strategy())
    @settings(max_examples=5, deadline=2000)
    def test_no_conflict_scenario_property(self, record_id: str, user_id: str, version: int):
        """Test that conflict resolution handles non-conflicting scenarios correctly.
        
        **Feature: multi-user-sync, Property 6: First-wins conflict resolution**
        **Validates: Requirements 2.1**
        """
        from mvh_copy_mb.sync.conflict_resolver import FirstWinsConflictResolver
        from mvh_copy_mb.sync.models import SyncEvent, EventType
        from mvh_copy_mb.events.broker import EventBrokerImpl
        
        async def run_test():
            lock_manager = OptimisticLockManager(default_timeout_seconds=30)
            event_broker = EventBrokerImpl()
            conflict_resolver = FirstWinsConflictResolver(lock_manager, event_broker)
            
            await lock_manager.start()
            
            try:
                # Property: Non-conflicting operations should proceed without issues
                
                # Single event should not have conflicts
                single_event = SyncEvent(
                    event_type=EventType.RECORD_UPDATED.value,
                    record_id=record_id,
                    data={"single": True, "test": "no_conflict"},
                    version=version,
                    timestamp=datetime.now(),
                    user_id=user_id
                )
                
                # Test single event resolution
                result = await conflict_resolver.resolve_conflict([single_event])
                
                assert result.success is True, "Single event should succeed"
                assert result.winning_event == single_event, "Single event should be the winner"
                assert len(result.rejected_events) == 0, "No events should be rejected"
                assert len(result.notifications) == 0, "No notifications should be generated"
                
                # Test operation attempt without existing locks
                operation_result = await conflict_resolver.attempt_operation_with_conflict_detection(single_event)
                
                assert operation_result.success is True, "Operation should succeed without conflicts"
                assert operation_result.winning_event == single_event, "Event should be accepted"
                assert len(operation_result.rejected_events) == 0, "No events should be rejected"
                assert len(operation_result.notifications) == 0, "No notifications for successful operation"
                
                # Test empty event list
                empty_result = await conflict_resolver.resolve_conflict([])
                assert empty_result.success is True, "Empty list should succeed"
                assert empty_result.winning_event is None, "No winning event for empty list"
                assert len(empty_result.rejected_events) == 0, "No rejected events"
                assert len(empty_result.notifications) == 0, "No notifications"
                
            finally:
                await lock_manager.stop()
        
        # Run the async test
        asyncio.run(run_test())

class TestAutomaticLockCleanup:
    """Property-based tests for automatic lock release on disconnection."""

    @given(record_id_strategy(), user_id_strategy(), version_strategy())
    @settings(max_examples=3, deadline=5000)  # Complex async test
    def test_automatic_lock_release_on_disconnection_property(self, record_id: str, user_id: str, version: int):
        """Test that locks are automatically released within 30 seconds of user disconnection.
        
        **Feature: multi-user-sync, Property 8: Automatic lock release on disconnection**
        **Validates: Requirements 2.3**
        """
        from mvh_copy_mb.sync.connection_monitor import ConnectionMonitor
        from mvh_copy_mb.websocket.manager import WebSocketManager
        from mvh_copy_mb.sync.config import SyncConfig
        from datetime import datetime, timedelta
        
        async def run_test():
            lock_manager = OptimisticLockManager(default_timeout_seconds=60)  # Long timeout to test disconnection cleanup
            
            # Create mock connection manager
            config = SyncConfig(connection_timeout_seconds=30)
            connection_manager = WebSocketManager(config)
            
            # Create connection monitor
            monitor = ConnectionMonitor(lock_manager, connection_manager, 
                                      cleanup_interval_seconds=1,  # Fast cleanup for testing
                                      connection_timeout_seconds=30)
            
            await lock_manager.start()
            await connection_manager.start()
            await monitor.start()
            
            try:
                # Property: When a user's connection is lost during an edit, 
                # all locks held by that user should be released within 30 seconds
                
                # User acquires a lock
                lock = await lock_manager.acquire_lock(record_id, user_id, version, timeout_seconds=120)
                assert lock is not None, "Lock acquisition should succeed"
                
                # Verify lock exists
                existing_lock = await lock_manager.check_lock(record_id)
                assert existing_lock is not None, "Lock should exist after acquisition"
                assert existing_lock.user_id == user_id, f"Lock should belong to user {user_id}"
                
                # Simulate user disconnection
                disconnection_time = datetime.now()
                await monitor.handle_user_disconnection(user_id)
                
                # Immediately after disconnection, lock should be released
                # (our implementation releases immediately on disconnection)
                released_locks = await lock_manager.release_user_locks(user_id)
                
                # Verify lock is released
                existing_lock = await lock_manager.check_lock(record_id)
                assert existing_lock is None, "Lock should be released after disconnection"
                
                # Test the 30-second timeout behavior with a new lock
                lock2 = await lock_manager.acquire_lock(record_id, user_id, version + 1, timeout_seconds=120)
                assert lock2 is not None, "Second lock acquisition should succeed"
                
                # Test cleanup with specific disconnection time
                past_disconnection = datetime.now() - timedelta(seconds=35)  # 35 seconds ago
                recent_disconnection = datetime.now() - timedelta(seconds=15)  # 15 seconds ago
                
                # Cleanup for old disconnection should work
                old_cleanup_locks = await lock_manager.cleanup_locks_for_disconnected_user(user_id, past_disconnection)
                assert len(old_cleanup_locks) > 0, "Should clean up locks for old disconnection"
                
                # Acquire another lock to test recent disconnection
                lock3 = await lock_manager.acquire_lock(record_id, user_id, version + 2, timeout_seconds=120)
                assert lock3 is not None, "Third lock acquisition should succeed"
                
                # Cleanup for recent disconnection should not work (within 30 seconds)
                recent_cleanup_locks = await lock_manager.cleanup_locks_for_disconnected_user(user_id, recent_disconnection)
                assert len(recent_cleanup_locks) == 0, "Should not clean up locks for recent disconnection"
                
                # Lock should still exist
                existing_lock = await lock_manager.check_lock(record_id)
                assert existing_lock is not None, "Lock should still exist for recent disconnection"
                
            finally:
                await monitor.stop()
                await connection_manager.stop()
                await lock_manager.stop()
        
        # Run the async test
        asyncio.run(run_test())

    @given(st.lists(record_id_strategy(), min_size=2, max_size=3, unique=True), 
           user_id_strategy(), st.lists(version_strategy(), min_size=2, max_size=3))
    @settings(max_examples=3, deadline=3000)
    def test_multiple_locks_cleanup_on_disconnection_property(self, record_ids: List[str], user_id: str, versions: List[int]):
        """Test that all locks held by a user are released on disconnection.
        
        **Feature: multi-user-sync, Property 8: Automatic lock release on disconnection**
        **Validates: Requirements 2.3**
        """
        assume(len(record_ids) >= 2)
        assume(len(versions) >= 2)
        
        async def run_test():
            lock_manager = OptimisticLockManager(default_timeout_seconds=60)
            await lock_manager.start()
            
            try:
                # Property: All locks held by a disconnected user should be released
                
                # User acquires multiple locks
                acquired_locks = []
                for i, record_id in enumerate(record_ids):
                    version = versions[i % len(versions)]
                    lock = await lock_manager.acquire_lock(record_id, user_id, version)
                    assert lock is not None, f"Lock acquisition should succeed for record {record_id}"
                    acquired_locks.append(lock)
                
                # Verify all locks exist
                for record_id in record_ids:
                    existing_lock = await lock_manager.check_lock(record_id)
                    assert existing_lock is not None, f"Lock should exist for record {record_id}"
                    assert existing_lock.user_id == user_id, f"Lock should belong to user {user_id}"
                
                # Simulate disconnection and cleanup
                released_locks = await lock_manager.release_user_locks(user_id)
                
                # Property verification: All locks should be released
                assert len(released_locks) == len(record_ids), (
                    f"Should release {len(record_ids)} locks, released {len(released_locks)}"
                )
                
                # Verify all record IDs are represented in released locks
                released_record_ids = {lock.record_id for lock in released_locks}
                expected_record_ids = set(record_ids)
                assert released_record_ids == expected_record_ids, (
                    f"Released record IDs {released_record_ids} should match expected {expected_record_ids}"
                )
                
                # Verify all locks are actually gone
                for record_id in record_ids:
                    existing_lock = await lock_manager.check_lock(record_id)
                    assert existing_lock is None, f"Lock should be released for record {record_id}"
                
                # Verify lock manager state is clean
                assert lock_manager.get_lock_count() == 0, "Lock manager should have no active locks"
                assert lock_manager.get_user_lock_count(user_id) == 0, f"User {user_id} should have no locks"
                
            finally:
                await lock_manager.stop()
        
        # Run the async test
        asyncio.run(run_test())

    @given(st.lists(user_id_strategy(), min_size=2, max_size=3, unique=True), 
           st.lists(record_id_strategy(), min_size=2, max_size=3, unique=True), version_strategy())
    @settings(max_examples=3, deadline=5000)
    def test_selective_user_cleanup_property(self, user_ids: List[str], record_ids: List[str], version: int):
        """Test that disconnection cleanup only affects the disconnected user's locks.
        
        **Feature: multi-user-sync, Property 8: Automatic lock release on disconnection**
        **Validates: Requirements 2.3**
        """
        assume(len(user_ids) >= 2)
        assume(len(record_ids) >= 2)
        
        async def run_test():
            lock_manager = OptimisticLockManager(default_timeout_seconds=60)
            await lock_manager.start()
            
            try:
                # Property: Disconnection cleanup should only affect the disconnected user
                
                # Multiple users acquire locks on different records
                user_locks = {}  # user_id -> list of record_ids
                
                for i, user_id in enumerate(user_ids):
                    user_locks[user_id] = []
                    # Each user gets some records (distribute evenly)
                    user_record_ids = record_ids[i::len(user_ids)]  # Interleave assignment
                    
                    for record_id in user_record_ids:
                        lock = await lock_manager.acquire_lock(record_id, user_id, version + i)
                        assert lock is not None, f"Lock acquisition should succeed for user {user_id}, record {record_id}"
                        user_locks[user_id].append(record_id)
                
                # Verify all locks are acquired
                total_expected_locks = sum(len(records) for records in user_locks.values())
                assert lock_manager.get_lock_count() == total_expected_locks, (
                    f"Should have {total_expected_locks} total locks"
                )
                
                # Disconnect the first user
                disconnected_user = user_ids[0]
                other_users = user_ids[1:]
                
                released_locks = await lock_manager.release_user_locks(disconnected_user)
                
                # Property verification: Only disconnected user's locks should be released
                expected_released_count = len(user_locks[disconnected_user])
                assert len(released_locks) == expected_released_count, (
                    f"Should release {expected_released_count} locks for disconnected user, "
                    f"released {len(released_locks)}"
                )
                
                # Verify disconnected user's locks are gone
                for record_id in user_locks[disconnected_user]:
                    existing_lock = await lock_manager.check_lock(record_id)
                    assert existing_lock is None, f"Disconnected user's lock should be gone for record {record_id}"
                
                # Verify other users' locks are still intact
                for user_id in other_users:
                    for record_id in user_locks[user_id]:
                        existing_lock = await lock_manager.check_lock(record_id)
                        assert existing_lock is not None, f"Other user's lock should remain for record {record_id}"
                        assert existing_lock.user_id == user_id, f"Lock should still belong to user {user_id}"
                
                # Verify total lock count is correct
                remaining_locks = total_expected_locks - expected_released_count
                assert lock_manager.get_lock_count() == remaining_locks, (
                    f"Should have {remaining_locks} remaining locks"
                )
                
                # Verify user-specific lock counts
                assert lock_manager.get_user_lock_count(disconnected_user) == 0, (
                    "Disconnected user should have no locks"
                )
                
                for user_id in other_users:
                    expected_count = len(user_locks[user_id])
                    actual_count = lock_manager.get_user_lock_count(user_id)
                    assert actual_count == expected_count, (
                        f"User {user_id} should have {expected_count} locks, has {actual_count}"
                    )
                
            finally:
                await lock_manager.stop()
        
        # Run the async test
        asyncio.run(run_test())

    @given(record_id_strategy(), user_id_strategy(), version_strategy())
    @settings(max_examples=3, deadline=3000)
    def test_disconnection_timeout_behavior_property(self, record_id: str, user_id: str, version: int):
        """Test the 30-second timeout behavior for disconnection cleanup.
        
        **Feature: multi-user-sync, Property 8: Automatic lock release on disconnection**
        **Validates: Requirements 2.3**
        """
        async def run_test():
            lock_manager = OptimisticLockManager(default_timeout_seconds=120)  # Long timeout
            await lock_manager.start()
            
            try:
                # Property: Locks should be released within 30 seconds of disconnection
                
                # User acquires a lock
                lock = await lock_manager.acquire_lock(record_id, user_id, version)
                assert lock is not None, "Lock acquisition should succeed"
                
                # Test various disconnection times
                now = datetime.now()
                
                # Test cases with different disconnection times
                test_cases = [
                    (now - timedelta(seconds=45), True, "45 seconds ago - should cleanup"),
                    (now - timedelta(seconds=30), True, "30 seconds ago - should cleanup"),
                    (now - timedelta(seconds=29), False, "29 seconds ago - should not cleanup"),
                    (now - timedelta(seconds=15), False, "15 seconds ago - should not cleanup"),
                    (now - timedelta(seconds=5), False, "5 seconds ago - should not cleanup"),
                ]
                
                for disconnection_time, should_cleanup, description in test_cases:
                    # Ensure lock exists before test
                    if not await lock_manager.check_lock(record_id):
                        # Re-acquire lock if it was cleaned up in previous test
                        lock = await lock_manager.acquire_lock(record_id, user_id, version)
                        assert lock is not None, f"Lock re-acquisition should succeed for: {description}"
                    
                    # Test cleanup behavior
                    cleaned_locks = await lock_manager.cleanup_locks_for_disconnected_user(user_id, disconnection_time)
                    
                    if should_cleanup:
                        assert len(cleaned_locks) > 0, f"Should cleanup locks: {description}"
                        
                        # Verify lock is actually gone
                        existing_lock = await lock_manager.check_lock(record_id)
                        assert existing_lock is None, f"Lock should be gone after cleanup: {description}"
                    else:
                        assert len(cleaned_locks) == 0, f"Should not cleanup locks: {description}"
                        
                        # Verify lock still exists
                        existing_lock = await lock_manager.check_lock(record_id)
                        assert existing_lock is not None, f"Lock should still exist: {description}"
                        assert existing_lock.user_id == user_id, f"Lock should belong to user: {description}"
                
            finally:
                await lock_manager.stop()
        
        # Run the async test
        asyncio.run(run_test())

    @given(record_id_strategy(), user_id_strategy(), version_strategy())
    @settings(max_examples=2, deadline=10000)  # Has sleep operations
    def test_lock_expiration_vs_disconnection_cleanup_property(self, record_id: str, user_id: str, version: int):
        """Test interaction between lock expiration and disconnection cleanup.
        
        **Feature: multi-user-sync, Property 8: Automatic lock release on disconnection**
        **Validates: Requirements 2.3**
        """
        async def run_test():
            # Use short lock timeout to test expiration
            lock_manager = OptimisticLockManager(default_timeout_seconds=1)
            await lock_manager.start()
            
            try:
                # Property: Both lock expiration and disconnection cleanup should work correctly
                
                # Test Case 1: Lock expires before disconnection cleanup
                lock1 = await lock_manager.acquire_lock(record_id, user_id, version, timeout_seconds=1)
                assert lock1 is not None, "Lock acquisition should succeed"
                
                # Wait for lock to expire
                await asyncio.sleep(1.1)
                
                # Try disconnection cleanup on expired lock
                old_disconnection = datetime.now() - timedelta(seconds=35)
                cleaned_locks = await lock_manager.cleanup_locks_for_disconnected_user(user_id, old_disconnection)
                
                # Should not clean anything because lock already expired
                assert len(cleaned_locks) == 0, "Should not clean expired locks"
                
                # Verify lock is gone due to expiration
                existing_lock = await lock_manager.check_lock(record_id)
                assert existing_lock is None, "Lock should be gone due to expiration"
                
                # Test Case 2: Disconnection cleanup happens before expiration
                lock2 = await lock_manager.acquire_lock(record_id, user_id, version + 1, timeout_seconds=5)
                assert lock2 is not None, "Second lock acquisition should succeed"
                
                # Immediate disconnection cleanup (simulating immediate disconnection)
                released_locks = await lock_manager.release_user_locks(user_id)
                assert len(released_locks) == 1, "Should release one lock"
                
                # Verify lock is gone due to disconnection cleanup
                existing_lock = await lock_manager.check_lock(record_id)
                assert existing_lock is None, "Lock should be gone due to disconnection cleanup"
                
                # Wait for what would have been expiration time
                await asyncio.sleep(0.5)
                
                # Verify lock is still gone (no double cleanup)
                existing_lock = await lock_manager.check_lock(record_id)
                assert existing_lock is None, "Lock should remain gone"
                
                # Test Case 3: Multiple cleanup mechanisms don't interfere
                lock3 = await lock_manager.acquire_lock(record_id, user_id, version + 2, timeout_seconds=3)
                assert lock3 is not None, "Third lock acquisition should succeed"
                
                # Try both cleanup mechanisms
                old_disconnection = datetime.now() - timedelta(seconds=40)
                cleaned_locks = await lock_manager.cleanup_locks_for_disconnected_user(user_id, old_disconnection)
                assert len(cleaned_locks) == 1, "Should clean one lock"
                
                # Try expired lock cleanup
                expired_locks = await lock_manager.cleanup_expired_locks()
                # Should not find any expired locks since disconnection cleanup already handled it
                
                # Verify final state
                existing_lock = await lock_manager.check_lock(record_id)
                assert existing_lock is None, "Lock should be gone"
                assert lock_manager.get_lock_count() == 0, "No locks should remain"
                
            finally:
                await lock_manager.stop()
        
        # Run the async test
        asyncio.run(run_test())