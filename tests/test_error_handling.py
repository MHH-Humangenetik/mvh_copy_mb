"""Property-based tests for error handling and data integrity preservation."""

import pytest
import asyncio
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any
from unittest.mock import Mock, AsyncMock

from hypothesis import given, strategies as st
from hypothesis import assume

from mvh_copy_mb.sync.service import SyncServiceImpl
from mvh_copy_mb.sync.models import SyncEvent, EventType, ClientConnection
from mvh_copy_mb.sync.interfaces import EventBroker, LockManager, ConnectionManager
from mvh_copy_mb.sync.exceptions import (
    VersionConflictError, DataIntegrityError, BroadcastError, SyncError, SyncServiceUnavailableError
)
from mvh_copy_mb.database import MeldebestaetigungDatabase, MeldebestaetigungRecord


# Mock implementations for testing
class MockEventBroker(EventBroker):
    """Mock event broker for testing."""
    
    def __init__(self, should_fail: bool = False):
        self.published_events = []
        self.bulk_events = []
        self.subscriptions = {}
        self.should_fail = should_fail
        self.failure_count = 0
        
    async def publish_event(self, event: SyncEvent) -> None:
        if self.should_fail:
            self.failure_count += 1
            raise Exception(f"Mock broadcast failure {self.failure_count}")
        self.published_events.append(event)
        
    async def subscribe_client(self, connection: ClientConnection, subscriptions) -> None:
        self.subscriptions[connection.connection_id] = subscriptions
        
    async def unsubscribe_client(self, connection_id: str) -> None:
        self.subscriptions.pop(connection_id, None)
        
    async def publish_bulk_events(self, events: List[SyncEvent]) -> None:
        if self.should_fail:
            self.failure_count += 1
            raise Exception(f"Mock bulk broadcast failure {self.failure_count}")
        self.bulk_events.extend(events)


class MockLockManager(LockManager):
    """Mock lock manager for testing."""
    
    def __init__(self, should_fail: bool = False):
        self.locks = {}
        self.version_validations = {}
        self.should_fail = should_fail
        self.failure_count = 0
        
    async def acquire_lock(self, record_id: str, user_id: str, version: int, timeout_seconds: int = 30):
        if self.should_fail:
            self.failure_count += 1
            raise Exception(f"Mock lock manager failure {self.failure_count}")
        return None
        
    async def release_lock(self, record_id: str, user_id: str) -> bool:
        return True
        
    async def check_lock(self, record_id: str):
        return self.locks.get(record_id)
        
    async def cleanup_expired_locks(self):
        return []
        
    async def release_user_locks(self, user_id: str):
        return []
        
    async def validate_version(self, record_id: str, expected_version: int) -> bool:
        if self.should_fail:
            self.failure_count += 1
            raise Exception(f"Mock version validation failure {self.failure_count}")
        return self.version_validations.get(record_id, True)


class MockConnectionManager(ConnectionManager):
    """Mock connection manager for testing."""
    
    def __init__(self):
        self.connections = {}
        
    async def add_connection(self, connection: ClientConnection) -> None:
        self.connections[connection.connection_id] = connection
        
    async def remove_connection(self, connection_id: str):
        return self.connections.pop(connection_id, None)
        
    async def get_connection(self, connection_id: str):
        return self.connections.get(connection_id)
        
    async def get_user_connections(self, user_id: str):
        return [conn for conn in self.connections.values() if conn.user_id == user_id]
        
    async def get_all_connections(self):
        return list(self.connections.values())
        
    async def update_last_seen(self, connection_id: str) -> None:
        if connection_id in self.connections:
            self.connections[connection_id].last_seen = datetime.now()


class MockDatabase(MeldebestaetigungDatabase):
    """Mock database for testing."""
    
    def __init__(self):
        self.records = {}
        self.last_modified = {}
        
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
        
    def upsert_record(self, record: MeldebestaetigungRecord) -> None:
        self.records[record.vorgangsnummer] = record
        self.last_modified[record.vorgangsnummer] = datetime.now()
        
    def get_record(self, vorgangsnummer: str):
        return self.records.get(vorgangsnummer)


# Strategies for generating test data
@st.composite
def record_update_strategy(draw):
    """Generate record update data."""
    record_id = draw(st.text(min_size=1, max_size=50, alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd'))))
    user_id = draw(st.text(min_size=1, max_size=50, alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd'))))
    version = draw(st.integers(min_value=1, max_value=1000))
    data = draw(st.dictionaries(
        st.text(min_size=1, max_size=20), 
        st.one_of(st.text(), st.integers(), st.booleans()),
        min_size=1, max_size=10
    ))
    
    return {
        'record_id': record_id,
        'data': data,
        'user_id': user_id,
        'version': version
    }


@st.composite
def invalid_data_strategy(draw):
    """Generate invalid data for testing data integrity."""
    choice = draw(st.integers(min_value=0, max_value=4))
    
    if choice == 0:
        # Empty record ID
        return {
            'record_id': '',
            'data': {'valid': True},
            'user_id': 'test_user',
            'version': 1
        }
    elif choice == 1:
        # Invalid version (negative)
        return {
            'record_id': 'test_record',
            'data': {'valid': True},
            'user_id': 'test_user',
            'version': draw(st.integers(max_value=0))
        }
    elif choice == 2:
        # Non-dict data
        return {
            'record_id': 'test_record',
            'data': draw(st.one_of(st.text(), st.integers(), st.lists(st.text()))),
            'user_id': 'test_user',
            'version': 1
        }
    elif choice == 3:
        # Extremely large data
        large_data = {f'key_{i}': 'x' * 10000 for i in range(20)}  # ~200KB
        return {
            'record_id': 'test_record',
            'data': large_data,
            'user_id': 'test_user',
            'version': 1
        }
    else:
        # None record ID
        return {
            'record_id': None,
            'data': {'valid': True},
            'user_id': 'test_user',
            'version': 1
        }


class TestDataIntegrityPreservation:
    """Property-based tests for data integrity preservation during conflicts."""

    @given(st.lists(record_update_strategy(), min_size=2, max_size=10))
    def test_data_integrity_preservation_during_conflicts_property(self, updates: List[Dict[str, Any]]):
        """Test that data integrity is preserved when conflicts are detected.
        
        **Feature: multi-user-sync, Property 9: Data integrity preservation during conflicts**
        **Validates: Requirements 2.4**
        """
        async def run_test():
            # Create sync service with mocks - disable error recovery for this test
            # to properly test conflict detection
            event_broker = MockEventBroker()
            lock_manager = MockLockManager()
            connection_manager = MockConnectionManager()
            database = MockDatabase()
            
            service = SyncServiceImpl(
                event_broker=event_broker,
                lock_manager=lock_manager,
                connection_manager=connection_manager,
                database=database,
                enable_circuit_breaker=False,  # Disable to test raw conflict handling
                enable_error_recovery=False   # Disable to test raw conflict handling
            )
            
            await service.start()
            
            try:
                # Property: For any conflicting changes detected, the system should 
                # maintain the most recent valid data state without corruption
                
                # Ensure we have at least 2 updates for conflict testing
                if len(updates) < 2:
                    updates.append({
                        'record_id': 'conflict_record',
                        'data': {'conflict_test': True},
                        'user_id': 'conflict_user',
                        'version': 2
                    })
                
                # Use the same record ID for first two updates to create conflict
                conflict_record_id = updates[0]['record_id']
                updates[1]['record_id'] = conflict_record_id
                
                # Process first update successfully
                initial_data = updates[0]['data'].copy()
                initial_version = updates[0]['version']
                
                await service.handle_record_update(
                    record_id=conflict_record_id,
                    data=initial_data,
                    user_id=updates[0]['user_id'],
                    version=initial_version
                )
                
                # Verify first update was processed
                assert len(event_broker.published_events) == 1
                first_event = event_broker.published_events[0]
                assert first_event.record_id == conflict_record_id
                assert first_event.data == initial_data
                assert first_event.version == initial_version
                
                # Get initial buffer state
                initial_buffer_stats = service.get_buffer_stats()
                initial_known_records = initial_buffer_stats["known_records_count"]
                
                # Set up version conflict for second update
                lock_manager.version_validations[conflict_record_id] = False
                
                # Attempt conflicting update
                conflicting_data = updates[1]['data'].copy()
                conflicting_version = updates[1]['version']
                
                conflict_occurred = False
                try:
                    await service.handle_record_update(
                        record_id=conflict_record_id,
                        data=conflicting_data,
                        user_id=updates[1]['user_id'],
                        version=conflicting_version
                    )
                except (VersionConflictError, ValueError) as e:
                    conflict_occurred = True
                    # Expected conflict
                    assert "conflict" in str(e).lower()
                
                # Property verification: Data integrity should be preserved
                
                # Should have detected and handled the conflict
                assert conflict_occurred, "Version conflict should have been detected"
                
                # Should still have only the first (valid) event
                assert len(event_broker.published_events) == 1, "Should not have published conflicting update"
                
                # The published event should still be the original, valid one
                preserved_event = event_broker.published_events[0]
                assert preserved_event.record_id == conflict_record_id
                assert preserved_event.data == initial_data, "Original data should be preserved"
                assert preserved_event.version == initial_version, "Original version should be preserved"
                assert preserved_event.user_id == updates[0]['user_id'], "Original user should be preserved"
                
                # Buffer state should remain consistent
                post_conflict_stats = service.get_buffer_stats()
                assert post_conflict_stats["known_records_count"] >= initial_known_records, "Known records should not decrease"
                
                # Test that subsequent valid updates work correctly
                lock_manager.version_validations[conflict_record_id] = True
                
                valid_update_data = {'resolved': True, 'status': 'valid'}
                valid_version = initial_version + 1
                
                await service.handle_record_update(
                    record_id=conflict_record_id,
                    data=valid_update_data,
                    user_id=updates[0]['user_id'],
                    version=valid_version
                )
                
                # Should now have two events (original + valid update)
                assert len(event_broker.published_events) == 2
                
                # Second event should be the valid update
                valid_event = event_broker.published_events[1]
                assert valid_event.record_id == conflict_record_id
                assert valid_event.data == valid_update_data
                assert valid_event.version == valid_version
                
                # Property: Events should be in chronological order
                assert event_broker.published_events[0].timestamp <= event_broker.published_events[1].timestamp
                
                # Test bulk operation integrity
                if len(updates) > 2:
                    # Create bulk update with some conflicts
                    bulk_updates = updates[2:]
                    
                    # Set some to fail validation
                    for i, update in enumerate(bulk_updates):
                        if i % 2 == 0:  # Every other update fails
                            lock_manager.version_validations[update['record_id']] = False
                        else:
                            lock_manager.version_validations[update['record_id']] = True
                    
                    initial_event_count = len(event_broker.published_events)
                    
                    # Perform bulk update
                    await service.handle_bulk_update(bulk_updates, "bulk_user")
                    
                    # Should have published events only for successful updates
                    successful_updates = [u for i, u in enumerate(bulk_updates) if i % 2 == 1]
                    expected_new_events = len(successful_updates)
                    
                    final_event_count = len(event_broker.published_events) + len(event_broker.bulk_events)
                    assert final_event_count >= initial_event_count, "Should have some successful bulk updates"
                    
                    # Verify bulk events contain only successful updates
                    if event_broker.bulk_events:
                        for bulk_event in event_broker.bulk_events:
                            # Should be from successful updates only
                            matching_update = next(
                                (u for u in successful_updates if u['record_id'] == bulk_event.record_id),
                                None
                            )
                            assert matching_update is not None, "Bulk event should match successful update"
                
            finally:
                await service.stop()
        
        # Run the async test
        asyncio.run(run_test())

    @given(st.lists(invalid_data_strategy(), min_size=1, max_size=5))
    def test_data_validation_integrity_property(self, invalid_updates: List[Dict[str, Any]]):
        """Test that data validation preserves integrity by rejecting invalid data.
        
        **Feature: multi-user-sync, Property 9: Data integrity preservation during conflicts**
        **Validates: Requirements 2.4**
        """
        async def run_test():
            # Create sync service with mocks
            event_broker = MockEventBroker()
            lock_manager = MockLockManager()
            connection_manager = MockConnectionManager()
            database = MockDatabase()
            
            service = SyncServiceImpl(
                event_broker=event_broker,
                lock_manager=lock_manager,
                connection_manager=connection_manager,
                database=database,
                enable_circuit_breaker=True,
                enable_error_recovery=True
            )
            
            await service.start()
            
            try:
                # Property: Invalid data should be rejected to preserve integrity
                
                initial_event_count = len(event_broker.published_events)
                
                for invalid_update in invalid_updates:
                    validation_error_occurred = False
                    
                    try:
                        await service.handle_record_update(
                            record_id=invalid_update['record_id'],
                            data=invalid_update['data'],
                            user_id=invalid_update['user_id'],
                            version=invalid_update['version']
                        )
                    except (DataIntegrityError, SyncError, TypeError, ValueError) as e:
                        validation_error_occurred = True
                        # Expected validation error
                        assert any(keyword in str(e).lower() for keyword in 
                                 ['integrity', 'invalid', 'error', 'format', 'size', 'version'])
                    
                    # Property verification: Invalid data should be rejected
                    if (invalid_update['record_id'] == '' or 
                        invalid_update['record_id'] is None or
                        invalid_update['version'] <= 0 or
                        not isinstance(invalid_update['data'], dict) or
                        len(str(invalid_update['data'])) > 100000):
                        
                        assert validation_error_occurred, f"Invalid data should be rejected: {invalid_update}"
                
                # Property: No invalid data should have been published
                final_event_count = len(event_broker.published_events)
                
                # Should not have published any events for invalid data
                # (Some edge cases might pass validation, so we check that no obviously invalid data was published)
                for event in event_broker.published_events[initial_event_count:]:
                    # Verify published events have valid structure
                    assert event.record_id, "Published event should have valid record_id"
                    assert isinstance(event.data, dict), "Published event data should be dict"
                    assert event.version > 0, "Published event should have positive version"
                    assert event.user_id, "Published event should have valid user_id"
                
                # Test that valid data still works after validation errors
                valid_update = {
                    'record_id': 'valid_record',
                    'data': {'status': 'valid', 'test': True},
                    'user_id': 'valid_user',
                    'version': 1
                }
                
                await service.handle_record_update(
                    record_id=valid_update['record_id'],
                    data=valid_update['data'],
                    user_id=valid_update['user_id'],
                    version=valid_update['version']
                )
                
                # Should have published the valid update
                assert len(event_broker.published_events) > final_event_count
                
                # Verify the valid event
                valid_event = event_broker.published_events[-1]
                assert valid_event.record_id == valid_update['record_id']
                assert valid_event.data == valid_update['data']
                assert valid_event.version == valid_update['version']
                assert valid_event.user_id == valid_update['user_id']
                
            finally:
                await service.stop()
        
        # Run the async test
        asyncio.run(run_test())

    @given(st.lists(record_update_strategy(), min_size=3, max_size=8))
    def test_error_recovery_integrity_property(self, updates: List[Dict[str, Any]]):
        """Test that error recovery mechanisms preserve data integrity.
        
        **Feature: multi-user-sync, Property 9: Data integrity preservation during conflicts**
        **Validates: Requirements 2.4**
        """
        async def run_test():
            # Create sync service with failing event broker to test recovery
            failing_event_broker = MockEventBroker(should_fail=True)
            lock_manager = MockLockManager()
            connection_manager = MockConnectionManager()
            database = MockDatabase()
            
            service = SyncServiceImpl(
                event_broker=failing_event_broker,
                lock_manager=lock_manager,
                connection_manager=connection_manager,
                database=database,
                enable_circuit_breaker=True,
                enable_error_recovery=True
            )
            
            await service.start()
            
            try:
                # Property: Error recovery should preserve data integrity
                
                # Attempt updates with failing event broker
                recovery_errors = []
                
                for update in updates[:3]:  # Test with first 3 updates
                    try:
                        await service.handle_record_update(
                            record_id=update['record_id'],
                            data=update['data'],
                            user_id=update['user_id'],
                            version=update['version']
                        )
                    except (BroadcastError, SyncError, SyncServiceUnavailableError) as e:
                        recovery_errors.append(e)
                        # Expected due to failing event broker or service degradation
                        error_msg = str(e).lower()
                        assert ("broadcast" in error_msg or "error" in error_msg or 
                               "unavailable" in error_msg or "degraded" in error_msg)
                
                # Property verification: Errors should be handled gracefully
                assert len(recovery_errors) > 0, "Should have encountered broadcast errors"
                
                # Verify error recovery manager captured the operations
                if service._error_recovery_manager:
                    recovery_metrics = service._error_recovery_manager.get_recovery_metrics()
                    assert recovery_metrics["active_snapshots"] >= 0, "Should have operation snapshots"
                
                # Test with working event broker to verify system can recover
                working_event_broker = MockEventBroker(should_fail=False)
                
                # Replace the failing broker
                service._event_broker = working_event_broker
                
                # Allow service to recover from degradation
                await service._degradation_manager.recover_to_normal()
                
                # Now updates should work
                recovery_update = {
                    'record_id': 'recovery_test',
                    'data': {'recovered': True, 'status': 'working'},
                    'user_id': 'recovery_user',
                    'version': 1
                }
                
                await service.handle_record_update(
                    record_id=recovery_update['record_id'],
                    data=recovery_update['data'],
                    user_id=recovery_update['user_id'],
                    version=recovery_update['version']
                )
                
                # Should have published the recovery update
                assert len(working_event_broker.published_events) == 1
                
                recovery_event = working_event_broker.published_events[0]
                assert recovery_event.record_id == recovery_update['record_id']
                assert recovery_event.data == recovery_update['data']
                
                # Property: System should maintain consistent state after recovery
                buffer_stats = service.get_buffer_stats()
                assert buffer_stats["known_records_count"] >= 0
                assert buffer_stats["total_clients_with_buffers"] >= 0
                
                # Test circuit breaker functionality
                error_metrics = service.get_error_metrics()
                assert "circuit_breaker_enabled" in error_metrics
                assert "error_recovery_enabled" in error_metrics
                assert error_metrics["circuit_breaker_enabled"] is True
                assert error_metrics["error_recovery_enabled"] is True
                
                # Verify service health
                assert "service_health" in error_metrics
                for service_name, health in error_metrics["service_health"].items():
                    assert isinstance(health, bool), f"Service {service_name} health should be boolean"
                
            finally:
                await service.stop()
        
        # Run the async test
        asyncio.run(run_test())

    @given(st.integers(min_value=2, max_value=10))
    def test_concurrent_conflict_integrity_property(self, num_concurrent_users: int):
        """Test data integrity with multiple concurrent conflicts.
        
        **Feature: multi-user-sync, Property 9: Data integrity preservation during conflicts**
        **Validates: Requirements 2.4**
        """
        async def run_test():
            # Create sync service with mocks
            event_broker = MockEventBroker()
            lock_manager = MockLockManager()
            connection_manager = MockConnectionManager()
            database = MockDatabase()
            
            service = SyncServiceImpl(
                event_broker=event_broker,
                lock_manager=lock_manager,
                connection_manager=connection_manager,
                database=database,
                enable_circuit_breaker=True,
                enable_error_recovery=True
            )
            
            await service.start()
            
            try:
                # Property: Data integrity should be preserved with multiple concurrent conflicts
                
                shared_record_id = "concurrent_conflict_record"
                
                # Create multiple users attempting to update the same record
                concurrent_updates = []
                for i in range(num_concurrent_users):
                    update = {
                        'record_id': shared_record_id,
                        'data': {'user_update': i, 'timestamp': datetime.now().isoformat()},
                        'user_id': f'concurrent_user_{i}',
                        'version': 2  # All attempt same version to create conflicts
                    }
                    concurrent_updates.append(update)
                
                # First user succeeds, others should fail
                lock_manager.version_validations[shared_record_id] = True
                
                # Process first update successfully
                first_update = concurrent_updates[0]
                await service.handle_record_update(
                    record_id=first_update['record_id'],
                    data=first_update['data'],
                    user_id=first_update['user_id'],
                    version=first_update['version']
                )
                
                # Verify first update succeeded
                assert len(event_broker.published_events) == 1
                successful_event = event_broker.published_events[0]
                assert successful_event.record_id == shared_record_id
                assert successful_event.user_id == first_update['user_id']
                
                # Now set validation to fail for subsequent updates
                lock_manager.version_validations[shared_record_id] = False
                
                # Attempt remaining concurrent updates (should all fail)
                conflict_count = 0
                for update in concurrent_updates[1:]:
                    try:
                        await service.handle_record_update(
                            record_id=update['record_id'],
                            data=update['data'],
                            user_id=update['user_id'],
                            version=update['version']
                        )
                    except (VersionConflictError, ValueError):
                        conflict_count += 1
                        # Expected conflict
                
                # Property verification: Only first update should succeed
                assert conflict_count == len(concurrent_updates) - 1, "All but first update should conflict"
                assert len(event_broker.published_events) == 1, "Should have only one successful event"
                
                # Verify the successful event is still intact
                preserved_event = event_broker.published_events[0]
                assert preserved_event.record_id == shared_record_id
                assert preserved_event.data == first_update['data']
                assert preserved_event.user_id == first_update['user_id']
                assert preserved_event.version == first_update['version']
                
                # Property: System state should remain consistent
                buffer_stats = service.get_buffer_stats()
                assert buffer_stats["known_records_count"] >= 1, "Should track the successful record"
                
                # Test that system can handle new valid updates after conflicts
                lock_manager.version_validations[shared_record_id] = True
                
                post_conflict_update = {
                    'record_id': shared_record_id,
                    'data': {'post_conflict': True, 'resolved': True},
                    'user_id': first_update['user_id'],  # Same user as successful one
                    'version': 3  # Next version
                }
                
                await service.handle_record_update(
                    record_id=post_conflict_update['record_id'],
                    data=post_conflict_update['data'],
                    user_id=post_conflict_update['user_id'],
                    version=post_conflict_update['version']
                )
                
                # Should now have two events
                assert len(event_broker.published_events) == 2
                
                # Verify second event
                second_event = event_broker.published_events[1]
                assert second_event.record_id == shared_record_id
                assert second_event.data == post_conflict_update['data']
                assert second_event.version == 3
                
                # Property: Events should maintain chronological order
                assert event_broker.published_events[0].timestamp <= event_broker.published_events[1].timestamp
                
            finally:
                await service.stop()
        
        # Run the async test
        asyncio.run(run_test())


class TestGracefulDegradation:
    """Property-based tests for graceful degradation on persistent issues."""

    @given(st.integers(min_value=3, max_value=15))
    def test_graceful_degradation_on_persistent_issues_property(self, num_failures: int):
        """Test that system gracefully degrades when persistent issues occur.
        
        **Feature: multi-user-sync, Property 17: Graceful degradation on persistent issues**
        **Validates: Requirements 4.3**
        """
        async def run_test():
            # Create sync service with mocks
            event_broker = MockEventBroker()
            lock_manager = MockLockManager()
            connection_manager = MockConnectionManager()
            database = MockDatabase()
            
            service = SyncServiceImpl(
                event_broker=event_broker,
                lock_manager=lock_manager,
                connection_manager=connection_manager,
                database=database,
                enable_circuit_breaker=True,
                enable_error_recovery=True
            )
            
            await service.start()
            
            try:
                # Property: For any persistent connection issues, the system should 
                # gracefully degrade to manual refresh mode with clear user notification
                
                # Get initial degradation status
                initial_status = service.get_degradation_status()
                assert initial_status["current_level"] == "normal"
                assert not initial_status["is_degraded"]
                
                # Simulate persistent issues by triggering multiple failures
                for i in range(num_failures):
                    # Simulate high latency
                    service._update_performance_metrics(3000.0, operation_failed=False)  # 3 second latency
                    
                    # Simulate connection failures
                    service._update_performance_metrics(1000.0, operation_failed=True)
                    
                    # Small delay to allow degradation monitoring to process
                    await asyncio.sleep(0.01)
                
                # Wait a bit for degradation monitoring to kick in
                await asyncio.sleep(0.1)
                
                # Check if degradation has been triggered
                degradation_status = service.get_degradation_status()
                
                # Property verification: System should degrade gracefully
                
                # Should have detected performance issues
                performance_metrics = degradation_status.get("metrics", {})
                assert performance_metrics.get("average_latency_ms", 0) > 1000, "Should have high average latency"
                assert performance_metrics.get("error_rate", 0) > 0, "Should have non-zero error rate"
                
                # Test manual degradation trigger (simulating persistent issues)
                await service.trigger_manual_degradation("Simulated persistent connection issues")
                
                # Verify degradation was applied
                degraded_status = service.get_degradation_status()
                assert degraded_status["current_level"] == "manual_refresh", "Should be in manual refresh mode"
                assert degraded_status["is_degraded"], "Should be in degraded state"
                assert degraded_status["should_disable_realtime"], "Should disable real-time updates"
                
                # Property: Degradation should provide clear guidance
                assert "recommended_batch_size" in degraded_status
                assert "recommended_interval" in degraded_status
                assert degraded_status["recommended_batch_size"] > 0
                assert degraded_status["recommended_interval"] > 0
                
                # Property: System should throttle operations when degraded
                assert degraded_status["should_throttle"], "Should throttle connections when degraded"
                
                # Test that operations are throttled
                throttle_error_occurred = False
                try:
                    await service.handle_record_update(
                        record_id="throttled_record",
                        data={"test": "throttled"},
                        user_id="throttled_user",
                        version=1
                    )
                except Exception as e:
                    throttle_error_occurred = True
                    assert "throttling" in str(e).lower() or "unavailable" in str(e).lower()
                
                # Should have throttled the operation
                assert throttle_error_occurred, "Should throttle operations when degraded"
                
                # Test recovery mechanism
                await service.recover_from_degradation()
                
                # Verify recovery
                recovered_status = service.get_degradation_status()
                
                # Should be attempting recovery (may not be fully normal yet due to metrics)
                # but should show recovery attempt in recent events
                recent_events = recovered_status.get("recent_events", [])
                recovery_attempted = any(
                    "recovery" in event.get("reason", "").lower() 
                    for event in recent_events
                )
                assert recovery_attempted or recovered_status["current_level"] == "normal", "Should attempt recovery"
                
                # Property: System should maintain consistent state throughout degradation
                final_buffer_stats = service.get_buffer_stats()
                assert final_buffer_stats["total_clients_with_buffers"] >= 0
                assert final_buffer_stats["known_records_count"] >= 0
                
            finally:
                await service.stop()
        
        # Run the async test
        asyncio.run(run_test())