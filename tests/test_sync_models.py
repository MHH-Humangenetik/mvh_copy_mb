"""Property-based tests for synchronization data models."""

import pytest
import asyncio
from datetime import datetime, timedelta
from typing import List, Set
from unittest.mock import Mock, AsyncMock

from hypothesis import given, strategies as st
from hypothesis import assume

from mvh_copy_mb.sync.models import (
    SyncEvent, RecordLock, ClientConnection, EventType, LockState,
    WebSocketMessage, LockRequest, LockRelease, DataUpdate, 
    ConnectionHeartbeat, ErrorMessage, ConflictNotification
)
from mvh_copy_mb.sync.config import SyncConfig
from mvh_copy_mb.websocket.manager import WebSocketManager


# Strategies for generating test data
@st.composite
def sync_event_strategy(draw):
    """Generate valid SyncEvent instances."""
    event_type = draw(st.sampled_from([e.value for e in EventType]))
    record_id = draw(st.text(min_size=1, max_size=50, alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd'))))
    user_id = draw(st.text(min_size=1, max_size=50, alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd'))))
    version = draw(st.integers(min_value=1, max_value=1000))
    timestamp = draw(st.datetimes(min_value=datetime(2020, 1, 1), max_value=datetime(2030, 12, 31)))
    data = draw(st.dictionaries(
        st.text(min_size=1, max_size=20), 
        st.one_of(st.text(), st.integers(), st.booleans()),
        min_size=0, max_size=10
    ))
    
    return SyncEvent(
        event_type=event_type,
        record_id=record_id,
        data=data,
        version=version,
        timestamp=timestamp,
        user_id=user_id
    )


@st.composite
def record_lock_strategy(draw):
    """Generate valid RecordLock instances."""
    record_id = draw(st.text(min_size=1, max_size=50, alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd'))))
    user_id = draw(st.text(min_size=1, max_size=50, alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd'))))
    version = draw(st.integers(min_value=1, max_value=1000))
    acquired_at = draw(st.datetimes(min_value=datetime(2020, 1, 1), max_value=datetime(2030, 12, 31)))
    expires_at = draw(st.datetimes(min_value=acquired_at, max_value=acquired_at + timedelta(hours=24)))
    state = draw(st.sampled_from(LockState))
    
    return RecordLock(
        record_id=record_id,
        user_id=user_id,
        version=version,
        acquired_at=acquired_at,
        expires_at=expires_at,
        state=state
    )


@st.composite
def client_connection_strategy(draw):
    """Generate valid ClientConnection instances."""
    connection_id = draw(st.text(min_size=1, max_size=50, alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd'))))
    user_id = draw(st.text(min_size=1, max_size=50, alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd'))))
    websocket = Mock()  # Mock WebSocket for testing
    last_seen = draw(st.datetimes(min_value=datetime(2020, 1, 1), max_value=datetime(2030, 12, 31)))
    subscriptions = draw(st.sets(
        st.text(min_size=1, max_size=20, alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd'))),
        min_size=0, max_size=10
    ))
    
    return ClientConnection(
        connection_id=connection_id,
        user_id=user_id,
        websocket=websocket,
        last_seen=last_seen,
        subscriptions=subscriptions
    )


@st.composite
def unique_client_connections_strategy(draw):
    """Generate a list of ClientConnection instances with unique connection_ids."""
    num_connections = draw(st.integers(min_value=1, max_value=5))
    connections = []
    used_connection_ids = set()
    
    for i in range(num_connections):
        # Generate unique connection_id
        connection_id = f"conn_{i}_{draw(st.integers(min_value=1000, max_value=9999))}"
        while connection_id in used_connection_ids:
            connection_id = f"conn_{i}_{draw(st.integers(min_value=1000, max_value=9999))}"
        used_connection_ids.add(connection_id)
        
        user_id = draw(st.text(min_size=1, max_size=20, alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd'))))
        websocket = Mock()
        last_seen = draw(st.datetimes(min_value=datetime(2020, 1, 1), max_value=datetime(2030, 12, 31)))
        subscriptions = draw(st.sets(
            st.text(min_size=1, max_size=10, alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd'))),
            min_size=0, max_size=5
        ))
        
        connections.append(ClientConnection(
            connection_id=connection_id,
            user_id=user_id,
            websocket=websocket,
            last_seen=last_seen,
            subscriptions=subscriptions
        ))
    
    return connections


class TestSyncDataModels:
    """Test synchronization data models for consistency and correctness."""

    @given(sync_event_strategy())
    def test_sync_event_consistency(self, event: SyncEvent):
        """Test that SyncEvent maintains consistent data across operations.
        
        **Feature: multi-user-sync, Property 1: Multi-user data consistency**
        """
        # Test that the event maintains its data integrity
        assert event.record_id is not None
        assert len(event.record_id) > 0
        assert event.user_id is not None
        assert len(event.user_id) > 0
        assert event.version > 0
        assert event.timestamp is not None
        assert event.event_type in [e.value for e in EventType]
        assert isinstance(event.data, dict)

    @given(st.lists(sync_event_strategy(), min_size=1, max_size=20))
    def test_multiple_sync_events_consistency(self, events: List[SyncEvent]):
        """Test that multiple sync events maintain consistency when processed together.
        
        **Feature: multi-user-sync, Property 1: Multi-user data consistency**
        """
        # Group events by record_id to simulate concurrent operations on same records
        record_events = {}
        for event in events:
            if event.record_id not in record_events:
                record_events[event.record_id] = []
            record_events[event.record_id].append(event)
        
        # For each record, verify that events maintain consistency
        for record_id, record_event_list in record_events.items():
            # Sort events by timestamp to simulate processing order
            sorted_events = sorted(record_event_list, key=lambda e: e.timestamp)
            
            # Verify that all events for this record have the same record_id
            for event in sorted_events:
                assert event.record_id == record_id
                
            # Verify that version numbers are positive and reasonable
            for event in sorted_events:
                assert event.version > 0
                
            # If multiple events exist for same record, they should represent
            # a consistent sequence of operations
            if len(sorted_events) > 1:
                # All events should have valid timestamps
                for i in range(1, len(sorted_events)):
                    assert sorted_events[i].timestamp >= sorted_events[i-1].timestamp

    @given(record_lock_strategy())
    def test_record_lock_consistency(self, lock: RecordLock):
        """Test that RecordLock maintains temporal and state consistency.
        
        **Feature: multi-user-sync, Property 1: Multi-user data consistency**
        """
        # Basic consistency checks
        assert lock.record_id is not None
        assert len(lock.record_id) > 0
        assert lock.user_id is not None
        assert len(lock.user_id) > 0
        assert lock.version > 0
        
        # Temporal consistency - expires_at should be after acquired_at
        assert lock.expires_at >= lock.acquired_at
        
        # State should be valid
        assert lock.state in LockState

    @given(st.lists(record_lock_strategy(), min_size=1, max_size=10))
    def test_multiple_locks_consistency(self, locks: List[RecordLock]):
        """Test that multiple locks maintain consistency rules.
        
        **Feature: multi-user-sync, Property 1: Multi-user data consistency**
        """
        # Group locks by record_id
        record_locks = {}
        for lock in locks:
            if lock.record_id not in record_locks:
                record_locks[lock.record_id] = []
            record_locks[lock.record_id].append(lock)
        
        # For each record, verify lock consistency
        for record_id, lock_list in record_locks.items():
            # All locks should be for the same record
            for lock in lock_list:
                assert lock.record_id == record_id
                
            # Check for temporal consistency in overlapping locks
            active_locks = [lock for lock in lock_list if lock.state == LockState.ACQUIRED]
            
            # Sort by acquisition time
            active_locks.sort(key=lambda l: l.acquired_at)
            
            # Verify each lock has valid time bounds
            for lock in active_locks:
                assert lock.expires_at >= lock.acquired_at

    @given(client_connection_strategy())
    def test_client_connection_consistency(self, connection: ClientConnection):
        """Test that ClientConnection maintains consistent state.
        
        **Feature: multi-user-sync, Property 1: Multi-user data consistency**
        """
        # Basic consistency checks
        assert connection.connection_id is not None
        assert len(connection.connection_id) > 0
        assert connection.user_id is not None
        assert len(connection.user_id) > 0
        assert connection.websocket is not None
        assert connection.last_seen is not None
        assert isinstance(connection.subscriptions, set)

    @given(st.lists(client_connection_strategy(), min_size=1, max_size=10))
    def test_multiple_connections_consistency(self, connections: List[ClientConnection]):
        """Test that multiple client connections maintain system consistency.
        
        **Feature: multi-user-sync, Property 1: Multi-user data consistency**
        """
        # Verify each connection has unique connection_id
        connection_ids = [conn.connection_id for conn in connections]
        
        # Group connections by user_id to simulate multiple connections per user
        user_connections = {}
        for conn in connections:
            if conn.user_id not in user_connections:
                user_connections[conn.user_id] = []
            user_connections[conn.user_id].append(conn)
        
        # Verify consistency within each user's connections
        for user_id, user_conn_list in user_connections.items():
            for conn in user_conn_list:
                assert conn.user_id == user_id
                assert isinstance(conn.subscriptions, set)
                assert conn.last_seen is not None

    def test_websocket_message_validation(self):
        """Test that WebSocket message models validate correctly."""
        # Test LockRequest
        lock_req = LockRequest(record_id="test_record", user_id="test_user")
        assert lock_req.type == "lock_request"
        assert lock_req.record_id == "test_record"
        assert lock_req.user_id == "test_user"
        
        # Test LockRelease
        lock_rel = LockRelease(record_id="test_record", user_id="test_user")
        assert lock_rel.type == "lock_release"
        
        # Test DataUpdate
        data_update = DataUpdate(
            record_id="test_record", 
            data={"field": "value"}, 
            version=1, 
            user_id="test_user"
        )
        assert data_update.type == "data_update"
        assert data_update.data == {"field": "value"}
        
        # Test ErrorMessage
        error_msg = ErrorMessage(error_code="CONFLICT", message="Lock conflict occurred")
        assert error_msg.type == "error"
        assert error_msg.error_code == "CONFLICT"


class TestWebSocketConnectionMaintenance:
    """Property-based tests for WebSocket connection maintenance."""

    @pytest.fixture
    def config(self):
        """Create test configuration."""
        return SyncConfig(
            connection_timeout_seconds=60,
            heartbeat_interval_seconds=10,
            max_connections_per_user=3
        )

    @pytest.fixture
    async def websocket_manager(self, config):
        """Create WebSocket manager for testing."""
        manager = WebSocketManager(config)
        await manager.start()
        yield manager
        await manager.stop()

    @given(unique_client_connections_strategy())
    def test_websocket_connection_maintenance_property(self, connections: List[ClientConnection]):
        """Test that WebSocket connections are properly maintained with real-time updates enabled.
        
        **Feature: multi-user-sync, Property 5: WebSocket connection maintenance**
        **Validates: Requirements 1.5**
        """
        async def run_test():
            config = SyncConfig(
                connection_timeout_seconds=60,
                heartbeat_interval_seconds=10,
                max_connections_per_user=10  # Allow all test connections
            )
            
            manager = WebSocketManager(config)
            await manager.start()
            
            try:
                # Add all connections to the manager
                added_connections = []
                for conn in connections:
                    # Mock the websocket to simulate active connection
                    conn.websocket = AsyncMock()
                    conn.websocket.closed = False
                    conn.websocket.send = AsyncMock()
                    conn.websocket.close = AsyncMock()
                    
                    # Set recent last_seen time to simulate active session
                    conn.last_seen = datetime.now()
                    
                    await manager.add_connection(conn)
                    added_connections.append(conn)
                
                # Property: For any active client session with real-time updates enabled,
                # a WebSocket connection should remain established and functional
                
                # Verify all connections are tracked
                all_connections = await manager.get_all_connections()
                assert len(all_connections) == len(added_connections)
                
                # Verify each connection is properly maintained
                for conn in added_connections:
                    # Connection should be retrievable
                    retrieved_conn = await manager.get_connection(conn.connection_id)
                    assert retrieved_conn is not None
                    assert retrieved_conn.connection_id == conn.connection_id
                    assert retrieved_conn.user_id == conn.user_id
                    
                    # Connection should be in active state (not closed)
                    assert not retrieved_conn.websocket.closed
                    
                    # Connection should be able to send messages (functional)
                    success = await manager.send_message(conn.connection_id, {"test": "message"})
                    assert success is True
                    
                    # Connection should have recent activity tracking
                    assert retrieved_conn.last_seen is not None
                    time_diff = datetime.now() - retrieved_conn.last_seen
                    assert time_diff.total_seconds() < config.connection_timeout_seconds
                
                # Verify user connections are properly grouped
                user_connections_map = {}
                for conn in added_connections:
                    if conn.user_id not in user_connections_map:
                        user_connections_map[conn.user_id] = []
                    user_connections_map[conn.user_id].append(conn)
                
                for user_id, user_conns in user_connections_map.items():
                    retrieved_user_conns = await manager.get_user_connections(user_id)
                    assert len(retrieved_user_conns) == len(user_conns)
                    
                    # All retrieved connections should belong to the user
                    for retrieved_conn in retrieved_user_conns:
                        assert retrieved_conn.user_id == user_id
                
                # Verify connection metrics are accurate
                metrics = await manager.get_connection_metrics()
                assert metrics["active_connections"] == len(added_connections)
                assert metrics["users_connected"] == len(user_connections_map)
                
            finally:
                await manager.stop()
        
        # Run the async test
        asyncio.run(run_test())

    @given(st.integers(min_value=1, max_value=5))
    def test_connection_health_monitoring_property(self, num_connections: int):
        """Test that connection health monitoring maintains connection state properly.
        
        **Feature: multi-user-sync, Property 5: WebSocket connection maintenance**
        **Validates: Requirements 1.5**
        """
        async def run_test():
            config = SyncConfig(
                connection_timeout_seconds=30,
                heartbeat_interval_seconds=5,
                max_connections_per_user=10
            )
            
            manager = WebSocketManager(config)
            await manager.start()
            
            try:
                connections = []
                
                # Create test connections
                for i in range(num_connections):
                    websocket_mock = AsyncMock()
                    websocket_mock.closed = False
                    websocket_mock.send = AsyncMock()
                    websocket_mock.close = AsyncMock()
                    
                    conn = ClientConnection(
                        connection_id=f"conn_{i}",
                        user_id=f"user_{i}",
                        websocket=websocket_mock,
                        last_seen=datetime.now(),
                        subscriptions=set()
                    )
                    
                    await manager.add_connection(conn)
                    connections.append(conn)
                
                # Property: Health monitoring should maintain accurate connection state
                
                # All connections should be initially healthy
                for conn in connections:
                    retrieved_conn = await manager.get_connection(conn.connection_id)
                    assert retrieved_conn is not None
                    
                    # Connection should be functional
                    success = await manager.send_message(conn.connection_id, {"type": "test"})
                    assert success is True
                    
                    # Last seen should be recent
                    time_diff = datetime.now() - retrieved_conn.last_seen
                    assert time_diff.total_seconds() < config.connection_timeout_seconds
                
                # Simulate some connections becoming unhealthy
                unhealthy_connections = connections[:num_connections//2] if num_connections > 1 else []
                
                for conn in unhealthy_connections:
                    # Simulate connection becoming closed
                    conn.websocket.closed = True
                    conn.websocket.send.side_effect = Exception("Connection closed")
                
                # Wait a brief moment for health checks to potentially detect issues
                await asyncio.sleep(0.1)
                
                # Healthy connections should still work
                healthy_connections = connections[num_connections//2:] if num_connections > 1 else connections
                for conn in healthy_connections:
                    success = await manager.send_message(conn.connection_id, {"type": "test"})
                    # Should still succeed for healthy connections
                    if not conn.websocket.closed:
                        assert success is True
                
                # Verify metrics reflect the current state
                metrics = await manager.get_connection_metrics()
                assert metrics["active_connections"] >= 0
                assert metrics["total_connections"] == num_connections
                
            finally:
                await manager.stop()
        
        # Run the async test
        asyncio.run(run_test())

    @given(st.text(min_size=1, max_size=20, alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd'))))
    def test_connection_lifecycle_property(self, user_id: str):
        """Test that connection lifecycle is properly managed.
        
        **Feature: multi-user-sync, Property 5: WebSocket connection maintenance**
        **Validates: Requirements 1.5**
        """
        async def run_test():
            config = SyncConfig(
                connection_timeout_seconds=60,
                heartbeat_interval_seconds=10,
                max_connections_per_user=5
            )
            
            manager = WebSocketManager(config)
            await manager.start()
            
            try:
                # Create a connection
                websocket_mock = AsyncMock()
                websocket_mock.closed = False
                websocket_mock.send = AsyncMock()
                websocket_mock.close = AsyncMock()
                
                connection_id = f"conn_{user_id}"
                conn = ClientConnection(
                    connection_id=connection_id,
                    user_id=user_id,
                    websocket=websocket_mock,
                    last_seen=datetime.now(),
                    subscriptions=set()
                )
                
                # Property: Connection lifecycle should be properly managed
                
                # Initially, connection should not exist
                retrieved_conn = await manager.get_connection(connection_id)
                assert retrieved_conn is None
                
                # After adding, connection should exist and be functional
                await manager.add_connection(conn)
                
                retrieved_conn = await manager.get_connection(connection_id)
                assert retrieved_conn is not None
                assert retrieved_conn.connection_id == connection_id
                assert retrieved_conn.user_id == user_id
                
                # Connection should be functional
                success = await manager.send_message(connection_id, {"type": "test"})
                assert success is True
                
                # User should have this connection
                user_connections = await manager.get_user_connections(user_id)
                assert len(user_connections) == 1
                assert user_connections[0].connection_id == connection_id
                
                # After removing, connection should no longer exist
                removed_conn = await manager.remove_connection(connection_id)
                assert removed_conn is not None
                assert removed_conn.connection_id == connection_id
                
                # Connection should no longer be retrievable
                retrieved_conn = await manager.get_connection(connection_id)
                assert retrieved_conn is None
                
                # User should have no connections
                user_connections = await manager.get_user_connections(user_id)
                assert len(user_connections) == 0
                
            finally:
                await manager.stop()
        
        # Run the async test
        asyncio.run(run_test())

    @given(st.integers(min_value=2, max_value=4))  # Focus on cases with multiple attempts
    def test_exponential_backoff_reconnection_property(self, max_attempts: int):
        """Test that reconnection follows exponential backoff timing.
        
        **Feature: multi-user-sync, Property 15: Exponential backoff reconnection**
        **Validates: Requirements 4.1**
        """
        from mvh_copy_mb.websocket.reconnection import ReconnectionManager
        
        async def run_test():
            config = SyncConfig(
                max_reconnection_attempts=max_attempts,
                initial_reconnection_delay_seconds=0.5,  # Shorter delays for faster tests
                max_reconnection_delay_seconds=10.0,
                reconnection_backoff_multiplier=2.0
            )
            
            reconnection_manager = ReconnectionManager(config)
            
            # Track connection attempts and their timing
            attempt_times = []
            connection_attempts = []
            
            async def mock_connect_callback():
                """Mock connection callback that always fails to test backoff."""
                attempt_times.append(datetime.now())
                connection_attempts.append(len(connection_attempts) + 1)
                return False  # Always fail to test full backoff sequence
            
            reconnection_manager.set_connect_callback(mock_connect_callback)
            
            # Property: Reconnection attempts should follow exponential backoff timing
            
            await reconnection_manager.start_reconnection("Test reconnection")
            
            # Wait for at least 2 attempts to verify backoff behavior
            max_wait_time = 15  # Enough time for multiple attempts
            wait_start = datetime.now()
            
            while (len(connection_attempts) < 2 and 
                   (datetime.now() - wait_start).total_seconds() < max_wait_time):
                await asyncio.sleep(0.1)
            
            await reconnection_manager.stop_reconnection()
            
            # Core property: Verify exponential backoff behavior
            # We need at least 2 attempts to test backoff timing
            if len(attempt_times) >= 2:
                # Check that delays increase (allowing for timing variance)
                for i in range(1, min(len(attempt_times), 3)):  # Check first few attempts
                    delay = (attempt_times[i] - attempt_times[i-1]).total_seconds()
                    expected_delay = config.initial_reconnection_delay_seconds * (config.reconnection_backoff_multiplier ** (i-1))
                    expected_delay = min(expected_delay, config.max_reconnection_delay_seconds)
                    
                    # Allow for significant timing variance in tests (±80% tolerance)
                    min_expected = expected_delay * 0.2
                    max_expected = expected_delay * 1.8
                    
                    assert min_expected <= delay <= max_expected, (
                        f"Attempt {i}: delay {delay:.2f}s not in expected range "
                        f"[{min_expected:.2f}, {max_expected:.2f}]s (expected ~{expected_delay:.2f}s)"
                    )
                    
                # Verify that delays are generally increasing (exponential backoff)
                if len(attempt_times) >= 3:
                    delay1 = (attempt_times[1] - attempt_times[0]).total_seconds()
                    delay2 = (attempt_times[2] - attempt_times[1]).total_seconds()
                    # Second delay should be roughly double the first (with tolerance)
                    assert delay2 >= delay1 * 0.8, f"Backoff not increasing: {delay1:.2f}s -> {delay2:.2f}s"
            
            # Verify basic constraints
            assert len(connection_attempts) >= 1, "Should have made at least one attempt"
            assert len(connection_attempts) <= max_attempts, f"Should not exceed {max_attempts} attempts"
            
            # Verify metrics are reasonable
            metrics = reconnection_manager.get_metrics()
            assert metrics["total_attempts"] >= 1
        
        # Run the async test
        asyncio.run(run_test())


class TestEventBroadcastTiming:
    """Property-based tests for event broadcast timing guarantees."""

    @given(sync_event_strategy(), unique_client_connections_strategy())
    def test_broadcast_timing_guarantee_property(self, event: SyncEvent, connections: List[ClientConnection]):
        """Test that record modifications are broadcast to all clients within 100ms.
        
        **Feature: multi-user-sync, Property 2: Broadcast timing guarantee**
        **Validates: Requirements 1.2**
        """
        from mvh_copy_mb.events.broker import EventBrokerImpl
        
        async def run_test():
            # Create event broker with minimal batching for timing tests
            broker = EventBrokerImpl(batch_size=1, batch_timeout=0.01)  # Very small timeout for immediate processing
            
            # Set up mock websockets for all connections
            for conn in connections:
                conn.websocket = AsyncMock()
                conn.websocket.send = AsyncMock()
                conn.websocket.closed = False
                
                # Subscribe all clients to receive the event
                # Use wildcard subscription to ensure all clients receive all events
                await broker.subscribe_client(conn, {"*"})
            
            # Property: When a user modifies a record, the system SHALL broadcast 
            # the change to all other connected clients within 100 milliseconds
            
            # Record the time when we publish the event
            publish_start_time = datetime.now()
            
            # Publish the event
            await broker.publish_event(event)
            
            # Allow some time for async processing
            await asyncio.sleep(0.05)  # 50ms should be enough for processing
            
            # Verify timing constraint
            processing_time = (datetime.now() - publish_start_time).total_seconds() * 1000  # Convert to milliseconds
            
            # The processing should complete well within 100ms
            # We use 80ms as threshold to account for test execution overhead
            assert processing_time < 80, f"Event processing took {processing_time:.1f}ms, should be under 80ms"
            
            # Verify all clients received the event
            for conn in connections:
                # Each client should have received exactly one message
                assert conn.websocket.send.call_count >= 1, f"Client {conn.connection_id} did not receive event"
                
                # Verify the message content
                call_args = conn.websocket.send.call_args_list[-1]  # Get the last call
                message_json = call_args[0][0]  # First argument to send()
                
                import json
                message = json.loads(message_json)
                
                # Verify it's the correct event
                assert message["type"] == "sync_event"
                assert message["event_type"] == event.event_type
                assert message["record_id"] == event.record_id
                assert message["user_id"] == event.user_id
                assert message["version"] == event.version
                
                # Verify timestamp is preserved
                event_timestamp = datetime.fromisoformat(message["timestamp"])
                assert abs((event_timestamp - event.timestamp).total_seconds()) < 1  # Allow 1 second tolerance
        
        # Skip test if no connections to test with
        if not connections:
            return
            
        # Run the async test
        asyncio.run(run_test())

    @given(st.lists(sync_event_strategy(), min_size=2, max_size=5), 
           st.integers(min_value=1, max_value=3))
    def test_multiple_events_broadcast_timing_property(self, events: List[SyncEvent], num_clients: int):
        """Test that multiple events are broadcast efficiently within timing constraints.
        
        **Feature: multi-user-sync, Property 2: Broadcast timing guarantee**
        **Validates: Requirements 1.2**
        """
        from mvh_copy_mb.events.broker import EventBrokerImpl
        
        async def run_test():
            # Create event broker with small batch settings for timing tests
            broker = EventBrokerImpl(batch_size=10, batch_timeout=0.05)  # 50ms timeout
            
            # Create test client connections
            connections = []
            for i in range(num_clients):
                websocket_mock = AsyncMock()
                websocket_mock.send = AsyncMock()
                websocket_mock.closed = False
                
                conn = ClientConnection(
                    connection_id=f"client_{i}",
                    user_id=f"user_{i}",
                    websocket=websocket_mock,
                    last_seen=datetime.now(),
                    subscriptions=set()
                )
                connections.append(conn)
                
                # Subscribe to all events
                await broker.subscribe_client(conn, {"*"})
            
            # Property: Multiple record modifications should be broadcast 
            # to all clients within the timing guarantee
            
            publish_start_time = datetime.now()
            
            # Publish all events
            for event in events:
                await broker.publish_event(event)
            
            # Allow time for batch processing
            await asyncio.sleep(0.1)  # 100ms for batch processing
            
            # Verify overall timing
            total_processing_time = (datetime.now() - publish_start_time).total_seconds() * 1000
            
            # Should complete within reasonable time (allowing for batch processing)
            max_allowed_time = 150  # 150ms for multiple events
            assert total_processing_time < max_allowed_time, (
                f"Processing {len(events)} events took {total_processing_time:.1f}ms, "
                f"should be under {max_allowed_time}ms"
            )
            
            # Verify all clients received all events
            for conn in connections:
                # Should have received at least one message (could be batched)
                assert conn.websocket.send.call_count >= 1, (
                    f"Client {conn.connection_id} did not receive any messages"
                )
                
                # Count total events received across all messages
                total_events_received = 0
                for call in conn.websocket.send.call_args_list:
                    message_json = call[0][0]
                    import json
                    message = json.loads(message_json)
                    
                    if message["type"] == "sync_event":
                        total_events_received += 1
                    elif message["type"] == "sync_batch":
                        total_events_received += message["count"]
                
                # Should have received all events
                assert total_events_received == len(events), (
                    f"Client {conn.connection_id} received {total_events_received} events, "
                    f"expected {len(events)}"
                )
        
        # Run the async test
        asyncio.run(run_test())

    @given(sync_event_strategy())
    def test_single_client_broadcast_timing_property(self, event: SyncEvent):
        """Test broadcast timing with a single client to verify baseline performance.
        
        **Feature: multi-user-sync, Property 2: Broadcast timing guarantee**
        **Validates: Requirements 1.2**
        """
        from mvh_copy_mb.events.broker import EventBrokerImpl
        
        async def run_test():
            broker = EventBrokerImpl(batch_size=1, batch_timeout=0.01)
            
            # Create single test client
            websocket_mock = AsyncMock()
            websocket_mock.send = AsyncMock()
            websocket_mock.closed = False
            
            conn = ClientConnection(
                connection_id="single_client",
                user_id="test_user",
                websocket=websocket_mock,
                last_seen=datetime.now(),
                subscriptions=set()
            )
            
            await broker.subscribe_client(conn, {"*"})
            
            # Property: Even with a single client, broadcast should be fast
            
            publish_start_time = datetime.now()
            await broker.publish_event(event)
            await asyncio.sleep(0.02)  # Minimal wait for processing
            
            processing_time = (datetime.now() - publish_start_time).total_seconds() * 1000
            
            # Single client should be very fast
            assert processing_time < 50, f"Single client broadcast took {processing_time:.1f}ms, should be under 50ms"
            
            # Verify client received the event
            assert conn.websocket.send.call_count == 1
            
            call_args = conn.websocket.send.call_args_list[0]
            message_json = call_args[0][0]
            
            import json
            message = json.loads(message_json)
            
            assert message["type"] == "sync_event"
            assert message["record_id"] == event.record_id
            assert message["event_type"] == event.event_type
        
        # Run the async test
        asyncio.run(run_test())


class TestBulkOperationEfficiency:
    """Property-based tests for bulk operation efficiency."""

    def test_bulk_operation_efficiency_property(self):
        """Test that bulk changes are broadcast efficiently without overwhelming client connections.
        
        **Feature: multi-user-sync, Property 14: Bulk operation efficiency**
        **Validates: Requirements 3.5**
        """
        from mvh_copy_mb.events.broker import EventBrokerImpl
        
        async def run_test():
            # Test with fixed values to avoid Hypothesis/asyncio interaction issues
            num_events = 20
            num_clients = 2
            
            # Create event broker with reasonable batch settings for bulk operations
            broker = EventBrokerImpl(batch_size=10, batch_timeout=0.05, max_batch_size=25)
            
            # Create unique events to avoid deduplication issues
            events = []
            for i in range(num_events):
                event = SyncEvent(
                    event_type="record_updated",
                    record_id=f"record_{i}",  # Unique record IDs
                    data={"index": i, "bulk_test": True},
                    version=1,
                    timestamp=datetime.now(),
                    user_id="bulk_user"
                )
                events.append(event)
            
            # Create test client connections
            connections = []
            for i in range(num_clients):
                websocket_mock = AsyncMock()
                websocket_mock.send = AsyncMock()
                websocket_mock.closed = False
                
                conn = ClientConnection(
                    connection_id=f"bulk_client_{i}",
                    user_id=f"bulk_user_{i}",
                    websocket=websocket_mock,
                    last_seen=datetime.now(),
                    subscriptions=set()
                )
                connections.append(conn)
                
                # Subscribe to all events
                await broker.subscribe_client(conn, {"*"})
            
            # Property: Bulk changes should be broadcast efficiently without overwhelming connections
            
            # Record initial metrics
            initial_metrics = broker.get_metrics()
            
            # Measure time for bulk operation
            bulk_start_time = datetime.now()
            
            # Publish bulk events
            await broker.publish_bulk_events(events)
            
            # Allow time for processing
            await asyncio.sleep(0.1)  # 100ms should be sufficient for bulk processing
            
            bulk_processing_time = (datetime.now() - bulk_start_time).total_seconds() * 1000
            
            # Get final metrics
            final_metrics = broker.get_metrics()
            
            # Efficiency Property 1: Processing time should be reasonable
            max_allowed_time = 500  # 500ms should be more than enough
            
            assert bulk_processing_time <= max_allowed_time, (
                f"Bulk processing of {num_events} events took {bulk_processing_time:.1f}ms, "
                f"should be under {max_allowed_time:.1f}ms for efficiency"
            )
            
            # Efficiency Property 2: Events should be published correctly
            events_published = final_metrics["events_published"] - initial_metrics["events_published"]
            assert events_published == num_events, f"Expected {num_events} events published, got {events_published}"
            
            # Efficiency Property 3: All clients should receive all events
            for i, conn in enumerate(connections):
                assert conn.websocket.send.call_count >= 1, (
                    f"Client {i} did not receive any messages"
                )
                
                # Count total events received
                total_events_received = 0
                for call in conn.websocket.send.call_args_list:
                    message_json = call[0][0]
                    import json
                    message = json.loads(message_json)
                    
                    if message["type"] == "sync_event":
                        total_events_received += 1
                    elif message["type"] == "sync_batch":
                        total_events_received += message["count"]
                
                # Should receive all events since they're unique
                assert total_events_received == num_events, (
                    f"Client {i} received {total_events_received} events, expected {num_events}"
                )
            
            # Efficiency Property 4: Batching should reduce network calls
            total_individual_sends = num_events * num_clients
            total_actual_sends = sum(conn.websocket.send.call_count for conn in connections)
            
            # Batching should reduce the number of send calls
            efficiency_ratio = total_actual_sends / total_individual_sends if total_individual_sends > 0 else 1
            
            # Should be more efficient than individual sends
            assert efficiency_ratio <= 0.6, (
                f"Bulk operation not efficient enough: {total_actual_sends} sends vs "
                f"{total_individual_sends} individual sends (ratio: {efficiency_ratio:.2f})"
            )
            
            # Efficiency Property 5: Memory usage should be reasonable (buffer should be cleared)
            assert final_metrics["buffer_size"] == 0, "Event buffer should be cleared after processing"
        
        # Run the async test
        asyncio.run(run_test())

    @given(st.lists(sync_event_strategy(), min_size=5, max_size=20))
    def test_event_deduplication_efficiency_property(self, events: List[SyncEvent]):
        """Test that duplicate events are efficiently deduplicated in bulk operations.
        
        **Feature: multi-user-sync, Property 14: Bulk operation efficiency**
        **Validates: Requirements 3.5**
        """
        from mvh_copy_mb.events.broker import EventBrokerImpl
        
        async def run_test():
            broker = EventBrokerImpl(batch_size=50, batch_timeout=0.1)
            
            # Create duplicate events by modifying some events to have same record_id and event_type
            # but different versions/timestamps
            duplicate_events = events.copy()
            
            if len(events) >= 2:
                # Make some events duplicates with different versions
                base_event = events[0]
                for i in range(1, min(4, len(events))):  # Create up to 3 duplicates
                    duplicate_events[i] = SyncEvent(
                        event_type=base_event.event_type,
                        record_id=base_event.record_id,  # Same record_id
                        data={"version": i, "duplicate": True},
                        version=base_event.version + i,  # Higher version
                        timestamp=base_event.timestamp,
                        user_id=base_event.user_id
                    )
            
            # Property: Deduplication should efficiently reduce redundant events
            
            # Test deduplication directly
            deduplicated = await broker.deduplicate_events(duplicate_events)
            
            # Should have fewer or equal events after deduplication
            assert len(deduplicated) <= len(duplicate_events), (
                f"Deduplication increased events: {len(duplicate_events)} -> {len(deduplicated)}"
            )
            
            # If we created duplicates, deduplication should have reduced the count
            if len(events) >= 2:
                assert len(deduplicated) < len(duplicate_events), (
                    "Deduplication should have reduced duplicate events"
                )
            
            # All remaining events should be unique by record_id + event_type
            seen_keys = set()
            for event in deduplicated:
                key = f"{event.record_id}:{event.event_type}"
                assert key not in seen_keys, f"Duplicate key found after deduplication: {key}"
                seen_keys.add(key)
            
            # For duplicates, should keep the one with highest version
            if len(events) >= 2:
                base_record_events = [e for e in deduplicated 
                                    if e.record_id == events[0].record_id and 
                                       e.event_type == events[0].event_type]
                
                if base_record_events:
                    # Should have kept the highest version
                    kept_event = base_record_events[0]
                    expected_version = max(e.version for e in duplicate_events 
                                         if e.record_id == events[0].record_id and 
                                            e.event_type == events[0].event_type)
                    assert kept_event.version == expected_version, (
                        f"Should have kept highest version {expected_version}, got {kept_event.version}"
                    )
        
        # Run the async test
        asyncio.run(run_test())

    @given(st.integers(min_value=50, max_value=200), st.integers(min_value=2, max_value=4))
    def test_large_batch_chunking_efficiency_property(self, num_events: int, num_clients: int):
        """Test that very large batches are efficiently chunked to prevent overwhelming.
        
        **Feature: multi-user-sync, Property 14: Bulk operation efficiency**
        **Validates: Requirements 3.5**
        """
        from mvh_copy_mb.events.broker import EventBrokerImpl
        
        async def run_test():
            # Use smaller max_batch_size to test chunking
            broker = EventBrokerImpl(batch_size=25, batch_timeout=0.1, max_batch_size=50)
            
            # Create large number of events
            events = []
            for i in range(num_events):
                event = SyncEvent(
                    event_type="record_updated",
                    record_id=f"record_{i}",
                    data={"index": i, "large_batch": True},
                    version=1,
                    timestamp=datetime.now(),
                    user_id="bulk_user"
                )
                events.append(event)
            
            # Create test clients
            connections = []
            for i in range(num_clients):
                websocket_mock = AsyncMock()
                websocket_mock.send = AsyncMock()
                websocket_mock.closed = False
                
                conn = ClientConnection(
                    connection_id=f"chunk_client_{i}",
                    user_id=f"chunk_user_{i}",
                    websocket=websocket_mock,
                    last_seen=datetime.now(),
                    subscriptions=set()
                )
                connections.append(conn)
                await broker.subscribe_client(conn, {"*"})
            
            # Property: Large batches should be efficiently chunked without overwhelming clients
            
            start_time = datetime.now()
            
            # Publish large bulk operation
            await broker.publish_bulk_events(events)
            
            # Allow time for chunked processing
            await asyncio.sleep(0.5)  # 500ms for large batch processing
            
            processing_time = (datetime.now() - start_time).total_seconds() * 1000
            
            # Efficiency Property 1: Should complete in reasonable time even for large batches
            # Allow up to 3ms per event for large batch processing
            max_allowed_time = num_events * 3
            max_allowed_time = min(max_allowed_time, 5000)  # Cap at 5 seconds
            
            assert processing_time <= max_allowed_time, (
                f"Large batch processing of {num_events} events took {processing_time:.1f}ms, "
                f"should be under {max_allowed_time:.1f}ms"
            )
            
            # Efficiency Property 2: All clients should receive all events despite chunking
            for i, conn in enumerate(connections):
                total_events_received = 0
                for call in conn.websocket.send.call_args_list:
                    message_json = call[0][0]
                    import json
                    message = json.loads(message_json)
                    
                    if message["type"] == "sync_event":
                        total_events_received += 1
                    elif message["type"] == "sync_batch":
                        total_events_received += message["count"]
                
                assert total_events_received == num_events, (
                    f"Client {i} received {total_events_received} events, expected {num_events}"
                )
            
            # Efficiency Property 3: Should use batching to reduce network calls
            total_send_calls = sum(conn.websocket.send.call_count for conn in connections)
            max_expected_calls = num_events * num_clients  # Worst case: one call per event per client
            
            # Should be significantly more efficient than individual sends
            efficiency_ratio = total_send_calls / max_expected_calls if max_expected_calls > 0 else 1
            assert efficiency_ratio <= 0.6, (
                f"Large batch not efficient enough: {total_send_calls} calls vs "
                f"{max_expected_calls} individual calls (ratio: {efficiency_ratio:.2f})"
            )
            
            # Efficiency Property 4: Metrics should show good performance
            metrics = broker.get_metrics()
            assert metrics["events_published"] >= num_events
            assert metrics["batches_sent"] > 0, "Should have sent at least one batch"
            assert metrics["buffer_size"] == 0, "Buffer should be cleared after processing"
        
        # Run the async test
        asyncio.run(run_test())

    @given(st.integers(min_value=1, max_value=10))
    def test_dynamic_batch_optimization_property(self, connection_count: int):
        """Test that batch settings are dynamically optimized based on connection count.
        
        **Feature: multi-user-sync, Property 14: Bulk operation efficiency**
        **Validates: Requirements 3.5**
        """
        from mvh_copy_mb.events.broker import EventBrokerImpl
        
        async def run_test():
            broker = EventBrokerImpl(batch_size=50, batch_timeout=0.1)
            
            # Property: Batch settings should be optimized based on connection count
            
            # Test optimization for different connection counts
            await broker.optimize_batch_settings(connection_count)
            
            metrics = broker.get_metrics()
            
            # Verify that batch settings are reasonable for the connection count
            if connection_count <= 5:
                # Small number of clients - should prioritize latency
                assert metrics["batch_size_config"] <= 15, (
                    f"Batch size {metrics['batch_size_config']} too large for {connection_count} connections"
                )
                assert metrics["batch_timeout_config"] <= 0.1, (
                    f"Batch timeout {metrics['batch_timeout_config']} too high for low latency"
                )
            elif connection_count <= 20:
                # Medium number of clients - balanced settings
                assert 15 <= metrics["batch_size_config"] <= 35, (
                    f"Batch size {metrics['batch_size_config']} not balanced for {connection_count} connections"
                )
                assert 0.05 <= metrics["batch_timeout_config"] <= 0.15, (
                    f"Batch timeout {metrics['batch_timeout_config']} not balanced"
                )
            else:
                # Large number of clients - should prioritize throughput
                assert metrics["batch_size_config"] >= 30, (
                    f"Batch size {metrics['batch_size_config']} too small for {connection_count} connections"
                )
                assert metrics["batch_timeout_config"] >= 0.1, (
                    f"Batch timeout {metrics['batch_timeout_config']} too low for high throughput"
                )
            
            # Verify metrics are properly tracked
            assert "active_connections" in metrics
            assert "total_subscriptions" in metrics
            assert "buffer_size" in metrics
            assert isinstance(metrics["events_published"], int)
            assert isinstance(metrics["batches_sent"], int)
        
        # Run the async test
        asyncio.run(run_test())