# Multi-User Synchronization Design

## Overview

The multi-user synchronization system enables real-time collaboration for the MVH Meldebestätigungen processing application. The design implements WebSocket-based communication with optimistic locking, automatic conflict resolution, and robust connection management to ensure data consistency across multiple concurrent users.

## Architecture

The system follows a hub-and-spoke architecture with the FastAPI server as the central synchronization hub:

```
┌─────────────┐    WebSocket    ┌──────────────────┐    ┌─────────────┐
│   Client 1  │◄──────────────►│  Sync Manager    │◄──►│   DuckDB    │
└─────────────┘                 │                  │    └─────────────┘
┌─────────────┐    WebSocket    │  - Event Broker  │
│   Client 2  │◄──────────────►│  - Lock Manager  │
└─────────────┘                 │  - Connection    │
┌─────────────┐    WebSocket    │    Pool          │
│   Client N  │◄──────────────►│                  │
└─────────────┘                 └──────────────────┘
```

### Core Components

1. **WebSocket Manager**: Handles client connections and message routing
2. **Event Broker**: Broadcasts data changes to all connected clients  
3. **Lock Manager**: Implements optimistic locking for conflict prevention
4. **Sync Service**: Coordinates data synchronization operations
5. **Connection Pool**: Maintains active client connections with health monitoring

## Components and Interfaces

### WebSocket Manager
- Manages WebSocket connection lifecycle (connect, disconnect, reconnect)
- Handles client authentication and session management
- Implements connection pooling with automatic cleanup
- Provides heartbeat mechanism for connection health monitoring

### Event Broker
- Publishes data change events to subscribed clients
- Implements event filtering based on client interests
- Handles bulk event batching for performance optimization
- Provides event persistence for offline client synchronization

### Lock Manager  
- Implements record-level optimistic locking using version numbers
- Manages lock timeouts and automatic release
- Handles lock conflict detection and resolution
- Provides lock status queries for UI feedback

### Sync Service
- Coordinates between database operations and event broadcasting
- Implements change detection and delta synchronization
- Handles batch operations with efficient event generation
- Provides rollback capabilities for failed synchronization

## Data Models

### SyncEvent
```python
@dataclass
class SyncEvent:
    event_type: str  # 'record_updated', 'record_added', 'record_deleted'
    record_id: str
    data: dict
    version: int
    timestamp: datetime
    user_id: str
```

### RecordLock
```python
@dataclass  
class RecordLock:
    record_id: str
    user_id: str
    version: int
    acquired_at: datetime
    expires_at: datetime
```

### ClientConnection
```python
@dataclass
class ClientConnection:
    connection_id: str
    user_id: str
    websocket: WebSocket
    last_seen: datetime
    subscriptions: Set[str]
```
## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system-essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

Property 1: Multi-user data consistency
*For any* set of concurrent operations across multiple clients, the final data state should be identical across all connected Client_Instances
**Validates: Requirements 1.1**

Property 2: Broadcast timing guarantee  
*For any* record modification, all other connected Client_Instances should receive the change notification within 100 milliseconds
**Validates: Requirements 1.2**

Property 3: Status change propagation
*For any* record status change by one user, all other users viewing that record should immediately see the updated status
**Validates: Requirements 1.3**

Property 4: Concurrent edit prevention
*For any* record being edited by one user, attempts by other users to edit the same record should be rejected with appropriate notification
**Validates: Requirements 1.4**

Property 5: WebSocket connection maintenance
*For any* active client session with real-time updates enabled, a WebSocket connection should remain established and functional
**Validates: Requirements 1.5**

Property 6: First-wins conflict resolution
*For any* two simultaneous modification attempts on the same record, only the first change should be applied and subsequent changes should be rejected
**Validates: Requirements 2.1**

Property 7: Conflict notification delivery
*For any* conflict that occurs, all affected users should receive specific details about the conflict
**Validates: Requirements 2.2**

Property 8: Automatic lock release on disconnection
*For any* user connection that is lost during an edit, all Record_Locks held by that user should be released within 30 seconds
**Validates: Requirements 2.3**

Property 9: Data integrity preservation during conflicts
*For any* conflicting changes detected, the system should maintain the most recent valid data state without corruption
**Validates: Requirements 2.4**

Property 10: Version validation before changes
*For any* change attempt using optimistic locking, the record version should be validated before applying the change
**Validates: Requirements 2.5**

Property 11: CSV upload synchronization
*For any* CSV file upload and processing, all new records should be broadcast to all connected Client_Instances
**Validates: Requirements 3.1**

Property 12: Database change propagation
*For any* new records added to the database, all active web interfaces should display the new data
**Validates: Requirements 3.2**

Property 13: External change detection
*For any* database update made externally, the Sync_System should detect the changes and synchronize all Client_Instances
**Validates: Requirements 3.4**

Property 14: Bulk operation efficiency
*For any* batch processing operation, bulk changes should be broadcast efficiently without overwhelming client connections
**Validates: Requirements 3.5**

Property 15: Exponential backoff reconnection
*For any* lost client connection, automatic reconnection attempts should follow exponential backoff timing
**Validates: Requirements 4.1**

Property 16: Missed update synchronization
*For any* client that reconnects after disconnection, all updates that occurred during disconnection should be synchronized
**Validates: Requirements 4.2**

Property 17: Graceful degradation on persistent issues
*For any* persistent connection issues, the system should gracefully degrade to manual refresh mode with clear user notification
**Validates: Requirements 4.3**

Property 18: Change buffering for offline clients
*For any* client offline for extended periods, a reasonable buffer of changes should be maintained for synchronization upon reconnection
**Validates: Requirements 4.4**

Property 19: Synchronization event logging
*For any* synchronization event, the system should log all data changes with timestamps and user identification
**Validates: Requirements 5.1**

Property 20: Conflict resolution logging
*For any* resolved conflict, the system should record the conflict details and resolution method
**Validates: Requirements 5.2**

Property 21: Connection event logging
*For any* connection issue, the system should log connection events with diagnostic information
**Validates: Requirements 5.3**

Property 22: Performance metrics collection
*For any* system performance degradation, metrics on synchronization latency and throughput should be provided
**Validates: Requirements 5.4**

Property 23: Complete audit trail maintenance
*For any* multi-user interaction, complete records should be maintained for audit trail purposes
**Validates: Requirements 5.5**
## Error Handling

### Connection Errors
- **WebSocket Connection Failure**: Implement exponential backoff with maximum retry limits
- **Network Timeout**: Graceful degradation to polling mode with user notification
- **Authentication Failure**: Clear error messages with re-authentication prompts

### Synchronization Errors  
- **Version Conflict**: Reject changes with detailed conflict information to user
- **Lock Timeout**: Automatic lock release with notification to lock holder
- **Broadcast Failure**: Retry mechanism with dead letter queue for failed deliveries

### Data Integrity Errors
- **Concurrent Modification**: First-wins strategy with clear rejection messages
- **Invalid State Transitions**: Validation before applying changes with rollback capability
- **Database Constraint Violations**: Proper error propagation with user-friendly messages

### System Errors
- **Memory Pressure**: Connection throttling and cleanup of inactive connections  
- **High Latency**: Adaptive batching and reduced update frequency
- **Service Unavailability**: Circuit breaker pattern with fallback to read-only mode

## Testing Strategy

### Unit Testing
The system will use pytest for unit testing with focus on:
- Individual component functionality (WebSocket Manager, Event Broker, Lock Manager)
- Error handling and edge cases
- Mock-based testing for external dependencies
- Integration points between components

### Property-Based Testing  
The system will use Hypothesis for property-based testing with minimum 100 iterations per test:
- Each correctness property will be implemented as a single property-based test
- Tests will be tagged with comments referencing design document properties
- Format: `**Feature: multi-user-sync, Property {number}: {property_text}**`
- Smart generators will be created for realistic test scenarios including:
  - Multiple concurrent client connections
  - Various record modification patterns  
  - Network failure and reconnection scenarios
  - Bulk data operations

### Integration Testing
- End-to-end WebSocket communication testing
- Database synchronization validation
- Multi-client scenario testing with real WebSocket connections
- Performance testing under load with multiple concurrent users

### Load Testing
- Connection scalability testing (target: 20+ concurrent users)
- Message throughput testing for bulk operations
- Memory usage monitoring under sustained load
- Latency measurement for synchronization operations

The testing approach ensures both specific examples work correctly (unit tests) and universal properties hold across all inputs (property tests), providing comprehensive coverage for the multi-user synchronization system.