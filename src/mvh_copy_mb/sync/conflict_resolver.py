"""Conflict resolution system for multi-user synchronization."""

import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import logging

from .models import SyncEvent, RecordLock, ConflictNotification, EventType
from .interfaces import LockManager, EventBroker


logger = logging.getLogger(__name__)


class ConflictType:
    """Types of conflicts that can occur."""
    SIMULTANEOUS_EDIT = "simultaneous_edit"
    VERSION_MISMATCH = "version_mismatch"
    LOCK_CONFLICT = "lock_conflict"
    STALE_UPDATE = "stale_update"


class ConflictResolutionResult:
    """Result of conflict resolution."""
    
    def __init__(self, success: bool, winning_event: Optional[SyncEvent] = None, 
                 rejected_events: Optional[List[SyncEvent]] = None, 
                 notifications: Optional[List[ConflictNotification]] = None):
        self.success = success
        self.winning_event = winning_event
        self.rejected_events = rejected_events or []
        self.notifications = notifications or []


class FirstWinsConflictResolver:
    """Implements first-wins conflict resolution strategy."""
    
    def __init__(self, lock_manager: LockManager, event_broker: EventBroker):
        """Initialize the conflict resolver.
        
        Args:
            lock_manager: Lock manager for checking and managing locks
            event_broker: Event broker for sending conflict notifications
        """
        self._lock_manager = lock_manager
        self._event_broker = event_broker
        self._pending_operations: Dict[str, List[Tuple[SyncEvent, datetime]]] = {}
        self._operation_lock = asyncio.Lock()
    
    async def resolve_conflict(self, events: List[SyncEvent]) -> ConflictResolutionResult:
        """Resolve conflicts between multiple events using first-wins strategy.
        
        Args:
            events: List of potentially conflicting events
            
        Returns:
            ConflictResolutionResult with resolution outcome
        """
        if not events:
            return ConflictResolutionResult(success=True)
        
        if len(events) == 1:
            # No conflict with single event
            return ConflictResolutionResult(success=True, winning_event=events[0])
        
        # Group events by record_id to detect conflicts
        record_events = {}
        for event in events:
            if event.record_id not in record_events:
                record_events[event.record_id] = []
            record_events[event.record_id].append(event)
        
        winning_events = []
        rejected_events = []
        notifications = []
        
        # Resolve conflicts for each record
        for record_id, record_event_list in record_events.items():
            if len(record_event_list) == 1:
                # No conflict for this record
                winning_events.append(record_event_list[0])
            else:
                # Conflict detected - apply first-wins strategy
                result = await self._resolve_record_conflict(record_id, record_event_list)
                if result.winning_event:
                    winning_events.append(result.winning_event)
                rejected_events.extend(result.rejected_events)
                notifications.extend(result.notifications)
        
        # Send conflict notifications
        for notification in notifications:
            await self._send_conflict_notification(notification)
        
        return ConflictResolutionResult(
            success=True,
            winning_event=winning_events[0] if len(winning_events) == 1 else None,
            rejected_events=rejected_events,
            notifications=notifications
        )
    
    async def detect_simultaneous_edit(self, record_id: str, user_id: str, 
                                     expected_version: int) -> Optional[str]:
        """Detect if a simultaneous edit conflict exists.
        
        Args:
            record_id: ID of the record being edited
            user_id: ID of the user attempting the edit
            expected_version: Expected version of the record
            
        Returns:
            Conflict type if conflict detected, None otherwise
        """
        # Check if record is currently locked by another user
        existing_lock = await self._lock_manager.check_lock(record_id)
        
        if existing_lock:
            if existing_lock.user_id != user_id:
                logger.info(f"Simultaneous edit detected: record {record_id} locked by {existing_lock.user_id}, "
                           f"edit attempted by {user_id}")
                return ConflictType.SIMULTANEOUS_EDIT
            
            # Check version mismatch
            if existing_lock.version != expected_version:
                logger.info(f"Version mismatch detected: record {record_id} has version {existing_lock.version}, "
                           f"expected {expected_version}")
                return ConflictType.VERSION_MISMATCH
        
        return None
    
    async def attempt_operation_with_conflict_detection(self, event: SyncEvent) -> ConflictResolutionResult:
        """Attempt an operation with automatic conflict detection and resolution.
        
        Args:
            event: The sync event to process
            
        Returns:
            ConflictResolutionResult indicating success or conflict
        """
        async with self._operation_lock:
            record_id = event.record_id
            user_id = event.user_id
            
            # Check for existing lock conflicts
            conflict_type = await self.detect_simultaneous_edit(record_id, user_id, event.version)
            
            if conflict_type:
                # Conflict detected - create notification
                notification = await self._create_conflict_notification(
                    event, conflict_type, "Operation rejected due to conflict"
                )
                
                await self._send_conflict_notification(notification)
                
                return ConflictResolutionResult(
                    success=False,
                    rejected_events=[event],
                    notifications=[notification]
                )
            
            # No conflict - operation can proceed
            return ConflictResolutionResult(success=True, winning_event=event)
    
    async def _resolve_record_conflict(self, record_id: str, 
                                     events: List[SyncEvent]) -> ConflictResolutionResult:
        """Resolve conflict for a specific record using first-wins strategy.
        
        Args:
            record_id: ID of the record with conflicts
            events: List of conflicting events for this record
            
        Returns:
            ConflictResolutionResult for this record
        """
        # Sort events by timestamp to determine "first"
        sorted_events = sorted(events, key=lambda e: e.timestamp)
        
        # First event wins
        winning_event = sorted_events[0]
        rejected_events = sorted_events[1:]
        
        logger.info(f"Conflict resolved for record {record_id}: "
                   f"event from {winning_event.user_id} at {winning_event.timestamp} wins, "
                   f"{len(rejected_events)} events rejected")
        
        # Create notifications for rejected events
        notifications = []
        for rejected_event in rejected_events:
            notification = await self._create_conflict_notification(
                rejected_event, 
                ConflictType.SIMULTANEOUS_EDIT,
                f"Edit rejected: {winning_event.user_id} modified record first at {winning_event.timestamp}"
            )
            notifications.append(notification)
        
        return ConflictResolutionResult(
            success=True,
            winning_event=winning_event,
            rejected_events=rejected_events,
            notifications=notifications
        )
    
    async def _create_conflict_notification(self, rejected_event: SyncEvent, 
                                          conflict_type: str, message: str) -> ConflictNotification:
        """Create a conflict notification for a rejected event.
        
        Args:
            rejected_event: The event that was rejected
            conflict_type: Type of conflict that occurred
            message: Human-readable conflict message
            
        Returns:
            ConflictNotification to send to the user
        """
        # Determine who the conflicting user is
        conflicting_user = "unknown"
        existing_lock = await self._lock_manager.check_lock(rejected_event.record_id)
        if existing_lock:
            conflicting_user = existing_lock.user_id
        
        return ConflictNotification(
            record_id=rejected_event.record_id,
            conflict_type=conflict_type,
            message=message,
            conflicting_user=conflicting_user,
            timestamp=datetime.now()
        )
    
    async def _send_conflict_notification(self, notification: ConflictNotification) -> None:
        """Send a conflict notification to affected users.
        
        Args:
            notification: The conflict notification to send
        """
        try:
            # Create a sync event for the conflict notification
            conflict_event = SyncEvent(
                event_type=EventType.RECORD_LOCKED.value,  # Use existing event type
                record_id=notification.record_id,
                data={
                    "conflict_type": notification.conflict_type,
                    "message": notification.message,
                    "conflicting_user": notification.conflicting_user,
                    "notification_type": "conflict"
                },
                version=0,  # Notifications don't have versions
                timestamp=notification.timestamp,
                user_id="system"  # System-generated notification
            )
            
            await self._event_broker.publish_event(conflict_event)
            logger.info(f"Conflict notification sent for record {notification.record_id}")
            
        except Exception as e:
            logger.error(f"Failed to send conflict notification: {e}")
    
    async def get_conflict_statistics(self) -> Dict[str, int]:
        """Get statistics about conflict resolution.
        
        Returns:
            Dictionary with conflict statistics
        """
        # This would typically be implemented with persistent storage
        # For now, return basic metrics
        return {
            "total_conflicts_resolved": 0,
            "simultaneous_edits": 0,
            "version_mismatches": 0,
            "lock_conflicts": 0,
            "first_wins_applied": 0
        }
    
    async def cleanup_pending_operations(self) -> None:
        """Clean up any pending operations that are no longer relevant."""
        async with self._operation_lock:
            current_time = datetime.now()
            
            # Remove operations older than 5 minutes
            expired_records = []
            for record_id, operations in self._pending_operations.items():
                # Filter out old operations
                recent_operations = [
                    (event, timestamp) for event, timestamp in operations
                    if (current_time - timestamp).total_seconds() < 300  # 5 minutes
                ]
                
                if recent_operations:
                    self._pending_operations[record_id] = recent_operations
                else:
                    expired_records.append(record_id)
            
            # Remove records with no recent operations
            for record_id in expired_records:
                del self._pending_operations[record_id]
            
            if expired_records:
                logger.debug(f"Cleaned up pending operations for {len(expired_records)} records")


class ConflictDetector:
    """Utility class for detecting various types of conflicts."""
    
    @staticmethod
    def detect_version_conflicts(events: List[SyncEvent]) -> List[Tuple[SyncEvent, SyncEvent]]:
        """Detect version conflicts between events.
        
        Args:
            events: List of events to check for version conflicts
            
        Returns:
            List of tuples containing conflicting event pairs
        """
        conflicts = []
        
        # Group events by record_id
        record_events = {}
        for event in events:
            if event.record_id not in record_events:
                record_events[event.record_id] = []
            record_events[event.record_id].append(event)
        
        # Check for version conflicts within each record
        for record_id, record_event_list in record_events.items():
            if len(record_event_list) > 1:
                # Sort by timestamp
                sorted_events = sorted(record_event_list, key=lambda e: e.timestamp)
                
                # Check if versions are sequential
                for i in range(1, len(sorted_events)):
                    prev_event = sorted_events[i-1]
                    curr_event = sorted_events[i]
                    
                    # If current event's version is not greater than previous,
                    # it's a potential conflict
                    if curr_event.version <= prev_event.version:
                        conflicts.append((prev_event, curr_event))
        
        return conflicts
    
    @staticmethod
    def detect_timing_conflicts(events: List[SyncEvent], 
                              conflict_window_seconds: float = 1.0) -> List[List[SyncEvent]]:
        """Detect events that occurred within a timing window (potential simultaneous edits).
        
        Args:
            events: List of events to check
            conflict_window_seconds: Time window in seconds to consider as simultaneous
            
        Returns:
            List of event groups that occurred within the timing window
        """
        # Group events by record_id
        record_events = {}
        for event in events:
            if event.record_id not in record_events:
                record_events[event.record_id] = []
            record_events[event.record_id].append(event)
        
        conflict_groups = []
        
        # Check timing conflicts within each record
        for record_id, record_event_list in record_events.items():
            if len(record_event_list) > 1:
                # Sort by timestamp
                sorted_events = sorted(record_event_list, key=lambda e: e.timestamp)
                
                # Group events within timing window
                current_group = [sorted_events[0]]
                
                for i in range(1, len(sorted_events)):
                    time_diff = (sorted_events[i].timestamp - current_group[0].timestamp).total_seconds()
                    
                    if time_diff <= conflict_window_seconds:
                        current_group.append(sorted_events[i])
                    else:
                        # If current group has conflicts, add it
                        if len(current_group) > 1:
                            conflict_groups.append(current_group)
                        
                        # Start new group
                        current_group = [sorted_events[i]]
                
                # Add final group if it has conflicts
                if len(current_group) > 1:
                    conflict_groups.append(current_group)
        
        return conflict_groups