"""Synchronization service coordinator for multi-user operations."""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Set
from collections import defaultdict

from .interfaces import SyncService, EventBroker, LockManager, ConnectionManager
from .models import SyncEvent, EventType, ClientConnection
from .logging_config import get_logger, log_sync_event, log_connection_event, log_conflict_event
from ..database import MeldebestaetigungDatabase, MeldebestaetigungRecord


logger = get_logger(__name__)


class SyncServiceImpl(SyncService):
    """Implementation of synchronization service coordinator."""
    
    def __init__(self, 
                 event_broker: EventBroker,
                 lock_manager: LockManager,
                 connection_manager: ConnectionManager,
                 database: MeldebestaetigungDatabase,
                 change_buffer_size: int = 1000,
                 change_buffer_ttl_hours: int = 24):
        """Initialize the sync service.
        
        Args:
            event_broker: Event broadcasting system
            lock_manager: Record locking system
            connection_manager: WebSocket connection management
            database: Database instance for change detection
            change_buffer_size: Maximum number of changes to buffer per client
            change_buffer_ttl_hours: Hours to keep changes in buffer
        """
        self._event_broker = event_broker
        self._lock_manager = lock_manager
        self._connection_manager = connection_manager
        self._database = database
        self._change_buffer_size = change_buffer_size
        self._change_buffer_ttl = timedelta(hours=change_buffer_ttl_hours)
        
        # Change buffering for offline clients
        self._client_buffers: Dict[str, List[SyncEvent]] = defaultdict(list)
        self._last_sync_timestamps: Dict[str, datetime] = {}
        
        # Change detection state
        self._last_db_check: Optional[datetime] = None
        self._known_records: Dict[str, datetime] = {}  # record_id -> last_modified
        
        # Background tasks
        self._change_detection_task: Optional[asyncio.Task] = None
        self._buffer_cleanup_task: Optional[asyncio.Task] = None
        
        self._lock = asyncio.Lock()
        
    async def start(self) -> None:
        """Start the sync service and background tasks."""
        if self._change_detection_task is None:
            self._change_detection_task = asyncio.create_task(self._periodic_change_detection())
            logger.info("Started periodic change detection")
            
        if self._buffer_cleanup_task is None:
            self._buffer_cleanup_task = asyncio.create_task(self._periodic_buffer_cleanup())
            logger.info("Started periodic buffer cleanup")
    
    async def stop(self) -> None:
        """Stop the sync service and background tasks."""
        if self._change_detection_task:
            self._change_detection_task.cancel()
            try:
                await self._change_detection_task
            except asyncio.CancelledError:
                pass
            self._change_detection_task = None
            
        if self._buffer_cleanup_task:
            self._buffer_cleanup_task.cancel()
            try:
                await self._buffer_cleanup_task
            except asyncio.CancelledError:
                pass
            self._buffer_cleanup_task = None
            
        logger.info("Sync service stopped")
    
    async def handle_record_update(self, record_id: str, data: Dict[str, Any], 
                                 user_id: str, version: int) -> None:
        """Handle a record update and broadcast to clients.
        
        Args:
            record_id: ID of the updated record
            data: Updated record data
            user_id: ID of the user making the update
            version: New version number of the record
        """
        try:
            # Validate version if lock exists
            if not await self._lock_manager.validate_version(record_id, version - 1):
                log_conflict_event(
                    logger, record_id, [user_id], 
                    "version_conflict", "rejected_update",
                    expected_version=version - 1,
                    attempted_version=version
                )
                raise ValueError(f"Version conflict for record {record_id}")
            
            # Create sync event
            event = SyncEvent(
                event_type=EventType.RECORD_UPDATED.value,
                record_id=record_id,
                data=data,
                version=version,
                timestamp=datetime.now(),
                user_id=user_id
            )
            
            # Log the sync event
            log_sync_event(
                logger, event.event_type, record_id, user_id,
                f"Record update processed for {record_id}",
                version=version,
                data_keys=list(data.keys()) if isinstance(data, dict) else None
            )
            
            # Update known records for change detection
            async with self._lock:
                self._known_records[record_id] = event.timestamp
            
            # Buffer event for offline clients
            await self._buffer_event_for_offline_clients(event)
            
            # Broadcast to all connected clients
            await self._event_broker.publish_event(event)
            
            logger.info(f"Record update handled for {record_id} by user {user_id}")
            
        except Exception as e:
            logger.error(f"Error handling record update for {record_id}: {e}", 
                        extra={'record_id': record_id, 'user_id': user_id, 'error': str(e)})
            raise
    
    async def handle_bulk_update(self, updates: List[Dict[str, Any]], 
                               user_id: str) -> None:
        """Handle bulk record updates efficiently.
        
        Args:
            updates: List of update dictionaries with record_id, data, version
            user_id: ID of the user making the updates
        """
        if not updates:
            return
            
        try:
            events = []
            now = datetime.now()
            
            for update in updates:
                record_id = update.get('record_id')
                data = update.get('data', {})
                version = update.get('version', 1)
                
                if not record_id:
                    logger.warning("Skipping update without record_id")
                    continue
                
                # Validate version if lock exists
                if not await self._lock_manager.validate_version(record_id, version - 1):
                    log_conflict_event(
                        logger, record_id, [user_id], 
                        "bulk_version_conflict", "skipped_update",
                        expected_version=version - 1,
                        attempted_version=version
                    )
                    continue
                
                event = SyncEvent(
                    event_type=EventType.RECORD_UPDATED.value,
                    record_id=record_id,
                    data=data,
                    version=version,
                    timestamp=now,
                    user_id=user_id
                )
                events.append(event)
            
            if not events:
                logger.warning("No valid events in bulk update")
                return
            
            # Update known records for change detection
            async with self._lock:
                for event in events:
                    self._known_records[event.record_id] = event.timestamp
            
            # Buffer events for offline clients
            for event in events:
                await self._buffer_event_for_offline_clients(event)
            
            # Log bulk update event
            log_sync_event(
                logger, "bulk_update", f"bulk_{len(events)}_records", user_id,
                f"Bulk update processed: {len(events)} records",
                record_count=len(events),
                skipped_count=len(updates) - len(events)
            )
            
            # Broadcast all events efficiently
            await self._event_broker.publish_bulk_events(events)
            
            logger.info(f"Bulk update handled: {len(events)} records by user {user_id}")
            
        except Exception as e:
            logger.error(f"Error handling bulk update: {e}")
            raise
    
    async def sync_client(self, connection_id: str, 
                         last_sync_timestamp: Optional[datetime] = None) -> None:
        """Synchronize a client with missed updates.
        
        Args:
            connection_id: ID of the client connection
            last_sync_timestamp: Last known sync timestamp for the client
        """
        try:
            connection = await self._connection_manager.get_connection(connection_id)
            if not connection:
                log_connection_event(
                    logger, connection_id, "unknown", "sync_failed",
                    f"Connection not found for sync: {connection_id}"
                )
                return
            
            # Get buffered events for this client
            async with self._lock:
                buffered_events = self._client_buffers.get(connection_id, [])
                
                # Filter events by timestamp if provided
                if last_sync_timestamp:
                    buffered_events = [
                        event for event in buffered_events 
                        if event.timestamp > last_sync_timestamp
                    ]
                
                # Clear the buffer after retrieving events
                self._client_buffers[connection_id] = []
                self._last_sync_timestamps[connection_id] = datetime.now()
            
            if buffered_events:
                # Log sync event
                log_connection_event(
                    logger, connection_id, connection.user_id, "sync_completed",
                    f"Synchronized {len(buffered_events)} events to client",
                    event_count=len(buffered_events)
                )
                
                # Send buffered events to client
                await self._event_broker.publish_bulk_events(buffered_events)
                logger.info(f"Synchronized {len(buffered_events)} events to client {connection_id}")
            else:
                log_connection_event(
                    logger, connection_id, connection.user_id, "sync_no_updates",
                    "No missed updates for client"
                )
                logger.debug(f"No missed updates for client {connection_id}")
                
        except Exception as e:
            logger.error(f"Error synchronizing client {connection_id}: {e}")
            raise
    
    async def sync_reconnected_client(self, connection_id: str, 
                                    disconnection_time: Optional[datetime] = None) -> int:
        """Synchronize a client that has reconnected after disconnection.
        
        Args:
            connection_id: ID of the reconnected client
            disconnection_time: When the client was disconnected (for filtering events)
            
        Returns:
            Number of events synchronized
        """
        try:
            connection = await self._connection_manager.get_connection(connection_id)
            if not connection:
                logger.warning(f"Connection not found for reconnection sync: {connection_id}")
                return 0
            
            # Determine the sync timestamp to use
            sync_timestamp = disconnection_time
            if not sync_timestamp:
                # Use last known sync timestamp for this client
                sync_timestamp = self._last_sync_timestamps.get(connection_id)
            
            # Get all buffered events for this client since disconnection
            async with self._lock:
                buffered_events = self._client_buffers.get(connection_id, [])
                
                # Filter events since disconnection/last sync
                if sync_timestamp:
                    missed_events = [
                        event for event in buffered_events 
                        if event.timestamp > sync_timestamp
                    ]
                else:
                    # If no timestamp available, sync all buffered events
                    missed_events = buffered_events.copy()
                
                # Sort events by timestamp for proper ordering
                missed_events.sort(key=lambda e: e.timestamp)
                
                # Update last sync timestamp
                self._last_sync_timestamps[connection_id] = datetime.now()
            
            if missed_events:
                # Send missed events in efficient batches
                await self._send_delta_updates(connection_id, missed_events)
                logger.info(f"Synchronized {len(missed_events)} missed updates to reconnected client {connection_id}")
                return len(missed_events)
            else:
                logger.debug(f"No missed updates for reconnected client {connection_id}")
                return 0
                
        except Exception as e:
            logger.error(f"Error synchronizing reconnected client {connection_id}: {e}")
            raise
    
    async def _send_delta_updates(self, connection_id: str, events: List[SyncEvent]) -> None:
        """Send delta updates efficiently to a specific client.
        
        Args:
            connection_id: ID of the client to send updates to
            events: List of events to send as delta updates
        """
        if not events:
            return
            
        try:
            # Optimize events for delta synchronization
            optimized_events = await self._optimize_delta_events(events)
            
            # Send events in manageable chunks to avoid overwhelming the client
            chunk_size = 25  # Reasonable chunk size for delta updates
            
            for i in range(0, len(optimized_events), chunk_size):
                chunk = optimized_events[i:i + chunk_size]
                
                # Create a targeted event broadcast for this specific client
                # In a real implementation, you'd want to send directly to the client
                # For now, we'll use the event broker but this could be optimized
                await self._event_broker.publish_bulk_events(chunk)
                
                # Small delay between chunks to prevent overwhelming
                if i + chunk_size < len(optimized_events):
                    await asyncio.sleep(0.01)  # 10ms delay between chunks
                    
            logger.debug(f"Sent {len(optimized_events)} delta updates to client {connection_id}")
            
        except Exception as e:
            logger.error(f"Error sending delta updates to client {connection_id}: {e}")
            raise
    
    async def _optimize_delta_events(self, events: List[SyncEvent]) -> List[SyncEvent]:
        """Optimize events for delta synchronization by removing redundant updates.
        
        Args:
            events: List of events to optimize
            
        Returns:
            Optimized list of events with redundant updates removed
        """
        if not events:
            return events
            
        # Group events by record_id to identify redundant updates
        record_events: Dict[str, List[SyncEvent]] = defaultdict(list)
        
        for event in events:
            record_events[event.record_id].append(event)
        
        optimized_events = []
        
        for record_id, record_event_list in record_events.items():
            # Sort events by timestamp
            record_event_list.sort(key=lambda e: e.timestamp)
            
            # For each record, keep only the most recent update of each type
            latest_events: Dict[str, SyncEvent] = {}
            
            for event in record_event_list:
                event_key = f"{event.event_type}:{event.record_id}"
                
                # Keep the event with the latest timestamp and highest version
                if (event_key not in latest_events or 
                    event.timestamp > latest_events[event_key].timestamp or
                    (event.timestamp == latest_events[event_key].timestamp and 
                     event.version > latest_events[event_key].version)):
                    latest_events[event_key] = event
            
            optimized_events.extend(latest_events.values())
        
        # Sort final events by timestamp for proper ordering
        optimized_events.sort(key=lambda e: e.timestamp)
        
        if len(optimized_events) < len(events):
            logger.debug(f"Optimized delta events: {len(events)} -> {len(optimized_events)}")
        
        return optimized_events
    
    async def get_client_sync_status(self, connection_id: str) -> Dict[str, Any]:
        """Get synchronization status for a specific client.
        
        Args:
            connection_id: ID of the client connection
            
        Returns:
            Dictionary with sync status information
        """
        async with self._lock:
            buffered_count = len(self._client_buffers.get(connection_id, []))
            last_sync = self._last_sync_timestamps.get(connection_id)
            
        connection = await self._connection_manager.get_connection(connection_id)
        
        return {
            "connection_id": connection_id,
            "is_connected": connection is not None,
            "buffered_events": buffered_count,
            "last_sync_timestamp": last_sync.isoformat() if last_sync else None,
            "user_id": connection.user_id if connection else None,
            "last_seen": connection.last_seen.isoformat() if connection else None
        }
    
    async def cleanup_disconnected_client_buffer(self, connection_id: str, 
                                                disconnection_time: datetime) -> int:
        """Clean up buffer for a disconnected client after grace period.
        
        Args:
            connection_id: ID of the disconnected client
            disconnection_time: When the client disconnected
            
        Returns:
            Number of events cleaned up
        """
        try:
            # Check if enough time has passed since disconnection
            time_since_disconnect = (datetime.now() - disconnection_time).total_seconds()
            grace_period_hours = self._change_buffer_ttl.total_seconds() / 3600
            
            if time_since_disconnect < grace_period_hours * 3600:
                logger.debug(f"Client {connection_id} within grace period, keeping buffer")
                return 0
            
            # Clean up the buffer
            async with self._lock:
                buffered_events = self._client_buffers.pop(connection_id, [])
                self._last_sync_timestamps.pop(connection_id, None)
            
            if buffered_events:
                logger.info(f"Cleaned up {len(buffered_events)} buffered events for disconnected client {connection_id}")
                return len(buffered_events)
            
            return 0
            
        except Exception as e:
            logger.error(f"Error cleaning up buffer for client {connection_id}: {e}")
            return 0
    
    async def detect_external_changes(self) -> List[SyncEvent]:
        """Detect changes made outside the sync system.
        
        Returns:
            List of SyncEvent objects representing detected changes
        """
        try:
            detected_events = []
            
            # Query database for recent changes
            # Note: This is a simplified implementation - in practice you might
            # use database triggers, change streams, or polling with timestamps
            
            # For now, we'll implement a basic polling mechanism
            # In a real implementation, you'd want to use database-specific
            # change detection mechanisms
            
            current_time = datetime.now()
            
            # Check if enough time has passed since last check
            if (self._last_db_check and 
                (current_time - self._last_db_check).total_seconds() < 5):
                return detected_events
            
            # This is a placeholder for actual database change detection
            # In practice, you would:
            # 1. Query for records modified since last check
            # 2. Compare with known state
            # 3. Generate events for changes
            
            async with self._lock:
                self._last_db_check = current_time
            
            logger.debug("External change detection completed")
            return detected_events
            
        except Exception as e:
            logger.error(f"Error detecting external changes: {e}")
            return []
    
    async def _buffer_event_for_offline_clients(self, event: SyncEvent) -> None:
        """Buffer an event for clients that might be offline.
        
        Args:
            event: Event to buffer
        """
        async with self._lock:
            # Get all connection IDs to determine who might need this event
            all_connections = await self._connection_manager.get_all_connections()
            active_connection_ids = {conn.connection_id for conn in all_connections}
            
            # Buffer for all known clients (including potentially offline ones)
            for connection_id in self._client_buffers.keys():
                if connection_id not in active_connection_ids:
                    # Client is offline, buffer the event
                    buffer = self._client_buffers[connection_id]
                    buffer.append(event)
                    
                    # Trim buffer if it exceeds size limit
                    if len(buffer) > self._change_buffer_size:
                        buffer.pop(0)  # Remove oldest event
            
            # Also add to buffers for any clients we haven't seen before
            # (This handles the case where a client connects for the first time)
            for connection in all_connections:
                if connection.connection_id not in self._client_buffers:
                    self._client_buffers[connection.connection_id] = [event]
    
    async def _periodic_change_detection(self) -> None:
        """Periodic task to detect external database changes."""
        while True:
            try:
                await asyncio.sleep(10)  # Check every 10 seconds
                
                detected_events = await self.detect_external_changes()
                
                if detected_events:
                    # Buffer and broadcast detected changes
                    for event in detected_events:
                        await self._buffer_event_for_offline_clients(event)
                    
                    await self._event_broker.publish_bulk_events(detected_events)
                    logger.info(f"Detected and broadcast {len(detected_events)} external changes")
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in periodic change detection: {e}")
                await asyncio.sleep(30)  # Wait longer on error
    
    async def _periodic_buffer_cleanup(self) -> None:
        """Periodic task to clean up old buffered events."""
        while True:
            try:
                await asyncio.sleep(3600)  # Run every hour
                
                cutoff_time = datetime.now() - self._change_buffer_ttl
                cleaned_count = 0
                
                async with self._lock:
                    for connection_id in list(self._client_buffers.keys()):
                        buffer = self._client_buffers[connection_id]
                        original_size = len(buffer)
                        
                        # Remove events older than TTL
                        self._client_buffers[connection_id] = [
                            event for event in buffer 
                            if event.timestamp > cutoff_time
                        ]
                        
                        cleaned_count += original_size - len(self._client_buffers[connection_id])
                        
                        # Remove empty buffers for connections that no longer exist
                        if not self._client_buffers[connection_id]:
                            # Check if connection still exists
                            connection = await self._connection_manager.get_connection(connection_id)
                            if not connection:
                                del self._client_buffers[connection_id]
                                self._last_sync_timestamps.pop(connection_id, None)
                
                if cleaned_count > 0:
                    logger.info(f"Cleaned up {cleaned_count} old buffered events")
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in periodic buffer cleanup: {e}")
                await asyncio.sleep(1800)  # Wait 30 minutes on error
    
    def get_buffer_stats(self) -> Dict[str, Any]:
        """Get statistics about change buffers (for monitoring/debugging).
        
        Returns:
            Dictionary with buffer statistics
        """
        total_buffered = sum(len(buffer) for buffer in self._client_buffers.values())
        
        return {
            "total_clients_with_buffers": len(self._client_buffers),
            "total_buffered_events": total_buffered,
            "average_buffer_size": total_buffered / len(self._client_buffers) if self._client_buffers else 0,
            "known_records_count": len(self._known_records),
            "last_db_check": self._last_db_check.isoformat() if self._last_db_check else None
        }