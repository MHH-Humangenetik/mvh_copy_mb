"""Event broker implementation for message distribution and routing."""

import asyncio
import json
import logging
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Set, Optional, Any
from websockets.exceptions import ConnectionClosed

from ..sync.interfaces import EventBroker
from ..sync.models import SyncEvent, ClientConnection, WebSocketMessage


logger = logging.getLogger(__name__)


class EventBrokerImpl(EventBroker):
    """Implementation of event broadcasting system with message routing."""
    
    def __init__(self, batch_size: int = 50, batch_timeout: float = 0.1, 
                 max_batch_size: int = 200, compression_threshold: int = 10):
        """Initialize the event broker.
        
        Args:
            batch_size: Default number of events to batch together
            batch_timeout: Maximum time to wait before sending a batch (seconds)
            max_batch_size: Maximum number of events in a single batch
            compression_threshold: Minimum batch size to enable compression
        """
        self._connections: Dict[str, ClientConnection] = {}
        self._subscriptions: Dict[str, Set[str]] = defaultdict(set)  # connection_id -> subscriptions
        self._event_buffer: List[SyncEvent] = []
        self._batch_size = batch_size
        self._batch_timeout = batch_timeout
        self._max_batch_size = max_batch_size
        self._compression_threshold = compression_threshold
        self._batch_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
        self._metrics = {
            "events_published": 0,
            "batches_sent": 0,
            "total_clients_notified": 0,
            "compression_used": 0
        }
        
    async def publish_event(self, event: SyncEvent) -> None:
        """Publish a synchronization event to all subscribed clients."""
        # Update metrics
        self._metrics["events_published"] += 1
        
        async with self._lock:
            # Add to buffer for batching
            self._event_buffer.append(event)
            
            # Start batch processing if not already running
            if self._batch_task is None or self._batch_task.done():
                self._batch_task = asyncio.create_task(self._process_batch())
                
            # If buffer is full, process immediately
            if len(self._event_buffer) >= self._batch_size:
                if not self._batch_task.done():
                    self._batch_task.cancel()
                await self._flush_buffer()
    
    async def subscribe_client(self, connection: ClientConnection, 
                             subscriptions: Set[str]) -> None:
        """Subscribe a client to specific event types or record IDs."""
        async with self._lock:
            self._connections[connection.connection_id] = connection
            self._subscriptions[connection.connection_id] = subscriptions.copy()
            
        logger.info(f"Client {connection.connection_id} subscribed to {len(subscriptions)} topics")
    
    async def unsubscribe_client(self, connection_id: str) -> None:
        """Unsubscribe a client from all events."""
        async with self._lock:
            self._connections.pop(connection_id, None)
            self._subscriptions.pop(connection_id, None)
            
        logger.info(f"Client {connection_id} unsubscribed from all events")
    
    async def publish_bulk_events(self, events: List[SyncEvent]) -> None:
        """Publish multiple events efficiently as a batch."""
        if not events:
            return
            
        # Update metrics
        self._metrics["events_published"] += len(events)
        
        async with self._lock:
            # For very large bulk operations, process in chunks to avoid overwhelming clients
            if len(events) > self._max_batch_size:
                # Process in chunks
                for i in range(0, len(events), self._max_batch_size):
                    chunk = events[i:i + self._max_batch_size]
                    self._event_buffer.extend(chunk)
                    
                    # Process this chunk
                    if self._batch_task and not self._batch_task.done():
                        self._batch_task.cancel()
                    await self._flush_buffer()
                    
                    # Small delay between chunks to prevent overwhelming
                    if i + self._max_batch_size < len(events):
                        await asyncio.sleep(0.01)  # 10ms delay between chunks
            else:
                self._event_buffer.extend(events)
                
                # Process immediately for bulk operations
                if self._batch_task and not self._batch_task.done():
                    self._batch_task.cancel()
                await self._flush_buffer()
    
    async def _process_batch(self) -> None:
        """Process batched events after timeout."""
        try:
            await asyncio.sleep(self._batch_timeout)
            async with self._lock:
                await self._flush_buffer()
        except asyncio.CancelledError:
            # Task was cancelled, buffer will be processed elsewhere
            pass
    
    async def _flush_buffer(self) -> None:
        """Flush the event buffer and send to clients with optimization."""
        if not self._event_buffer:
            return
            
        events_to_send = self._event_buffer.copy()
        self._event_buffer.clear()
        
        # Deduplicate events to reduce unnecessary traffic
        events_to_send = await self.deduplicate_events(events_to_send)
        
        # Group events by target clients
        client_events: Dict[str, List[SyncEvent]] = defaultdict(list)
        
        for event in events_to_send:
            target_clients = self._get_target_clients(event)
            for client_id in target_clients:
                client_events[client_id].append(event)
        
        # Optimize batch settings based on current connection count
        if len(self._connections) != getattr(self, '_last_connection_count', 0):
            self._optimize_batch_settings_sync(len(self._connections))
            self._last_connection_count = len(self._connections)
        
        # Send events to each client
        send_tasks = []
        for client_id, events in client_events.items():
            if client_id in self._connections:
                task = asyncio.create_task(
                    self._send_events_to_client(self._connections[client_id], events)
                )
                send_tasks.append(task)
        
        # Wait for all sends to complete
        if send_tasks:
            results = await asyncio.gather(*send_tasks, return_exceptions=True)
            
            # Log any exceptions that occurred
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"Error in send task {i}: {result}")
    
    def _get_target_clients(self, event: SyncEvent) -> Set[str]:
        """Determine which clients should receive this event."""
        target_clients = set()
        
        for connection_id, subscriptions in self._subscriptions.items():
            # Check if client is subscribed to this event type
            if event.event_type in subscriptions:
                target_clients.add(connection_id)
                continue
                
            # Check if client is subscribed to this specific record
            if event.record_id in subscriptions:
                target_clients.add(connection_id)
                continue
                
            # Check for wildcard subscription
            if "*" in subscriptions:
                target_clients.add(connection_id)
        
        return target_clients
    
    async def _send_events_to_client(self, connection: ClientConnection, 
                                   events: List[SyncEvent]) -> None:
        """Send events to a specific client connection with optimization."""
        try:
            # Serialize events for transmission
            messages = []
            for event in events:
                message = {
                    "type": "sync_event",
                    "event_type": event.event_type,
                    "record_id": event.record_id,
                    "data": event.data,
                    "version": event.version,
                    "timestamp": event.timestamp.isoformat(),
                    "user_id": event.user_id
                }
                messages.append(message)
            
            # Optimize payload based on batch size
            if len(messages) == 1:
                payload_data = messages[0]
            else:
                # Use batch format for multiple events
                payload_data = {
                    "type": "sync_batch",
                    "events": messages,
                    "count": len(messages),
                    "batch_id": f"batch_{datetime.now().timestamp()}"
                }
                
                # Add compression hint for large batches
                if len(messages) >= self._compression_threshold:
                    payload_data["compression_recommended"] = True
                    self._metrics["compression_used"] += 1
            
            # Serialize to JSON
            payload = json.dumps(payload_data, separators=(',', ':'))  # Compact JSON
            
            # Send the payload
            await connection.websocket.send(payload)
            
            # Update metrics and connection state
            connection.last_seen = datetime.now()
            self._metrics["total_clients_notified"] += 1
            if len(messages) > 1:
                self._metrics["batches_sent"] += 1
            
            logger.debug(f"Sent {len(events)} events to client {connection.connection_id}")
            
        except ConnectionClosed:
            logger.warning(f"Connection closed for client {connection.connection_id}")
            # Remove the connection
            await self.unsubscribe_client(connection.connection_id)
        except Exception as e:
            logger.error(f"Error sending events to client {connection.connection_id}: {e}")
    
    def get_connection_count(self) -> int:
        """Get the number of active connections."""
        return len(self._connections)
    
    def get_subscription_count(self) -> int:
        """Get the total number of subscriptions across all clients."""
        return sum(len(subs) for subs in self._subscriptions.values())
    
    async def get_client_subscriptions(self, connection_id: str) -> Set[str]:
        """Get subscriptions for a specific client."""
        return self._subscriptions.get(connection_id, set()).copy()
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get performance metrics for the event broker."""
        return {
            **self._metrics,
            "active_connections": len(self._connections),
            "total_subscriptions": sum(len(subs) for subs in self._subscriptions.values()),
            "buffer_size": len(self._event_buffer),
            "batch_size_config": self._batch_size,
            "batch_timeout_config": self._batch_timeout
        }
    
    def reset_metrics(self) -> None:
        """Reset performance metrics."""
        self._metrics = {
            "events_published": 0,
            "batches_sent": 0,
            "total_clients_notified": 0,
            "compression_used": 0
        }
    
    def _optimize_batch_settings_sync(self, connection_count: int) -> None:
        """Synchronously optimize batch settings (called when lock is already held)."""
        if connection_count <= 5:
            # Small number of clients - prioritize latency
            self._batch_size = 10
            self._batch_timeout = 0.05  # 50ms
        elif connection_count <= 20:
            # Medium number of clients - balance latency and throughput
            self._batch_size = 25
            self._batch_timeout = 0.1   # 100ms
        else:
            # Large number of clients - prioritize throughput
            self._batch_size = 50
            self._batch_timeout = 0.2   # 200ms
            
        logger.info(f"Optimized batch settings for {connection_count} connections: "
                   f"batch_size={self._batch_size}, timeout={self._batch_timeout}s")

    async def optimize_batch_settings(self, connection_count: int) -> None:
        """Dynamically optimize batch settings based on connection count."""
        async with self._lock:
            self._optimize_batch_settings_sync(connection_count)
    
    async def deduplicate_events(self, events: List[SyncEvent]) -> List[SyncEvent]:
        """Remove duplicate events to optimize batch processing."""
        if len(events) <= 1:
            return events
            
        # Group events by record_id and keep only the latest version
        record_events: Dict[str, SyncEvent] = {}
        
        for event in events:
            key = f"{event.record_id}:{event.event_type}"
            
            # Keep the event with the highest version number
            if (key not in record_events or 
                event.version > record_events[key].version or
                event.timestamp > record_events[key].timestamp):
                record_events[key] = event
        
        deduplicated = list(record_events.values())
        
        if len(deduplicated) < len(events):
            logger.debug(f"Deduplicated {len(events)} events to {len(deduplicated)}")
            
        return deduplicated