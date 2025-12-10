"""Base interfaces for synchronization components."""

from abc import ABC, abstractmethod
from typing import List, Optional, Set, Dict, Any
from datetime import datetime

from .models import SyncEvent, RecordLock, ClientConnection


class EventBroker(ABC):
    """Interface for event broadcasting system."""
    
    @abstractmethod
    async def publish_event(self, event: SyncEvent) -> None:
        """Publish a synchronization event to all subscribed clients."""
        pass
    
    @abstractmethod
    async def subscribe_client(self, connection: ClientConnection, 
                             subscriptions: Set[str]) -> None:
        """Subscribe a client to specific event types or record IDs."""
        pass
    
    @abstractmethod
    async def unsubscribe_client(self, connection_id: str) -> None:
        """Unsubscribe a client from all events."""
        pass
    
    @abstractmethod
    async def publish_bulk_events(self, events: List[SyncEvent]) -> None:
        """Publish multiple events efficiently as a batch."""
        pass


class LockManager(ABC):
    """Interface for record locking system."""
    
    @abstractmethod
    async def acquire_lock(self, record_id: str, user_id: str, 
                          version: int, timeout_seconds: int = 30) -> Optional[RecordLock]:
        """Attempt to acquire a lock on a record."""
        pass
    
    @abstractmethod
    async def release_lock(self, record_id: str, user_id: str) -> bool:
        """Release a lock held by a user."""
        pass
    
    @abstractmethod
    async def check_lock(self, record_id: str) -> Optional[RecordLock]:
        """Check if a record is currently locked."""
        pass
    
    @abstractmethod
    async def cleanup_expired_locks(self) -> List[RecordLock]:
        """Clean up expired locks and return the list of cleaned locks."""
        pass
    
    @abstractmethod
    async def release_user_locks(self, user_id: str) -> List[RecordLock]:
        """Release all locks held by a specific user."""
        pass


class ConnectionManager(ABC):
    """Interface for WebSocket connection management."""
    
    @abstractmethod
    async def add_connection(self, connection: ClientConnection) -> None:
        """Add a new client connection."""
        pass
    
    @abstractmethod
    async def remove_connection(self, connection_id: str) -> Optional[ClientConnection]:
        """Remove a client connection."""
        pass
    
    @abstractmethod
    async def get_connection(self, connection_id: str) -> Optional[ClientConnection]:
        """Get a specific connection by ID."""
        pass
    
    @abstractmethod
    async def get_user_connections(self, user_id: str) -> List[ClientConnection]:
        """Get all connections for a specific user."""
        pass
    
    @abstractmethod
    async def get_all_connections(self) -> List[ClientConnection]:
        """Get all active connections."""
        pass
    
    @abstractmethod
    async def update_last_seen(self, connection_id: str) -> None:
        """Update the last seen timestamp for a connection."""
        pass


class SyncService(ABC):
    """Interface for coordinating synchronization operations."""
    
    @abstractmethod
    async def handle_record_update(self, record_id: str, data: Dict[str, Any], 
                                 user_id: str, version: int) -> None:
        """Handle a record update and broadcast to clients."""
        pass
    
    @abstractmethod
    async def handle_bulk_update(self, updates: List[Dict[str, Any]], 
                               user_id: str) -> None:
        """Handle bulk record updates efficiently."""
        pass
    
    @abstractmethod
    async def sync_client(self, connection_id: str, 
                         last_sync_timestamp: Optional[datetime] = None) -> None:
        """Synchronize a client with missed updates."""
        pass
    
    @abstractmethod
    async def detect_external_changes(self) -> List[SyncEvent]:
        """Detect changes made outside the sync system."""
        pass