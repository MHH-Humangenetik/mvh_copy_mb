"""Data models for multi-user synchronization."""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Dict, Any, Set
from pydantic import BaseModel, Field
from websockets.asyncio.server import ServerConnection


class EventType(Enum):
    """Types of synchronization events."""
    RECORD_UPDATED = "record_updated"
    RECORD_ADDED = "record_added"
    RECORD_DELETED = "record_deleted"
    RECORD_LOCKED = "record_locked"
    RECORD_UNLOCKED = "record_unlocked"
    BULK_UPDATE = "bulk_update"


class LockState(Enum):
    """States of record locks."""
    ACQUIRED = "acquired"
    RELEASED = "released"
    EXPIRED = "expired"


@dataclass
class SyncEvent:
    """Represents a synchronization event to be broadcast to clients."""
    event_type: str
    record_id: str
    data: Dict[str, Any]
    version: int
    timestamp: datetime
    user_id: str


@dataclass
class RecordLock:
    """Represents a lock on a specific record."""
    record_id: str
    user_id: str
    version: int
    acquired_at: datetime
    expires_at: datetime
    state: LockState = LockState.ACQUIRED


@dataclass
class ClientConnection:
    """Represents an active client WebSocket connection."""
    connection_id: str
    user_id: str
    websocket: ServerConnection
    last_seen: datetime
    subscriptions: Set[str]


# Pydantic models for WebSocket message validation

class WebSocketMessage(BaseModel):
    """Base WebSocket message structure."""
    type: str
    timestamp: datetime = Field(default_factory=datetime.now)


class LockRequest(WebSocketMessage):
    """Request to acquire a lock on a record."""
    type: str = "lock_request"
    record_id: str
    user_id: str


class LockRelease(WebSocketMessage):
    """Request to release a lock on a record."""
    type: str = "lock_release"
    record_id: str
    user_id: str


class DataUpdate(WebSocketMessage):
    """Notification of data update."""
    type: str = "data_update"
    record_id: str
    data: Dict[str, Any]
    version: int
    user_id: str


class ConnectionHeartbeat(WebSocketMessage):
    """Heartbeat message to maintain connection."""
    type: str = "heartbeat"
    connection_id: str


class ErrorMessage(WebSocketMessage):
    """Error message for client notification."""
    type: str = "error"
    error_code: str
    message: str
    details: Dict[str, Any] = Field(default_factory=dict)


class ConflictNotification(WebSocketMessage):
    """Notification of a conflict during synchronization."""
    type: str = "conflict"
    record_id: str
    conflict_type: str
    message: str
    conflicting_user: str