"""Configuration for synchronization system."""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class SyncConfig:
    """Configuration settings for the synchronization system."""
    
    # WebSocket settings
    websocket_host: str = "localhost"
    websocket_port: int = 8001
    websocket_path: str = "/ws"
    
    # Connection settings
    connection_timeout_seconds: int = 300  # 5 minutes
    heartbeat_interval_seconds: int = 30
    max_connections_per_user: int = 5
    
    # Lock settings
    default_lock_timeout_seconds: int = 30
    lock_cleanup_interval_seconds: int = 60
    
    # Event settings
    max_batch_size: int = 100
    batch_timeout_milliseconds: int = 100
    event_buffer_size: int = 1000
    
    # Reconnection settings
    max_reconnection_attempts: int = 5
    initial_reconnection_delay_seconds: float = 1.0
    max_reconnection_delay_seconds: float = 30.0
    reconnection_backoff_multiplier: float = 2.0
    
    # Logging settings
    log_level: str = "INFO"
    log_sync_events: bool = True
    log_connection_events: bool = True
    log_conflict_events: bool = True
    
    # Performance settings
    enable_event_batching: bool = True
    enable_connection_pooling: bool = True
    enable_metrics_collection: bool = True
    
    @classmethod
    def from_env(cls) -> "SyncConfig":
        """Create configuration from environment variables."""
        return cls(
            websocket_host=os.getenv("SYNC_WEBSOCKET_HOST", "localhost"),
            websocket_port=int(os.getenv("SYNC_WEBSOCKET_PORT", "8001")),
            websocket_path=os.getenv("SYNC_WEBSOCKET_PATH", "/ws"),
            
            connection_timeout_seconds=int(os.getenv("SYNC_CONNECTION_TIMEOUT", "300")),
            heartbeat_interval_seconds=int(os.getenv("SYNC_HEARTBEAT_INTERVAL", "30")),
            max_connections_per_user=int(os.getenv("SYNC_MAX_CONNECTIONS_PER_USER", "5")),
            
            default_lock_timeout_seconds=int(os.getenv("SYNC_LOCK_TIMEOUT", "30")),
            lock_cleanup_interval_seconds=int(os.getenv("SYNC_LOCK_CLEANUP_INTERVAL", "60")),
            
            max_batch_size=int(os.getenv("SYNC_MAX_BATCH_SIZE", "100")),
            batch_timeout_milliseconds=int(os.getenv("SYNC_BATCH_TIMEOUT", "100")),
            event_buffer_size=int(os.getenv("SYNC_EVENT_BUFFER_SIZE", "1000")),
            
            max_reconnection_attempts=int(os.getenv("SYNC_MAX_RECONNECTION_ATTEMPTS", "5")),
            initial_reconnection_delay_seconds=float(os.getenv("SYNC_INITIAL_RECONNECTION_DELAY", "1.0")),
            max_reconnection_delay_seconds=float(os.getenv("SYNC_MAX_RECONNECTION_DELAY", "30.0")),
            reconnection_backoff_multiplier=float(os.getenv("SYNC_RECONNECTION_BACKOFF_MULTIPLIER", "2.0")),
            
            log_level=os.getenv("SYNC_LOG_LEVEL", "INFO"),
            log_sync_events=os.getenv("SYNC_LOG_SYNC_EVENTS", "true").lower() == "true",
            log_connection_events=os.getenv("SYNC_LOG_CONNECTION_EVENTS", "true").lower() == "true",
            log_conflict_events=os.getenv("SYNC_LOG_CONFLICT_EVENTS", "true").lower() == "true",
            
            enable_event_batching=os.getenv("SYNC_ENABLE_EVENT_BATCHING", "true").lower() == "true",
            enable_connection_pooling=os.getenv("SYNC_ENABLE_CONNECTION_POOLING", "true").lower() == "true",
            enable_metrics_collection=os.getenv("SYNC_ENABLE_METRICS_COLLECTION", "true").lower() == "true",
        )