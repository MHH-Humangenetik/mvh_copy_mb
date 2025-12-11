"""Configuration for synchronization system."""

import os
from dataclasses import dataclass
from typing import Optional, Dict, Any
import logging
from pathlib import Path


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
    
    def validate(self) -> None:
        """Validate configuration parameters."""
        errors = []
        
        # Validate port ranges
        if not (1 <= self.websocket_port <= 65535):
            errors.append(f"WebSocket port must be between 1-65535, got {self.websocket_port}")
        
        # Validate timeouts
        if self.connection_timeout_seconds <= 0:
            errors.append(f"Connection timeout must be positive, got {self.connection_timeout_seconds}")
        
        if self.heartbeat_interval_seconds <= 0:
            errors.append(f"Heartbeat interval must be positive, got {self.heartbeat_interval_seconds}")
        
        if self.default_lock_timeout_seconds <= 0:
            errors.append(f"Lock timeout must be positive, got {self.default_lock_timeout_seconds}")
        
        # Validate batch settings
        if self.max_batch_size <= 0:
            errors.append(f"Max batch size must be positive, got {self.max_batch_size}")
        
        if self.batch_timeout_milliseconds <= 0:
            errors.append(f"Batch timeout must be positive, got {self.batch_timeout_milliseconds}")
        
        if self.event_buffer_size <= 0:
            errors.append(f"Event buffer size must be positive, got {self.event_buffer_size}")
        
        # Validate reconnection settings
        if self.max_reconnection_attempts < 0:
            errors.append(f"Max reconnection attempts must be non-negative, got {self.max_reconnection_attempts}")
        
        if self.initial_reconnection_delay_seconds <= 0:
            errors.append(f"Initial reconnection delay must be positive, got {self.initial_reconnection_delay_seconds}")
        
        if self.max_reconnection_delay_seconds <= 0:
            errors.append(f"Max reconnection delay must be positive, got {self.max_reconnection_delay_seconds}")
        
        if self.reconnection_backoff_multiplier <= 1.0:
            errors.append(f"Reconnection backoff multiplier must be > 1.0, got {self.reconnection_backoff_multiplier}")
        
        # Validate log level
        valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if self.log_level.upper() not in valid_log_levels:
            errors.append(f"Log level must be one of {valid_log_levels}, got {self.log_level}")
        
        # Validate connection limits
        if self.max_connections_per_user <= 0:
            errors.append(f"Max connections per user must be positive, got {self.max_connections_per_user}")
        
        if errors:
            raise ValueError(f"Configuration validation failed: {'; '.join(errors)}")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            "websocket_host": self.websocket_host,
            "websocket_port": self.websocket_port,
            "websocket_path": self.websocket_path,
            "connection_timeout_seconds": self.connection_timeout_seconds,
            "heartbeat_interval_seconds": self.heartbeat_interval_seconds,
            "max_connections_per_user": self.max_connections_per_user,
            "default_lock_timeout_seconds": self.default_lock_timeout_seconds,
            "lock_cleanup_interval_seconds": self.lock_cleanup_interval_seconds,
            "max_batch_size": self.max_batch_size,
            "batch_timeout_milliseconds": self.batch_timeout_milliseconds,
            "event_buffer_size": self.event_buffer_size,
            "max_reconnection_attempts": self.max_reconnection_attempts,
            "initial_reconnection_delay_seconds": self.initial_reconnection_delay_seconds,
            "max_reconnection_delay_seconds": self.max_reconnection_delay_seconds,
            "reconnection_backoff_multiplier": self.reconnection_backoff_multiplier,
            "log_level": self.log_level,
            "log_sync_events": self.log_sync_events,
            "log_connection_events": self.log_connection_events,
            "log_conflict_events": self.log_conflict_events,
            "enable_event_batching": self.enable_event_batching,
            "enable_connection_pooling": self.enable_connection_pooling,
            "enable_metrics_collection": self.enable_metrics_collection,
        }
    
    @classmethod
    def from_env(cls) -> "SyncConfig":
        """Create configuration from environment variables with validation."""
        try:
            config = cls(
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
            
            # Validate the configuration
            config.validate()
            return config
            
        except ValueError as e:
            if "invalid literal" in str(e):
                raise ValueError(f"Invalid environment variable format: {e}")
            raise
        except Exception as e:
            raise ValueError(f"Failed to load configuration from environment: {e}")
    
    @classmethod
    def from_file(cls, config_path: str) -> "SyncConfig":
        """Load configuration from a .env file."""
        from dotenv import load_dotenv
        
        config_file = Path(config_path)
        if not config_file.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        # Load environment variables from file
        load_dotenv(config_file)
        
        # Create configuration from loaded environment
        return cls.from_env()
    
    def get_production_overrides(self) -> Dict[str, Any]:
        """Get recommended production configuration overrides."""
        return {
            "log_level": "WARNING",
            "enable_metrics_collection": True,
            "connection_timeout_seconds": 600,  # 10 minutes for production
            "max_connections_per_user": 3,  # Stricter limit for production
            "event_buffer_size": 2000,  # Larger buffer for production
            "max_batch_size": 200,  # Larger batches for efficiency
        }
    
    def apply_production_settings(self) -> "SyncConfig":
        """Apply production-ready configuration settings."""
        overrides = self.get_production_overrides()
        
        # Create new config with production overrides
        config_dict = self.to_dict()
        config_dict.update(overrides)
        
        # Create new instance with updated values
        new_config = SyncConfig(**config_dict)
        new_config.validate()
        
        return new_config