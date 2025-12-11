"""Configuration utilities for the synchronization system."""

import os
import sys
from pathlib import Path
from typing import Dict, Any, Optional
import logging

from .config import SyncConfig


logger = logging.getLogger(__name__)


class ConfigurationManager:
    """Manages configuration loading and validation for the sync system."""
    
    def __init__(self):
        self._config: Optional[SyncConfig] = None
        self._config_source: Optional[str] = None
    
    def load_config(self, config_path: Optional[str] = None, 
                   environment: str = "development") -> SyncConfig:
        """
        Load configuration from various sources with environment-specific defaults.
        
        Args:
            config_path: Optional path to configuration file
            environment: Environment type ('development', 'production', 'testing')
        
        Returns:
            Validated SyncConfig instance
        """
        try:
            if config_path:
                # Load from specific file
                self._config = SyncConfig.from_file(config_path)
                self._config_source = f"file:{config_path}"
                logger.info(f"Configuration loaded from file: {config_path}")
            else:
                # Load from environment variables
                self._config = SyncConfig.from_env()
                self._config_source = "environment"
                logger.info("Configuration loaded from environment variables")
            
            # Apply environment-specific settings
            if environment == "production":
                self._config = self._config.apply_production_settings()
                logger.info("Applied production configuration overrides")
            elif environment == "testing":
                self._config = self._apply_testing_overrides(self._config)
                logger.info("Applied testing configuration overrides")
            
            # Log configuration summary
            self._log_config_summary()
            
            return self._config
            
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            raise
    
    def get_config(self) -> SyncConfig:
        """Get the current configuration."""
        if self._config is None:
            raise RuntimeError("Configuration not loaded. Call load_config() first.")
        return self._config
    
    def reload_config(self) -> SyncConfig:
        """Reload configuration from the same source."""
        if self._config_source is None:
            raise RuntimeError("No configuration source available for reload")
        
        if self._config_source.startswith("file:"):
            config_path = self._config_source[5:]  # Remove "file:" prefix
            return self.load_config(config_path)
        else:
            return self.load_config()
    
    def validate_environment(self) -> Dict[str, Any]:
        """
        Validate the current environment for sync system deployment.
        
        Returns:
            Dictionary with validation results and recommendations
        """
        results = {
            "valid": True,
            "warnings": [],
            "errors": [],
            "recommendations": []
        }
        
        if self._config is None:
            results["errors"].append("No configuration loaded")
            results["valid"] = False
            return results
        
        # Check WebSocket port availability
        if self._config.websocket_port < 1024 and os.getuid() != 0:
            results["warnings"].append(
                f"WebSocket port {self._config.websocket_port} requires root privileges"
            )
        
        # Check resource limits
        if self._config.max_connections_per_user > 10:
            results["warnings"].append(
                "High connection limit per user may impact performance"
            )
        
        if self._config.event_buffer_size > 5000:
            results["warnings"].append(
                "Large event buffer may consume significant memory"
            )
        
        # Check production readiness
        if self._config.log_level == "DEBUG":
            results["recommendations"].append(
                "Consider using WARNING or INFO log level for production"
            )
        
        if not self._config.enable_metrics_collection:
            results["recommendations"].append(
                "Enable metrics collection for production monitoring"
            )
        
        # Check security settings
        if self._config.websocket_host == "0.0.0.0":
            results["recommendations"].append(
                "Ensure proper firewall rules when binding to all interfaces"
            )
        
        return results
    
    def _apply_testing_overrides(self, config: SyncConfig) -> SyncConfig:
        """Apply testing-specific configuration overrides."""
        overrides = {
            "log_level": "DEBUG",
            "connection_timeout_seconds": 10,  # Shorter timeouts for tests
            "heartbeat_interval_seconds": 5,
            "default_lock_timeout_seconds": 5,
            "max_batch_size": 10,  # Smaller batches for tests
            "event_buffer_size": 100,
            "max_reconnection_attempts": 2,
            "initial_reconnection_delay_seconds": 0.1,
            "max_reconnection_delay_seconds": 1.0,
        }
        
        config_dict = config.to_dict()
        config_dict.update(overrides)
        
        new_config = SyncConfig(**config_dict)
        new_config.validate()
        
        return new_config
    
    def _log_config_summary(self):
        """Log a summary of the current configuration."""
        if self._config is None:
            return
        
        logger.info("Sync system configuration summary:")
        logger.info(f"  WebSocket: {self._config.websocket_host}:{self._config.websocket_port}")
        logger.info(f"  Max connections per user: {self._config.max_connections_per_user}")
        logger.info(f"  Lock timeout: {self._config.default_lock_timeout_seconds}s")
        logger.info(f"  Event batching: {self._config.enable_event_batching}")
        logger.info(f"  Metrics collection: {self._config.enable_metrics_collection}")
        logger.info(f"  Log level: {self._config.log_level}")


def get_config_for_environment(environment: str = None) -> SyncConfig:
    """
    Convenience function to get configuration for a specific environment.
    
    Args:
        environment: Environment name ('development', 'production', 'testing')
                    If None, determined from ENVIRONMENT env var or defaults to 'development'
    
    Returns:
        Configured SyncConfig instance
    """
    if environment is None:
        environment = os.getenv("ENVIRONMENT", "development")
    
    manager = ConfigurationManager()
    
    # Try to load from environment-specific config file first
    config_files = [
        f".env.{environment}",
        ".env.local",
        ".env"
    ]
    
    for config_file in config_files:
        if Path(config_file).exists():
            return manager.load_config(config_file, environment)
    
    # Fall back to environment variables
    return manager.load_config(environment=environment)


def print_config_help():
    """Print help information about configuration options."""
    help_text = """
Multi-User Synchronization Configuration Help

Environment Variables:
  SYNC_WEBSOCKET_HOST          WebSocket server host (default: localhost)
  SYNC_WEBSOCKET_PORT          WebSocket server port (default: 8001)
  SYNC_WEBSOCKET_PATH          WebSocket endpoint path (default: /ws)
  
  SYNC_CONNECTION_TIMEOUT      Connection timeout in seconds (default: 300)
  SYNC_HEARTBEAT_INTERVAL      Heartbeat interval in seconds (default: 30)
  SYNC_MAX_CONNECTIONS_PER_USER Max connections per user (default: 5)
  
  SYNC_LOCK_TIMEOUT           Lock timeout in seconds (default: 30)
  SYNC_LOCK_CLEANUP_INTERVAL  Lock cleanup interval in seconds (default: 60)
  
  SYNC_MAX_BATCH_SIZE         Maximum batch size for events (default: 100)
  SYNC_BATCH_TIMEOUT          Batch timeout in milliseconds (default: 100)
  SYNC_EVENT_BUFFER_SIZE      Event buffer size (default: 1000)
  
  SYNC_LOG_LEVEL              Log level (DEBUG/INFO/WARNING/ERROR/CRITICAL)
  SYNC_LOG_SYNC_EVENTS        Log sync events (true/false)
  SYNC_LOG_CONNECTION_EVENTS  Log connection events (true/false)
  SYNC_LOG_CONFLICT_EVENTS    Log conflict events (true/false)

Configuration Files:
  .env                        Default configuration file
  .env.production            Production configuration template
  .env.local                 Local overrides (not in version control)
  .env.{environment}         Environment-specific configuration

Usage Examples:
  # Development (default)
  python -m mvh_copy_mb.web
  
  # Production
  ENVIRONMENT=production python -m mvh_copy_mb.web
  
  # Custom config file
  CONFIG_FILE=.env.custom python -m mvh_copy_mb.web
"""
    print(help_text)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "help":
        print_config_help()
    else:
        # Validate current configuration
        try:
            config = get_config_for_environment()
            manager = ConfigurationManager()
            manager._config = config
            
            validation = manager.validate_environment()
            
            print("Configuration validation results:")
            print(f"Valid: {validation['valid']}")
            
            if validation['errors']:
                print("\nErrors:")
                for error in validation['errors']:
                    print(f"  - {error}")
            
            if validation['warnings']:
                print("\nWarnings:")
                for warning in validation['warnings']:
                    print(f"  - {warning}")
            
            if validation['recommendations']:
                print("\nRecommendations:")
                for rec in validation['recommendations']:
                    print(f"  - {rec}")
                    
        except Exception as e:
            print(f"Configuration error: {e}")
            sys.exit(1)