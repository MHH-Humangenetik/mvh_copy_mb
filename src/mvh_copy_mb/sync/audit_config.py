"""Configuration for audit trail system."""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional


@dataclass
class AuditRetentionPolicy:
    """Configuration for audit event retention."""
    retention_days: int
    archive_after_days: Optional[int] = None


@dataclass
class AuditConfig:
    """Configuration for the audit trail system."""
    
    # Database configuration
    audit_db_path: Path
    
    # Retention policies by event type
    retention_policies: Dict[str, AuditRetentionPolicy]
    
    # Performance settings
    batch_size: int = 100
    cleanup_interval_hours: int = 24
    
    # Security settings
    enable_ip_logging: bool = True
    enable_user_agent_logging: bool = True
    
    @classmethod
    def from_environment(cls) -> 'AuditConfig':
        """Create audit configuration from environment variables."""
        
        # Get audit database path
        audit_db_path_str = os.getenv('AUDIT_DB_PATH', './data/audit.duckdb')
        audit_db_path = Path(audit_db_path_str)
        
        # Ensure parent directory exists
        audit_db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Default retention policies (can be overridden by environment)
        default_policies = {
            # Critical events - keep longer
            "system_error": AuditRetentionPolicy(365, 180),
            "system_warning": AuditRetentionPolicy(180, 90),
            "sync_conflict_detected": AuditRetentionPolicy(180, 90),
            "sync_conflict_resolved": AuditRetentionPolicy(180, 90),
            
            # User actions - moderate retention
            "user_login": AuditRetentionPolicy(90, 30),
            "user_logout": AuditRetentionPolicy(90, 30),
            "user_session_start": AuditRetentionPolicy(90, 30),
            "user_session_end": AuditRetentionPolicy(90, 30),
            "record_status_change": AuditRetentionPolicy(365, 180),
            "csv_upload": AuditRetentionPolicy(365, 180),
            "data_export": AuditRetentionPolicy(180, 90),
            "bulk_operation": AuditRetentionPolicy(180, 90),
            
            # Connection events - shorter retention
            "connection_established": AuditRetentionPolicy(30, 7),
            "connection_lost": AuditRetentionPolicy(90, 30),
            "connection_reconnected": AuditRetentionPolicy(90, 30),
            "connection_timeout": AuditRetentionPolicy(90, 30),
            
            # Record operations - moderate retention
            "record_view": AuditRetentionPolicy(30, 7),
            "record_edit_start": AuditRetentionPolicy(180, 90),
            "record_edit_complete": AuditRetentionPolicy(365, 180),
            "record_edit_cancel": AuditRetentionPolicy(90, 30),
            "record_lock_acquire": AuditRetentionPolicy(90, 30),
            "record_lock_release": AuditRetentionPolicy(90, 30),
            "record_lock_timeout": AuditRetentionPolicy(90, 30),
            
            # Sync events - shorter retention
            "sync_event_broadcast": AuditRetentionPolicy(30, 7),
            "sync_event_received": AuditRetentionPolicy(7, 1),
        }
        
        # Allow environment variable overrides for specific event types
        retention_policies = {}
        for event_type, default_policy in default_policies.items():
            retention_days = int(os.getenv(
                f'AUDIT_RETENTION_{event_type.upper()}_DAYS',
                default_policy.retention_days
            ))
            archive_days = default_policy.archive_after_days
            if archive_days is not None:
                archive_days = int(os.getenv(
                    f'AUDIT_ARCHIVE_{event_type.upper()}_DAYS',
                    archive_days
                ))
            
            retention_policies[event_type] = AuditRetentionPolicy(
                retention_days=retention_days,
                archive_after_days=archive_days
            )
        
        # Performance settings
        batch_size = int(os.getenv('AUDIT_BATCH_SIZE', '100'))
        cleanup_interval_hours = int(os.getenv('AUDIT_CLEANUP_INTERVAL_HOURS', '24'))
        
        # Security settings
        enable_ip_logging = os.getenv('AUDIT_ENABLE_IP_LOGGING', 'true').lower() == 'true'
        enable_user_agent_logging = os.getenv('AUDIT_ENABLE_USER_AGENT_LOGGING', 'true').lower() == 'true'
        
        return cls(
            audit_db_path=audit_db_path,
            retention_policies=retention_policies,
            batch_size=batch_size,
            cleanup_interval_hours=cleanup_interval_hours,
            enable_ip_logging=enable_ip_logging,
            enable_user_agent_logging=enable_user_agent_logging
        )
    
    def get_retention_policy(self, event_type: str) -> Optional[AuditRetentionPolicy]:
        """Get retention policy for a specific event type."""
        return self.retention_policies.get(event_type)
    
    def set_retention_policy(self, event_type: str, policy: AuditRetentionPolicy) -> None:
        """Set retention policy for a specific event type."""
        self.retention_policies[event_type] = policy
    
    def get_all_event_types(self) -> list[str]:
        """Get all configured event types."""
        return list(self.retention_policies.keys())


# Global configuration instance
_audit_config: Optional[AuditConfig] = None


def get_audit_config() -> AuditConfig:
    """Get the global audit configuration instance."""
    global _audit_config
    if _audit_config is None:
        _audit_config = AuditConfig.from_environment()
    return _audit_config


def set_audit_config(config: AuditConfig) -> None:
    """Set the global audit configuration instance."""
    global _audit_config
    _audit_config = config