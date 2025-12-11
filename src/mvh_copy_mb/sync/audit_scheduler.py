"""Scheduled tasks for audit trail maintenance."""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional

from .audit_manager import AuditTrailManager
from .audit_config import AuditConfig, get_audit_config

logger = logging.getLogger(__name__)


class AuditScheduler:
    """Manages scheduled audit trail maintenance tasks."""
    
    def __init__(self, audit_manager: AuditTrailManager, config: Optional[AuditConfig] = None):
        """Initialize audit scheduler.
        
        Args:
            audit_manager: Audit trail manager instance
            config: Audit configuration (uses global config if None)
        """
        self.audit_manager = audit_manager
        self.config = config or get_audit_config()
        self._cleanup_task: Optional[asyncio.Task] = None
        self._running = False
    
    async def start(self) -> None:
        """Start scheduled audit maintenance tasks."""
        if self._running:
            logger.warning("Audit scheduler is already running")
            return
        
        self._running = True
        
        # Start cleanup task
        self._cleanup_task = asyncio.create_task(self._periodic_cleanup())
        
        logger.info(f"Audit scheduler started with cleanup interval: {self.config.cleanup_interval_hours} hours")
    
    async def stop(self) -> None:
        """Stop scheduled audit maintenance tasks."""
        if not self._running:
            return
        
        self._running = False
        
        # Cancel cleanup task
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            self._cleanup_task = None
        
        logger.info("Audit scheduler stopped")
    
    async def _periodic_cleanup(self) -> None:
        """Periodic task to clean up old audit events."""
        cleanup_interval = timedelta(hours=self.config.cleanup_interval_hours)
        
        while self._running:
            try:
                # Wait for the cleanup interval
                await asyncio.sleep(cleanup_interval.total_seconds())
                
                if not self._running:
                    break
                
                logger.info("Starting scheduled audit cleanup")
                
                # Perform cleanup
                stats = await self._run_cleanup()
                
                if stats["events_deleted"] > 0 or stats["events_archived"] > 0:
                    logger.info(
                        f"Audit cleanup completed: "
                        f"deleted {stats['events_deleted']}, "
                        f"archived {stats['events_archived']} events"
                    )
                else:
                    logger.debug("Audit cleanup completed: no events to clean up")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in periodic audit cleanup: {e}", exc_info=True)
                # Wait a shorter time on error before retrying
                await asyncio.sleep(3600)  # 1 hour
    
    async def _run_cleanup(self) -> dict:
        """Run audit cleanup and return statistics."""
        try:
            # Run cleanup (not dry run)
            stats = self.audit_manager.cleanup_old_audit_events(dry_run=False)
            
            # Log detailed statistics if verbose logging is enabled
            if logger.isEnabledFor(logging.DEBUG) and stats.get("by_event_type"):
                logger.debug("Cleanup statistics by event type:")
                for event_type, type_stats in stats["by_event_type"].items():
                    if type_stats["deleted"] > 0 or type_stats["archived"] > 0:
                        logger.debug(
                            f"  {event_type}: deleted {type_stats['deleted']}, "
                            f"archived {type_stats['archived']}"
                        )
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to run audit cleanup: {e}", exc_info=True)
            return {
                "events_deleted": 0,
                "events_archived": 0,
                "error": str(e)
            }
    
    async def run_manual_cleanup(self, dry_run: bool = False) -> dict:
        """Run manual audit cleanup.
        
        Args:
            dry_run: If True, only show what would be cleaned up
            
        Returns:
            Dictionary with cleanup statistics
        """
        logger.info(f"Running manual audit cleanup (dry_run={dry_run})")
        
        try:
            stats = self.audit_manager.cleanup_old_audit_events(dry_run=dry_run)
            
            if dry_run:
                logger.info(
                    f"Manual cleanup dry run: would delete {stats['events_to_delete']}, "
                    f"archive {stats['events_to_archive']} events"
                )
            else:
                logger.info(
                    f"Manual cleanup completed: deleted {stats['events_deleted']}, "
                    f"archived {stats['events_archived']} events"
                )
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to run manual audit cleanup: {e}", exc_info=True)
            raise
    
    async def get_cleanup_preview(self) -> dict:
        """Get a preview of what would be cleaned up without actually doing it.
        
        Returns:
            Dictionary with cleanup preview statistics
        """
        try:
            return self.audit_manager.cleanup_old_audit_events(dry_run=True)
        except Exception as e:
            logger.error(f"Failed to get cleanup preview: {e}", exc_info=True)
            raise
    
    def is_running(self) -> bool:
        """Check if the scheduler is currently running."""
        return self._running
    
    def get_next_cleanup_time(self) -> Optional[datetime]:
        """Get the estimated time of the next scheduled cleanup.
        
        Returns:
            Datetime of next cleanup, or None if scheduler is not running
        """
        if not self._running or not self._cleanup_task:
            return None
        
        # This is an approximation - the actual time depends on when the task was started
        cleanup_interval = timedelta(hours=self.config.cleanup_interval_hours)
        return datetime.now() + cleanup_interval
    
    def get_status(self) -> dict:
        """Get current scheduler status.
        
        Returns:
            Dictionary with scheduler status information
        """
        return {
            "running": self._running,
            "cleanup_interval_hours": self.config.cleanup_interval_hours,
            "next_cleanup_time": self.get_next_cleanup_time().isoformat() if self.get_next_cleanup_time() else None,
            "task_status": "running" if self._cleanup_task and not self._cleanup_task.done() else "stopped"
        }