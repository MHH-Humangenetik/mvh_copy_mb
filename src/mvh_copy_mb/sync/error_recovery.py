"""Error recovery and rollback mechanisms for sync operations."""

import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Callable, Tuple
from dataclasses import dataclass, field
from enum import Enum

from .exceptions import SyncError, DataIntegrityError, VersionConflictError, BroadcastError, ConnectionError
from .models import SyncEvent


class RecoveryAction(Enum):
    """Types of recovery actions."""
    RETRY = "retry"
    ROLLBACK = "rollback"
    COMPENSATE = "compensate"
    IGNORE = "ignore"
    ESCALATE = "escalate"


@dataclass
class RecoveryStep:
    """Represents a single recovery step."""
    action: RecoveryAction
    description: str
    execute_func: Callable
    args: tuple = field(default_factory=tuple)
    kwargs: dict = field(default_factory=dict)
    max_attempts: int = 3
    timeout_seconds: float = 30.0


@dataclass
class OperationSnapshot:
    """Snapshot of operation state for rollback."""
    operation_id: str
    timestamp: datetime
    operation_type: str
    affected_records: List[str]
    original_data: Dict[str, Any]
    user_id: str
    version_info: Dict[str, int]


class ErrorRecoveryManager:
    """Manages error recovery and rollback operations."""
    
    def __init__(self, max_snapshots: int = 1000):
        """Initialize error recovery manager.
        
        Args:
            max_snapshots: Maximum number of operation snapshots to keep
        """
        self.max_snapshots = max_snapshots
        self._snapshots: Dict[str, OperationSnapshot] = {}
        self._recovery_strategies: Dict[type, List[RecoveryStep]] = {}
        self._active_recoveries: Dict[str, asyncio.Task] = {}
        
        self.logger = logging.getLogger(__name__)
        
        # Register default recovery strategies
        self._register_default_strategies()
    
    def _register_default_strategies(self) -> None:
        """Register default recovery strategies for common errors."""
        
        # Version conflicts should NOT be retried - they are business logic conflicts
        self._recovery_strategies[VersionConflictError] = [
            RecoveryStep(
                RecoveryAction.ESCALATE,
                "Version conflict requires manual resolution",
                self._escalate_error
            )
        ]
        
        # Broadcast errors can be retried
        self._recovery_strategies[BroadcastError] = [
            RecoveryStep(
                RecoveryAction.RETRY,
                "Retry broadcast with exponential backoff",
                self._retry_with_backoff,
                max_attempts=3
            ),
            RecoveryStep(
                RecoveryAction.ESCALATE,
                "Escalate to manual intervention",
                self._escalate_error
            )
        ]
        
        # Connection errors can be retried
        self._recovery_strategies[ConnectionError] = [
            RecoveryStep(
                RecoveryAction.RETRY,
                "Retry connection with exponential backoff",
                self._retry_with_backoff,
                max_attempts=3
            ),
            RecoveryStep(
                RecoveryAction.ESCALATE,
                "Escalate to manual intervention",
                self._escalate_error
            )
        ]
        
        # Data integrity error recovery
        self._recovery_strategies[DataIntegrityError] = [
            RecoveryStep(
                RecoveryAction.ROLLBACK,
                "Rollback to last known good state",
                self._rollback_operation
            ),
            RecoveryStep(
                RecoveryAction.COMPENSATE,
                "Apply compensating transaction",
                self._apply_compensation
            )
        ]
    
    async def create_snapshot(self, operation_id: str, operation_type: str,
                            affected_records: List[str], original_data: Dict[str, Any],
                            user_id: str, version_info: Dict[str, int]) -> None:
        """Create a snapshot of operation state for potential rollback.
        
        Args:
            operation_id: Unique identifier for the operation
            operation_type: Type of operation (e.g., 'record_update', 'bulk_update')
            affected_records: List of record IDs affected by operation
            original_data: Original data before operation
            user_id: User performing the operation
            version_info: Version information for affected records
        """
        snapshot = OperationSnapshot(
            operation_id=operation_id,
            timestamp=datetime.now(),
            operation_type=operation_type,
            affected_records=affected_records,
            original_data=original_data,
            user_id=user_id,
            version_info=version_info
        )
        
        self._snapshots[operation_id] = snapshot
        
        # Clean up old snapshots if we exceed the limit
        if len(self._snapshots) > self.max_snapshots:
            oldest_id = min(self._snapshots.keys(), 
                          key=lambda k: self._snapshots[k].timestamp)
            del self._snapshots[oldest_id]
        
        self.logger.debug(f"Created snapshot for operation {operation_id}")
    
    async def handle_error(self, error: Exception, operation_id: str,
                         context: Optional[Dict[str, Any]] = None) -> bool:
        """Handle an error using registered recovery strategies.
        
        Args:
            error: The exception that occurred
            operation_id: ID of the failed operation
            context: Additional context for recovery
            
        Returns:
            True if error was successfully recovered, False otherwise
        """
        error_type = type(error)
        context = context or {}
        
        self.logger.warning(f"Handling error for operation {operation_id}: {error}")
        
        # Find recovery strategies for this error type
        strategies = self._get_recovery_strategies(error_type)
        
        if not strategies:
            self.logger.error(f"No recovery strategies found for {error_type}")
            return False
        
        # Execute recovery strategies in order
        for strategy in strategies:
            try:
                success = await self._execute_recovery_step(
                    strategy, error, operation_id, context
                )
                
                if success:
                    self.logger.info(
                        f"Successfully recovered from error using {strategy.action.value}"
                    )
                    return True
                    
            except Exception as recovery_error:
                self.logger.error(
                    f"Recovery step {strategy.action.value} failed: {recovery_error}"
                )
                continue
        
        self.logger.error(f"All recovery strategies failed for operation {operation_id}")
        return False
    
    def _get_recovery_strategies(self, error_type: type) -> List[RecoveryStep]:
        """Get recovery strategies for an error type."""
        # Check for exact match first
        if error_type in self._recovery_strategies:
            return self._recovery_strategies[error_type]
        
        # Check for parent class matches
        for registered_type, strategies in self._recovery_strategies.items():
            if issubclass(error_type, registered_type):
                return strategies
        
        return []
    
    async def _execute_recovery_step(self, step: RecoveryStep, error: Exception,
                                   operation_id: str, context: Dict[str, Any]) -> bool:
        """Execute a single recovery step.
        
        Args:
            step: Recovery step to execute
            error: Original error
            operation_id: Operation ID
            context: Recovery context
            
        Returns:
            True if recovery step succeeded
        """
        self.logger.info(f"Executing recovery step: {step.description}")
        
        try:
            # Execute with timeout
            result = await asyncio.wait_for(
                step.execute_func(error, operation_id, context, *step.args, **step.kwargs),
                timeout=step.timeout_seconds
            )
            return bool(result)
            
        except asyncio.TimeoutError:
            self.logger.error(f"Recovery step timed out after {step.timeout_seconds}s")
            return False
        except Exception as e:
            self.logger.error(f"Recovery step failed: {e}")
            return False
    
    async def _retry_with_backoff(self, error: Exception, operation_id: str,
                                context: Dict[str, Any]) -> bool:
        """Retry operation with exponential backoff."""
        max_attempts = 3
        base_delay = 1.0
        
        for attempt in range(max_attempts):
            if attempt > 0:
                delay = base_delay * (2 ** (attempt - 1))
                self.logger.info(f"Retrying operation {operation_id} in {delay}s (attempt {attempt + 1})")
                await asyncio.sleep(delay)
            
            try:
                # Get retry function from context
                retry_func = context.get('retry_func')
                if not retry_func:
                    self.logger.error("No retry function provided in context")
                    return False
                
                # Execute retry - always await since we're in async context
                result = retry_func()
                if asyncio.iscoroutine(result):
                    await result
                
                self.logger.info(f"Retry successful for operation {operation_id}")
                return True
                
            except Exception as retry_error:
                self.logger.warning(f"Retry attempt {attempt + 1} failed: {retry_error}")
                if attempt == max_attempts - 1:
                    return False
        
        return False
    
    async def _rollback_operation(self, error: Exception, operation_id: str,
                                context: Dict[str, Any]) -> bool:
        """Rollback operation to previous state."""
        snapshot = self._snapshots.get(operation_id)
        if not snapshot:
            self.logger.error(f"No snapshot found for operation {operation_id}")
            return False
        
        try:
            # Get rollback function from context
            rollback_func = context.get('rollback_func')
            if not rollback_func:
                self.logger.error("No rollback function provided in context")
                return False
            
            # Execute rollback
            if asyncio.iscoroutinefunction(rollback_func):
                await rollback_func(snapshot)
            else:
                rollback_func(snapshot)
            
            self.logger.info(f"Successfully rolled back operation {operation_id}")
            
            # Clean up snapshot after successful rollback
            del self._snapshots[operation_id]
            return True
            
        except Exception as rollback_error:
            self.logger.error(f"Rollback failed for operation {operation_id}: {rollback_error}")
            return False
    
    async def _apply_compensation(self, error: Exception, operation_id: str,
                                context: Dict[str, Any]) -> bool:
        """Apply compensating transaction."""
        try:
            # Get compensation function from context
            compensate_func = context.get('compensate_func')
            if not compensate_func:
                self.logger.error("No compensation function provided in context")
                return False
            
            # Execute compensation
            if asyncio.iscoroutinefunction(compensate_func):
                await compensate_func(error, operation_id)
            else:
                compensate_func(error, operation_id)
            
            self.logger.info(f"Successfully applied compensation for operation {operation_id}")
            return True
            
        except Exception as comp_error:
            self.logger.error(f"Compensation failed for operation {operation_id}: {comp_error}")
            return False
    
    async def _escalate_error(self, error: Exception, operation_id: str,
                            context: Dict[str, Any]) -> bool:
        """Escalate error for manual intervention."""
        self.logger.critical(
            f"Escalating error for manual intervention - Operation: {operation_id}, "
            f"Error: {error}"
        )
        
        # In a real implementation, this would:
        # 1. Send alerts to administrators
        # 2. Create support tickets
        # 3. Log to monitoring systems
        # 4. Potentially disable affected functionality
        
        return False  # Escalation doesn't "fix" the error
    
    def register_recovery_strategy(self, error_type: type, 
                                 strategies: List[RecoveryStep]) -> None:
        """Register custom recovery strategies for an error type.
        
        Args:
            error_type: Exception type to handle
            strategies: List of recovery steps to execute
        """
        self._recovery_strategies[error_type] = strategies
        self.logger.info(f"Registered {len(strategies)} recovery strategies for {error_type}")
    
    def get_snapshot(self, operation_id: str) -> Optional[OperationSnapshot]:
        """Get operation snapshot by ID."""
        return self._snapshots.get(operation_id)
    
    def cleanup_old_snapshots(self, max_age_hours: int = 24) -> int:
        """Clean up old snapshots.
        
        Args:
            max_age_hours: Maximum age of snapshots to keep
            
        Returns:
            Number of snapshots cleaned up
        """
        cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
        old_snapshots = [
            op_id for op_id, snapshot in self._snapshots.items()
            if snapshot.timestamp < cutoff_time
        ]
        
        for op_id in old_snapshots:
            del self._snapshots[op_id]
        
        if old_snapshots:
            self.logger.info(f"Cleaned up {len(old_snapshots)} old snapshots")
        
        return len(old_snapshots)
    
    def get_recovery_metrics(self) -> Dict[str, Any]:
        """Get recovery manager metrics."""
        return {
            "active_snapshots": len(self._snapshots),
            "active_recoveries": len(self._active_recoveries),
            "registered_strategies": len(self._recovery_strategies),
            "oldest_snapshot": (
                min(s.timestamp for s in self._snapshots.values()).isoformat()
                if self._snapshots else None
            )
        }