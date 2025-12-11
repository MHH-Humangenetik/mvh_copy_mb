"""Graceful degradation mechanisms for sync system reliability."""

import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable, Any
from enum import Enum
from dataclasses import dataclass

from .exceptions import SyncServiceUnavailableError


class DegradationLevel(Enum):
    """Levels of service degradation."""
    NORMAL = "normal"
    REDUCED = "reduced"
    MINIMAL = "minimal"
    MANUAL_REFRESH = "manual_refresh"
    OFFLINE = "offline"


class DegradationTrigger(Enum):
    """Triggers that can cause service degradation."""
    HIGH_LATENCY = "high_latency"
    CONNECTION_FAILURES = "connection_failures"
    MEMORY_PRESSURE = "memory_pressure"
    CIRCUIT_BREAKER_OPEN = "circuit_breaker_open"
    ERROR_RATE_HIGH = "error_rate_high"
    MANUAL = "manual"


@dataclass
class DegradationRule:
    """Rule for triggering degradation."""
    trigger: DegradationTrigger
    threshold: float
    duration_seconds: float
    target_level: DegradationLevel
    description: str


@dataclass
class DegradationEvent:
    """Event representing a degradation state change."""
    timestamp: datetime
    level: DegradationLevel
    trigger: DegradationTrigger
    reason: str
    metrics: Dict[str, Any]


class GracefulDegradationManager:
    """Manages graceful degradation of sync services."""
    
    def __init__(self, 
                 connection_throttle_threshold: int = 50,
                 latency_threshold_ms: float = 5000.0,
                 error_rate_threshold: float = 0.3,
                 memory_threshold_mb: float = 500.0):
        """Initialize degradation manager.
        
        Args:
            connection_throttle_threshold: Max connections before throttling
            latency_threshold_ms: Latency threshold for degradation (ms)
            error_rate_threshold: Error rate threshold (0.0-1.0)
            memory_threshold_mb: Memory usage threshold (MB)
        """
        self.connection_throttle_threshold = connection_throttle_threshold
        self.latency_threshold_ms = latency_threshold_ms
        self.error_rate_threshold = error_rate_threshold
        self.memory_threshold_mb = memory_threshold_mb
        
        self._current_level = DegradationLevel.NORMAL
        self._degradation_history: List[DegradationEvent] = []
        self._degradation_callbacks: Dict[DegradationLevel, List[Callable]] = {}
        self._metrics: Dict[str, Any] = {
            "connection_count": 0,
            "average_latency_ms": 0.0,
            "error_rate": 0.0,
            "memory_usage_mb": 0.0,
            "last_update": datetime.now()
        }
        
        # Degradation rules
        self._rules = [
            DegradationRule(
                trigger=DegradationTrigger.HIGH_LATENCY,
                threshold=latency_threshold_ms,
                duration_seconds=30.0,
                target_level=DegradationLevel.REDUCED,
                description=f"High latency > {latency_threshold_ms}ms"
            ),
            DegradationRule(
                trigger=DegradationTrigger.CONNECTION_FAILURES,
                threshold=error_rate_threshold,
                duration_seconds=60.0,
                target_level=DegradationLevel.MINIMAL,
                description=f"Error rate > {error_rate_threshold * 100}%"
            ),
            DegradationRule(
                trigger=DegradationTrigger.MEMORY_PRESSURE,
                threshold=memory_threshold_mb,
                duration_seconds=10.0,
                target_level=DegradationLevel.REDUCED,
                description=f"Memory usage > {memory_threshold_mb}MB"
            )
        ]
        
        self._trigger_timestamps: Dict[DegradationTrigger, datetime] = {}
        self._monitoring_task: Optional[asyncio.Task] = None
        
        self.logger = logging.getLogger(__name__)
    
    async def start_monitoring(self) -> None:
        """Start degradation monitoring."""
        if self._monitoring_task is None:
            self._monitoring_task = asyncio.create_task(self._monitoring_loop())
            self.logger.info("Started graceful degradation monitoring")
    
    async def stop_monitoring(self) -> None:
        """Stop degradation monitoring."""
        if self._monitoring_task:
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass
            self._monitoring_task = None
            self.logger.info("Stopped graceful degradation monitoring")
    
    def update_metrics(self, **metrics) -> None:
        """Update system metrics for degradation evaluation.
        
        Args:
            **metrics: Metric values to update
        """
        self._metrics.update(metrics)
        self._metrics["last_update"] = datetime.now()
    
    def register_callback(self, level: DegradationLevel, callback: Callable) -> None:
        """Register callback for degradation level changes.
        
        Args:
            level: Degradation level to monitor
            callback: Function to call when level is reached
        """
        if level not in self._degradation_callbacks:
            self._degradation_callbacks[level] = []
        self._degradation_callbacks[level].append(callback)
    
    async def trigger_degradation(self, trigger: DegradationTrigger, 
                                reason: str, level: Optional[DegradationLevel] = None) -> None:
        """Manually trigger degradation.
        
        Args:
            trigger: What triggered the degradation
            reason: Human-readable reason
            level: Target degradation level (auto-determined if None)
        """
        if level is None:
            # Find appropriate level based on trigger
            for rule in self._rules:
                if rule.trigger == trigger:
                    level = rule.target_level
                    break
            else:
                level = DegradationLevel.REDUCED  # Default fallback
        
        await self._apply_degradation(level, trigger, reason)
    
    async def recover_to_normal(self) -> None:
        """Attempt to recover to normal operation."""
        if self._current_level != DegradationLevel.NORMAL:
            await self._apply_degradation(
                DegradationLevel.NORMAL, 
                DegradationTrigger.MANUAL, 
                "Manual recovery to normal operation"
            )
    
    def get_current_level(self) -> DegradationLevel:
        """Get current degradation level."""
        return self._current_level
    
    def is_degraded(self) -> bool:
        """Check if system is in degraded state."""
        return self._current_level != DegradationLevel.NORMAL
    
    def should_throttle_connections(self) -> bool:
        """Check if new connections should be throttled."""
        return (self._current_level in [DegradationLevel.MINIMAL, DegradationLevel.MANUAL_REFRESH] or
                self._metrics.get("connection_count", 0) >= self.connection_throttle_threshold)
    
    def should_reduce_update_frequency(self) -> bool:
        """Check if update frequency should be reduced."""
        return self._current_level in [DegradationLevel.REDUCED, DegradationLevel.MINIMAL]
    
    def should_disable_realtime_updates(self) -> bool:
        """Check if real-time updates should be disabled."""
        return self._current_level in [DegradationLevel.MANUAL_REFRESH, DegradationLevel.OFFLINE]
    
    def get_recommended_batch_size(self) -> int:
        """Get recommended batch size based on degradation level."""
        if self._current_level == DegradationLevel.NORMAL:
            return 50
        elif self._current_level == DegradationLevel.REDUCED:
            return 25
        elif self._current_level == DegradationLevel.MINIMAL:
            return 10
        else:
            return 5
    
    def get_recommended_update_interval(self) -> float:
        """Get recommended update interval in seconds."""
        if self._current_level == DegradationLevel.NORMAL:
            return 0.1  # 100ms
        elif self._current_level == DegradationLevel.REDUCED:
            return 0.5  # 500ms
        elif self._current_level == DegradationLevel.MINIMAL:
            return 2.0  # 2s
        else:
            return 10.0  # 10s (manual refresh mode)
    
    def get_degradation_status(self) -> Dict[str, Any]:
        """Get current degradation status."""
        return {
            "current_level": self._current_level.value,
            "is_degraded": self.is_degraded(),
            "should_throttle": self.should_throttle_connections(),
            "should_reduce_frequency": self.should_reduce_update_frequency(),
            "should_disable_realtime": self.should_disable_realtime_updates(),
            "recommended_batch_size": self.get_recommended_batch_size(),
            "recommended_interval": self.get_recommended_update_interval(),
            "metrics": self._metrics.copy(),
            "recent_events": [
                {
                    "timestamp": event.timestamp.isoformat(),
                    "level": event.level.value,
                    "trigger": event.trigger.value,
                    "reason": event.reason
                }
                for event in self._degradation_history[-5:]  # Last 5 events
            ]
        }
    
    async def _monitoring_loop(self) -> None:
        """Main monitoring loop for degradation evaluation."""
        while True:
            try:
                await asyncio.sleep(5.0)  # Check every 5 seconds
                await self._evaluate_degradation_rules()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in degradation monitoring: {e}")
                await asyncio.sleep(10.0)  # Wait longer on error
    
    async def _evaluate_degradation_rules(self) -> None:
        """Evaluate degradation rules against current metrics."""
        current_time = datetime.now()
        
        for rule in self._rules:
            metric_value = self._get_metric_for_trigger(rule.trigger)
            
            if metric_value is not None and metric_value > rule.threshold:
                # Threshold exceeded
                if rule.trigger not in self._trigger_timestamps:
                    self._trigger_timestamps[rule.trigger] = current_time
                else:
                    # Check if duration threshold is met
                    duration = (current_time - self._trigger_timestamps[rule.trigger]).total_seconds()
                    if duration >= rule.duration_seconds:
                        # Trigger degradation if not already at this level or worse
                        if self._should_degrade_to_level(rule.target_level):
                            await self._apply_degradation(
                                rule.target_level,
                                rule.trigger,
                                f"{rule.description} for {duration:.1f}s"
                            )
            else:
                # Threshold not exceeded, clear trigger timestamp
                self._trigger_timestamps.pop(rule.trigger, None)
        
        # Check for recovery conditions
        await self._evaluate_recovery()
    
    def _get_metric_for_trigger(self, trigger: DegradationTrigger) -> Optional[float]:
        """Get metric value for a specific trigger."""
        if trigger == DegradationTrigger.HIGH_LATENCY:
            return self._metrics.get("average_latency_ms", 0.0)
        elif trigger == DegradationTrigger.CONNECTION_FAILURES:
            return self._metrics.get("error_rate", 0.0)
        elif trigger == DegradationTrigger.MEMORY_PRESSURE:
            return self._metrics.get("memory_usage_mb", 0.0)
        return None
    
    def _should_degrade_to_level(self, target_level: DegradationLevel) -> bool:
        """Check if we should degrade to the target level."""
        level_order = [
            DegradationLevel.NORMAL,
            DegradationLevel.REDUCED,
            DegradationLevel.MINIMAL,
            DegradationLevel.MANUAL_REFRESH,
            DegradationLevel.OFFLINE
        ]
        
        current_index = level_order.index(self._current_level)
        target_index = level_order.index(target_level)
        
        return target_index > current_index
    
    async def _evaluate_recovery(self) -> None:
        """Evaluate conditions for recovery to better service level."""
        if self._current_level == DegradationLevel.NORMAL:
            return
        
        # Check if all metrics are below thresholds for recovery
        all_metrics_good = True
        
        for rule in self._rules:
            metric_value = self._get_metric_for_trigger(rule.trigger)
            if metric_value is not None and metric_value > rule.threshold * 0.8:  # 80% of threshold for hysteresis
                all_metrics_good = False
                break
        
        if all_metrics_good:
            # Gradually recover (one level at a time)
            level_order = [
                DegradationLevel.OFFLINE,
                DegradationLevel.MANUAL_REFRESH,
                DegradationLevel.MINIMAL,
                DegradationLevel.REDUCED,
                DegradationLevel.NORMAL
            ]
            
            current_index = level_order.index(self._current_level)
            if current_index > 0:
                better_level = level_order[current_index - 1]
                await self._apply_degradation(
                    better_level,
                    DegradationTrigger.MANUAL,
                    "Automatic recovery - metrics improved"
                )
    
    async def _apply_degradation(self, level: DegradationLevel, 
                               trigger: DegradationTrigger, reason: str) -> None:
        """Apply degradation to specified level."""
        if level == self._current_level:
            return  # No change needed
        
        previous_level = self._current_level
        self._current_level = level
        
        # Create degradation event
        event = DegradationEvent(
            timestamp=datetime.now(),
            level=level,
            trigger=trigger,
            reason=reason,
            metrics=self._metrics.copy()
        )
        
        self._degradation_history.append(event)
        
        # Keep only last 100 events
        if len(self._degradation_history) > 100:
            self._degradation_history = self._degradation_history[-100:]
        
        # Log degradation change
        if level.value in ["manual_refresh", "offline"]:
            log_level = logging.WARNING
        elif level == DegradationLevel.NORMAL:
            log_level = logging.INFO
        else:
            log_level = logging.WARNING
        
        self.logger.log(
            log_level,
            f"Service degradation: {previous_level.value} -> {level.value} "
            f"(trigger: {trigger.value}, reason: {reason})"
        )
        
        # Execute callbacks
        callbacks = self._degradation_callbacks.get(level, [])
        for callback in callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(event)
                else:
                    callback(event)
            except Exception as e:
                self.logger.error(f"Error executing degradation callback: {e}")
    
    def add_degradation_rule(self, rule: DegradationRule) -> None:
        """Add custom degradation rule."""
        self._rules.append(rule)
        self.logger.info(f"Added degradation rule: {rule.description}")
    
    def remove_degradation_rule(self, trigger: DegradationTrigger) -> bool:
        """Remove degradation rule by trigger."""
        initial_count = len(self._rules)
        self._rules = [rule for rule in self._rules if rule.trigger != trigger]
        removed = len(self._rules) < initial_count
        
        if removed:
            self.logger.info(f"Removed degradation rule for trigger: {trigger.value}")
        
        return removed