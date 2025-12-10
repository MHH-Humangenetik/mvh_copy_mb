"""WebSocket reconnection logic with exponential backoff."""

import asyncio
import logging
import random
from datetime import datetime, timedelta
from typing import Optional, Callable, Dict, Any
from enum import Enum

from ..sync.config import SyncConfig
from ..sync.logging_config import get_logger


class ReconnectionState(Enum):
    """States of reconnection process."""
    IDLE = "idle"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    FAILED = "failed"
    DEGRADED = "degraded"


class ReconnectionManager:
    """Manages automatic reconnection with exponential backoff."""
    
    def __init__(self, config: SyncConfig):
        self.config = config
        self.logger = get_logger(__name__)
        
        # Reconnection state
        self._state = ReconnectionState.IDLE
        self._attempt_count = 0
        self._last_attempt_time: Optional[datetime] = None
        self._reconnection_task: Optional[asyncio.Task] = None
        
        # Callbacks
        self._connect_callback: Optional[Callable] = None
        self._degradation_callback: Optional[Callable] = None
        self._success_callback: Optional[Callable] = None
        
        # Metrics
        self._total_attempts = 0
        self._successful_reconnections = 0
        self._failed_reconnections = 0
        
    def set_connect_callback(self, callback: Callable) -> None:
        """Set callback function for connection attempts."""
        self._connect_callback = callback
        
    def set_degradation_callback(self, callback: Callable) -> None:
        """Set callback function for graceful degradation."""
        self._degradation_callback = callback
        
    def set_success_callback(self, callback: Callable) -> None:
        """Set callback function for successful reconnection."""
        self._success_callback = callback
        
    async def start_reconnection(self, reason: str = "Connection lost") -> None:
        """Start the reconnection process."""
        if self._state == ReconnectionState.CONNECTING:
            self.logger.debug("Reconnection already in progress")
            return
            
        self.logger.info(f"Starting reconnection process: {reason}")
        self._state = ReconnectionState.CONNECTING
        self._attempt_count = 0
        
        # Cancel any existing reconnection task
        if self._reconnection_task:
            self._reconnection_task.cancel()
            
        self._reconnection_task = asyncio.create_task(self._reconnection_loop())
        
    async def stop_reconnection(self) -> None:
        """Stop the reconnection process."""
        if self._reconnection_task:
            self._reconnection_task.cancel()
            try:
                await self._reconnection_task
            except asyncio.CancelledError:
                pass
            self._reconnection_task = None
            
        self._state = ReconnectionState.IDLE
        
    async def mark_connected(self) -> None:
        """Mark the connection as successfully established."""
        if self._state == ReconnectionState.CONNECTING:
            self._successful_reconnections += 1
            self.logger.info(
                f"Reconnection successful after {self._attempt_count} attempts"
            )
            
            if self._success_callback:
                try:
                    await self._success_callback()
                except Exception as e:
                    self.logger.error(f"Error in success callback: {e}")
                    
        self._state = ReconnectionState.CONNECTED
        self._attempt_count = 0
        await self.stop_reconnection()
        
    async def mark_failed(self, reason: str = "Connection failed") -> None:
        """Mark the current connection attempt as failed."""
        if self._state != ReconnectionState.CONNECTING:
            return
            
        self._attempt_count += 1
        self._total_attempts += 1
        self._last_attempt_time = datetime.now()
        
        self.logger.warning(
            f"Reconnection attempt {self._attempt_count} failed: {reason}"
        )
        
        # Check if we should give up
        if self._attempt_count >= self.config.max_reconnection_attempts:
            self.logger.error(
                f"Reconnection failed after {self._attempt_count} attempts. "
                "Switching to degraded mode."
            )
            await self._enter_degraded_mode()
            return
            
    def get_state(self) -> ReconnectionState:
        """Get the current reconnection state."""
        return self._state
        
    def get_metrics(self) -> Dict[str, Any]:
        """Get reconnection metrics."""
        return {
            "state": self._state.value,
            "current_attempt": self._attempt_count,
            "total_attempts": self._total_attempts,
            "successful_reconnections": self._successful_reconnections,
            "failed_reconnections": self._failed_reconnections,
            "last_attempt_time": self._last_attempt_time.isoformat() if self._last_attempt_time else None
        }
        
    async def _reconnection_loop(self) -> None:
        """Main reconnection loop with exponential backoff."""
        while self._state == ReconnectionState.CONNECTING:
            try:
                # Calculate delay with exponential backoff
                delay = self._calculate_backoff_delay()
                
                if delay > 0:
                    self.logger.debug(f"Waiting {delay:.1f}s before reconnection attempt")
                    await asyncio.sleep(delay)
                    
                # Check if we should still be reconnecting
                if self._state != ReconnectionState.CONNECTING:
                    break
                    
                # Attempt connection
                self.logger.info(f"Reconnection attempt {self._attempt_count + 1}")
                
                if self._connect_callback:
                    try:
                        success = await self._connect_callback()
                        if success:
                            await self.mark_connected()
                            break
                        else:
                            await self.mark_failed("Connection callback returned False")
                    except Exception as e:
                        await self.mark_failed(f"Connection callback error: {e}")
                else:
                    self.logger.error("No connection callback set")
                    break
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in reconnection loop: {e}")
                await self.mark_failed(f"Reconnection loop error: {e}")
                
    def _calculate_backoff_delay(self) -> float:
        """Calculate the delay for the next reconnection attempt using exponential backoff."""
        if self._attempt_count == 0:
            return 0  # First attempt is immediate
            
        # Exponential backoff with jitter
        base_delay = self.config.initial_reconnection_delay_seconds
        exponential_delay = base_delay * (self.config.reconnection_backoff_multiplier ** (self._attempt_count - 1))
        
        # Cap at maximum delay
        capped_delay = min(exponential_delay, self.config.max_reconnection_delay_seconds)
        
        # Add jitter (±25% of the delay)
        jitter_range = capped_delay * 0.25
        jitter = random.uniform(-jitter_range, jitter_range)
        
        return max(0, capped_delay + jitter)
        
    async def _enter_degraded_mode(self) -> None:
        """Enter graceful degradation mode."""
        self._state = ReconnectionState.DEGRADED
        self._failed_reconnections += 1
        
        self.logger.warning("Entering degraded mode - manual refresh required")
        
        if self._degradation_callback:
            try:
                await self._degradation_callback()
            except Exception as e:
                self.logger.error(f"Error in degradation callback: {e}")
                
        await self.stop_reconnection()


class ConnectionHealthMonitor:
    """Monitors connection health and triggers reconnection when needed."""
    
    def __init__(self, config: SyncConfig, reconnection_manager: ReconnectionManager):
        self.config = config
        self.reconnection_manager = reconnection_manager
        self.logger = get_logger(__name__)
        
        # Health monitoring state
        self._is_monitoring = False
        self._monitor_task: Optional[asyncio.Task] = None
        self._last_successful_ping: Optional[datetime] = None
        self._consecutive_failures = 0
        
        # Health check callbacks
        self._health_check_callback: Optional[Callable] = None
        
    def set_health_check_callback(self, callback: Callable) -> None:
        """Set callback function for health checks."""
        self._health_check_callback = callback
        
    async def start_monitoring(self) -> None:
        """Start health monitoring."""
        if self._is_monitoring:
            return
            
        self.logger.info("Starting connection health monitoring")
        self._is_monitoring = True
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        
    async def stop_monitoring(self) -> None:
        """Stop health monitoring."""
        self._is_monitoring = False
        
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
            self._monitor_task = None
            
    async def mark_healthy(self) -> None:
        """Mark the connection as healthy."""
        self._last_successful_ping = datetime.now()
        self._consecutive_failures = 0
        
    async def mark_unhealthy(self, reason: str = "Health check failed") -> None:
        """Mark the connection as unhealthy."""
        self._consecutive_failures += 1
        self.logger.warning(
            f"Connection health check failed ({self._consecutive_failures} consecutive): {reason}"
        )
        
        # Trigger reconnection after multiple failures
        if self._consecutive_failures >= 3:
            self.logger.error("Multiple health check failures - triggering reconnection")
            await self.reconnection_manager.start_reconnection(
                f"Health check failures: {reason}"
            )
            
    async def _monitor_loop(self) -> None:
        """Main health monitoring loop."""
        while self._is_monitoring:
            try:
                await asyncio.sleep(self.config.heartbeat_interval_seconds)
                
                if not self._is_monitoring:
                    break
                    
                # Perform health check
                if self._health_check_callback:
                    try:
                        is_healthy = await self._health_check_callback()
                        if is_healthy:
                            await self.mark_healthy()
                        else:
                            await self.mark_unhealthy("Health check returned False")
                    except Exception as e:
                        await self.mark_unhealthy(f"Health check error: {e}")
                else:
                    # No health check callback - just check if we've had recent activity
                    if self._last_successful_ping:
                        time_since_ping = datetime.now() - self._last_successful_ping
                        if time_since_ping.total_seconds() > self.config.connection_timeout_seconds:
                            await self.mark_unhealthy("No recent activity")
                            
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in health monitor loop: {e}")