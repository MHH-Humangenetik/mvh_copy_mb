"""Circuit breaker pattern implementation for service reliability."""

import asyncio
import time
from datetime import datetime, timedelta
from enum import Enum
from typing import Callable, Any, Optional, Dict
import logging

from .exceptions import CircuitBreakerError


class CircuitState(Enum):
    """States of the circuit breaker."""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, blocking requests
    HALF_OPEN = "half_open"  # Testing if service recovered


class CircuitBreaker:
    """Circuit breaker for protecting against cascading failures."""
    
    def __init__(self, 
                 failure_threshold: int = 5,
                 recovery_timeout: float = 60.0,
                 expected_exception: type = Exception,
                 name: str = "circuit_breaker"):
        """Initialize circuit breaker.
        
        Args:
            failure_threshold: Number of failures before opening circuit
            recovery_timeout: Seconds to wait before trying half-open
            expected_exception: Exception type that counts as failure
            name: Name for logging and identification
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self.name = name
        
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time: Optional[float] = None
        self._success_count = 0
        self._total_requests = 0
        
        self.logger = logging.getLogger(f"{__name__}.{name}")
        
    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker protection.
        
        Args:
            func: Function to execute
            *args: Function arguments
            **kwargs: Function keyword arguments
            
        Returns:
            Function result
            
        Raises:
            CircuitBreakerError: When circuit is open
            Exception: Original exception from function
        """
        self._total_requests += 1
        
        # Check if circuit should transition from open to half-open
        if self._state == CircuitState.OPEN:
            if self._should_attempt_reset():
                self._state = CircuitState.HALF_OPEN
                self.logger.info(f"Circuit breaker {self.name} transitioning to half-open")
            else:
                raise CircuitBreakerError(
                    self.name, self._failure_count, self.failure_threshold
                )
        
        try:
            # Execute the function
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)
            
            # Success - reset failure count and close circuit if half-open
            self._on_success()
            return result
            
        except self.expected_exception as e:
            # Expected failure - increment failure count
            self._on_failure()
            raise e
        except Exception as e:
            # Unexpected exception - don't count as failure for circuit breaker
            self.logger.warning(f"Unexpected exception in {self.name}: {e}")
            raise e
    
    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt reset."""
        if self._last_failure_time is None:
            return True
        return time.time() - self._last_failure_time >= self.recovery_timeout
    
    def _on_success(self) -> None:
        """Handle successful execution."""
        self._success_count += 1
        
        if self._state == CircuitState.HALF_OPEN:
            # Success in half-open state - close the circuit
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self.logger.info(f"Circuit breaker {self.name} closed after successful recovery")
        elif self._state == CircuitState.CLOSED:
            # Reset failure count on success in closed state
            self._failure_count = 0
    
    def _on_failure(self) -> None:
        """Handle failed execution."""
        self._failure_count += 1
        self._last_failure_time = time.time()
        
        if self._failure_count >= self.failure_threshold:
            if self._state == CircuitState.CLOSED:
                self._state = CircuitState.OPEN
                self.logger.warning(
                    f"Circuit breaker {self.name} opened after {self._failure_count} failures"
                )
            elif self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.OPEN
                self.logger.warning(
                    f"Circuit breaker {self.name} reopened - recovery attempt failed"
                )
    
    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        return self._state
    
    @property
    def failure_count(self) -> int:
        """Get current failure count."""
        return self._failure_count
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get circuit breaker metrics."""
        return {
            "name": self.name,
            "state": self._state.value,
            "failure_count": self._failure_count,
            "success_count": self._success_count,
            "total_requests": self._total_requests,
            "failure_threshold": self.failure_threshold,
            "recovery_timeout": self.recovery_timeout,
            "last_failure_time": self._last_failure_time,
            "time_since_last_failure": (
                time.time() - self._last_failure_time 
                if self._last_failure_time else None
            )
        }
    
    def reset(self) -> None:
        """Manually reset the circuit breaker."""
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time = None
        self.logger.info(f"Circuit breaker {self.name} manually reset")


class CircuitBreakerManager:
    """Manages multiple circuit breakers for different services."""
    
    def __init__(self):
        self._breakers: Dict[str, CircuitBreaker] = {}
        self.logger = logging.getLogger(__name__)
    
    def get_breaker(self, name: str, **kwargs) -> CircuitBreaker:
        """Get or create a circuit breaker by name.
        
        Args:
            name: Circuit breaker name
            **kwargs: Circuit breaker configuration
            
        Returns:
            CircuitBreaker instance
        """
        if name not in self._breakers:
            self._breakers[name] = CircuitBreaker(name=name, **kwargs)
            self.logger.info(f"Created circuit breaker: {name}")
        
        return self._breakers[name]
    
    def get_all_metrics(self) -> Dict[str, Dict[str, Any]]:
        """Get metrics for all circuit breakers."""
        return {
            name: breaker.get_metrics() 
            for name, breaker in self._breakers.items()
        }
    
    def reset_all(self) -> None:
        """Reset all circuit breakers."""
        for breaker in self._breakers.values():
            breaker.reset()
        self.logger.info("All circuit breakers reset")
    
    def get_unhealthy_services(self) -> list:
        """Get list of services with open circuit breakers."""
        return [
            name for name, breaker in self._breakers.items()
            if breaker.state == CircuitState.OPEN
        ]