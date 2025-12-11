"""WebSocket connection manager with health monitoring and connection pooling."""

import asyncio
import logging
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Any
from enum import Enum

from websockets.asyncio.server import ServerConnection
from websockets.exceptions import ConnectionClosed, WebSocketException

from ..sync.models import ClientConnection, ConnectionHeartbeat, ErrorMessage
from ..sync.interfaces import ConnectionManager
from ..sync.config import SyncConfig
from ..sync.logging_config import get_logger, log_connection_event
from .reconnection import ReconnectionManager, ConnectionHealthMonitor


class ConnectionState(Enum):
    """States of WebSocket connections."""
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    RECONNECTING = "reconnecting"
    EXPIRED = "expired"


class WebSocketManager(ConnectionManager):
    """Manages WebSocket connections with health monitoring and automatic cleanup."""
    
    def __init__(self, config: SyncConfig):
        self.config = config
        self.logger = get_logger(__name__)
        
        # Connection storage
        self._connections: Dict[str, ClientConnection] = {}
        self._user_connections: Dict[str, Set[str]] = {}  # user_id -> connection_ids
        self._connection_states: Dict[str, ConnectionState] = {}
        
        # Health monitoring
        self._heartbeat_tasks: Dict[str, asyncio.Task] = {}
        self._cleanup_task: Optional[asyncio.Task] = None
        
        # Reconnection management
        self._reconnection_managers: Dict[str, ReconnectionManager] = {}
        self._health_monitors: Dict[str, ConnectionHealthMonitor] = {}
        
        # Metrics
        self._connection_count = 0
        self._total_connections = 0
        self._disconnection_count = 0
        
    async def start(self) -> None:
        """Start the connection manager and background tasks."""
        self.logger.info("Starting WebSocket connection manager")
        
        # Start cleanup task
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        
    async def stop(self) -> None:
        """Stop the connection manager and cleanup resources."""
        self.logger.info("Stopping WebSocket connection manager")
        
        # Cancel cleanup task
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        
        # Cancel all heartbeat tasks
        for task in self._heartbeat_tasks.values():
            task.cancel()
        
        # Wait for heartbeat tasks to complete
        if self._heartbeat_tasks:
            await asyncio.gather(*self._heartbeat_tasks.values(), return_exceptions=True)
        
        # Close all connections
        for connection in list(self._connections.values()):
            await self._close_connection(connection.connection_id, "Server shutdown")
        
    async def add_connection(self, connection: ClientConnection) -> None:
        """Add a new client connection with health monitoring."""
        connection_id = connection.connection_id
        user_id = connection.user_id
        
        # Check connection limits per user
        user_connection_count = len(self._user_connections.get(user_id, set()))
        if user_connection_count >= self.config.max_connections_per_user:
            self.logger.warning(
                f"User {user_id} exceeded connection limit ({self.config.max_connections_per_user})"
            )
            await self._send_error(
                connection.websocket,
                "connection_limit_exceeded",
                f"Maximum {self.config.max_connections_per_user} connections per user"
            )
            await connection.websocket.close()
            return
        
        # Store connection with timing information
        connection._connection_start_time = time.time()
        self._connections[connection_id] = connection
        self._connection_states[connection_id] = ConnectionState.CONNECTED
        
        # Track user connections
        if user_id not in self._user_connections:
            self._user_connections[user_id] = set()
        self._user_connections[user_id].add(connection_id)
        
        # Start heartbeat monitoring
        heartbeat_task = asyncio.create_task(
            self._heartbeat_monitor(connection_id)
        )
        self._heartbeat_tasks[connection_id] = heartbeat_task
        
        # Set up reconnection management
        reconnection_manager = ReconnectionManager(self.config)
        self._reconnection_managers[connection_id] = reconnection_manager
        
        # Set up health monitoring
        health_monitor = ConnectionHealthMonitor(self.config, reconnection_manager)
        self._health_monitors[connection_id] = health_monitor
        
        # Configure callbacks
        reconnection_manager.set_connect_callback(
            lambda: self._attempt_reconnection(connection_id)
        )
        reconnection_manager.set_degradation_callback(
            lambda: self._handle_degradation(connection_id)
        )
        health_monitor.set_health_check_callback(
            lambda: self._check_connection_health(connection_id)
        )
        
        # Start health monitoring
        await health_monitor.start_monitoring()
        
        # Update metrics
        self._connection_count += 1
        self._total_connections += 1
        
        # Log connection event with diagnostic information
        log_connection_event(
            self.logger, connection_id, user_id, "connected",
            f"WebSocket connection established",
            total_connections=self._connection_count,
            user_connection_count=len(self._user_connections.get(user_id, set())),
            connection_start=time.time(),
            client_ip=getattr(connection.websocket, 'remote_address', ['unknown'])[0] if hasattr(connection.websocket, 'remote_address') else 'unknown',
            user_agent=getattr(connection.websocket, 'request_headers', {}).get('User-Agent', 'unknown') if hasattr(connection.websocket, 'request_headers') else 'unknown'
        )
        
        self.logger.info(
            f"Added connection {connection_id} for user {user_id}. "
            f"Total connections: {self._connection_count}"
        )
        
    async def remove_connection(self, connection_id: str) -> Optional[ClientConnection]:
        """Remove a client connection and cleanup resources."""
        connection = self._connections.get(connection_id)
        if not connection:
            return None
        
        await self._cleanup_connection(connection_id)
        return connection
        
    async def get_connection(self, connection_id: str) -> Optional[ClientConnection]:
        """Get a specific connection by ID."""
        return self._connections.get(connection_id)
        
    async def get_user_connections(self, user_id: str) -> List[ClientConnection]:
        """Get all active connections for a specific user."""
        connection_ids = self._user_connections.get(user_id, set())
        connections = []
        
        for connection_id in connection_ids:
            connection = self._connections.get(connection_id)
            if connection and self._connection_states.get(connection_id) == ConnectionState.CONNECTED:
                connections.append(connection)
                
        return connections
        
    async def get_all_connections(self) -> List[ClientConnection]:
        """Get all active connections."""
        active_connections = []
        
        for connection_id, connection in self._connections.items():
            if self._connection_states.get(connection_id) == ConnectionState.CONNECTED:
                active_connections.append(connection)
                
        return active_connections
        
    async def update_last_seen(self, connection_id: str) -> None:
        """Update the last seen timestamp for a connection."""
        connection = self._connections.get(connection_id)
        if connection:
            connection.last_seen = datetime.now()
            
    async def send_message(self, connection_id: str, message: dict) -> bool:
        """Send a message to a specific connection."""
        connection = self._connections.get(connection_id)
        if not connection or self._connection_states.get(connection_id) != ConnectionState.CONNECTED:
            return False
            
        try:
            await connection.websocket.send(str(message))
            await self.update_last_seen(connection_id)
            return True
        except (ConnectionClosed, WebSocketException) as e:
            self.logger.warning(f"Failed to send message to {connection_id}: {e}")
            await self._handle_connection_error(connection_id, str(e))
            return False
            
    async def broadcast_message(self, message: dict, exclude_connection: Optional[str] = None) -> int:
        """Broadcast a message to all active connections."""
        sent_count = 0
        
        for connection_id in list(self._connections.keys()):
            if connection_id != exclude_connection:
                if await self.send_message(connection_id, message):
                    sent_count += 1
                    
        return sent_count
        
    async def get_connection_metrics(self) -> Dict[str, int]:
        """Get connection metrics."""
        return {
            "active_connections": self._connection_count,
            "total_connections": self._total_connections,
            "disconnections": self._disconnection_count,
            "users_connected": len([user for user, conns in self._user_connections.items() if conns])
        }
        
    async def _heartbeat_monitor(self, connection_id: str) -> None:
        """Monitor connection health with heartbeat messages."""
        while connection_id in self._connections:
            try:
                await asyncio.sleep(self.config.heartbeat_interval_seconds)
                
                connection = self._connections.get(connection_id)
                if not connection:
                    break
                    
                # Check if connection is still alive
                current_time = datetime.now()
                time_since_last_seen = current_time - connection.last_seen
                
                if time_since_last_seen.total_seconds() > self.config.connection_timeout_seconds:
                    self.logger.warning(
                        f"Connection {connection_id} timed out "
                        f"(last seen {time_since_last_seen.total_seconds():.1f}s ago)"
                    )
                    await self._handle_connection_timeout(connection_id)
                    break
                    
                # Send heartbeat
                heartbeat = ConnectionHeartbeat(connection_id=connection_id)
                success = await self.send_message(connection_id, heartbeat.model_dump())
                
                if not success:
                    self.logger.warning(f"Heartbeat failed for connection {connection_id}")
                    break
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in heartbeat monitor for {connection_id}: {e}")
                await self._handle_connection_error(connection_id, str(e))
                break
                
    async def _cleanup_loop(self) -> None:
        """Background task for cleaning up expired connections."""
        while True:
            try:
                await asyncio.sleep(self.config.lock_cleanup_interval_seconds)
                await self._cleanup_expired_connections()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in cleanup loop: {e}")
                
    async def _cleanup_expired_connections(self) -> None:
        """Clean up connections that have expired."""
        current_time = datetime.now()
        expired_connections = []
        
        for connection_id, connection in self._connections.items():
            time_since_last_seen = current_time - connection.last_seen
            
            if (time_since_last_seen.total_seconds() > self.config.connection_timeout_seconds and
                self._connection_states.get(connection_id) != ConnectionState.DISCONNECTED):
                expired_connections.append(connection_id)
                
        for connection_id in expired_connections:
            self.logger.info(f"Cleaning up expired connection {connection_id}")
            await self._cleanup_connection(connection_id)
            
    async def _cleanup_connection(self, connection_id: str) -> None:
        """Clean up a specific connection and its resources."""
        connection = self._connections.get(connection_id)
        if not connection:
            return
            
        # Cancel heartbeat task
        heartbeat_task = self._heartbeat_tasks.get(connection_id)
        if heartbeat_task:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass
            del self._heartbeat_tasks[connection_id]
            
        # Stop reconnection manager
        reconnection_manager = self._reconnection_managers.get(connection_id)
        if reconnection_manager:
            await reconnection_manager.stop_reconnection()
            del self._reconnection_managers[connection_id]
            
        # Stop health monitor
        health_monitor = self._health_monitors.get(connection_id)
        if health_monitor:
            await health_monitor.stop_monitoring()
            del self._health_monitors[connection_id]
            
        # Remove from user connections
        user_id = connection.user_id
        if user_id in self._user_connections:
            self._user_connections[user_id].discard(connection_id)
            if not self._user_connections[user_id]:
                del self._user_connections[user_id]
                
        # Remove connection
        del self._connections[connection_id]
        self._connection_states.pop(connection_id, None)
        
        # Update metrics
        self._connection_count -= 1
        self._disconnection_count += 1
        
        # Calculate connection duration
        connection_start = getattr(connection, '_connection_start_time', None)
        connection_duration_ms = None
        if connection_start:
            connection_duration_ms = (time.time() - connection_start) * 1000
        
        # Log disconnection event with diagnostic information
        log_connection_event(
            self.logger, connection_id, user_id, "disconnected",
            f"WebSocket connection cleaned up",
            remaining_connections=self._connection_count,
            total_disconnections=self._disconnection_count,
            connection_duration_ms=connection_duration_ms,
            cleanup_reason=getattr(connection, '_cleanup_reason', 'normal')
        )
        
        self.logger.info(
            f"Cleaned up connection {connection_id} for user {user_id}. "
            f"Remaining connections: {self._connection_count}"
        )
        
    async def _handle_connection_timeout(self, connection_id: str) -> None:
        """Handle connection timeout."""
        self._connection_states[connection_id] = ConnectionState.EXPIRED
        await self._close_connection(connection_id, "Connection timeout")
        
    async def _handle_connection_error(self, connection_id: str, error: str) -> None:
        """Handle connection error."""
        self.logger.warning(f"Connection error for {connection_id}: {error}")
        self._connection_states[connection_id] = ConnectionState.DISCONNECTED
        await self._close_connection(connection_id, f"Connection error: {error}")
        
    async def _close_connection(self, connection_id: str, reason: str) -> None:
        """Close a connection with a specific reason."""
        connection = self._connections.get(connection_id)
        if connection:
            try:
                await self._send_error(
                    connection.websocket,
                    "connection_closed",
                    reason
                )
                await connection.websocket.close()
            except Exception as e:
                self.logger.debug(f"Error closing connection {connection_id}: {e}")
                
        await self._cleanup_connection(connection_id)
        
    async def _send_error(self, websocket: ServerConnection, error_code: str, message: str) -> None:
        """Send an error message to a WebSocket connection."""
        try:
            error_msg = ErrorMessage(
                error_code=error_code,
                message=message
            )
            await websocket.send(error_msg.model_dump_json())
        except Exception as e:
            self.logger.debug(f"Failed to send error message: {e}")
            
    async def _attempt_reconnection(self, connection_id: str) -> bool:
        """Attempt to reconnect a specific connection."""
        connection = self._connections.get(connection_id)
        if not connection:
            return False
            
        try:
            # In a real implementation, this would attempt to re-establish the WebSocket
            # For now, we'll simulate a reconnection attempt
            self.logger.info(f"Attempting reconnection for {connection_id}")
            
            # Check if the connection is still valid
            if connection.websocket.closed:
                self.logger.warning(f"WebSocket for {connection_id} is closed")
                return False
                
            # Update connection state
            self._connection_states[connection_id] = ConnectionState.CONNECTED
            connection.last_seen = datetime.now()
            
            # Mark health monitor as healthy
            health_monitor = self._health_monitors.get(connection_id)
            if health_monitor:
                await health_monitor.mark_healthy()
                
            return True
            
        except Exception as e:
            self.logger.error(f"Reconnection attempt failed for {connection_id}: {e}")
            return False
            
    async def _handle_degradation(self, connection_id: str) -> None:
        """Handle graceful degradation for a connection."""
        connection = self._connections.get(connection_id)
        if not connection:
            return
            
        self.logger.warning(
            f"Connection {connection_id} entering degraded mode - "
            "manual refresh required"
        )
        
        # Send degradation notice to client
        try:
            degradation_msg = ErrorMessage(
                error_code="connection_degraded",
                message="Connection issues detected. Please refresh the page to restore real-time updates.",
                details={"manual_refresh_required": True}
            )
            await connection.websocket.send(degradation_msg.model_dump_json())
        except Exception as e:
            self.logger.debug(f"Failed to send degradation message: {e}")
            
        # Update connection state
        self._connection_states[connection_id] = ConnectionState.DISCONNECTED
        
    async def _check_connection_health(self, connection_id: str) -> bool:
        """Check the health of a specific connection."""
        connection = self._connections.get(connection_id)
        if not connection:
            return False
            
        try:
            # Check if WebSocket is still open
            if connection.websocket.closed:
                return False
                
            # Check last seen time
            current_time = datetime.now()
            time_since_last_seen = current_time - connection.last_seen
            
            if time_since_last_seen.total_seconds() > self.config.connection_timeout_seconds:
                return False
                
            return True
            
        except Exception as e:
            self.logger.debug(f"Health check error for {connection_id}: {e}")
            return False
            
    async def trigger_reconnection(self, connection_id: str, reason: str = "Manual trigger") -> None:
        """Manually trigger reconnection for a specific connection."""
        reconnection_manager = self._reconnection_managers.get(connection_id)
        if reconnection_manager:
            await reconnection_manager.start_reconnection(reason)
        else:
            self.logger.warning(f"No reconnection manager found for {connection_id}")
            
    async def get_reconnection_status(self, connection_id: str) -> Optional[Dict[str, Any]]:
        """Get reconnection status for a specific connection."""
        reconnection_manager = self._reconnection_managers.get(connection_id)
        if reconnection_manager:
            return reconnection_manager.get_metrics()
        return None