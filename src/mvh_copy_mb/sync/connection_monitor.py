"""Connection monitoring and automatic lock cleanup for disconnected users."""

import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Set, Optional
import logging

from .interfaces import LockManager, ConnectionManager
from .models import ClientConnection, RecordLock


logger = logging.getLogger(__name__)


class ConnectionMonitor:
    """Monitors client connections and handles automatic lock cleanup on disconnection."""
    
    def __init__(self, lock_manager: LockManager, connection_manager: ConnectionManager,
                 cleanup_interval_seconds: int = 10, connection_timeout_seconds: int = 30):
        """Initialize the connection monitor.
        
        Args:
            lock_manager: Lock manager for releasing locks
            connection_manager: Connection manager for tracking connections
            cleanup_interval_seconds: How often to run cleanup (default: 10 seconds)
            connection_timeout_seconds: How long to wait before considering connection lost (default: 30 seconds)
        """
        self._lock_manager = lock_manager
        self._connection_manager = connection_manager
        self._cleanup_interval = cleanup_interval_seconds
        self._connection_timeout = connection_timeout_seconds
        self._monitor_task: Optional[asyncio.Task] = None
        self._user_last_seen: Dict[str, datetime] = {}
        self._disconnected_users: Set[str] = set()
        self._cleanup_stats = {
            "total_cleanups": 0,
            "locks_released": 0,
            "users_cleaned": 0
        }
    
    async def start(self) -> None:
        """Start the connection monitoring and cleanup task."""
        if self._monitor_task is None:
            self._monitor_task = asyncio.create_task(self._monitor_connections())
            logger.info(f"Connection monitor started (cleanup interval: {self._cleanup_interval}s, "
                       f"timeout: {self._connection_timeout}s)")
    
    async def stop(self) -> None:
        """Stop the connection monitoring task."""
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
            self._monitor_task = None
            logger.info("Connection monitor stopped")
    
    async def handle_user_disconnection(self, user_id: str) -> List[RecordLock]:
        """Handle immediate cleanup when a user disconnects.
        
        Args:
            user_id: ID of the user who disconnected
            
        Returns:
            List of locks that were released
        """
        logger.info(f"Handling disconnection for user {user_id}")
        
        # Mark user as disconnected
        self._disconnected_users.add(user_id)
        self._user_last_seen[user_id] = datetime.now()
        
        # Release all locks held by this user
        released_locks = await self._lock_manager.release_user_locks(user_id)
        
        if released_locks:
            logger.info(f"Released {len(released_locks)} locks for disconnected user {user_id}")
            self._cleanup_stats["locks_released"] += len(released_locks)
            self._cleanup_stats["users_cleaned"] += 1
        
        return released_locks
    
    async def handle_user_reconnection(self, user_id: str) -> None:
        """Handle user reconnection by removing them from disconnected set.
        
        Args:
            user_id: ID of the user who reconnected
        """
        if user_id in self._disconnected_users:
            self._disconnected_users.remove(user_id)
            logger.info(f"User {user_id} reconnected, removed from disconnected set")
        
        # Update last seen time
        self._user_last_seen[user_id] = datetime.now()
    
    async def update_user_activity(self, user_id: str) -> None:
        """Update the last seen time for a user.
        
        Args:
            user_id: ID of the user with activity
        """
        self._user_last_seen[user_id] = datetime.now()
        
        # If user was marked as disconnected but is now active, reconnect them
        if user_id in self._disconnected_users:
            await self.handle_user_reconnection(user_id)
    
    async def check_connection_health(self, user_id: str) -> bool:
        """Check if a user's connection is healthy.
        
        Args:
            user_id: ID of the user to check
            
        Returns:
            True if connection is healthy, False if timed out
        """
        # Check if user has active connections
        user_connections = await self._connection_manager.get_user_connections(user_id)
        
        if not user_connections:
            # No active connections
            return False
        
        # Check if any connection is recent
        now = datetime.now()
        for connection in user_connections:
            time_since_last_seen = now - connection.last_seen
            if time_since_last_seen.total_seconds() < self._connection_timeout:
                return True
        
        # All connections are stale
        return False
    
    async def get_stale_users(self) -> List[str]:
        """Get list of users with stale connections that should be cleaned up.
        
        Returns:
            List of user IDs with stale connections
        """
        stale_users = []
        all_connections = await self._connection_manager.get_all_connections()
        
        # Group connections by user
        user_connections = {}
        for connection in all_connections:
            if connection.user_id not in user_connections:
                user_connections[connection.user_id] = []
            user_connections[connection.user_id].append(connection)
        
        # Check each user's connection health
        now = datetime.now()
        for user_id, connections in user_connections.items():
            # Check if all connections for this user are stale
            all_stale = True
            for connection in connections:
                time_since_last_seen = now - connection.last_seen
                if time_since_last_seen.total_seconds() < self._connection_timeout:
                    all_stale = False
                    break
            
            if all_stale:
                stale_users.append(user_id)
        
        return stale_users
    
    async def cleanup_stale_connections(self) -> Dict[str, int]:
        """Clean up stale connections and release associated locks.
        
        Returns:
            Dictionary with cleanup statistics
        """
        cleanup_results = {
            "stale_users_found": 0,
            "locks_released": 0,
            "connections_removed": 0
        }
        
        # Find users with stale connections
        stale_users = await self.get_stale_users()
        cleanup_results["stale_users_found"] = len(stale_users)
        
        if not stale_users:
            return cleanup_results
        
        logger.info(f"Found {len(stale_users)} users with stale connections")
        
        # Clean up each stale user
        for user_id in stale_users:
            # Release locks
            released_locks = await self._lock_manager.release_user_locks(user_id)
            cleanup_results["locks_released"] += len(released_locks)
            
            if released_locks:
                logger.info(f"Released {len(released_locks)} locks for stale user {user_id}")
            
            # Remove stale connections
            user_connections = await self._connection_manager.get_user_connections(user_id)
            for connection in user_connections:
                time_since_last_seen = (datetime.now() - connection.last_seen).total_seconds()
                if time_since_last_seen >= self._connection_timeout:
                    await self._connection_manager.remove_connection(connection.connection_id)
                    cleanup_results["connections_removed"] += 1
            
            # Mark user as disconnected
            self._disconnected_users.add(user_id)
            self._user_last_seen[user_id] = datetime.now()
        
        # Update overall stats
        self._cleanup_stats["total_cleanups"] += 1
        self._cleanup_stats["locks_released"] += cleanup_results["locks_released"]
        self._cleanup_stats["users_cleaned"] += len(stale_users)
        
        return cleanup_results
    
    async def force_cleanup_user(self, user_id: str) -> Dict[str, int]:
        """Force cleanup of a specific user's locks and connections.
        
        Args:
            user_id: ID of the user to clean up
            
        Returns:
            Dictionary with cleanup results
        """
        logger.info(f"Force cleanup requested for user {user_id}")
        
        # Release all locks
        released_locks = await self._lock_manager.release_user_locks(user_id)
        
        # Remove all connections
        user_connections = await self._connection_manager.get_user_connections(user_id)
        for connection in user_connections:
            await self._connection_manager.remove_connection(connection.connection_id)
        
        # Mark as disconnected
        self._disconnected_users.add(user_id)
        self._user_last_seen[user_id] = datetime.now()
        
        results = {
            "locks_released": len(released_locks),
            "connections_removed": len(user_connections)
        }
        
        logger.info(f"Force cleanup completed for user {user_id}: "
                   f"{results['locks_released']} locks, {results['connections_removed']} connections")
        
        return results
    
    async def get_connection_status(self, user_id: str) -> Dict[str, any]:
        """Get detailed connection status for a user.
        
        Args:
            user_id: ID of the user to check
            
        Returns:
            Dictionary with connection status information
        """
        user_connections = await self._connection_manager.get_user_connections(user_id)
        now = datetime.now()
        
        connection_info = []
        for connection in user_connections:
            time_since_last_seen = now - connection.last_seen
            connection_info.append({
                "connection_id": connection.connection_id,
                "last_seen": connection.last_seen,
                "seconds_since_last_seen": time_since_last_seen.total_seconds(),
                "is_stale": time_since_last_seen.total_seconds() >= self._connection_timeout,
                "subscriptions": list(connection.subscriptions)
            })
        
        # Check for locks held by this user
        # Note: This would require extending the LockManager interface to get user locks
        # For now, we'll use a placeholder
        user_locks_count = 0  # await self._lock_manager.get_user_lock_count(user_id)
        
        return {
            "user_id": user_id,
            "is_disconnected": user_id in self._disconnected_users,
            "last_seen": self._user_last_seen.get(user_id),
            "connection_count": len(user_connections),
            "connections": connection_info,
            "locks_held": user_locks_count,
            "is_healthy": await self.check_connection_health(user_id)
        }
    
    async def get_monitor_statistics(self) -> Dict[str, any]:
        """Get monitoring and cleanup statistics.
        
        Returns:
            Dictionary with monitoring statistics
        """
        all_connections = await self._connection_manager.get_all_connections()
        stale_users = await self.get_stale_users()
        
        return {
            "total_connections": len(all_connections),
            "disconnected_users": len(self._disconnected_users),
            "stale_users": len(stale_users),
            "cleanup_interval_seconds": self._cleanup_interval,
            "connection_timeout_seconds": self._connection_timeout,
            "cleanup_stats": self._cleanup_stats.copy(),
            "monitor_running": self._monitor_task is not None and not self._monitor_task.done()
        }
    
    async def _monitor_connections(self) -> None:
        """Main monitoring loop that runs periodic cleanup."""
        logger.info("Connection monitoring loop started")
        
        while True:
            try:
                await asyncio.sleep(self._cleanup_interval)
                
                # Perform periodic cleanup
                cleanup_results = await self.cleanup_stale_connections()
                
                if cleanup_results["stale_users_found"] > 0:
                    logger.info(f"Periodic cleanup completed: "
                               f"{cleanup_results['stale_users_found']} stale users, "
                               f"{cleanup_results['locks_released']} locks released, "
                               f"{cleanup_results['connections_removed']} connections removed")
                
                # Clean up expired locks (additional safety measure)
                expired_locks = await self._lock_manager.cleanup_expired_locks()
                if expired_locks:
                    logger.debug(f"Cleaned up {len(expired_locks)} expired locks")
                
            except asyncio.CancelledError:
                logger.info("Connection monitoring loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in connection monitoring loop: {e}")
                # Continue monitoring despite errors
                await asyncio.sleep(self._cleanup_interval)


class DisconnectionHandler:
    """Handles specific disconnection scenarios and cleanup strategies."""
    
    def __init__(self, connection_monitor: ConnectionMonitor):
        """Initialize the disconnection handler.
        
        Args:
            connection_monitor: Connection monitor for performing cleanup
        """
        self._monitor = connection_monitor
        self._disconnection_callbacks: List[callable] = []
    
    def add_disconnection_callback(self, callback: callable) -> None:
        """Add a callback to be called when a user disconnects.
        
        Args:
            callback: Async function to call on disconnection (user_id, released_locks)
        """
        self._disconnection_callbacks.append(callback)
    
    async def handle_websocket_close(self, connection: ClientConnection) -> None:
        """Handle WebSocket connection close event.
        
        Args:
            connection: The connection that was closed
        """
        user_id = connection.user_id
        logger.info(f"WebSocket closed for user {user_id}, connection {connection.connection_id}")
        
        # Check if user has other active connections
        user_connections = await self._monitor._connection_manager.get_user_connections(user_id)
        active_connections = [conn for conn in user_connections 
                            if conn.connection_id != connection.connection_id]
        
        if not active_connections:
            # User has no other connections, trigger cleanup
            released_locks = await self._monitor.handle_user_disconnection(user_id)
            
            # Call disconnection callbacks
            for callback in self._disconnection_callbacks:
                try:
                    await callback(user_id, released_locks)
                except Exception as e:
                    logger.error(f"Error in disconnection callback: {e}")
        else:
            logger.debug(f"User {user_id} still has {len(active_connections)} active connections")
    
    async def handle_connection_timeout(self, user_id: str) -> None:
        """Handle connection timeout for a user.
        
        Args:
            user_id: ID of the user whose connection timed out
        """
        logger.info(f"Connection timeout detected for user {user_id}")
        
        # Verify the connection is actually stale
        is_healthy = await self._monitor.check_connection_health(user_id)
        
        if not is_healthy:
            # Connection is indeed stale, perform cleanup
            released_locks = await self._monitor.handle_user_disconnection(user_id)
            
            # Call disconnection callbacks
            for callback in self._disconnection_callbacks:
                try:
                    await callback(user_id, released_locks)
                except Exception as e:
                    logger.error(f"Error in timeout callback: {e}")
        else:
            logger.debug(f"User {user_id} connection is actually healthy, no cleanup needed")
    
    async def handle_graceful_disconnect(self, user_id: str) -> None:
        """Handle graceful user disconnect (e.g., user explicitly logs out).
        
        Args:
            user_id: ID of the user who is disconnecting gracefully
        """
        logger.info(f"Graceful disconnect for user {user_id}")
        
        # Immediately clean up all resources for this user
        cleanup_results = await self._monitor.force_cleanup_user(user_id)
        
        logger.info(f"Graceful disconnect cleanup completed for user {user_id}: "
                   f"{cleanup_results['locks_released']} locks, "
                   f"{cleanup_results['connections_removed']} connections")
        
        # Call disconnection callbacks
        for callback in self._disconnection_callbacks:
            try:
                await callback(user_id, [])  # Empty locks list since already cleaned
            except Exception as e:
                logger.error(f"Error in graceful disconnect callback: {e}")