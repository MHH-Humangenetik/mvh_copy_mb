"""Implementation of optimistic locking system for multi-user synchronization."""

import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging

from .interfaces import LockManager
from .models import RecordLock, LockState


logger = logging.getLogger(__name__)


class OptimisticLockManager(LockManager):
    """Implementation of optimistic locking with version control."""
    
    def __init__(self, default_timeout_seconds: int = 30):
        """Initialize the lock manager.
        
        Args:
            default_timeout_seconds: Default timeout for locks in seconds
        """
        self._locks: Dict[str, RecordLock] = {}
        self._default_timeout = default_timeout_seconds
        self._cleanup_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
        
    async def start(self) -> None:
        """Start the lock manager and cleanup task."""
        if self._cleanup_task is None:
            self._cleanup_task = asyncio.create_task(self._periodic_cleanup())
            logger.info("Lock manager started with periodic cleanup")
    
    async def stop(self) -> None:
        """Stop the lock manager and cleanup task."""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            self._cleanup_task = None
            logger.info("Lock manager stopped")
    
    async def acquire_lock(self, record_id: str, user_id: str, 
                          version: int, timeout_seconds: int = None) -> Optional[RecordLock]:
        """Attempt to acquire a lock on a record.
        
        Args:
            record_id: ID of the record to lock
            user_id: ID of the user requesting the lock
            version: Expected version of the record
            timeout_seconds: Lock timeout in seconds (uses default if None)
            
        Returns:
            RecordLock if successful, None if lock cannot be acquired
        """
        if timeout_seconds is None:
            timeout_seconds = self._default_timeout
            
        async with self._lock:
            # Check if record is already locked
            existing_lock = self._locks.get(record_id)
            
            if existing_lock:
                # Check if lock is expired
                if existing_lock.expires_at <= datetime.now():
                    # Lock expired, remove it
                    await self._release_lock_internal(record_id)
                    logger.info(f"Expired lock removed for record {record_id}")
                else:
                    # Lock is still active
                    if existing_lock.user_id == user_id:
                        # User already holds the lock, extend it
                        existing_lock.expires_at = datetime.now() + timedelta(seconds=timeout_seconds)
                        logger.debug(f"Lock extended for record {record_id} by user {user_id}")
                        return existing_lock
                    else:
                        # Lock held by another user
                        logger.warning(f"Lock acquisition failed for record {record_id}: held by {existing_lock.user_id}")
                        return None
            
            # Create new lock
            now = datetime.now()
            new_lock = RecordLock(
                record_id=record_id,
                user_id=user_id,
                version=version,
                acquired_at=now,
                expires_at=now + timedelta(seconds=timeout_seconds),
                state=LockState.ACQUIRED
            )
            
            self._locks[record_id] = new_lock
            logger.info(f"Lock acquired for record {record_id} by user {user_id} (version {version})")
            return new_lock
    
    async def release_lock(self, record_id: str, user_id: str) -> bool:
        """Release a lock held by a user.
        
        Args:
            record_id: ID of the record to unlock
            user_id: ID of the user releasing the lock
            
        Returns:
            True if lock was released, False if no lock or wrong user
        """
        async with self._lock:
            existing_lock = self._locks.get(record_id)
            
            if not existing_lock:
                logger.warning(f"No lock found for record {record_id}")
                return False
                
            if existing_lock.user_id != user_id:
                logger.warning(f"Lock release failed for record {record_id}: held by {existing_lock.user_id}, not {user_id}")
                return False
            
            await self._release_lock_internal(record_id)
            logger.info(f"Lock released for record {record_id} by user {user_id}")
            return True
    
    async def check_lock(self, record_id: str) -> Optional[RecordLock]:
        """Check if a record is currently locked.
        
        Args:
            record_id: ID of the record to check
            
        Returns:
            RecordLock if locked, None if not locked
        """
        async with self._lock:
            existing_lock = self._locks.get(record_id)
            
            if not existing_lock:
                return None
                
            # Check if lock is expired
            if existing_lock.expires_at <= datetime.now():
                await self._release_lock_internal(record_id)
                logger.debug(f"Expired lock removed during check for record {record_id}")
                return None
                
            return existing_lock
    
    async def cleanup_expired_locks(self) -> List[RecordLock]:
        """Clean up expired locks and return the list of cleaned locks.
        
        Returns:
            List of RecordLock objects that were cleaned up
        """
        expired_locks = []
        now = datetime.now()
        
        async with self._lock:
            expired_record_ids = []
            
            for record_id, lock in self._locks.items():
                if lock.expires_at <= now:
                    expired_record_ids.append(record_id)
                    expired_locks.append(lock)
            
            for record_id in expired_record_ids:
                await self._release_lock_internal(record_id)
        
        if expired_locks:
            logger.info(f"Cleaned up {len(expired_locks)} expired locks")
            
        return expired_locks
    
    async def release_user_locks(self, user_id: str) -> List[RecordLock]:
        """Release all locks held by a specific user.
        
        Args:
            user_id: ID of the user whose locks should be released
            
        Returns:
            List of RecordLock objects that were released
        """
        released_locks = []
        
        async with self._lock:
            user_record_ids = []
            
            for record_id, lock in self._locks.items():
                if lock.user_id == user_id:
                    user_record_ids.append(record_id)
                    released_locks.append(lock)
            
            for record_id in user_record_ids:
                await self._release_lock_internal(record_id)
        
        if released_locks:
            logger.info(f"Released {len(released_locks)} locks for user {user_id}")
            
        return released_locks
    
    async def cleanup_locks_for_disconnected_user(self, user_id: str, 
                                                 disconnection_time: datetime) -> List[RecordLock]:
        """Clean up locks for a user who disconnected, respecting the 30-second timeout.
        
        Args:
            user_id: ID of the disconnected user
            disconnection_time: When the user disconnected
            
        Returns:
            List of RecordLock objects that were released
        """
        now = datetime.now()
        time_since_disconnect = (now - disconnection_time).total_seconds()
        
        # Only clean up if 30 seconds have passed since disconnection
        if time_since_disconnect < 30:
            logger.debug(f"User {user_id} disconnected {time_since_disconnect:.1f}s ago, "
                        f"waiting for 30s timeout")
            return []
        
        # Check if user still has any active locks (they might have expired already)
        async with self._lock:
            user_locks = []
            for record_id, lock in self._locks.items():
                if lock.user_id == user_id:
                    # Check if lock is still valid (not expired)
                    if lock.expires_at > now:
                        user_locks.append((record_id, lock))
        
        # If no active locks, nothing to clean up
        if not user_locks:
            logger.debug(f"No active locks found for user {user_id} (may have already expired)")
            return []
        
        # Clean up the active locks for this user
        released_locks = []
        async with self._lock:
            for record_id, lock in user_locks:
                if record_id in self._locks and self._locks[record_id].user_id == user_id:
                    released_locks.append(lock)
                    await self._release_lock_internal(record_id)
        
        if released_locks:
            logger.info(f"Cleaned up {len(released_locks)} locks for user {user_id} "
                       f"after {time_since_disconnect:.1f}s disconnection timeout")
        
        return released_locks
    
    async def validate_version(self, record_id: str, expected_version: int) -> bool:
        """Validate that a record version matches the expected version.
        
        Args:
            record_id: ID of the record to validate
            expected_version: Expected version number
            
        Returns:
            True if version matches or no lock exists, False if version mismatch
        """
        async with self._lock:
            existing_lock = self._locks.get(record_id)
            
            if not existing_lock:
                # No lock exists, version validation passes
                return True
                
            # Check if lock is expired
            if existing_lock.expires_at <= datetime.now():
                await self._release_lock_internal(record_id)
                return True
                
            # Validate version
            return existing_lock.version == expected_version
    
    async def _release_lock_internal(self, record_id: str) -> None:
        """Internal method to release a lock without additional checks.
        
        Args:
            record_id: ID of the record to unlock
        """
        if record_id in self._locks:
            lock = self._locks[record_id]
            lock.state = LockState.RELEASED
            del self._locks[record_id]
    
    async def _periodic_cleanup(self) -> None:
        """Periodic task to clean up expired locks."""
        while True:
            try:
                await asyncio.sleep(10)  # Run cleanup every 10 seconds
                await self.cleanup_expired_locks()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error during periodic lock cleanup: {e}")
                await asyncio.sleep(30)  # Wait longer on error
    
    def get_lock_count(self) -> int:
        """Get the current number of active locks (for testing/monitoring)."""
        return len(self._locks)
    
    def get_user_lock_count(self, user_id: str) -> int:
        """Get the number of locks held by a specific user (for testing/monitoring)."""
        return sum(1 for lock in self._locks.values() if lock.user_id == user_id)