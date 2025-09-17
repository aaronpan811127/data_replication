"""
Backup module for data replication system.

This module provides backup functionality with deep clone operations
for delta tables and streaming tables.
"""

from .backup_manager import BackupManager
from .backup_provider import BackupProvider

__all__ = [
    "BackupManager",
    "BackupProvider",
]
