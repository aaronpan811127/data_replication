"""
CLI module for data replication system.

This module provides CLI functionality for data replication operations.
"""

from .backup_cli import main as backup_main

__all__ = [
    "backup_main",
]