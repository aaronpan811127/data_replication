"""
CLI module entry point.

This allows the CLI to be run as:
python -m data_replication.cli.backup_cli
"""

from data_replication.cli.backup_cli import main

if __name__ == "__main__":
    exit(main())
