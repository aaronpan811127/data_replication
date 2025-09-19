#!/usr/bin/env python3
"""
Main entry point for the data replication system.

This module provides the primary CLI interface for all replication operations
including backup, delta share, replication, and reconciliation.
"""

import argparse
import sys
from pathlib import Path
import uuid
from databricks.sdk import WorkspaceClient
from .utils import create_spark_session
from .audit.logger import DataReplicationLogger
from .backup.backup_manager import BackupManager
from .config.loader import ConfigLoader
from .core.exceptions import ConfigurationError
from .replication.replication_manager import ReplicationManager
from .reconciliation.reconciliation_manager import ReconciliationManager


def create_logger(config) -> DataReplicationLogger:
    """Create logger instance from configuration."""
    logger = DataReplicationLogger("data_replication")
    if hasattr(config, "logging") and config.logging:
        logger.setup_logging(config.logging)
    return logger


def run_backup_only(
    config,
    logger,
    run_id: str,
    source_host: str,
    source_token: str,
    logging_host: str,
    logging_token: str,
) -> int:
    """Run only backup operations."""
    spark = create_spark_session(source_host, source_token)
    logging_spark = create_spark_session(logging_host, logging_token)
    backup_manager = BackupManager(config, spark, logging_spark, logger, run_id)
    summary = backup_manager.run_backup_operations()

    if summary.failed_operations > 0:
        logger.error(f"Backup completed with {summary.failed_operations} failures")
        return 1

    logger.info("All backup operations completed successfully")
    return 0


def run_replication_only(
    config, logger, run_id: str, target_host: str, target_token: str
) -> int:
    """Run only replication operations."""
    spark = create_spark_session(target_host, target_token)

    replication_manager = ReplicationManager(config, spark, spark, logger, run_id)
    summary = replication_manager.run_replication_operations()

    if summary.failed_operations > 0:
        logger.error(f"Replication completed with {summary.failed_operations} failures")
        return 1

    logger.info("All replication operations completed successfully")
    return 0


def run_reconciliation_only(
    config, logger, run_id: str, target_host: str, target_token: str
) -> int:
    """Run only reconciliation operations."""
    spark = create_spark_session(target_host, target_token)

    reconciliation_manager = ReconciliationManager(config, spark, spark, logger, run_id)
    summary = reconciliation_manager.run_reconciliation_operations()

    if summary.failed_operations > 0:
        logger.error(f"Reconciliation completed with {summary.failed_operations} failures")
        return 1

    logger.info("All reconciliation operations completed successfully")
    return 0


def main():
    """Main entry point for data replication system."""
    parser = argparse.ArgumentParser(
        description="Data Replication Tool for Databricks",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("config_file", help="Path to the YAML configuration file")

    parser.add_argument(
        "--operation",
        "-o",
        choices=["all", "backup", "delta_share", "replication", "reconciliation"],
        default="all",
        help="Specific operation to run (default: all enabled operations)",
    )

    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Only validate configuration without running operations",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be processed without executing operations",
    )

    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    args = parser.parse_args()

    # Validate config file exists
    config_path = Path(args.config_file)
    if not config_path.exists():
        print(f"Error: Configuration file not found: {config_path}", file=sys.stderr)
        return 1

    try:
        # Load and validate configuration
        config = ConfigLoader.load_from_file(config_path)
        logger = create_logger(config)

        logger.info(f"Loaded configuration from {config_path}")

        if args.validate_only:
            logger.info("Configuration validation completed successfully")
            return 0

        if args.dry_run:
            logger.info("Dry-run mode: showing what would be processed")
            # Add dry-run logic here
            return 0

        run_id = str(uuid.uuid4())

        w = WorkspaceClient()
        # Note: In production, tokens should be retrieved from Databricks secrets
        source_host = config.source_databricks_connect_config.host
        source_token = w.dbutils.secrets.get(
            config.source_databricks_connect_config.token.secret_scope,
            config.source_databricks_connect_config.token.secret_key,
        )
        target_host = config.target_databricks_connect_config.host
        target_token = w.dbutils.secrets.get(
            config.target_databricks_connect_config.token.secret_scope,
            config.target_databricks_connect_config.token.secret_key,
        )
        if args.operation in ["all", "backup"]:
            # Check if backup is configured
            backup_catalogs = [
                cat
                for cat in config.target_catalogs
                if cat.backup_config and cat.backup_config.enabled
            ]

            if backup_catalogs:
                logger.info(
                    f"Running backup operations for {len(backup_catalogs)} catalogs"
                )

                result = run_backup_only(
                    config,
                    logger,
                    run_id,
                    source_host,
                    source_token,
                    target_host,
                    target_token,
                )
                if result != 0:
                    return result
            elif args.operation == "backup":
                logger.error("No catalogs configured for backup")
                return 1

        if args.operation in ["all", "delta_share"]:
            logger.info("Delta share operations not yet implemented")

        if args.operation in ["all", "replication"]:
            # Check if replication is configured
            replication_catalogs = [
                cat
                for cat in config.target_catalogs
                if cat.replication_config and cat.replication_config.enabled
            ]

            if replication_catalogs:
                logger.info(
                    f"Running replication operations for {len(replication_catalogs)} catalogs"
                )

                result = run_replication_only(
                    config, logger, run_id, target_host, target_token
                )
                if result != 0:
                    return result
            elif args.operation == "replication":
                logger.error("No catalogs configured for replication")
                return 1

        if args.operation in ["all", "reconciliation"]:
            # Check if reconciliation is configured
            reconciliation_catalogs = [
                cat
                for cat in config.target_catalogs
                if cat.reconciliation_config and cat.reconciliation_config.enabled
            ]

            if reconciliation_catalogs:
                logger.info(
                    f"Running reconciliation operations for {len(reconciliation_catalogs)} catalogs"
                )

                result = run_reconciliation_only(
                    config, logger, run_id, target_host, target_token
                )
                if result != 0:
                    return result
            elif args.operation == "reconciliation":
                logger.error("No catalogs configured for reconciliation")
                return 1

        logger.info("All requested operations completed successfully")
        return 0

    except ConfigurationError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Operation failed: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
