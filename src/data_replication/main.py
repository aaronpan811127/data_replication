#!/usr/bin/env python3
"""
Main entry point for the data replication system.

This module provides the primary CLI interface for all replication operations
including backup, delta share, replication, and reconciliation.
"""

import sys
import os
import argparse
from pathlib import Path
import time
import uuid
from databricks.sdk import WorkspaceClient
from data_replication.utils import create_spark_session
from data_replication.audit.logger import DataReplicationLogger
from data_replication.config.loader import ConfigLoader
from data_replication.exceptions import ConfigurationError
from data_replication.providers.provider_factory import ProviderFactory

# Determine the parent directory of the current script for module imports
pwd = ""
try:
    pwd = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )  # type: ignore  # noqa: E501
    parent_folder = os.path.dirname(os.path.dirname(pwd))
except NameError:
    # Fallback when running outside Databricks (e.g. local development or tests)
    parent_folder = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Append the framework path to the system path for module resolution
if parent_folder not in sys.path:
    sys.path.append(parent_folder)


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
    backup_factory = ProviderFactory(
        "backup", config, spark, logging_spark, logger, run_id
    )
    logger.info(f"Backup operations in {config.execute_at.value} environment")
    summary = backup_factory.run_backup_operations()

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

    replication_factory = ProviderFactory(
        "replication", config, spark, spark, logger, run_id
    )
    logger.info(f"Replication operations in {config.execute_at.value} environment")
    summary = replication_factory.run_replication_operations()

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

    reconciliation_factory = ProviderFactory(
        "reconciliation", config, spark, spark, logger, run_id
    )
    logger.info(f"Reconciliation operations in {config.execute_at.value} environment")
    summary = reconciliation_factory.run_reconciliation_operations()

    if summary.failed_operations > 0:
        logger.error(
            f"Reconciliation completed with {summary.failed_operations} failures"
        )
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
        choices=["all", "backup", "replication", "reconciliation"],
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

        if config.execute_at == "source" and args.operation not in ["backup"]:
            logger.error(
                "When execute_at is 'source', only 'backup' operation is allowed"
            )
            return 1

        if config.execute_at == "target" and args.operation in ["backup", "all"]:
            if (
                not config.source_databricks_connect_config.host
                or not config.source_databricks_connect_config.token
            ):
                logger.error(
                    "Source Databricks Connect configuration must be provided for backup operations when execute_at is 'target'"
                )
                return 1

        run_id = str(uuid.uuid4())

        w = WorkspaceClient()
        # Note: In production, tokens should be retrieved from Databricks secrets
        source_host = None
        source_token = None
        target_host = None
        target_token = None

        if (
            config.source_databricks_connect_config.host
            and config.source_databricks_connect_config.token
        ):
            source_host = config.source_databricks_connect_config.host
            source_token = w.dbutils.secrets.get(
                config.source_databricks_connect_config.token.secret_scope,
                config.source_databricks_connect_config.token.secret_key,
            )
        if (
            config.target_databricks_connect_config.host
            and config.target_databricks_connect_config.token
        ):
            target_host = config.target_databricks_connect_config.host
            target_token = w.dbutils.secrets.get(
                config.target_databricks_connect_config.token.secret_scope,
                config.target_databricks_connect_config.token.secret_key,
            )

        logger.info(
            f"Log run_id {run_id} in {config.audit_config.audit_table if config.audit_config and config.audit_config.audit_table else 'No audit table configured'}"
        )

        logger.info(f"All Operations Begins {'-' * 60}")

        if args.operation in ["all", "backup"]:
            # Check if backup is configured
            backup_catalogs = [
                cat
                for cat in config.target_catalogs
                if cat.backup_config and cat.backup_config.enabled
            ]

            if backup_catalogs:
                logger.info(f"Backup Begins {'-' * 60}")
                logger.info(
                    f"Running backup operations for {len(backup_catalogs)} catalogs"
                )

                logging_host = target_host
                logging_token = target_token
                if config.execute_at == "source":
                    logging_host = source_host
                    logging_token = source_token

                run_backup_only(
                    config,
                    logger,
                    run_id,
                    source_host,
                    source_token,
                    logging_host,
                    logging_token,
                )
                logger.info(f"Backup Ends {'-' * 60}")
            elif args.operation == "backup":
                logger.info("Backup disabled or No catalogs configured for backup")

        # if args.operation in ["all", "delta_share"]:
        #     logger.info("Delta share operations not yet implemented")

        if args.operation in ["all", "replication"]:
            # if args.operation == "all":
            #     time.sleep(
            #         120
            #     )  # Ensure delta share metadata is available before replication

            # Check if replication is configured
            replication_catalogs = [
                cat
                for cat in config.target_catalogs
                if cat.replication_config and cat.replication_config.enabled
            ]

            if replication_catalogs:
                logger.info(f"Replication Begins {'-' * 60}")
                logger.info(
                    f"Running replication operations for {len(replication_catalogs)} catalogs"
                )

                run_replication_only(config, logger, run_id, target_host, target_token)
                logger.info(f"Replication Ends {'-' * 60}")
            elif args.operation == "replication":
                logger.info(
                    "Replication disabled or No catalogs configured for replication"
                )

        if args.operation in ["all", "reconciliation"]:
            # Check if reconciliation is configured
            reconciliation_catalogs = [
                cat
                for cat in config.target_catalogs
                if cat.reconciliation_config and cat.reconciliation_config.enabled
            ]

            if reconciliation_catalogs:
                logger.info(f"Reconciliation Begins {'-' * 60}")
                logger.info(
                    f"Running reconciliation operations for {len(reconciliation_catalogs)} catalogs"
                )

                run_reconciliation_only(
                    config, logger, run_id, target_host, target_token
                )
                logger.info(f"Reconciliation Ends {'-' * 60}")
            elif args.operation == "reconciliation":
                logger.info(
                    "Reconciliation disabled or No catalogs configured for reconciliation"
                )

        logger.info(f"All Operations Ends {'-' * 60}")

    except ConfigurationError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
    except Exception as e:
        print(f"Operation failed: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
