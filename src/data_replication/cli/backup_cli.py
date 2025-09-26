#!/usr/bin/env python3
"""
Command-line interface for backup operations.

This module provides a dedicated CLI for running backup operations
independently of other replication features.
"""

import argparse
import sys
from pathlib import Path

from databricks.connect import DatabricksSession
from ..audit.logger import DataReplicationLogger
from ..config.loader import ConfigLoader
from ..exceptions import ConfigurationError
from ..providers.provider_factory import ProviderFactory


def create_logger(config) -> DataReplicationLogger:
    """Create logger instance from configuration."""
    logger = DataReplicationLogger("backup_operations")
    if hasattr(config, "logging") and config.logging:
        logger.setup_logging(config.logging)
    return logger


def create_spark_session() -> DatabricksSession:
    """Create Databricks Spark session."""
    return DatabricksSession.builder.serverless(True).getOrCreate()  # type: ignore


def validate_backup_configuration(config) -> bool:
    """
    Validate that at least one catalog has backup enabled.

    Args:
        config: Replication system configuration

    Returns:
        True if backup is configured, False otherwise
    """
    for catalog in config.target_catalogs:
        if catalog.backup_config and catalog.backup_config.enabled:
            return True
    return False


def run_backup_command(
    config_path: Path, validate_only: bool = False, dry_run: bool = False
) -> int:
    """
    Run backup command with given parameters.

    Args:
        config_path: Path to configuration file
        validate_only: Only validate configuration
        dry_run: Run in dry-run mode (validate and log what would be done)

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    try:
        # Load and validate configuration
        config = ConfigLoader.load_from_file(config_path)
        logger = create_logger(config)

        logger.info(f"Loaded configuration from {config_path}")

        # Validate backup configuration
        if not validate_backup_configuration(config):
            logger.error("No catalogs configured for backup operations")
            return 1

        # Count configured backup operations
        backup_catalogs = [
            cat for cat in config.target_catalogs if cat.backup_config and cat.backup_config.enabled
        ]

        logger.info(f"Found {len(backup_catalogs)} catalogs configured for backup")

        for catalog in backup_catalogs:
            if catalog.backup_config:  # Safe check since we already filtered for enabled configs
                logger.info(
                    f"  - {catalog.catalog_name}: "
                    f"{catalog.backup_config.source_catalog} -> "
                    f"{catalog.backup_config.backup_catalog}"
                )

        if validate_only:
            logger.info("Configuration validation completed successfully")
            return 0

        if dry_run:
            logger.info("Dry-run mode: would execute backup operations for the above catalogs")
            return 0

        # Create Spark sessions
        spark = create_spark_session()
        logging_spark = create_spark_session()

        # Run backup operations
        backup_factory = ProviderFactory("backup", config, spark, logging_spark, logger)
        summary = backup_factory.run_backup_operations()

        # Log results
        if summary.failed_operations > 0:
            logger.error(
                f"Backup completed with {summary.failed_operations} failures out of "
                f"{summary.successful_operations + summary.failed_operations} operations"
            )
            return 1
        
        logger.info(
            f"All backup operations completed successfully "
            f"({summary.successful_operations} operations)"
        )
        return 0

    except ConfigurationError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Backup operation failed: {e}", file=sys.stderr)
        return 1


def main():
    """Main entry point for backup CLI."""
    parser = argparse.ArgumentParser(
        description="Data Replication Backup Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run backup operations
  python -m data_replication.cli.backup_cli config.yaml
  
  # Validate configuration only
  python -m data_replication.cli.backup_cli config.yaml --validate-only
  
  # Dry run to see what would be backed up
  python -m data_replication.cli.backup_cli config.yaml --dry-run
        """,
    )

    parser.add_argument("config_file", help="Path to the YAML configuration file")

    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Only validate configuration without running backup operations",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be backed up without actually performing operations",
    )

    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")

    args = parser.parse_args()

    # Validate config file exists
    config_path = Path(args.config_file)
    if not config_path.exists():
        print(f"Error: Configuration file not found: {config_path}", file=sys.stderr)
        return 1

    # Run backup command
    exit_code = run_backup_command(
        config_path=config_path,
        validate_only=args.validate_only,
        dry_run=args.dry_run,
    )

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
