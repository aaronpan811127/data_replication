"""
Backup manager with concurrent execution and comprehensive logging.

This module provides a complete backup solution with concurrent processing,
retry logic, and detailed audit logging.
"""

from datetime import datetime, timezone
from typing import List

from data_replication.config.models import (
    RunResult,
    RunSummary,
)
from data_replication.core.base_manager import BaseManager
from data_replication.audit.audit_logger import AuditLogger
from .backup_provider import BackupProvider



class BackupManager(BaseManager):
    """Manager for coordinating backup operations across multiple catalogs."""

    def __init__(self, config, spark, logger, run_id):
        super().__init__(config, spark, logger, run_id)
        self.audit_logger = AuditLogger(
            spark, self.db_ops, logger, run_id, config.audit_config.backup_audit_table
        )

    def run_backup_operations(self) -> RunSummary:
        """
        Run backup operations for all configured catalogs.

        Returns:
            RunSummary with operation results
        """
        start_time = datetime.now(timezone.utc)

        self.logger.info(f"Starting backup operations (run_id: {self.run_id})")

        try:

            # Get catalogs that need backup
            backup_catalogs = [
                catalog
                for catalog in self.config.target_catalogs
                if catalog.backup_config and catalog.backup_config.enabled
            ]

            if not backup_catalogs:
                self.logger.info("No catalogs configured for backup")
                return self.create_summary(
                    start_time, [], "backup"
                )

            results = self._run_backup_operations()

            summary = self.create_summary(
                start_time, results, "backup"
            )

            # Log individual results
            self.log_run_results(results)
            # Log final summary
            self.log_run_summary(summary, "backup")

            return summary

        except Exception as e:
            error_msg = f"Backup operation failed: {str(e)}"
            self.logger.error(error_msg, exc_info=True)

            summary = self.create_summary(
                start_time, [], "backup", error_msg
            )
            self.log_run_summary(summary, "backup")

            return summary

        finally:
            if self.spark:
                self.spark.stop()

    def _run_backup_operations(self) -> List[RunResult]:
        """Run backup operations with concurrency within schemas."""
        results = []
        max_workers = self.config.concurrency.max_workers

        self.logger.info(
            f"Running backup with {max_workers} concurrent workers per schema"
        )

        # Process catalogs sequentially, but use concurrency within each schema
        for catalog in self.config.target_catalogs:
            try:
                provider = BackupProvider(
                    self.spark,
                    self.logger,
                    self.db_ops,
                    self.run_id,
                    catalog,
                    self.config.retry,
                    self.config.concurrency.max_workers,
                    self.config.concurrency.timeout_seconds,
                )
                catalog_results = provider.backup_catalog()
                results.extend(catalog_results)

                successful = sum(1 for r in catalog_results if r.status == "success")
                total = len(catalog_results)

                self.logger.info(
                    f"Completed backup for catalog {catalog.catalog_name}: "
                    f"{successful}/{total} operations successful"
                )

            except Exception as e:
                error_msg = (
                    f"Backup failed for catalog {catalog.catalog_name}: {str(e)}"
                )
                self.logger.error(error_msg, exc_info=True)

                # Create failure result
                now = datetime.now(timezone.utc)
                result = RunResult(
                    operation_type="backup",
                    catalog_name=catalog.catalog_name,
                    status="failed",
                    start_time=now.isoformat(),
                    end_time=now.isoformat(),
                    duration_seconds=0.0,
                    error_message=error_msg,
                )
                results.append(result)

        return results
