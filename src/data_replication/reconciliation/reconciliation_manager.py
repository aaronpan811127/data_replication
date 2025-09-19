"""
Reconciliation manager with concurrent execution and comprehensive logging.

This module provides a complete reconciliation solution with concurrent processing,
retry logic, and detailed audit logging.
"""

from datetime import datetime, timezone
from typing import List

from data_replication.config.models import (
    RunResult,
    RunSummary,
)
from data_replication.core.base_manager import BaseManager

from .reconciliation_provider import ReconciliationProvider


class ReconciliationManager(BaseManager):
    """Manager for coordinating reconciliation operations across multiple catalogs."""

    def run_reconciliation_operations(self) -> RunSummary:
        """
        Run reconciliation operations for all configured catalogs.

        Returns:
            RunSummary with operation results
        """
        start_time = datetime.now(timezone.utc)

        self.logger.info(f"Starting reconciliation operations (run_id: {self.run_id})")

        try:
            # Get catalogs that need reconciliation
            reconciliation_catalogs = [
                catalog
                for catalog in self.config.target_catalogs
                if catalog.reconciliation_config and catalog.reconciliation_config.enabled
            ]

            if not reconciliation_catalogs:
                self.logger.info("No catalogs configured for reconciliation")
                return self.create_summary(start_time, [], "reconciliation")

            results = self._run_reconciliation_operations()

            summary = self.create_summary(start_time, results, "reconciliation")

            # Log individual results
            self.log_run_results(results)
            # Log final summary
            self.log_run_summary(summary, "reconciliation")

            return summary

        except Exception as e:
            error_msg = f"Reconciliation operation failed: {str(e)}"
            self.logger.error(error_msg, exc_info=True)

            summary = self.create_summary(start_time, [], "reconciliation", error_msg)
            self.log_run_summary(summary, "reconciliation")

            return summary

        finally:
            if self.spark:
                self.spark.stop()

    def _run_reconciliation_operations(self) -> List[RunResult]:
        """Run reconciliation operations with concurrency within schemas."""
        results = []
        max_workers = self.config.concurrency.max_workers

        self.logger.info(
            f"Running reconciliation with {max_workers} concurrent workers per schema"
        )

        # Process catalogs sequentially, but use concurrency within each schema
        for catalog in self.config.target_catalogs:
            try:
                provider = ReconciliationProvider(
                    self.spark,
                    self.logger,
                    self.db_ops,
                    self.run_id,
                    catalog,
                    self.config.retry,
                    self.config.concurrency.max_workers,
                    self.config.concurrency.timeout_seconds,
                )
                catalog_results = provider.reconcile_catalog()
                results.extend(catalog_results)

                successful = sum(1 for r in catalog_results if r.status == "success")
                total = len(catalog_results)

                self.logger.info(
                    f"Completed reconciliation for catalog {catalog.catalog_name}: "
                    f"{successful}/{total} operations successful"
                )

            except Exception as e:
                error_msg = (
                    f"Reconciliation failed for catalog {catalog.catalog_name}: {str(e)}"
                )
                self.logger.error(error_msg, exc_info=True)

                # Create failure result
                now = datetime.now(timezone.utc)
                result = RunResult(
                    operation_type="reconciliation",
                    catalog_name=catalog.catalog_name,
                    status="failed",
                    start_time=now.isoformat(),
                    end_time=now.isoformat(),
                    error_message=error_msg,
                )
                results.append(result)

        return results