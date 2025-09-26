"""
Backup provider implementation for data replication system.

This module handles backup operations using deep clone functionality
for both delta tables and streaming tables/materialized views.
"""

from datetime import datetime, timezone
from typing import List


from ..config.models import (
    RunResult,
)
from .base_provider import BaseProvider
from ..utils import retry_with_logging


class BackupProvider(BaseProvider):
    """Provider for backup operations using deep clone."""


    def get_operation_name(self) -> str:
        """Get the name of the operation for logging purposes."""
        return "backup"

    def is_operation_enabled(self) -> bool:
        """Check if the backup operation is enabled in the configuration."""
        return (
            self.catalog_config.backup_config
            and self.catalog_config.backup_config.enabled
        )

    def process_catalog(self) -> List[RunResult]:
        """Process all tables in a catalog for backup operations."""
        return self.backup_catalog()

    def process_table(self, schema_name: str, table_name: str) -> RunResult:
        """Process a single table for backup."""
        return self._backup_table(schema_name, table_name)

    def backup_catalog(self) -> List[RunResult]:
        """
        Backup all tables in a catalog based on configuration.

        Returns:
            List of RunResult objects for each backup operation
        """
        if not self.is_operation_enabled():
            self.logger.info(
                f"Backup is disabled for catalog: {self.catalog_config.catalog_name}"
            )
            return []

        backup_config = self.catalog_config.backup_config
        results = []
        start_time = datetime.now(timezone.utc)

        try:
            # Ensure backup catalog exists
            self.db_ops.create_catalog_if_not_exists(backup_config.backup_catalog)

            self.logger.info(
                f"Starting backup for catalog: {self.catalog_config.catalog_name}",
                extra={"run_id": self.run_id, "operation": "backup"},
            )

            # Get schemas to backup
            schema_list = self._get_schemas()

            for schema_name, table_list in schema_list:
                schema_results = self.process_schema_concurrently(
                    schema_name, table_list
                )
                results.extend(schema_results)

        except Exception as e:
            error_msg = (
                f"Failed to backup catalog {self.catalog_config.catalog_name}: {str(e)}"
            )
            self.logger.error(error_msg, exc_info=True)
            result = self._create_failed_result(
                self.catalog_config.catalog_name, "", None, error_msg, start_time
            )
            results.append(result)

        return results

    def process_schema_concurrently(
        self, schema_name: str, table_list: List
    ) -> List[RunResult]:
        """Override to add backup-specific schema setup."""
        backup_config = self.catalog_config.backup_config

        # Ensure schema exists in backup catalog before processing
        self.db_ops.create_schema_if_not_exists(
            backup_config.backup_catalog, schema_name
        )

        return super().process_schema_concurrently(schema_name, table_list)

    def _backup_table(
        self,
        schema_name: str,
        table_name: str,
    ) -> RunResult:
        """
        Backup a single table using deep clone.

        Args:
            schema_name: Schema name
            table_name: Table name to backup

        Returns:
            RunResult object for the backup operation
        """
        start_time = datetime.now(timezone.utc)
        backup_config = self.catalog_config.backup_config
        source_catalog = (
            backup_config.source_catalog
            if backup_config.source_catalog
            else self.catalog_config.catalog_name
        )
        source_table = f"{source_catalog}.{schema_name}.{table_name}"
        backup_table = f"{backup_config.backup_catalog}.{schema_name}.{table_name}"

        self.logger.info(
            f"Starting backup: {source_table} -> {backup_table}",
            extra={"run_id": self.run_id, "operation": "backup"},
        )

        try:
            table_details = self.db_ops.get_table_details(source_table)
            actual_source_table = table_details["table_name"]
            dlt_flag = table_details["is_dlt"]

            # Perform backup using deep clone and unset parentTableId property
            backup_query = f"""CREATE OR REPLACE TABLE {backup_table}
                            DEEP CLONE {actual_source_table};
                            """

            unset_query = f"""ALTER TABLE {backup_table}
                            UNSET TBLPROPERTIES (spark.sql.internal.pipelines.parentTableId)"""

            # Use custom retry decorator with logging
            @retry_with_logging(self.retry, self.logger)
            def backup_operation(backup_query: str, unset_query: str):
                self.spark.sql(backup_query)
                self.spark.sql(unset_query)
                return True

            result, last_exception, attempt, max_attempts = backup_operation(
                backup_query, unset_query
            )

            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()

            if result:
                self.logger.info(
                    f"Backup completed successfully: {source_table} -> {backup_table} "
                    f"({duration:.2f}s)",
                    extra={"run_id": self.run_id, "operation": "backup"},
                )

                return RunResult(
                    operation_type="backup",
                    catalog_name=source_catalog,
                    schema_name=schema_name,
                    table_name=table_name,
                    status="success",
                    start_time=start_time.isoformat(),
                    end_time=end_time.isoformat(),
                    details={
                        "backup_table": backup_table,
                        "source_table": actual_source_table,
                        "backup_query": backup_query,
                        "dlt_flag": dlt_flag,
                    },
                    attempt_number=attempt,
                    max_attempts=max_attempts,
                )
            else:
                error_msg = (
                    f"Backup failed after {max_attempts} attempts: "
                    f"{source_table} -> {backup_table}"
                )
                if last_exception:
                    error_msg += f" | Last error: {str(last_exception)}"

                self.logger.error(
                    error_msg,
                    extra={"run_id": self.run_id, "operation": "backup"},
                )

                return RunResult(
                    operation_type="backup",
                    catalog_name=source_catalog,
                    schema_name=schema_name,
                    table_name=table_name,
                    status="failed",
                    start_time=start_time.isoformat(),
                    end_time=end_time.isoformat(),
                    duration_seconds=duration,
                    error_message=error_msg,
                    details={
                        "backup_table": backup_table,
                        "source_table": actual_source_table,
                        "backup_query": backup_query,
                    },
                    attempt_number=attempt,
                    max_attempts=max_attempts,
                )

        except Exception as e:
            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()
            error_msg = f"Failed to backup table {source_table}: {str(e)}"

            self.logger.error(
                error_msg,
                extra={"run_id": self.run_id, "operation": "backup"},
                exc_info=True,
            )

            return RunResult(
                operation_type="backup",
                catalog_name=source_catalog,
                schema_name=schema_name,
                table_name=table_name,
                status="failed",
                start_time=start_time.isoformat(),
                end_time=end_time.isoformat(),
                duration_seconds=duration,
                error_message=error_msg,
                details={
                    "backup_table": backup_table,
                    "source_table": actual_source_table,
                    "backup_query": backup_query,
                    "dlt_flag": dlt_flag,
                },
            )
