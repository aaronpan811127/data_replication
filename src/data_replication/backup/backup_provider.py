"""
Backup provider implementation for data replication system.

This module handles backup operations using deep clone functionality
for both delta tables and streaming tables/materialized views.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import List, Tuple

from databricks.connect import DatabricksSession

from data_replication.audit.logger import DataReplicationLogger
from data_replication.config.models import (
    RetryConfig,
    RunResult,
    TargetCatalogConfig,
    TableConfig,
)
from data_replication.databricks_operations import DatabricksOperations
from data_replication.utils import retry_with_logging


class BackupProvider:
    """Provider for backup operations using deep clone."""

    def __init__(
        self,
        spark: DatabricksSession,
        logger: DataReplicationLogger,
        db_ops: DatabricksOperations,
        run_id: str,
        catalog_config: TargetCatalogConfig,
        retry: RetryConfig = None,
        max_workers: int = 2,
        timeout_seconds: int = 1800,
    ):
        """
        Initialize the backup provider.

        Args:
            spark: Spark session for source databricks workspace
            logger: Logger instance for audit logging
            run_id: Unique run identifier
            catalog_config: Target catalog configuration containing backup config
            retry: Retry configuration
            max_workers: Maximum number of concurrent workers
            timeout_seconds: Timeout for operations
        """
        self.spark = spark
        self.logger = logger
        self.db_ops = db_ops
        self.run_id = run_id
        self.catalog_config = catalog_config
        self.retry = retry if retry else RetryConfig(max_attempts=1, delay_seconds=5)
        self.max_workers = max_workers
        self.timeout_seconds = timeout_seconds

    def backup_catalog(self) -> List[RunResult]:
        """
        Backup all tables in a catalog based on configuration.

        Returns:
            List of RunResult objects for each backup operation
        """
        if (
            not self.catalog_config.backup_config
            or not self.catalog_config.backup_config.enabled
        ):
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
                schema_results = self._backup_schema(schema_name, table_list)
                results.extend(schema_results)

        except Exception as e:
            error_msg = (
                f"Failed to backup catalog {self.catalog_config.catalog_name}: {str(e)}"
            )
            self.logger.error(error_msg, exc_info=True)
            result = RunResult(
                operation_type="backup",
                catalog_name=self.catalog_config.catalog_name,
                status="failed",
                start_time=start_time.isoformat(),
                end_time=datetime.now(timezone.utc).isoformat(),
                error_message=error_msg,
            )
            results.append(result)

        return results

    def _backup_schema(
        self, schema_name: str, table_list: List[TableConfig]
    ) -> List[RunResult]:
        """
        Backup all tables in a schema concurrently.

        Args:
            schema_name: Schema name to backup

        Returns:
            List of RunResult objects for each table backup
        """
        results = []
        backup_config = self.catalog_config.backup_config
        catalog_name = (
            backup_config.source_catalog
            if backup_config.source_catalog
            else self.catalog_config.catalog_name
        )
        start_time = datetime.now(timezone.utc)

        try:
            # Ensure schema exists in backup catalog
            self.db_ops.create_schema_if_not_exists(
                backup_config.backup_catalog, schema_name
            )

            # Get all tables in the schema
            tables = self._get_tables(catalog_name, schema_name, table_list)

            if not tables:
                self.logger.info(
                    f"No tables found in schema {catalog_name}.{schema_name}"
                )
                return results

            self.logger.info(
                f"Starting concurrent backup of {len(tables)} tables in schema {catalog_name}.{schema_name} using {self.max_workers} workers"
            )

            # Process tables concurrently within the schema
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit backup jobs for all tables in the schema
                future_to_table = {
                    executor.submit(
                        self._backup_table, schema_name, table_name
                    ): table_name
                    for table_name in tables
                }

                # Collect results
                for future in as_completed(future_to_table):
                    table_name = future_to_table[future]
                    try:
                        result = future.result(timeout=self.timeout_seconds)
                        results.append(result)

                        if result.status == "success":
                            self.logger.info(
                                f"Successfully backed up table {catalog_name}.{schema_name}.{table_name}"
                            )
                        else:
                            self.logger.error(
                                f"Failed to backup table {catalog_name}.{schema_name}.{table_name}: {result.error_message}"
                            )

                    except Exception as e:
                        error_msg = f"Failed to backup table {catalog_name}.{schema_name}.{table_name}: {str(e)}"
                        self.logger.error(error_msg, exc_info=True)
                        result = RunResult(
                            operation_type="backup",
                            catalog_name=catalog_name,
                            schema_name=schema_name,
                            table_name=table_name,
                            status="failed",
                            start_time=start_time.isoformat(),
                            end_time=datetime.now(timezone.utc).isoformat(),
                            error_message=error_msg,
                        )
                        results.append(result)

        except Exception as e:
            error_msg = (
                f"Failed to backup schema {catalog_name}.{schema_name}: {str(e)}"
            )
            self.logger.error(error_msg, exc_info=True)
            result = RunResult(
                operation_type="backup",
                catalog_name=catalog_name,
                schema_name=schema_name,
                status="failed",
                start_time=start_time.isoformat(),
                end_time=datetime.now(timezone.utc).isoformat(),
                error_message=error_msg,
            )
            results.append(result)

        return results

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
                    f"Backup completed successfully: {source_table} -> {backup_table} ({duration:.2f}s)",
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
                error_msg = f"Backup failed after {max_attempts} attempts: {source_table} -> {backup_table}"
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

    def _get_schemas(
        self, catalog_name: str = None
    ) -> List[Tuple[str, List[TableConfig]]]:
        """
        Get list of schemas to replicate based on configuration.

        Returns:
            List of schema names and table list to replicate
        """

        if catalog_name:
            # Replicate all schemas
            schema_list = self.db_ops.get_all_schemas(catalog_name)
            return [[item, []] for item in schema_list]

        if self.catalog_config.target_schemas:
            # Use explicitly configured schemas
            return [
                (
                    schema.schema_name,
                    [
                        item
                        for item in (schema.tables or [])
                        if item not in (schema.exclude_tables or [])
                    ],
                )
                for schema in self.catalog_config.target_schemas
            ]

        if self.catalog_config.schema_filter_expression:
            # Use schema filter expression
            schema_list = self.db_ops.get_schemas_by_filter(
                self.catalog_config.catalog_name,
                self.catalog_config.schema_filter_expression,
            )
        else:
            # Replicate all schemas
            schema_list = self.db_ops.get_all_schemas(self.catalog_config.catalog_name)

        return [[item, []] for item in schema_list]

    def _get_tables(
        self, catalog_name: str, schema_name: str, table_list: List[TableConfig]
    ) -> List[str]:
        """
        Get list of tables to replicate in a schema based on configuration.

        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema
            table_list: List of table configurations to replicate in the schema
        Returns:
            List of table names to replicate
        """
        if table_list:
            # Use explicitly configured tables
            return [item.table_name for item in table_list]

        # Replicate all tables in the schema
        return self.db_ops.get_tables_in_schema(catalog_name, schema_name)
