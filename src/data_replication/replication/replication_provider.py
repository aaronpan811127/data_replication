"""
Replication provider implementation for data replication system.

This module handles replication operations with support for deep clone,
streaming tables, materialized views, and intermediate catalogs.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import List, Tuple

from databricks.connect import DatabricksSession

from data_replication.audit.logger import DataReplicationLogger
from data_replication.config.models import (
    RetryConfig,
    RunResult,
    TableConfig,
    TargetCatalogConfig,
)
from data_replication.databricks_operations import DatabricksOperations
from data_replication.utils import retry_with_logging


class ReplicationProvider:
    """Provider for replication operations using deep clone and insert overwrite."""

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
        Initialize the replication provider.

        Args:
            spark: Spark session for source databricks workspace
            logger: Logger instance for audit logging
            db_ops: Databricks operations helper
            run_id: Unique run identifier
            catalog_config: Target catalog configuration containing replication config
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

    def replicate_catalog(self) -> List[RunResult]:
        """
        Replicate all tables in a catalog based on configuration.

        Returns:
            List of RunResult objects for each replication operation
        """
        if (
            not self.catalog_config.replication_config
            or not self.catalog_config.replication_config.enabled
        ):
            self.logger.info(
                f"Replication is disabled for catalog: {self.catalog_config.catalog_name}"
            )
            return []

        replication_config = self.catalog_config.replication_config
        results = []
        start_time = datetime.now(timezone.utc)

        try:
            # Ensure intermediate catalog exists
            if replication_config.intermediate_catalog:
                self.db_ops.create_catalog_if_not_exists(
                    replication_config.intermediate_catalog
                )

            self.logger.info(
                f"Starting replication for catalog: {self.catalog_config.catalog_name}",
                extra={"run_id": self.run_id, "operation": "replication"},
            )

            # Get schemas to replicate
            schema_list = self._get_schemas()

            for schema_name, table_list in schema_list:
                schema_results = self._replicate_schema(schema_name, table_list)
                results.extend(schema_results)

        except Exception as e:
            error_msg = f"Failed to replicate catalog {self.catalog_config.catalog_name}: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            result = RunResult(
                operation_type="replication",
                catalog_name=self.catalog_config.catalog_name,
                status="failed",
                start_time=start_time.isoformat(),
                end_time=datetime.now(timezone.utc).isoformat(),
                error_message=error_msg,
            )
            results.append(result)

        return results

    def _replicate_schema(
        self, schema_name: str, table_list: List[TableConfig]
    ) -> List[RunResult]:
        """
        Replicate all tables in a schema concurrently.

        Args:
            schema_name: Schema name to replicate
            table_list: List of table configurations to replicate

        Returns:
            List of RunResult objects for each table replication
        """
        results = []
        replication_config = self.catalog_config.replication_config
        catalog_name = self.catalog_config.catalog_name
        intermediate_catalog = replication_config.intermediate_catalog
        start_time = datetime.now(timezone.utc)

        try:
            # Create intermediate schema if needed
            if intermediate_catalog:
                self.db_ops.create_schema_if_not_exists(
                    intermediate_catalog, schema_name
                )

            # Get all tables in the schema
            tables = self._get_tables(catalog_name, schema_name, table_list)

            if not tables:
                self.logger.info(
                    f"No tables found in schema {catalog_name}.{schema_name}"
                )
                return results

            self.logger.info(
                f"Starting concurrent replication of {len(tables)} tables in schema {catalog_name}.{schema_name} using {self.max_workers} workers"
            )

            # Process tables concurrently within the schema
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit replication jobs for all tables in the schema
                future_to_table = {
                    executor.submit(
                        self._replicate_table, schema_name, table_name
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
                                f"Successfully replicated table {catalog_name}.{schema_name}.{table_name}"
                            )
                        else:
                            self.logger.error(
                                f"Failed to replicate table {catalog_name}.{schema_name}.{table_name}: {result.error_message}"
                            )

                    except Exception as e:
                        error_msg = f"Failed to replicate table {catalog_name}.{schema_name}.{table_name}: {str(e)}"
                        self.logger.error(error_msg, exc_info=True)
                        result = RunResult(
                            operation_type="replication",
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
            error_msg = f"Failed to replicate schema {self.catalog_config.catalog_name}.{schema_name}: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            result = RunResult(
                operation_type="replication",
                catalog_name=catalog_name,
                schema_name=schema_name,
                status="failed",
                start_time=start_time.isoformat(),
                end_time=datetime.now(timezone.utc).isoformat(),
                error_message=error_msg,
            )
            results.append(result)

        return results

    def _replicate_table(
        self,
        schema_name: str,
        table_name: str,
    ) -> RunResult:
        """
        Replicate a single table using deep clone or insert overwrite.

        Args:
            schema_name: Schema name
            table_name: Table name to replicate

        Returns:
            RunResult object for the replication operation
        """
        start_time = datetime.now(timezone.utc)
        replication_config = self.catalog_config.replication_config
        source_catalog = replication_config.source_catalog
        target_catalog = self.catalog_config.catalog_name
        source_table = f"{source_catalog}.{schema_name}.{table_name}"
        target_table = f"{target_catalog}.{schema_name}.{table_name}"

        self.logger.info(
            f"Starting replication: {source_table} -> {target_table}",
            extra={"run_id": self.run_id, "operation": "replication"},
        )

        try:
            actual_target_table, dlt_flag = self.db_ops.get_table_name_and_dlt_flag(
                target_table
            )

            # Use custom retry decorator with logging
            @retry_with_logging(self.retry, self.logger)
            def replication_operation(query: str):
                self.spark.sql(query)
                return True

            # Determine replication strategy based on table type and config
            if replication_config.intermediate_catalog:
                # Two-step replication via intermediate catalog
                result, last_exception, attempt, max_attempts, step1_query, step2_query  = (
                    self._replicate_via_intermediate(
                        source_table,
                        actual_target_table,
                        schema_name,
                        table_name,
                        dlt_flag,
                        replication_operation,
                    )
                )
            else:
                # Direct replication
                result, last_exception, attempt, max_attempts, step1_query, step2_query = self._replicate_direct(
                    source_table, actual_target_table, dlt_flag, replication_operation
                )

            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()

            if result:
                self.logger.info(
                    f"Replication completed successfully: {source_table} -> {target_table} ({duration:.2f}s)",
                    extra={"run_id": self.run_id, "operation": "replication"},
                )

                return RunResult(
                    operation_type="replication",
                    catalog_name=target_catalog,
                    schema_name=schema_name,
                    table_name=table_name,
                    status="success",
                    start_time=start_time.isoformat(),
                    end_time=end_time.isoformat(),
                    details={
                        "target_table": actual_target_table,
                        "source_table": source_table,
                        "dlt_flag": dlt_flag,
                        "intermediate_catalog": replication_config.intermediate_catalog,
                        "step1_query": step1_query,
                        "step2_query": step2_query,
                    },
                    attempt_number=attempt,
                    max_attempts=max_attempts,
                )

            error_msg = f"Replication failed after {max_attempts} attempts: {source_table} -> {target_table}"
            if last_exception:
                error_msg += f" | Last error: {str(last_exception)}"

            self.logger.error(
                error_msg,
                extra={"run_id": self.run_id, "operation": "replication"},
            )

            return RunResult(
                operation_type="replication",
                catalog_name=target_catalog,
                schema_name=schema_name,
                table_name=table_name,
                status="failed",
                start_time=start_time.isoformat(),
                end_time=end_time.isoformat(),
                error_message=error_msg,
                details={
                    "target_table": actual_target_table,
                    "source_table": source_table,
                    "dlt_flag": dlt_flag,
                    "intermediate_catalog": replication_config.intermediate_catalog,
                    "step1_query": step1_query,
                    "step2_query": step2_query,
                },
                attempt_number=attempt,
                max_attempts=max_attempts,
            )

        except Exception as e:
            end_time = datetime.now(timezone.utc)
            error_msg = f"Failed to replicate table {source_table}: {str(e)}"

            self.logger.error(
                error_msg,
                extra={"run_id": self.run_id, "operation": "replication"},
                exc_info=True,
            )

            return RunResult(
                operation_type="replication",
                catalog_name=target_catalog,
                schema_name=schema_name,
                table_name=table_name,
                status="failed",
                start_time=start_time.isoformat(),
                end_time=end_time.isoformat(),
                error_message=error_msg,
                details={
                    "target_table": actual_target_table,
                    "source_table": source_table,
                    "dlt_flag": dlt_flag,
                    "intermediate_catalog": replication_config.intermediate_catalog,
                    "step1_query": step1_query,
                    "step2_query": step2_query,
                }
            )

    def _replicate_via_intermediate(
        self,
        source_table: str,
        target_table: str,
        schema_name: str,
        table_name: str,
        dlt_flag: bool,
        replication_operation,
    ) -> tuple:
        """Replicate table via intermediate catalog."""
        replication_config = self.catalog_config.replication_config
        intermediate_table = (
            f"{replication_config.intermediate_catalog}.{schema_name}.{table_name}"
        )

        # Step 1: Deep clone to intermediate
        step1_query = f"CREATE OR REPLACE TABLE {intermediate_table} DEEP CLONE {source_table}"

        result1, last_exception, attempt, max_attempts = replication_operation(
            step1_query
        )
        if not result1:
            return result1, last_exception, attempt, max_attempts, step1_query, None

        # Step 2: Use insert overwrite to replicate from intermediate to target
        step2_query = self._build_insert_overwrite_query(
            intermediate_table, target_table
            )

        return *replication_operation(step2_query), step1_query, step2_query

    def _replicate_direct(
        self,
        source_table: str,
        target_table: str,
        dlt_flag: bool,
        replication_operation,
    ) -> tuple:
        """Replicate table directly to target."""

        # Use insert overwrite for streaming tables/materialized views
        step1_query = self._build_insert_overwrite_query(source_table, target_table)

        return *replication_operation(step1_query), step1_query, None

    def _build_insert_overwrite_query(
        self, source_table: str, target_table: str
    ) -> str:
        """Build insert overwrite query based on enforce_schema setting."""
        replication_config = self.catalog_config.replication_config

        if replication_config.enforce_schema:
            # Use SELECT * (all fields)
            return f"INSERT OVERWRITE {target_table} SELECT * FROM {source_table}"
        else:
            # Get common fields between source and target
            common_fields = self.db_ops.get_common_fields(source_table, target_table)
            if common_fields:
                field_list = "`" + "`,`".join(common_fields) + "`"
                return f"INSERT OVERWRITE {target_table} ({field_list}) SELECT {field_list} FROM {source_table}"

            # Fallback to SELECT * if no common fields found
            return f"INSERT OVERWRITE {target_table} SELECT * FROM {source_table}"

    def _get_schemas(
        self,
    ) -> List[Tuple[str, List[TableConfig]]]:
        """
        Get list of schemas to replicate based on configuration.

        Returns:
            List of schema names and table list to replicate
        """
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
                self.catalog_config.replication_config.source_catalog,
                self.catalog_config.schema_filter_expression,
            )
        else:
            # Replicate all schemas
            schema_list = self.db_ops.get_all_schemas(
                self.catalog_config.replication_config.source_catalog
            )

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
