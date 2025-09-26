"""
Base provider class for data replication operations.

This module provides shared functionality that can be reused across different
provider types like backup, replication, and reconciliation.
"""

from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import List, Tuple, Optional

from databricks.connect import DatabricksSession

from ..audit.logger import DataReplicationLogger
from ..config.models import (
    RetryConfig,
    RunResult,
    TargetCatalogConfig,
    TableConfig,
)
from ..databricks_operations import DatabricksOperations


class BaseProvider(ABC):
    """Base provider class with shared functionality for data replication operations."""

    def __init__(
        self,
        spark: DatabricksSession,
        logger: DataReplicationLogger,
        db_ops: DatabricksOperations,
        run_id: str,
        catalog_config: TargetCatalogConfig,
        retry: Optional[RetryConfig] = None,
        max_workers: int = 2,
        timeout_seconds: int = 1800,
    ):
        """
        Initialize the base provider.

        Args:
            spark: Spark session for databricks workspace
            logger: Logger instance for audit logging
            db_ops: Databricks operations helper
            run_id: Unique run identifier
            catalog_config: Target catalog configuration
            retry: Retry configuration
            max_workers: Maximum number of concurrent workers
            timeout_seconds: Timeout for operations
        """
        self.spark = spark
        self.logger = logger
        self.db_ops = db_ops
        self.run_id = run_id
        self.catalog_config = catalog_config
        self.retry = (
            retry if retry else RetryConfig(max_attempts=1, retry_delay_seconds=5)
        )
        self.max_workers = max_workers
        self.timeout_seconds = timeout_seconds

    @abstractmethod
    def process_catalog(self) -> List[RunResult]:
        """
        Process all tables in a catalog based on configuration.
        Must be implemented by subclasses.

        Returns:
            List of RunResult objects for each operation
        """

    @abstractmethod
    def process_table(self, schema_name: str, table_name: str) -> RunResult:
        """
        Process a single table.
        Must be implemented by subclasses.

        Args:
            schema_name: Schema name
            table_name: Table name

        Returns:
            RunResult object for the operation
        """

    @abstractmethod
    def get_operation_name(self) -> str:
        """
        Get the name of the operation for logging purposes.
        Must be implemented by subclasses.

        Returns:
            String name of the operation (e.g., "backup", "replication")
        """
        pass

    @abstractmethod
    def is_operation_enabled(self) -> bool:
        """
        Check if the operation is enabled in the configuration.
        Must be implemented by subclasses.

        Returns:
            True if operation is enabled, False otherwise
        """
        pass

    def process_schema_concurrently(
        self, schema_name: str, table_list: List[TableConfig]
    ) -> List[RunResult]:
        """
        Process all tables in a schema concurrently using ThreadPoolExecutor.

        Args:
            schema_name: Schema name to process
            table_list: List of table configurations to process

        Returns:
            List of RunResult objects for each table operation
        """
        results: List[RunResult] = []
        catalog_name = self.catalog_config.catalog_name
        start_time = datetime.now(timezone.utc)

        try:
            # Get all tables in the schema
            tables = self._get_tables(catalog_name, schema_name, table_list)

            if not tables:
                self.logger.info(
                    f"No tables found in schema {catalog_name}.{schema_name}"
                )
                return results

            self.logger.info(
                f"Starting concurrent {self.get_operation_name()} of {len(tables)} tables "
                f"in schema {catalog_name}.{schema_name} using {self.max_workers} workers"
            )

            # Process tables concurrently within the schema
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit jobs for all tables in the schema
                future_to_table = {
                    executor.submit(
                        self.process_table, schema_name, table_name
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
                                f"Successfully processed table "
                                f"{catalog_name}.{schema_name}.{table_name}"
                            )
                        else:
                            self.logger.error(
                                f"Failed to process table "
                                f"{catalog_name}.{schema_name}.{table_name}: "
                                f"{result.error_message}"
                            )

                    except Exception as e:
                        error_msg = (
                            f"Failed to process table "
                            f"{catalog_name}.{schema_name}.{table_name}: {str(e)}"
                        )
                        self.logger.error(error_msg, exc_info=True)
                        result = self._create_failed_result(
                            catalog_name, schema_name, table_name, error_msg, start_time
                        )
                        results.append(result)

        except Exception as e:
            error_msg = (
                f"Failed to process schema {catalog_name}.{schema_name}: {str(e)}"
            )
            self.logger.error(error_msg, exc_info=True)
            result = self._create_failed_result(
                catalog_name, schema_name, "", error_msg, start_time
            )
            results.append(result)

        return results

    def _create_failed_result(
        self,
        catalog_name: str,
        schema_name: str,
        table_name: Optional[str] = None,
        error_msg: str = "",
        start_time: Optional[datetime] = None,
    ) -> RunResult:
        """
        Create a failed RunResult object with consistent structure.

        Args:
            catalog_name: Catalog name
            schema_name: Schema name
            table_name: Optional table name
            error_msg: Error message
            start_time: Operation start time

        Returns:
            RunResult object with failed status
        """
        if start_time is None:
            start_time = datetime.now(timezone.utc)

        return RunResult(
            operation_type=self.get_operation_name(),
            catalog_name=catalog_name,
            schema_name=schema_name,
            table_name=table_name,
            status="failed",
            start_time=start_time.isoformat(),
            end_time=datetime.now(timezone.utc).isoformat(),
            error_message=error_msg,
        )

    def _get_schemas(
        self, catalog_name: Optional[str] = None
    ) -> List[Tuple[str, List[TableConfig]]]:
        """
        Get list of schemas to process based on configuration.

        Args:
            catalog_name: Optional catalog name to override config catalog

        Returns:
            List of tuples containing schema names and table lists to process
        """
        if catalog_name:
            # Process all schemas
            schema_list = self.db_ops.get_all_schemas(catalog_name)
            return [(item, []) for item in schema_list]

        if self.catalog_config.target_schemas:
            # Use explicitly configured schemas
            return [
                (
                    schema.schema_name,
                    schema.tables or [],
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
            # Process all schemas
            schema_list = self.db_ops.get_all_schemas(self.catalog_config.catalog_name)

        return [(item, []) for item in schema_list]

    def _get_tables(
        self,
        catalog_name: str,
        schema_name: str,
        table_list: List[TableConfig]
    ) -> List[str]:
        """
        Get list of tables to process in a schema based on configuration.

        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema
            table_list: List of table configurations to process in the schema

        Returns:
            List of table names to process
        """
        # Find exclusions for this schema from configuration
        exclude_names = set()
        if self.catalog_config.target_schemas:
            for schema_config in self.catalog_config.target_schemas:
                if schema_config.schema_name == schema_name:
                    if schema_config.exclude_tables:
                        exclude_names = {table.table_name for table in schema_config.exclude_tables}
                    break

        if table_list:
            # Use explicitly configured tables
            tables = [item.table_name for item in table_list]
        else:
            # Process all tables in the schema
            tables = self.db_ops.get_tables_in_schema(catalog_name, schema_name)

        # Apply exclusions first
        tables = [table for table in tables if table not in exclude_names]
        
        # Then filter by table type (STREAMING_TABLE and MANAGED only)
        return self.db_ops.filter_tables_by_type(catalog_name, schema_name, tables)
