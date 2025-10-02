"""
Replication provider implementation for data replication system.

This module handles replication operations with support for deep clone,
streaming tables, materialized views, and intermediate catalogs.
"""

from datetime import datetime, timezone
from typing import List


# from delta.tables import DeltaTable
from ..config.models import (
    RunResult,
)
from .base_provider import BaseProvider
from ..utils import retry_with_logging
from ..exceptions import ReplicationError, TableNotFoundError


class ReplicationProvider(BaseProvider):
    """Provider for replication operations using deep clone and insert overwrite."""

    def get_operation_name(self) -> str:
        """Get the name of the operation for logging purposes."""
        return "replication"

    def is_operation_enabled(self) -> bool:
        """Check if the replication operation is enabled in the configuration."""
        return (
            self.catalog_config.replication_config
            and self.catalog_config.replication_config.enabled
        )

    def process_table(self, schema_name: str, table_name: str) -> RunResult:
        """Process a single table for replication."""
        return self._replicate_table(schema_name, table_name)

    def setup_operation_catalogs(self) -> str:
        """Setup replication-specific catalogs."""
        replication_config = self.catalog_config.replication_config
        if replication_config.intermediate_catalog:
            self.db_ops.create_catalog_if_not_exists(
                replication_config.intermediate_catalog
            )
        self.logger.info(f"Cloning catalog: {replication_config.source_catalog}")
        return replication_config.source_catalog

    def process_schema_concurrently(
        self, schema_name: str, table_list: List
    ) -> List[RunResult]:
        """Override to add replication-specific schema setup."""
        replication_config = self.catalog_config.replication_config

        # Create intermediate schema if needed
        if replication_config.intermediate_catalog:
            self.db_ops.create_schema_if_not_exists(
                replication_config.intermediate_catalog, schema_name
            )

        return super().process_schema_concurrently(schema_name, table_list)

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
            # Refresh source table delta share metadata
            if not self.db_ops.table_exists(source_table):
                raise TableNotFoundError(f"Source table does not exist: {source_table}")

            try:
                table_details = self.db_ops.get_table_details(target_table)
                actual_target_table = table_details["table_name"]
                dlt_flag = table_details["is_dlt"]
                pipeline_id = table_details["pipeline_id"]
            except TableNotFoundError as exc:
                table_details = self.db_ops.get_table_details(source_table)
                if table_details["is_dlt"]:
                    raise TableNotFoundError(
                        f"Target table {target_table} does not exist. Cannot replicate DLT table without existing target."
                    ) from exc
                dlt_flag = False
                pipeline_id = None
                actual_target_table = target_table

            if self.db_ops.table_exists(target_table):
                if self.db_ops.get_table_fields(
                    source_table
                ) != self.db_ops.get_table_fields(actual_target_table):
                    raise ReplicationError(
                        f"Schema mismatch between intermediate table {source_table} "
                        f"and target table {target_table}"
                    )
            else:
                raise TableNotFoundError(f"Source table does not exist: {source_table}")

            # Use custom retry decorator with logging
            @retry_with_logging(self.retry, self.logger)
            def replication_operation(query: str):
                self.spark.sql(query)
                return True

            # Determine replication strategy based on table type and config
            if replication_config.intermediate_catalog:
                # Two-step replication via intermediate catalog
                (
                    result,
                    last_exception,
                    attempt,
                    max_attempts,
                    step1_query,
                    step2_query,
                ) = self._replicate_via_intermediate(
                    source_table,
                    actual_target_table,
                    schema_name,
                    table_name,
                    pipeline_id,
                    replication_operation,
                )
            else:
                # Direct replication
                (
                    result,
                    last_exception,
                    attempt,
                    max_attempts,
                    step1_query,
                    step2_query,
                ) = self._replicate_direct(
                    source_table,
                    actual_target_table,
                    pipeline_id,
                    replication_operation,
                )

            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()

            if result:
                self.logger.info(
                    f"Replication completed successfully: {source_table} -> {target_table} "
                    f"({duration:.2f}s)",
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

            error_msg = (
                f"Replication failed after {max_attempts} attempts: "
                f"{source_table} -> {target_table}"
            )
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

            # Wrap in ReplicationError for better error categorization
            if not isinstance(e, ReplicationError):
                e = ReplicationError(f"Replication operation failed: {str(e)}")

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
                },
                attempt_number=attempt,
                max_attempts=max_attempts,
            )

    def _replicate_via_intermediate(
        self,
        source_table: str,
        target_table: str,
        schema_name: str,
        table_name: str,
        pipeline_id: str,
        replication_operation,
    ) -> tuple:
        """Replicate table via intermediate catalog."""
        replication_config = self.catalog_config.replication_config
        intermediate_table = (
            f"{replication_config.intermediate_catalog}.{schema_name}.{table_name}"
        )

        # Step 1: Deep clone to intermediate
        step1_query = (
            f"CREATE OR REPLACE TABLE {intermediate_table} DEEP CLONE {source_table}"
        )

        result1, last_exception, attempt, max_attempts = replication_operation(
            step1_query
        )
        if not result1:
            return result1, last_exception, attempt, max_attempts, step1_query, None

        # # Step 2: Use insert overwrite to replicate from intermediate to target
        # step2_query = self._build_insert_overwrite_query(
        #     intermediate_table, target_table
        # )

        # Use deep clone
        step2_query = self._build_deep_clone_query(
            source_table, target_table, pipeline_id
        )

        return (
            *replication_operation(step2_query),
            step1_query,
            step2_query,
        )

    def _replicate_direct(
        self,
        source_table: str,
        target_table: str,
        pipeline_id: str,
        replication_operation,
    ) -> tuple:
        """Replicate table directly to target."""

        # # Use insert overwrite for streaming tables/materialized views
        # step1_query = self._build_insert_overwrite_query(source_table, target_table)

        # Use deep clone
        step1_query = self._build_deep_clone_query(
            source_table, target_table, pipeline_id
        )

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

    def _build_deep_clone_query(
        self, source_table: str, target_table: str, pipeline_id: str = None
    ) -> str:
        """Build deep clone query."""

        sql = f"CREATE OR REPLACE TABLE {target_table} DEEP CLONE {source_table} "

        if pipeline_id:
            # For dlt streaming tables/materialized views, use CREATE OR REPLACE TABLE with pipelineId property
            return f"{sql} TBLPROPERTIES ('pipelines.pipelineId'='{pipeline_id}')"
        else:
            # For regular tables, just return the deep clone query
            return sql
