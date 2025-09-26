"""
Reconciliation provider implementation for data replication system.

This module handles reconciliation operations including schema checks,
row count validation, and missing data detection between source and target tables.
"""

from datetime import datetime, timezone
from typing import List, Dict, Any

from databricks.connect import DatabricksSession

from ..audit.logger import DataReplicationLogger
from ..config.models import (
    RetryConfig,
    RunResult,
    TableConfig,
    TargetCatalogConfig,
)
from .base_provider import BaseProvider
from ..databricks_operations import DatabricksOperations
from ..utils import retry_with_logging


class ReconciliationProvider(BaseProvider):
    """Provider for reconciliation operations between source and target tables."""

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
        Initialize the reconciliation provider.

        Args:
            spark: Spark session for target databricks workspace
            logger: Logger instance for audit logging
            db_ops: Databricks operations helper
            run_id: Unique run identifier
            catalog_config: Target catalog configuration containing reconciliation config
            retry: Retry configuration
            max_workers: Maximum number of concurrent workers
            timeout_seconds: Timeout for operations
        """
        super().__init__(
            spark, logger, db_ops, run_id, catalog_config, retry, max_workers, timeout_seconds
        )

    def get_operation_name(self) -> str:
        """Get the name of the operation for logging purposes."""
        return "reconciliation"

    def is_operation_enabled(self) -> bool:
        """Check if the reconciliation operation is enabled in the configuration."""
        return (
            self.catalog_config.reconciliation_config
            and self.catalog_config.reconciliation_config.enabled
        )

    def process_catalog(self) -> List[RunResult]:
        """Process all tables in a catalog for reconciliation operations."""
        return self.reconcile_catalog()

    def process_table(self, schema_name: str, table_name: str) -> RunResult:
        """Process a single table for reconciliation."""
        return self._reconcile_table(schema_name, table_name)

    def reconcile_catalog(self) -> List[RunResult]:
        """
        Reconcile all tables in a catalog based on configuration.

        Returns:
            List of RunResult objects for each reconciliation operation
        """
        if not self.is_operation_enabled():
            self.logger.info(
                f"Reconciliation is disabled for catalog: {self.catalog_config.catalog_name}"
            )
            return []

        reconciliation_config = self.catalog_config.reconciliation_config
        results = []
        start_time = datetime.now(timezone.utc)

        try:
            # Ensure reconciliation outputs catalog exists
            self.db_ops.create_catalog_if_not_exists(
                reconciliation_config.recon_outputs_catalog
            )

            self.logger.info(
                f"Starting reconciliation for catalog: {self.catalog_config.catalog_name}",
                extra={"run_id": self.run_id, "operation": "reconciliation"},
            )

            # Get schemas to reconcile
            schema_list = self._get_schemas()

            for schema_name, table_list in schema_list:
                schema_results = self.process_schema_concurrently(schema_name, table_list)
                results.extend(schema_results)

        except Exception as e:
            error_msg = f"Failed to reconcile catalog {self.catalog_config.catalog_name}: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            result = self._create_failed_result(
                self.catalog_config.catalog_name, "", None, error_msg, start_time
            )
            results.append(result)

        return results

    def process_schema_concurrently(
        self, schema_name: str, table_list: List
    ) -> List[RunResult]:
        """Override to add reconciliation-specific schema setup."""
        reconciliation_config = self.catalog_config.reconciliation_config

        # Ensure reconciliation schema exists in output catalog before processing
        self.db_ops.create_schema_if_not_exists(
            reconciliation_config.recon_outputs_catalog, schema_name
        )

        return super().process_schema_concurrently(schema_name, table_list)

    def _reconcile_table(
        self,
        schema_name: str,
        table_name: str,
    ) -> RunResult:
        """
        Reconcile a single table between source and target.

        Args:
            schema_name: Schema name
            table_name: Table name to reconcile

        Returns:
            RunResult object for the reconciliation operation
        """
        start_time = datetime.now(timezone.utc)
        reconciliation_config = self.catalog_config.reconciliation_config
        source_catalog = reconciliation_config.source_catalog
        target_catalog = self.catalog_config.catalog_name
        source_table = f"{source_catalog}.{schema_name}.{table_name}"
        target_table = f"{target_catalog}.{schema_name}.{table_name}"

        recon_table_prefix = (
            f"{reconciliation_config.recon_outputs_catalog}.{schema_name}.{table_name}"
        )

        self.logger.info(
            f"Starting reconciliation: {source_table} vs {target_table}",
            extra={"run_id": self.run_id, "operation": "reconciliation"},
        )

        try:
            # Refresh source table delta share metadata
            if not self.spark.catalog.tableExists(source_table):
                self.spark.catalog.tableExists(source_table)

            reconciliation_results = {}
            failed_checks = []

            # Use custom retry decorator with logging
            @retry_with_logging(self.retry, self.logger)
            def reconciliation_operation(query: str):
                return self.spark.sql(query)

            # Schema check
            if reconciliation_config.schema_check:
                schema_result = self._perform_schema_check(
                    source_table,
                    target_table,
                    recon_table_prefix,
                    reconciliation_operation,
                )
                reconciliation_results["schema_check"] = schema_result
                if not schema_result["passed"]:
                    failed_checks.append("schema_check")

            # Row count check
            if reconciliation_config.row_count_check:
                row_count_result = self._perform_row_count_check(
                    source_table,
                    target_table,
                    recon_table_prefix,
                    reconciliation_operation,
                )
                reconciliation_results["row_count_check"] = row_count_result
                if not row_count_result["passed"]:
                    failed_checks.append("row_count_check")

            # Missing data check
            if reconciliation_config.missing_data_check:
                missing_data_result = self._perform_missing_data_check(
                    source_table,
                    target_table,
                    recon_table_prefix,
                    reconciliation_operation,
                )
                reconciliation_results["missing_data_check"] = missing_data_result
                if not missing_data_result["passed"]:
                    failed_checks.append("missing_data_check")

            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()

            if not failed_checks:
                self.logger.info(
                    f"Reconciliation passed all checks: {source_table} vs {target_table} "
                    f"({duration:.2f}s)",
                    extra={"run_id": self.run_id, "operation": "reconciliation"},
                )

                return RunResult(
                    operation_type="reconciliation",
                    catalog_name=target_catalog,
                    schema_name=schema_name,
                    table_name=table_name,
                    status="success",
                    start_time=start_time.isoformat(),
                    end_time=end_time.isoformat(),
                    details={
                        "source_table": source_table,
                        "target_table": target_table,
                        "reconciliation_results": reconciliation_results,
                        "failed_checks": failed_checks,
                        "recon_outputs_prefix": recon_table_prefix,
                    },
                )
            else:
                error_msg = f"Reconciliation failed checks: {failed_checks}"
                self.logger.error(
                    f"Reconciliation failed: {source_table} vs {target_table} - {error_msg}",
                    extra={"run_id": self.run_id, "operation": "reconciliation"},
                )

                return RunResult(
                    operation_type="reconciliation",
                    catalog_name=target_catalog,
                    schema_name=schema_name,
                    table_name=table_name,
                    status="failed",
                    start_time=start_time.isoformat(),
                    end_time=end_time.isoformat(),
                    error_message=error_msg,
                    details={
                        "source_table": source_table,
                        "target_table": target_table,
                        "reconciliation_results": reconciliation_results,
                        "failed_checks": failed_checks,
                        "recon_outputs_prefix": recon_table_prefix,
                    },
                )

        except Exception as e:
            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()
            error_msg = f"Failed to reconcile table {source_table}: {str(e)}"

            self.logger.error(
                error_msg,
                extra={"run_id": self.run_id, "operation": "reconciliation"},
                exc_info=True,
            )

            return RunResult(
                operation_type="reconciliation",
                catalog_name=target_catalog,
                schema_name=schema_name,
                table_name=table_name,
                status="failed",
                start_time=start_time.isoformat(),
                end_time=end_time.isoformat(),
                duration_seconds=duration,
                error_message=error_msg,
                details={
                    "source_table": source_table,
                    "target_table": target_table,
                    "recon_outputs_prefix": recon_table_prefix,
                },
            )

    def _perform_schema_check(
        self,
        source_table: str,
        target_table: str,
        recon_table_prefix: str,
        reconciliation_operation,
    ) -> Dict[str, Any]:
        """Perform schema comparison between source and target tables."""
        try:
            # Create schema comparison result table
            schema_comparison_table = f"{recon_table_prefix}_schema_comparison"
            source_catalog = source_table.split(".")[0]
            target_catalog = target_table.split(".")[0]
            schema_query = f"""
            CREATE OR REPLACE TABLE {schema_comparison_table} AS
            WITH source_schema AS (
                SELECT 
                    '{source_table}' as table_name,
                    'source' as table_type,
                    column_name,
                    data_type,
                    is_nullable,
                    column_default,
                    ordinal_position
                FROM {source_catalog}.information_schema.columns 
                WHERE table_name = split('{source_table}', '\\.')[2]
                  AND table_schema = split('{source_table}', '\\.')[1]
                  AND table_catalog = split('{source_table}', '\\.')[0]
            ),
            target_schema AS (
                SELECT 
                    '{target_table}' as table_name,
                    'target' as table_type,
                    column_name,
                    data_type,
                    is_nullable,
                    column_default,
                    ordinal_position
                FROM {target_catalog}.information_schema.columns 
                WHERE table_name = split('{target_table}', '\\.')[2]
                  AND table_schema = split('{target_table}', '\\.')[1]
                  AND table_catalog = split('{target_table}', '\\.')[0]
            ),
            schema_diff AS (
                SELECT 
                    COALESCE(s.column_name, t.column_name) as column_name,
                    s.data_type as source_data_type,
                    t.data_type as target_data_type,
                    s.is_nullable as source_nullable,
                    t.is_nullable as target_nullable,
                    CASE 
                        WHEN s.column_name IS NULL THEN 'missing_in_source'
                        WHEN t.column_name IS NULL THEN 'missing_in_target'
                        WHEN s.data_type != t.data_type THEN 'type_mismatch'
                        WHEN s.is_nullable != t.is_nullable THEN 'nullable_mismatch'
                        ELSE 'match'
                    END as comparison_result
                FROM source_schema s
                FULL OUTER JOIN target_schema t ON s.column_name = t.column_name
            )
            SELECT 
                column_name,
                source_data_type,
                target_data_type,
                source_nullable,
                target_nullable,
                comparison_result,
                current_timestamp() as check_timestamp
            FROM schema_diff
            ORDER BY column_name
            """

            result, last_exception, attempt, max_attempts = reconciliation_operation(
                schema_query
            )

            if not result:
                return {
                    "passed": False,
                    "error": f"Schema check query failed: {last_exception}",
                    "output_table": schema_comparison_table,
                }

            # Check if there are any mismatches
            mismatch_count_df = self.spark.sql(f"""
                SELECT COUNT(*) as mismatch_count 
                FROM {schema_comparison_table}
                WHERE comparison_result != 'match'
            """)

            mismatch_count = mismatch_count_df.collect()[0]["mismatch_count"]

            return {
                "passed": mismatch_count == 0,
                "mismatch_count": mismatch_count,
                "output_table": schema_comparison_table,
                "details": "Schema check completed successfully",
            }

        except Exception as e:
            return {
                "passed": False,
                "error": f"Schema check failed: {str(e)}",
                "output_table": schema_comparison_table,
            }

    def _perform_row_count_check(
        self,
        source_table: str,
        target_table: str,
        recon_table_prefix: str,
        reconciliation_operation,
    ) -> Dict[str, Any]:
        """Perform row count comparison between source and target tables."""
        try:
            # Create row count comparison result table
            row_count_table = f"{recon_table_prefix}_row_count_comparison"

            row_count_query = f"""
            CREATE OR REPLACE TABLE {row_count_table} AS
            WITH source_count AS (
                SELECT COUNT(*) as source_row_count FROM {source_table}
            ),
            target_count AS (
                SELECT COUNT(*) as target_row_count FROM {target_table}
            )
            SELECT 
                '{source_table}' as source_table,
                '{target_table}' as target_table,
                source_row_count,
                target_row_count,
                target_row_count - source_row_count as row_count_diff,
                CASE 
                    WHEN source_row_count = target_row_count THEN 'match'
                    ELSE 'mismatch'
                END as comparison_result,
                current_timestamp() as check_timestamp
            FROM source_count, target_count
            """

            result, last_exception, attempt, max_attempts = reconciliation_operation(
                row_count_query
            )

            if not result:
                return {
                    "passed": False,
                    "error": f"Row count check query failed: {last_exception}",
                    "output_table": row_count_table,
                }

            # Get the comparison result
            comparison_df = self.spark.sql(f"SELECT * FROM {row_count_table}")
            comparison_result = comparison_df.collect()[0]

            return {
                "passed": comparison_result["comparison_result"] == "match",
                "source_count": comparison_result["source_row_count"],
                "target_count": comparison_result["target_row_count"],
                "difference": comparison_result["row_count_diff"],
                "output_table": row_count_table,
                "details": "Row count check completed successfully",
            }

        except Exception as e:
            return {
                "passed": False,
                "error": f"Row count check failed: {str(e)}",
                "output_table": row_count_table,
            }

    def _perform_missing_data_check(
        self,
        source_table: str,
        target_table: str,
        recon_table_prefix: str,
        reconciliation_operation,
    ) -> Dict[str, Any]:
        """Perform missing data check between source and target tables."""
        try:
            # Create missing data comparison result table
            missing_data_table = f"{recon_table_prefix}_missing_data_comparison"

            # Get common columns between source and target
            common_fields = self.db_ops.get_common_fields(source_table, target_table)

            if not common_fields:
                return {
                    "passed": False,
                    "error": "No common fields found between source and target tables",
                    "output_table": missing_data_table,
                }

            # Create a hash-based comparison for data content
            field_list = "`" + "`,`".join(common_fields) + "`"

            missing_data_query = f"""
            CREATE OR REPLACE TABLE {missing_data_table} AS
            WITH source_hashes AS (
                SELECT 
                    hash({field_list}) as row_hash,
                    'source' as table_type,
                    {field_list}
                FROM {source_table}
            ),
            target_hashes AS (
                SELECT 
                    hash({field_list}) as row_hash,
                    'target' as table_type,
                    {field_list}
                FROM {target_table}
            ),
            missing_in_target AS (
                SELECT 
                    'missing_in_target' as issue_type,
                    COUNT(*) as row_count
                FROM source_hashes s
                LEFT JOIN target_hashes t ON s.row_hash = t.row_hash
                WHERE t.row_hash IS NULL
            ),
            missing_in_source AS (
                SELECT 
                    'missing_in_source' as issue_type,
                    COUNT(*) as row_count
                FROM target_hashes t
                LEFT JOIN source_hashes s ON t.row_hash = s.row_hash
                WHERE s.row_hash IS NULL
            )
            SELECT 
                issue_type,
                row_count,
                current_timestamp() as check_timestamp
            FROM missing_in_target
            UNION ALL
            SELECT 
                issue_type,
                row_count,
                current_timestamp() as check_timestamp
            FROM missing_in_source
            """

            result, last_exception, attempt, max_attempts = reconciliation_operation(
                missing_data_query
            )

            if not result:
                return {
                    "passed": False,
                    "error": f"Missing data check query failed: {last_exception}",
                    "output_table": missing_data_table,
                }

            # Get the comparison results
            comparison_df = self.spark.sql(f"SELECT * FROM {missing_data_table}")
            comparison_results = comparison_df.collect()

            total_missing = sum(row["row_count"] for row in comparison_results)

            missing_breakdown = {
                row["issue_type"]: row["row_count"] for row in comparison_results
            }

            return {
                "passed": total_missing == 0,
                "total_missing_rows": total_missing,
                "missing_breakdown": missing_breakdown,
                "common_fields_count": len(common_fields),
                "output_table": missing_data_table,
                "details": "Missing data check completed successfully",
            }

        except Exception as e:
            return {
                "passed": False,
                "error": f"Missing data check failed: {str(e)}",
                "output_table": missing_data_table,
            }
