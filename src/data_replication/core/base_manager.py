"""
Base manager class with common functionality for data replication operations.

This module provides shared functionality that can be reused across different
operation types like backup, replication, and reconciliation.
"""

import uuid
from datetime import datetime, timezone
from typing import List, Optional
import json

from databricks.connect import DatabricksSession
from data_replication.databricks_operations import DatabricksOperations
from data_replication.audit.logger import DataReplicationLogger
from data_replication.audit.audit_logger import AuditLogger
from data_replication.config.models import (
    ReplicationSystemConfig,
    RunResult,
    RunSummary,
)


class BaseManager:
    """Base manager class with common functionality for data replication operations."""

    def __init__(
        self,
        config: ReplicationSystemConfig,
        spark: DatabricksSession,
        logging_spark: DatabricksSession,
        logger: DataReplicationLogger,
        run_id: Optional[str] = None,
    ):
        """
        Initialize the base manager.

        Args:
            config: System configuration
            spark: Spark session for database operations
            logger: Logger instance
            run_id: Optional run identifier
        """
        self.config = config
        self.spark = spark
        self.logging_spark = logging_spark
        self.logger = logger
        self.run_id = run_id or str(uuid.uuid4())
        self.db_ops = DatabricksOperations(spark)
        self.audit_logger = AuditLogger(
            logging_spark,
            config,
            logger,
            audit_table=config.audit_config.audit_table,
            run_id=self.run_id
        )

    def log_run_result(self, result: RunResult) -> None:
        """
        Log a RunResult to the configured audit table.

        Args:
            result: RunResult object to log
        """
        try:
            # Log to each configured audit table
            details_str = None
            if result.details:
                details_str = json.dumps(result.details)

            # Parse string timestamps back to datetime objects
            start_dt = datetime.fromisoformat(result.start_time.replace("Z", "+00:00"))
            end_dt = datetime.fromisoformat(result.end_time.replace("Z", "+00:00"))
            duration = (end_dt - start_dt).total_seconds()

            self.audit_logger.log_operation(
                operation_type=result.operation_type,
                catalog_name=result.catalog_name,
                schema_name=result.schema_name or "",
                table_name=result.table_name or "",
                status=result.status,
                start_time=start_dt,
                end_time=end_dt,
                duration_seconds=duration,
                error_message=result.error_message,
                details=details_str,
                attempt_number=result.attempt_number or 1,
                max_attempts=result.max_attempts or 1,
            )
        except Exception as e:
            self.logger.error(f"Failed to log run result: {str(e)}", exc_info=True)

    def log_run_results(self, results: List[RunResult]) -> None:
        """
        Log multiple RunResult objects to all configured audit tables.

        Args:
            results: List of RunResult objects to log
        """
        for result in results:
            self.log_run_result(result)

    def create_summary(
        self,
        start_time: datetime,
        results: List[RunResult],
        operation_type: str = "operation",
        error_message: Optional[str] = None,
    ) -> RunSummary:
        """
        Create a run summary object.

        Args:
            start_time: Operation start time
            results: List of operation results
            operation_type: Type of operation (backup, replication, etc.)
            error_message: Optional error message
        """
        # Suppress unused argument warning for error_message
        _ = error_message

        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()

        # Calculate summary statistics
        successful_operations = sum(1 for r in results if r.status == "success")
        failed_operations = sum(1 for r in results if r.status == "failed")
        total_operations = len(results)

        status = "completed" if failed_operations == 0 else "completed_with_failures"

        # Count unique catalogs, schemas, and tables
        catalogs = set(r.catalog_name for r in results if r.catalog_name)
        schemas = set(
            f"{r.catalog_name}.{r.schema_name}" for r in results if r.schema_name
        )
        tables = set(
            f"{r.catalog_name}.{r.schema_name}.{r.table_name}"
            for r in results
            if r.table_name
        )

        success_rate = (
            (successful_operations / total_operations * 100)
            if total_operations > 0
            else 0
        )
        summary_text = (
            f"{operation_type.title()} operation completed in {duration:.1f}s. "
            f"Processed {len(catalogs)} catalogs, {len(schemas)} schemas, "
            f"{len(tables)} tables. Success rate: {successful_operations}/"
            f"{total_operations} ({success_rate:.1f}%)"
        )

        return RunSummary(
            run_id=self.run_id,
            start_time=start_time.isoformat(),
            end_time=end_time.isoformat(),
            duration=duration,
            status=status,
            total_catalogs=len(catalogs),
            total_schemas=len(schemas),
            total_tables=len(tables),
            successful_operations=successful_operations,
            failed_operations=failed_operations,
            summary=summary_text,
        )

    def log_run_summary(
        self, summary: RunSummary, operation_type: str = "operation"
    ) -> None:
        """
        Log run summary to audit tables.

        Args:
            summary: Run summary to log
            operation_type: Type of operation for audit logging
        """
        try:
            # Log run summary to audit table
            # Parse string timestamps back to datetime objects
            start_dt = datetime.fromisoformat(summary.start_time.replace("Z", "+00:00"))
            end_dt = datetime.fromisoformat(summary.end_time.replace("Z", "+00:00"))

            self.audit_logger.log_operation(
                operation_type=f"{operation_type}_run_summary",
                catalog_name="ALL",
                schema_name="",
                table_name="",
                status=summary.status,
                start_time=start_dt,
                end_time=end_dt,
                duration_seconds=summary.duration,
                error_message=None,
                details=summary.summary,
                attempt_number=1,
                max_attempts=1,
            )
        except Exception as e:
            self.logger.error(f"Failed to log run result: {str(e)}", exc_info=True)
