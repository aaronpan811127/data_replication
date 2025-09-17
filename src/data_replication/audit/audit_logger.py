"""
Audit logger for data replication operations.

This module provides audit logging functionality that can be used across
different components to log operations to audit tables.
"""

from datetime import datetime, timezone
from typing import Optional

from databricks.connect import DatabricksSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DoubleType,
    IntegerType,
)
from data_replication.databricks_operations import DatabricksOperations
from data_replication.audit.logger import DataReplicationLogger


class AuditLogger:
    """Audit logger for logging operations to database tables."""

    def __init__(
        self,
        spark: DatabricksSession,
        db_ops: DatabricksOperations,
        logger: DataReplicationLogger,
        run_id: str,
        audit_table: str,
    ):
        """
        Initialize the audit logger and create audit table.

        Args:
            spark: Spark session for database operations
            logger: Logger instance for standard logging
            run_id: Unique run identifier
            audit_table: Full audit table name (catalog.schema.table)
        """
        self.spark = spark
        self.db_ops = db_ops
        self.logger = logger
        self.run_id = run_id
        self.audit_table = audit_table

        # Create audit table during instantiation
        self.create_audit_table()

    def create_audit_table(self) -> None:
        """
        Create audit log table for operations.

        Args:
            audit_table: Full audit table name (catalog.schema.table)
        """

        # Create audit catalog and schema
        audit_parts = self.audit_table.split(".")
        if len(audit_parts) >= 2:
            audit_catalog = audit_parts[0]
            audit_schema = audit_parts[1]

            self.db_ops.create_catalog_if_not_exists(audit_catalog)
            self.db_ops.create_schema_if_not_exists(audit_catalog, audit_schema)
        else:
            # Fallback to standard logging if audit table logging fails
            self.logger.warning(f"Invalid audit table name: {self.audit_table}. ")
            raise ValueError(
                f"Invalid audit table name: {self.audit_table}. "
                "Expected format: catalog.schema.table"
            )

        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.audit_table} (
            run_id STRING,
            logging_time TIMESTAMP,
            operation_type STRING,
            catalog_name STRING,
            schema_name STRING,
            table_name STRING,
            status STRING,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            duration_seconds DOUBLE,
            error_message STRING,
            details STRING,
            attempt_number INT,
            max_attempts INT
        ) USING DELTA
        """
        self.spark.sql(create_table_sql)
        self.logger.debug(f"Created/verified audit table: {self.audit_table}")

    def log_operation(
        self,
        operation_type: str,
        catalog_name: str,
        schema_name: str,
        table_name: str,
        status: str,
        start_time: datetime,
        end_time: datetime,
        duration_seconds: float,
        error_message: Optional[str] = None,
        details: Optional[str] = None,
        attempt_number: int = 1,
        max_attempts: int = 1,
    ) -> None:
        """
        Log operation to audit table.

        Args:
            operation_type: Type of operation (backup, restore, etc.)
            catalog_name: Catalog name
            schema_name: Schema name
            table_name: Table name
            status: Operation status
            start_time: Operation start time
            end_time: Operation end time
            duration_seconds: Operation duration in seconds
            error_message: Error message if failed
            details: Additional details about the operation
            attempt_number: Current attempt number
            max_attempts: Total number of attempts
        """
        audit_data = [
            (
                self.run_id,
                datetime.now(timezone.utc),
                operation_type,
                catalog_name,
                schema_name,
                table_name,
                status,
                start_time,
                end_time,
                duration_seconds,
                error_message,
                details,
                attempt_number,
                max_attempts,
            )
        ]

        # Define schema explicitly to avoid type inference issues
        schema = StructType(
            [
                StructField("run_id", StringType(), True),
                StructField("logging_time", TimestampType(), True),
                StructField("operation_type", StringType(), True),
                StructField("catalog_name", StringType(), True),
                StructField("schema_name", StringType(), True),
                StructField("table_name", StringType(), True),
                StructField("status", StringType(), True),
                StructField("start_time", TimestampType(), True),
                StructField("end_time", TimestampType(), True),
                StructField("duration_seconds", DoubleType(), True),
                StructField("error_message", StringType(), True),
                StructField("details", StringType(), True),
                StructField("attempt_number", IntegerType(), True),
                StructField("max_attempts", IntegerType(), True),
            ]
        )

        try:
            audit_df = self.spark.createDataFrame(audit_data, schema)
            audit_df.write.mode("append").saveAsTable(self.audit_table)
        except Exception as e:
            # Fallback to standard logging if audit table logging fails
            self.logger.warning(f"Failed to log to audit table {self.audit_table}: {str(e)}")
