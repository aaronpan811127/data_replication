"""
Configuration models for the data replication system using Pydantic.

This module defines all the configuration models that validate and parse
the YAML configuration file for the data replication system.
"""

from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class TableType(str, Enum):
    """Enumeration of supported table types."""

    DELTA = "delta"
    DELTA_LIVE_TABLE = "delta_live_table"


class SecretConfig(BaseModel):
    """Configuration for Databricks secrets."""

    secret_scope: str
    secret_key: str


class AuditConfig(BaseModel):
    """Configuration for audit tables"""

    audit_table: Optional[str] = None


class DatabricksConnectConfig(BaseModel):
    """Configuration for Databricks Connect."""

    name: str
    host: str
    token: SecretConfig


class TableConfig(BaseModel):
    """Configuration for individual tables."""

    table_name: str


class SchemaConfig(BaseModel):
    """Configuration for individual schemas."""

    schema_name: str
    tables: Optional[List[TableConfig]] = None
    exclude_tables: Optional[List[TableConfig]] = None

class BackupConfig(BaseModel):
    """Configuration for backup operations."""

    enabled: bool = True
    source_catalog: Optional[str] = None
    backup_catalog: Optional[str] = '__replication_internal_aaron_to_aws'

    # @model_validator(mode="after")
    # def validate_backup_config(self):
    #     """Validate required fields when backup is enabled."""
    #     if self.enabled:
    #         required_fields = ["source_catalog", "backup_catalog"]
    #         missing_fields = [
    #             field for field in required_fields if getattr(self, field) is None
    #         ]

    #         if missing_fields:
    #             raise ValueError(
    #                 f"When backup is enabled, the following fields are required: "
    #                 f"{missing_fields}"
    #             )
    #     return self


class DeltaShareConfig(BaseModel):
    """Configuration for Delta Share operations."""

    enabled: bool = True
    recipient_id: Optional[str] = None
    shared_catalog: Optional[str] = '__replication_internal_aaron_to_aws'
    share_name: Optional[str] = '__replication_internal_aaron_to_aws'
    shared_catalog_name: Optional[str] = '__replication_internal_aaron_from_azure'

    @model_validator(mode="after")
    def validate_deltashare_config(self):
        """Validate required fields when delta share is enabled."""
        if self.enabled:
            required_fields = [
                "recipient_id"
            ]
            missing_fields = [
                field for field in required_fields if getattr(self, field) is None
            ]

            if missing_fields:
                raise ValueError(
                    f"When delta share is enabled, the following fields are "
                    f"required: {missing_fields}"
                )
        return self


class ReplicationConfig(BaseModel):
    """Configuration for replication operations."""

    enabled: bool = True
    source_catalog: Optional[str] = '__replication_internal_aaron_from_azure'
    intermediate_catalog: Optional[str] = None
    enforce_schema: Optional[bool] = True

    # @model_validator(mode="after")
    # def validate_replication_config(self):
    #     """Validate required fields when replication is enabled."""
    #     if self.enabled:
    #         required_fields = [
    #             "source_catalog",
    #         ]
    #         missing_fields = [
    #             field for field in required_fields if getattr(self, field) is None
    #         ]

    #         if missing_fields:
    #             raise ValueError(
    #                 f"When replication is enabled, the following fields are "
    #                 f"required: {missing_fields}"
    #             )
    #     return self


class ReconciliationConfig(BaseModel):
    """Configuration for reconciliation operations."""

    enabled: bool = True
    delta_share_config: Optional[DeltaShareConfig] = None
    source_catalog: Optional[str] = 'aaron_azure_shared'
    recon_outputs_catalog: Optional[str] = 'aaron_azure_shared_recon_results'
    schema_check: Optional[bool] = True
    row_count_check: Optional[bool] = True
    missing_data_check: Optional[bool] = True

    @model_validator(mode="after")
    def validate_reconciliation_config(self):
        """Validate required fields when reconciliation is enabled."""
        if self.enabled:
            required_fields = [
                "recon_outputs_catalog",
            ]
            missing_fields = [
                field for field in required_fields if getattr(self, field) is None
            ]

            if missing_fields:
                raise ValueError(
                    f"When reconciliation is enabled, the following fields are "
                    f"required: {missing_fields}"
                )
        return self


class TargetCatalogConfig(BaseModel):
    """Configuration for target catalogs."""

    catalog_name: str
    schema_filter_expression: Optional[str] = None
    backup_config: Optional[BackupConfig] = None
    delta_share_config: Optional[DeltaShareConfig] = None
    replication_config: Optional[ReplicationConfig] = None
    reconciliation_config: Optional[ReconciliationConfig] = None
    target_schemas: Optional[List[SchemaConfig]] = None

    @model_validator(mode="after")
    def validate_catalog_config(self):
        """
        Validate that at least one of schema_filter_expression or target_schemas
        is provided.
        """
        if not self.schema_filter_expression and not self.target_schemas:
            raise ValueError(
                "At least one of 'schema_filter_expression' or 'target_schemas' "
                "must be provided"
            )
        return self


class ConcurrencyConfig(BaseModel):
    """Configuration for concurrency settings."""

    max_workers: int = Field(default=4, ge=1, le=32)
    timeout_seconds: int = Field(default=3600, ge=60)


class LoggingConfig(BaseModel):
    """Configuration for logging settings."""

    level: str = Field(default="INFO")
    format: str = Field(default="json")  # "text" or "json"
    log_to_file: bool = Field(default=False)
    log_file_path: Optional[str] = None

    @field_validator("level")
    @classmethod
    def validate_level(cls, v):
        """Validate logging level."""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in valid_levels:
            raise ValueError(
                f"Invalid logging level: {v}. Must be one of {valid_levels}"
            )
        return v.upper()

    @field_validator("format")
    @classmethod
    def validate_format(cls, v):
        """Validate logging format."""
        valid_formats = ["text", "json"]
        if v.lower() not in valid_formats:
            raise ValueError(
                f"Invalid logging format: {v}. Must be one of {valid_formats}"
            )
        return v.lower()


class RetryConfig(BaseModel):
    """Configuration for retry settings."""

    max_attempts: int = Field(default=3, ge=1, le=10)
    retry_delay_seconds: int = Field(default=5, ge=1)


class ReplicationSystemConfig(BaseModel):
    """Root configuration model for the replication system."""

    version: str
    replication_group: str
    source_databricks_connect_config: DatabricksConnectConfig
    target_databricks_connect_config: DatabricksConnectConfig
    audit_config: AuditConfig
    target_catalogs: List[TargetCatalogConfig]
    concurrency: Optional[ConcurrencyConfig] = Field(default_factory=ConcurrencyConfig)
    retry: Optional[RetryConfig] = Field(default_factory=RetryConfig)
    logging: Optional[LoggingConfig] = Field(default_factory=LoggingConfig)

    @field_validator("version")
    @classmethod
    def validate_version(cls, v):
        """Validate configuration version."""
        if v != "1.0":
            raise ValueError(f"Unsupported configuration version: {v}")
        return v

    @field_validator("target_catalogs")
    @classmethod
    def validate_target_catalogs(cls, v):
        """Ensure at least one target catalog is configured."""
        if not v:
            raise ValueError("At least one target catalog must be configured")
        return v

class AuditLogEntry(BaseModel):
    """Model for audit log entries."""

    run_id: str
    timestamp: str
    operation_type: str  # backup, delta_share, replication, reconciliation
    catalog_name: str
    schema_name: Optional[str] = None
    table_name: Optional[str] = None
    status: str  # started, completed, failed
    details: Optional[str] = None
    error_message: Optional[str] = None
    duration_seconds: Optional[float] = None
    attempt_number: Optional[int] = None
    max_attempts: Optional[int] = None


class RunSummary(BaseModel):
    """Model for run summary logging."""

    run_id: str
    start_time: str
    end_time: Optional[str] = None
    duration: Optional[float] = None
    status: str  # started, completed, failed
    total_catalogs: int
    total_schemas: int
    total_tables: int
    successful_operations: int = 0
    failed_operations: int = 0
    summary: Optional[str] = None


class RunResult(BaseModel):
    """Model for operation run results."""

    operation_type: str
    catalog_name: str
    schema_name: Optional[str] = None
    table_name: Optional[str] = None
    status: str  # success, failed
    start_time: str
    end_time: str
    error_message: Optional[str] = None
    details: Optional[dict] = None
    attempt_number: Optional[int] = None
    max_attempts: Optional[int] = None
