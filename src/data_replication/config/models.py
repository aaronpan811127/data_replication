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

    MANAGED = "managed"
    STREAMING_TABLE = "streaming_table"
    EXTERNAL = "external"
    VIEW = "view"
    MATERIALIZED_VIEW = "materialized_view"


class ExecuteAt(str, Enum):
    """Enumeration of execution locations for operations."""

    SOURCE = "source"
    TARGET = "target"
    EXTERNAL = "external"


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
    host: Optional[str]
    token: Optional[SecretConfig]


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
    backup_catalog: Optional[str] = None


# Delta Share support will be enabled in a future release
# class DeltaShareConfig(BaseModel):
#     """Configuration for Delta Share operations."""

#     enabled: bool = True
#     recipient_id: Optional[str] = None
#     shared_catalog: Optional[str] = '__replication_internal_aaron_to_aws'
#     share_name: Optional[str] = '__replication_internal_aaron_to_aws'
#     shared_catalog_name: Optional[str] = '__replication_internal_aaron_from_azure'

#     @model_validator(mode="after")
#     def validate_deltashare_config(self):
#         """Validate required fields when delta share is enabled."""
#         if self.enabled:
#             required_fields = [
#                 "recipient_id"
#             ]
#             missing_fields = [
#                 field for field in required_fields if getattr(self, field) is None
#             ]

#             if missing_fields:
#                 raise ValueError(
#                     f"When delta share is enabled, the following fields are "
#                     f"required: {missing_fields}"
#                 )
#         return self


class ReplicationConfig(BaseModel):
    """Configuration for replication operations."""

    enabled: bool = True
    source_catalog: Optional[str] = None
    intermediate_catalog: Optional[str] = None
    enforce_schema: Optional[bool] = True


class ReconciliationConfig(BaseModel):
    """Configuration for reconciliation operations."""

    enabled: bool = True
    # delta_share_config: Optional[DeltaShareConfig] = None
    source_catalog: str
    recon_outputs_catalog: str
    schema_check: Optional[bool] = True
    row_count_check: Optional[bool] = True
    missing_data_check: Optional[bool] = True

    @model_validator(mode="after")
    def validate_reconciliation_config(self):
        """Validate required fields when reconciliation is enabled."""
        if self.enabled:
            required_fields = [
                "source_catalog",
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
    table_types: List[TableType] = Field(
        default_factory=lambda: [TableType.MANAGED, TableType.STREAMING_TABLE]
    )
    schema_filter_expression: Optional[str] = None
    backup_config: Optional[BackupConfig] = None
    # delta_share_config: Optional[DeltaShareConfig] = None
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
    execute_at: Optional[ExecuteAt] = Field(default=ExecuteAt.TARGET)

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

    @model_validator(mode="after")
    def derive_default_catalogs(self):
        """
        Derive default catalogs when not provided in the config.
        - Backup catalogs: __replication_internal_{catalog_name}_to_{target_databricks_connect_config.name}
        - Replication source catalogs: __replication_internal_{catalog_name}_from_{source_databricks_connect_config.name}
        """
        target_name = self.target_databricks_connect_config.name
        source_name = self.source_databricks_connect_config.name

        for catalog in self.target_catalogs:
            # Derive default backup catalogs
            if catalog.backup_config and catalog.backup_config.enabled:
                if catalog.backup_config.backup_catalog is None:
                    default_backup_catalog = f"__replication_internal_{catalog.catalog_name}_to_{target_name}"
                    catalog.backup_config.backup_catalog = default_backup_catalog
                # Derive default backup source catalogs
                if catalog.backup_config.source_catalog is None:
                    catalog.backup_config.source_catalog = catalog.catalog_name

            # Derive default replication source catalogs
            if catalog.replication_config and catalog.replication_config.enabled:
                if catalog.replication_config.source_catalog is None:
                    default_source_catalog = f"__replication_internal_{catalog.catalog_name}_from_{source_name}"
                    catalog.replication_config.source_catalog = default_source_catalog

        return self


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
    config_details: Optional[str] = None  # JSON string of the full configuration
    execution_user: Optional[str] = None  # User who executed the operation


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
