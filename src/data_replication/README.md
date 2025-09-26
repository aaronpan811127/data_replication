# Databricks Data Replication System

A comprehensive system for replicating, backing up, and reconciling Databricks tables across different metastores using delta sharing.

## Overview

The Data Replication System provides enterprise-grade capabilities for:

- **Backup**: Deep clone tables from source tables to backup catalogs
- **Delta Sharing**: Set up cross-workspace data sharing with recipients and shares - remove dependency on Terraform
- **Replication**: Deep clone tables across metastores with schema enforcement
- **Reconciliation**: Validate data consistency with row counts, schema checks, and missing data detection

## Key Features

- ✅ **DLT Streaming Table and Managed Delta Table Support**: Automatically identifies and handles DLT Streaming tables
- ✅ **Unified Orchestration**: End-to-end orchestration of replication and reconciliation
- ✅ **Comprehensive Logging**: Centralized structured logging with audit trails to Databricks tables
- ✅ **Error Handling**: Built in retry logic
- ✅ **Concurrency**: Parallel execution with configurable worker pools
- ✅ **Modular Design**: Flexibility to run single operation
- ✅ **Extensible Architecture**: Plugin-based provider system for future enhancements
- ✅ **Configuration-Driven**: YAML-based configuration with Pydantic validation
- ✅ **Reduce dependency on Terraform**: catalog, schema and delta share automatically setup


## Architecture

```text
src/data_replication/
├── config/              # Configuration models and YAML loading
│   ├── models.py        # Pydantic configuration models
│   └── loader.py        # Configuration file loading utilities
├── core/                # Base interfaces and abstractions
│   ├── base_manager.py  # Abstract base classes for managers
│   └── exceptions.py    # Custom exception classes
├── audit/               # Centralized logging and audit system
│   ├── logger.py        # Basic logging utilities
│   └── audit_logger.py  # Structured audit logging to Databricks
├── backup/              # Deep clone backup operations
│   ├── backup_manager.py    # Backup orchestration logic
│   └── backup_provider.py   # Core backup implementation
├── replication/         # Cross-metastore table replication
│   ├── replication_manager.py   # Replication orchestration
│   └── replication_provider.py  # Core replication implementation
├── reconciliation/      # Data validation and consistency checks
│   ├── reconciliation_manager.py   # Reconciliation orchestration
│   └── reconciliation_provider.py  # Core reconciliation implementation
├── cli/                 # Command-line interface components
│   ├── __main__.py      # CLI entry point
│   └── backup_cli.py    # Backup-specific CLI commands
├── main.py              # Main orchestrator and CLI coordination
├── databricks_operations.py  # Databricks API and Spark operations
└── utils.py             # DLT utilities, retry logic, validation helpers
```

## Installation

```bash
# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install development dependencies
pip install -r requirements-dev.txt

# Install the package in development mode
pip install -e .
```

## Configuration

Create a `config.yaml` file with your settings:

```yaml
version: "1.0"

# Source Databricks workspace (Azure)
source_databricks_connect_config:
  host: "https://adb-984752964297111.11.azuredatabricks.net"
  token:
    secret_scope: "test_kr"
    secret_key: "pat_source"

# Target Databricks workspace (AWS)
target_databricks_connect_config:
  host: "https://e2-demo-field-eng.cloud.databricks.com"
  token:
    secret_scope: "test_kr"
    secret_key: "pat_target"

# Centralized audit logging
audit_config:
  audit_table: "aaron.audit.audit_loggging"

# Target catalogs and their configurations
target_catalogs:
  - catalog_name: "aaron"
    schema_filter_expression: "databaseName like 'bronze_%'"
    
    # Backup configuration - deep clone to DR catalog
    backup_config:
      enabled: true
      source_catalog: "aaron"
      backup_catalog: "aaron_dr"
    
    # Replication configuration - cross-metastore replication
    replication_config:
      enabled: true
      source_catalog: "aaron_dr_azure_shared"
      intermediate_catalog: "aaron_dr"
      enforce_schema: true
    
    # Reconciliation configuration - data validation
    reconciliation_config:
      enabled: true
      source_catalog: "aaron_azure_shared"
      recon_outputs_catalog: "recon_results"
      schema_check: true
      row_count_check: true
      missing_data_check: true
    
    # Target schemas and tables
    target_schemas:
      - schema_name: "bronze_1"
        tables:
          - table_name: "table_1"
          - table_name: "table_2"
          - table_name: "sample_trips_ldp"
          - table_name: "delta_table_1"
          - table_name: "mat_view1"
        exclude_tables:
          - table_name: "table_3"

# Concurrency settings
concurrency:
  max_workers: 2
  timeout_seconds: 1800

# Retry settings
retry:
  max_attempts: 2
  retry_delay_seconds: 3
```

## Usage


```bash
# Run all enabled operations
python -m data_replication configs/config.yaml

# Run specific operation only
python -m data_replication configs/config.yaml --operation backup
python -m data_replication configs/config.yaml --operation replication
python -m data_replication configs/config.yaml --operation reconciliation

# Validate configuration only
python -m data_replication configs/config.yaml --validate-only

# Dry run to see what would be executed
python -m data_replication configs/config.yaml --dry-run

# Combine dry run with specific operation
python -m data_replication configs/config.yaml --operation backup --dry-run
```

### Programmatic Usage

```python
from data_replication.config.loader import ConfigLoader
from data_replication.backup.backup_manager import BackupManager
from data_replication.replication.replication_manager import ReplicationManager
from data_replication.reconciliation.reconciliation_manager import ReconciliationManager
from data_replication.utils import create_spark_session
from databricks.sdk import WorkspaceClient

# Load configuration
config = ConfigLoader.load_config("configs/config.yaml")

# Create Spark session and workspace clients
spark = create_spark_session(config.source_databricks_connect_config)
source_client = WorkspaceClient(
    host=config.source_databricks_connect_config.host,
    token=config.source_databricks_connect_config.token.get_secret_value()
)
target_client = WorkspaceClient(
    host=config.target_databricks_connect_config.host,
    token=config.target_databricks_connect_config.token.get_secret_value()
)

try:
    # Run backup operations
    if any(cat.backup_config.enabled for cat in config.target_catalogs):
        backup_manager = BackupManager(spark, source_client, config)
        backup_manager.run_backup_operations()
    
    # Run replication operations
    if any(cat.replication_config.enabled for cat in config.target_catalogs):
        replication_manager = ReplicationManager(spark, source_client, target_client, config)
        replication_manager.run_replication_operations()
    
    # Run reconciliation operations
    if any(cat.reconciliation_config.enabled for cat in config.target_catalogs):
        reconciliation_manager = ReconciliationManager(spark, source_client, target_client, config)
        reconciliation_manager.run_reconciliation_operations()
        
finally:
    spark.stop()
```

## DLT Table Handling

The system automatically handles DLT (Delta Live Tables) complexities:

1. **Pipeline ID Extraction**: Uses `DESCRIBE DETAIL` to extract pipeline information from DLT tables
2. **Internal Table Identification**: Constructs internal table paths following the pattern:
   - Pipeline schema: `__dlt_materialization_schema_<pipeline_id_with_underscores>`  
   - Internal table: `__materialization_mat_<pipeline_id_with_underscores>_<table_name>_1`
3. **Deep Clone Operations**: Performs operations on internal tables rather than DLT tables directly
4. **Streaming Tables and Materialized Views**: Automatically detects and handles both DLT table types

## Audit and Logging

All operations are logged with comprehensive audit trails:

- **Centralized Logging**: All operations log to a single audit table specified in configuration
- **Structured Data**: JSON format with correlation IDs, timestamps, and operation metadata
- **Run Tracking**: Unique run IDs allow tracking operations across multiple components
- **Error Details**: Full error information including stack traces for troubleshooting
- **Operation Results**: Success/failure status and detailed results for each table operation

## Error Handling and Retry

The system includes robust error handling:

- **Built-in Retry Logic**: Configurable retry attempts with delay settings
- **Graceful Degradation**: Operations continue even if individual tables fail
- **Detailed Error Reporting**: Comprehensive error messages with context
- **Operation Isolation**: Failures in one operation don't affect others
- **Audit Trail**: All errors are logged to audit tables for analysis

## Development Status

✅ **Completed Components:**

- Configuration system with Pydantic validation and YAML loading
- Databricks workspace client integration with token-based authentication
- DLT table utilities for pipeline ID extraction and internal table handling
- Backup operations using deep clone from source to backup catalogs
- Cross-metastore replication with schema enforcement and intermediate staging
- Data reconciliation with row count, schema, and missing data validation
- Centralized audit logging system with structured data storage
- CLI interface with operation selection, validation, and dry-run capabilities
- Manager-provider architecture for extensible operation handling
- Comprehensive error handling with retry logic and graceful degradation

🚧 **Future Enhancements:**

- Integration test suite with real Databricks workspace connections
- Performance optimizations for large dataset operations
- Advanced concurrency patterns and parallel execution improvements
- Real-time monitoring and alerting capabilities
- Additional data source and target connector implementations
- Delta Sharing integration for cross-cloud data sharing

## Testing

```bash
# Run all tests with coverage
pytest

# Run specific test types
pytest -m unit      # Unit tests only
pytest -m integration  # Integration tests only
pytest -m slow      # Slow running tests

# Run tests with detailed coverage report
pytest --cov=data_replication --cov-report=html --cov-report=term-missing
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Implement changes with tests
4. Submit a pull request

## License

Apache License 2.0

## Support

For issues and questions:

- Create issues in the GitHub repository
- Contact: <aaron.pan@databricks.com>