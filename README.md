# Databricks Data Replication System

A comprehensive system for replicating, backing up, and reconciling Databricks DLT tables across different metastores using delta sharing.

## Overview

The Data Replication System provides enterprise-grade capabilities for:

- **Backup**: Deep clone DLT tables from internal tables to backup catalogs
- **Delta Sharing**: Set up cross-workspace data sharing with recipients and shares  
- **Replication**: Replicate tables across metastores with schema enforcement
- **Reconciliation**: Validate data consistency with row counts, schema checks, and missing data detection

## Key Features

- ✅ **DLT Table Support**: Automatically identifies and handles DLT internal tables
- ✅ **Configuration-Driven**: YAML-based configuration with Pydantic validation
- ✅ **Comprehensive Logging**: Structured logging with audit trails to Databricks tables
- ✅ **Error Handling**: Retry logic with exponential backoff
- ✅ **Concurrency**: Parallel execution with configurable worker pools
- ✅ **CLI Interface**: Rich command-line interface with progress tracking
- ✅ **Extensible Architecture**: Plugin-based provider system for future enhancements

## Architecture

```
src/data_replication/
├── config/          # Configuration models and loading
├── core/            # Base interfaces and abstractions  
├── connectors/      # Databricks connector implementation
├── audit/           # Logging and audit system
├── backup/          # Backup operations using deep clone
├── deltashare/      # Delta sharing setup and management
├── replication/     # Table replication (TODO)
├── reconciliation/  # Data reconciliation (TODO)
├── utils/           # DLT utilities, retry logic, validation
├── cli/             # Command-line interface
└── orchestrator.py  # Main coordination logic
```

## Installation

```bash
# Install dependencies
pip install -r requirements-dev.txt

# Install the package in development mode
pip install -e .
```

## Configuration

Create a `config.yaml` file with your settings:

```yaml
version: "1.0"

# Concurrency settings
concurrency:
  max_workers: 4
  timeout_seconds: 3600

# Retry settings
retry: 
  max_attempts: 3
  retry_delay_seconds: 5

# Source Databricks workspace
source_databricks_connect_config: 
  host: "https://source-databricks-instance"
  token: 
    secret_scope: "my_secret_scope"
    secret_key: "my_secret_key"

# Target Databricks workspace
target_databricks_connect_config: 
  host: "https://target-databricks-instance"
  token: 
    secret_scope: "my_secret_scope"
    secret_key: "my_secret_key"

# Backup configuration
backup_config:
  enabled: true
  audit_log_table: "operations.audit.backup_logs"
  catalogs:
    - catalog_name: "aaron"
      backup_catalog: "backup_catalog"
      schemas:
        - schema_name: "source_schema"
          backup_schema: "backup_source_schema"
        - schema_name: "analytics_schema"
          backup_schema_suffix: "_backup"
          tables:
            - table_name: "product_metrics"
            - table_name: "sales_data"

# Delta share configuration
delta_share_config: 
  enabled: true
  recipient_id: "azure_recipient"
  shares:
    - share_name: "main_share"
      catalog_name: "aaron"
      schema_suffix: "_share"
      shared_catalog_name: "aaron_aws"
      schemas:
        - schema_name: "source_schema"

# Replication configuration (TODO)
replication_config:
  enabled: false
  # ... configuration details

# Reconciliation configuration (TODO)  
reconciliation_config:
  enabled: false
  # ... configuration details
```

## Usage

### Command Line Interface

```bash
# Run all enabled operations
data-replicator run --config config.yaml

# Validate configuration without running
data-replicator validate --config config.yaml

# Dry run to see what would be executed
data-replicator run --config config.yaml --dry-run

# Show version
data-replicator version
```

### Programmatic Usage

```python
from data_replication.orchestrator import DataReplicationOrchestrator

# Create orchestrator from config file
orchestrator = DataReplicationOrchestrator.from_config_file("config.yaml")

# Initialize connections
orchestrator.initialize()

try:
    # Run all operations
    results = orchestrator.run_all_operations()
    
    # Process results
    for operation_type, operation_results in results.items():
        print(f"{operation_type}: {len(operation_results)} operations")
        
finally:
    # Cleanup resources
    orchestrator.cleanup()
```

## DLT Table Handling

The system automatically handles DLT table complexities:

1. **Pipeline ID Extraction**: Uses `DESCRIBE DETAIL` to extract pipeline information
2. **Internal Table Identification**: Constructs internal table paths following the pattern:
   - Pipeline schema: `__dlt_materialization_schema_<pipeline_id_with_underscores>`  
   - Internal table: `__materialization_mat_<pipeline_id_with_underscores>_<table_name>_1`
3. **Deep Clone Operations**: Performs deep clone from internal tables, not DLT tables directly

## Audit and Logging

All operations are logged with:

- **Structured Logging**: JSON format with correlation IDs
- **Audit Tables**: Complete audit trail stored in Databricks tables
- **Run Tracking**: Unique run IDs for traceability
- **Error Details**: Full error information for troubleshooting

## Error Handling and Retry

- **Configurable Retry**: Exponential backoff with configurable attempts
- **Graceful Degradation**: Operations continue even if individual tables fail
- **Detailed Error Reporting**: Comprehensive error messages and stack traces

## Development Status

✅ **Completed Components:**
- Configuration system with Pydantic validation
- Databricks connector with authentication
- DLT table utilities for pipeline ID extraction  
- Backup operations using deep clone from DLT internal tables
- Delta sharing setup (recipient, share, catalog creation)
- Table replication with schema enforcement and intermediate staging
- Data reconciliation with row count, schema, and missing data checks
- Comprehensive logging and audit system with run tracking
- CLI interface with rich output and validation
- Main orchestrator with error handling and retry logic
- Basic test suite with pytest configuration

🚧 **Future Enhancements:**
- Comprehensive integration test suite with real Databricks instances
- Performance optimizations for large datasets
- Advanced concurrency patterns
- Real-time monitoring and alerting
- Additional data source/target connectors

## Testing

```bash
# Run tests (when implemented)
pytest tests/

# Run with coverage
pytest --cov=data_replication tests/
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
- Contact: aaron.pan@databricks.com