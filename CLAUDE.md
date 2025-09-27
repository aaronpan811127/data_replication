# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Setup and Installation
```bash
# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install development dependencies
pip install -r requirements-dev.txt

# Install the package in development mode
pip install -e .
```

### Testing
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

### Code Quality
```bash
# Format code
black src/ tests/

# Sort imports
isort src/ tests/

# Type checking
mypy src/

# Lint code
flake8 src/ tests/

# Run all quality checks
black src/ tests/ && isort src/ tests/ && flake8 src/ tests/ && mypy src/
```

### Running the Application
```bash
# Main CLI entry point - run all enabled operations
data-replicator configs/config.yaml

# Run specific operation only
data-replicator configs/config.yaml --operation backup
data-replicator configs/config.yaml --operation replication
data-replicator configs/config.yaml --operation reconciliation

# Validate configuration
data-replicator configs/config.yaml --validate-only

# Dry run to preview operations
data-replicator configs/config.yaml --dry-run

# Combine dry run with specific operation
data-replicator configs/config.yaml --operation backup --dry-run

# Enable verbose logging
data-replicator configs/config.yaml --verbose
```

## Architecture Overview

### Core Components
- **main.py**: Primary CLI interface and orchestration logic for all operations
- **cli/main.py**: Lightweight CLI wrapper that defers heavy imports
- **config/**: Configuration models with Pydantic validation and YAML loading
- **audit/**: Structured logging and audit trail system with Databricks table storage
- **databricks_operations.py**: Core Databricks utilities and table operations
- **utils.py**: Spark session management, DLT table utilities, and retry logic
- **exceptions.py**: Custom exception classes for error handling

### Operation Providers
- **providers/backup_provider.py**: Deep clone operations from DLT internal tables to backup catalogs
- **providers/replication_provider.py**: Cross-workspace table replication with schema enforcement
- **providers/reconciliation_provider.py**: Data validation with row counts, schema checks, missing data detection
- **providers/provider_factory.py**: Factory for creating and managing provider instances
- **providers/base_provider.py**: Base abstract class for all providers

### DLT Table Handling
The system automatically handles DLT (Delta Live Tables) complexities:
- Extracts pipeline IDs using `DESCRIBE DETAIL` on DLT tables
- Constructs internal table paths: `__dlt_materialization_schema_<pipeline_id>` and `__materialization_mat_<pipeline_id>_<table_name>_1`
- Performs operations on internal tables rather than DLT tables directly

### Configuration Structure
YAML-based configuration with these main sections:
- `source_databricks_connect_config` / `target_databricks_connect_config`: Workspace connection details
- `audit_config`: Audit logging table configuration  
- `target_catalogs`: List of catalogs with nested backup/replication configs
- Each catalog can have `backup_config` and `replication_config` blocks

### Error Handling and Retry
- Configurable retry logic with exponential backoff using tenacity
- Graceful degradation where operations continue if individual tables fail
- Comprehensive error logging with correlation IDs and full stack traces
- All operations tracked in audit tables for troubleshooting

## Testing Configuration

pytest is configured with:
- Test discovery in `tests/` directory
- Coverage reporting with 70% minimum threshold
- Markers for `unit`, `integration`, and `slow` test categories
- Source path set to `src/` for proper imports

## Key Dependencies

### Runtime Dependencies
- **databricks-connect**: Primary Databricks connectivity
- **databricks-sdk**: Databricks REST API operations  
- **databricks-dlt**: Delta Live Tables operations
- **pydantic**: Configuration validation
- **typer/click**: CLI framework
- **tenacity**: Retry logic
- **structlog**: Structured logging
- **pyyaml**: Configuration file parsing
- **requests**: HTTP utilities
- **rich**: Enhanced CLI output

### Development Dependencies
- **pytest**: Testing framework with coverage, mock, and asyncio plugins
- **black/isort/flake8/mypy**: Code quality tools
- **pre-commit**: Git hooks for code quality

## Important Implementation Notes

### Configuration Loading
- Configuration files should be placed in the `configs/` directory
- The system validates configuration against Pydantic models in `config/models.py`
- Secret management uses Databricks secret scopes rather than hardcoded values
- Support for schema filtering expressions and table inclusion/exclusion

### Execution Environments
- `execute_at: source` - Run operations on source workspace (backup only)
- `execute_at: target` - Run operations on target workspace (all operations)
- `execute_at: external` - Run operations from external compute

### Audit and Logging
- All operations create audit entries in configured Databricks tables
- Unique run IDs allow tracking operations across multiple components
- Structured logging provides detailed context for debugging
- DataReplicationLogger provides centralized logging configuration

### Concurrency and Performance
- The system supports configurable worker pools for parallel operations
- Operations are designed to handle large datasets with appropriate timeouts
- Error handling ensures partial failures don't block other operations
- Automatic table type detection (STREAMING_TABLE, MANAGED) with SQL optimization

### DLT Internal Table Resolution
- Multiple fallback strategies for finding DLT internal tables
- Handles different naming conventions and catalog structures
- Automatic pipeline ID extraction from table properties
- Support for both materialized views and streaming tables