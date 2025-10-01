"""
Databricks operations utility for common database DB operations.

This module provides utilities for interacting with Databricks catalogs,
schemas, and tables.
"""

from typing import List
from typing import Tuple
from databricks.connect import DatabricksSession
from pyspark.sql.functions import col

from data_replication.config.models import TableType
from data_replication.exceptions import TableNotFoundError


class DatabricksOperations:
    """Utility class for Databricks operations."""

    def __init__(self, spark: DatabricksSession):
        """
        Initialize Databricks operations.

        Args:
            spark: DatabricksSession instance
        """
        self.spark = spark

    def create_catalog_if_not_exists(self, catalog_name: str) -> None:
        """
        Create catalog if it doesn't exist.

        Args:
            catalog_name: Name of the catalog to create
        """
        try:
            self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
        except Exception as e:
            # Some environments might not support catalog creation
            print(f"Warning: Could not create catalog {catalog_name}: {e}")

    def create_schema_if_not_exists(self, catalog_name: str, schema_name: str) -> None:
        """
        Create schema if it doesn't exist.

        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema to create
        """
        full_schema = f"{catalog_name}.{schema_name}"
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full_schema}")

    def get_tables_in_schema(self, catalog_name: str, schema_name: str) -> List[str]:
        """
        Get all tables in a schema, including STREAMING_TABLE and MANAGED table types.

        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema

        Returns:
            List of table names
        """
        try:
            full_schema = f"{catalog_name}.{schema_name}"

            # Get visible tables using SHOW TABLES (excludes internal tables)
            show_tables_df = self.spark.sql(f"SHOW TABLES IN {full_schema}").filter(
                "isTemporary == false"
            )
            return [row.tableName for row in show_tables_df.collect()]

        except Exception:
            # Schema might not exist or be accessible
            return []

    def filter_tables_by_type(
        self,
        catalog_name: str,
        schema_name: str,
        table_names: List[str],
        table_types: List[TableType],
    ) -> List[str]:
        """
        Filter a list of table names to only include STREAMING_TABLE and MANAGED types.

        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema
            table_names: List of table names to filter

        Returns:
            List of table names that are STREAMING_TABLE or MANAGED
        """

        return [
            table
            for table in table_names
            if self.get_table_type(f"{catalog_name}.{schema_name}.{table}").upper()
            in [type.upper() for type in table_types]
        ]

    def get_all_schemas(self, catalog_name: str) -> List[str]:
        """
        Get all schemas in a catalog.

        Args:
            catalog_name: Name of the catalog

        Returns:
            List of schema names
        """
        try:
            schemas_df = self.spark.sql(f"SHOW SCHEMAS IN {catalog_name}").filter(
                'databaseName != "information_schema"'
            )
            return [row.databaseName for row in schemas_df.collect()]
        except Exception:
            # Catalog might not exist or be accessible
            return []

    def get_schemas_by_filter(
        self, catalog_name: str, filter_expression: str
    ) -> List[str]:
        """
        Get schemas matching a filter expression.

        Args:
            catalog_name: Name of the catalog
            filter_expression: SQL filter expression

        Returns:
            List of schema names matching the filter
        """
        try:
            # Get all schemas first
            schemas_df = self.spark.sql(f"SHOW SCHEMAS IN {catalog_name}").filter(
                'databaseName != "information_schema"'
            )

            # Apply filter expression
            filtered_df = schemas_df.filter(filter_expression)

            return [row.databaseName for row in filtered_df.collect()]
        except Exception as e:
            print(f"Warning: Could not filter schemas in {catalog_name}: {e}")
            return []

    def describe_table_detail(self, table_name: str) -> dict:
        """
        Get detailed information about a table.

        Args:
            table_name: Full table name (catalog.schema.table)

        Returns:
            Dictionary containing table details
        """
        try:
            details = (
                self.spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0].asDict()
            )
            return details

        except Exception:
            details_str = (
                self.spark.sql(f"DESCRIBE EXTENDED {table_name}")
                .filter(col("col_name") == "Table Properties")
                .select("data_type")
                .collect()[0][0]
            )
            properties = {}
            result_clean = details_str.replace("[", "").replace("]", "")
            properties = dict(
                item.split("=") for item in result_clean.split(",") if "=" in item
            )
            return {"properties": properties}

    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists.

        Args:
            table_name: Full table name (catalog.schema.table)

        Returns:
            True if table exists, False otherwise
        """
        return self.spark.catalog.tableExists(table_name)

    def get_table_type(self, table_name) -> str:
        pipeline_id = None
        table_type = None
        if self.spark.catalog.tableExists(table_name):
            # First check if it's a view using DESCRIBE EXTENDED to avoid DESCRIBE DETAIL error
            try:
                table_type = (
                    self.spark.sql(f"DESCRIBE EXTENDED {table_name}")
                    .filter(
                        (col("col_name") == "Type")
                        & (
                            (col("data_type").contains("MANAGED"))
                            | (col("data_type").contains("EXTERNAL"))
                            | (col("data_type").contains("STREAMING_TABLE"))
                            | (col("data_type").contains("MATERIALIZED_VIEW"))
                            | (col("data_type").contains("VIEW"))
                        )
                    )
                    .select("data_type")
                    .collect()[0][0]
                )
                if table_type != "MANAGED":
                    return table_type
            except Exception:
                pass
            # when it's Managed, check if it's STREAMING_TABLE as it may be a backup table
            pipeline_id = (
                self.spark.sql(f"DESCRIBE DETAIL {table_name}")
                .collect()[0]
                .asDict()["properties"]
                .get("pipelines.pipelineId", None)
            )
            if pipeline_id:
                table_type = "STREAMING_TABLE"
                return table_type

            return "MANAGED"

        raise TableNotFoundError(f"Table {table_name} does not exist")

    def drop_table_if_exists(self, table_name: str) -> None:
        """
        Drop table if it exists.

        Args:
            table_name: Full table name (catalog.schema.table)
        """
        try:
            self.spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        except Exception as e:
            print(f"Warning: Could not drop table {table_name}: {e}")

    def get_pipeline_id(self, table_name: str) -> str:
        """
        get pipleline id from table properties

        Args:
            table_name: table full name (catalog.schema.table)
        Returns:
            pipeline id if exists else None
        """

        table_details = self.describe_table_detail(table_name)

        return table_details["properties"].get("pipelines.pipelineId", None)

    def get_table_details(self, table_name: str) -> Tuple[str, bool]:
        if self.spark.catalog.tableExists(table_name):
            pipeline_id = self.get_pipeline_id(table_name)
            if pipeline_id:
                # Handle streaming table or materialized view
                actual_table_name = self._get_internal_table_name(
                    table_name, pipeline_id
                )
                return {
                    "table_name": actual_table_name,
                    "is_dlt": True,
                    "pipeline_id": pipeline_id,
                }

            # If not a DLT table, just return the original table name and "delta"
            return {"table_name": table_name, "is_dlt": False, "pipeline_id": None}
        else:
            raise TableNotFoundError(f"Table {table_name} does not exist")

    def _get_internal_table_name(self, table_name: str, pipeline_id: str) -> str:
        """
        Get the internal table name for streaming tables or materialized views.

        Args:
            table_name: Original table name
            pipeline_id: Pipeline ID for the table

        Returns:
            Internal table name for deep clone
        """

        # Extract table name from full table name
        table_name_only = table_name.split(".")[-1]

        # Construct internal table name
        pipeline_id_underscores = pipeline_id.replace("-", "_")
        internal_table_name = (
            f"__materialization_mat_{pipeline_id_underscores}_{table_name_only}_1"
        )

        # Construct internal schema name
        internal_schema_name = f"__dlt_materialization_schema_{pipeline_id_underscores}"

        # Get catalog from original table name
        catalog_name = table_name.split(".")[0]

        # Check possible locations for the internal table 1
        full_internal_table_name = (
            f"`__databricks_internal`.`{internal_schema_name}`.`{table_name_only}`"
        )
        # print(full_internal_table_name)
        if self.table_exists(full_internal_table_name):
            return full_internal_table_name

        # Check possible locations for the internal table 2
        full_internal_table_name = (
            f"`__databricks_internal`.`{internal_schema_name}`.`{internal_table_name}`"
        )
        # print(full_internal_table_name)
        if self.table_exists(full_internal_table_name):
            return full_internal_table_name

        # Check possible locations for the internal table 3
        full_internal_table_name = (
            f"{catalog_name}.{table_name.split('.')[1]}.`{internal_table_name}`"
        )
        # print(full_internal_table_name)
        if self.table_exists(full_internal_table_name):
            return full_internal_table_name

        # Check possible locations for the internal table 4
        internal_table_name = f"__materialization_mat_{table_name_only}_1"
        full_internal_table_name = (
            f"`__databricks_internal`.`{internal_schema_name}`.`{internal_table_name}`"
        )
        # print(full_internal_table_name)
        if self.table_exists(full_internal_table_name):
            return full_internal_table_name

        raise Exception(
            f"Could not find internal table for {table_name} with pipeline ID {pipeline_id}"
        )

    def get_common_fields(self, source_table: str, target_table: str) -> List[str]:
        """
        Get common fields between source and target tables.

        Args:
            source_table: Full source table name (catalog.schema.table)
            target_table: Full target table name (catalog.schema.table)

        Returns:
            List of common field names
        """
        source_fields = set(self.get_table_fields(source_table))
        target_fields = set(self.get_table_fields(target_table))
        return list(source_fields & target_fields)

    def get_table_fields(self, table_name: str) -> List[str]:
        """
        Get field names of a table.

        Args:
            table_name: Full table name (catalog.schema.table)

        Returns:
            List of field names
        """
        try:
            df = self.spark.table(table_name)
            return df.columns
        except Exception as e:
            print(f"Warning: Could not get fields for table {table_name}: {e}")
            return []

    def create_delta_sharing_recipient(self, recipient_name: str, authentication_type: str = "TOKEN") -> bool:
        """
        Create a Delta Sharing recipient.

        Args:
            recipient_name: Name of the recipient
            authentication_type: Authentication type (TOKEN or IP_ACCESS_LIST)

        Returns:
            True if created successfully, False if already exists
        """
        try:
            self.spark.sql(f"""
                CREATE RECIPIENT IF NOT EXISTS {recipient_name}
            """)
            return True
        except Exception as e:
            print(f"Warning: Could not create recipient {recipient_name}: {e}")
            return False

    def create_share(self, share_name: str) -> bool:
        """
        Create a Delta Share.

        Args:
            share_name: Name of the share

        Returns:
            True if created successfully, False if already exists
        """
        try:
            self.spark.sql(f"""
                CREATE SHARE IF NOT EXISTS {share_name}
            """)
            return True
        except Exception as e:
            print(f"Warning: Could not create share {share_name}: {e}")
            return False

    def grant_share_to_recipient(self, share_name: str, recipient_name: str) -> bool:
        """
        Grant a share to a recipient.

        Args:
            share_name: Name of the share
            recipient_name: Name of the recipient

        Returns:
            True if granted successfully, False otherwise
        """
        try:
            self.spark.sql(f"""
                GRANT SELECT ON SHARE {share_name} TO RECIPIENT {recipient_name}
            """)
            return True
        except Exception as e:
            print(f"Warning: Could not grant share {share_name} to recipient {recipient_name}: {e}")
            return False

    def add_schema_to_share(self, share_name: str, schema_name: str) -> bool:
        """
        Add schema to share, checking if schema already exists in share.

        Args:
            share_name: Name of the share
            schema_name: Full schema name (catalog.schema)

        Returns:
            True if added, False if already exists in share
        """
        try:
            # Check if schema already exists in the share
            existing_schemas_df = self.spark.sql(f"SHOW ALL IN SHARE {share_name}")
            existing_schemas = [row['name'] for row in existing_schemas_df.collect() 
                             if row['type'] == 'SCHEMA' and row['name'] == schema_name]
            
            if existing_schemas:
                print(f"Schema {schema_name} already exists in share {share_name}")
                return False
            
            # Add schema to share
            self.spark.sql(f"""
                ALTER SHARE {share_name} ADD SCHEMA {schema_name}
            """)
            print(f"Added schema {schema_name} to share {share_name}")
            return True
        except Exception as e:
            print(f"Warning: Could not add schema {schema_name} to share {share_name}: {e}")
            return False

    def create_catalog_from_share(self, catalog_name: str, share_name: str, provider: str) -> bool:
        """
        Create a catalog from a Delta Share.

        Args:
            catalog_name: Name of the catalog to create
            share_name: Name of the share
            provider: Provider name for the share

        Returns:
            True if created successfully, False otherwise
        """
        try:
            self.spark.sql(f"""
                CREATE CATALOG IF NOT EXISTS {catalog_name}
                USING DELTASHARING
                OPTIONS (
                    share = '{provider}.{share_name}'
                )
            """)
            return True
        except Exception as e:
            print(f"Warning: Could not create catalog {catalog_name} from share {share_name}: {e}")
            return False
