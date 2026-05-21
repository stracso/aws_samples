"""
Iceberg Datalake Migration Glue Job

Migrates Iceberg tables from a shared bucket to a dedicated bucket by:
1. Retrieving the latest metadata file for each table from the Glue Catalog
2. Dropping and re-registering tables pointing to the new S3 location
3. Rewriting data files to update internal path references
4. Cleaning up old snapshots and orphan files
5. Validating row counts post-migration

Usage:
    This script is intended to run as an AWS Glue ETL job (Glue 5.0, PySpark).

    Job Parameters:
        --database          : Glue catalog database name
        --tables            : Comma-separated list of table names to migrate
        --dedicated_bucket  : Target S3 bucket for the migrated tables
        --db_s3_prefix      : S3 prefix within the bucket (default: "warehouse")
        --region            : AWS region (default: "us-west-2")
        --retain_snapshots  : Number of snapshots to retain after cleanup (default: 1)
        --skip_cleanup      : If "true", skip snapshot expiration and orphan file removal
"""

import sys
import os
import boto3
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job


class IcebergMigrationJob:
    """Orchestrates the migration of Iceberg tables to a dedicated S3 bucket."""

    def __init__(self, args: dict):
        self.glue_endpoint = args["GLUE_ENDPOINT"]
        self.database = args["DATABASE"]
        self.table_names = [t.strip() for t in args["TABLES"].split(",")]
        self.dedicated_bucket = args["DEDICATED_BUCKET"]
        self.dedicated_uri = f"{self.dedicated_bucket}/{args['DEDICATED_PREFIX']}"
        self.shared_bucket = args["SHARED_BUCKET"]
        self.shared_uri = f"{self.shared_bucket}/{args['SHARED_PREFIX']}"        
        self.region = args.get("REGION", "us-east-1")
        self.retain_snapshots = "1"
        self.skip_cleanup = args.get("SKIP_CLEANUP", "false").lower() == "true"
        self.dryrun = args.get("DRYRUN", "false").lower() == "true"

        self.spark = (
            SparkSession.builder
            .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
            .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
            .getOrCreate()
        )        
        self.glue_context = GlueContext(self.spark.sparkContext)
        self.job = Job(self.glue_context)
        self.job.init(args.get("JOB_NAME", "iceberg_migration_job"), args)

        self.glue_client = boto3.client("glue", region_name=self.region, endpoint_url=self.glue_endpoint)
        self.metadata_files: dict[str, str] = {}
        self.FIELDS_TO_STRIP = [
            "DatabaseName",
            "CreateTime",
            "UpdateTime",
            "CreatedBy",
            "IsRegisteredWithLakeFormation",
            "CatalogId",
            "VersionId",
            "IsMultiDialectView",
            "Status",
            ]

    def _replace_strings(self, obj, old: str, new: str):
        """Recursively walk a JSON-like structure and replace old with new in all strings."""
        if isinstance(obj, str):
            return obj.replace(old, new)
        if isinstance(obj, list):
            return [replace_strings(item, old, new) for item in obj]
        if isinstance(obj, dict):
            return {k: replace_strings(v, old, new) for k, v in obj.items()}
        return obj


    def _strip_read_only_fields(self, table_def: dict) -> dict:
        """Remove fields that are not accepted by update_table."""
        for field in self.FIELDS_TO_STRIP:
            table_def.pop(field, None)
        return table_def


    def _get_all_tables(self) -> list[str]:
        """Paginate through all tables in a Glue database."""
        table_names = []
        paginator = self.glue_client.get_paginator("get_tables")
        for page in paginator.paginate(DatabaseName=self.database):
            for table in page["TableList"]:
                table_names.append(table["Name"])
        return table_names


    def _migrate_table(self, table_name: str, old_prefix: str, new_prefix: str, dry_run: bool) -> dict:
        """Fetch a table definition, patch bucket references, and update it in Glue."""
        response = self.glue_client.get_table(DatabaseName=self.database, Name=table_name)
        table_def = response["Table"]

        table_input = self.strip_read_only_fields(table_def)
        table_input["StorageDescriptor"] = replace_strings(table_input["StorageDescriptor"], old_prefix, new_prefix)

        if dry_run:
            print(f"  [DRY RUN] Would update {table_name}")
            print(f"    Location: {table_input.get('StorageDescriptor', {}).get('Location', 'N/A')}")
            metadata_loc = table_input.get("Parameters", {}).get("metadata_location", "N/A")
            print(f"    Metadata: {metadata_loc}")
        else:
            self.glue_client.update_table(DatabaseName=self.database, TableInput=table_input)
            print(f"  Updated: {table_name}")

        return table_input

    # ------------------------------------------------------------------
    # Step 0: Resolve latest metadata file for each table
    # ------------------------------------------------------------------

    def _resolve_metadata_files(self):
        """Retrieve the latest metadata file location from the Glue Catalog."""
        print("Resolving metadata files for %d table(s)...", len(self.table_names))

        for table_name in self.table_names:
            response = self.glue_client.get_table(
                DatabaseName=self.database, Name=table_name
            )
            parameters = response["Table"].get("Parameters", {})
            metadata_location = parameters.get("metadata_location")

            if not metadata_location:
                raise ValueError(
                    f"metadata_location not found for table '{self.database}.{table_name}'. "
                    "Ensure the table is a registered Iceberg table."
                )

            metadata_filename = os.path.basename(metadata_location)
            self.metadata_files[table_name] = metadata_filename
            print(f"Table {table_name} -> metadata file: {metadata_filename}")

    # ------------------------------------------------------------------
    # Step 1: Drop and re-register tables at the new location
    # ------------------------------------------------------------------

    def _register_tables(self):
        """Drop existing tables and re-register them from the new metadata location."""
        print("Registering tables at new location...")
        dryrun_prefix = ""
        if self.dryrun:
            dryrun_prefix = "[DRYRUN]"
        for table_name in self.table_names:
            latest_metadata_file = self.metadata_files[table_name]
            metadata_path = (
                f"s3://{self.dedicated_uri}/"
                f"{table_name}/metadata/{latest_metadata_file}"
            )

            fqn = f"glue_catalog.{self.database}.{table_name}"

            print(f"{dryrun_prefix}Dropping table {fqn} (if exists)...")
            if not self.dryrun:
                self.spark.sql(f"DROP TABLE IF EXISTS {fqn}")

            print(f"{dryrun_prefix}Registering table {fqn} from {metadata_path}")
            if not self.dryrun:
                self.spark.sql(
                    f"""
                    CALL glue_catalog.system.register_table(
                        table => '{self.database}.{table_name}',
                        metadata_file => '{metadata_path}'
                    )
                    """
                )
            print(f"Registered: {fqn}")

    # ------------------------------------------------------------------
    # Step 2: Rewrite data files to update internal path references
    # ------------------------------------------------------------------

    def _rewrite_data_files(self):
        """Rewrite data files so all internal references point to the new bucket."""
        print("Rewriting data files...")
        dryrun_prefix = ""
        if self.dryrun:
            dryrun_prefix = "[DRYRUN]"
        for table_name in self.table_names:
            fqn = f"{self.database}.{table_name}"
            print(f"{dryrun_prefix}Rewriting data files for {fqn}...")
            if not self.dryrun:
                self.spark.sql(
                    f"""
                    CALL glue_catalog.system.rewrite_data_files(
                        table => '{fqn}'
                    )
                    """
                )
            print(f"Rewrite complete for {fqn}")

    # ------------------------------------------------------------------
    # Step 3: Clean up old snapshots and orphan files
    # ------------------------------------------------------------------

    def _cleanup_metadata(self):
        """Expire old snapshots and remove orphan files."""
        dryrun_prefix = ""
        if self.dryrun:
            dryrun_prefix = "[DRYRUN]"
        print(
            "Cleaning up metadata (retain_snapshots=%d)...", self.retain_snapshots
        )

        for table_name in self.table_names:
            fqn = f"{self.database}.{table_name}"

            print(f"{dryrun_prefix}Expiring snapshots for {fqn}...")
            if not self.dryrun:
                self.spark.sql(
                    f"""
                    CALL glue_catalog.system.expire_snapshots(
                        table => '{fqn}',
                        retain_last => {self.retain_snapshots}
                    )
                    """
                )

            print(f"{dryrun_prefix}Removing orphan files for {fqn}...")
            if not self.dryrun:
                self.spark.sql(
                    f"""
                    CALL glue_catalog.system.remove_orphan_files(
                        table => '{fqn}'
                    )
                    """
                )
            print(f"Cleanup complete for {fqn}")

    # ------------------------------------------------------------------
    # Step 4: Update Glue Catalog table locations for Iceberg datalake migration.
    # ------------------------------------------------------------------
    def _update_table_location(self):
        succeeded = []
        failed = []
        old_prefix = f"s3://{self.shared_uri}"
        new_prefix = f"s3://{self.dedicated_uri}"
        print("---- Updating tables location ---")
        print(f"Database: {self.database}, {len(self.table_names)} tables, from {old_prefix} to {new_prefix}")
        for table_name in self.table_names:
            print(f"Processing: {table_name}")
            try:
                self._migrate_table(table_name, old_prefix, new_prefix, self.dryrun)
                succeeded.append(table_name)
            except Exception as e:
                print(f"[ERROR]  FAILED: {table_name} — {e}")
                failed.append(table_name)
        print(f"DONE. Suceeded: {len(succeeded)}, Failed: {len(failed)}")
        if failed:
            print(f"Failed tables: {', '.join(failed)}")

    # ------------------------------------------------------------------
    # Step 5: Validate row counts
    # ------------------------------------------------------------------

    def _validate(self):
        """Log row counts for each migrated table as a sanity check."""
        print("Validating migrated tables...")

        for table_name in self.table_names:
            fqn = f"glue_catalog.{self.database}.{table_name}"
            df = self.spark.sql(f"SELECT count(*) AS cnt FROM {fqn}")
            count = df.collect()[0]["cnt"]
            print(f"Table {fqn} -> row count: {count}")
    
    def run(self):
        """Execute the full migration pipeline."""
        print(f"Starting Iceberg migration for database={self.database}', tables={self.table_names}, bucket={self.dedicated_bucket}")

        self._resolve_metadata_files()
        self._register_tables()
        self._rewrite_data_files()
        self._update_table_location()

        if not self.skip_cleanup:
            self._cleanup_metadata()

        self._validate()

        self.job.commit()
        print("Migration completed successfully.")

# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------

if __name__ == "__main__":
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "REGION",
            "GLUE_ENDPOINT",
            "DATABASE",
            "TABLES",
            "SHARED_BUCKET",
            "SHARED_PREFIX",
            "DEDICATED_BUCKET",
            "DEDICATED_PREFIX",
            "SKIP_CLEANUP",
            "DRYRUN"
        ],
    )
    print(f"INPUT ARGS: {args}")
    migration = IcebergMigrationJob(args)
    migration.run()
