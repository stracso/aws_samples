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
import logging

import boto3
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class IcebergMigrationJob:
    """Orchestrates the migration of Iceberg tables to a dedicated S3 bucket."""

    def __init__(self, args: dict):
        self.database = args["database"]
        self.tables = [t.strip() for t in args["tables"].split(",")]
        self.dedicated_bucket = args["dedicated_bucket"]
        self.db_s3_prefix = args.get("db_s3_prefix", "warehouse")
        self.region = args.get("region", "us-west-2")
        self.retain_snapshots = int(args.get("retain_snapshots", "1"))
        self.skip_cleanup = args.get("skip_cleanup", "false").lower() == "true"

        self.spark = SparkSession.builder.getOrCreate()
        self.glue_context = GlueContext(self.spark.sparkContext)
        self.job = Job(self.glue_context)
        self.job.init(args.get("JOB_NAME", "iceberg_migration_job"), args)

        self.glue_client = boto3.client("glue", region_name=self.region)
        self.metadata_files: dict[str, str] = {}

    def run(self):
        """Execute the full migration pipeline."""
        logger.info(
            "Starting Iceberg migration for database='%s', tables=%s, bucket='%s'",
            self.database,
            self.tables,
            self.dedicated_bucket,
        )

        self._resolve_metadata_files()
        self._register_tables()
        self._rewrite_data_files()

        if not self.skip_cleanup:
            self._cleanup_metadata()

        self._validate()

        self.job.commit()
        logger.info("Migration completed successfully.")

    # ------------------------------------------------------------------
    # Step 0: Resolve latest metadata file for each table
    # ------------------------------------------------------------------

    def _resolve_metadata_files(self):
        """Retrieve the latest metadata file location from the Glue Catalog."""
        logger.info("Resolving metadata files for %d table(s)...", len(self.tables))

        for table_name in self.tables:
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
            logger.info(
                "Table '%s' -> metadata file: %s", table_name, metadata_filename
            )

    # ------------------------------------------------------------------
    # Step 1: Drop and re-register tables at the new location
    # ------------------------------------------------------------------

    def _register_tables(self):
        """Drop existing tables and re-register them from the new metadata location."""
        logger.info("Registering tables at new location...")

        for table_name in self.tables:
            latest_metadata_file = self.metadata_files[table_name]
            metadata_path = (
                f"s3://{self.dedicated_bucket}/{self.db_s3_prefix}/"
                f"{table_name}/metadata/{latest_metadata_file}"
            )

            fqn = f"glue_catalog.{self.database}.{table_name}"

            logger.info("Dropping table %s (if exists)...", fqn)
            self.spark.sql(f"DROP TABLE IF EXISTS {fqn}")

            logger.info("Registering table %s from %s", fqn, metadata_path)
            self.spark.sql(
                f"""
                CALL glue_catalog.system.register_table(
                    table => '{self.database}.{table_name}',
                    metadata_file => '{metadata_path}'
                )
                """
            )
            logger.info("Registered: %s", fqn)

    # ------------------------------------------------------------------
    # Step 2: Rewrite data files to update internal path references
    # ------------------------------------------------------------------

    def _rewrite_data_files(self):
        """Rewrite data files so all internal references point to the new bucket."""
        logger.info("Rewriting data files...")

        for table_name in self.tables:
            fqn = f"{self.database}.{table_name}"
            logger.info("Rewriting data files for %s...", fqn)

            self.spark.sql(
                f"""
                CALL glue_catalog.system.rewrite_data_files(
                    table => '{fqn}'
                )
                """
            )
            logger.info("Rewrite complete for %s", fqn)

    # ------------------------------------------------------------------
    # Step 3: Clean up old snapshots and orphan files
    # ------------------------------------------------------------------

    def _cleanup_metadata(self):
        """Expire old snapshots and remove orphan files."""
        logger.info(
            "Cleaning up metadata (retain_snapshots=%d)...", self.retain_snapshots
        )

        for table_name in self.tables:
            fqn = f"{self.database}.{table_name}"

            logger.info("Expiring snapshots for %s...", fqn)
            self.spark.sql(
                f"""
                CALL glue_catalog.system.expire_snapshots(
                    table => '{fqn}',
                    retain_last => {self.retain_snapshots}
                )
                """
            )

            logger.info("Removing orphan files for %s...", fqn)
            self.spark.sql(
                f"""
                CALL glue_catalog.system.remove_orphan_files(
                    table => '{fqn}'
                )
                """
            )
            logger.info("Cleanup complete for %s", fqn)

    # ------------------------------------------------------------------
    # Step 4: Validate row counts
    # ------------------------------------------------------------------

    def _validate(self):
        """Log row counts for each migrated table as a sanity check."""
        logger.info("Validating migrated tables...")

        for table_name in self.tables:
            fqn = f"glue_catalog.{self.database}.{table_name}"
            df = self.spark.sql(f"SELECT count(*) AS cnt FROM {fqn}")
            count = df.collect()[0]["cnt"]
            logger.info("Table %s -> row count: %d", fqn, count)


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------

if __name__ == "__main__":
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "database",
            "tables",
            "dedicated_bucket",
            "db_s3_prefix",
            "region",
            "retain_snapshots",
            "skip_cleanup",
        ],
    )

    migration = IcebergMigrationJob(args)
    migration.run()
