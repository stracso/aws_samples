"""
Iceberg Datalake Migration Glue Job (Cross-Region)

Migrates Iceberg tables from a source bucket in one region to a dedicated bucket
in another region by:
1. Reading metadata from the Glue Catalog 
2. Dropping and re-registering tables pointing to the new S3 location
4. Rewriting table paths in metadata (manifests) to point to the new bucket
   using Iceberg 1.10's rewrite_table_path() — no data files are rewritten
56. Validating row counts post-migration

Usage:
    This script is intended to run as an AWS Glue ETL job (Glue 5.1, PySpark).
    Requires Iceberg 1.10+ for the rewrite_table_path() stored procedure.
    The job should run in the DESTINATION region so Spark catalog operations
    target the correct Glue endpoint.

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
        self.region = args['REGION'] 
        self.glue_endpoint = args["GLUE_ENDPOINT"]        
        self.database = args["DATABASE"]
        self.table_names = []
        if 'TABLES' in args and len(args['TABLES']) > 0:
            self.table_names = [t.strip() for t in args["TABLES"].split(",")]
        self.shared_bucket = args["SHARED_BUCKET"]
        self.shared_uri = f"{self.shared_bucket}/{args['SHARED_PREFIX']}"
        self.dedicated_bucket = args["DEDICATED_BUCKET"]
        self.dedicated_uri = f"{self.dedicated_bucket}/{args['DEDICATED_PREFIX']}"
        self.retain_snapshots = "1"
        self.dryrun = args.get("DRYRUN", "false").lower() == "true"
        self.spark = (
            SparkSession.builder
            .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
            .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.glue_catalog.warehouse", f"s3://{self.dedicated_uri}/") 
            .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
            .getOrCreate()
        )
        self.glue_context = GlueContext(self.spark.sparkContext)
        self.job = Job(self.glue_context)
        self.job.init(args.get("JOB_NAME", "iceberg_migration_job"), args)

        #  Glue client — used to WRITE/UPDATE tables in the target 
        self.glue_client = boto3.client(
            "glue", region_name=self.region, endpoint_url=self.glue_endpoint)

        self.metadata_files: dict[str, str] = {}

    def _get_all_tables(self) -> list[str]:
        """Paginate through all tables in the  Glue database."""
        print("Fetching list of table names from source database...")
        table_names = []
        paginator = self.glue_client.get_paginator("get_tables")
        for page in paginator.paginate(DatabaseName=self.database):
            for table in page["TableList"]:
                table_names.append(table["Name"])
        return table_names

    def _register_tables(self, table_name, metadata_file):
        """Register tables in destination catalog from SOURCE metadata location.
        
        We register using the original metadata file at the source bucket path.
        This is the known-good metadata. rewrite_table_path() will then produce
        new metadata pointing to the destination paths.
        """
        print("Registering tables in destination catalog from new metadata...")
        dryrun_prefix = "[DRYRUN] " if self.dryrun else ""

        fqn = f"glue_catalog.{self.database}.{table_name}"

        # Use Glue API to delete the table entry without Spark trying to read metadata
        print(f"{dryrun_prefix}Removing table {table_name} from  catalog (if exists)...")
        if not self.dryrun:
            try:
                self.glue_client.delete_table(
                    DatabaseName=self.database, Name=table_name
                )
                print(f"  Deleted existing table entry: {table_name}")
            except self.glue_client.exceptions.EntityNotFoundException:
                print(f"  Table {table_name} does not exist yet, skipping delete.")

        print(f"{dryrun_prefix}Registering table {fqn} from {metadata_file}")
        if not self.dryrun:
            self.spark.sql(
                f"""
                CALL glue_catalog.system.register_table(
                    table => '{self.database}.{table_name}',
                    metadata_file => '{metadata_file}'
                )
                """
            )

        print(f"Registered: {fqn}")

    def _copy_metadata_files(self, s3_client, staging_location, table_name):
        # Copy staged metadata to the target table's metadata folder
        staged_metadata_path = f"{staging_location}/metadata"
        dest_metadata_path = f"s3://{self.dedicated_uri}/{table_name}/metadata"

        # Parse bucket and key from staged path
        staged_no_scheme = staged_metadata_path.replace("s3://", "")
        staged_bucket = staged_no_scheme.split("/", 1)[0]
        staged_key = staged_no_scheme.split("/", 1)[1]

        # Parse bucket and key for destination
        dest_no_scheme = dest_metadata_path.replace("s3://", "")
        dest_bucket = dest_no_scheme.split("/", 1)[0]
        dest_key = dest_no_scheme.split("/", 1)[1]

        print(f"  Copying metadata: {staged_metadata_path} -> {dest_metadata_path}")
        # List all files in staging and copy all metadata to the target metadata folder
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=staged_bucket, Prefix=staged_key):
            for obj in page.get("Contents", []):
                staged_obj_key = obj["Key"]                       
                # Determine relative path from staging and place under table metadata
                relative_path = staged_obj_key[len(staged_key):].lstrip("/")
                target_key = f"{self.dedicated_uri.split('/', 1)[1]}/{table_name}/metadata/{relative_path}"
                print(f"  Copying staged file: {relative_path}")
                print(f"  SRC:[{staged_obj_key}] -> DEST:[{target_key}]")
                s3_client.copy_object(
                    Bucket=dest_bucket,
                    Key=target_key,
                    CopySource={"Bucket": staged_bucket, "Key": staged_obj_key},
                )

    # ------------------------------------------------------------------
    # Rewrite table paths (metadata-only, no data rewrite)
    # ------------------------------------------------------------------
    def _rewrite_table_path(self):
        """Rewrite internal metadata path references from source to destination bucket.
        
        Uses Iceberg 1.10's rewrite_table_path() procedure (available in Glue 5.1)
        which updates file paths in metadata/manifests without rewriting actual data files.
        
        Flow per table:
        1. Call rewrite_table_path() with a staging location inside the destination table path
        2. Copy the new metadata from staging to the correct metadata location in the target
        3. Delete the old catalog entry and re-register using the copied metadata file
        """
        print("Rewriting table paths (metadata-only)...")
        dryrun_prefix = "[DRYRUN] " if self.dryrun else ""
        s3_client = boto3.client("s3", region_name=self.region)

        for table_name in self.table_names:
            fqn = f"{self.database}.{table_name}"
            src = f"s3://{self.shared_uri}/{table_name}"
            tgt = f"s3://{self.dedicated_uri}/{table_name}"
            # Staging location inside the destination table path
            staging_location = f"s3://{self.dedicated_uri}/{table_name}/copy-table-staging"

            print(f"{dryrun_prefix}Rewriting table path for {fqn}...")
            print(f"  source_prefix: {src}")
            print(f"  target_prefix: {tgt}")
            print(f"  staging_location: {staging_location}")

            if not self.dryrun:
                # Step 1: Run rewrite_table_path with staging location in destination
                result = self.spark.sql(
                    f"""
                    CALL glue_catalog.system.rewrite_table_path(
                        table => '{fqn}',
                        source_prefix => '{src}',
                        target_prefix => '{tgt}',
                        staging_location => '{staging_location}'
                    )
                    """
                )
                rows = result.collect()
                if rows:
                    staged_metadata_output = rows[0][0]
                    print(f"  Staged metadata output: {staged_metadata_output}")
                else:
                    raise RuntimeError(f"rewrite_table_path returned no output for {fqn}")

                self._copy_metadata_files(s3_client, staging_location, table_name)

                # Remove old catalog entry and register with new metadata
                dest_metadata_file = f"s3://{self.dedicated_uri}/{table_name}/metadata/{staged_metadata_output}"                
                self._register_tables(table_name, dest_metadata_file)                
            print(f"Path rewrite complete for {fqn}")

    # ------------------------------------------------------------------
    # Step 6: Validate row counts (reads from DESTINATION catalog)
    # ------------------------------------------------------------------

    def _validate(self):
        """Log row counts for each migrated table as a sanity check."""
        print("Validating migrated tables in destination catalog...")

        for table_name in self.table_names:
            fqn = f"glue_catalog.{self.database}.{table_name}"
            print(f">>> Validating details for {fqn} ...")
            try:
                self.spark.sql(f"DESCRIBE EXTENDED {fqn}").show(truncate=False)
                self.spark.sql(f"SELECT * from {fqn}.snapshots").show(truncate=False)
                df = self.spark.sql(f"SELECT count(*) AS cnt FROM {fqn}")
                count = df.collect()[0]["cnt"]
                print(f"Table {fqn} -> row count: {count}")
            except Exception as ex:
                print(f"[ERROR][Could not get table count] {ex}")

    def run(self):
        """Execute the full cross-region migration pipeline."""
        if len(self.table_names) == 0:
            self.table_names = self._get_all_tables()
        print(f"Starting Iceberg migration")
        print(f"  Source: {self.shared_uri}, Database: {self.database}")
        print(f"  Destination: {self.dedicated_uri}")
        print(f"  Tables: {self.table_names}")
        
        self._rewrite_table_path()        
        self._validate()

        self.job.commit()
        print("Cross-region migration completed successfully.")


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------

if __name__ == "__main__":
    required_args = [
        "JOB_NAME", "REGION", "DATABASE", "GLUE_ENDPOINT", 
        "DEDICATED_BUCKET", "SHARED_BUCKET", "DEDICATED_PREFIX", "SHARED_PREFIX",
        "DRYRUN",
    ]
    optional_args = ["TABLES"]
    resolved = getResolvedOptions(sys.argv, required_args)
    for opt in optional_args:
        if f"--{opt}" in sys.argv:
            resolved.update(getResolvedOptions(sys.argv, [opt]))

    print(f"INPUT ARGS: {resolved}")
    migration = IcebergMigrationJob(resolved)
    migration.run()
