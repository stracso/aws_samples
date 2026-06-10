"""
Iceberg Datalake Migration Glue Job (Cross-Region)

Migrates Iceberg tables from a source bucket in one region to a dedicated bucket
in another region by:
1. Reading metadata from the source Glue Catalog (SOURCE_REGION)
2. Ensuring the database exists in the destination Glue Catalog (DEST_REGION)
3. Dropping and re-registering tables pointing to the new S3 location
4. Rewriting table paths in metadata (manifests) to point to the new bucket
   using Iceberg 1.10's rewrite_table_path() — no data files are rewritten
5. Cleaning up old snapshots and orphan files
6. Validating row counts post-migration

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
    """Orchestrates the cross-region migration of Iceberg tables to a dedicated S3 bucket."""
    def __init__(self, args: dict):
        self.source_region = args.get("SOURCE_REGION", "us-west-2")
        self.dest_region = args.get("DEST_REGION", "us-east-1")
        self.source_glue_endpoint = args.get(
            "SOURCE_GLUE_ENDPOINT", f"https://glue.{self.source_region}.amazonaws.com"
        )
        self.dest_glue_endpoint = args.get(
            "DEST_GLUE_ENDPOINT", f"https://glue.{self.dest_region}.amazonaws.com"
        )

        self.database = args["DATABASE"]
        self.table_names = []
        if 'TABLES' in args and len(args['TABLES']) > 0:
            self.table_names = [t.strip() for t in args["TABLES"].split(",")]
        self.shared_bucket = args["SHARED_BUCKET"]
        self.shared_uri = f"{self.shared_bucket}/{args['SHARED_PREFIX']}"
        self.dedicated_bucket = args["DEDICATED_BUCKET"]
        self.dedicated_uri = f"{self.dedicated_bucket}/{args['DEDICATED_PREFIX']}"
        self.retain_snapshots = "1"
        self.skip_cleanup = args.get("SKIP_CLEANUP", "false").lower() == "true"
        self.dryrun = args.get("DRYRUN", "false").lower() == "true"

        # Spark catalog configured to point at the DESTINATION Glue region
        # s3.cross-region-access-enabled allows reading source data from a different region
        self.spark = (
            SparkSession.builder
            .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
            .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.sql.catalog.glue_catalog.s3.cross-region-access-enabled", "true")
            .config("spark.sql.catalog.glue_catalog.glue.region", self.dest_region)
            .config("spark.sql.catalog.glue_catalog.glue.endpoint", self.dest_glue_endpoint)
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
            .getOrCreate()
        )
        self.glue_context = GlueContext(self.spark.sparkContext)
        self.job = Job(self.glue_context)
        self.job.init(args.get("JOB_NAME", "iceberg_migration_job"), args)

        # Source Glue client — used to READ table metadata from the origin region
        self.source_glue_client = boto3.client(
            "glue", region_name=self.source_region, endpoint_url=self.source_glue_endpoint
        )
        # Destination Glue client — used to WRITE/UPDATE tables in the target region
        self.dest_glue_client = boto3.client(
            "glue", region_name=self.dest_region, endpoint_url=self.dest_glue_endpoint
        )

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
            return [self._replace_strings(item, old, new) for item in obj]
        if isinstance(obj, dict):
            return {k: self._replace_strings(v, old, new) for k, v in obj.items()}
        return obj

    def _strip_read_only_fields(self, table_def: dict) -> dict:
        """Remove fields that are not accepted by update_table."""
        for field in self.FIELDS_TO_STRIP:
            table_def.pop(field, None)
        return table_def

    def _get_all_tables(self) -> list[str]:
        """Paginate through all tables in the SOURCE Glue database."""
        print("Fetching list of table names from source database...")
        table_names = []
        paginator = self.source_glue_client.get_paginator("get_tables")
        for page in paginator.paginate(DatabaseName=self.database):
            for table in page["TableList"]:
                table_names.append(table["Name"])
        return table_names

    def _migrate_table(self, table_name: str, old_prefix: str, new_prefix: str, dry_run: bool) -> dict:
        """Fetch a table definition from SOURCE, patch bucket references, and write to DESTINATION."""
        response = self.source_glue_client.get_table(DatabaseName=self.database, Name=table_name)
        table_def = response["Table"]

        table_input = self._strip_read_only_fields(table_def)
        # Replace all references to old bucket/prefix with new ones
        table_input["StorageDescriptor"] = self._replace_strings(
            table_input["StorageDescriptor"], old_prefix, new_prefix
        )
        # Also patch metadata_location in Parameters if present
        if "Parameters" in table_input:
            table_input["Parameters"] = self._replace_strings(
                table_input["Parameters"], old_prefix, new_prefix
            )

        if dry_run:
            print(f"  [DRY RUN] Would create/update {table_name} in {self.dest_region}")
            print(f"    Location: {table_input.get('StorageDescriptor', {}).get('Location', 'N/A')}")
            metadata_loc = table_input.get("Parameters", {}).get("metadata_location", "N/A")
            print(f"    Metadata: {metadata_loc}")
        else:
            # Use create_table if it doesn't exist yet, otherwise update
            try:
                self.dest_glue_client.get_table(DatabaseName=self.database, Name=table_name)
                self.dest_glue_client.update_table(DatabaseName=self.database, TableInput=table_input)
                print(f"  Updated in destination: {table_name}")
            except self.dest_glue_client.exceptions.EntityNotFoundException:
                self.dest_glue_client.create_table(DatabaseName=self.database, TableInput=table_input)
                print(f"  Created in destination: {table_name}")

        return table_input

    # ------------------------------------------------------------------
    # Step 0: Ensure database exists in destination
    # ------------------------------------------------------------------

    def _ensure_database_exists(self):
        """Create the database in the destination region if it doesn't exist."""
        try:
            self.dest_glue_client.get_database(Name=self.database)
            print(f"Database '{self.database}' already exists in {self.dest_region}.")
        except self.dest_glue_client.exceptions.EntityNotFoundException:
            print(f"Database '{self.database}' not found in {self.dest_region}, creating...")
            if not self.dryrun:
                self.dest_glue_client.create_database(
                    DatabaseInput={
                        "Name": self.database,
                        "Description": f"Migrated from {self.source_region}",
                    }
                )
                print(f"Database '{self.database}' created in {self.dest_region}.")
            else:
                print(f"  [DRY RUN] Would create database '{self.database}' in {self.dest_region}")

    # ------------------------------------------------------------------
    # Step 1: Resolve latest metadata file for each table (from SOURCE)
    # ------------------------------------------------------------------

    def _resolve_metadata_files(self):
        """Retrieve the latest metadata file location from the SOURCE Glue Catalog."""
        print(f"Resolving metadata files for {len(self.table_names)} table(s) from {self.source_region}...")

        for table_name in self.table_names:
            response = self.source_glue_client.get_table(
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
    # Step 2: Drop and re-register tables at the new location (in DESTINATION)
    # ------------------------------------------------------------------

    def _verify_metadata_in_destination(self):
        """Verify that metadata files exist in the destination bucket before registration."""
        print("Verifying metadata files exist in destination bucket...")
        s3_client = boto3.client("s3", region_name=self.dest_region)
        missing = []
        for table_name in self.table_names:
            latest_metadata_file = self.metadata_files[table_name]
            key = f"{self.dedicated_uri.split('/', 1)[1]}/{table_name}/metadata/{latest_metadata_file}"
            bucket = self.dedicated_bucket
            try:
                s3_client.head_object(Bucket=bucket, Key=key)
                print(f"  ✓ {table_name}: metadata file found at s3://{bucket}/{key}")
            except Exception:
                print(f"  ✗ {table_name}: metadata file NOT FOUND at s3://{bucket}/{key}")
                missing.append(table_name)

        if missing and not self.dryrun:
            raise RuntimeError(
                f"Metadata files missing in destination bucket for tables: {missing}. "
                f"Ensure S3 data (including /metadata/ folder) has been copied to "
                f"s3://{self.dedicated_uri}/ before running migration."
            )

    def _register_tables(self):
        """Register tables in destination catalog from SOURCE metadata location.
        
        We register using the original metadata file at the source bucket path.
        This is the known-good metadata. rewrite_table_path() will then produce
        new metadata pointing to the destination paths.
        """
        print("Registering tables in destination catalog from source metadata...")
        dryrun_prefix = "[DRYRUN] " if self.dryrun else ""

        for table_name in self.table_names:
            latest_metadata_file = self.metadata_files[table_name]
            # Use the SOURCE metadata file location (original, known-good)
            metadata_path = (
                f"s3://{self.shared_uri}/"
                f"{table_name}/metadata/{latest_metadata_file}"
            )

            fqn = f"glue_catalog.{self.database}.{table_name}"

            # Use Glue API to delete the table entry without Spark trying to read metadata
            print(f"{dryrun_prefix}Removing table {table_name} from destination catalog (if exists)...")
            if not self.dryrun:
                try:
                    self.dest_glue_client.delete_table(
                        DatabaseName=self.database, Name=table_name
                    )
                    print(f"  Deleted existing table entry: {table_name}")
                except self.dest_glue_client.exceptions.EntityNotFoundException:
                    print(f"  Table {table_name} does not exist yet, skipping delete.")

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
    # Step 3: Rewrite table paths (metadata-only, no data rewrite)
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
        s3_client = boto3.client("s3", region_name=self.dest_region)

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

                # Step 2: Copy staged metadata to the target table's metadata folder
                # The procedure returns just the filename, not a full S3 URI.
                # Construct the full path using the known staging location.
                if staged_metadata_output.startswith("s3://"):
                    staged_metadata_path = staged_metadata_output
                    staged_metadata_filename = os.path.basename(staged_metadata_path)
                else:
                    # It's just a filename — build full path from staging location
                    staged_metadata_filename = staged_metadata_output
                    staged_metadata_path = f"{staging_location}/metadata/{staged_metadata_filename}"

                dest_metadata_path = f"s3://{self.dedicated_uri}/{table_name}/metadata/{staged_metadata_filename}"

                # Parse bucket and key from staged path
                staged_no_scheme = staged_metadata_path.replace("s3://", "")
                staged_bucket = staged_no_scheme.split("/", 1)[0]
                staged_key = staged_no_scheme.split("/", 1)[1]

                # Parse bucket and key for destination
                dest_no_scheme = dest_metadata_path.replace("s3://", "")
                dest_bucket = dest_no_scheme.split("/", 1)[0]
                dest_key = dest_no_scheme.split("/", 1)[1]

                print(f"  Copying metadata: {staged_metadata_path} -> {dest_metadata_path}")
                s3_client.copy_object(
                    Bucket=dest_bucket,
                    Key=dest_key,
                    CopySource={"Bucket": staged_bucket, "Key": staged_key},
                )

                # Also copy any manifest files from staging
                staging_no_scheme = staging_location.replace("s3://", "")
                staging_bucket = staging_no_scheme.split("/", 1)[0]
                staging_prefix = staging_no_scheme.split("/", 1)[1]

                # List all files in staging and copy manifests to the target metadata folder
                paginator = s3_client.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=staging_bucket, Prefix=staging_prefix):
                    for obj in page.get("Contents", []):
                        staged_obj_key = obj["Key"]
                        # Skip the metadata.json itself (already copied above)
                        if staged_obj_key == staged_key:
                            continue
                        # Determine relative path from staging and place under table metadata
                        relative_path = staged_obj_key[len(staging_prefix):].lstrip("/")
                        target_key = f"{self.dedicated_uri.split('/', 1)[1]}/{table_name}/metadata/{relative_path}"
                        print(f"  Copying staged file: {relative_path}")
                        s3_client.copy_object(
                            Bucket=dest_bucket,
                            Key=target_key,
                            CopySource={"Bucket": staging_bucket, "Key": staged_obj_key},
                        )

                # Step 3: Remove old catalog entry and register with new metadata
                print(f"  Re-registering {fqn} with rewritten metadata at {dest_metadata_path}...")
                try:
                    self.dest_glue_client.delete_table(
                        DatabaseName=self.database, Name=table_name
                    )
                except self.dest_glue_client.exceptions.EntityNotFoundException:
                    pass

                self.spark.sql(
                    f"""
                    CALL glue_catalog.system.register_table(
                        table => '{self.database}.{table_name}',
                        metadata_file => '{dest_metadata_path}'
                    )
                    """
                )
                print(f"  Registered {fqn} with metadata at {dest_metadata_path}")

            print(f"Path rewrite complete for {fqn}")

    # ------------------------------------------------------------------
    # Step 4: Clean up old snapshots and orphan files
    # ------------------------------------------------------------------

    def _cleanup_metadata(self):
        """Expire old snapshots and remove orphan files."""
        dryrun_prefix = "[DRYRUN] " if self.dryrun else ""
        print(f"Cleaning up metadata (retain_snapshots={self.retain_snapshots})...")

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
    # Step 5: Update Glue Catalog table locations (in DESTINATION)
    # ------------------------------------------------------------------

    def _update_table_location(self):
        """Fetch table defs from SOURCE, patch locations, and write to DESTINATION catalog."""
        succeeded = []
        failed = []
        old_prefix = f"s3://{self.shared_uri}"
        new_prefix = f"s3://{self.dedicated_uri}"
        print("---- Updating tables location (cross-region) ---")
        print(f"Source: {self.source_region}, Destination: {self.dest_region}")
        print(f"Database: {self.database}, {len(self.table_names)} tables")
        print(f"From: {old_prefix}")
        print(f"To:   {new_prefix}")

        for table_name in self.table_names:
            print(f"Processing: {table_name}")
            try:
                self._migrate_table(table_name, old_prefix, new_prefix, self.dryrun)
                succeeded.append(table_name)
            except Exception as e:
                print(f"[ERROR]  FAILED: {table_name} — {e}")
                failed.append(table_name)

        print(f"DONE. Succeeded: {len(succeeded)}, Failed: {len(failed)}")
        if failed:
            print(f"Failed tables: {', '.join(failed)}")

    # ------------------------------------------------------------------
    # Step 6: Validate row counts (reads from DESTINATION catalog)
    # ------------------------------------------------------------------

    def _validate(self):
        """Log row counts for each migrated table as a sanity check."""
        print("Validating migrated tables in destination catalog...")

        for table_name in self.table_names:
            fqn = f"glue_catalog.{self.database}.{table_name}"
            try:
                df = self.spark.sql(f"SELECT count(*) AS cnt FROM {fqn}")
                count = df.collect()[0]["cnt"]
                print(f"Table {fqn} -> row count: {count}")
            except Exception as ex:
                print(f"[ERROR][Could not get table count] {ex}")

    def run(self):
        """Execute the full cross-region migration pipeline."""
        if len(self.table_names) == 0:
            self.table_names = self._get_all_tables()
        print(f"Starting cross-region Iceberg migration")
        print(f"  Source: {self.source_region}, Database: {self.database}")
        print(f"  Destination: {self.dest_region}, Bucket: {self.dedicated_bucket}")
        print(f"  Tables: {self.table_names}")

        self._ensure_database_exists()
        self._resolve_metadata_files()
        self._verify_metadata_in_destination()
        self._register_tables()
        self._rewrite_table_path()

        if not self.skip_cleanup:
            self._cleanup_metadata()

        self._validate()

        self.job.commit()
        print("Cross-region migration completed successfully.")


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------

if __name__ == "__main__":
    required_args = [
        "JOB_NAME", "SOURCE_REGION", "DEST_REGION", "DATABASE",
        "DEDICATED_BUCKET", "SHARED_BUCKET", "DEDICATED_PREFIX", "SHARED_PREFIX",
        "SKIP_CLEANUP", "DRYRUN",
    ]
    optional_args = ["TABLES", "SOURCE_GLUE_ENDPOINT", "DEST_GLUE_ENDPOINT"]
    resolved = getResolvedOptions(sys.argv, required_args)
    for opt in optional_args:
        if f"--{opt}" in sys.argv:
            resolved.update(getResolvedOptions(sys.argv, [opt]))

    print(f"INPUT ARGS: {resolved}")
    migration = IcebergMigrationJob(resolved)
    migration.run()
