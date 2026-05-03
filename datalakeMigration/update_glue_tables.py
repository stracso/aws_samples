"""
Update Glue Catalog table locations for Iceberg datalake migration.

Iterates all tables in a Glue database, replaces old bucket references
with the new bucket in every string field, and updates the table definition.
"""

import argparse
import json
import boto3


# Fields that Glue returns on get_table but rejects on update_table
FIELDS_TO_STRIP = [
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


def replace_strings(obj, old: str, new: str):
    """Recursively walk a JSON-like structure and replace old with new in all strings."""
    if isinstance(obj, str):
        return obj.replace(old, new)
    if isinstance(obj, list):
        return [replace_strings(item, old, new) for item in obj]
    if isinstance(obj, dict):
        return {k: replace_strings(v, old, new) for k, v in obj.items()}
    return obj


def strip_read_only_fields(table_def: dict) -> dict:
    """Remove fields that are not accepted by update_table."""
    for field in FIELDS_TO_STRIP:
        table_def.pop(field, None)
    return table_def


def get_all_tables(glue_client, database_name: str) -> list[str]:
    """Paginate through all tables in a Glue database."""
    table_names = []
    paginator = glue_client.get_paginator("get_tables")
    for page in paginator.paginate(DatabaseName=database_name):
        for table in page["TableList"]:
            table_names.append(table["Name"])
    return table_names


def migrate_table(
    glue_client, database_name: str, table_name: str, old_prefix: str, new_prefix: str, dry_run: bool
) -> dict:
    """Fetch a table definition, patch bucket references, and update it in Glue."""
    response = glue_client.get_table(DatabaseName=database_name, Name=table_name)
    table_def = response["Table"]

    table_input = strip_read_only_fields(table_def)
    table_input = replace_strings(table_input, old_prefix, new_prefix)

    if dry_run:
        print(f"  [DRY RUN] Would update {table_name}")
        print(f"    Location: {table_input.get('StorageDescriptor', {}).get('Location', 'N/A')}")
        metadata_loc = table_input.get("Parameters", {}).get("metadata_location", "N/A")
        print(f"    Metadata: {metadata_loc}")
    else:
        glue_client.update_table(DatabaseName=database_name, TableInput=table_input)
        print(f"  Updated: {table_name}")

    return table_input


def main():
    parser = argparse.ArgumentParser(description="Migrate Glue Iceberg table locations between S3 buckets.")
    parser.add_argument("--database", required=True, help="Glue database name")
    parser.add_argument("--shared-bucket", required=True, help="Source bucket name (without s3:// prefix)")
    parser.add_argument("--dedicated-bucket", required=True, help="Destination bucket name (without s3:// prefix)")
    parser.add_argument("--tables", nargs="*", help="Specific table names to migrate (default: all tables)")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without updating Glue")
    parser.add_argument("--profile", help="AWS CLI profile name")
    parser.add_argument("--region", help="AWS region")
    args = parser.parse_args()

    session_kwargs = {}
    if args.profile:
        session_kwargs["profile_name"] = args.profile
    if args.region:
        session_kwargs["region_name"] = args.region

    session = boto3.Session(**session_kwargs)
    glue_client = session.client("glue")

    old_prefix = f"s3://{args.shared_bucket}"
    new_prefix = f"s3://{args.dedicated_bucket}"

    if args.tables:
        table_names = args.tables
    else:
        table_names = get_all_tables(glue_client, args.database)

    print(f"Database: {args.database}")
    print(f"Migrating: {old_prefix} -> {new_prefix}")
    print(f"Tables:    {len(table_names)} found")
    if args.dry_run:
        print("Mode:      DRY RUN\n")
    else:
        print()

    succeeded = []
    failed = []

    for table_name in table_names:
        print(f"Processing: {table_name}")
        try:
            migrate_table(glue_client, args.database, table_name, old_prefix, new_prefix, args.dry_run)
            succeeded.append(table_name)
        except Exception as e:
            print(f"  FAILED: {table_name} — {e}")
            failed.append(table_name)

    print(f"\nDone. Succeeded: {len(succeeded)}, Failed: {len(failed)}")
    if failed:
        print(f"Failed tables: {', '.join(failed)}")


if __name__ == "__main__":
    main()
