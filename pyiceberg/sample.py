
import boto3
from pyiceberg.catalog.glue import GlueCatalog
import os

# Step 1: Configure boto3 to use Glue VPC endpoint
# Replace with your actual VPC endpoint DNS name
glue_vpc_endpoint = "https://vpce-xxxxx-xxxxx.glue.us-west-2.vpce.amazonaws.com"

# Create a custom boto3 session
session = boto3.Session(region_name='us-west-2')

# Create Glue client with VPC endpoint
glue_client = session.client(
    'glue',
    endpoint_url=glue_vpc_endpoint
)

# Verify the connection works
try:
    response = glue_client.get_databases()
    print(f"Successfully connected via VPC endpoint. Found {len(response['DatabaseList'])} databases.")
except Exception as e:
    print(f"Error connecting to Glue via VPC endpoint: {e}")

# Step 2: Create PyIceberg GlueCatalog using the custom boto3 session
catalog = GlueCatalog(
    name="glue_catalog",
    **{
        # Pass the custom boto3 session that uses VPC endpoint
        "glue.session": session,
        "glue.endpoint-url": glue_vpc_endpoint,
        
        # S3 configuration
        "s3.region": "us-west-2",
        "warehouse": "s3://jpmc-iceberg-warehouse-038676220235-us-west-2/warehouse/",
        
        # Optional: Use S3 VPC endpoint as well
        "s3.endpoint": "https://bucket.vpce-xxxxx-xxxxx.s3.us-west-2.vpce.amazonaws.com"
    }
)

# Step 3: Get table metadata
namespace = "jpmc_demo_warehouse"
table_name = "main_eqtbl"

try:
    # Load the table
    table = catalog.load_table(f"{namespace}.{table_name}")
    
    # Get comprehensive table metadata
    print(f"
=== Table Metadata ===")
    print(f"Table: {table.name()}")
    print(f"Location: {table.location()}")
    print(f"Format Version: {table.format_version}")
    
    # Schema information
    print(f"
=== Schema ===")
    for field in table.schema().fields:
        print(f"  {field.name}: {field.field_type} (required={field.required})")
    
    # Snapshot information
    if table.current_snapshot():
        snapshot = table.current_snapshot()
        print(f"
=== Current Snapshot ===")
        print(f"Snapshot ID: {snapshot.snapshot_id}")
        print(f"Timestamp: {snapshot.timestamp_ms}")
        print(f"Summary: {snapshot.summary}")
    else:
        print("
No snapshots available")
    
    # Table properties
    print(f"
=== Table Properties ===")
    for key, value in table.properties.items():
        print(f"  {key}: {value}")
    
    # Partition spec
    print(f"
=== Partition Spec ===")
    if table.spec().is_unpartitioned():
        print("  Table is unpartitioned")
    else:
        for field in table.spec().fields:
            print(f"  {field}")
    
    # Sort order
    print(f"
=== Sort Order ===")
    if table.sort_order().is_unsorted:
        print("  Table has no sort order")
    else:
        for field in table.sort_order().fields:
            print(f"  {field}")
    
    # Metadata location
    print(f"
=== Metadata ===")
    print(f"Metadata Location: {table.metadata_location}")
    
    # Get table statistics
    print(f"
=== Statistics ===")
    if table.current_snapshot():
        print(f"Total Records: {table.current_snapshot().summary.get('total-records', 'N/A')}")
        print(f"Total Data Files: {table.current_snapshot().summary.get('total-data-files', 'N/A')}")
        print(f"Total Delete Files: {table.current_snapshot().summary.get('total-delete-files', 'N/A')}")
    
except Exception as e:
    print(f"Error loading table: {e}")
    import traceback
    traceback.print_exc()

# Optional: List all tables in the namespace
try:
    print(f"
=== Tables in {namespace} ===")
    tables = catalog.list_tables(namespace)
    for tbl in tables:
        print(f"  {tbl}")
except Exception as e:
    print(f"Error listing tables: {e}")

