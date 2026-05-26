"""
Athena JDBC Connection Script
==============================
Connects to Amazon Athena using the Athena JDBC 3.x driver via JayDeBeApi.
Uses AWS credentials from ~/.aws/credentials (DefaultChain provider).

Prerequisites:
    1. Java 8+ installed (JAVA_HOME set)
    2. Download the Athena JDBC 3.x uber jar from:
       https://docs.aws.amazon.com/athena/latest/ug/jdbc-v3-driver.html
       Place it in this directory as 'athena-jdbc-3.x.jar' (or update JDBC_JAR_PATH below)
    3. Install Python dependencies:
       pip install jaydebeapi

Usage:
    python athena_jdbc_connect.py
"""

import os
import sys
import jaydebeapi

# ---------------------------------------------------------------------------
# Configuration – update these values for your environment
# ---------------------------------------------------------------------------

# Path to the Athena JDBC uber jar
JDBC_JAR_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "athena-jdbc-3.7.0-with-dependencies.jar")

# Athena JDBC 3.x driver class
JDBC_DRIVER_CLASS = "com.amazon.athena.jdbc.AthenaDriver"

# Connection parameters
REGION = "us-west-2"                    # AWS region where Athena is available
WORKGROUP = "primary"                   # Athena workgroup
CATALOG = "AwsDataCatalog"             # Glue Data Catalog name
DATABASE = "ATHENADB"                    # Default database to query
OUTPUT_LOCATION = "s3://ATHENA_RESULTS_BUCKET/ "  # S3 path for query results

# Credentials provider – DefaultChain reads from env vars, ~/.aws/credentials, instance profile, etc.
CREDENTIALS_PROVIDER = "DefaultChain"

# Optional: specify an AWS profile from ~/.aws/credentials
# If set, the driver uses ProfileCredentialsProvider instead of DefaultChain
AWS_PROFILE = None  # e.g. "my-profile"

# Optional: VPC endpoint for private connectivity (PrivateLink)
# Set this to your Athena VPC endpoint URL to route traffic through your VPC
# instead of the public Athena endpoint.
# Format: https://vpce-XXXXXXXXX.athena.REGION.vpce.amazonaws.com
ATHENA_VPC_ENDPOINT = None  # e.g. "https://vpce-0abc123def456.athena.us-west-2.vpce.amazonaws.com"


def build_jdbc_url():
    """Build the JDBC connection URL with inline parameters."""
    params = {
        "Region": REGION,
        "Workgroup": WORKGROUP,
        "Catalog": CATALOG,
        "Database": DATABASE,
        "OutputLocation": OUTPUT_LOCATION,
    }

    if AWS_PROFILE:
        params["CredentialsProvider"] = "ProfileCredentials"
        params["ProfileName"] = AWS_PROFILE
    else:
        params["CredentialsProvider"] = CREDENTIALS_PROVIDER

    # VPC endpoint – routes Athena API calls through PrivateLink
    if ATHENA_VPC_ENDPOINT:
        params["EndpointOverride"] = ATHENA_VPC_ENDPOINT

    # Build URL: jdbc:athena://key=value;key=value;
    param_str = ";".join(f"{k}={v}" for k, v in params.items()) + ";"
    return f"jdbc:athena://{param_str}"


def run_query(connection, query):
    """Execute a query and print results."""
    print(f"\n{'='*60}")
    print(f"Executing: {query}")
    print(f"{'='*60}")

    cursor = connection.cursor()
    cursor.execute(query)

    # Print column headers
    columns = [desc[0] for desc in cursor.description]
    print(f"\nColumns: {columns}")
    print("-" * 60)

    # Print rows
    rows = cursor.fetchall()
    for row in rows:
        print(row)

    print(f"\nTotal rows: {len(rows)}")
    cursor.close()
    return rows


def main():
    # Validate jar exists
    if not os.path.isfile(JDBC_JAR_PATH):
        print(f"ERROR: JDBC jar not found at: {JDBC_JAR_PATH}")
        print()
        print("Download the Athena JDBC 3.x uber jar from:")
        print("  https://docs.aws.amazon.com/athena/latest/ug/jdbc-v3-driver.html")
        print()
        print(f"Place it in: {os.path.dirname(os.path.abspath(__file__))}/")
        print("Or update JDBC_JAR_PATH in this script.")
        sys.exit(1)

    jdbc_url = build_jdbc_url()
    print(f"Connecting to Athena...")
    print(f"  Region:    {REGION}")
    print(f"  Workgroup: {WORKGROUP}")
    print(f"  Database:  {DATABASE}")
    print(f"  Profile:   {AWS_PROFILE or '(DefaultChain)'}")
    print(f"  VPC Endpoint: {ATHENA_VPC_ENDPOINT or '(public)'}")
    print(f"  JDBC URL:  {jdbc_url}")

    try:
        # Connect using JayDeBeApi
        connection = jaydebeapi.connect(
            JDBC_DRIVER_CLASS,
            jdbc_url,
            {},  # No additional properties needed – all params are in the URL
            JDBC_JAR_PATH,
        )
        print("\nConnected successfully!")

        # Run a sample query – list databases
        run_query(connection, "SHOW DATABASES")

        # Run another sample query – you can replace this with your own
        run_query(connection, f"SHOW TABLES IN {DATABASE}")
        
        run_query(connection, f"select * from {DATABASE}.orders_pftest_small_datepart limit 10")
        run_query(connection, f"select count(1) from {DATABASE}.orders_pftest_small_datepart limit 10")
        connection.close()
        print("\nConnection closed.")

    except Exception as e:
        print(f"\nERROR: Failed to connect to Athena: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
