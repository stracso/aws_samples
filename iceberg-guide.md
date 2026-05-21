# Apache Iceberg Tables: A Comprehensive Guide

## Introduction

Apache Iceberg is an open table format for large analytic datasets. It provides a metadata layer that sits between compute engines and storage, enabling reliable data management at petabyte scale.

### Architecture

Iceberg's architecture consists of three layers:

1. **Catalog** — Tracks the current metadata pointer for each table (e.g., Hive Metastore, AWS Glue, Nessie, REST catalog)
2. **Metadata Layer** — Contains metadata files, manifest lists, and manifest files that track all data files and their statistics
3. **Data Layer** — The actual data files stored in formats like Parquet, ORC, or Avro

### Key Benefits

- **Schema Evolution** — Add, drop, rename, or reorder columns without rewriting data
- **Partition Evolution** — Change partitioning strategy without rewriting existing data
- **Time Travel** — Query historical snapshots by timestamp or snapshot ID
- **ACID Transactions** — Serializable isolation using optimistic concurrency control
- **Hidden Partitioning** — Users don't need to know the partition layout to write efficient queries
- **File-Level Statistics** — Min/max values per column enable efficient file pruning

---

## Compute Engine Configuration

Iceberg supports multiple compute engines. Below are configuration details for each, including required dependencies and catalog settings.

### Required JARs Summary

| Engine | Dependency |
|--------|-----------|
| Spark 3.5 | `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2` |
| Flink 1.18 | `org.apache.iceberg:iceberg-flink-runtime-1.18:1.5.2` |
| Hive | `org.apache.iceberg:iceberg-hive-runtime:1.5.2` |
| Trino | Built-in (bundled connector) |
| Presto | Built-in (bundled connector) |

### Apache Spark (Primary)

Spark is the most fully-featured engine for Iceberg. Configure via `spark-defaults.conf` or at session level:

```properties
# Required JARs (add to spark.jars.packages)
spark.jars.packages = org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2

# Catalog configuration
spark.sql.catalog.my_catalog = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.my_catalog.type = hive
spark.sql.catalog.my_catalog.uri = thrift://metastore-host:9083

# For Hadoop catalog (file-based)
spark.sql.catalog.hadoop_catalog = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.hadoop_catalog.type = hadoop
spark.sql.catalog.hadoop_catalog.warehouse = s3://my-bucket/warehouse

# For REST catalog
spark.sql.catalog.rest_catalog = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.rest_catalog.type = rest
spark.sql.catalog.rest_catalog.uri = http://rest-catalog-host:8181

# For AWS Glue catalog
spark.sql.catalog.glue_catalog = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.glue_catalog.catalog-impl = org.apache.iceberg.aws.glue.GlueCatalog
spark.sql.catalog.glue_catalog.warehouse = s3://my-bucket/warehouse

# Recommended settings
spark.sql.extensions = org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.defaultCatalog = my_catalog
```

### Apache Flink

```properties
# Flink SQL connector dependency
# Add: org.apache.iceberg:iceberg-flink-runtime-1.18:1.5.2

# In Flink SQL:
CREATE CATALOG iceberg_catalog WITH (
  'type' = 'iceberg',
  'catalog-type' = 'hive',
  'uri' = 'thrift://metastore-host:9083',
  'warehouse' = 's3://my-bucket/warehouse'
);
```

### Trino

```properties
# connector.name=iceberg in catalog properties file (e.g., /etc/trino/catalog/iceberg.properties)
connector.name = iceberg
iceberg.catalog.type = hive_metastore
hive.metastore.uri = thrift://metastore-host:9083

# For Glue catalog
connector.name = iceberg
iceberg.catalog.type = glue
```

### Presto

```properties
# In catalog properties file
connector.name = iceberg
hive.metastore.uri = thrift://metastore-host:9083
iceberg.catalog.type = hive
```

### Apache Hive

```properties
# Hive 4.0+ has native Iceberg support
# For earlier versions, add iceberg-hive-runtime JAR to Hive classpath

SET iceberg.catalog.my_catalog.type = hive;
SET iceberg.catalog.my_catalog.uri = thrift://metastore-host:9083;

-- Register an Iceberg table in Hive
ALTER TABLE db.table SET TBLPROPERTIES ('storage_handler'='org.apache.iceberg.mr.hive.HiveIcebergStorageHandler');
```

### Engine Feature Comparison

| Feature | Spark | Flink | Trino | Presto | Hive |
|---------|-------|-------|-------|--------|------|
| Read | ✅ | ✅ | ✅ | ✅ | ✅ |
| Write (append) | ✅ | ✅ | ✅ | ✅ | ✅ |
| Write (overwrite) | ✅ | ✅ | ✅ | ✅ | ❌ |
| MERGE INTO | ✅ | ❌ | ✅ | ❌ | ❌ |
| Schema evolution | ✅ | ✅ | ✅ | ✅ | ✅ |
| Time travel | ✅ | ✅ | ✅ | ✅ | ✅ |
| Streaming write | ✅ | ✅ | ❌ | ❌ | ❌ |


---

## Table Creation

### CREATE TABLE Syntax

```sql
CREATE TABLE catalog.db.my_table (
    id BIGINT,
    name STRING,
    ts TIMESTAMP,
    category STRING,
    amount DECIMAL(10, 2)
)
USING iceberg
PARTITIONED BY (bucket(16, id), days(ts), truncate(10, category))
WRITE ORDERED BY (category, ts)
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.target-file-size-bytes' = '536870912',
    'write.parquet.compression-codec' = 'zstd'
);
```

### Partition Transforms

Iceberg supports hidden partitioning via transform functions — no need to add partition columns to your data:

| Transform | Description | Example |
|-----------|-------------|---------|
| `year(ts)` | Partition by year extracted from timestamp | `PARTITIONED BY (year(event_time))` |
| `month(ts)` | Partition by year-month | `PARTITIONED BY (month(event_time))` |
| `day(ts)` | Partition by date | `PARTITIONED BY (day(event_time))` |
| `hour(ts)` | Partition by hour | `PARTITIONED BY (hour(event_time))` |
| `bucket(N, col)` | Hash into N buckets | `PARTITIONED BY (bucket(16, user_id))` |
| `truncate(W, col)` | Truncate string/int to width W | `PARTITIONED BY (truncate(10, city))` |
| `identity(col)` | Partition by exact value | `PARTITIONED BY (identity(region))` |

### Sort Orders (WRITE ORDERED BY)

Sort orders control how data is organized within files, improving query performance through better data clustering:

```sql
CREATE TABLE catalog.db.events (
    event_id BIGINT,
    event_type STRING,
    event_time TIMESTAMP,
    user_id BIGINT
)
USING iceberg
PARTITIONED BY (day(event_time))
WRITE ORDERED BY (event_type, user_id);
```

### Key Table Properties

| Property | Default | Description |
|----------|---------|-------------|
| `write.format.default` | `parquet` | File format: parquet, orc, or avro |
| `write.target-file-size-bytes` | `536870912` (512 MB) | Target size for data files |
| `write.parquet.compression-codec` | `gzip` | Compression: gzip, snappy, zstd, lz4 |
| `write.metadata.delete-after-commit.enabled` | `false` | Auto-clean old metadata files |
| `write.metadata.previous-versions-max` | `100` | Max metadata versions to keep |
| `read.split.target-size` | `134217728` (128 MB) | Target split size for reads |
| `write.distribution-mode` | `none` | Data distribution: none, hash, range |

### CREATE TABLE AS SELECT (CTAS)

```sql
CREATE TABLE catalog.db.new_table
USING iceberg
PARTITIONED BY (day(event_time))
TBLPROPERTIES ('write.format.default' = 'parquet')
AS SELECT * FROM source_table WHERE event_time > '2024-01-01';
```

### CREATE OR REPLACE TABLE

```sql
CREATE OR REPLACE TABLE catalog.db.my_table (
    id BIGINT,
    data STRING,
    ts TIMESTAMP
)
USING iceberg
PARTITIONED BY (day(ts));
```


---

## CRUD Operations and MERGE INTO

### INSERT INTO

Append rows to an Iceberg table:

```sql
-- Insert literal values
INSERT INTO catalog.db.events VALUES
    (1, 'click', TIMESTAMP '2024-06-01 10:00:00', 1001),
    (2, 'view', TIMESTAMP '2024-06-01 10:05:00', 1002);

-- Insert from a query
INSERT INTO catalog.db.events
SELECT event_id, event_type, event_time, user_id
FROM staging.raw_events
WHERE event_time >= '2024-06-01';
```

### INSERT OVERWRITE

Replace data in matching partitions (only affects partitions present in the source data):

```sql
-- Dynamic overwrite (default) — replaces only partitions that appear in the result set
INSERT OVERWRITE catalog.db.events
SELECT event_id, event_type, event_time, user_id
FROM staging.raw_events
WHERE event_time >= '2024-06-01' AND event_time < '2024-06-02';

-- Static overwrite — explicitly specify the partition to replace
INSERT OVERWRITE catalog.db.events
PARTITION (event_time_day = '2024-06-01')
SELECT event_id, event_type, event_time, user_id
FROM staging.corrected_events;
```

> **Note:** Set `spark.sql.iceberg.overwrite-mode` to `dynamic` (default) or `static` to control behavior.

### UPDATE

Modify existing rows in place:

```sql
-- Update specific rows
UPDATE catalog.db.events
SET event_type = 'purchase'
WHERE event_id = 1;

-- Update with expressions
UPDATE catalog.db.events
SET event_time = event_time + INTERVAL 1 HOUR
WHERE user_id = 1001 AND event_type = 'click';
```

### DELETE

Remove rows from the table:

```sql
-- Delete specific rows
DELETE FROM catalog.db.events
WHERE event_id = 2;

-- Delete with a subquery condition
DELETE FROM catalog.db.events
WHERE user_id IN (SELECT user_id FROM catalog.db.blocked_users);

-- Delete old data
DELETE FROM catalog.db.events
WHERE event_time < TIMESTAMP '2023-01-01 00:00:00';
```

### MERGE INTO

Perform upserts (insert or update) in a single atomic operation:

```sql
MERGE INTO catalog.db.events AS target
USING staging.new_events AS source
ON target.event_id = source.event_id
WHEN MATCHED AND source.event_type = 'delete' THEN
    DELETE
WHEN MATCHED THEN
    UPDATE SET
        event_type = source.event_type,
        event_time = source.event_time,
        user_id = source.user_id
WHEN NOT MATCHED THEN
    INSERT (event_id, event_type, event_time, user_id)
    VALUES (source.event_id, source.event_type, source.event_time, source.user_id);
```

#### MERGE INTO with Multiple MATCHED Clauses

```sql
MERGE INTO catalog.db.customers AS target
USING staging.customer_updates AS source
ON target.customer_id = source.customer_id
WHEN MATCHED AND source.is_deleted = true THEN
    DELETE
WHEN MATCHED AND source.updated_at > target.updated_at THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *;
```

### Copy-on-Write vs Merge-on-Read

Iceberg supports two strategies for row-level operations (UPDATE, DELETE, MERGE):

| Property | Copy-on-Write (default) | Merge-on-Read |
|----------|------------------------|---------------|
| Write cost | Higher (rewrites entire files) | Lower (writes delete files) |
| Read cost | Lower (no reconciliation) | Higher (applies deletes at read time) |
| Best for | Read-heavy workloads | Write-heavy workloads |

Configure per table:

```sql
ALTER TABLE catalog.db.events SET TBLPROPERTIES (
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);
```


---

## Schema Evolution

Iceberg supports in-place schema evolution — changes are metadata-only operations that never require rewriting existing data files.

### ADD COLUMN

```sql
-- Add a single column
ALTER TABLE catalog.db.my_table ADD COLUMN email STRING;

-- Add a nested column inside a struct
ALTER TABLE catalog.db.my_table ADD COLUMN address.zip STRING;

-- Add multiple columns
ALTER TABLE catalog.db.my_table ADD COLUMNS (
    phone STRING,
    verified BOOLEAN,
    score DOUBLE
);
```

### DROP COLUMN

```sql
ALTER TABLE catalog.db.my_table DROP COLUMN email;

-- Drop a nested column
ALTER TABLE catalog.db.my_table DROP COLUMN address.zip;
```

### RENAME COLUMN

```sql
ALTER TABLE catalog.db.my_table RENAME COLUMN name TO full_name;
```

### ALTER COLUMN (Type Promotion)

Iceberg allows safe type widening without rewriting data:

| From | To |
|------|-----|
| `INT` | `BIGINT` |
| `FLOAT` | `DOUBLE` |
| `DECIMAL(P, S)` | `DECIMAL(P', S)` where P' > P |

```sql
-- Widen int to bigint
ALTER TABLE catalog.db.my_table ALTER COLUMN id TYPE BIGINT;

-- Widen decimal precision
ALTER TABLE catalog.db.my_table ALTER COLUMN amount TYPE DECIMAL(12, 2);
```

### Reorder and Require Columns

```sql
-- Move column position
ALTER TABLE catalog.db.my_table ALTER COLUMN email AFTER name;
ALTER TABLE catalog.db.my_table ALTER COLUMN id FIRST;

-- Add NOT NULL constraint (only for new writes)
ALTER TABLE catalog.db.my_table ALTER COLUMN id SET NOT NULL;

-- Add a comment
ALTER TABLE catalog.db.my_table ALTER COLUMN id COMMENT 'Primary identifier';
```

### How It Works

Schema changes are tracked by unique column IDs (not names or positions). When reading data files written under an older schema:
- New columns return `NULL` for old files
- Dropped columns are simply ignored
- Renamed columns map by ID, so old data is still accessible
- Widened types are promoted at read time

---

## Partition Evolution

Iceberg allows changing a table's partition scheme without rewriting existing data. Old data remains in the old partition layout; new data uses the new layout. Queries spanning both layouts work transparently.

### ALTER TABLE SET PARTITION SPEC

```sql
-- Start with daily partitioning
CREATE TABLE catalog.db.events (
    event_id BIGINT,
    event_time TIMESTAMP,
    data STRING
)
USING iceberg
PARTITIONED BY (day(event_time));

-- Later, switch to hourly partitioning for higher granularity
ALTER TABLE catalog.db.events SET PARTITION SPEC (hour(event_time));

-- Or switch to a multi-field partition spec
ALTER TABLE catalog.db.events SET PARTITION SPEC (hour(event_time), bucket(8, event_id));
```

### Evolution Without Data Rewrite

```sql
-- Original table partitioned by month
CREATE TABLE catalog.db.orders (
    order_id BIGINT,
    order_date TIMESTAMP,
    customer_id BIGINT,
    total DECIMAL(10, 2)
)
USING iceberg
PARTITIONED BY (month(order_date));

-- After data volume grows, evolve to daily partitioning
ALTER TABLE catalog.db.orders SET PARTITION SPEC (day(order_date));

-- Existing monthly-partitioned files remain untouched
-- New writes use daily partitioning
-- Queries automatically handle both layouts via metadata
```

### Removing Partitioning

```sql
-- Remove all partitioning (new data written unpartitioned)
ALTER TABLE catalog.db.events SET PARTITION SPEC ();
```

### How It Works

- Each partition spec is versioned and stored in table metadata
- Data files reference the partition spec under which they were written
- The query planner uses metadata to prune files across all partition specs
- No background rewrite is needed — old and new layouts coexist

---

## Time Travel Queries

Iceberg maintains a history of table snapshots, enabling queries against any previous state of the table.

### Snapshot-Based Queries

Query a specific snapshot by its ID:

```sql
-- Spark SQL syntax
SELECT * FROM catalog.db.events VERSION AS OF 5678901234;

-- Alternative SYSTEM_VERSION syntax
SELECT * FROM catalog.db.events SYSTEM_VERSION AS OF 5678901234;
```

### Timestamp-Based Queries

Query the table as it existed at a specific point in time:

```sql
-- Spark SQL syntax
SELECT * FROM catalog.db.events TIMESTAMP AS OF '2024-01-15 10:00:00';

-- Alternative SYSTEM_TIME syntax
SELECT * FROM catalog.db.events SYSTEM_TIME AS OF '2024-01-15 10:00:00';

-- Using a timestamp expression
SELECT * FROM catalog.db.events TIMESTAMP AS OF current_timestamp() - INTERVAL 1 HOUR;
```

### Listing Snapshots via Metadata Tables

Iceberg exposes metadata tables to inspect snapshot history:

```sql
-- List all snapshots
SELECT * FROM catalog.db.events.snapshots;

-- View snapshot details: snapshot_id, parent_id, timestamp, operation, summary
SELECT snapshot_id, committed_at, operation, summary
FROM catalog.db.events.snapshots
ORDER BY committed_at DESC;

-- List snapshot history (linear ancestry)
SELECT * FROM catalog.db.events.history;

-- View manifest files for a specific snapshot
SELECT * FROM catalog.db.events.manifests;

-- View data files tracked by the table
SELECT file_path, file_format, record_count, file_size_in_bytes
FROM catalog.db.events.files;
```

### Rollback Procedures

Roll back the table to a previous state:

```sql
-- Rollback to a specific snapshot ID
CALL catalog.system.rollback_to_snapshot('db.events', 5678901234);

-- Rollback to a specific timestamp
CALL catalog.system.rollback_to_timestamp('db.events', TIMESTAMP '2024-01-15 10:00:00');
```

Rollback creates a new snapshot that points to the same data as the target snapshot — it does not delete any data files. Expired snapshots can still be cleaned up later via snapshot expiration.

### Incremental Reads (Between Snapshots)

Read only the changes between two snapshots:

```sql
-- View appended data between two snapshots
CALL catalog.system.create_changelog_view(
  table => 'db.events',
  options => map('start-snapshot-id', '123456', 'end-snapshot-id', '789012')
);

-- In Spark DataFrame API
spark.read.format("iceberg")
  .option("start-snapshot-id", "123456")
  .option("end-snapshot-id", "789012")
  .load("catalog.db.events");
```


---

## 7. Table Maintenance

Regular maintenance keeps Iceberg tables performant and storage costs under control.

### Compaction (Rewriting Data Files)

Small files degrade query performance. Compaction merges them into optimally-sized files.

```sql
-- Basic compaction
CALL catalog.system.rewrite_data_files('db.orders');

-- With target file size (512 MB)
CALL catalog.system.rewrite_data_files(
  table => 'db.orders',
  options => map('target-file-size-bytes', '536870912')
);

-- Compact only a specific partition
CALL catalog.system.rewrite_data_files(
  table => 'db.orders',
  where => 'order_date >= date "2024-01-01"'
);

-- Sort-order aware compaction
CALL catalog.system.rewrite_data_files(
  table => 'db.orders',
  strategy => 'sort',
  sort_order => 'order_date ASC NULLS LAST, customer_id ASC NULLS LAST'
);
```

### Snapshot Expiration

Old snapshots accumulate metadata and prevent data file cleanup. Expire them to reclaim storage.

```sql
-- Expire snapshots older than 7 days, keeping at least 5
CALL catalog.system.expire_snapshots(
  table => 'db.orders',
  older_than => TIMESTAMP '2024-06-01 00:00:00',
  retain_last => 5
);

-- Expire with concurrency control
CALL catalog.system.expire_snapshots(
  table => 'db.orders',
  older_than => TIMESTAMP '2024-06-01 00:00:00',
  max_concurrent_deletes => 10
);
```

> **Note:** Expiring snapshots deletes data files no longer referenced by any retained snapshot. Time travel to expired snapshots becomes impossible.

### Orphan File Cleanup

Orphan files are data files not referenced by any table metadata, often left by failed writes.

```sql
-- Remove orphan files older than 3 days (default)
CALL catalog.system.remove_orphan_files(
  table => 'db.orders'
);

-- Custom age threshold
CALL catalog.system.remove_orphan_files(
  table => 'db.orders',
  older_than => TIMESTAMP '2024-05-01 00:00:00'
);

-- Dry run — list orphan files without deleting
CALL catalog.system.remove_orphan_files(
  table => 'db.orders',
  dry_run => true
);
```

> **Warning:** Only run orphan file cleanup when no write operations are in progress. Files from in-progress commits may appear orphaned.

### Rewriting Manifests

Manifest files track data files. Rewriting them optimizes metadata for faster query planning.

```sql
-- Rewrite manifests to merge small manifest files
CALL catalog.system.rewrite_manifests('db.orders');

-- Disable Spark caching during rewrite (for large tables)
CALL catalog.system.rewrite_manifests(
  table => 'db.orders',
  use_caching => false
);
```

### Recommended Maintenance Schedule

| Procedure | Frequency | Purpose |
|-----------|-----------|---------|
| `rewrite_data_files` | Daily/Weekly | Merge small files, improve read performance |
| `expire_snapshots` | Daily | Reclaim storage, reduce metadata size |
| `remove_orphan_files` | Weekly | Clean up failed write artifacts |
| `rewrite_manifests` | Weekly | Optimize query planning metadata |


---

## 9. AWS Integration

### Glue Data Catalog as Iceberg Catalog

AWS Glue Data Catalog serves as a centralized Iceberg catalog, enabling table discovery across Athena, EMR, and other AWS services.

```sql
-- Spark configuration for Glue Catalog
SET spark.sql.catalog.glue_catalog = org.apache.iceberg.spark.SparkCatalog;
SET spark.sql.catalog.glue_catalog.catalog-impl = org.apache.iceberg.aws.glue.GlueCatalog;
SET spark.sql.catalog.glue_catalog.warehouse = s3a://my-warehouse-bucket/iceberg/;
SET spark.sql.catalog.glue_catalog.io-impl = org.apache.iceberg.aws.s3.S3FileIO;
```

### S3 as Storage Layer

Iceberg stores data and metadata files on S3 using `s3a://` paths:

```sql
CREATE TABLE glue_catalog.db.events (
  event_id BIGINT,
  event_type STRING,
  event_time TIMESTAMP
)
USING iceberg
LOCATION 's3a://my-warehouse-bucket/iceberg/db/events';
```

Key S3 configuration properties:

```properties
spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.glue_catalog.s3.sse.type=s3   # Server-side encryption
spark.sql.catalog.glue_catalog.s3.path-style-access=false
```

### Querying with Athena

Athena v3 supports Iceberg tables natively when registered in Glue Data Catalog:

```sql
-- Create an Iceberg table directly in Athena
CREATE TABLE my_database.events (
  event_id BIGINT,
  event_type STRING,
  event_time TIMESTAMP
)
PARTITIONED BY (day(event_time))
LOCATION 's3://my-warehouse-bucket/iceberg/db/events'
TBLPROPERTIES ('table_type' = 'ICEBERG');

-- Time travel in Athena
SELECT * FROM my_database.events FOR SYSTEM_TIME AS OF TIMESTAMP '2024-06-01 00:00:00';

-- MERGE INTO in Athena
MERGE INTO my_database.events t
USING my_database.events_staging s ON t.event_id = s.event_id
WHEN MATCHED THEN UPDATE SET event_type = s.event_type
WHEN NOT MATCHED THEN INSERT (event_id, event_type, event_time) VALUES (s.event_id, s.event_type, s.event_time);
```

### EMR Cluster Configuration

For EMR 6.x+ with Iceberg support:

```json
[
  {
    "Classification": "spark-defaults",
    "Properties": {
      "spark.sql.catalog.glue_catalog": "org.apache.iceberg.spark.SparkCatalog",
      "spark.sql.catalog.glue_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
      "spark.sql.catalog.glue_catalog.warehouse": "s3a://my-warehouse-bucket/iceberg/",
      "spark.sql.catalog.glue_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
      "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    }
  },
  {
    "Classification": "iceberg-defaults",
    "Properties": {
      "iceberg.enabled": "true"
    }
  }
]
```

Launch with the Iceberg application:

```bash
aws emr create-cluster \
  --release-label emr-6.15.0 \
  --applications Name=Spark Name=Iceberg \
  --configurations file://iceberg-config.json \
  --instance-type m5.xlarge \
  --instance-count 3
```

### IAM Permissions

Minimum IAM policy for Iceberg operations with Glue Catalog and S3:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase", "glue:GetDatabases",
        "glue:CreateTable", "glue:GetTable", "glue:GetTables",
        "glue:UpdateTable", "glue:DeleteTable"
      ],
      "Resource": [
        "arn:aws:glue:*:*:catalog",
        "arn:aws:glue:*:*:database/*",
        "arn:aws:glue:*:*:table/*/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
        "s3:ListBucket", "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::my-warehouse-bucket",
        "arn:aws:s3:::my-warehouse-bucket/*"
      ]
    }
  ]
}
```
---

## Best Practices

### Partitioning Strategy

**Avoid over-partitioning.** Each partition should contain at least 128MB of data. Too many partitions create excessive metadata and small files.

```sql
-- BAD: Over-partitioning by high-cardinality column
CREATE TABLE events (
  event_id BIGINT,
  user_id BIGINT,
  event_time TIMESTAMP
) PARTITIONED BY (user_id);  -- millions of partitions!

-- GOOD: Use hidden partition transforms to reduce cardinality
CREATE TABLE events (
  event_id BIGINT,
  user_id BIGINT,
  event_time TIMESTAMP
) PARTITIONED BY (day(event_time), bucket(16, user_id));
```

**Use hidden partitions** — Iceberg's partition transforms (`year`, `month`, `day`, `hour`, `bucket`, `truncate`) decouple the physical layout from the logical schema. Users query raw columns; Iceberg applies the transform automatically.

**Guidelines:**
- Target 100–10,000 partitions total (not millions)
- Use `days(ts)` instead of storing a separate `date` column for partitioning
- Use `bucket(N, col)` for high-cardinality join keys (N = 16–512 depending on data volume)
- Re-evaluate partitioning as data volume grows — use partition evolution to adjust without rewriting

### File Sizing

Target **128MB–512MB** per data file (Parquet). This balances:
- Read throughput (fewer file opens, better sequential I/O)
- Parallelism (enough files for distributed reads)
- Metadata overhead (fewer manifest entries)

```sql
-- Set target file size at table creation
CREATE TABLE orders (...)
TBLPROPERTIES (
  'write.target-file-size-bytes' = '536870912'  -- 512MB
);

-- Or alter existing table
ALTER TABLE orders SET TBLPROPERTIES (
  'write.target-file-size-bytes' = '134217728'  -- 128MB
);
```

**Streaming workloads** produce many small files. Schedule compaction to merge them:

```sql
CALL catalog.system.rewrite_data_files(
  table => 'db.orders',
  options => map('target-file-size-bytes', '268435456', 'min-file-size-bytes', '134217728')
);
```

### Catalog Configuration

- **Production:** Use AWS Glue Data Catalog or a REST catalog for multi-engine access and centralized governance
- **Development:** Hadoop catalog on local/S3 paths is acceptable for single-engine testing
- **Multi-engine access:** Ensure all engines point to the same catalog to avoid metadata conflicts
- **Catalog locking:** For Hadoop catalogs on S3, configure a DynamoDB lock table to prevent concurrent metadata corruption:

```properties
spark.sql.catalog.my_catalog.lock.table = iceberg_lock
spark.sql.catalog.my_catalog.lock-impl = org.apache.iceberg.aws.dynamodb.DynamoDbLockManager
```

### Compaction Scheduling

- Run `rewrite_data_files` daily for streaming tables, weekly for batch tables
- Schedule during off-peak hours to avoid resource contention
- Use `partial-progress.enabled=true` for large tables to commit progress incrementally:

```sql
CALL catalog.system.rewrite_data_files(
  table => 'db.events',
  options => map(
    'partial-progress.enabled', 'true',
    'partial-progress.max-commits', '10'
  )
);
```

### Snapshot Retention Policies

- Retain snapshots for **1–7 days** depending on time-travel requirements
- Expire snapshots daily to prevent unbounded metadata growth:

```sql
CALL catalog.system.expire_snapshots(
  table => 'db.orders',
  older_than => TIMESTAMP '2024-01-14 00:00:00',
  retain_last => 5
);
```

- Set default retention at table level:

```sql
ALTER TABLE orders SET TBLPROPERTIES (
  'history.expire.max-snapshot-age-ms' = '86400000'  -- 1 day
);
```

### Common Pitfalls

| Pitfall | Cause | Solution |
|---------|-------|----------|
| **Small files problem** | Frequent streaming writes or over-partitioning | Schedule regular compaction; increase `write.target-file-size-bytes` |
| **Too many snapshots** | No expiration policy | Run `expire_snapshots` daily; set `history.expire.max-snapshot-age-ms` |
| **Partition explosion** | Partitioning by high-cardinality column | Use `bucket()` or `truncate()` transforms; target <10K partitions |
| **Slow query planning** | Millions of manifest entries | Run `rewrite_manifests`; ensure partition pruning is effective |
| **Metadata file bloat** | Never cleaning orphan files | Schedule weekly `remove_orphan_files` |
| **Concurrent write conflicts** | Multiple writers without retry logic | Enable `write.wap.enabled` or implement commit retry in application code |
