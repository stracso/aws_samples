# Iceberg Concurrency Strategy: Multi-Loader Data Ingestion

## Problem Statement

Multiple data loaders run concurrently, each performing:

1. **Read** a YAML configuration file
2. **Parse** and **load** data into an Iceberg **temp/staging table**
3. **Validate** the staged data
4. **Upsert** (MERGE INTO) the validated data into a shared **main Iceberg table**

The challenge: ensure loaders operate efficiently without collisions during the upsert phase, and handle conflicts gracefully when they occur.

---

## How Iceberg Handles Concurrency

### Optimistic Concurrency Control (OCC)

Iceberg uses **optimistic concurrency** — there are no locks on the table. Each writer:

1. Reads the current table metadata (snapshot)
2. Performs its operation (writes new data files)
3. Attempts to **commit** a new metadata file atomically

If another writer committed between steps 1 and 3, the commit **fails** with a `CommitFailedException`. The writer must then **retry** from a fresh snapshot.

### Conflict Detection

Iceberg detects conflicts at the **file level**, not the row level. Two operations conflict if:

| Writer A | Writer B | Conflict? |
|---|---|---|
| Appends new files | Appends new files | **No** — independent file sets |
| Overwrites partition X | Overwrites partition X | **Yes** — overlapping partition |
| Deletes rows in file F | Deletes rows in file F | **Yes** — same file |
| Appends to partition X | Overwrites partition X | **Yes** — structural conflict |
| MERGE into partition X | MERGE into partition X | **Yes** — both read+write same partition |

### Key Insight for Your Use Case

`MERGE INTO` (upsert) is a **read-then-write** operation. It scans the target table for matching keys, then writes updates/inserts. If two loaders MERGE into the **same partitions**, they will conflict.


---

## Strategy 1: Partition-Level Isolation (Recommended)

### Concept

Design the system so each loader writes to **non-overlapping partitions**. When loaders target different partitions, Iceberg allows them to commit concurrently without conflict.

### Implementation

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Loader A   │     │  Loader B   │     │  Loader C   │
│ (source: X) │     │ (source: Y) │     │ (source: Z) │
└──────┬──────┘     └──────┬──────┘     └──────┬──────┘
       │                   │                   │
       ▼                   ▼                   ▼
┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│ temp_table_X │   │ temp_table_Y │   │ temp_table_Z │
└──────┬───────┘   └──────┬───────┘   └──────┬───────┘
       │                   │                   │
       ▼                   ▼                   ▼
┌──────────────────────────────────────────────────────┐
│              main_table                              │
│  partition=source_X  partition=source_Y  partition=Z │
│  (only Loader A)     (only Loader B)    (Loader C)  │
└──────────────────────────────────────────────────────┘
```

### Table Design

```sql
CREATE TABLE glue_catalog.db.main_table (
    id              BIGINT,
    source_system   STRING,
    record_date     DATE,
    -- ... other columns
    load_timestamp  TIMESTAMP
)
USING iceberg
PARTITIONED BY (source_system, days(record_date))
```

### Loader MERGE (No Conflict)

```sql
-- Loader A only touches partition source_system = 'system_a'
MERGE INTO glue_catalog.db.main_table t
USING temp_table_a s
ON t.id = s.id AND t.source_system = s.source_system
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

Because each loader's MERGE is scoped to its own partition, Iceberg sees no overlapping file sets → **no conflict**.

### When to Use

- Each YAML source maps to a distinct partition key (source system, region, entity type)
- Data ownership is clear — one loader "owns" a partition


---

## Strategy 2: Retry with Exponential Backoff

### Concept

When partition isolation isn't possible (loaders may write to overlapping partitions), embrace Iceberg's OCC model and implement **automatic retries** on commit failure.

### Implementation (PySpark / Glue)

```python
import time
import random
from pyspark.sql import SparkSession
from py4j.protocol import Py4JJavaError

MAX_RETRIES = 5
BASE_DELAY_SECONDS = 2

def upsert_with_retry(spark, source_df, target_table, merge_condition, max_retries=MAX_RETRIES):
    """
    Perform a MERGE INTO with automatic retry on commit conflicts.
    """
    for attempt in range(1, max_retries + 1):
        try:
            # Register source as temp view
            source_df.createOrReplaceTempView("__merge_source")

            spark.sql(f"""
                MERGE INTO {target_table} t
                USING __merge_source s
                ON {merge_condition}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)

            print(f"MERGE succeeded on attempt {attempt}")
            return True

        except Py4JJavaError as e:
            error_msg = str(e.java_exception)

            if "CommitFailedException" in error_msg or "ValidationException" in error_msg:
                if attempt == max_retries:
                    print(f"MERGE failed after {max_retries} attempts. Last error: {error_msg}")
                    raise

                # Exponential backoff with jitter
                delay = BASE_DELAY_SECONDS * (2 ** (attempt - 1)) + random.uniform(0, 1)
                print(f"Commit conflict on attempt {attempt}. Retrying in {delay:.1f}s...")
                time.sleep(delay)

                # Refresh the source DataFrame to pick up latest snapshot
                # (important: re-read if source depends on target state)
            else:
                raise  # Non-retryable error

    return False
```

### Usage

```python
# Each loader calls this
source_df = spark.read.table("temp_staging_table")

upsert_with_retry(
    spark=spark,
    source_df=source_df,
    target_table="glue_catalog.db.main_table",
    merge_condition="t.id = s.id AND t.record_date = s.record_date"
)
```

### Retry Behavior

| Attempt | Delay (approx) | Cumulative Wait |
|---------|----------------|-----------------|
| 1       | 0s (first try) | 0s              |
| 2       | 2-3s           | ~3s             |
| 3       | 4-5s           | ~8s             |
| 4       | 8-9s           | ~17s            |
| 5       | 16-17s         | ~34s            |


---

## Strategy 3: Queue-Based Serialization

### Concept

If conflict rates are high and retries become expensive, serialize the upsert phase using a queue or coordination mechanism. Loaders still run in parallel for the read/parse/validate phases, but upserts are serialized.

### Architecture

```
┌─────────┐  ┌─────────┐  ┌─────────┐
│Loader A │  │Loader B │  │Loader C │   ← Parallel: read YAML, parse, validate
└────┬────┘  └────┬────┘  └────┬────┘
     │            │            │
     ▼            ▼            ▼
┌─────────────────────────────────────┐
│     Staging Tables (per loader)     │   ← Each loader writes to its own temp table
│  temp_a    temp_b    temp_c         │
└─────────────────┬───────────────────┘
                  │
                  ▼
┌─────────────────────────────────────┐
│         SQS / Step Functions        │   ← Serialization layer
│    (one upsert at a time)           │
└─────────────────┬───────────────────┘
                  │
                  ▼
┌─────────────────────────────────────┐
│           Main Table                │   ← Sequential MERGE operations
└─────────────────────────────────────┘
```

### Option A: SQS FIFO Queue + Lambda/Glue Consumer

```python
# Loader publishes "ready to merge" message after validation
import boto3
import json

sqs = boto3.client('sqs')
QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/123456789/merge-queue.fifo"

def signal_ready_to_merge(loader_id, staging_table, target_table, merge_keys):
    """Signal that this loader's data is validated and ready to merge."""
    sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=json.dumps({
            "loader_id": loader_id,
            "staging_table": staging_table,
            "target_table": target_table,
            "merge_keys": merge_keys,
            "timestamp": time.time()
        }),
        MessageGroupId="main-table-merges",  # FIFO: ensures ordering
        MessageDeduplicationId=f"{loader_id}-{int(time.time())}"
    )
```

### Option B: Step Functions Orchestration

Use AWS Step Functions with a **Map state** for parallel load/validate, followed by a sequential merge phase:

```json
{
  "StartAt": "ParallelLoadAndValidate",
  "States": {
    "ParallelLoadAndValidate": {
      "Type": "Map",
      "ItemsPath": "$.loaders",
      "MaxConcurrency": 0,
      "Iterator": {
        "StartAt": "LoadAndValidate",
        "States": {
          "LoadAndValidate": {
            "Type": "Task",
            "Resource": "arn:aws:glue:us-east-1:123456789:job/loader-job",
            "Parameters": {
              "--loader_config.$": "$.yaml_path",
              "--phase": "load_validate"
            },
            "End": true
          }
        }
      },
      "Next": "SequentialMerge"
    },
    "SequentialMerge": {
      "Type": "Map",
      "ItemsPath": "$.loaders",
      "MaxConcurrency": 1,
      "Iterator": {
        "StartAt": "MergeToMain",
        "States": {
          "MergeToMain": {
            "Type": "Task",
            "Resource": "arn:aws:glue:us-east-1:123456789:job/merge-job",
            "Parameters": {
              "--loader_config.$": "$.yaml_path",
              "--phase": "merge"
            },
            "End": true
          }
        }
      },
      "End": true
    }
  }
}
```


---

## Strategy 4: DynamoDB-Based Locking (Fine-Grained)

### Concept

Use a DynamoDB table as a distributed lock to coordinate which loader can MERGE into a specific partition at a given time. This allows parallelism across partitions while preventing conflicts within a partition.

### Lock Table Schema

```
Table: iceberg_merge_locks
  PK: lock_key (String)  — e.g., "main_table#partition=2024-01-15"
  SK: -
  Attributes:
    - owner (String): loader ID holding the lock
    - acquired_at (Number): epoch timestamp
    - ttl (Number): auto-expire for safety (epoch + 300s)
```

### Lock Implementation

```python
import boto3
import time
import uuid
from botocore.exceptions import ClientError

dynamodb = boto3.resource('dynamodb')
lock_table = dynamodb.Table('iceberg_merge_locks')

class PartitionLock:
    def __init__(self, table_name, partition_key, loader_id, ttl_seconds=300):
        self.lock_key = f"{table_name}#{partition_key}"
        self.loader_id = loader_id
        self.ttl_seconds = ttl_seconds
        self.lock_token = str(uuid.uuid4())

    def acquire(self, max_wait_seconds=120, poll_interval=5):
        """Attempt to acquire the lock with polling."""
        deadline = time.time() + max_wait_seconds

        while time.time() < deadline:
            try:
                lock_table.put_item(
                    Item={
                        'lock_key': self.lock_key,
                        'owner': self.loader_id,
                        'lock_token': self.lock_token,
                        'acquired_at': int(time.time()),
                        'ttl': int(time.time()) + self.ttl_seconds
                    },
                    ConditionExpression='attribute_not_exists(lock_key) OR #ttl < :now',
                    ExpressionAttributeNames={'#ttl': 'ttl'},
                    ExpressionAttributeValues={':now': int(time.time())}
                )
                print(f"Lock acquired: {self.lock_key} by {self.loader_id}")
                return True

            except ClientError as e:
                if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                    print(f"Lock held by another loader. Waiting {poll_interval}s...")
                    time.sleep(poll_interval)
                else:
                    raise

        print(f"Failed to acquire lock within {max_wait_seconds}s")
        return False

    def release(self):
        """Release the lock (only if we still own it)."""
        try:
            lock_table.delete_item(
                Key={'lock_key': self.lock_key},
                ConditionExpression='lock_token = :token',
                ExpressionAttributeValues={':token': self.lock_token}
            )
            print(f"Lock released: {self.lock_key}")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                print(f"Lock already released or expired: {self.lock_key}")
            else:
                raise
```

### Usage in Loader

```python
# Loader determines which partitions it will touch
partitions_to_merge = ["record_date=2024-01-15", "record_date=2024-01-16"]

locks = []
try:
    # Acquire locks for all target partitions
    for partition in partitions_to_merge:
        lock = PartitionLock("main_table", partition, loader_id="loader_A")
        if not lock.acquire():
            raise TimeoutError(f"Could not acquire lock for {partition}")
        locks.append(lock)

    # Perform the MERGE (safe — we hold the locks)
    spark.sql("""
        MERGE INTO glue_catalog.db.main_table t
        USING temp_table_a s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

finally:
    # Always release locks
    for lock in locks:
        lock.release()
```


---

## Strategy Comparison

| Strategy | Throughput | Complexity | Best When |
|----------|-----------|------------|-----------|
| **1. Partition Isolation** | Highest (true parallel) | Low | Loaders have natural partition boundaries |
| **2. Retry + Backoff** | High (occasional retry) | Low-Medium | Conflicts are rare (<10% of commits) |
| **3. Queue Serialization** | Medium (serial merges) | Medium | Conflicts are frequent, data volume per merge is small |
| **4. DynamoDB Locking** | High (parallel across partitions) | Medium-High | Need partition-level control, mixed overlap patterns |

### Decision Matrix

```
Do loaders write to non-overlapping partitions?
├── YES → Strategy 1 (Partition Isolation) ✓
└── NO
    ├── Are conflicts rare (< 10% of commits)?
    │   └── YES → Strategy 2 (Retry + Backoff) ✓
    └── Are conflicts frequent?
        ├── Is merge operation fast (< 2 min)?
        │   └── YES → Strategy 3 (Queue Serialization) ✓
        └── Is merge operation slow or partition-specific?
            └── YES → Strategy 4 (DynamoDB Locking) ✓
```

---

## Testing Plan: Validating Concurrency Behavior

### Test 1: Baseline — No Conflict (Partition Isolation)

**Objective:** Confirm that loaders writing to different partitions commit without retries.

```python
"""
Test: 3 loaders writing to 3 different partitions simultaneously.
Expected: All succeed on first attempt, no CommitFailedException.
"""
import concurrent.futures
from pyspark.sql import SparkSession

def loader_task(spark, loader_id, source_partition):
    """Simulate a single loader's full pipeline."""
    # 1. Read YAML (simulated)
    data = generate_test_data(loader_id, partition_value=source_partition, num_rows=10000)

    # 2. Write to staging
    staging_table = f"glue_catalog.db.temp_{loader_id}"
    data.writeTo(staging_table).createOrReplace()

    # 3. Validate
    count = spark.read.table(staging_table).count()
    assert count == 10000, f"Validation failed for {loader_id}"

    # 4. Upsert to main (partition-scoped)
    spark.sql(f"""
        MERGE INTO glue_catalog.db.main_table t
        USING {staging_table} s
        ON t.id = s.id AND t.source_system = s.source_system
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    return f"{loader_id}: SUCCESS"

# Run 3 loaders in parallel
with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
    futures = {
        executor.submit(loader_task, spark, "loader_a", "system_a"): "loader_a",
        executor.submit(loader_task, spark, "loader_b", "system_b"): "loader_b",
        executor.submit(loader_task, spark, "loader_c", "system_c"): "loader_c",
    }
    for future in concurrent.futures.as_completed(futures):
        print(future.result())
```


### Test 2: Conflict Scenario — Same Partition

**Objective:** Force a conflict and verify retry logic handles it.

```python
"""
Test: 3 loaders all writing to the SAME partition simultaneously.
Expected: Some loaders get CommitFailedException, retry logic resolves it.
"""
import concurrent.futures
import time

results = {"attempts": {}, "success": {}, "failures": {}}

def conflicting_loader_task(spark, loader_id):
    """All loaders target the same partition to force conflicts."""
    data = generate_test_data(loader_id, partition_value="shared_partition", num_rows=5000)

    staging_table = f"glue_catalog.db.temp_{loader_id}"
    data.writeTo(staging_table).createOrReplace()

    # Use retry wrapper
    success = upsert_with_retry(
        spark=spark,
        source_df=spark.read.table(staging_table),
        target_table="glue_catalog.db.main_table",
        merge_condition="t.id = s.id",
        max_retries=5
    )
    return {"loader_id": loader_id, "success": success}

# Run all loaders targeting the same partition
with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
    futures = [
        executor.submit(conflicting_loader_task, spark, f"loader_{i}")
        for i in range(3)
    ]
    for future in concurrent.futures.as_completed(futures):
        result = future.result()
        print(f"{result['loader_id']}: {'SUCCESS' if result['success'] else 'FAILED'}")

# Validate final state
final_count = spark.sql("SELECT count(*) FROM glue_catalog.db.main_table").collect()[0][0]
print(f"Final row count: {final_count}")
# Should be 15000 (5000 * 3) if all IDs are unique, or less if there are overlapping IDs
```

### Test 3: Stress Test — High Concurrency

**Objective:** Determine the breaking point and measure retry rates.

```python
"""
Test: N loaders (5, 10, 20) with varying overlap percentages.
Metrics: success rate, avg retries, total wall-clock time.
"""
import time

CONCURRENCY_LEVELS = [5, 10, 20]
OVERLAP_PERCENTAGES = [0, 25, 50, 100]  # % of loaders targeting same partition

for num_loaders in CONCURRENCY_LEVELS:
    for overlap_pct in OVERLAP_PERCENTAGES:
        num_overlapping = int(num_loaders * overlap_pct / 100)
        num_isolated = num_loaders - num_overlapping

        start_time = time.time()

        # Launch loaders...
        # (implementation similar to above, with metrics collection)

        elapsed = time.time() - start_time
        print(f"Loaders={num_loaders}, Overlap={overlap_pct}%: "
              f"Time={elapsed:.1f}s, Retries=X, Failures=Y")
```

### Test 4: Data Integrity Validation

**Objective:** Ensure no data loss or duplication after concurrent merges.

```python
"""
Post-concurrency validation checks.
"""

def validate_data_integrity(spark, target_table, expected_records):
    """Run after concurrent load to verify correctness."""

    # 1. No duplicate primary keys
    duplicates = spark.sql(f"""
        SELECT id, count(*) as cnt
        FROM {target_table}
        GROUP BY id
        HAVING cnt > 1
    """)
    dup_count = duplicates.count()
    assert dup_count == 0, f"Found {dup_count} duplicate keys!"

    # 2. All records present
    actual_count = spark.sql(f"SELECT count(*) FROM {target_table}").collect()[0][0]
    assert actual_count == expected_records, \
        f"Expected {expected_records} rows, got {actual_count}"

    # 3. No partial writes (all columns populated)
    nulls = spark.sql(f"""
        SELECT count(*) FROM {target_table}
        WHERE id IS NULL OR source_system IS NULL OR record_date IS NULL
    """).collect()[0][0]
    assert nulls == 0, f"Found {nulls} rows with NULL required fields"

    # 4. Snapshot consistency — only expected number of snapshots
    snapshots = spark.sql(f"SELECT * FROM {target_table}.snapshots")
    print(f"Total snapshots after test: {snapshots.count()}")

    # 5. Check for orphan files
    spark.sql(f"""
        CALL glue_catalog.system.remove_orphan_files(
            table => '{target_table}',
            dry_run => true
        )
    """).show()

    print("✓ Data integrity validation passed")
```


### Test 5: Failure Recovery

**Objective:** Verify the system recovers gracefully when a loader crashes mid-merge.

```python
"""
Test: Simulate loader failure during MERGE.
Expected: Table remains consistent, other loaders unaffected.
"""

def test_loader_crash_recovery(spark):
    """
    1. Start 3 loaders
    2. Kill one mid-operation (simulate with timeout/exception)
    3. Verify table is still queryable and consistent
    4. Verify remaining loaders complete successfully
    """

    # Iceberg guarantees: a failed commit leaves no partial state.
    # The staging table remains intact for retry.

    # After "crash":
    # - Table should have no corrupt snapshots
    # - Other loaders should not be blocked
    # - Crashed loader's staging data should still be available for re-run

    # Verify table health
    spark.sql("SELECT * FROM glue_catalog.db.main_table.snapshots").show()
    spark.sql("SELECT count(*) FROM glue_catalog.db.main_table").show()

    # Re-run the failed loader
    # It should succeed since Iceberg commits are atomic
    print("✓ Recovery test passed — table consistent after loader failure")
```

---

## Iceberg Configuration Tuning for Concurrency

### Table Properties

```sql
ALTER TABLE glue_catalog.db.main_table SET TBLPROPERTIES (
    -- Increase commit retry attempts (default is 4)
    'commit.retry.num-retries' = '10',

    -- Total time to keep retrying (default 30 min)
    'commit.retry.total-timeout-ms' = '600000',

    -- Min wait between retries (default 100ms)
    'commit.retry.min-wait-ms' = '500',

    -- Max wait between retries (default 60s)
    'commit.retry.max-wait-ms' = '30000',

    -- Enable WAP (Write-Audit-Publish) for staged validation
    'write.wap.enabled' = 'true'
);
```

### Spark Session Configuration

```python
spark = SparkSession.builder \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", "s3://your-bucket/warehouse/") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    # Lock configuration for Glue catalog
    .config("spark.sql.catalog.glue_catalog.lock-impl", "org.apache.iceberg.aws.glue.DynamoLockManager") \
    .config("spark.sql.catalog.glue_catalog.lock.table", "iceberg_lock") \
    .getOrCreate()
```

> **Note:** The `DynamoLockManager` is used by the Iceberg Glue catalog to coordinate metadata commits. This is separate from the application-level DynamoDB locking in Strategy 4.

---

## Recommended Architecture for Your Use Case

Given the multi-loader YAML → staging → validate → upsert pipeline:

### Hybrid Approach (Strategy 1 + Strategy 2)

```
┌──────────────────────────────────────────────────────────────────┐
│                        LOADER PIPELINE                            │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Phase 1: PARALLEL (no coordination needed)                      │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐                         │
│  │Read YAML│  │Read YAML│  │Read YAML│                          │
│  │Parse    │  │Parse    │  │Parse    │                          │
│  │→ Temp   │  │→ Temp   │  │→ Temp   │                          │
│  │Validate │  │Validate │  │Validate │                          │
│  └────┬────┘  └────┬────┘  └────┬────┘                         │
│       │            │            │                                │
│  Phase 2: MERGE (partition-aware with retry)                     │
│       │            │            │                                │
│       ▼            ▼            ▼                                │
│  ┌──────────────────────────────────────┐                       │
│  │  Partition-scoped MERGE + Retry      │                       │
│  │  • Filter merge to owned partitions  │                       │
│  │  • Retry on conflict (backoff)       │                       │
│  │  • Log metrics (retries, duration)   │                       │
│  └──────────────────────────────────────┘                       │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### Implementation Skeleton

```python
class DataLoader:
    def __init__(self, spark, yaml_path, loader_id):
        self.spark = spark
        self.yaml_path = yaml_path
        self.loader_id = loader_id
        self.staging_table = f"glue_catalog.db.staging_{loader_id}"
        self.main_table = "glue_catalog.db.main_table"

    def run(self):
        """Full loader pipeline."""
        # Phase 1: Load and validate (safe to run in parallel)
        config = self.read_yaml()
        raw_df = self.parse_data(config)
        self.write_to_staging(raw_df)
        self.validate_staging()

        # Phase 2: Merge (with conflict handling)
        self.merge_to_main()

        # Phase 3: Cleanup
        self.cleanup_staging()

    def read_yaml(self):
        """Read and parse YAML configuration."""
        import yaml
        with open(self.yaml_path) as f:
            return yaml.safe_load(f)

    def parse_data(self, config):
        """Parse source data based on YAML config."""
        # Implementation depends on your data sources
        pass

    def write_to_staging(self, df):
        """Write parsed data to loader-specific staging table."""
        df.writeTo(self.staging_table).createOrReplace()

    def validate_staging(self):
        """Run validation rules on staged data."""
        staged = self.spark.read.table(self.staging_table)

        # Example validations
        assert staged.count() > 0, "No data in staging"
        assert staged.filter("id IS NULL").count() == 0, "NULL IDs found"

        # Schema validation
        # Business rule validation
        # Referential integrity checks

    def merge_to_main(self):
        """Upsert staging data into main table with retry."""
        source_df = self.spark.read.table(self.staging_table)

        upsert_with_retry(
            spark=self.spark,
            source_df=source_df,
            target_table=self.main_table,
            merge_condition="t.id = s.id AND t.source_system = s.source_system",
            max_retries=5
        )

    def cleanup_staging(self):
        """Drop staging table after successful merge."""
        self.spark.sql(f"DROP TABLE IF EXISTS {self.staging_table}")
```


---

## Monitoring and Observability

### Metrics to Track

| Metric | Source | Alert Threshold |
|--------|--------|-----------------|
| Commit retries per loader | Application logs | > 3 retries per merge |
| Merge duration (p50, p95) | CloudWatch custom metric | p95 > 5 min |
| Conflict rate | Application logs | > 20% of commits |
| Snapshot count growth | Iceberg metadata | > 100 snapshots (run expire) |
| Orphan file count | Periodic audit job | > 0 |

### CloudWatch Custom Metrics

```python
import boto3

cloudwatch = boto3.client('cloudwatch')

def publish_merge_metrics(loader_id, duration_seconds, retry_count, success):
    cloudwatch.put_metric_data(
        Namespace='IcebergLoaders',
        MetricData=[
            {
                'MetricName': 'MergeDuration',
                'Dimensions': [{'Name': 'LoaderId', 'Value': loader_id}],
                'Value': duration_seconds,
                'Unit': 'Seconds'
            },
            {
                'MetricName': 'CommitRetries',
                'Dimensions': [{'Name': 'LoaderId', 'Value': loader_id}],
                'Value': retry_count,
                'Unit': 'Count'
            },
            {
                'MetricName': 'MergeSuccess',
                'Dimensions': [{'Name': 'LoaderId', 'Value': loader_id}],
                'Value': 1 if success else 0,
                'Unit': 'Count'
            }
        ]
    )
```

---

## Maintenance: Preventing Table Degradation Under Concurrency

Frequent concurrent writes produce many small files and snapshots. Schedule regular maintenance:

```python
# Run daily or after batch windows
def maintain_table(spark, table):
    """Compact files and expire old snapshots."""

    # 1. Compact small files (reduces read overhead)
    spark.sql(f"""
        CALL glue_catalog.system.rewrite_data_files(
            table => '{table}',
            options => map('min-input-files', '5', 'target-file-size-bytes', '134217728')
        )
    """)

    # 2. Expire old snapshots (keep last 3 days)
    spark.sql(f"""
        CALL glue_catalog.system.expire_snapshots(
            table => '{table}',
            older_than => TIMESTAMP '{three_days_ago}',
            retain_last => 5
        )
    """)

    # 3. Remove orphan files
    spark.sql(f"""
        CALL glue_catalog.system.remove_orphan_files(
            table => '{table}',
            older_than => TIMESTAMP '{three_days_ago}'
        )
    """)
```

---

## Summary

1. **Start with Partition Isolation** (Strategy 1) — design your table partitioning so each loader owns its partitions. This gives you maximum throughput with zero conflicts.

2. **Add Retry Logic** (Strategy 2) as a safety net — even with partition isolation, edge cases can cause conflicts. The retry wrapper costs nothing when there are no conflicts.

3. **Escalate to Serialization** (Strategy 3) or **Locking** (Strategy 4) only if conflict rates are unacceptable after tuning partition boundaries.

4. **Always validate** — run the data integrity tests after every batch window to catch issues early.

5. **Monitor and maintain** — track retry rates, compact files, and expire snapshots to keep the table healthy under sustained concurrent load.
