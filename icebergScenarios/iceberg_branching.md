# Iceberg Schema Evolution & Branching

This document explains the `nb-schema-evolution_branching` notebook, which demonstrates Apache Iceberg schema evolution patterns and branching strategies on AWS Glue.

## Environment

- **Runtime**: AWS Glue 5.0 interactive session with Iceberg support
- **Catalog**: AWS Glue Data Catalog (`glue_catalog`)
- **Storage**: S3 (`s3://<bucket>/warehouse/`)
- **Workers**: 2x G.1X

## What the Notebook Demonstrates

### 1. Base Table Creation (Schema v0)

Creates an Iceberg table `jpmc_demo_warehouse.events` with three columns:

| Column     | Type      |
|------------|-----------|
| id         | BIGINT    |
| name       | STRING    |
| created_at | TIMESTAMP |

Inserts 3 initial rows (alice, bob, charlie).

### 2. Schema Evolution: Adding Columns (v0 → v1)

Adds `region` and `event_type` columns via `ALTER TABLE ADD COLUMNS`. Existing rows automatically return `NULL` for the new columns without rewriting data files.

### 3. Branching for Safe Schema Changes

Creates a branch (`branch_schema_test`) to preserve the current table state before making destructive schema changes on the main branch.

After branching, the `event_type` column is dropped from the main branch. Key observations:

- **Main branch**: `event_type` is no longer visible in queries.
- **Branch / old snapshots**: `event_type` data is still accessible via `VERSION AS OF <snapshot_id>` or branch references.
- The underlying data files still physically contain the dropped column's data.

### 4. Write-Audit-Publish (WAP) with Branches

Enables `write.wap.enabled` on the table and uses `spark.wap.branch` to direct writes to a specific branch.

**Key finding**: Iceberg enforces that all writes conform to the *current main schema*. Attempting to write data with the old schema (including `event_type`) fails with `INSERT_COLUMN_ARITY_MISMATCH`. The workaround is to drop the extra columns from the DataFrame before writing.

### 5. Loading Old-Schema Data into an Evolved Table

Demonstrates four approaches for handling data that contains columns no longer in the table schema:

#### Approach A: Drop Extra Columns (Lossy)

Read the current table schema, select only matching columns from the incoming DataFrame, and discard the rest. Simple but loses the dropped column data permanently.

#### Approach B: Temporarily Re-add Dropped Columns (Lossless)

1. `ALTER TABLE ADD COLUMNS (event_type STRING)` — re-adds the column
2. Insert old data with all columns intact
3. `ALTER TABLE DROP COLUMN event_type` — hides it again

Data is physically preserved in files; the column is just hidden from the schema projection.

#### Approach C: Staging Table

1. Create a staging table with the full old schema (including dropped columns)
2. Load old data into staging
3. `INSERT INTO production SELECT <current_columns> FROM staging`

Provides an isolated landing zone; useful when old data needs validation before promotion.

#### Approach D: Branch + Temporary Schema Expansion (Recommended)

Combines branching with temporary column re-addition for zero-downtime backfill:

1. Create a branch (`backfill_old_data`) with a retention policy
2. Re-add the dropped column to the table schema
3. Write old data to the branch via WAP (`spark.wap.branch`)
4. Drop the column again (hides from future reads)
5. Validate data on the branch
6. `fast_forward` main to the branch — atomically promotes the backfill
7. Drop the branch

This approach is the safest: main remains untouched until validation passes, and the operation is atomic.

## Key Takeaways

| Concept | Behavior |
|---------|----------|
| Adding columns | Existing rows return NULL for new columns; no data rewrite needed |
| Dropping columns | Column hidden from schema projection; data files retain it physically |
| Branching | Preserves a point-in-time reference to a snapshot; useful for safe experimentation |
| WAP (Write-Audit-Publish) | Directs writes to a branch for validation before promotion |
| Schema enforcement on write | All writes must conform to the current table schema — extra columns are rejected |
| Fast-forward | Atomically advances the main branch pointer to include branch commits |
| Time travel | `VERSION AS OF <snapshot_id>` reads data with the schema that existed at that snapshot |
