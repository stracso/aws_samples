"""
Synthetic Data Generator - AWS Glue PySpark Script

Reads the schema of an existing Iceberg source table, creates a new Iceberg
target table with the same schema but a different partition strategy, samples
rows from the source, and generates synthetic data persisted as a staging
Parquet dataset.

Usage:
  Run as an AWS Glue job with the following job parameters:
    --DATABASE_NAME       : Glue catalog database (required)
    --SOURCE_TABLE        : Source Iceberg table name (required)
    --TARGET_TABLE        : Target Iceberg table name to create (required)
    --TARGET_PARTITION    : Partition spec for the target table, e.g.
                            "month(event_ts), bucket(8, user_id)" (required)
    --TARGET_ROWS         : Number of synthetic rows to generate (required)
    --STAGING_PATH        : S3 path for staging Parquet data (required)
"""

import re
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, ShortType,
    ByteType, FloatType, DoubleType, DecimalType, BooleanType,
    TimestampType, DateType, BinaryType, ArrayType, MapType, StructType as ST,
)
from awsglue.utils import getResolvedOptions

# Maximum number of distinct values to generate for partition columns.
NUM_PARTITIONS = 100


# ---------------------------------------------------------------------------
# Configuration & Parameters
# ---------------------------------------------------------------------------

def get_params():
    """Parse Glue job parameters."""
    args = getResolvedOptions(sys.argv, [
        "JOB_NAME",
        "DATABASE_NAME",
        "SOURCE_TABLE",
        "TARGET_TABLE",
        "TARGET_PARTITION",
        "TARGET_ROWS",
        "STAGING_PATH",
    ])
    return {
        "job_name":         args["JOB_NAME"],
        "database":         args["DATABASE_NAME"],
        "source_table":     args["SOURCE_TABLE"],
        "target_table":     args["TARGET_TABLE"],
        "target_partition": args["TARGET_PARTITION"],
        "target_rows":      int(args["TARGET_ROWS"]),
        "staging_path":     args["STAGING_PATH"],
    }


def init_spark():
    """Initialise Spark session with Iceberg + Glue catalog."""
    spark = (
        SparkSession.builder
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
        .getOrCreate()
    )
    return spark


def table_fqn(database, table_name):
    """Return fully-qualified Iceberg table name."""
    return f"glue_catalog.{database}.{table_name}"


def elapsed(start):
    return round(time.time() - start, 3)


# ---------------------------------------------------------------------------
# Schema Introspection
# ---------------------------------------------------------------------------

def get_source_schema(spark, source_fqn):
    """Read the schema from the source table."""
    df = spark.table(source_fqn)
    return df.schema


def get_source_sample(spark, source_fqn, sample_size=1000):
    """Read a sample of rows from the source table for profiling."""
    df = spark.table(source_fqn)
    total = df.count()
    fraction = min(1.0, (sample_size * 2) / max(total, 1))
    sample_df = df.sample(withReplacement=False, fraction=fraction).limit(sample_size)
    return sample_df


# ---------------------------------------------------------------------------
# Synthetic Data Generation
# ---------------------------------------------------------------------------

def build_synthetic_column(col_name, data_type, sample_df, num_rows):
    """
    Build a synthetic column expression based on the data type and sample
    statistics. Returns a Spark Column expression.
    """
    if isinstance(data_type, (LongType, IntegerType, ShortType, ByteType)):
        stats = sample_df.agg(
            F.min(col_name).alias("min_val"),
            F.max(col_name).alias("max_val"),
        ).collect()[0]
        min_val = stats["min_val"] if stats["min_val"] is not None else 0
        max_val = stats["max_val"] if stats["max_val"] is not None else 1000000
        range_val = max(max_val - min_val, 1)
        return (F.lit(min_val) + (F.rand() * range_val)).cast(data_type)

    if isinstance(data_type, (FloatType, DoubleType)):
        stats = sample_df.agg(
            F.min(col_name).alias("min_val"),
            F.max(col_name).alias("max_val"),
        ).collect()[0]
        min_val = float(stats["min_val"]) if stats["min_val"] is not None else 0.0
        max_val = float(stats["max_val"]) if stats["max_val"] is not None else 1000.0
        range_val = max(max_val - min_val, 0.01)
        return (F.lit(min_val) + (F.rand() * range_val)).cast(data_type)

    if isinstance(data_type, DecimalType):
        stats = sample_df.agg(
            F.min(col_name).alias("min_val"),
            F.max(col_name).alias("max_val"),
        ).collect()[0]
        min_val = float(stats["min_val"]) if stats["min_val"] is not None else 0.0
        max_val = float(stats["max_val"]) if stats["max_val"] is not None else 10000.0
        range_val = max(max_val - min_val, 0.01)
        return F.round(F.lit(min_val) + (F.rand() * range_val), data_type.scale).cast(data_type)

    if isinstance(data_type, BooleanType):
        return (F.rand() > 0.5).cast(BooleanType())

    if isinstance(data_type, TimestampType):
        stats = sample_df.agg(
            F.min(col_name).alias("min_val"),
            F.max(col_name).alias("max_val"),
        ).collect()[0]
        if stats["min_val"] is not None and stats["max_val"] is not None:
            min_ts = stats["min_val"]
            max_ts = stats["max_val"]
            range_secs = int((max_ts - min_ts).total_seconds())
            range_secs = max(range_secs, 1)
            return (
                F.lit(min_ts.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp")
                + F.expr(f"make_interval(0,0,0,0,0, cast(rand() * {range_secs} as int))")
            )
        return (
            F.lit("2024-01-01 00:00:00").cast("timestamp")
            + F.expr(f"make_interval(0,0,0,0,0, cast(rand() * {730 * 86400} as int))")
        )

    if isinstance(data_type, DateType):
        stats = sample_df.agg(
            F.min(col_name).alias("min_val"),
            F.max(col_name).alias("max_val"),
        ).collect()[0]
        if stats["min_val"] is not None and stats["max_val"] is not None:
            min_d = stats["min_val"]
            max_d = stats["max_val"]
            range_days = max((max_d - min_d).days, 1)
            return F.date_add(F.lit(min_d), (F.rand() * range_days).cast("int"))
        return F.date_add(F.lit("2024-01-01"), (F.rand() * 730).cast("int"))

    if isinstance(data_type, StringType):
        distinct_vals = (
            sample_df.select(col_name)
            .filter(F.col(col_name).isNotNull())
            .distinct()
            .limit(200)
            .rdd.flatMap(lambda x: x)
            .collect()
        )
        if 1 < len(distinct_vals) <= 100:
            return F.element_at(
                F.array([F.lit(v) for v in distinct_vals]),
                (F.rand() * len(distinct_vals)).cast("int") + 1,
            )
        return F.concat(F.lit(f"{col_name}_"), F.monotonically_increasing_id().cast("string"))

    if isinstance(data_type, ArrayType):
        elem_type = data_type.elementType
        if isinstance(elem_type, StringType):
            return F.array(
                F.concat(F.lit("val_"), (F.rand() * 100).cast("int").cast("string")),
                F.concat(F.lit("val_"), (F.rand() * 100).cast("int").cast("string")),
            )
        return F.array(F.lit(None).cast(elem_type))

    if isinstance(data_type, MapType):
        return F.create_map(
            F.lit("key"), F.lit("value").cast(data_type.valueType)
        )

    if isinstance(data_type, BinaryType):
        return F.encode(
            F.concat(F.lit("bin_"), F.monotonically_increasing_id().cast("string")),
            "utf-8",
        )

    # Fallback for struct or unknown types — null
    return F.lit(None).cast(data_type)


def parse_partition_columns(partition_spec):
    """
    Extract column names referenced in an Iceberg partition spec string.

    Handles bare column names and transform expressions such as:
        "month(event_ts), bucket(8, user_id)"  ->  ["event_ts", "user_id"]
        "region"                                ->  ["region"]
        "year(ts), category"                    ->  ["ts", "category"]
    """
    columns = []
    for token in partition_spec.split(","):
        token = token.strip()
        if not token:
            continue
        # Match transform expressions like month(col) or bucket(N, col)
        match = re.match(r'\w+\s*\((.+)\)', token)
        if match:
            inner = match.group(1)
            # The column name is the last argument (first may be a numeric param)
            parts = [p.strip() for p in inner.split(",")]
            columns.append(parts[-1])
        else:
            # Bare column name
            columns.append(token)
    return columns


def generate_synthetic_df(spark, schema, sample_df, num_rows, partition_spec="", num_spark_partitions=200):
    """
    Generate a DataFrame of synthetic data matching the given schema,
    using sample statistics to produce realistic value distributions.

    Partition columns (derived from *partition_spec*) are constrained to
    at most NUM_PARTITIONS distinct values so the resulting Iceberg table
    has a controlled number of partitions.
    """
    partition_cols = set(parse_partition_columns(partition_spec))

    base_df = spark.range(0, num_rows, numPartitions=num_spark_partitions)

    for field in schema.fields:
        col_expr = build_synthetic_column(field.name, field.dataType, sample_df, num_rows)
        if field.name in partition_cols:
            # Constrain partition column to NUM_PARTITIONS distinct values
            # by assigning each row to a bucket and mapping it back to a
            # value within the column's range.
            col_expr = build_synthetic_column(field.name, field.dataType, sample_df, NUM_PARTITIONS)
            bucket_id = (F.monotonically_increasing_id() % NUM_PARTITIONS).cast("long")
            col_expr = _limit_partition_values(field.name, field.dataType, sample_df, bucket_id)
        base_df = base_df.withColumn(field.name, col_expr)

    schema_col_names = [f.name for f in schema.fields]
    if "id" not in schema_col_names and "id" in base_df.columns:
        base_df = base_df.drop("id")

    base_df = base_df.select(*schema_col_names)
    return base_df


def _limit_partition_values(col_name, data_type, sample_df, bucket_id):
    """
    Return a column expression that maps *bucket_id* (0 .. NUM_PARTITIONS-1)
    to a deterministic value appropriate for *data_type*, ensuring at most
    NUM_PARTITIONS distinct values are produced.
    """
    n = NUM_PARTITIONS

    if isinstance(data_type, (LongType, IntegerType, ShortType, ByteType)):
        stats = sample_df.agg(
            F.min(col_name).alias("min_val"),
            F.max(col_name).alias("max_val"),
        ).collect()[0]
        min_val = stats["min_val"] if stats["min_val"] is not None else 0
        max_val = stats["max_val"] if stats["max_val"] is not None else 1000000
        step = max((max_val - min_val) // n, 1)
        return (F.lit(min_val) + bucket_id * F.lit(step)).cast(data_type)

    if isinstance(data_type, (FloatType, DoubleType)):
        stats = sample_df.agg(
            F.min(col_name).alias("min_val"),
            F.max(col_name).alias("max_val"),
        ).collect()[0]
        min_val = float(stats["min_val"]) if stats["min_val"] is not None else 0.0
        max_val = float(stats["max_val"]) if stats["max_val"] is not None else 1000.0
        step = (max_val - min_val) / n
        return (F.lit(min_val) + bucket_id.cast("double") * F.lit(step)).cast(data_type)

    if isinstance(data_type, DecimalType):
        stats = sample_df.agg(
            F.min(col_name).alias("min_val"),
            F.max(col_name).alias("max_val"),
        ).collect()[0]
        min_val = float(stats["min_val"]) if stats["min_val"] is not None else 0.0
        max_val = float(stats["max_val"]) if stats["max_val"] is not None else 10000.0
        step = (max_val - min_val) / n
        return F.round(F.lit(min_val) + bucket_id.cast("double") * F.lit(step), data_type.scale).cast(data_type)

    if isinstance(data_type, TimestampType):
        stats = sample_df.agg(
            F.min(col_name).alias("min_val"),
            F.max(col_name).alias("max_val"),
        ).collect()[0]
        if stats["min_val"] is not None and stats["max_val"] is not None:
            min_ts = stats["min_val"]
            max_ts = stats["max_val"]
            range_secs = max(int((max_ts - min_ts).total_seconds()), 1)
        else:
            min_ts_str = "2024-01-01 00:00:00"
            range_secs = 730 * 86400
            return (
                F.lit(min_ts_str).cast("timestamp")
                + F.expr(f"make_interval(0,0,0,0,0, cast(({bucket_id}) * {range_secs // n} as int))")
            )
        step_secs = range_secs // n
        return (
            F.lit(min_ts.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp")
            + F.expr(f"make_interval(0,0,0,0,0, cast(({bucket_id}) * {step_secs} as int))")
        )

    if isinstance(data_type, DateType):
        stats = sample_df.agg(
            F.min(col_name).alias("min_val"),
            F.max(col_name).alias("max_val"),
        ).collect()[0]
        if stats["min_val"] is not None and stats["max_val"] is not None:
            min_d = stats["min_val"]
            range_days = max((stats["max_val"] - min_d).days, 1)
        else:
            min_d = "2024-01-01"
            range_days = 730
        step_days = max(range_days // n, 1)
        return F.date_add(F.lit(min_d), (bucket_id * F.lit(step_days)).cast("int"))

    if isinstance(data_type, StringType):
        distinct_vals = (
            sample_df.select(col_name)
            .filter(F.col(col_name).isNotNull())
            .distinct()
            .limit(n)
            .rdd.flatMap(lambda x: x)
            .collect()
        )
        if len(distinct_vals) > 0:
            vals = distinct_vals[:n]
            return F.element_at(
                F.array([F.lit(v) for v in vals]),
                (bucket_id % len(vals)).cast("int") + 1,
            )
        return F.concat(F.lit(f"{col_name}_"), (bucket_id % n).cast("string"))

    # Fallback: use the normal random generator (no cardinality limit)
    return build_synthetic_column(col_name, data_type, sample_df, n)


# ---------------------------------------------------------------------------
# Target Table Creation
# ---------------------------------------------------------------------------

def build_create_table_ddl(target_fqn, schema, target_partition):
    """Build a CREATE TABLE DDL string from the schema and partition spec."""
    col_defs = []
    for field in schema.fields:
        spark_type_str = field.dataType.simpleString()
        col_defs.append(f"    {field.name}  {spark_type_str}")

    columns_sql = ",\n".join(col_defs)
    partition_clause = f"PARTITIONED BY ({target_partition})" if target_partition.strip() else ""

    ddl = f"""
        CREATE TABLE IF NOT EXISTS {target_fqn} (
{columns_sql}
        )
        USING iceberg
        {partition_clause}
        OPTIONS (
            'format-version'='2'
        );
    """
    return ddl


def create_target_table(spark, target_fqn, schema, target_partition):
    """Drop (if exists) and create the target Iceberg table."""
    spark.sql(f"DROP TABLE IF EXISTS {target_fqn}")
    ddl = build_create_table_ddl(target_fqn, schema, target_partition)
    print(f"Creating target table with DDL:\n{ddl}")
    spark.sql(ddl)


# ---------------------------------------------------------------------------
# Parquet Staging
# ---------------------------------------------------------------------------

def write_parquet_staging(synthetic_df, staging_path, target_rows):
    """Write synthetic data to a Parquet dataset on S3."""
    print(f"  Staging path: {staging_path}")
    start = time.time()
    synthetic_df.write.mode("overwrite").parquet(staging_path)
    print(f"  Parquet staging write completed in {elapsed(start)}s — {target_rows:,} rows")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    params = get_params()
    spark = init_spark()

    source_fqn = table_fqn(params["database"], params["source_table"])
    target_fqn = table_fqn(params["database"], params["target_table"])
    target_partition = params["target_partition"]
    target_rows = params["target_rows"]
    staging_path = f"{params['staging_path']}/{params['target_table']}_parquet_staging"

    print("=" * 60)
    print("Synthetic Data Generator")
    print("=" * 60)
    print(f"  Source table     : {source_fqn}")
    print(f"  Target table     : {target_fqn}")
    print(f"  Target partition : {target_partition}")
    print(f"  Target rows      : {target_rows:,}")
    print(f"  Staging path     : {staging_path}")
    print()

    # Step 1: Read source schema
    print("Step 1: Reading source table schema...")
    schema = get_source_schema(spark, source_fqn)
    print(f"  Schema: {schema.simpleString()}")
    print(f"  Columns: {[f.name for f in schema.fields]}")

    # Step 2: Sample source data for profiling
    print("\nStep 2: Sampling source data for value profiling...")
    start = time.time()
    sample_df = get_source_sample(spark, source_fqn, sample_size=1000)
    sample_df.cache()
    sample_count = sample_df.count()
    print(f"  Sampled {sample_count} rows in {elapsed(start)}s")

    # Step 3: Create target Iceberg table
    print("\nStep 3: Creating target Iceberg table...")
    create_target_table(spark, target_fqn, schema, target_partition)
    print(f"  Target table created: {target_fqn}")

    # Step 4: Generate synthetic data
    print(f"\nStep 4: Generating {target_rows:,} synthetic rows...")
    start = time.time()
    synthetic_df = generate_synthetic_df(spark, schema, sample_df, target_rows, partition_spec=target_partition)
    print(f"  DataFrame built in {elapsed(start)}s (lazy — not materialised yet)")

    # Step 5: Write to staging Parquet
    print(f"\nStep 5: Writing synthetic data to staging Parquet...")
    start = time.time()
    write_parquet_staging(synthetic_df, staging_path, target_rows)
    parquet_time = elapsed(start)
    print(f"  Parquet staging completed in {parquet_time}s")

    # Cleanup
    sample_df.unpersist()
    print("\nSynthetic data generation complete.")
    print(f"Staging Parquet ready at: {staging_path}")


if __name__ == "__main__":
    main()
