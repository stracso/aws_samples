import sys
import boto3
from urllib.parse import urlparse
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

required_args = ["JOB_NAME", "s3_uri"]
optional_args = ["dryrun"]

resolved = getResolvedOptions(sys.argv, required_args)
for opt in optional_args:
    if f"--{opt}" in sys.argv:
        resolved.update(getResolvedOptions(sys.argv, [opt]))

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(resolved["JOB_NAME"], resolved)

s3_uri = resolved["s3_uri"].rstrip("/")
parsed = urlparse(s3_uri)
bucket = parsed.netloc
prefix = parsed.path.lstrip("/")

dryrun = resolved.get("dryrun", "False").lower() == "true"

s3 = boto3.client("s3")
paginator = s3.get_paginator("list_objects_v2")

DELETE_BATCH_SIZE = 1000  # S3 delete_objects supports up to 1000 keys per call


def purge_objects(bucket, prefix):
    """Delete all objects under the given bucket/prefix.

    Uses batched delete_objects calls for efficiency.
    Returns the total number of objects deleted.
    """
    total_deleted = 0
    batch = []

    paginate_args = {"Bucket": bucket}
    if prefix:
        paginate_args["Prefix"] = prefix

    for page in paginator.paginate(**paginate_args):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if dryrun:
                print(f"[DRYRUN] Would delete s3://{bucket}/{key} ({obj['Size']} bytes)")
                total_deleted += 1
            else:
                batch.append({"Key": key})
                if len(batch) >= DELETE_BATCH_SIZE:
                    s3.delete_objects(
                        Bucket=bucket,
                        Delete={"Objects": batch, "Quiet": True},
                    )
                    total_deleted += len(batch)
                    print(f"Deleted batch of {len(batch)} objects ({total_deleted} total so far)")
                    batch = []

    # flush remaining batch
    if batch and not dryrun:
        s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": batch, "Quiet": True},
        )
        total_deleted += len(batch)
        print(f"Deleted final batch of {len(batch)} objects ({total_deleted} total)")

    return total_deleted


deleted = purge_objects(bucket, prefix)

if dryrun:
    print(f"[DRYRUN] Would delete {deleted} objects from {s3_uri}")
else:
    print(f"Deleted {deleted} objects from {s3_uri}")

job.commit()
