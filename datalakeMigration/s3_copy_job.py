import sys
import boto3
from urllib.parse import urlparse
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

required_args = ["JOB_NAME", "target_uri"]
optional_args = ["source_uri", "database_name", "dryrun"]

resolved = getResolvedOptions(sys.argv, required_args)
for opt in optional_args:
    if f"--{opt}" in sys.argv:
        resolved.update(getResolvedOptions(sys.argv, [opt]))

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(resolved["JOB_NAME"], resolved)

target_uri = resolved["target_uri"].rstrip("/")
target_parsed = urlparse(target_uri)
target_bucket = target_parsed.netloc
target_prefix = target_parsed.path.lstrip("/")

database_name = resolved.get("database_name")
source_uri = resolved.get("source_uri")
dryrun = resolved.get("dryrun", "False").lower() == "true"

MULTIPART_THRESHOLD = 5 * 1024 * 1024 * 1024  # 5 GB
PART_SIZE = 1 * 1024 * 1024 * 1024  # 1 GB parts

s3 = boto3.client("s3")
paginator = s3.get_paginator("list_objects_v2")


def multipart_copy(source_bucket, source_key, target_bucket, target_key, size):
    mpu = s3.create_multipart_upload(Bucket=target_bucket, Key=target_key)
    upload_id = mpu["UploadId"]
    parts = []
    try:
        offset = 0
        part_number = 1
        while offset < size:
            end = min(offset + PART_SIZE, size) - 1
            range_str = f"bytes={offset}-{end}"
            resp = s3.upload_part_copy(
                Bucket=target_bucket,
                Key=target_key,
                CopySource={"Bucket": source_bucket, "Key": source_key},
                CopySourceRange=range_str,
                PartNumber=part_number,
                UploadId=upload_id,
            )
            parts.append({"ETag": resp["CopyPartResult"]["ETag"], "PartNumber": part_number})
            offset += PART_SIZE
            part_number += 1
        s3.complete_multipart_upload(
            Bucket=target_bucket,
            Key=target_key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
    except Exception:
        s3.abort_multipart_upload(Bucket=target_bucket, Key=target_key, UploadId=upload_id)
        raise


def copy_objects(src_bucket, src_prefix, tgt_bucket, tgt_prefix):
    count = 0
    for page in paginator.paginate(Bucket=src_bucket, Prefix=src_prefix):
        for obj in page.get("Contents", []):
            source_key = obj["Key"]
            relative_path = source_key[len(src_prefix):].lstrip("/")
            if not relative_path:
                continue
            target_key = f"{tgt_prefix}/{relative_path}" if tgt_prefix else relative_path

            if dryrun:
                print(f"[DRYRUN] s3://{src_bucket}/{source_key} -> s3://{tgt_bucket}/{target_key} ({obj['Size']} bytes)")
            elif obj["Size"] >= MULTIPART_THRESHOLD:
                multipart_copy(src_bucket, source_key, tgt_bucket, target_key, obj["Size"])
            else:
                copy_source = {"Bucket": src_bucket, "Key": source_key}
                s3.copy_object(CopySource=copy_source, Bucket=tgt_bucket, Key=target_key)
            count += 1
    return count


copied = 0

if database_name:
    glue_client = boto3.client("glue")
    table_paginator = glue_client.get_paginator("get_tables")
    for page in table_paginator.paginate(DatabaseName=database_name):
        for table in page["TableList"]:
            table_name = table["Name"]
            location = table.get("StorageDescriptor", {}).get("Location", "")
            if not location:
                print(f"Skipping table {table_name}: no S3 location found")
                continue
            location = location.rstrip("/")
            parsed = urlparse(location)
            src_bucket = parsed.netloc
            src_prefix = parsed.path.lstrip("/")
            tgt_prefix_for_table = f"{target_prefix}/{table_name}" if target_prefix else table_name
            count = copy_objects(src_bucket, src_prefix, target_bucket, tgt_prefix_for_table)
            print(f"Copied {count} objects for table {table_name}")
            copied += count
    print(f"Copied {copied} total objects from database {database_name} to {target_uri}")
else:
    if not source_uri:
        raise ValueError("Either --source_uri or --database_name must be provided")
    source_uri = source_uri.rstrip("/")
    source_parsed = urlparse(source_uri)
    source_bucket = source_parsed.netloc
    source_prefix = source_parsed.path.lstrip("/")
    copied = copy_objects(source_bucket, source_prefix, target_bucket, target_prefix)
    print(f"Copied {copied} objects from {source_uri} to {target_uri}")

job.commit()
