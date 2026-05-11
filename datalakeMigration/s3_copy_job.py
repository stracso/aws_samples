import sys
import boto3
from urllib.parse import urlparse
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME", "source_uri", "target_uri"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

source_uri = args["source_uri"].rstrip("/")
target_uri = args["target_uri"].rstrip("/")

source_parsed = urlparse(source_uri)
source_bucket = source_parsed.netloc
source_prefix = source_parsed.path.lstrip("/")

target_parsed = urlparse(target_uri)
target_bucket = target_parsed.netloc
target_prefix = target_parsed.path.lstrip("/")

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


copied = 0
for page in paginator.paginate(Bucket=source_bucket, Prefix=source_prefix):
    for obj in page.get("Contents", []):
        source_key = obj["Key"]
        relative_path = source_key[len(source_prefix):].lstrip("/")
        if not relative_path:
            continue
        target_key = f"{target_prefix}/{relative_path}" if target_prefix else relative_path

        if obj["Size"] >= MULTIPART_THRESHOLD:
            multipart_copy(source_bucket, source_key, target_bucket, target_key, obj["Size"])
        else:
            copy_source = {"Bucket": source_bucket, "Key": source_key}
            s3.copy_object(CopySource=copy_source, Bucket=target_bucket, Key=target_key)
        copied += 1

print(f"Copied {copied} objects from {source_uri} to {target_uri}")

job.commit()
