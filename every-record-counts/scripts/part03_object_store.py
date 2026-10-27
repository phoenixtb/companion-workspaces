"""Part 3: what object storage offers instead of a filesystem, through SeaweedFS's S3 API.

uv run python scripts/part03_object_store.py      # needs `make up-core` (and `make upload` for step 3)

1. Conditional writes: create-if-absent and compare-and-swap, the primitives a table commit is built on.
2. No rename: "moving" an object is a copy and a delete, and both copies exist in between.
3. LIST is paged: at most 1,000 keys per request.
"""

import botocore.session
from botocore.config import Config
from botocore.exceptions import ClientError

BUCKET = "larkspur-lake"
PREFIX = "part03/object-store-demo/"

s3 = botocore.session.get_session().create_client(
    "s3",
    endpoint_url="http://localhost:8333",
    aws_access_key_id="larkspur",
    aws_secret_access_key="larkspur-lab-secret",
    region_name="us-east-1",
    config=Config(s3={"addressing_style": "path"}),
)


def put(key: str, body: bytes, **condition: str) -> str:
    """PUT an object; return its new ETag, or the error code if a precondition failed."""
    try:
        return s3.put_object(Bucket=BUCKET, Key=PREFIX + key, Body=body, **condition)["ETag"]
    except ClientError as e:
        return f"{e.response['Error']['Code']} (HTTP {e.response['ResponseMetadata']['HTTPStatusCode']})"


def keys(prefix: str) -> list[str]:
    return [o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix).get("Contents", [])]


for key in keys(PREFIX):  # start clean, so every run prints the same thing
    s3.delete_object(Bucket=BUCKET, Key=key)

print("1a. Two writers create the same commit file, If-None-Match: *")
print(f"    writer A: {put('commits/00001.json', b'{"writer": "A"}', IfNoneMatch='*')}")
print(f"    writer B: {put('commits/00001.json', b'{"writer": "B"}', IfNoneMatch='*')}")
body = s3.get_object(Bucket=BUCKET, Key=PREFIX + "commits/00001.json")["Body"].read().decode()
print(f"    the object holds {body}")

print("1b. Two writers move the same pointer from version 1, If-Match: <ETag they read>")
v1 = put("current.json", b'{"version": 1}')
print(f"    both read version 1, ETag {v1}")
print(f"    writer A -> version 2: {put('current.json', b'{"version": 2, "writer": "A"}', IfMatch=v1)}")
print(f"    writer B -> version 2: {put('current.json', b'{"version": 2, "writer": "B"}', IfMatch=v1)}")
body = s3.get_object(Bucket=BUCKET, Key=PREFIX + "current.json")["Body"].read().decode()
print(f"    the pointer holds {body}; B must re-read, rebase and retry")

print("2. 'Rename' staging/part-0.parquet to data/part-0.parquet")
put("staging/part-0.parquet", b"PAR1...PAR1")
s3.copy_object(Bucket=BUCKET, Key=PREFIX + "data/part-0.parquet", CopySource=f"{BUCKET}/{PREFIX}staging/part-0.parquet")
print(f"    after CopyObject:   {[k.removeprefix(PREFIX) for k in keys(PREFIX + 'data/') + keys(PREFIX + 'staging/')]}")
s3.delete_object(Bucket=BUCKET, Key=PREFIX + "staging/part-0.parquet")
print(f"    after DeleteObject: {[k.removeprefix(PREFIX) for k in keys(PREFIX + 'data/') + keys(PREFIX + 'staging/')]}")

print("3. LIST requests to enumerate a prefix (ListObjectsV2, 1,000 keys per page)")
for prefix in ("raw/pos/events/", "raw/"):
    pages = list(s3.get_paginator("list_objects_v2").paginate(Bucket="larkspur-raw", Prefix=prefix))
    count = sum(len(p.get("Contents", [])) for p in pages)
    requests = f"{len(pages)} LIST request" + ("s" if len(pages) > 1 else "")
    print(f"    s3://larkspur-raw/{prefix:<16} {count:>5,} objects, {requests}")
