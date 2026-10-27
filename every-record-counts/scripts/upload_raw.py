"""Copy the generated raw zone (data/raw) into the larkspur-raw bucket on SeaweedFS."""

import sys
import time
from pathlib import Path

import s3fs

LOCAL = Path(sys.argv[1] if len(sys.argv) > 1 else "data/raw")
fs = s3fs.S3FileSystem(
    key="larkspur",
    secret="larkspur-lab-secret",
    client_kwargs={"endpoint_url": "http://localhost:8333", "region_name": "us-east-1"},
)
started = time.monotonic()
if fs.exists("larkspur-raw/raw"):
    fs.rm("larkspur-raw/raw", recursive=True)
fs.put(str(LOCAL) + "/", "larkspur-raw/raw/", recursive=True)
files = [p for p in LOCAL.rglob("*") if p.is_file()]
print(
    f"uploaded {len(files)} files, {sum(p.stat().st_size for p in files) / 1e6:.0f} MB "
    f"to s3://larkspur-raw/raw/ in {time.monotonic() - started:.0f}s"
)
