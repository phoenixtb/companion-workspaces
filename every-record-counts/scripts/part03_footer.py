"""Part 3: open a Parquet file from the end, the way every reader does, with nothing but bytes and PyArrow.

uv run python scripts/part03_footer.py [data/lab/part03/by_date.parquet] [row group]
"""

import struct
import sys
from pathlib import Path

import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
path = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "data/lab/part03/by_date.parquet"
group = int(sys.argv[2]) if len(sys.argv) > 2 else 20
size = path.stat().st_size

# 1. The two reads a Parquet reader starts with: the last 8 bytes, then the footer they point to.
with path.open("rb") as f:
    head = f.read(4)
    f.seek(size - 8)
    tail = f.read(8)
footer_length = struct.unpack("<I", tail[:4])[0]  # little-endian 32-bit length, then the magic again
footer_start = size - 8 - footer_length
print(f"{path.relative_to(ROOT)}: {size:,} bytes")
print(f"{'first 4 bytes':<22}{head!r} (magic)")
print(f"{'last 8 bytes':<22}{tail.hex(' ')} -> footer length {footer_length:,}, then {tail[4:]!r}")
print(f"{'footer':<22}bytes {footer_start:,} to {size - 9:,} ({footer_length / size:.2%} of the file)")

# 2. The footer, decoded: the file's schema and a directory of every row group and column chunk.
meta = pq.ParquetFile(path).metadata
print(f"{'written by':<22}{meta.created_by}, format version {meta.format_version}")
print(f"{'rows, row groups':<22}{meta.num_rows:,} in {meta.num_row_groups} row groups of {meta.num_columns} columns")

rg = meta.row_group(group)
print(f"\nrow group {group}: {rg.num_rows:,} rows, {rg.total_byte_size:,} bytes of column chunks")
for name in ("business_date", "basket_id"):
    chunk = next(rg.column(i) for i in range(rg.num_columns) if rg.column(i).path_in_schema == name)
    s = chunk.statistics
    pages = "dictionary page + data pages" if chunk.has_dictionary_page else "data pages only"
    bloom = f"{chunk.bloom_filter_length} bytes" if chunk.bloom_filter_length else "none"
    page_index = "yes" if chunk.has_column_index or chunk.has_offset_index else "none"
    print(f"  {name}")
    start = chunk.dictionary_page_offset if chunk.has_dictionary_page else chunk.data_page_offset
    print(f"    {'at byte':<16}{start:,}: {pages}, {chunk.encodings}")
    print(f"    {'size':<16}{chunk.total_uncompressed_size:,} encoded -> {chunk.total_compressed_size:,} stored")
    print(f"    {'statistics':<16}min {s.min}, max {s.max}, nulls {s.null_count}, distinct {s.distinct_count}")
    print(f"    {'bloom filter':<16}{bloom}; page index: {page_index}")

# 3. Read two column chunks of one row group into Arrow: contiguous, uncompressed, ready to compute on.
table = pq.ParquetFile(path).read_row_group(group, columns=["business_date", "amount_minor"])
print(f"\nread_row_group({group}, 2 columns) -> Arrow table, {table.num_rows:,} rows, {table.nbytes:,} bytes in memory")
print(f"  {table.schema.field('business_date')}, {table.schema.field('amount_minor')}")
