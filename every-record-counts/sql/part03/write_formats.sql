-- Part 3: the same sales lines (sql/part01/sales_line.sql, sorted by basket_id) in four formats
-- and several Parquet layouts (DuckDB). scripts/part03_formats.py fills in {out}.

-- Row formats: CSV (text, header only), JSON Lines (text, every key on every row), Avro (binary, one schema per file).
COPY sales_line TO '{out}/sales_line.csv' (FORMAT csv, HEADER);
COPY sales_line TO '{out}/sales_line.csv.gz' (FORMAT csv, HEADER, COMPRESSION gzip);
COPY sales_line TO '{out}/sales_line.jsonl' (FORMAT json);
COPY sales_line TO '{out}/sales_line.jsonl.gz' (FORMAT json, COMPRESSION gzip);
COPY sales_line TO '{out}/sales_line.avro' (FORMAT avro);
COPY sales_line TO '{out}/sales_line.deflate.avro' (FORMAT avro, CODEC 'deflate');

-- Parquet as Part 1 wrote it: DuckDB's defaults (Snappy, row groups of 122,880 rows), then two other codecs.
COPY sales_line TO '{out}/sales_line.parquet' (FORMAT parquet);
COPY sales_line TO '{out}/sales_line.zstd.parquet' (FORMAT parquet, COMPRESSION zstd);
COPY sales_line TO '{out}/sales_line.uncompressed.parquet' (FORMAT parquet, COMPRESSION uncompressed);

-- Sorted for the questions finance asks (by business date, then store), with three row-group sizes.
COPY (FROM sales_line ORDER BY business_date, store_id, basket_id, line_no)
TO '{out}/by_date.parquet' (FORMAT parquet);
COPY (FROM sales_line ORDER BY business_date, store_id, basket_id, line_no)
TO '{out}/by_date.rg10k.parquet' (FORMAT parquet, ROW_GROUP_SIZE 10000);
COPY (FROM sales_line ORDER BY business_date, store_id, basket_id, line_no)
TO '{out}/by_date.rg1m.parquet' (FORMAT parquet, ROW_GROUP_SIZE 1000000);

-- One folder per store, one small file in each.
COPY sales_line TO '{out}/per_store' (FORMAT parquet, PARTITION_BY (store_id));
