-- The raw zone as the pipeline sees it: only what the tills have uploaded so far (DuckDB).
-- landed_root is set by scripts/part02_pipeline.py.
SET TimeZone = 'UTC';

CREATE OR REPLACE TEMP VIEW landed_stores AS
SELECT * FROM read_csv(getvariable('landed_root') || '/reference/stores/stores.csv');

CREATE OR REPLACE TEMP VIEW landed_pos_events AS
SELECT * FROM read_json(getvariable('landed_root') || '/pos/events/*/*.jsonl.gz', hive_partitioning = true,
                                        maximum_sample_files = 1);
