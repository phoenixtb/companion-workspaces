-- Part 28: a mistake and how Flink answers it, before any data moves. Run with: python -m scripts.part28_flink errors
SET 'execution.runtime-mode' = 'streaming';

-- 3. The namespace stream without backticks: stream is a reserved word in Flink SQL.
SELECT COUNT(*) FROM lake.stream.store_sales_daily;
