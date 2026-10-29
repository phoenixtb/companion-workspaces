-- Part 24: the month's FOCUS-shaped rows, summed by the data_product tag (DuckDB). Rows without one are shared costs:
-- storage, which no tag in the lab assigns to a product yet.
SELECT coalesce(json_extract_string(Tags, '$.data_product'), '(shared: ' || ServiceCategory || ')') AS data_product,
       round(sum(EffectiveCost), 2) AS usd,
       round(100 * sum(EffectiveCost) / sum(sum(EffectiveCost)) OVER (), 1) AS pct
FROM read_parquet('{path}')
GROUP BY 1
ORDER BY usd DESC;
