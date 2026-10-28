-- Transaction fact. Grain: one row per line of a POS basket or an online order.
-- Microbatch (Part 18): one batch per UTC day of event_ts. Each run rebuilds the current day and the two before it
-- (lookback 2), because a till can upload up to 16.5 hours late and the run happens after midnight UTC.
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='event_ts',
    batch_size='day',
    lookback=2,
    begin='2026-05-31',
    on_schema_change='fail'
) }}
{{ sales_line_fact() }}
