-- Part 18's second pitfall: microbatch with dbt's default lookback of one batch (enabled with part18_lab).
{{ config(
    enabled=var('part18_lab', false),
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='event_ts',
    batch_size='day',
    lookback=1,
    begin='2026-05-31'
) }}
{{ sales_line_fact() }}
