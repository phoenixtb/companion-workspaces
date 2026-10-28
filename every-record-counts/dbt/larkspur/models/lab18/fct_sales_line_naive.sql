-- Part 18's pitfall, kept for the replay (enabled with --vars '{part18_lab: true}', dev target only).
-- Append everything newer than the newest event already loaded: the high-water mark is event time.
{{ config(enabled=var('part18_lab', false), materialized='incremental', incremental_strategy='append') }}
{{ sales_line_fact() }}
{% if is_incremental() %}
where l.event_ts > (select max(event_ts) from {{ this }})
{% endif %}
