-- Part 18's reference: the sales fact's SELECT as a plain table, rebuilt from everything landed (enabled with
-- part18_lab, dev target only). `scripts.part18_dbt compare` checks each incremental variant against it.
{{ config(enabled=var('part18_lab', false), materialized='table') }}
{{ sales_line_fact() }}
