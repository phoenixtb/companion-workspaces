{#- The few places where Trino and DuckDB spell the same thing differently. Models tagged `portable` use only
    these, so the same files build on the lake (Trino) and on a local DuckDB file (dbt-duckdb or dbt-oss). -#}

{#- The local calendar date of a UTC timestamp in an IANA zone: a business date. -#}
{% macro local_date(ts, tz) -%}
    {{ return(adapter.dispatch('local_date')(ts, tz)) }}
{%- endmacro %}

{% macro trino__local_date(ts, tz) -%}
    cast(at_timezone({{ ts }}, {{ tz }}) as date)
{%- endmacro %}

{% macro duckdb__local_date(ts, tz) -%}
    cast(timezone({{ tz }}, {{ ts }}) as date)
{%- endmacro %}

{#- Minor units in, minor units out, at a DECIMAL(18, 8) rate: half away from zero on the absolute amount, so
    converting -x gives exactly minus converting x (Part 5). No float touches money. -#}
{% macro convert_minor(amount_minor, rate) -%}
    cast(sign({{ amount_minor }}) * round(abs({{ amount_minor }}) * {{ rate }}) as bigint)
{%- endmacro %}

{#- Money for people: minor units as a DECIMAL in major units. -#}
{% macro major(amount_minor) -%}
    cast({{ amount_minor }} as decimal(18, 0)) * 0.01
{%- endmacro %}

{#- yyyymmdd as an integer: the date dimension's key. -#}
{% macro date_key(d) -%}
    (extract(year from {{ d }}) * 10000 + extract(month from {{ d }}) * 100 + extract(day from {{ d }}))
{%- endmacro %}

{#- ISO day of the week: Monday 1 ... Sunday 7. -#}
{% macro iso_day_of_week(d) -%}
    {{ return(adapter.dispatch('iso_day_of_week')(d)) }}
{%- endmacro %}

{% macro trino__iso_day_of_week(d) -%}
    day_of_week({{ d }})
{%- endmacro %}

{% macro duckdb__iso_day_of_week(d) -%}
    isodow({{ d }})
{%- endmacro %}
