{#- dbt-trino's docs require this for snapshots on Iceberg: its default, current_timestamp, is a TIMESTAMP(3) WITH
    TIME ZONE and Iceberg stores microseconds. In the lab, Trino 484 widened the type itself and snapshots ran without
    the override; it's kept anyway. -#}
{% macro trino__current_timestamp() -%}
    current_timestamp(6)
{%- endmacro %}
