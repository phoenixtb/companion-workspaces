{#- Models go to the namespace named in their config (silver, gold) as is, not "<target schema>_<name>",
    dbt's default. The namespaces exist already: in Polaris only the platform team creates them (Part 9). -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name | trim if custom_schema_name is not none else target.schema }}
{%- endmacro %}
