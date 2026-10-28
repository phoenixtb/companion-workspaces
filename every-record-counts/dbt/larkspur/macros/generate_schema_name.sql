{#- In production, models go to the namespace named in their config (silver, gold) as is, not "<target schema>_<name>",
    dbt's default. The namespaces exist already: in Polaris only the platform team creates them (Part 9).
    Any other target (dev, CI) puts every model in its own schema, so a build there never touches production. -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if target.name == 'prod' and custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}
    {%- endif -%}
{%- endmacro %}
