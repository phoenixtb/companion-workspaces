{#- Grain test for big facts: like dbt_utils.unique_combination_of_columns, but it groups by a 64-bit hash of the
    combination instead of the columns. Two 32-character surrogate keys per row made the plain version run past the
    lab Trino node's per-query memory on 11.7 million rows. A duplicate always fails it; a hash collision would
    raise a false alarm (never hide a duplicate), and at 2^64 that's not a practical worry at this size. -#}
{% test unique_combination_hashed(model, combination_of_columns) %}
with keyed as (
    select from_big_endian_64(xxhash64(to_utf8(concat_ws('|',
        {%- for col in combination_of_columns %}
            coalesce(cast({{ col }} as varchar), '<null>'){{ ',' if not loop.last }}
        {%- endfor %}
    )))) as key_hash
    from {{ model }}
)
select key_hash, count(*) as n
from keyed
group by key_hash
having count(*) > 1
{% endtest %}
