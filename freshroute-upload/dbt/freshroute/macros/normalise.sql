{# ' FAILED_-_not home ', 'Delivereds', 'N/A' -> 'failed not home', 'delivereds', 'n a'
   Then joined to the seed category_mapping, which says what each spelling really means. #}
{% macro normalise_category(col) -%}
    trim(regexp_replace(lower(coalesce({{ col }}, '')), '[^a-z]+', ' '))
{%- endmacro %}

{# '5,6633', ' 25.7963 units', '-8.8' -> 5.6633, 25.7963, -8.8 #}
{% macro clean_float(col) -%}
    try_to_number(replace(regexp_substr({{ col }}, '-?[0-9]+([.,][0-9]+)?'), ',', '.'), 12, 4)
{%- endmacro %}
