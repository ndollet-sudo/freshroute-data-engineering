-- Reusable cleaning rules: write them once, use them in every model.

{% macro clean_text(col) -%}
    iff(upper(trim({{ col }})) in ('', 'N/A', 'NULL', '-'), null, trim({{ col }}))
{%- endmacro %}

{% macro clean_int(col) -%}
    try_to_number(regexp_substr({{ col }}, '-?[0-9]+'))
{%- endmacro %}

{# '£1,234.50', '12.50 GBP', '€9.90', '12,50'  ->  number #}
{% macro clean_money(col) -%}
    try_to_number(replace(
        iff(regexp_like(trim({{ col }}), '^-?[0-9]+,[0-9]{1,2}$'), replace(trim({{ col }}), ',', '.'), replace(trim({{ col }}), ',', '')),
        ' ', ''), 12, 2)
{%- endmacro %}

{# ISO with Z or offset, 'yyyy-mm-dd hh:mi:ss' (UTC), 'dd/mm/yyyy hh:mi', epoch seconds. 1970 / 2099 = defaults -> null #}
{% macro clean_ts(col) -%}
    {%- set parsed -%}
    coalesce(
        try_to_timestamp_tz(trim({{ col }}), 'YYYY-MM-DD"T"HH24:MI:SSTZH:TZM'),
        try_to_timestamp_tz(replace(trim({{ col }}), 'Z', '+00:00'), 'YYYY-MM-DD"T"HH24:MI:SSTZH:TZM'),
        try_to_timestamp_tz(trim({{ col }}) || ' +00:00', 'YYYY-MM-DD HH24:MI:SS TZH:TZM'),
        try_to_timestamp_tz(trim({{ col }}) || ' +00:00', 'DD/MM/YYYY HH24:MI TZH:TZM'),
        iff(regexp_like(trim({{ col }}), '^[0-9]{9,10}$'), to_timestamp_tz(try_to_number(trim({{ col }}))), null))
    {%- endset -%}
    iff(year({{ parsed }}) in (1970, 2099), null, {{ parsed }})
{%- endmacro %}
