{% macro month_start(col) -%}
    cast(date_trunc('month', {{ col }}) as date)
{%- endmacro %}

{% macro months_between(start_col, end_col) -%}
    {{ dbt.datediff(start_col, end_col, 'month') }}
{%- endmacro %}

{# Money is stored as fixed-point so sums reconcile to the cent. #}
{% macro money(expr) -%}
    cast({{ expr }} as decimal(18, 2))
{%- endmacro %}
