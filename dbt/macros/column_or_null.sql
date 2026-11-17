{#
  Select a column if the source table has it, otherwise a typed NULL.

  The events feed gained a column partway through its life. A staging model
  that hard-codes the column fails on any warehouse built before the change
  (and on every day-by-day replay of history). Checking the real table at
  compile time makes the model work on both sides of a schema change.
#}
{% macro column_or_null(relation, column_name, data_type='varchar') -%}
    {%- set found = [] -%}
    {%- if execute -%}
        {%- for col in adapter.get_columns_in_relation(relation) -%}
            {%- if col.name | lower == column_name | lower -%}{%- do found.append(1) -%}{%- endif -%}
        {%- endfor -%}
    {%- endif -%}
    {%- if found -%}{{ column_name }}{%- else -%}cast(null as {{ data_type }}){%- endif -%}
{%- endmacro %}
