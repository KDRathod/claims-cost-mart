{#
  Prod (target.name == 'snowflake' with no CI prefix) writes to clean schema
  names: STAGING, MARTS, RESTRICTED. CI and dev runs set DBT_SCHEMA_PREFIX
  (for example ci_pr_42) so every pull request builds in its own sandbox and
  can never overwrite production tables.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set prefix = env_var('DBT_SCHEMA_PREFIX', '') -%}
    {%- set base = custom_schema_name | trim if custom_schema_name is not none else target.schema -%}
    {%- if prefix -%}{{ prefix }}_{{ base }}{%- else -%}{{ base }}{%- endif -%}
{%- endmacro %}
