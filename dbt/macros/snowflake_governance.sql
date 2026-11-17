{#
  Column masking and row access policies, applied as post-hooks.

  On Snowflake: PII columns in RESTRICTED.DIM_MEMBER_PII are masked for every
  role except CLAIMS_PII_READER, and the claims fact is filtered by payer for
  roles mapped in the PAYER_ACCESS seed. On DuckDB these are no-ops: there is
  no role system, so the protection there is structural (PII lives in its own
  schema and the marts only carry de-identified columns).
#}
{% macro apply_masking_policy(column, policy='PII_STRING_MASK') -%}
    {%- if target.type == 'snowflake' -%}
        alter table {{ this }} modify column {{ column }}
            set masking policy {{ target.database }}.GOVERNANCE.{{ policy }} force
    {%- else -%}
        select 1
    {%- endif -%}
{%- endmacro %}

{% macro apply_row_access_policy(column, policy='PAYER_ROW_ACCESS') -%}
    {%- if target.type == 'snowflake' -%}
        alter table {{ this }} add row access policy {{ target.database }}.GOVERNANCE.{{ policy }} on ({{ column }})
    {%- else -%}
        select 1
    {%- endif -%}
{%- endmacro %}
