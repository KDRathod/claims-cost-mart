{#
  The warehouse's notion of "today".

  Using current_date would make every number depend on the day you happen to
  run dbt, which makes tests flaky and backfills wrong. Instead "today" is
  the latest date we have data for, or an explicit --vars override.
#}
{% macro as_of_date() -%}
    {%- if var('as_of_date') is not none -%}
        cast('{{ var("as_of_date") }}' as date)
    {%- else -%}
        (select max(posted_date) from {{ ref('stg_claim_events') }})
    {%- endif -%}
{%- endmacro %}
