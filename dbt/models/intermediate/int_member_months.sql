{#
  One row per member, payer and calendar month in which the member had at
  least one covered day. This is the denominator of every PMPM number.
#}

with spans as (
    select * from {{ ref('stg_enrollment') }}
),

bounds as (
    select
        {{ month_start('min(coverage_start_date)') }} as first_month,
        {{ month_start(as_of_date()) }}                as last_month
    from spans
),

months as (

    {{ dbt_utils.date_spine(
        datepart="month",
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2030-01-01' as date)"
    ) }}

),

month_spine as (
    select cast(m.date_month as date) as coverage_month
    from months m
    cross join bounds b
    where cast(m.date_month as date) between b.first_month and b.last_month
),

covered as (

    select
        s.member_id,
        s.payer_id,
        m.coverage_month,
        -- a member can have two spans with the same payer inside one month
        min(s.coverage_start_date) as first_covered_date,
        max(s.coverage_end_date)   as last_covered_date
    from spans s
    inner join month_spine m
        on s.coverage_start_date < {{ dbt.dateadd('month', 1, 'm.coverage_month') }}
       and s.coverage_end_date  >= m.coverage_month
    group by s.member_id, s.payer_id, m.coverage_month

)

select
    {{ dbt_utils.generate_surrogate_key(['member_id', 'payer_id', 'coverage_month']) }} as member_month_id,
    member_id,
    payer_id,
    coverage_month,
    1 as member_months
from covered
