{#
  Completion factor = share of a service month's final cost that has usually
  been paid after N months of run-out.

  Built only from MATURE service months (at least `mature_after_months` of
  run-out), where we actually know the final number. Volume-weighted across
  months so one odd month cannot swing the factor. Kept per service category
  because a hospital stay settles much more slowly than an office visit.

  Grain: service_category x lag_months
#}

with triangle as (
    select * from {{ ref('mart_claims_triangle') }}
),

as_of as (
    select {{ month_start(as_of_date()) }} as as_of_month
),

by_category as (

    select service_month, service_category, lag_months, sum(cumulative_paid) as cumulative_paid
    from triangle
    group by 1, 2, 3

),

mature as (

    select b.*
    from by_category b
    cross join as_of a
    where {{ months_between('b.service_month', 'a.as_of_month') }} >= {{ var('mature_after_months') }}
      and b.lag_months <= {{ var('mature_after_months') }}

),

ultimate as (
    -- what each mature month had paid by the maturity point
    select service_month, service_category, cumulative_paid as ultimate_paid
    from mature
    where lag_months = {{ var('mature_after_months') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['m.service_category', 'm.lag_months']) }} as completion_factor_id,
    m.service_category,
    m.lag_months,
    count(distinct m.service_month)                         as mature_months_used,
    {{ money('sum(m.cumulative_paid)') }}                   as cumulative_paid_at_lag,
    {{ money('sum(u.ultimate_paid)') }}                     as ultimate_paid,
    cast(sum(m.cumulative_paid) / nullif(sum(u.ultimate_paid), 0) as decimal(9, 6)) as completion_factor
from mature m
inner join ultimate u
    on m.service_month = u.service_month and m.service_category = u.service_category
group by m.service_category, m.lag_months
