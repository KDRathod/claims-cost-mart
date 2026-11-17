{#
  Honest test of the completion-factor estimate: would it have been right?

  For every service month that is now mature (so the final cost is known),
  go back to what was known after 0..5 months of run-out and re-create the
  estimate using ONLY information available at that time:

    * paid so far for that month, and
    * completion factors built from the 12 service months that were already
      mature on that date (no peeking at the future).

  Then compare two answers to the real final cost:
    naive      "cost = what has been paid so far"
    estimate   paid so far / completion factor

  Grain: service_month x months_of_runout
#}

{% set horizon = var('mature_after_months') %}

with triangle as (

    select service_month, service_category, lag_months, sum(cumulative_paid) as cumulative_paid
    from {{ ref('mart_claims_triangle') }}
    where lag_months <= {{ horizon }}
    group by 1, 2, 3

),

ultimate as (
    select service_month, service_category, cumulative_paid as ultimate_paid
    from triangle
    where lag_months = {{ horizon }}
),

targets as (

    select
        t.service_month,
        t.service_category,
        t.lag_months,
        t.cumulative_paid                                                    as paid_at_lag,
        u.ultimate_paid,
        cast({{ dbt.dateadd('month', 't.lag_months', 't.service_month') }} as date) as observed_month
    from triangle t
    inner join ultimate u
        on t.service_month = u.service_month and t.service_category = u.service_category
    where t.lag_months <= 5

),

factors_known_then as (

    -- history months that had already reached maturity on the observation date
    select
        tg.service_month,
        tg.service_category,
        tg.lag_months,
        count(distinct h.service_month)   as history_months,
        sum(h.cumulative_paid)            as history_paid_at_lag,
        sum(hu.ultimate_paid)             as history_ultimate
    from targets tg
    inner join triangle h
        on  h.service_category = tg.service_category
        and h.lag_months       = tg.lag_months
        and h.service_month   <= cast({{ dbt.dateadd('month', -horizon, 'tg.observed_month') }} as date)
        and h.service_month   >  cast({{ dbt.dateadd('month', -(horizon + 12), 'tg.observed_month') }} as date)
    inner join ultimate hu
        on h.service_month = hu.service_month and h.service_category = hu.service_category
    group by 1, 2, 3

),

by_category as (

    select
        tg.service_month,
        tg.lag_months,
        tg.paid_at_lag,
        tg.ultimate_paid,
        f.history_months,
        tg.paid_at_lag / nullif(f.history_paid_at_lag / nullif(f.history_ultimate, 0), 0) as estimated_ultimate
    from targets tg
    inner join factors_known_then f
        on  tg.service_month    = f.service_month
        and tg.service_category = f.service_category
        and tg.lag_months       = f.lag_months
    -- need a reasonable amount of history before trusting a factor
    where f.history_months >= 6

),

rolled as (

    select
        service_month,
        lag_months                  as months_of_runout,
        min(history_months)         as history_months_used,
        sum(paid_at_lag)            as paid_to_date_then,
        sum(estimated_ultimate)     as estimated_ultimate_then,
        sum(ultimate_paid)          as actual_ultimate
    from by_category
    group by 1, 2
    -- only keep months where every service category could be estimated
    having count(*) = (select count(distinct service_category) from targets)

)

select
    {{ dbt_utils.generate_surrogate_key(['service_month', 'months_of_runout']) }} as backtest_id,
    service_month,
    months_of_runout,
    history_months_used,
    {{ money('paid_to_date_then') }}       as paid_to_date_then,
    {{ money('estimated_ultimate_then') }} as estimated_ultimate_then,
    {{ money('actual_ultimate') }}         as actual_ultimate,
    cast((paid_to_date_then - actual_ultimate) / nullif(actual_ultimate, 0) as decimal(9, 4))       as naive_error_pct,
    cast((estimated_ultimate_then - actual_ultimate) / nullif(actual_ultimate, 0) as decimal(9, 4)) as estimate_error_pct
from rolled
