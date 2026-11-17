{#
  The claims run-out triangle: for each service month, how much had been paid
  after 0, 1, 2 ... months. This is the table that answers "why did last
  month's number change?" because it stores what was known at every point.

  Grain: service_month x payer x service_category x lag_months (dense: a row
  exists for every lag up to the as-of month even when nothing was posted).
#}

with events as (

    select
        service_month,
        payer_id,
        service_category,
        lag_months,
        sum(paid_amount_delta)    as paid_in_lag,
        sum(allowed_amount_delta) as allowed_in_lag
    from {{ ref('fct_claim_events') }}
    where not is_orphan_event
    group by 1, 2, 3, 4

),

as_of as (
    select {{ month_start(as_of_date()) }} as as_of_month
),

cells as (
    select distinct service_month, payer_id, service_category from events
),

lags as (
    select cast(generated_number - 1 as integer) as lag_months
    from ({{ dbt_utils.generate_series(120) }}) g
),

grid as (

    select c.service_month, c.payer_id, c.service_category, l.lag_months
    from cells c
    cross join as_of a
    inner join lags l
        on l.lag_months <= {{ months_between('c.service_month', 'a.as_of_month') }}

),

filled as (

    select
        g.service_month,
        g.payer_id,
        g.service_category,
        g.lag_months,
        coalesce(e.paid_in_lag, 0)    as paid_in_lag,
        coalesce(e.allowed_in_lag, 0) as allowed_in_lag
    from grid g
    left join events e
        on  g.service_month    = e.service_month
        and g.payer_id         = e.payer_id
        and g.service_category = e.service_category
        and g.lag_months       = e.lag_months

)

select
    {{ dbt_utils.generate_surrogate_key(['service_month', 'payer_id', 'service_category', 'lag_months']) }} as triangle_cell_id,
    service_month,
    payer_id,
    service_category,
    lag_months,
    cast({{ dbt.dateadd('month', 'lag_months', 'service_month') }} as date) as known_as_of_month,
    {{ money('paid_in_lag') }} as paid_in_lag,
    {{ money('sum(paid_in_lag) over (
        partition by service_month, payer_id, service_category
        order by lag_months rows between unbounded preceding and current row)') }} as cumulative_paid,
    {{ money('sum(allowed_in_lag) over (
        partition by service_month, payer_id, service_category
        order by lag_months rows between unbounded preceding and current row)') }} as cumulative_allowed
from filled
