{#
  Paid to date, plus an estimate of where each service month will finish.

      estimated_ultimate_paid = paid_to_date / completion_factor
      ibnr_estimate           = estimated_ultimate_paid - paid_to_date

  IBNR = "incurred but not reported": care that already happened but has not
  been paid yet. Recent months look cheap only because the bills have not
  arrived, so reporting paid-to-date alone understates cost.

  Grain: service_month x payer x service_category
#}

with triangle as (
    select * from {{ ref('mart_claims_triangle') }}
),

latest as (
    -- the most recent diagonal: what we know today
    select *
    from triangle
    qualify row_number() over (
        partition by service_month, payer_id, service_category order by lag_months desc
    ) = 1
),

factors as (
    select * from {{ ref('mart_completion_factors') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['l.service_month', 'l.payer_id', 'l.service_category']) }} as cost_month_id,
    l.service_month,
    l.payer_id,
    l.service_category,
    l.lag_months                                             as months_of_runout,
    l.cumulative_paid                                        as paid_to_date,
    l.cumulative_allowed                                     as allowed_to_date,
    coalesce(f.completion_factor, 1)                         as completion_factor,
    l.lag_months >= {{ var('mature_after_months') }}         as is_mature,
    case
        when l.lag_months >= {{ var('mature_after_months') }} then 'final'
        when coalesce(f.completion_factor, 1) >= 0.90 then 'high'
        when coalesce(f.completion_factor, 1) >= 0.60 then 'medium'
        else 'low'
    end                                                      as estimate_reliability,
    {{ money('l.cumulative_paid / nullif(coalesce(f.completion_factor, 1), 0)') }} as estimated_ultimate_paid,
    {{ money('l.cumulative_paid / nullif(coalesce(f.completion_factor, 1), 0) - l.cumulative_paid') }} as ibnr_estimate
from latest l
left join factors f
    on l.service_category = f.service_category and l.lag_months = f.lag_months
