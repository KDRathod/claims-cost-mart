{#
  Per member per month (PMPM) cost by payer: the headline number.

  Two versions are published side by side on purpose:
    pmpm_paid_to_date         what has actually been paid (moves for months)
    pmpm_estimated_ultimate   completion-adjusted estimate (stable sooner)

  Grain: service_month x payer
#}

with cost as (

    select
        service_month,
        payer_id,
        max(months_of_runout)          as months_of_runout,
        sum(paid_to_date)              as paid_to_date,
        sum(allowed_to_date)           as allowed_to_date,
        sum(estimated_ultimate_paid)   as estimated_ultimate_paid,
        sum(ibnr_estimate)             as ibnr_estimate
    from {{ ref('mart_cost_by_category_monthly') }}
    group by 1, 2

),

member_months as (

    select coverage_month, payer_id, sum(member_months) as member_months
    from {{ ref('fct_member_months') }}
    group by 1, 2

),

claim_counts as (

    select
        service_month,
        payer_id,
        count(*)                                                as claim_count,
        sum(case when claim_status = 'denied' then 1 else 0 end)  as denied_claim_count,
        sum(case when claim_status = 'pending' then 1 else 0 end) as pending_claim_count
    from {{ ref('fct_claims') }}
    group by 1, 2

)

select
    {{ dbt_utils.generate_surrogate_key(['m.coverage_month', 'm.payer_id']) }} as pmpm_month_id,
    m.coverage_month                                         as service_month,
    m.payer_id,
    p.payer_name,
    p.payer_type,
    m.member_months,
    coalesce(k.claim_count, 0)                               as claim_count,
    coalesce(k.denied_claim_count, 0)                        as denied_claim_count,
    coalesce(k.pending_claim_count, 0)                       as pending_claim_count,
    coalesce(c.months_of_runout, 0)                          as months_of_runout,
    {{ money('coalesce(c.paid_to_date, 0)') }}               as paid_to_date,
    {{ money('coalesce(c.allowed_to_date, 0)') }}            as allowed_to_date,
    {{ money('coalesce(c.estimated_ultimate_paid, 0)') }}    as estimated_ultimate_paid,
    {{ money('coalesce(c.ibnr_estimate, 0)') }}              as ibnr_estimate,
    {{ money('coalesce(c.paid_to_date, 0) / m.member_months') }}            as pmpm_paid_to_date,
    {{ money('coalesce(c.estimated_ultimate_paid, 0) / m.member_months') }} as pmpm_estimated_ultimate,
    coalesce(c.months_of_runout, 0) >= {{ var('mature_after_months') }}     as is_mature
from member_months m
left join cost c          on m.coverage_month = c.service_month and m.payer_id = c.payer_id
left join claim_counts k  on m.coverage_month = k.service_month and m.payer_id = k.payer_id
left join {{ ref('dim_payer') }} p on m.payer_id = p.payer_id
