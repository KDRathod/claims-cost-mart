{{ config(severity='warn') }}
-- PMPM is driven from member months. Paid dollars for a payer-month with no
-- member months would vanish from the headline number. Report any gap.
select c.service_month, c.payer_id, sum(c.paid_to_date) as paid_not_in_pmpm
from {{ ref('mart_cost_by_category_monthly') }} c
left join {{ ref('mart_pmpm_monthly') }} p
    on c.service_month = p.service_month and c.payer_id = p.payer_id
where p.pmpm_month_id is null
group by 1, 2
having sum(c.paid_to_date) <> 0
