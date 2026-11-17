-- Every dollar in the ledger must show up exactly once in each finance mart.
-- Catches fan-out joins and dropped rows, the two classic ways a mart lies.
with ledger as (
    select sum(paid_amount_delta) as paid from {{ ref('fct_claim_events') }} where not is_orphan_event
),

checks as (
    select 'fct_claims' as mart, sum(paid_amount) as paid from {{ ref('fct_claims') }}
    union all
    select 'mart_cost_by_category_monthly', sum(paid_to_date) from {{ ref('mart_cost_by_category_monthly') }}
    union all
    select 'mart_claims_triangle', sum(paid_in_lag) from {{ ref('mart_claims_triangle') }}
)

select c.mart, c.paid as mart_paid, l.paid as ledger_paid
from checks c
cross join ledger l
where c.paid <> l.paid
