-- fct_claims is maintained incrementally (only changed claims are rebuilt).
-- int_claim_state rebuilds every claim from the full ledger. If the
-- incremental logic ever drifts, the two disagree and this test returns rows.
with incremental as (
    select claim_id, paid_amount, allowed_amount, claim_status, event_count from {{ ref('fct_claims') }}
),

full_rebuild as (
    select s.claim_id, s.paid_amount, s.allowed_amount, s.claim_status, s.event_count
    from {{ ref('int_claim_state') }} s
    inner join {{ ref('stg_claims') }} c on s.claim_id = c.claim_id
)

select coalesce(i.claim_id, f.claim_id) as claim_id, i.paid_amount as incremental_paid, f.paid_amount as full_paid
from incremental i
full outer join full_rebuild f on i.claim_id = f.claim_id
where i.claim_id is null
   or f.claim_id is null
   or i.paid_amount   <> f.paid_amount
   or i.allowed_amount <> f.allowed_amount
   or i.claim_status  <> f.claim_status
   or i.event_count   <> f.event_count
