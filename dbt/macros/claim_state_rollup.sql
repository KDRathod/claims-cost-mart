{#
  Collapse a claim's ledger into its current state.

  Shared by int_claim_state (all claims, used by tests) and fct_claims
  (only the claims that changed). A claim is always rebuilt from ALL of its
  events, never patched with just the new ones, so the result does not
  depend on the order the events arrived in.
#}
{% macro claim_state_rollup(events_cte) %}
with rolled_up as (

    select
        claim_id,
        {{ money('sum(allowed_amount_delta)') }}                       as allowed_amount,
        {{ money('sum(paid_amount_delta)') }}                          as paid_amount,
        {{ money('sum(member_responsibility_delta)') }}                as member_responsibility_amount,
        min(case when event_type = 'SUBMITTED' then posted_date end)   as submitted_date,
        min(case when event_type in ('PAID', 'DENIED') then posted_date end) as first_decision_date,
        min(case when event_type = 'PAID' then posted_date end)        as first_paid_date,
        max(posted_date)                                               as last_posted_date,
        max(_loaded_at)                                                as last_loaded_at,
        count(*)                                                       as event_count,
        max(case when event_type = 'DENIED' then 1 else 0 end) = 1     as was_ever_denied,
        max(case when event_type = 'ADJUSTED' then 1 else 0 end) = 1   as was_adjusted,
        max(case when event_type = 'REVERSED' then 1 else 0 end) = 1   as was_reversed,
        max(case when reason_code = 'APPEAL' then 1 else 0 end) = 1    as was_overturned_on_appeal,
        -- the most recent decision decides the status
        max_by(
            case when event_type <> 'SUBMITTED' then event_type end,
            case when event_type <> 'SUBMITTED' then event_at end
        )                                                              as last_decision_event,
        max_by(
            case when event_type = 'DENIED' then reason_code end,
            case when event_type = 'DENIED' then event_at end
        )                                                              as denial_reason_code
    from {{ events_cte }}
    group by claim_id

)

select
    *,
    case
        when last_decision_event is null             then 'pending'
        when last_decision_event = 'DENIED'          then 'denied'
        when last_decision_event = 'REVERSED'        then 'reversed'
        when last_decision_event in ('PAID', 'ADJUSTED') then 'paid'
    end as claim_status
from rolled_up
{% endmacro %}
