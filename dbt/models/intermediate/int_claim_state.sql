{#
  Current state of every claim, rebuilt from the full ledger.

  This is the slow-but-obviously-correct version. fct_claims computes the
  same thing incrementally, and a test checks the two always agree.
#}

with events as (
    select * from {{ ref('fct_claim_events') }}
),

state as (
    {{ claim_state_rollup('events') }}
)

select * from state
